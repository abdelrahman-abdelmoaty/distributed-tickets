"""
name_service.py
===============
A single-process TCP registry that peers and clients use to discover each
other. Responsibilities:

  - REGISTER(peer_id, host, port)   -> add peer, push PEER_JOINED to others
  - DEREGISTER(peer_id)             -> remove peer, push PEER_DEAD to others
  - HEARTBEAT(peer_id)              -> refresh last-seen timestamp
  - LIST_PEERS                      -> return live peer list

A background sweeper thread declares a peer dead if its last heartbeat is
older than DEAD_AFTER_SECS, removes it from the registry, and pushes a
PEER_DEAD notification to the surviving peers.

Push notifications are short outbound TCP connections to each peer's own
listening socket. The name service therefore needs the (host, port) it
collected at registration time — which it already has.

The address of this name service is assumed to be globally known (we hard-
code it in config.py and pass it via the command line).
"""

import argparse
import threading
import time

from common import (
    LamportClock,           # not strictly needed, but useful for trace logs
    MSG_DEREGISTER,
    MSG_DEREGISTER_ACK,
    MSG_HEARTBEAT,
    MSG_HEARTBEAT_ACK,
    MSG_LIST_PEERS,
    MSG_PEERS_REPLY,
    MSG_REGISTER,
    MSG_REGISTER_ACK,
    MSG_ERROR,
    ThreadedTCPServer,
    recv_msg,
    send_msg,
    send_oneway,
)


# -------- Configuration --------
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 5000
HEARTBEAT_INTERVAL_SECS = 2.0   # peers send a heartbeat this often
DEAD_AFTER_SECS = 6.0           # 3 missed heartbeats -> declared dead
SWEEP_INTERVAL_SECS = 1.0


# Push-notification message types — defined here because they are emitted
# only by the NameService. Peers must accept them.
MSG_PEER_JOINED = "PEER_JOINED"
MSG_PEER_DEAD   = "PEER_DEAD"


class NameService:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        # peers: peer_id -> {"host": str, "port": int, "last_seen": float}
        self._peers: dict = {}
        self._lock = threading.Lock()
        self._server = ThreadedTCPServer(host, port, self._handle_conn)
        self._sweep_stop = threading.Event()
        self._sweep_thread = threading.Thread(
            target=self._sweep_loop, daemon=True,
        )

    # -------- lifecycle --------

    def start(self) -> None:
        self._server.start()
        self._sweep_thread.start()
        print(f"[name_service] listening on {self.host}:{self.port}")

    def stop(self) -> None:
        self._sweep_stop.set()
        self._server.stop()

    # -------- request handler --------

    def _handle_conn(self, conn, addr) -> None:
        try:
            msg = recv_msg(conn)
        except (ConnectionError, ValueError):
            return
        mtype = msg.get("type")

        if mtype == MSG_REGISTER:
            self._on_register(conn, msg)
        elif mtype == MSG_DEREGISTER:
            self._on_deregister(conn, msg)
        elif mtype == MSG_HEARTBEAT:
            self._on_heartbeat(conn, msg)
        elif mtype == MSG_LIST_PEERS:
            self._on_list_peers(conn)
        else:
            try:
                send_msg(conn, {"type": MSG_ERROR,
                                "reason": f"unknown type {mtype!r}"})
            except OSError:
                pass

    # -------- individual handlers --------

    def _on_register(self, conn, msg: dict) -> None:
        pid = msg["peer_id"]
        host = msg["host"]
        port = int(msg["port"])
        now = time.monotonic()

        # Snapshot the *other* peers before we add the new one so we can
        # push PEER_JOINED to them after replying.
        with self._lock:
            other_peers = [
                (p, info["host"], info["port"])
                for p, info in self._peers.items() if p != pid
            ]
            self._peers[pid] = {
                "host": host, "port": port, "last_seen": now,
            }
        send_msg(conn, {"type": MSG_REGISTER_ACK, "peer_id": pid})
        print(f"[name_service] REGISTER {pid} @ {host}:{port}")

        # Fan out a PEER_JOINED to everyone else. Done in a background
        # thread to avoid blocking the request handler.
        threading.Thread(
            target=self._notify_all,
            args=(other_peers,
                  {"type": MSG_PEER_JOINED,
                   "peer_id": pid, "host": host, "port": port}),
            daemon=True,
        ).start()

    def _on_deregister(self, conn, msg: dict) -> None:
        pid = msg["peer_id"]
        with self._lock:
            self._peers.pop(pid, None)
            other_peers = [
                (p, info["host"], info["port"])
                for p, info in self._peers.items()
            ]
        send_msg(conn, {"type": MSG_DEREGISTER_ACK, "peer_id": pid})
        print(f"[name_service] DEREGISTER {pid}")
        threading.Thread(
            target=self._notify_all,
            args=(other_peers, {"type": MSG_PEER_DEAD, "peer_id": pid}),
            daemon=True,
        ).start()

    def _on_heartbeat(self, conn, msg: dict) -> None:
        pid = msg["peer_id"]
        with self._lock:
            if pid in self._peers:
                self._peers[pid]["last_seen"] = time.monotonic()
                known = True
            else:
                # Peer wasn't registered (or was reaped). Tell it so it can
                # re-REGISTER.
                known = False
        send_msg(conn, {
            "type": MSG_HEARTBEAT_ACK,
            "peer_id": pid,
            "known": known,
        })

    def _on_list_peers(self, conn) -> None:
        with self._lock:
            peers = [
                {"peer_id": p, "host": info["host"], "port": info["port"]}
                for p, info in self._peers.items()
            ]
        send_msg(conn, {"type": MSG_PEERS_REPLY, "peers": peers})

    # -------- background failure detector --------

    def _sweep_loop(self) -> None:
        while not self._sweep_stop.wait(SWEEP_INTERVAL_SECS):
            now = time.monotonic()
            dead_ids = []
            remaining = []
            with self._lock:
                for pid, info in list(self._peers.items()):
                    if now - info["last_seen"] > DEAD_AFTER_SECS:
                        dead_ids.append(pid)
                        del self._peers[pid]
                for p, info in self._peers.items():
                    remaining.append((p, info["host"], info["port"]))

            for dead in dead_ids:
                print(f"[name_service] PEER_DEAD detected {dead} "
                      f"(no heartbeat for >{DEAD_AFTER_SECS}s)")
                self._notify_all(
                    remaining,
                    {"type": MSG_PEER_DEAD, "peer_id": dead},
                )

    @staticmethod
    def _notify_all(targets, msg: dict) -> None:
        """Best-effort fan-out of a push notification to every target peer."""
        for pid, host, port in targets:
            ok = send_oneway(host, port, msg, timeout=2.0)
            if not ok:
                print(f"[name_service] push to {pid} @ {host}:{port} "
                      f"failed (will be caught by next sweep if dead)")


def main():
    parser = argparse.ArgumentParser(description="Distributed-tickets name service")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    ns = NameService(args.host, args.port)
    ns.start()
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\n[name_service] shutting down")
        ns.stop()


if __name__ == "__main__":
    main()
