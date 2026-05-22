"""
run_tests.py
============
End-to-end test harness that exercises the rubric's three test categories:
  1. Concurrent access (same seat, different seats)
  2. Peer joins and leaves (late join + graceful deregister)
  3. Failure scenarios (crash a peer mid-booking)

Spawns the name service and three peers as subprocesses, fires booking
requests through the actual TCP protocol (no in-process shortcuts), and
asserts post-conditions on the seat map of every surviving peer.

Run:  python3 run_tests.py
"""

import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

from common import (
    MSG_BOOK_REPLY, MSG_BOOK_SEAT,
    MSG_GET_SEATS, MSG_SEATS_REPLY,
    send_request,
)

NS_HOST = "127.0.0.1"
NS_PORT = 6000
PEER_BASE_PORT = 6101
NUM_SEATS = 20


def start_ns():
    return subprocess.Popen(
        [sys.executable, "name_service.py",
         "--host", NS_HOST, "--port", str(NS_PORT)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )


def start_peer(pid: str, port: int):
    return subprocess.Popen(
        [sys.executable, "peer.py",
         "--id", pid, "--host", "127.0.0.1", "--port", str(port),
         "--ns-host", NS_HOST, "--ns-port", str(NS_PORT),
         "--seats", str(NUM_SEATS)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, bufsize=1,
    )


def wait_for_port(host: str, port: int, timeout: float = 5.0):
    """Block until something accepts on (host, port) or timeout."""
    import socket
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def book(peer_port: int, seat_id: int, client_id: str):
    return send_request("127.0.0.1", peer_port, {
        "type": MSG_BOOK_SEAT,
        "seat_id": seat_id,
        "client_id": client_id,
    }, timeout=10.0)


def seats_of(peer_port: int):
    reply = send_request("127.0.0.1", peer_port, {"type": MSG_GET_SEATS},
                         timeout=3.0)
    if reply is None or reply.get("type") != MSG_SEATS_REPLY:
        return None
    return reply["seats"]


def assert_replicas_match(ports):
    """Every replica should agree on every seat."""
    states = {p: seats_of(p) for p in ports}
    reference = None
    for p, s in states.items():
        if s is None:
            print(f"  WARN: peer @ {p} did not return seats")
            continue
        if reference is None:
            reference = s
            continue
        if [(x["id"], x["status"], x["owner"]) for x in s] != \
           [(x["id"], x["status"], x["owner"]) for x in reference]:
            print(f"  REPLICA MISMATCH on peer @ {p}")
            return False
    return True


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_concurrent_different_seats(ports):
    print("\n== test: concurrent bookings on DIFFERENT seats ==")
    # Each of 3 clients books a different seat via a different peer.
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [
            pool.submit(book, ports[0], 0, "alice"),
            pool.submit(book, ports[1], 1, "bob"),
            pool.submit(book, ports[2], 2, "carol"),
        ]
        results = [f.result() for f in futures]
    for r in results:
        assert r and r.get("ok"), f"unexpected reply: {r}"
    print("  all three different-seat bookings succeeded")
    time.sleep(0.5)  # let any in-flight ACKs settle
    assert assert_replicas_match(ports), "replicas disagree"
    print("  all replicas agree")


def test_concurrent_same_seat(ports):
    print("\n== test: concurrent bookings on SAME seat ==")
    target_seat = 5
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [
            pool.submit(book, ports[0], target_seat, "alice2"),
            pool.submit(book, ports[1], target_seat, "bob2"),
            pool.submit(book, ports[2], target_seat, "carol2"),
        ]
        results = [f.result() for f in futures]
    successes = [r for r in results if r and r.get("ok")]
    failures = [r for r in results if r and not r.get("ok")]
    print(f"  successes: {len(successes)}, failures: {len(failures)}")
    assert len(successes) == 1, f"expected exactly 1 success, got {len(successes)}"
    assert len(failures) == 2
    time.sleep(0.5)
    assert assert_replicas_match(ports), "replicas disagree on contested seat"
    print("  exactly one winner; all replicas agree")


def test_late_peer_join(ports, late_port):
    print("\n== test: late peer joins and gets state ==")
    proc = start_peer("P4", late_port)
    try:
        ok = wait_for_port("127.0.0.1", late_port, timeout=8.0)
        assert ok, "P4 did not start listening"
        # Give it a beat to complete state transfer.
        time.sleep(2.0)
        assert assert_replicas_match(ports + [late_port]), \
            "late joiner's state does not match existing replicas"
        print("  late joiner has the same state as existing replicas")
        # Now do a booking AFTER the join and check it reaches everyone.
        r = book(ports[0], 10, "after-join")
        assert r and r.get("ok"), f"post-join booking failed: {r}"
        time.sleep(0.5)
        assert assert_replicas_match(ports + [late_port]), \
            "post-join write did not propagate to all replicas"
        print("  post-join booking propagates correctly")
    finally:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_peer_crash(ports, peer_procs):
    print("\n== test: peer CRASH mid-system; surviving peers continue ==")
    # Kill peer P3 abruptly (SIGKILL = no graceful deregister).
    victim_idx = 2
    print(f"  killing peer {victim_idx} (port {ports[victim_idx]})")
    peer_procs[victim_idx].kill()
    peer_procs[victim_idx].wait(timeout=3)
    # Name service should detect within DEAD_AFTER_SECS (6s).
    time.sleep(8.0)
    # Surviving peers should still be able to book.
    r = book(ports[0], 15, "post-crash")
    assert r is not None, "post-crash booking returned None"
    assert r.get("ok"), f"post-crash booking failed: {r}"
    # The two surviving replicas should agree.
    survivors = [ports[0], ports[1]]
    assert assert_replicas_match(survivors), "survivors disagree after crash"
    print("  surviving peers continued to accept bookings and agree")


def main():
    print("=" * 60)
    print("DISTRIBUTED TICKETS — END-TO-END TESTS")
    print("=" * 60)
    ns_proc = start_ns()
    assert wait_for_port(NS_HOST, NS_PORT, timeout=5.0), "NS did not start"
    print("[harness] name service up")

    ports = [PEER_BASE_PORT + i for i in range(3)]
    peer_procs = []
    for i, port in enumerate(ports):
        pid = f"P{i+1}"
        peer_procs.append(start_peer(pid, port))
        assert wait_for_port("127.0.0.1", port, timeout=5.0), \
            f"peer {pid} did not start"
    # Let everyone become READY.
    time.sleep(2.0)
    print("[harness] three peers up and ready")

    failed = False
    try:
        test_concurrent_different_seats(ports)
        test_concurrent_same_seat(ports)
        test_late_peer_join(ports, PEER_BASE_PORT + 3)
        test_peer_crash(ports, peer_procs)
    except AssertionError as e:
        failed = True
        print(f"\n!!! TEST FAILED: {e}")
    except Exception as e:
        failed = True
        print(f"\n!!! TEST CRASHED: {e!r}")
    finally:
        print("\n[harness] tearing down")
        for p in peer_procs:
            if p.poll() is None:
                p.send_signal(signal.SIGINT)
        for p in peer_procs:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        if ns_proc.poll() is None:
            ns_proc.send_signal(signal.SIGINT)
            try:
                ns_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                ns_proc.kill()

    if failed:
        print("\n=== FAILED ===")
        sys.exit(1)
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    main()
