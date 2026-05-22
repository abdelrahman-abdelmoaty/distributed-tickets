"""
client.py
=========
A Tkinter desktop client for the distributed ticket-booking system.

Boot-up sequence:
  1. Ask the name service for the live peer list.
  2. Pick one peer at random (or as overridden on the command line).
  3. Start a polling loop that refreshes the seat grid every POLL_MS.
  4. Render a clickable grid; clicking a seat sends BOOK_SEAT.

If the chosen peer becomes unreachable mid-session, the client refreshes
the peer list from the name service and switches to another live peer
transparently.

The UI is intentionally simple — the assignment grading is on the
distributed mechanics, not on UI polish. We just need:
  - clear visual distinction FREE vs BOOKED
  - a status bar showing which peer we're connected to (so the demo
    video can show "Client A is on Peer 1, Client B is on Peer 2")
  - a refresh button for live demo control
"""

import argparse
import random
import string
import threading
import tkinter as tk
from tkinter import messagebox

from common import (
    MSG_BOOK_REPLY,
    MSG_BOOK_SEAT,
    MSG_GET_SEATS,
    MSG_LIST_PEERS,
    MSG_PEERS_REPLY,
    MSG_SEATS_REPLY,
    send_request,
)


POLL_MS = 1500            # how often the GUI refreshes its seat view
COLS = 5                  # seats per row in the grid


# ---------------------------------------------------------------------------
# Color scheme: kept explicit so the demo video reads clearly.
# ---------------------------------------------------------------------------
COLOR_FREE         = "#cce8cc"   # pale green
COLOR_FREE_HOVER   = "#a8d8a8"
COLOR_BOOKED       = "#e8b8b8"   # pale red
COLOR_BOOKED_MINE  = "#b8d8e8"   # pale blue when I own it
COLOR_DISABLED_FG  = "#666666"


class TicketClient:
    def __init__(self, ns_host: str, ns_port: int, client_id: str,
                 preferred_peer: str = None):
        self.ns_host = ns_host
        self.ns_port = ns_port
        self.client_id = client_id
        self.preferred_peer = preferred_peer
        self.current_peer = None        # dict: peer_id, host, port

        self.seats_state = []           # list of {id, status, owner}
        self.buttons = {}               # seat_id -> tk.Button

        # -- Tk setup ---------------------------------------------------
        self.root = tk.Tk()
        self.root.title(f"Tickets — client {client_id}")
        self.root.geometry("520x520")

        self.status_var = tk.StringVar(value="Connecting…")
        status_bar = tk.Label(
            self.root, textvariable=self.status_var, anchor="w",
            relief=tk.SUNKEN, font=("Helvetica", 11), padx=8, pady=4,
        )
        status_bar.pack(side=tk.TOP, fill=tk.X)

        header = tk.Frame(self.root)
        header.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)
        tk.Label(header, text="EVENT: Demo Event — pick a seat",
                 font=("Helvetica", 14, "bold")).pack(side=tk.LEFT)

        legend = tk.Frame(self.root)
        legend.pack(side=tk.TOP, fill=tk.X, padx=8)
        for text, color in (
            ("FREE", COLOR_FREE),
            ("BOOKED", COLOR_BOOKED),
            ("YOURS", COLOR_BOOKED_MINE),
        ):
            tk.Label(legend, text=text, bg=color, padx=8, pady=2,
                     relief=tk.SOLID, borderwidth=1).pack(side=tk.LEFT, padx=4)

        self.grid_frame = tk.Frame(self.root)
        self.grid_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True,
                             padx=8, pady=8)

    # =====================================================================
    # Peer discovery
    # =====================================================================

    def _fetch_peer_list(self):
        reply = send_request(self.ns_host, self.ns_port,
                             {"type": MSG_LIST_PEERS}, timeout=3.0)
        if not reply or reply.get("type") != MSG_PEERS_REPLY:
            return []
        return reply.get("peers", [])

    def _pick_peer(self) -> bool:
        peers = self._fetch_peer_list()
        if not peers:
            self.status_var.set("No peers registered with the name service.")
            return False

        if self.preferred_peer:
            for p in peers:
                if p["peer_id"] == self.preferred_peer:
                    self.current_peer = p
                    return True
            # Preferred not found — fall through to random pick.

        self.current_peer = random.choice(peers)
        return True

    # =====================================================================
    # Server interactions
    # =====================================================================

    def _get_seats(self):
        if self.current_peer is None:
            return None
        return send_request(
            self.current_peer["host"], self.current_peer["port"],
            {"type": MSG_GET_SEATS}, timeout=3.0,
        )

    def _book_seat(self, seat_id: int):
        if self.current_peer is None:
            return None
        return send_request(
            self.current_peer["host"], self.current_peer["port"],
            {"type": MSG_BOOK_SEAT, "seat_id": seat_id,
             "client_id": self.client_id},
            timeout=10.0,  # booking involves RA + replication
        )

    # =====================================================================
    # GUI flow
    # =====================================================================

    def start(self):
        self._reconnect()
        self.root.after(POLL_MS, self._poll_tick)
        self.root.mainloop()

    def _reconnect(self):
        ok = self._pick_peer()
        if ok:
            self.status_var.set(
                f"Connected to peer {self.current_peer['peer_id']}"
                f" @ {self.current_peer['host']}:{self.current_peer['port']}"
            )
            self._refresh_now()
        else:
            self.status_var.set("Could not reach any peer. Retrying…")
            self.root.after(2000, self._reconnect)

    def _poll_tick(self):
        self._refresh_seats_async()
        self.root.after(POLL_MS, self._poll_tick)

    def _refresh_now(self):
        self._refresh_seats_async()

    def _refresh_seats_async(self):
        # Network call off the Tk main thread so the UI doesn't freeze.
        threading.Thread(target=self._refresh_seats_worker,
                         daemon=True).start()

    def _refresh_seats_worker(self):
        reply = self._get_seats()
        if reply is None:
            # Current peer is unreachable. Switch to another.
            self.root.after(0, self._handle_peer_lost)
            return
        if reply.get("type") != MSG_SEATS_REPLY:
            # Likely "peer not ready" — wait and try again.
            self.root.after(0, lambda: self.status_var.set(
                f"Peer {self.current_peer['peer_id']} not ready, waiting…"))
            return
        self.seats_state = reply["seats"]
        self.root.after(0, self._render_grid)

    def _handle_peer_lost(self):
        old_id = self.current_peer["peer_id"] if self.current_peer else "?"
        self.status_var.set(f"Lost connection to {old_id}. Reconnecting…")
        self.current_peer = None
        self._reconnect()

    def _render_grid(self):
        for w in self.grid_frame.winfo_children():
            w.destroy()
        self.buttons = {}

        for idx, seat in enumerate(self.seats_state):
            row, col = divmod(idx, COLS)
            seat_id = seat["id"]
            status = seat["status"]
            owner = seat.get("owner")
            label = f"Seat {seat_id+1}\n"
            if status == "BOOKED":
                if owner == self.client_id:
                    color = COLOR_BOOKED_MINE
                    label += "YOURS"
                    fg = "#003355"
                else:
                    color = COLOR_BOOKED
                    short_owner = (owner or "")[:8]
                    label += f"booked\n({short_owner})"
                    fg = "#552222"
                clickable = False
            else:
                color = COLOR_FREE
                label += "FREE"
                fg = "#114411"
                clickable = True

            # We use tk.Label rather than tk.Button because on macOS the
            # native button widget ignores `bg` and renders as a plain
            # grey rounded button regardless. Labels honour the colour.
            cell = tk.Label(
                self.grid_frame, text=label, bg=color, fg=fg,
                width=10, height=3,
                font=("Helvetica", 11, "bold"),
                relief=tk.RAISED, borderwidth=2,
            )
            cell.grid(row=row, column=col, padx=4, pady=4)
            if clickable:
                cell.bind("<Button-1>",
                          lambda e, s=seat_id: self._on_seat_click(s))
                cell.bind("<Enter>",
                          lambda e, c=cell: c.config(bg=COLOR_FREE_HOVER))
                cell.bind("<Leave>",
                          lambda e, c=cell: c.config(bg=COLOR_FREE))
                cell.config(cursor="hand2")
            self.buttons[seat_id] = cell

    # =====================================================================
    # Booking
    # =====================================================================

    def _on_seat_click(self, seat_id: int):
        cell = self.buttons.get(seat_id)
        if cell:
            # Indicate the click visually and stop the cell from
            # responding to further input until the next refresh.
            cell.unbind("<Button-1>")
            cell.unbind("<Enter>")
            cell.unbind("<Leave>")
            cell.config(text=f"Seat {seat_id+1}\nbooking…",
                        bg="#f0e090", cursor="")
        self.status_var.set(f"Booking seat {seat_id+1}…")
        threading.Thread(target=self._book_seat_worker,
                         args=(seat_id,), daemon=True).start()

    def _book_seat_worker(self, seat_id: int):
        reply = self._book_seat(seat_id)
        self.root.after(0, lambda: self._book_seat_done(seat_id, reply))

    def _book_seat_done(self, seat_id: int, reply):
        if reply is None:
            messagebox.showerror(
                "Booking failed",
                f"Could not reach peer to book seat {seat_id+1}.")
            self._handle_peer_lost()
            return
        if reply.get("type") != MSG_BOOK_REPLY:
            messagebox.showerror(
                "Booking failed",
                f"Unexpected reply: {reply}")
            return
        if reply.get("ok"):
            self.status_var.set(
                f"Seat {seat_id+1} booked. write_id={reply.get('write_id','?')}")
        else:
            self.status_var.set(
                f"Seat {seat_id+1} not booked: {reply.get('reason','?')}")
        # Either way, refresh.
        self._refresh_seats_async()


def random_client_id():
    return "C-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=4))


def main():
    parser = argparse.ArgumentParser(description="Ticket-booking client (Tkinter)")
    parser.add_argument("--ns-host", default="127.0.0.1")
    parser.add_argument("--ns-port", type=int, default=5000)
    parser.add_argument("--id", default=None,
                        help="Client identifier (auto-generated if omitted)")
    parser.add_argument("--peer", default=None,
                        help="Preferred peer id (otherwise random)")
    args = parser.parse_args()
    client_id = args.id or random_client_id()
    client = TicketClient(
        ns_host=args.ns_host,
        ns_port=args.ns_port,
        client_id=client_id,
        preferred_peer=args.peer,
    )
    client.start()


if __name__ == "__main__":
    main()
