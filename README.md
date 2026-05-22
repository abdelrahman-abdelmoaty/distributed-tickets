# Distributed Ticket Booking System
CSE463 Distributed Systems — Spring 2026

A peer-to-peer distributed event-ticket booking system with three+ active
replicas, Lamport logical clocks, per-seat Ricart-Agrawala mutual
exclusion, synchronous active replication, heartbeat-based failure
detection, and a Tkinter desktop client.

## File layout

```
.
├── name_service.py      # Standalone TCP registry / failure detector
├── peer.py              # Replica peer (run 3+ instances)
├── client.py            # Tkinter GUI client
├── common.py            # Lamport clock, JSON framing, ORB helpers
├── run_tests.py         # Headless end-to-end test harness
├── ARCHITECTURE.md      # Design notes used as the seed for the report
├── REPORT.md / REPORT.docx        # Full project report
├── REPORT_DECK.pptx     # Presentation deck (12 slides)
├── AI_DISCLOSURE.md     # Generative-AI usage disclosure
├── TEST_RESULTS.txt     # Sample output of run_tests.py
└── demo1.mp4 … demo4.mp4  # Demo videos (see below)
```

## Demo videos

Four short screen recordings covering the four rubric scenarios.

https://github.com/user-attachments/assets/07068142-12c7-4226-833a-45c65fc31d28

https://github.com/user-attachments/assets/e6e6da52-abed-4c9d-b73b-d2d2780d9a75

https://github.com/user-attachments/assets/4cddec0c-8f09-40fd-b7e5-0aa3d3f0a465

https://github.com/user-attachments/assets/3a2b32ba-5e93-42cd-b942-a3bb639d04e0


## Requirements

- Python 3.9 or newer (3.10+ recommended).
- Tkinter (bundled with most Python installs; on Ubuntu install
  `python3-tk`).
- No third-party packages are required — `socket`, `threading`,
  `json`, `tkinter` from the standard library are enough.

## Running a 3-peer cluster locally

Open four terminals (or use tmux / screen).

**Terminal 1 — name service:**
```bash
python3 name_service.py --host 127.0.0.1 --port 5000
```

**Terminals 2, 3, 4 — peers:**
```bash
python3 peer.py --id P1 --port 5101 --ns-host 127.0.0.1 --ns-port 5000 --seats 20
python3 peer.py --id P2 --port 5102 --ns-host 127.0.0.1 --ns-port 5000 --seats 20
python3 peer.py --id P3 --port 5103 --ns-host 127.0.0.1 --ns-port 5000 --seats 20
```

`--seats` must match across every peer (otherwise replicas disagree on
which seat IDs exist).

**Terminal 5+ — clients (one per ticket buyer):**
```bash
python3 client.py --ns-host 127.0.0.1 --ns-port 5000
```

By default the client picks a peer at random; pin it to a specific peer
with `--peer P2` to demonstrate two clients on different peers booking
concurrently:

```bash
python3 client.py --id alice --peer P1
python3 client.py --id bob   --peer P2
```

## Demonstrating each rubric requirement

| Rubric line | How to show it |
|---|---|
| Logical clocks | Watch the peer logs — every booking line prints `ts=N`. Higher ts ⇒ later in the partial order. |
| Distributed mutex | Click the same seat in two clients pinned to different peers within ~100ms. Exactly one client sees "booked", the other sees "already booked". |
| Three replicas | Three peer processes running. After any booking, the seat colour updates in every other client within ~POLL_MS (~1.5s). |
| Active replication | Peer logs show `applied REPLICATE` lines on the non-originating peers. |
| Peer join | Start peers 1–3, book several seats, then start peer 4 (`--port 5104`). Its log shows `installed snapshot from PX`. A client pinned to peer 4 sees every prior booking. |
| Peer leave | Ctrl-C peer 2. Within ~6 s the name service logs `PEER_DEAD detected P2`. Clients on the other peers continue working; clients on P2 reconnect to another peer automatically. |
| Crash failure | `kill -9 <peer_pid>`. Same outcome as graceful leave but path goes through heartbeat timeout. |

## Running the automated tests

```bash
python3 run_tests.py
```

This spawns the name service and three peers as subprocesses, drives
booking requests through the real TCP protocol (no in-process shortcuts),
and asserts on the seat map of every surviving peer. Covers all four
rubric scenarios (concurrent different seats, concurrent same seat,
late peer join, peer crash). Sample output is saved in `TEST_RESULTS.txt`.

## Tunables

In `name_service.py`:
- `HEARTBEAT_INTERVAL_SECS = 2.0` — peers send a heartbeat this often.
- `DEAD_AFTER_SECS = 6.0` — declared dead after this many seconds of silence.

In `peer.py`:
- `DEFAULT_NUM_SEATS = 20` — overridden by `--seats`.

In `client.py`:
- `POLL_MS = 1500` — seat-grid refresh interval.
- `COLS = 5` — seat-grid width.

## Known limitations / non-goals

- Single name service is a single point of failure (mitigated only by
  the fact that peers re-register on heartbeat; if it stays down,
  newly-launched clients can't discover peers).
- Best-effort replication on partial failure: if every other replica
  fails mid-write, the originating peer still commits and tells the
  client "booked" (logged as a warning). For a stronger guarantee we
  would need a configurable write quorum.
- Peer is trusted (no Byzantine handling).
- Seats are FREE/BOOKED only — no cancellation. Adding cancellation
  would be a straight extension of the existing RA+replicate path.

## Cleanup

If a previous run left processes lingering:
```bash
pkill -f name_service.py
pkill -f peer.py
```
