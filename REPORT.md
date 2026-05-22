# Distributed Ticket Booking System

**CSE463 Distributed Systems — Spring 2026 — Project Report**

**Submission date:** May 20, 2026
**Repository:** https://github.com/abdelrahman-abdelmoaty/distributed-tickets

---

## 1. Application Features

The system implements an online ticket-booking service for a single event
with a fixed seat inventory. The user-visible features are:

The user opens a desktop client and is shown a live grid of seats. Each
seat is either free or already booked, with free seats clickable. A
click sends a booking request to the backend, which either confirms
the seat ("booked") or returns a reason it failed (most commonly,
another buyer got it first). The same seat-map view is consistent
across all clients regardless of which backend replica they happen to
be connected to.

Behind that interface the backend is a peer-to-peer cluster of three or
more replicas. Each replica holds a full copy of the seat map; clients
are routed to a replica through a name service at startup. The cluster
tolerates a peer failing or leaving and lets a new peer join at any
time and catch up automatically.

## 2. System Assumptions

Network reliability is assumed to be high enough that a TCP connection
between any two live components succeeds (we do not implement
Byzantine fault tolerance, leader election, or partition handling).
Peers are trusted; an attacker with control of a peer process is out
of scope. The seat count is fixed at startup and identical on every
peer (configured via a CLI flag). The name service's address is
globally known and configured statically. Clocks across peers may
drift arbitrarily — all synchronization is via Lamport logical clocks,
not wall-clock time.

## 3. System Architecture

### 3.1 Component Diagram

```
                +---------------------+
                |    NameService      |   global registry
                |     127.0.0.1:5000  |   peers REGISTER + HEARTBEAT here
                +----+------------+---+   clients ask it for the peer list
                     ^            ^
        register / heartbeat      LIST_PEERS
                     |            |
         +-----------+--+      +--+----------+
         |              |      |             |
     +-------+      +-------+      +-------+
     | Peer1 |<---->| Peer2 |<---->| Peer3 |     full mesh
     +-------+      +-------+      +-------+     JSON over TCP
        ^              ^              ^
        |              |              |
      Client A       Client B       Client C    JSON over TCP
     (Tkinter)      (Tkinter)      (Tkinter)
```

Components and their roles: the **NameService** is a single TCP process
holding a registry of live peer addresses; peers register on startup
and send a heartbeat every 2 seconds. Each **Peer** runs a TCP server,
holds a full replica of the seat map, and participates in a
Ricart-Agrawala mutual-exclusion protocol per seat. Each **Client** is
a Tkinter GUI process that picks a peer via the NameService, polls
that peer for the current seat map, and sends BOOK_SEAT requests
through the same connection.

### 3.2 Communication Flow Diagram

The end-to-end flow of a single booking — the central use case — is
shown below. The originating peer P1 holds the lock during the entire
flow; the other replicas apply the write only when REPLICATE arrives.

```
Client                Peer P1                 Peer P2, P3 (replicas)
  |                     |                            |
  | -- BOOK(seat=5) --> |                            |
  |                     | --- LOCK_REQUEST(5,ts) --> |   Ricart-Agrawala
  |                     |                            |   per-seat mutex
  |                     | <----- LOCK_REPLY(5) ----- |
  |                     |                            |
  |                     | (apply locally: seat 5 = booked)
  |                     |                            |
  |                     | --- REPLICATE(5, owner) -> |   active replication
  |                     | <--- REPLICATE_ACK(5) ---- |   synchronous wait
  |                     |                            |
  |                     | (release lock — send       |
  |                     |  any deferred LOCK_REPLYs) |
  |                     |                            |
  | <- BOOK_REPLY(ok) - |                            |
```

### 3.3 Communication Model

The system uses a **hybrid** paradigm. Client↔peer is client-server (a
client sends requests, a peer replies). Peer↔peer is peer-to-peer
(every peer can talk to every other peer). Peer↔NameService is
client-server.

The wire protocol is **length-prefixed JSON over TCP**: every message
is a 4-byte big-endian unsigned length header followed by a UTF-8
JSON payload. Length prefixing is necessary because TCP is a byte
stream, not a message stream — the receiver uses the prefix to know
where one message ends and the next begins. JSON was chosen for
debuggability (a packet capture is human readable) and because the
standard library handles serialization with zero dependencies.

The **name service interface** is straightforward: peers send
`REGISTER {peer_id, host, port}` on startup and `HEARTBEAT {peer_id}`
every 2 seconds; clients and peers send `LIST_PEERS` to discover the
current membership. The name service pushes `PEER_JOINED` and
`PEER_DEAD` notifications to peers when membership changes, so peers
do not have to poll.

The **ORB-like middleware** is a small set of helpers in `common.py`
that abstract the connect / frame / parse / close lifecycle behind a
single call. `send_request(host, port, msg)` opens a connection, sends
one length-prefixed JSON message, reads exactly one reply, and closes.
`broadcast(targets, msg)` is the same operation against every target
concurrently (one thread per target) and returns a per-target
reply dict. Every peer↔peer and client↔peer remote call goes through
these helpers.

### 3.4 Synchronization and Concurrency Control

**Clock mechanism:** Lamport logical clocks. Each peer holds an integer
counter. The counter is incremented on any event that needs a
timestamp (e.g. starting a booking request). On receiving a message
carrying a remote timestamp `t`, the counter is updated to
`max(local, t) + 1`. The resulting `(timestamp, peer_id)` pair is a
total order on all inter-peer events — this is what Ricart-Agrawala
uses to break ties between concurrent requests.

**Distributed mutex:** classical Ricart-Agrawala, request/reply style,
**per seat**. A peer that wants to book seat `S` increments its
Lamport clock, records the request's `(ts, peer_id)`, and broadcasts
`LOCK_REQUEST(seat=S, ts, peer_id)` to every other live peer. It then
waits until it has received `LOCK_REPLY(S)` from every one of them.
A receiving peer either replies immediately (if it does not want `S`
and is not holding `S`) or defers the reply (if it holds `S`, or
wants `S` with a smaller `(ts, peer_id)` than the requester). Once
the requester has all replies, it enters the critical section,
performs the write + replication, and on exit sends a `LOCK_REPLY` to
every deferred requester.

Lock granularity was deliberately set at the per-seat level. A
single global lock on the whole seat map would have serialized every
booking; per-seat allows two clients booking *different* seats to
proceed in parallel. This is the system's main scalability point.

**Critical sections and the resources they protect:** there is one
critical section per seat. The resource it protects is the seat's
record `{status, owner, write_id}` and the corresponding `applied_writes`
membership across all replicas — i.e. the invariant that at most one
client is recorded as the owner of any given seat.

### 3.5 Replication and Fault Tolerance

**Strategy:** active replication. Every replica applies every write;
there is no primary or backup. The peer that acquires the lock on a
seat is the one that drives the write — it applies the booking locally,
broadcasts `REPLICATE(write_id, seat_id, owner, ts)` to every other
live peer, waits for every `REPLICATE_ACK`, then releases the lock.
`write_id` is a globally unique identifier (`peer_id-uuid`) used at
replicas to deduplicate writes that arrive twice (e.g. via the
state-transfer buffer flush).

**Consistency model:** sequential consistency, effectively linearizable
per seat. Because the lock-holder waits for all replicas to ACK before
releasing the lock, when the client is told "booked" every live
replica has the booking. Reads (rendering the seat grid in the client)
are served by any peer without taking the lock; they may briefly trail
an in-flight write that has not yet ACKed, but never observe a write
that was never confirmed to a client.

**Peer joins:** a new peer registers with the name service, fetches the
existing peer list, asks one existing peer for a `STATE_REQUEST` (the
full seat map plus the `applied_writes` set and the Lamport clock
value), installs the snapshot, then broadcasts `STATE_READY` so the
other peers begin treating it as a full replica. Between the moment
the new peer is announced and the moment it sends `STATE_READY`, every
existing peer buffers writes destined for the joiner; when
`STATE_READY` arrives, the buffer is flushed and the joiner
deduplicates against the snapshot's `applied_writes`.

The state transfer is carefully ordered to avoid a race we discovered
during testing: REPLICATE messages can arrive at the joiner while it
is still waiting for the snapshot reply, and would otherwise be
overwritten when the snapshot is installed. To prevent this, the
joiner's REPLICATE handler buffers incoming messages until the
snapshot is installed, then drains the buffer through the normal
apply path (which deduplicates against `applied_writes`).

**Peer leaves (graceful):** the leaving peer sends `DEREGISTER` to the
name service before exiting. The name service immediately pushes
`PEER_DEAD` to the remaining peers, which remove the peer from their
local membership table and from any pending Ricart-Agrawala
`replies_needed` sets.

**Node failures (crash):** the name service declares a peer dead if no
heartbeat is received for `DEAD_AFTER_SECS` (6 seconds — three missed
heartbeats). It then pushes `PEER_DEAD` exactly as in the graceful
case. The critical part is that any peer that was waiting on a
LOCK_REPLY from the dead peer must not block forever — when the
`PEER_DEAD` notification arrives, every peer treats any pending
reply from the dead peer as already received and wakes the waiting
booking thread.

### 3.6 Peer / Component Lifecycle

**Peer registration:** on startup the peer begins listening on its
configured port, sends a `REGISTER` request to the name service, then
asks for the current peer list. If the list is empty the peer is the
bootstrap node and declares itself ready immediately with an empty
seat map. Otherwise it picks an existing peer at random, sends a
`STATE_REQUEST`, installs the returned snapshot, then sends
`STATE_READY` to every other peer. After this point the peer is a
full replica and accepts client requests.

**State synchronization on join:** described in detail in 3.5. The
joiner's snapshot is consistent because it is taken under the
provider's seats_lock, capturing `seats`, `applied_writes`, and
`clock` as one atomic value.

**Peer deregistration:** on graceful shutdown the peer sends
`DEREGISTER` to the name service, then closes its server. Remaining
peers receive `PEER_DEAD` and remove it from their membership.

## 4. System Component Specification (required Table 1)

| Component | Type | Protocol | Description |
|---|---|---|---|
| NameService | Service | JSON/TCP | Global registry of live peers; failure detector via heartbeats; pushes PEER_JOINED / PEER_DEAD notifications |
| ObjectRequestBroker (helpers in common.py) | Middleware | JSON/TCP | Length-prefixed JSON framing; `send_request` and `broadcast` primitives used by every peer-peer and client-peer call |
| LamportClock | Module | N/A | Per-peer scalar logical clock; `tick()` on local events, `update(ts)` on remote receives |
| DistributedLock | Mutex | Ricart-Agrawala | Per-seat request/reply mutex over Lamport timestamps; one state machine per critical section |
| ReplicaManager | Module | JSON/TCP | Applies local writes; broadcasts REPLICATE; collects REPLICATE_ACKs; serves state snapshots |
| PeerList | Module | N/A | Each peer's local view of cluster membership; refreshed from NS and from PEER_JOINED / PEER_DEAD pushes |
| Client (Tkinter) | UI process | JSON/TCP | Seat grid; polls peer for GET_SEATS; sends BOOK_SEAT to chosen peer |

## 5. Replication and Consistency Specification (required Table 2)

| Operation | Replica Manager | Lock Required | Propagation | Consistency Model |
|---|---|---|---|---|
| Read (GET_SEATS) | Any peer (client picks via NameService) | No | None | Sequential (may briefly trail an in-flight write that hasn't ACKed) |
| Write (BOOK_SEAT) | Lock-holder peer applies, then broadcasts to all live replicas | Yes (per-seat Ricart-Agrawala) | Synchronous broadcast + wait for all REPLICATE_ACKs | Sequential / linearizable per seat |
| Peer Join | Joining peer fetches snapshot from one existing peer | No (joiner uses bootstrap-buffer so writes during state transfer are not lost) | Full state transfer + replay of buffered writes | State synchronization |
| Peer Leave (graceful) | Remaining peers | No | NameService pushes PEER_DEAD; remaining peers drop the peer from their membership | Graceful degradation |
| Peer Failure (crash) | Remaining peers | No | NameService detects via missed heartbeats; pushes PEER_DEAD; pending RA replies from dead peer are treated as received | Graceful degradation |

## 6. Major Modules

The system is split across four Python files. Each is small enough
that the entire module fits in working memory while reading.

The **common module** (`common.py`) provides the wire-framing primitives
(length-prefixed JSON), the Lamport-clock class, the ORB-style
request/broadcast helpers, and a small threaded TCP server scaffold.
Every other module imports from it.

The **name service** (`name_service.py`) is a single process exposing a
JSON/TCP interface for peer registration, heartbeats, and
membership queries. A background sweeper declares peers dead if no
heartbeat is received within the configured window and pushes
PEER_DEAD notifications. It also pushes PEER_JOINED when a new peer
registers.

The **peer** (`peer.py`) is the centerpiece. Each peer holds a full
replica of the seat map, runs the Ricart-Agrawala state machine per
seat, drives synchronous active replication when it is the lock
holder, and applies REPLICATE messages from other peers. The peer
also handles state transfer (both as snapshot provider and as
joiner) and tolerates concurrent peer joins, peer deaths, and
network races (in particular, races between PEER_JOINED notifications
from the name service and STATE_READY broadcasts from the joiner).

The **client** (`client.py`) is a Tkinter desktop application. On
startup it asks the name service for the peer list, picks one peer,
polls it every 1.5 seconds for the current seat map, and renders a
clickable grid. Clicking a free seat sends BOOK_SEAT and updates the
UI based on the reply. If the chosen peer becomes unreachable the
client refreshes the peer list and reconnects to another peer.

## 7. User Manual

### 7.1 Setup

Requirements: Python 3.9 or newer, with Tkinter available (the
`python3-tk` package on Ubuntu / Debian; bundled with the standard
Python installer on macOS and Windows). No third-party packages are
required.

### 7.2 Starting the system

Open one terminal for the name service, three more for the peers, and
one per client. The order matters: the name service must be up before
any peer registers, and at least one peer must be up before clients
attempt to connect.

```
# terminal 1
python3 name_service.py --host 127.0.0.1 --port 5000

# terminals 2-4 (one peer each, distinct ports, same seat count)
python3 peer.py --id P1 --port 5101 --ns-host 127.0.0.1 --ns-port 5000 --seats 20
python3 peer.py --id P2 --port 5102 --ns-host 127.0.0.1 --ns-port 5000 --seats 20
python3 peer.py --id P3 --port 5103 --ns-host 127.0.0.1 --ns-port 5000 --seats 20

# terminal 5+: clients
python3 client.py --ns-host 127.0.0.1 --ns-port 5000 --id alice
python3 client.py --ns-host 127.0.0.1 --ns-port 5000 --id bob --peer P2
```

### 7.3 Using the client

When the client window opens, the status bar at the top shows which
peer the client connected to. The legend below the title shows three
seat colours: green for FREE, red for BOOKED by someone else, blue
for "YOURS" (booked by this client). Click any green seat to book it;
the seat turns blue if the booking succeeded and the status bar
shows the write identifier. If the seat was booked by someone else
in the meantime, the status bar reports "already booked" and the
seat colour updates to red on the next poll cycle (typically within
1.5 seconds).

The Refresh button forces an immediate poll of the seat map without
waiting for the next polling tick.

### 7.4 Screenshots

Insert screenshots showing: (a) two clients connected to different
peers with overlapping seat views, (b) a successful booking, (c) a
"seat already booked" outcome on the losing client of a race.
_<placeholders — capture during the demo recording session>_

## 8. Test Results

Test scenarios were executed both manually (via the GUI) and
automatically (via `run_tests.py`, which drives the system through
the real TCP protocol and asserts on the post-conditions of every
surviving replica). The automated harness covers the four rubric
categories.

### 8.1 Concurrent access, different seats

Three clients each book a different seat simultaneously, going
through three different peers. Expected outcome: all three bookings
succeed and every replica reports an identical seat map.

```
== test: concurrent bookings on DIFFERENT seats ==
  all three different-seat bookings succeeded
  all replicas agree
```

Observations: the Lamport timestamps printed in each peer's log
diverge as each peer processes the local write and the inbound
REPLICATE messages, but the totally-ordered `(ts, peer_id)` pair
remains consistent across replicas.

### 8.2 Concurrent access, same seat

Three clients race to book the same seat. Expected outcome: exactly
one succeeds, the other two receive "already booked", and all replicas
agree on the winner.

```
== test: concurrent bookings on SAME seat ==
  successes: 1, failures: 2
  exactly one winner; all replicas agree
```

Observations: the winner is determined by the lexicographic order of
the `(ts, peer_id)` pair on each peer's LOCK_REQUEST. The two losers
re-check the seat after acquiring their RA lock and abort because
the seat is already BOOKED locally (the REPLICATE from the winner
has already arrived).

### 8.3 Peer joins (late joiner)

After several bookings have been made on the running cluster, a
fourth peer is started. Expected outcome: the new peer fetches the
state and reports the same seat map as the existing peers; a booking
performed after the join propagates to all four peers.

```
== test: late peer joins and gets state ==
  late joiner has the same state as existing replicas
  post-join booking propagates correctly
```

### 8.4 Peer failure (crash)

A peer is SIGKILLed while the system is running. Expected outcome:
the name service declares it dead within 6 seconds, the surviving
peers continue to accept bookings, no peer remains blocked on a
LOCK_REPLY from the dead peer.

```
== test: peer CRASH mid-system; surviving peers continue ==
  killing peer 2 (port 6103)
  surviving peers continued to accept bookings and agree
```

### 8.5 Manual scenarios verified via the GUI

Beyond the automated tests, the following were verified by hand and
recorded for the demo video:

A client whose connected peer is killed reconnects automatically to
another peer and continues to show the live seat map. A peer started
with the name service down displays a register failure and exits
cleanly. Two clients pinned to different peers (via `--peer P1`
and `--peer P2`) booking the same seat simultaneously produce
exactly one winner. Stopping and restarting the name service does
not cause peers to crash; they re-register on the next heartbeat
tick.

## 9. Discussion (concepts the project demonstrates)

The implementation exercises several core ideas from the course in
a way that is meant to be defensible in the project discussion.
Lamport clocks are used to produce a total order over otherwise
unrelated concurrent events, and the `(timestamp, peer_id)` lexicographic
order resolves the tie-breaking case that would otherwise allow two
peers with the same timestamp to both think they hold the lock.
Ricart-Agrawala is implemented in its classical request/reply form
rather than the token variant, because the request/reply form maps
more cleanly onto our message-bus and because we want concurrency
per seat (token-passing would require maintaining one token per
seat, which has the same cost as per-seat request/reply but more
state).

Active replication with synchronous ACKs was chosen because it gives
the system the strongest consistency guarantee available without
introducing a primary. The cost is latency — a write must wait for
every live replica to ACK — but for human-facing booking latency the
cost is acceptable. The alternative, passive replication with a
primary, would require leader-election when the primary fails, which
is a substantially larger system.

The most subtle correctness issue we encountered during development
was a race between the joining peer's snapshot installation and
REPLICATE messages arriving from existing peers that had already
been told (via the name service push) that the joiner was a member
of the cluster. The fix was a bootstrap buffer at the joiner that
holds REPLICATE messages until the snapshot is installed, then
replays them through the normal apply path. This is documented in
`peer.py` next to the `bootstrap_buffer` field.

## 10. Limitations and Future Work

The name service is a single point of failure. In a production
system it would be replicated (e.g. via a small Raft cluster) but
for this assignment the cost outweighs the benefit.

Replication is best-effort on partial failure: if every other replica
goes dark while a write is in flight, the originating peer still
commits and tells the client "booked". A stronger guarantee would
require a configurable write quorum (e.g. "ACK from at least N/2+1
replicas"). Adding this would be a small change to `_replicate_write`.

Seat state is FREE/BOOKED only — there is no cancellation. Adding
cancellation would be a straight extension of the existing
RA-acquire + apply + replicate pipeline.

Clients use polling rather than push notifications for seat-map
updates. A small extension would have peers push state-change
notifications to subscribed clients; the existing JSON/TCP
infrastructure supports this trivially.

## 11. Appendix — File Inventory

```
ARCHITECTURE.md   Design notes (this report's seed)
README.md         Setup and run instructions
REPORT.md         This report
common.py         Lamport clock, JSON framing, ORB helpers
name_service.py   Standalone registry / failure detector
peer.py           Replica peer
client.py         Tkinter GUI client
run_tests.py      Headless end-to-end test harness
TEST_RESULTS.txt  Sample test-harness output
AI_DISCLOSURE.md  Generative-AI usage disclosure per course policy
```
