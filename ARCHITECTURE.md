# Distributed Ticket Booking — Architecture

## 1. System Overview

A distributed event-ticket booking system. A single event with a fixed grid of
seats. Three (or more) replica peers each hold a full copy of the seat map.
Clients launch a Tkinter GUI, are routed to a peer through a name service, see
the live seat grid, and click to book. Concurrency-safety, replication, and
fault-tolerance are handled inside the peer mesh.

```
                +---------------------+
                |    NameService      |   single TCP/IP registry
                |   (host:5000)       |   peers register here on startup;
                +---------+-----------+   clients ask it for the peer list
                          ^
            register/heartbeat / list_peers
                          |
   +----------------------+----------------------+
   |                      |                      |
+-----+               +-----+                +-----+
|Peer1|<------------->|Peer2|<-------------->|Peer3|   peer-to-peer mesh
+-----+               +-----+                +-----+   JSON over TCP
   ^                     ^                     ^
   |                     |                     |
client                 client                client       client-server
(Tkinter)             (Tkinter)             (Tkinter)     JSON over TCP
```

Communication paradigm: **Hybrid.** Client↔peer is client-server. Peer↔peer is
peer-to-peer (full mesh). Peer↔name-service is client-server.

Messaging protocol: **Length-prefixed JSON over TCP.** Every message on the wire
is a 4-byte big-endian length header followed by a UTF-8 JSON payload. This
gives clean message boundaries on a streaming transport.

## 2. Modules (high level)

| Module | Process | Responsibility |
|---|---|---|
| `name_service.py` | NameService | Registry of live peers; heartbeat-based failure detection; serves peer list to clients and peers |
| `common.py` | shared library | Lamport clock; JSON framing helpers; message type constants; the small "ORB" — `send_request(host, port, msg)` for one-shot RPC |
| `peer.py` | Peer ×3+ | TCP server; runs Ricart-Agrawala mutex per seat; active replication; heartbeats to name service; state-transfer on join |
| `client.py` | Client ×N | Tkinter GUI; discovers peers via name service; renders seat grid; sends booking requests |

The "Object Request Broker" the spec asks about is the small set of helpers in
`common.py` that hide the socket / framing / JSON-encode-decode dance behind a
`send_request(host, port, msg) -> reply` and `broadcast(peers, msg)` interface.
Every other module talks to remote nodes through those calls.

## 3. Synchronization & Concurrency Control

### 3.1 Logical clock — Lamport
Each peer holds an integer counter `clock`.
- On any local event that needs a timestamp (e.g. starting a booking): `clock += 1`, use `clock`.
- On receiving a message with timestamp `t`: `clock = max(clock, t) + 1`.

This gives a total order on inter-peer events (broken by `peer_id` for ties),
which Ricart-Agrawala uses to decide who wins contested requests.

### 3.2 Critical sections — what we're protecting
The shared resource is the seat map, but the **per-seat slot** is the unit of
mutual exclusion. Two clients booking *different* seats can proceed in
parallel; two clients booking the *same* seat must serialize. This is the key
scalability point for the report.

### 3.3 Distributed mutex — Ricart-Agrawala (classical, request/reply)
Per-seat state machine on each peer:

```
state[seat] in {RELEASED, WANTED, HELD}
deferred[seat] = []   # peer_ids whose reply we owe when we exit CS
```

To book seat `S` (peer `P` wants the lock):
1. `clock += 1`; record `(my_ts, my_id) = (clock, P)`; `state[S] = WANTED`.
2. Send `LOCK_REQUEST(seat=S, ts=my_ts, peer_id=P)` to every other peer.
3. Wait until a `LOCK_REPLY(S)` is received from every other live peer.
4. `state[S] = HELD`. Enter critical section.
5. Apply the booking locally; broadcast `REPLICATE`; wait for all `REPLICATE_ACK`s.
6. Exit CS: `state[S] = RELEASED`, send `LOCK_REPLY(S)` to every peer in `deferred[S]`, clear the list.

On receiving `LOCK_REQUEST(S, their_ts, their_id)` at peer `Q`:
- Update Lamport clock with `their_ts`.
- If `state[S] == HELD`: defer (append `their_id` to `deferred[S]`).
- Else if `state[S] == WANTED` and `(my_ts, my_id) < (their_ts, their_id)`: defer.
- Else: reply immediately with `LOCK_REPLY(S)`.

The `(ts, peer_id)` lexicographic compare is what breaks ties — without it two
peers issuing simultaneous requests with the same timestamp can deadlock.

### 3.4 Failure handling inside the mutex
A peer that crashes while we are waiting for its `LOCK_REPLY` would freeze the
algorithm forever. To avoid this, peers subscribe to the name service's
failure-detection notifications: when peer `Q` is declared dead, every other
peer removes `Q` from its "outstanding replies" set for every seat. Equivalent
to treating any pending reply from `Q` as already received.

## 4. Replication & Consistency

### 4.1 Strategy: active replication
Every write is applied at every replica. There is no primary. The peer that
acquires the lock on a seat is the one that drives the write — it commits
locally then broadcasts `REPLICATE` to the rest, then waits for all
`REPLICATE_ACK`s before releasing the lock.

### 4.2 Consistency model: sequential consistency (effectively linearizable per seat)
Because the lock-holder broadcasts the write and waits for ACKs from all live
replicas before releasing the lock, when the client sees "booked" all replicas
have the update. Subsequent reads from any peer see the booking. Per seat,
this is linearizable; across seats, sequential consistency holds because all
writes to a given seat are totally ordered by RA.

Reads (loading the seat map for the GUI) are served by any peer with no lock —
they can in principle observe a state slightly behind a concurrent write that
hasn't yet completed its ACK round, which is acceptable: the booking is not
yet confirmed when that read happens.

### 4.3 Write path (end-to-end)
```
Client                Peer P (its peer)        Peers Q, R (other replicas)
  | --- BOOK(S) ----> |
  |                   | RA acquire(S): LOCK_REQUEST(S,ts,P) ---> Q, R
  |                   | <--- LOCK_REPLY(S) ---  Q, R (eventually)
  |                   | apply locally: seats[S].owner = client_id
  |                   | REPLICATE(S, client_id, ts) ----------> Q, R
  |                   | <--- REPLICATE_ACK(S) -- Q, R
  |                   | RA release(S): send deferred LOCK_REPLYs
  | <-- BOOK_OK ----- |
```

## 5. Peer Lifecycle

### 5.1 Registration / deregistration
- **Register** — peer starts → connects to NameService → sends `REGISTER(peer_id, host, port)` → starts a 2-second heartbeat loop.
- **Deregister** — graceful shutdown → sends `DEREGISTER(peer_id)` → NameService drops it and broadcasts a `PEER_LEFT` to remaining peers (or peers poll on next list).
- **Crash** — heartbeat stops → NameService marks peer dead after 6 seconds (3× heartbeat) → notifies remaining peers.

### 5.2 State synchronization on join
When a new peer comes up:
1. Register with NameService.
2. Fetch peer list; pick one existing peer `H`.
3. Send `STATE_REQUEST` to `H`.
4. `H` returns a snapshot: `{seat_map, lamport_clock}`. It also flips a flag to start *buffering* outgoing REPLICATE messages destined for the new peer for the duration of the handoff.
5. New peer installs the snapshot and sets `clock = max(0, snapshot_clock)`.
6. New peer sends `STATE_READY` to all peers. From this point everyone treats
   it as a normal replica.
7. `H` flushes the buffered REPLICATEs to the new peer.

This guarantees the new peer sees every write whose lock was acquired after
its snapshot, with no duplicates (REPLICATEs carry a write-id used for
idempotency).

## 6. Table 1 — System Component Specification

| Component | Type | Protocol | Description |
|---|---|---|---|
| NameService | Service | JSON/TCP | Global registry of live peers; failure detector via heartbeats; serves `LIST_PEERS` to clients |
| ObjectRequestBroker (`common.py` helpers) | Middleware | JSON/TCP | Length-prefixed JSON framing; `send_request` / `broadcast` primitives used by every peer-peer and client-peer call |
| LamportClock | Module | N/A | Per-peer integer counter; `tick()`, `update(ts)`; produces totally-ordered logical timestamps |
| DistributedLock | Mutex | Ricart-Agrawala | Per-seat request/reply mutex over Lamport timestamps; one state machine per critical section |
| ReplicaManager | Module | JSON/TCP | Applies local writes; broadcasts `REPLICATE`; collects `REPLICATE_ACK`s; serves snapshots on join |
| PeerList | Module | N/A | Each peer's local view of live peers; refreshed from NameService and from failure notifications |
| Client (Tkinter) | UI process | JSON/TCP | Seat grid; polls peer for `GET_SEATS`; sends `BOOK(seat_id)` to chosen peer |

## 7. Table 2 — Replication and Consistency Specification

| Operation | Replica Manager | Lock Required | Propagation | Consistency Model |
|---|---|---|---|---|
| Read (GET_SEATS) | Any peer (client picks one via NameService) | No | None | Sequential (may briefly trail an in-flight write that hasn't ACKed) |
| Write (BOOK_SEAT) | Lock-holder peer applies, then broadcasts to all live replicas | Yes (per-seat RA) | Synchronous broadcast + wait for all REPLICATE_ACKs | Sequential / linearizable per seat |
| Peer Join | Joining peer fetches snapshot from one existing peer | No (but joining peer flips into "buffering" mode for write-during-join) | Full state transfer + replay of buffered writes | State synchronization |
| Peer Leave (graceful) | Remaining peers | No | NameService notifies remaining peers; they drop the peer from their replica set | Graceful degradation |
| Peer Failure (crash) | Remaining peers | No | NameService detects via missed heartbeats; broadcasts `PEER_DEAD`; pending RA replies from dead peer are treated as received | Graceful degradation |

## 8. Message Catalog

All messages are JSON objects with a `type` field. Headers shown; payloads
listed under each.

Client ↔ Peer:
- `GET_SEATS` → `SEATS_REPLY { seats: [{id, status, owner?}] }`
- `BOOK_SEAT { seat_id, client_id }` → `BOOK_REPLY { ok: bool, reason?, seat_id }`

Peer ↔ Peer:
- `LOCK_REQUEST { seat_id, ts, peer_id }`
- `LOCK_REPLY { seat_id, ts, peer_id }`
- `REPLICATE { write_id, seat_id, owner, ts, peer_id }`
- `REPLICATE_ACK { write_id, peer_id }`
- `STATE_REQUEST { peer_id }` → `STATE_REPLY { seats, clock }`
- `STATE_READY { peer_id }`

Peer ↔ NameService:
- `REGISTER { peer_id, host, port }` → `REGISTER_ACK`
- `HEARTBEAT { peer_id }` → `HEARTBEAT_ACK`
- `DEREGISTER { peer_id }` → `DEREGISTER_ACK`
- `LIST_PEERS` → `PEERS_REPLY { peers: [{peer_id, host, port}] }`

NameService → Peer (push notifications):
- `PEER_JOINED { peer_id, host, port }`
- `PEER_DEAD { peer_id }`

## 9. Test Scenarios (rubric: "concurrent access, peer joins/leaves, failure scenarios")
1. **Concurrent booking, different seats** — 2 clients on 2 peers each book a different seat at the same instant. Both succeed; both bookings visible on the third peer within ms.
2. **Concurrent booking, same seat** — 2 clients on 2 peers race for the same seat. Exactly one wins. The other receives `BOOK_REPLY { ok: false, reason: "already booked" }`.
3. **Late-joining peer** — start with 3 peers, book several seats, start a 4th peer. The 4th peer's seat grid matches the others after `STATE_READY`.
4. **Graceful peer leave** — peer 3 sends DEREGISTER while peer 1 holds the lock on a seat. RA must still complete (peer 3 dropped from outstanding replies).
5. **Peer crash mid-booking** — kill peer 2 while a booking is in flight on peer 1. NameService detects after ≤6s; peer 1's RA cleans up and the booking completes.
6. **Name service restart** — kill and restart NameService; existing peers re-register on next heartbeat tick.

## 10. Assumptions
- Network is reliable enough that TCP connections succeed within retries; we don't try to handle Byzantine failures.
- Peers are trusted (no malicious peers).
- Event has a fixed seat count known at startup (config).
- All peers and NameService run on the same machine for the demo, but `host:port` is parameterized so they can be on different hosts.
- Clients are short-lived sessions — a client picks one peer at startup; if that peer dies, the client must reconnect (re-querying the NameService).
