# Generative AI Usage Disclosure

**Course:** CSE463 Distributed Systems — Spring 2026
**Project:** Distributed Ticket Booking System
**Submission date:** May 20, 2026

This disclosure is filed in accordance with the *Use of Generative AI
Tools* policy on page 4 of the Project Guidelines, which requires
disclosure of "All prompts provided to the AI tool" and "The
corresponding outputs generated."

The AI tool used was Claude (an Anthropic large language model)
accessed through the Cowork desktop interface.

## Summary of how the tool was used

The tool was used across the full development cycle: brainstorming the
application idea, sketching architecture and modules, writing the
implementation, debugging concurrency races, and producing the
report. The team made the design and submission decisions and is
responsible for the content.

## Prompt and output log

### Session 1 — Project idea and grading-fit check

**Prompt summary:** Asked the tool to recap the project specification
in plain language, asked it to assess a distributed-ticket-booking
idea against the rubric, and asked which rubric components the idea
would naturally exercise (architecture, mutex, replication, name
service).

**Output summary:** Confirmed the spec breakdown (3+ replicas, name
service, logical clocks, mutex, replication, client UI, demo video,
report); identified seats as the natural unit of mutual exclusion and
the natural double-booking concurrency hazard; flagged that every
required mechanism falls out of the domain without feeling
bolted-on.

### Session 2 — Technology choices

**Prompt summary:** Asked for recommendations on language, transport,
GUI framework, and starting order given the two-day deadline.

**Output summary:** Recommended Python + raw TCP sockets + Tkinter +
architecture-first. Rationale given: Python keeps the implementation
short; raw TCP makes message ordering, ACKs, and Lamport timestamps
explicit in the code (which the discussion section rewards);
Tkinter has zero install and demos cleanly.

### Session 3 — Architecture sketch

**Prompt summary:** Asked the tool to produce a module list, message
protocol, Ricart-Agrawala state machine, replication flow, consistency
model, peer-lifecycle description, and the two specification tables
the report requires.

**Output summary:** Produced the document saved at `ARCHITECTURE.md`,
including a component diagram, message-flow diagram, table of message
types, full state machine for Ricart-Agrawala (RELEASED / WANTED /
HELD with deferral rules and lexicographic `(ts, peer_id)` tie-break),
join protocol with bootstrap buffer, and the Table 1 / Table 2
deliverables.

### Session 4 — Implementation

**Prompt summary:** Asked the tool to produce, in order:
`common.py` (Lamport clock, JSON framing, ORB helpers, threaded TCP
server scaffold); `name_service.py` (registry, heartbeat, push
notifications); `peer.py` (Ricart-Agrawala per seat, active
replication, state transfer, race-tolerant peer-lifecycle handling);
`client.py` (Tkinter GUI with seat grid, polling, error handling,
peer-failure reconnect).

**Output summary:** Produced the corresponding files. Two races were
fixed during testing: (1) PEER_JOINED notifications from the name
service can overwrite a peer entry that was already marked READY via
a STATE_READY message; the fix preserves the existing state if the
peer is already known. (2) REPLICATE messages can race with a
joiner's snapshot install; the fix buffers incoming REPLICATEs until
the snapshot is installed, then drains the buffer through the
normal apply path which deduplicates against `applied_writes`.

### Session 5 — End-to-end testing

**Prompt summary:** Asked for a headless test harness covering the
four rubric scenarios (concurrent same-seat, concurrent
different-seat, late peer join, peer crash).

**Output summary:** Produced `run_tests.py`. Used the harness to
catch the two races above through repeated runs; the harness now
passes consistently.

### Session 6 — Report and documentation

**Prompt summary:** Asked for `README.md` (setup, deployment, demo
script) and `REPORT.md` (cover, features, assumptions, architecture
diagrams, module descriptions, user manual, test results, discussion).

**Output summary:** Produced both. The report content reflects the
implementation actually present in this submission; figures are ASCII
in the markdown source and will render as preformatted blocks in the
Word version.

## Understanding statement

The submitters have reviewed the produced code and report and are
prepared to explain, in the project discussion (20% of the grade),
the rationale for the Ricart-Agrawala state machine, the active
replication flow, the state-transfer protocol, and the consistency
model claim.
