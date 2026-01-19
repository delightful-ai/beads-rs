# CRDT Audit (v0.1)

Date: 2026-01-20
Repo: beads-rs
Scope: core CRDT semantics, mutation planning, WAL/replication ordering, and
git sync/checkpoint merge behavior that affect convergence. This audit does not
cover CLI/UI rendering, auth, or performance-only concerns.

## Summary (high level)

- The core model is mostly LWW-based and deterministic.
- Two ops are order-dependent today: workflow and claim updates depend on
  existing state during apply.
- Note handling is strict in live apply (collision error, missing bead error),
  but merges can silently pick a winner in the presence of collisions.
- Deps are convergent but keep earliest created metadata, which limits
  redundancy/compaction.
- WAL/replication provide per-origin ordering + hash-chain checks, but apply
  errors after WAL commit can leave durable events unapplied.

## CRDT type matrix (current behavior)

| Component / Field | CRDT type and merge | Apply semantics | Order independence | Notes / refs |
|---|---|---|---|---|
| BeadCore (id, created) | Immutable, must match | Collision if created differs | Yes (deterministic) | Collision check: `src/core/apply.rs:104` |
| title, description | LWW register | update_lww on event stamp | Yes | LWW join: `src/core/crdt.rs:25` ; updates: `src/core/apply.rs:329` |
| design, acceptance_criteria | LWW register of Option | update_lww on event stamp | Yes | Updates: `src/core/apply.rs:339` |
| priority, bead_type | LWW register | update_lww on event stamp | Yes | Updates: `src/core/apply.rs:355` |
| labels | LWW register of entire set | update_lww on event stamp | Yes (full-set LWW, not add-wins) | Patch is full set: `src/daemon/mutation_engine.rs:1016` ; update: `src/core/apply.rs:361` |
| external_ref, source_repo | LWW register of Option | update_lww on event stamp | Yes | Updates: `src/core/apply.rs:364` |
| estimated_minutes | LWW register of Option | update_lww on event stamp | Yes | Updates: `src/core/apply.rs:380` |
| workflow | LWW register of Workflow | build_workflow reads existing state on Keep | **No** (order-dependent) | Apply: `src/core/apply.rs:389`, `src/core/apply.rs:421`; Close patch uses Keep by default: `src/daemon/mutation_engine.rs:1053` |
| claim | LWW register of Claim | build_claim reads existing state on Keep | **No** (order-dependent) | Apply: `src/core/apply.rs:399`, `src/core/apply.rs:442`; ExtendClaim uses Keep: `src/daemon/mutation_engine.rs:1412` |
| notes (per bead) | Grow-only set keyed by note_id | union by id; collisions error on apply | Conditional (depends on causal delivery) | Missing bead error: `src/core/apply.rs:483`; collision error: `src/core/apply.rs:499`; merge keeps left: `src/core/collections.rs:152` |
| tombstones (global) | LWW on deletion stamp | delete ignored if bead updated newer (resurrection) | Yes (deterministic) | Resurrection check: `src/core/apply.rs:185`; merge LWW: `src/core/tombstone.rs:83` |
| tombstones (lineage) | LWW on deletion stamp | unconditional delete for matching lineage | Yes (deterministic) | Lineage delete path: `src/core/apply.rs:169` |
| deps (edge life) | LWW on life + min(created) | insert_dep join (life LWW, created min) | Yes (deterministic) | DepEdge join: `src/core/dep.rs:209`; apply upsert: `src/core/apply.rs:202` |

## Known CRDT-safety risks

1. Workflow and Claim are order-dependent
   - `build_workflow` reads existing state when patch uses Keep. `src/core/apply.rs:421`
   - `build_claim` reads existing state when patch uses Keep. `src/core/apply.rs:442`
   - Close leaves closed_reason/on_branch as Keep unless set. `src/daemon/mutation_engine.rs:1053`
   - ExtendClaim uses assignee Keep with new expires. `src/daemon/mutation_engine.rs:1412`
   - Fix: make these ops self-contained (explicit Set/Clear for all affected fields).

2. Note append requires bead to already exist
   - `apply_note_append` errors if bead missing. `src/core/apply.rs:483`
   - Without causal delivery across origins, a replica can diverge on out-of-order events.
   - Fix: either enforce causal delivery for note ops or make note apply total (buffer/no-op or create stub).

3. Note collision handling is inconsistent
   - Live apply errors on collision. `src/core/apply.rs:499`
   - Merge (`NoteLog::join`) silently picks left. `src/core/collections.rs:152`
   - Fix: either treat collision as corruption everywhere (error/metric) or define deterministic resolution.

4. Lineage tombstones always win
   - Lineage delete path is unconditional. `src/core/apply.rs:169`
   - If lineage tombstones can be emitted from stale replicas, they can delete newer live beads.
   - If lineage tombstones are strictly internal collision records, this is OK but must be documented.

## Assumptions that must be explicit (today)

- Bead IDs and Note IDs are globally unique in practice (collision is out-of-spec).
  NoteId generator + uniqueness loop: `src/core/identity.rs:711`, `src/daemon/mutation_engine.rs:1992`
- Mutation planner computes patches from current local state; LWW wins on conflict.
  Label removal rewrites full set: `src/daemon/mutation_engine.rs:1016`
- Note append occurs only after the creator has observed the bead (causality assumed).
  Missing bead is an apply error: `src/core/apply.rs:483`

## Recommended CRDT-safe adjustments (minimal changes)

1. Make workflow ops self-contained
   - Close/Reopen should explicitly Set/Clear closed_reason and closed_on_branch.
   - Do not use Keep for those fields when status changes.

2. Make claim ops self-contained
   - Claim/Extend/Unclaim should emit explicit assignee and expires.
   - Avoid Keep in claim-related patches.

3. Make note apply total
   - Either enforce causal delivery for note ops or allow buffering/stubs.

4. Align note collision policy
   - If collisions are corruption, surface consistently (apply and merge).

## System pipeline audit (realtime + sync)

### Event framing & hash-chain invariants

- Event frames are verified against canonical CBOR body, store identity, event id,
  and sha256; prev_sha is checked for contiguous events and deferred otherwise.
  `src/core/event.rs:2064`
- WAL record headers must match event bodies (origin, seq, time, txn_id, client_request_id),
  and sha256 is computed over the payload bytes. `src/daemon/wal/record.rs:240`

### Local mutation pipeline (daemon)

- Local mutations append to WAL + index, commit, then apply to in-memory state.
  Apply failures occur after WAL commit (durable but unapplied event).
  `src/daemon/executor.rs:339`
- Local watermarks advance only after apply succeeds. `src/daemon/executor.rs:410`

### Replication ordering & contiguity

- Incoming frames are verified and fed into a per-origin gap buffer; only contiguous
  sequences are applied. `src/daemon/repl/session.rs:563`, `src/daemon/repl/gap_buffer.rs:220`
- Gaps are buffered; buffer overflow or timeout rejects the session with a lag error.
  `src/daemon/repl/gap_buffer.rs:363`
- Expected prev hash is enforced only when seq == durable+1; otherwise events may be
  deferred until the gap fills. `src/daemon/repl/session.rs:727`

### WAL/index idempotency + equivocation checks

- WAL index uses (namespace, origin, seq) as the event identity; duplicate seq with
  different sha is treated as equivocation. `src/daemon/wal/index.rs:494`
- client_request_id is idempotent per (namespace, origin); conflicting request_sha
  is rejected. `src/daemon/wal/index.rs:642`

### Remote ingest path

- Replication ingest writes WAL + index first, then applies to state; apply errors
  can leave durable but unapplied events (same risk as local path).
  `src/daemon/core.rs:1511`, `src/daemon/core.rs:1612`
- Watermarks advance only after apply succeeds. `src/daemon/core.rs:1644`

### Git sync / checkpoint merge semantics

- Git sync merges CanonicalState after collision resolution; CRDT merge is pairwise,
  no base needed. `src/git/sync.rs:451`
- Deleted deps are GC’d on merge; tombstones are GC’d by TTL if configured, which can
  reintroduce old data if replicas are offline past TTL. `src/git/sync.rs:511`
- Checkpoint import merges per-namespace state via CanonicalState::join and is tested
  for commutativity. `src/git/checkpoint/import.rs:299`, `src/git/checkpoint/import.rs:814`

## Optional (future) stronger semantics

- If you want add-wins/remove-wins behavior for labels or deps, consider OR-Set or
  dotted version vectors for causal context.
- If you want to preserve concurrent values (MVRegister), move from total-order
  LWW to causal partial order for those fields.

## Decisions (locked, 2026-01-20)

These decisions supersede earlier assumptions. They are intended to be
self-contained so we do not need to re-litigate design intent.

### D1: All ops must be self-contained

Rule: Applying an event must not require reading existing state to compute the
new value of any field it touches. If an op changes a field, the op must carry
the final value (explicit Set or explicit Clear), never implicit Keep.

Implications:
- Workflow updates must include explicit closed_reason/closed_on_branch values
  when status changes. No Keep semantics for these fields in Close/Reopen.
- Claim updates must include explicit assignee and expires values (or explicit
  Clear). No Keep semantics in Claim/ExtendClaim/Unclaim.
- Patch fields that are not part of the op remain omitted, but any field the op
  mutates must be fully specified.

Goal: order-independent, commutative apply for all ops.

### D2: Labels and deps use OR-Set with dotted version vectors

We are dropping earliest provenance for deps and moving labels/deps to a true
CRDT set model.

Data model (conceptual):
- Each add produces a unique dot: (replica_id, orset_counter).
- Each remove carries a dotted version vector (DVV) that represents the
  observed dots at the time of removal.
- State stores active dots; membership is "value is present if any active dot".

Semantics:
- Add: insert dot for value. Idempotent if dot already present.
- Remove: delete all dots dominated by the DVV context.
- Merge: union dots then remove dots dominated by merged DVV context.

Notes:
- This is observed-remove (add-wins for concurrent add vs remove).
- If we ever need remove-wins, we will layer a remove-tombstone per value
  (not part of this decision).

Storage / metadata:
- We add a per-replica monotonic orset_counter (persisted) for dot allocation.
- Dots are per-value, not per-event. Multiple label/deps ops in one event get
  distinct dots.

### D3: Note apply must be total (Option 2: orphan note set)

We will not error if a NoteAppend arrives before the bead exists.

Rule:
- NoteAppend inserts into an orphan note map keyed by (bead_id, note_id) when the
  bead is missing.
- When a bead is present, queries attach notes from the global note map for that
  bead_id (no transfer is required).

Collision handling:
- Duplicate note_id with identical payload is a no-op.
- If payload differs, apply the deterministic collision rule (D4).

Rationale:
- This makes apply total and removes causal ordering assumptions for notes.

### D4: Deterministic collision handling (global policy)

Collisions are resolved deterministically, not by error/abort.
We still log/metric any collision.

Rules:
- Note collision (same bead_id + note_id, different payload):
  choose the winner by comparing:
  1) note.at (WriteStamp)
  2) note.author (ActorId)
  3) sha256(note.content) as a final tie-breaker
- Bead creation collision (same bead_id, different creation stamp):
  choose winner by greater creation stamp (Stamp ordering).
  Insert a lineage tombstone for the losing creation stamp.
- Dep/label dot collision (same dot, different value):
  choose by lexical compare of value, tie-breaker by sha256(serialized op).

Goal: deterministic convergence even under malformed inputs.

### D5: Drop earliest provenance for deps

We no longer preserve "earliest created" for dependency edges.
Deps are modeled purely as OR-Set membership (see D2).

Rationale:
- Simpler merge semantics.
- Simplifies compaction and reduces state size.
