Absolutely. Reading your spec, you don’t have “one system” so much as a stack of interlocking little machines (identity machine, event-log machine, replication machine, durability machine, checkpoint machine, GC machine…). If you try to model all of it at once, Stateright turns into a fog machine. If you model the right *seams* in the right order, you get counterexamples that map directly to real bugs.

Below is the “model portfolio” I’d want for Beads v0.4, in order of importance. For each, I’ll say what it buys you, what you can safely abstract away, and the properties that make it genuinely informative about the implementation.

---

## 0) The meta-rule: you want multiple small models, not one big model

Think of each model as a lens with a specific focal length. Your spec already divides the world into lanes (realtime vs Git), namespaces, and “durable vs applied”. Those are natural boundaries for independent models.

A good sign you picked the right boundary: a counterexample trace can be translated into an integration test without interpretive dance.

---

## 1) Model: Realtime replication core (events, gaps, WANT, ACK)

**Why it’s #1:** this is the correctness backbone. If `seen_map`, gap buffering, equivocation detection, and ACK monotonicity are wrong, everything else becomes meaningless. This model catches the deep bugs: double-apply, skipping gaps, “ACK lies”, out-of-order corner cases, WANT serving mistakes.

### Scope (what to include)

* Per-namespace streams.
* Event identity: `(origin_replica_id, namespace, origin_seq)`.
* Receiver `seen_map` per `(namespace, origin_replica_id)` tracking highest contiguous applied.
* Gap buffer with strict bounds and timeout behavior.
* `EVENTS`, `ACK`, `WANT` messages and the core rules:

  * don’t advance past gaps
  * buffer out-of-order
  * WANT requests missing ranges
  * idempotent apply
  * equivocation check: same `event_id` different `sha` is fatal

### What to abstract away

* The actual bead/deps/notes content. Treat an event as “payload token” plus `sha`.
* WAL segments, SQLite, Git, snapshots.
* Durability classes at first. You can treat “durable = applied” initially just to validate the *shape* of progress and gap rules, then split them in model #2.

### Properties to check (the ones that map to real bugs)

Safety invariants:

1. **No-gap ACK:** For all replicas, for all `(ns, origin)`, `ack_durable[ns][origin] <= seen_contiguous[ns][origin]` and never exceeds the first missing sequence.
2. **Idempotence:** Applying an already-applied `event_id` never changes canonical state (in the model, state can be a set of applied event_ids or a hash accumulator).
3. **Equivocation detection:** If an `event_id` arrives with different `sha`, the session must error and no state advances.
4. **Monotonicity:** `seen_map`, `ack_durable`, `ack_applied` (if present) are monotonic.
5. **Origin ordering constraint:** Sender must not send decreasing `origin_seq` for a given `(ns, origin)` on a connection (or else receiver must handle safely).

Reachability (“sometimes”):

* Sometimes the system reaches a state where all replicas have applied up to N in each namespace (proves you didn’t accidentally model a dead protocol).

### Implementation tie-in

This model becomes “real” when:

* Your model’s event ingestion rules are literally the same functions as your implementation’s ingest rules:

  * `should_buffer_or_apply(ns, origin, seq, head_sha, seen_map, gap_buffer)`
  * `on_events(...) -> {buffered, want, applied, ack_update}`
* Even if the daemon uses threads/channels, the *state machine* for per-(ns, origin) contiguity and equivocation should be shared or kept mechanically identical.

---

## 2) Model: Durability semantics and “ACK means durable”

**Why it’s #2:** your spec makes durability an explicit contract (`LOCAL_FSYNC`, `REPLICATED_FSYNC(k)`, optional Git counting). The failure modes are subtle: acknowledging too early, counting the wrong replicas, or not handling retries correctly.

### Scope

* Split watermarks into **applied** vs **durable**.
* Model the rule: a replica only advances `durable` after it has “persisted” the event (in the model, persistence can be a boolean flag per event).
* Model `REPLICATED_FSYNC(k)` waiting logic (coordinator behavior).
* Include the “eligible durable replica” constraint (ephemeral/client-mode replicas never count).

### What to abstract away

* Actual fsync and WAL mechanics (that’s model #8/#9).
* Git checkpointing (that’s model #4).

### Properties

Safety invariants:

1. **No false durability:** If a replica’s `ACK.durable[ns][origin] >= s`, then that replica must have persisted all events `<= s` for that origin+namespace in the model.
2. **Coordinator correctness for k:** A mutation with durability `REPLICATED_FSYNC(k)` is acknowledged to the client only if at least `k` eligible replicas have `durable >= that_event_seq` for the coordinator’s origin.
3. **No counting ineligible replicas:** client-mode/ephemeral replicas never satisfy a quorum slot.
4. **Timeout behavior is honest:** if the client gets a timeout error with a receipt, the event(s) are locally durable, and retries don’t mint new identities or stamps (this overlaps with model #3, but the timing boundary belongs here).

Reachability:

* It’s possible to complete `REPLICATED_FSYNC(k)` under some network schedule (proves your model isn’t requiring the impossible).

### Implementation tie-in

This model should line up with your real “durability coordinator” module:

* Inputs: local append complete, remote ACK updates, eligibility roster/policy, timeout.
* Output: client success/timeout + receipt.

If you keep that logic pure (no IO inside), you can reuse it in a Stateright model with very little glue.

---

## 3) Model: Idempotency and receipts across retries (client_request_id + txn_id)

**Why it’s #3:** this is where real systems bleed in production: clients retry, networks flap, processes crash between “fsync” and “reply”, etc. Your spec is clear: “retry with same `client_request_id` returns original receipt and MUST NOT mint new write stamps.”

### Scope

* `client_request_id` (optional) and its mapping to:

  * `txn_id`
  * set/list of `event_id`s
  * “receipt”
* The “PENDING vs COMMITTED” shape you describe in the plan.
* Crash/restart nondeterminism: you can model “process crashes” at specific cut points.

### What to abstract away

* Real SQLite schema, WAL offsets, file truncation.
* Real stamp generation details. You can model “stamps” as increasing integers and assert they don’t change on retry.

### Properties

Safety invariants:

1. **At-most-once per client_request_id:** For a fixed `(ns, client_request_id)`, the system produces at most one logical mutation (same `txn_id`, same event_ids).
2. **Retry returns original identity:** A retry with same `client_request_id` returns the original receipt, never a fresh `txn_id`, never new `event_id`s.
3. **No stamp remint on retry:** Any stamp minted by the first attempt is identical on all successful retries.
4. **Mismatch detection:** If the same `client_request_id` is reused with a different request digest, the system rejects (your plan calls this out, and it’s a huge footgun preventer).

Crash-window reachability checks:

* If crash occurs after local durability but before reply, retry eventually returns the same receipt.
* If crash occurs before durability, retry may create the mutation, but still only once.

### Implementation tie-in

This is the model that best maps to a deterministic, replayable integration test:

* Reify “cut points” in the implementation (or test harness) and ensure the model’s cut points match:

  * after WAL append before fsync
  * after fsync before index/receipt commit
  * after receipt commit before reply

---

## 4) Model: Checkpoint lane correctness (export/import, included watermarks, multi-writer)

**Why it’s #4:** Git checkpoints are your “distribution and bootstrap lane”. The tricky parts are not “Git works”, but:

* `included` watermarks must be truthful,
* imports must advance durability correctly,
* multi-writer retry logic must converge,
* chain continuity across pruning needs `included_heads`.

### Scope

* Represent checkpoints as abstract snapshots containing:

  * `state_digest` (not full state)
  * `included` watermark vector
  * `included_heads` per `(ns, origin)` if you rely on hash-chain continuity after pruning
* Model multiple writers pushing to the same ref and non-FF failures.
* Model import + merge and “advance durable to at least meta.included”.

### What to abstract away

* Sharded JSONL, manifest hashing, tar/zstd. Those are deterministic engineering tasks better tested with property tests and golden fixtures (see model #9).
* The Git transport itself. Model it as “a register holding the latest checkpoint per group” with non-FF conflicts.

### Properties

Safety invariants:

1. **Truthful inclusion:** A checkpoint claiming `included[ns][origin] = s` must only be possible if the writer had locally durable events up to s at the moment of snapshot.
2. **Import advances durability safely:** After importing a checkpoint, the replica’s durable watermark is at least meta.included for those namespaces.
3. **Multi-writer convergence:** Under arbitrary interleavings, repeated “fetch, merge, re-export, retry” eventually results in a checkpoint that includes the max of what any writer had included (modulo ongoing writes).
4. **No epoch mixing:** A checkpoint with different `store_epoch` is never merged (ties into model #6).

### Implementation tie-in

This model should mirror your “checkpoint scheduler + worker” logic at the boundary:

* The coordinator captures an immutable snapshot barrier (state digest + included watermarks).
* The worker writes and pushes.
* On failure: fetch latest, import+merge, re-export.

If you model that control loop, you will catch the “claimed included advanced too far” bug early.

---

## 5) Model: GC authority, GC markers, and floors

**Why it’s #5:** GC is one of those features that seems “backgroundy” until it destroys correctness. Your spec is careful: only the authority decides, and everyone else enforces `gc_floor_ms`. Also: old events become no-ops but still advance watermarks.

### Scope

* `namespace_gc_marker` events.
* `gc_floor_ms[namespace] = max(...)`.
* The rule: events with `event_time_ms <= gc_floor_ms[namespace]` are ignored as state mutations, but still count for contiguity/ACK.

### What to abstract away

* Real TTL evaluation and “now”. In the model, GC authority can nondeterministically emit a marker with some cutoff (or you can model cutoff based on an event-time counter).

### Properties

Safety invariants:

1. **Floor monotonicity:** `gc_floor_ms[ns]` never decreases.
2. **Old events don’t resurrect:** After floor advances, applying any event at or below the floor never changes state.
3. **But progress still happens:** Even if ignored, those events still advance `seen_map` contiguously and are ACKed normally.
4. **Determinism:** If all replicas observe the same marker sequence and the same event stream, they converge to the same post-GC state.

### Implementation tie-in

This model should map directly to:

* `apply_gc_marker(ns, cutoff_ms, enforce_floor)`
* `should_ignore_event_due_to_floor(event_time_ms, gc_floor_ms)`

The subtle bug this model catches: treating ignored events as “not applied”, which would stall ACKs forever on old origins.

---

## 6) Model: Store identity + epoch mismatch + reseed behavior

**Why it’s #6:** it’s a “guardrail model”. The goal is to prove you never accidentally merge two incompatible histories (store reset, wrong repo, wrong remote). This prevents catastrophic, silent corruption.

### Scope

* `store_id`, `store_epoch`.
* Handshake rule: mismatch yields ERROR and no state merge.
* Bootstrap rule: epoch mismatch requires explicit reseed/reset.

### What to abstract away

* Git discovery mechanics, URL normalization. Model just the outcomes: sometimes peers/checkpoints have mismatched ids/epochs.

### Properties

Safety invariants:

1. **No cross-epoch merge:** state is never merged when epochs differ.
2. **No cross-store merge:** same for store_id.
3. **Protocol refusal is consistent:** all replication/checkpoint paths enforce the same rule, not “some do, some forget”.

### Implementation tie-in

This should correspond to one shared validation function used by:

* replication HELLO/WELCOME processing
* checkpoint import
* WAL segment header validation

If those three sites diverge, you get “works in one lane, corrupts in another”.

---

## 7) Model: Resource bounds + fairness across namespaces

**Why it’s #7:** correctness includes “doesn’t explode” and “core doesn’t starve behind wf.” You have explicit bounded buffers and fairness requirements.

### Scope

* Bounded gap buffer per connection.
* In-flight event window.
* Round-robin scheduling across namespaces/origins.

### What to abstract away

* Actual byte sizes. You can model “cost” as small integers.

### Properties

Safety invariants:

1. **No unbounded queues:** buffer sizes never exceed configured bounds.
2. **No starvation:** if `core` has pending events and the connection remains live, `core` eventually makes progress even if `wf` is high churn (this is a liveness-ish property; you can also check it as a “sometimes core advances while wf churns” reachability if you want to avoid full liveness).

### Implementation tie-in

This is best tied to your batch construction logic:

* “Pick next `(ns, origin)` round-robin, cap per-origin batch size.”

This is the model that prevents the “wf turned on and now core never catches up” incident.

---

## 8) Model: Crash/recovery cut points for WAL + index (high-value, but not purely Stateright)

**Why it’s #8:** it’s critical, but it’s often better validated with crash tests + fault injection than with a full actor model. Still, a small abstract model is useful to prove “no duplicate origin_seq” and “durability doesn’t lie”.

### Scope

* The ordering contract you describe (append record, fsync, index commit, apply, receipt finalize).
* Crash at any cut point.
* Recovery behavior: replay WAL, rebuild/catch-up index, reconcile pending receipts.

### What to abstract away

* CRC, segment rotation details, SQLite pragmas. Those are implementation tests.

### Properties

Safety invariants:

1. **No origin_seq reuse:** after any crash and recovery, next local `origin_seq` is strictly greater than any durable event.
2. **Durability receipt honesty:** if a receipt claims LocalFsync, the event must survive crash.
3. **Index rebuild correctness (abstract):** recovery can discover durable-but-unindexed events and won’t “forget them”.

### Implementation tie-in

This becomes extremely informative if you align the model’s cut points with real failpoints in code (even just in tests).

---

## 9) Model: Deterministic serialization and hashing (canonical CBOR, checkpoint determinism)

**Why it’s #9:** it’s foundational but mechanical. This is where you want *property tests*, golden vectors, and cross-version compatibility tests more than a distributed model.

### What to test/model

* Canonical CBOR encoding for `EventEnvelope` preimage and full envelope.
* `sha256` computation stability.
* Checkpoint `manifest_hash` and `content_hash` recomputation and reject-on-mismatch behavior.

### Best tool

* Not Stateright first. Use:

  * property tests: encode → decode → encode is stable
  * golden test vectors committed to repo
  * fuzz tests for framing, CBOR depth/size limits
  * cross-version tests: vN writer, vN-1 reader

Still, a *tiny* Stateright model can treat hashes as uninterpreted unique IDs to validate equivocation logic. But the encoding itself should be tested directly.

---

# A concrete “start tomorrow” modeling sequence

If you want maximum usefulness quickly, do this sequence:

1. **Realtime core model (Model #1)** with 3 replicas, 2 namespaces (`core`, `wf`), unordered delivery, duplication allowed, bounded buffers.
2. Extend it to **durable vs applied watermarks (Model #2)** and add a coordinator that waits for `REPLICATED_FSYNC(1)` first, then `k=2`.
3. Add **idempotency receipts (Model #3)** with crash cut points.
4. Only then pull in **checkpoint lane (Model #4)**, because now you’ll have the invariants to tell you when checkpoints are lying.

That gives you counterexamples that are “about your real system” instead of “about your model’s imagination”.

---

# If you want, I can sketch the minimal Stateright state machines

Without asking you a bunch of questions, I can propose a very specific first Stateright model structure:

* Actors: `Replica`, `Network` (optional), maybe a `Client`.
* Replica state: `seen_applied`, `seen_durable`, `gap_buffer`, `head_sha`, `eligible`, plus a tiny “canonical state digest”.
* Messages: `HELLO/WELCOME`, `EVENTS`, `ACK`, `WANT`, `PING/PONG` (you can omit keepalive initially).
* Properties: the 5 invariants from Model #1 plus the “no false durability” invariant from Model #2.

If you tell me what operations you want to include first (just `bead_upsert`? also `bead_delete`? notes?), I’ll pick the smallest “state digest” that still catches the real merge bugs you care about (resurrection, note collision, dep cross-namespace rejection).

