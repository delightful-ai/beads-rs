# REALTIME_SPEC_DRAFT Implementation Plan (Detailed, v0.4, self-contained)

This document describes the fundamental, architecture-level changes required to
implement REALTIME_SPEC_DRAFT. It is intentionally detailed and focuses on
data flow, invariants, module boundaries, and shared types rather than concrete
code edits. It preserves all prior plan details and augments them with decisions
made in review: shared types live in src/core, canonical CBOR uses minicbor with
custom canonical encoding for hashed payloads, WAL indexing uses SQLite behind
an abstraction, and replication remains threaded.

It is also intentionally self-contained. The spec remains the source of truth,
but this plan restates the high-risk and high-coupling requirements so an
engineer can implement independently without re-deriving semantics.

This plan assumes the current baseline:
- Canonical state lives in memory and syncs via Git ref `refs/heads/beads/store`.
- Current "WAL" is a per-remote snapshot file (`src/daemon/wal.rs`), not an event log.
- IPC is ndjson over Unix socket (`src/daemon/ipc.rs`).
- Mutations apply directly to `CanonicalState` (`src/daemon/executor.rs` +
  apply_wal_mutation in `src/daemon/core.rs`).
- CRDT semantics are LWW per-field plus tombstones/deps (`src/core/*`).

---

## North star

Ops produce events; events go to WAL; WAL drives apply; apply drives checkpoint;
replication ships events and watermarks.

More concretely:
1) Client sends mutation request (namespace-scoped).
2) Daemon validates, builds a single deterministic event delta (Txn-style),
   appends one event to the event WAL, fsyncs per durability policy.
3) Daemon applies the event(s) to in-memory state using a pure deterministic
   apply path.
4) Daemon returns an explicit durability receipt (including min-seen watermarks).
5) Daemon asynchronously replicates events to peers/anchors and schedules Git
   checkpoints (groups).

---

## Current state vs target state

Current state:
- Canonical state lives in memory and syncs via Git ref `refs/heads/beads/store`.
- Local "WAL" is a per-remote snapshot file, not an event log.
- State identity is effectively derived from remote URL and repo path.

Target state (REALTIME_SPEC_DRAFT):
- Event WAL per namespace is the unit of durability and replication.
- Git becomes a deterministic checkpoint lane (hot or cold), not the log.
- Store identity is stable and independent of transport.

---

## 0) Locked decisions and guiding constraints (expanded, must not drift)

The following decisions are locked and should be treated as non-negotiable.
They are restated verbatim with options, rationale, and impacts to ensure no
detail is lost during implementation.

### 0.0 Prior locked constraints (from the original plan)

- Shared types live in `src/core/*` (no new `src/store/*` root).
  This keeps daemon, git, API, and CLI on a single domain model.
- Canonical CBOR uses `minicbor` with a custom canonical encoder for hashed payloads.
  `minicbor_serde` is allowed only for non-hashed messages.
- WAL index uses SQLite (via `rusqlite`) behind a WalIndex trait.
  WAL segments are authoritative; the index is rebuildable cache state.
- Replication uses thread-based sessions, consistent with current daemon architecture.

### 0.1 Store identity: stop keying by RemoteUrl, keep multi-clone ergonomics

Options:
1) Keep current `RemoteUrl(normalize_url(...))` as primary key and bolt
   `store_id` on later.
2) Move to `StoreId` as the sole canonical key, treat `RemoteUrl` purely
   as a legacy discovery hint.
3) Hybrid: daemon internal map keyed by `StoreId`, plus a separate "transport
   identity" map keyed by `RemoteUrl` that only exists for Git lane compatibility
   and migration.

Recommendation to lock now: choose the hybrid with a strong rule: "all correctness
and state are keyed by StoreId; RemoteUrl only selects a checkpoint lane and
assists discovery during migration."

Rationale:
- `Daemon { repos: BTreeMap<RemoteUrl, RepoState>, path_to_remote: HashMap<PathBuf, RemoteUrl> }`
  in `src/daemon/core.rs` is the current system spine.
- If identity is not corrected now, every later change (namespaces, WAL, replication,
  durability) becomes entangled with legacy identity.

Risks if deferred:
- Namespace/WAL/index are implemented twice: once "per remote", then again
  "per store".
- Migration becomes non-local because identity leaks into filenames
  (`src/daemon/wal.rs` hashes remote URL into WAL filename) and into operational
  semantics (WAL deletion after sync).

Modules impacted:
- `src/daemon/core.rs` (replace repos map key, rewrite resolve/load flow)
- `src/daemon/remote.rs` (relegated to legacy normalization + fallback)
- `src/paths.rs` (store directory path shape)
- `src/git/sync.rs` (checkpoint lane keyed by store_id/group, not remote-url)
- `src/daemon/wal.rs` (legacy WAL naming uses remote hash today)

What must be true to proceed safely:
- A single source-of-truth mapping from (repo path or git remote) to StoreId
  must exist and must not depend on the remote URL string being stable.

### 0.2 Store directory layout and process-level locking

Options:
1) No locking, assume one daemon process per user.
2) File lock per store dir (recommended if you ever run multiple daemons or crash
   mid-write).
3) Lock per subsystem (WAL lock separate from checkpoint lock).

Recommendation to lock now: take a single lock file in
`$BD_DATA_DIR/stores/<store_id>/` at daemon open (store-global lock).
Keep it simple.

Lock metadata + stale handling (normative):
- The lock file MUST contain (at least): store_id, replica_id, pid, started_at_ms, daemon_version.
- On lock acquisition failure:
  - If the lock is held, fail fast with a clear error pointing to lock metadata.
  - If the lock appears stale (pid does not exist), provide a documented manual
    recovery procedure (do not auto-delete by default).

Rationale:
- Today, durability is "atomic rename + fsync" into a single snapshot WAL file.
- With segmented WAL + SQLite, you have multiple mutable files and background
  threads. A process-level lock prevents "two writers" bugs that are otherwise
  very hard to diagnose.

Risks if deferred:
- Locks get added after format assumptions in WAL replay and index rebuild,
  creating subtle deadlocks or partial write states.

Modules impacted:
- `src/paths.rs` (store dir computation; env overrides)
- new `src/daemon/store_runtime.rs` or similar (open store dir, acquire lock)
- new `src/daemon/wal/index.rs` (SQLite connection mode depends on locking choice)

What must be true to proceed safely:
- Code that opens SQLite and code that appends WAL must share the same locking story.
  "SQLite locking will save us" is not a plan because WAL segments are separate files.

### 0.3 Namespaces: where namespace lives in the data model

Options:
1) Make `CanonicalState` namespace-aware by changing keys to `(NamespaceId, BeadId)`
   and updating all internal indexes.
2) Keep `CanonicalState` exactly as-is and introduce
   `StoreState = BTreeMap<NamespaceId, CanonicalState>`.
3) Hybrid: `StoreState` map plus selective global indexes for cross-namespace queries.

Recommendation to lock now: keep `CanonicalState` unchanged and add `StoreState`
as the namespace boundary. Enforce "no cross-namespace deps" by construction:
dep operations always operate within a `CanonicalState` selected by namespace.

Rationale:
- `src/core/state.rs` contains derived dep indexes keyed by `BeadId`. If you
  namespace the keys inside `CanonicalState`, you touch almost every line in core.
- With `StoreState`, you localize namespace handling to call sites and keep CRDT
  algebra unchanged.

Risks if deferred:
- Every query and operation assumes single namespace and you retrofit later,
  causing inconsistent behavior (some paths default to core, others accidentally
  operate across all).
- Dep invariants (DAG enforcement for Blocks/Parent) get tricky if IDs collide
  across namespaces.

Modules impacted:
- `src/core/state.rs` (add `StoreState` type and accessors, leave CanonicalState
  mostly intact)
- `src/daemon/query.rs` (queries become namespace-scoped, and "status" might be
  per-namespace or aggregated)
- `src/daemon/ipc.rs` (add `namespace: Option<String>` to requests, default "core")
- `src/daemon/executor.rs` (every apply_* chooses namespace first)
- `src/api/mod.rs` (surface namespace in outputs)

What must be true to proceed safely:
- There must be a single defaulting rule ("if absent, namespace = core") used by IPC,
  CLI, and internal calls so the system cannot diverge.

### 0.4 Namespace policy representation

Options:
1) Config-only (`config.toml`), keep repo as dumb store.
2) Store-local `namespaces.toml` under `$BD_DATA_DIR/stores/<store_id>/`.
3) Git-published policies (checkpoint meta) as authoritative.

Recommendation to lock now: policies are store-local files (`namespaces.toml`)
and are optionally published via checkpoint metadata later. Keep initial
implementation minimal: load policies at store open; changes require restart
or explicit reload op.

Reload semantics (normative):
- Add an admin-only reload operation that re-reads namespaces.toml and validates it.
- Live-reload MAY apply changes that do not alter on-disk format or cross-component invariants:
  - visibility, ready_eligible, retention windows, ttl_basis (if GC marker semantics unchanged)
  - per-namespace limits (max record bytes, max ops) if only tightening constraints
- Changes that MUST require restart (or explicit "drain+restart" workflow):
  - checkpoint group membership, durable_copy_via_git toggles, replication listen params,
    replicate_mode changes that would require reconnecting sessions or rewriting durable sets.

Rationale:
- If policies are Git-authoritative too early, you couple checkpoint import to
  configuration semantics, complicating bootstrapping and migration.

Modules impacted:
- `src/config.rs` (likely expands, but do not overload it with store identity)
- new `src/core/namespace.rs` (NamespaceId + policy structs)
- new daemon store open path (load policies)

What must be true to proceed safely:
- Policy parsing must be deterministic and validated early so replication and
  checkpoint code can rely on it without defensive checks everywhere.

### 0.5 Event identity and origin sequence allocation

Options:
1) In-memory counter per namespace, persisted periodically (risk: duplicates).
2) Allocate origin_seq by reading "max seq for local replica" from SQLite inside
   WAL append transaction and incrementing (strong).
3) Allocate by scanning WAL tail at startup and caching (works but still needs
   concurrency control to avoid duplicates under parallel appends).

Recommendation to lock now: origin sequence allocation happens inside
`EventWal::append` using the SQLite index as the authoritative counter for
`(namespace, local_replica_id)`, inside a transaction that also records appended
event offsets. On startup, if the index is missing, rebuild it by scanning WAL
segments, then sequence allocation works again.

Rationale:
- The plan treats SQLite as a rebuildable cache, but for sequence allocation,
  you need an authoritative monotonic source to avoid equivocation.
- WAL is authoritative, but scanning WAL to allocate the next seq per append is
  too expensive.

Risks if deferred:
- Apply/replication logic assumes uniqueness, then duplicates appear after crash
  windows. Duplicates are protocol-level corruption because event_id must be unique.

Modules impacted:
- new `src/daemon/wal/mod.rs` (append API returns assigned seqs)
- new `src/daemon/wal/index.rs` (must support "next seq" or equivalent)
- new `src/core/identity.rs` (ReplicaId, EventId)
- replication receive path (must reject existing event_id with different sha)

What must be true to proceed safely:
- append must be single-writer per namespace within a process (mutex or channel
  serialization), and "next seq" allocation must be tied to the same durability
  boundary as writing the record.

### 0.6 Canonical CBOR encoding and hashing

Options:
1) Use `minicbor_serde` everywhere and hope it matches canonical requirements
   (it will not for hashing).
2) Manual `minicbor::Encode` with explicit ordering and definite lengths for
   hashed types only (plan).
3) Use numeric keys to reduce size and canonicalize easily, deviating from spec's
   human-readable field names.

Recommendation to lock now: keep spec-aligned string keys for envelope and delta
maps, and implement a canonical encoder for `EventEnvelope` that:
1) encodes a definite-length map,
2) emits keys in a precomputed canonical order (by canonical CBOR key ordering),
3) never encodes floats,
4) supports two canonical encodings:
   - canonical_preimage_bytes: envelope encoded with `sha256` and `prev_sha256` omitted
   - canonical_envelope_bytes: envelope encoded including `sha256` and `prev_sha256`

Hashing rule (clarified, normative):
- sha256 MUST be computed over canonical_preimage_bytes.
- WAL storage and replication payloads MUST use canonical_envelope_bytes.

Rationale:
- The minute you ship events between replicas, the hash is a correctness primitive.
  If encoding differs across platforms or versions, you have a fork in reality.

Risks if deferred:
- You will write a WAL format and replication protocol, then discover the
  encoding is not stable. Retrofitting canonical encoding after data exists
  means WAL migration tools.

Modules impacted:
- new `src/core/event.rs` (EventEnvelope + encode_canonical + hash computation)
- new `src/daemon/wal/frame.rs` (stores bytes, computes crc on bytes)
- replication protocol modules (EVENTS frames contain canonical bytes)

What must be true to proceed safely:
- Decide now and encode explicitly:
  - canonical_preimage_bytes is for hashing only.
  - canonical_envelope_bytes is for WAL + wire.
  The implementation MUST NOT conflate them.

### 0.6.1 Forward-compatible hashing via unknown-key preservation (normative)

Problem:
- Future protocol versions will add optional keys to EventEnvelope. Older nodes that
  decode into a strict typed struct will drop unknown keys and then fail sha256
  verification when re-encoding the preimage.

Requirement (normative):
- EventEnvelope decoding MUST be *lossless* for unknown CBOR map entries.
- The in-memory envelope representation MUST preserve:
  - unknown key bytes (canonical CBOR bytes for the key)
  - unknown value bytes (canonical CBOR bytes for the value)
  so that canonical_preimage_bytes can be reconstructed exactly.
- Duplicate keys are forbidden:
  - If a key appears more than once (including once in known fields and once in extras),
    decoding MUST fail with corruption error.

Encoding rule (normative):
- canonical_preimage_bytes and canonical_envelope_bytes MUST include unknown entries
  exactly as received (byte-for-byte) and MUST sort all keys (known + unknown)
  by canonical CBOR key ordering.

### 0.7 Delta schema choice

Options:
1) Reuse `src/git/wire.rs` WireBead for checkpoints and `bead_upsert` deltas.
2) Create `src/core/wire_bead.rs` with WireBeadFull and WireBeadPatch, and treat
   git wire as legacy-only.
3) Hybrid: keep field names identical to git wire for compatibility, but define
   new types in core and only share conversion helpers.

Recommendation to lock now: hybrid. Define new core wire types for checkpoint and
delta (realtime lane not coupled to Git lane), but keep field names and stamp
representation identical to existing WireBead so conversion logic can be shared.

Rationale:
- `src/git/wire.rs` is currently an implementation detail of the legacy ref
  `refs/heads/beads/store`. Reusing it directly for realtime events couples
  legacy layout to realtime protocol.

Risks if deferred:
- You will "just call wire::serialize_state" for events, then later need patch
  semantics and notes omission rules, forcing invasive refactors.

Modules impacted:
- new `src/core/wire_bead.rs` (or similar)
- `src/git/wire.rs` (likely stays, but becomes legacy + maybe shared helpers)
- new `src/core/apply.rs` (apply patch semantics)
- `src/core/bead.rs` (notes handling decisions)

What must be true to proceed safely:
- Lock the notes rule now: `bead_upsert` deltas SHOULD omit notes, and if present
  are interpreted as set-union only (never truncation). This affects apply and storage.

### 0.8 Deterministic apply semantics

Options:
1) Apply events directly inside daemon state mutators.
2) Create `core::apply_event(&mut CanonicalState, &EventEnvelope) -> ApplyOutcome`
   as pure logic, keep all I/O and indexing outside.
3) Apply returns index updates or notifications to keep side effects out.

Recommendation to lock now: make `core::apply_event` pure over a namespace state
and keep side effects in daemon. Keep the outcome minimal but useful: changed
bead ids, changed deps, new notes, advanced watermark, or no-op.

Rationale:
- CRDT joins already exist (`Lww::join`, `Tombstone::join`, `DepEdge::join`).
  The apply layer should be the only place that decides how deltas map to
  these primitives. This is mandatory for convergence.

Risks if deferred:
- Local ops and remote events diverge in corner cases (tombstone vs update
  resurrection, dep delete vs restore stamps, note id collision), and you will
  not detect it until replication.

Modules impacted:
- new `src/core/apply.rs`
- `src/daemon/executor.rs` (rewrite to generate deltas instead of mutating state)
- replication receive path (calls apply_event)
- checkpoint import (calls apply or merges states deterministically)

What must be true to proceed safely:
- Decide what "idempotent no-op" means. Spec: events with origin_seq <= seen_map
  must be no-op. Note id collision with different content is corruption, not LWW.

### 0.9 WAL segment naming and lifecycle

Options:
1) Name segments by timestamp only (`segment-<ts>.wal`), derive ranges by scanning.
2) Name by `(created_at_ms, first_origin_seq)` or include both.
3) One directory per namespace (spec) vs a flat directory with namespace in filename.

Recommendation to lock now: keep spec directory layout `wal/<namespace>/` and
name segments with a sortable prefix plus segment id (available at creation time):
`segment-<created_at_ms>-<segment_id>.wal`

`first_seq` remains a useful operator hint, but it MUST live in durable metadata
(segment header + SQLite `segments` table), not in the filename.

Rationale:
- Rebuild path needs deterministic ordering, and operators will look at filenames
  to sanity-check state.
- Embedding `first_seq` in the filename forces a rename after the first record is
  appended (because `first_seq` is unknown at file creation). Renames add directory
  fsyncs and create crash states (half-renamed segments or index rows pointing at
  a path that never became durable).
- `segment_id` is stable, globally unique, and becomes a natural join key across
  logs, SQLite rows, and filenames.

Risks if deferred:
- Index rebuild logic implicitly assumes ordering, then you rename format later
  and break compatibility or require migration.

Modules impacted:
- new `src/daemon/wal/segment.rs`
- new `src/daemon/wal/replay.rs` (scan order assumptions)
- new `src/daemon/wal/index.rs` (segment_path stored; naming affects stability)

What must be true to proceed safely:
- Define whether segment paths are part of the index primary key or just stored values.
  If you ever move/rename segments, index rebuild must be able to recover.

### 0.10 WAL framing and durability boundary (LocalFsync)

Options:
1) LocalFsync means fsync the record write only (fsync file, not directory).
2) LocalFsync means fsync file + directory entry durability where needed.
3) LocalFsync means fsync only at segment rotation boundaries.

Recommendation to lock now: LocalFsync means: append record, flush, fsync the
active segment file. On Unix, also fsync the directory when creating a new
segment file (on rotation), not on every record. This mirrors current snapshot
WAL logic (fsync file then directory after rename).

Performance note (recommended, correctness-preserving):
- The daemon MAY implement bounded group commit for LocalFsync and stronger classes:
  - multiple events can be appended under a single SQLite transaction and one fsync,
    as long as receipts are not returned until after the fsync and transaction commit.
  - bounds MUST be enforced (max latency/events/bytes) to keep tail latency predictable.

Rationale:
- Record-level fsync is the durability promise. Segment creation is metadata.
  Separate them to keep performance reasonable without weakening semantics.

Risks if deferred:
- Receipts and client behavior rely on durability that later turns out weaker
  than implied, which is a contract break.

Modules impacted:
- new `src/daemon/wal/segment.rs` (append + fsync)
- new `src/core/durability.rs` (semantic mapping)
- `src/daemon/ipc.rs` response timing (when to return receipt)

What must be true to proceed safely:
- WAL append must be atomic enough that replay either sees the record or does not.
  Tail-truncation rules must be implemented before trusting fsync boundaries.

### 0.11 SQLite index schema (replication + idempotency + receipts)

Options:
1) Events table only, no watermarks table.
2) Events + watermarks (plan suggests).
3) Add receipts table keyed by txn_id and/or client_request_id.

Recommendation to lock now: implement events + watermarks + a dedicated
`client_requests` (or `receipts`) table keyed by `(namespace, client_request_id)`
that stores: txn_id, requested/achieved durability class, list of event ids
(or compact range encoding), and min_seen watermark snapshot at commit time.

Crash-safety refinement (normative):
- client_requests MUST be a 2-phase state machine:
  - PENDING row inserted as part of the same append/index transaction (bound to event_ids + txn_id)
  - COMMITTED row finalized after apply and durability wait resolution, storing min_seen + proof
- On startup, the daemon MUST reconcile any PENDING rows deterministically and either:
  - finalize to COMMITTED, or
  - leave PENDING if corresponding events are not present/durable and require repair.

Idempotency safety refinement (normative):
- client_requests MUST store a request digest `request_sha256` computed from the parsed
  mutation request in a canonical form (excluding retry-only fields like wait_timeout_ms).
- If a request arrives with an existing (namespace, origin_replica_id, client_request_id)
  but a different request_sha256, the daemon MUST reject it with
  ERROR(code="client_request_id_reuse_mismatch") and MUST NOT return the prior receipt.
- Additionally (normative): WAL record headers MUST persist request_sha256 for locally
  authored events that carry client_request_id, so client_requests can be rebuilt from WAL
  even if SQLite is dropped/corrupted.

Receipt semantics:
- min_seen is applied watermarks at response/commit time.
- If waiting for durability times out but local append succeeded, return a retryable
  error including receipt.
- On idempotent retry, the daemon SHOULD return the same txn_id + event_ids + min_seen,
  but MAY return a stronger (monotonic) durability_proof if additional durability
  has been achieved since the original response.

Rationale:
- Idempotency "return exact same receipt and do not mint new stamps" is much easier
  if receipts are stored explicitly rather than reconstructed ad hoc.

Risks if deferred:
- You implement idempotency by scanning or uniqueness constraints, then later need
  receipts anyway for API. Retrofitting is costly and error-prone.

Modules impacted:
- new `src/daemon/wal/index.rs` (schema + queries)
- new `src/core/durability.rs` (receipt shape)
- mutation pipeline module (idempotency check uses index)
- `src/api/mod.rs` (DurabilityReceipt exposure)

What must be true to proceed safely:
- Decide on binary vs text storage for UUIDs in SQLite (BLOB 16 bytes is best).
  Lock this now because schema migration is annoying.

### 0.12 Watermark semantics: applied vs durable

Options:
1) Treat durable == applied (simpler, defeats durability classes).
2) Track both separately and advance durable only after fsync and peer ACK.
3) Track per-peer ACKs and compute a "durable quorum watermark" for ReplicatedFsync(k).

Recommendation to lock now:
- applied watermark advances when apply_event is performed.
- durable watermark advances when the event is on local disk (fsync) for that replica,
  and when a peer ACKs durable for replicated durability.
- min_seen in receipts is the server's applied watermark map at the time it returns
  success (or at least at time of commit).

Rationale:
- Spec explicitly distinguishes applied and durable. If collapsed early, you will
  have to unwind it later across replication and IPC.

Risks if deferred:
- Inconsistent client behavior when using require_min_seen and retries after
  partial durability.

Modules impacted:
- new `src/core/watermark.rs`
- new `src/daemon/durability_coordinator.rs` (or similar)
- replication ACK handler (updates watermarks)
- checkpoint export (uses durable watermarks in meta.included)

What must be true to proceed safely:
- Lock which watermark vector gets written into checkpoint meta.included (spec: durable),
  and ensure checkpoint import advances durable watermarks accordingly.
- prev_sha256 continuity MUST be validated against the durable chain head (not applied).

### 0.12.1 Hash-chain continuity across pruning and checkpoint baselines (normative)

Problem:
- prev_sha256 continuity checks require knowing the sha256 of the last included event for each
  (namespace, origin_replica_id). After WAL pruning, that boundary event may not exist locally.

Requirement:
- Every checkpoint meta.json MUST include a chain anchor for each included (namespace, origin):
  - `included_heads: { namespace -> { origin_replica_id -> sha256_hex } }`
  - Each value is the sha256 of the event at seq = meta.included[namespace][origin_replica_id].
  - If seq==0, the origin MAY be omitted from included_heads.

Import rule:
- On checkpoint import, StoreRuntime MUST seed `head_sha` from included_heads for any origin
  where the local WAL does not include the boundary event, so that subsequent events can be
  validated via prev_sha256 continuity.

### 0.13 Replication integration: threads vs core

Options:
1) Replication threads mutate state directly.
2) Replication threads decode frames and send ingest messages to daemon thread
   which appends + applies + acks.
3) Use an async runtime and shared state (large architectural shift).

Recommendation to lock now: keep current architecture style: one coordinator thread
owns state. Replication sessions are threads that communicate via channels to
the daemon event loop. The daemon does: verify hash, WAL append, apply, update
watermarks, then emits ACK back via session channel.

Rationale:
- `src/daemon/core.rs` is already the serialization point. Keeping that invariant
  avoids race conditions between WAL, apply, and indexes.

Risks if deferred:
- You "temporarily" let replication apply events and later discover index
  corruption or watermark races.

Modules impacted:
- new `src/daemon/repl/*` (session, manager, server)
- `src/daemon/core.rs` (new ingress path for replication events)
- new `src/daemon/wal/*` (append during replication receive)

What must be true to proceed safely:
- Define backpressure and bounds at the channel boundary, so a fast peer cannot OOM
  the daemon by sending huge batches.
- When ingest queue is full, the session MUST apply backpressure by:
  - stopping reads (letting TCP backpressure kick in), OR
  - responding with ERROR(code="overloaded") and closing the connection.
  Silent unbounded buffering is forbidden.

### 0.14 IPC protocol evolution

Options:
1) Bump protocol and require clients to upgrade, keep Request enum strict.
2) Backward-compatible additions: optional fields with defaults, plus new ops.
3) Introduce a new IPC transport for streaming subscriptions.

Recommendation to lock now: do backward-compatible additions for mutation requests:
add `namespace: Option<String>`, `durability: Option<DurabilityClass>`,
`client_request_id: Option<String/Uuid>` with defaults. Keep IPC_PROTOCOL_VERSION
bump only when you introduce streaming responses (Subscribe).

Rationale:
- `src/daemon/ipc.rs` is the compatibility boundary. Optional fields are easy;
  streaming is the real protocol change.

Risks if deferred:
- You change CLI and daemon together and accidentally break scripting users, or
  add streaming ad hoc that interferes with request/response framing.

Modules impacted:
- `src/daemon/ipc.rs` (Request variants, Response payload to include receipt)
- `src/api/mod.rs` (receipt types)
- `src/cli/mod.rs` (flags -> request fields)

What must be true to proceed safely:
- Decide how receipts appear on failure paths (timeouts). Spec suggests a retryable
  error including receipt. This needs a clear ErrorPayload extension strategy.

### 0.15 Git checkpoint lane redesign

Options:
1) Incrementally morph `src/git/sync.rs` into checkpoint import/export while keeping typestate.
2) Keep `git/sync.rs` as legacy lane and add `src/git/checkpoint/*` fresh.
3) Replace the whole git subsystem at once (high risk).

Recommendation to lock now: keep `src/git/sync.rs` as legacy and implement new
checkpoint modules in `src/git/checkpoint/*` as the plan says, with a clean API:
`export_checkpoint(StoreState, included_watermarks) -> tree`,
`import_checkpoint(tree) -> StoreState + included_watermarks`.

Rationale:
- `git/sync.rs` writes a single ref with monolithic files. The new spec requires
  sharded layout, manifest.json, meta.json with hashes, new refs, and discovery ref.
  Evolving the existing layout risks mixing formats.

Risks if deferred:
- You create half-checkpoints where some pieces follow the old layout and some follow
  the new, making import verification and determinism difficult.

Modules impacted:
- new `src/git/checkpoint/*` (layout, manifest, meta, export, import)
- `src/git/sync.rs` (becomes migration reader or removed later)
- `src/git/wire.rs` (likely reused for checkpoint JSONL lines, not for layout)
- `src/daemon/core.rs` (checkpoint scheduler replaces sync scheduler)

What must be true to proceed safely:
- Lock the checkpoint JSONL record schema. If you keep the current sparse `_v`
  format (recommended for continuity), state it explicitly so checkpoint and
  delta conversion can share logic.

### 0.16 Migration boundary (legacy to realtime)

Options:
1) Convert legacy git state into snapshot event per namespace (spec rejects snapshot-as-event).
2) Import legacy git state into StoreState and treat as baseline, then start WAL at origin_seq=0.
3) Reconstruct historical event log (not feasible, not required).

Recommendation to lock now: migration imports legacy state as initial checkpoint baseline
("seed snapshot") and starts WAL sequencing fresh. No synthetic historical deltas.
Persist StoreMeta and initial watermarks accordingly.

Rationale:
- Realtime correctness depends on deterministic apply of deltas from a baseline.
  Baseline can be a checkpoint. No need to fabricate history.

Risks if deferred:
- You waste time fabricating fake events and end up with incorrect stamps or broken idempotency.

Modules impacted:
- `src/daemon/wal.rs` (legacy snapshot WAL import, read-only)
- `src/git/sync.rs` + `src/git/wire.rs` (legacy ref import path)
- new checkpoint import path (seed baseline)

What must be true to proceed safely:
- Decide initial watermarks after importing a checkpoint. They should reflect
  "everything in checkpoint is applied and durable" for the origins represented,
  or you risk immediate replication gaps.

### 0.17 ActorId vs ReplicaId

Options:
1) Replace ActorId with ReplicaId everywhere.
2) Keep ActorId for stamps and provenance, ReplicaId only for event_id sequencing.
3) Encode both into stamps.

Recommendation to lock now: keep ActorId unchanged for LWW stamps. ReplicaId is
transport/durability identity only.

Rationale:
- CRDT ordering uses `Stamp { at: WriteStamp, by: ActorId }` with deterministic
  tie-break. This is independent of replication identity.

Risks if deferred:
- You rewrite core CRDT types and wire formats unnecessarily and create awkward UX
  for created_by/deleted_by fields.

Modules impacted:
- `src/core/time.rs` (Stamp ties to ActorId)
- `src/core/identity.rs` (add ReplicaId/StoreId/TxnId without touching ActorId)
- event delta schema (created_by fields remain strings)

What must be true to proceed safely:
- Decide how daemon selects ActorId by default (currently passed to Daemon::new(actor, wal)).
  This remains; replication must not treat actor strings as replica identity.

### 0.18 Top 5 lock-now list

If you want a top 5 lock-now list for early argument and alignment, it is:
- StoreId as the sole correctness identity (RemoteUrl is legacy discovery only).
- StoreState = namespaced map of CanonicalState (CanonicalState stays unchanged).
- origin_seq allocation inside WAL append via SQLite-backed counter semantics.
- One canonical CBOR bytestring used for WAL storage, wire transmission, and hashing.
- New checkpoint lane modules, leaving existing git sync as legacy/migration only.

---

## 0.19 Normative defaults and safety bounds (from spec)

These defaults are not optional; implementations MUST enforce bounds.

- MAX_FRAME_BYTES default 16 MiB.
- MAX_EVENT_BATCH_EVENTS default 10,000.
- MAX_EVENT_BATCH_BYTES default 10 MiB.
- KEEPALIVE_MS default 5s.
- DEAD_MS default 30s.
- WAL_SEGMENT_MAX_BYTES default 32 MiB.
- WAL_SEGMENT_MAX_AGE default 60s.
- MAX_WAL_RECORD_BYTES MUST be enforced; default <= 16 MiB.

Content-level limits (normative defaults; can be tightened per-namespace):
- MAX_NOTE_BYTES default 64 KiB (reject larger note_append content).
- MAX_OPS_PER_TXN default 10,000 (sum across bead/deps/notes ops).
- MAX_NOTE_APPENDS_PER_TXN default 1,000.
- MAX_LABELS_PER_BEAD default 256 (apply-time enforcement).

- WAL_GROUP_COMMIT_MAX_LATENCY_MS default 2ms (0 disables group commit).
- WAL_GROUP_COMMIT_MAX_EVENTS default 64.
- WAL_GROUP_COMMIT_MAX_BYTES default 1 MiB.
- MAX_SNAPSHOT_BYTES default 512 MiB (reject larger; require Git checkpoint or admin override).
- MAX_CONCURRENT_SNAPSHOTS default 1 per store (queue additional requests or reject).

CBOR decoding limits (normative):
- MAX_CBOR_DEPTH default 32 (prevents pathological nesting).
- MAX_CBOR_MAP_ENTRIES default 10,000 (prevents resource exhaustion during decode).
- MAX_CBOR_ARRAY_ENTRIES default 10,000.
- MAX_CBOR_BYTES_STRING_LEN default 16 MiB (must be <= MAX_FRAME_BYTES and <= MAX_WAL_RECORD_BYTES).
- MAX_CBOR_TEXT_STRING_LEN default 16 MiB (same bound as bytes strings).
- Reject payloads exceeding these limits before fully decoding.

Definite-length rule (normative for hashed structures):
- EventEnvelope and EventFrameV1 decoding MUST reject indefinite-length maps/arrays/strings.

Internal backpressure defaults (normative):
- MAX_IPC_INFLIGHT_MUTATIONS default 1024 (reject or apply backpressure when exceeded).
- MAX_REPL_INGEST_QUEUE_BYTES default 32 MiB per session (bounded channel).
- MAX_REPL_INGEST_QUEUE_EVENTS default 50,000 per session.
- MAX_CHECKPOINT_JOB_QUEUE default 8 per store (coalesce by group when full).

Overload shedding (new, normative):
- Add AdmissionController that enforces priority:
  1) local IPC mutations
  2) replication ingest
  3) WANT servicing
  4) checkpoint/scrub/snapshot serving
- When overloaded:
  - replication sessions MUST stop reads or close with ERROR(code="overloaded")
  - IPC mutations MAY return ERROR(code="overloaded") only when MAX_IPC_INFLIGHT_MUTATIONS exceeded
    (local mutations are highest priority; prefer shedding replication first)

I/O budgeting defaults (normative; may be tightened):
- MAX_REPL_INGEST_BYTES_PER_SEC default 64 MiB/s per session (token bucket; 0 disables).
- MAX_BACKGROUND_IO_BYTES_PER_SEC default 128 MiB/s per store for checkpoint/snapshot/scrub work.
- Priority rule (normative): WAL append+fsync for local mutations MUST NOT be starved by
  background jobs. Background jobs MUST yield when budgets are exhausted.

---

## 1) Build dependencies and crate wiring (compile shaping)

These changes are prerequisites for the core types and WAL/replication.

### 1.1 Add dependencies (Cargo)

Update `Cargo.toml` with required categories:
- `uuid` (with serde feature) for StoreId/ReplicaId/TxnId
- `minicbor` (and optional derive helpers)
- `rusqlite` for WAL index
- `crc32c` for WAL and replication framing
- Existing: `sha2`, `hex`, `serde`, `serde_json`, `thiserror`
- Performance (recommended):
  - `bytes` for cheap shared byte buffers in replication/WAL streaming
  - `memmap2` (optional) for zero-copy segment reads in stream_range_bytes
- Observability (recommended for production operability):
  - `tracing`, `tracing-subscriber`
  - optional: `metrics` + exporter (Prometheus or log-based)

### 1.2 Core module exports

Update `src/core/mod.rs` (and `src/lib.rs` if needed) to re-export:
- identity additions (StoreId, ReplicaId, TxnId, EventId)
- namespace
- watermark
- event
- durability
- apply
- store_state (if split out)

---

## 2) Core domain model changes (src/core/*)

These are shared by daemon, git, IPC, and CLI.

### 2.1 Identity and store metadata

Add newtypes and identifiers in `src/core/identity.rs`:
- `StoreId(uuid::Uuid)`
- `ReplicaId(uuid::Uuid)`
- `TxnId(uuid::Uuid)`
- `ClientRequestId(uuid::Uuid)` // client-provided idempotency token
- `StoreEpoch(u64)`
- `SegmentId(uuid::Uuid)` // WAL segment identity, stable join key for indexing/repair
- `EventId { origin_replica_id: ReplicaId, namespace: NamespaceId, origin_seq: u64 }`

Add store metadata struct for on-disk identity, prefer `src/core/store_meta.rs`:
- `StoreMeta { store_id, store_epoch, replica_id, store_format_version,
               wal_format_version, checkpoint_format_version,
               replication_protocol_version, created_at_ms }`

Additional spec-derived identity rules:
- `store_id` is generated once at store init and persisted in meta.json.
- `store_epoch` exists and starts at 0; it only changes on explicit reset.
- For repo-backed stores, `refs/beads/meta` contains `store_meta.json` with:
  - checkpoint_format_version
  - store_id
  - store_epoch
  - checkpoint_groups: { group_name -> git_ref } (map keys sorted)
- `store_slug` (if present) is a human alias and not a stable identifier.

Rationale:
- Current system keys state by RemoteUrl and repo path, which is unstable.
- Realtime replication requires stable IDs and a store epoch for invalidation.

### 2.1.1 Replica ID collision handling (normative)

Copying `$BD_DATA_DIR/stores/<store_id>/` to another machine duplicates `replica_id`,
which can cause event_id collisions and equivocation. This is an operational hazard.

Prevention and detection:
- On startup, the daemon SHOULD check if `replica_id` appears in the peer roster
  (if replicas.toml exists) as a different peer and emit a warning.
- On replication handshake, if `peer_replica_id == local_replica_id`, the daemon
  MUST reject the connection with ERROR(code="replica_id_collision").

Recovery:
- Provide `admin.rotate_replica_id` IPC operation:
  - Generates a new replica_id (UUID).
  - Updates meta.json.
  - Logs the old -> new mapping for traceability.
- After rotation, the replica starts with origin_seq = 0 for the new identity.
  Existing events authored by the old replica_id remain valid and attributed to
  the old identity.

### 2.1.2 Replica roster (strongly recommended; normative for durability/pruning)

Add store-local roster file:
- Path: `$BD_DATA_DIR/stores/<store_id>/replicas.toml`

Roster record fields (minimum viable):
- replica_id: UUID
- name: string (human label)
- role: anchor | peer | observer
- durability_eligible: bool  // counts toward ReplicatedFsync(k) and pruning safety gates
- allowed_namespaces: [string] (optional; if present, restrict replication scope)
- expire_after_ms: u64 (optional; if set, replica may be treated as inactive if not seen recently)

Semantics:
- If replicas.toml exists, inbound replication from unknown replica_id MUST be rejected with
  ERROR(code="unknown_replica") (non-cryptographic safety rail to prevent miswiring).
- DurabilityCoordinator MUST select eligible replicas from roster where durability_eligible=true
  and namespace policy allows replication.
- WAL pruning MUST treat replicas as "active" only if (now_ms - last_seen_ms) <= expire_after_ms
  (or if expire_after_ms is absent, treat as always active).
- The daemon SHOULD persist last_seen_ms for known replicas so pruning and admin.status remain correct
  across restarts (see wal.sqlite schema addition below).

### 2.2 Namespaces and policies

Add `src/core/namespace.rs`:
- `NamespaceId(String)` validation `[a-z][a-z0-9_]{0,31}`
- `NamespacePolicy` fields:
  - persist_to_git: bool
  - replicate_mode: none | anchors | peers | p2p
  - retention: forever | ttl:<duration> | size:<bytes>
  - ready_eligible: bool
  - visibility: normal | pinned
  - gc_authority: checkpoint_writer | explicit_replica:<replica_id> | none
  - ttl_basis: last_mutation_stamp | event_time | explicit_field
- `CheckpointGroup` config:
  - namespaces included
  - git remote/ref
  - writer set, primary writer
  - debounce, max interval, max events
  - durable_copy_via_git

Defaults (from spec):
- core: persist_to_git=true, replicate=peers, retention=forever, ready_eligible=true,
  visibility=normal, gc_authority=checkpoint_writer, ttl_basis=last_mutation_stamp
- sys: persist_to_git=true, replicate=anchors, retention=forever, ready_eligible=false,
  visibility=pinned, gc_authority=checkpoint_writer, ttl_basis=last_mutation_stamp
- wf: persist_to_git=false, replicate=anchors, retention=ttl:7d, ready_eligible=false,
  visibility=normal, gc_authority=checkpoint_writer, ttl_basis=last_mutation_stamp
- tmp: persist_to_git=false, replicate=none, retention=ttl:24h, ready_eligible=false,
  visibility=normal, gc_authority=none

Rationale:
- Namespaces are first-class; retrofitting them later breaks indexing and queries.

### 2.3 Watermarks

Add `src/core/watermark.rs`:
- `WatermarkMap = BTreeMap<NamespaceId, BTreeMap<ReplicaId, u64>>`
  (deterministic ordering)

Helpers:
- `get(ns, origin) -> u64` (default 0)
- `advance_contiguous(ns, origin, new_seq)`
- `merge_max(other)`

Use cases:
- ACK semantics for replication
- Checkpoint inclusion proof
- Durability receipts (min_seen)
- Subscription gating (require_min_seen)

### 2.4 Event envelopes and deltas

Add `src/core/event.rs`:

EventEnvelope fields (spec-aligned):
- envelope_v
- store_id, store_epoch, namespace
- origin_replica_id, origin_seq
- event_time_ms, txn_id, client_request_id: Option<ClientRequestId>
- hlc_max: Option<{ actor_id: ActorId, physical_ms: u64, logical: u32 }>
- kind, delta
- sha256, prev_sha256
- unknown: CborUnknownFields  // preserves unrecognized CBOR map entries (see 0.6.1)

ClientRequestId scope rule:
- client_request_id uniqueness and idempotency MUST be scoped to:
  (namespace, origin_replica_id, client_request_id).
  It MUST NOT be treated as globally unique within a store.

EventKind:
- TxnV1
- NamespaceGcMarker

DeltaV1:
- TxnDeltaV1 (the default for client mutations; carries 0..N ops)
- NamespaceGcMarkerV1

TxnDeltaV1 contains operation lists (all intra-namespace):
- bead_upserts: Vec<WireBeadPatch>
- bead_deletes: Vec<{ id, deleted_at, deleted_by, ... }>
- dep_upserts: Vec<{ from, to, kind, live_at, live_by, ... }>
- dep_deletes: Vec<{ from, to, kind, dead_at, dead_by, ... }>
- note_appends: Vec<{ note_id, bead_id, content, author, at }>

Limits (normative):
- note_append.content MUST be <= MAX_NOTE_BYTES.
- Total ops in TxnDeltaV1 MUST be <= MAX_OPS_PER_TXN (reject at mutation planning and at replication ingest).

Event-time rule:
- `event_time_ms` MUST equal the physical wall time used to mint the write stamp
  for the mutation that produced this event.
- If delta contains multiple new stamps, event_time_ms MUST be >= the max wall time
  and SHOULD equal it.

HLC max rule (normative):
- For TxnV1 events that mint stamps, `hlc_max` MUST be present and MUST equal the
  maximum stamp minted in the txn for the effective ActorId.
- This field exists to support fast, correct HLC recovery after restart or index rebuild.

Hash rules:
- sha256 MUST be computed over canonical CBOR encoding of EventEnvelope with
  sha256 and prev_sha256 omitted.
- prev_sha256 (if present) MUST match the previous hash for same (origin_replica_id, namespace).

Delta guidance:
- TxnDeltaV1 `bead_upserts` should omit notes; notes replicate via `note_appends`.
- A `bead_upsert` that includes notes is interpreted as "at least these notes exist"
  (set-union), never truncation.
- `note_append` carries note id, content, author, stamp.
- `bead_delete` includes deletion stamp and lineage fields if present.
- `dep_upsert` and `dep_delete` include from/to/kind and life stamps.
- `dep_upsert`/`dep_delete` are intra-namespace only; cross-namespace is rejected.
- `namespace_gc_marker` includes ttl_basis, cutoff_ms, safety_margin_ms,
  gc_authority_replica_id, enforce_floor.

### 2.5 Wire bead representations

Add `src/core/wire_bead.rs`:
- `WireBeadFull` for checkpoint snapshots (all fields present)
- `WireBeadPatch` for deltas (mutable fields as Option<T>)

WireBead field names match legacy git wire (sparse `_v` format) to allow shared
conversion logic:
- core: id, key?, created_at, created_by, created_on_branch?
- fields: title, description, design?, acceptance_criteria?, priority, type, labels,
  external_ref?, source_repo?, estimated_minutes?
- workflow: status, closed_at?, closed_by?, closed_reason?, closed_on_branch?
- claim: assignee?, assignee_at?, assignee_expires?
- notes? (note objects: id, content, author, at)
- version metadata: _at, _by, _v?

### 2.6 Durability semantics

Add `src/core/durability.rs`:
- `DurabilityClass`:
  - LocalFsync
  - ReplicatedFsync { k: u32 }
  - GitCheckpointed
- `DurabilityProofV1` (normative, extensible):
  - local_fsync: { at_ms: u64, durable_seq: WatermarkMap }  // proof of local disk durability
  - replicated: Option<{ k: u32, acked_by: Vec<ReplicaId> }>
  - git: Option<{ checkpoint_group: String, git_ref: String, commit_id: String, included: WatermarkMap }>
- `DurabilityReceipt`:
  - store_id, store_epoch, txn_id, events: Vec<EventId>
  - requested_class: DurabilityClass
  - durability_proof: DurabilityProofV1
  - achieved_class: Option<DurabilityClass>  // derived convenience only
  - min_seen: WatermarkMap

Receipt semantics:
- min_seen is applied watermarks at response/commit time.
- If waiting for durability times out but local append succeeded, return a retryable
  error including receipt.

### 2.7 Namespaced store state

Add `StoreState = BTreeMap<NamespaceId, CanonicalState>` in
`src/core/state.rs` or `src/core/store_state.rs`.
Add helpers:
- `ns()` / `ns_mut()`
- `core()` / `core_mut()` convenience for legacy paths

Rule:
- Global indexes keyed by BeadId/DepKey/TombstoneKey remain per-namespace by
  containment, not by altering key types.

### 2.8 Write stamps (explicit requirements)

Write stamps are used only for conflict resolution (not replication progress).
They must be monotonic per actor on a replica and totally ordered by
(physical_ms, logical_counter, actor_id tie-break).

Persistence requirement (normative):
- The daemon MUST persist enough HLC state to prevent stamp regression after restart
  or wall-clock rollback. At minimum, persist (last_physical_ms, last_logical)
  for the daemon's active ActorId.
- On startup, initialize the HLC from persisted state.
- On mint, if system_time_ms < last_physical_ms, clamp physical_ms to last_physical_ms
  and increment logical_counter.

Recovery requirement (normative):
- If the SQLite index is rebuilt or missing, the daemon MUST reconstruct HLC state
  by scanning locally-authored WAL records and taking max(hlc_max) per ActorId.
- This MUST NOT require decoding full deltas; it should use `hlc_max` from the envelope.

---

## 3) Canonical CBOR with minicbor

We will use `minicbor` directly for hashed payloads. `minicbor_serde` is allowed
for non-hashed messages only.

### 3.1 Canonical encoding requirements (RFC 8949)
- Definite-length maps and arrays only.
- Keys sorted by canonical CBOR ordering:
  - sort by encoded key length, then by encoded key bytes.
- No floats in event envelopes to avoid canonical float ambiguity.
- Exclude sha256 and prev_sha256 from the hash input.

### 3.2 Encoding strategy
- Implement `EventEnvelope::encode_canonical()` using `minicbor::Encoder`.
- Precompute a canonical key order for envelope maps to avoid per-encode sorting.
- Encoder MUST merge known fields and preserved unknown fields (0.6.1) and sort by
  canonical CBOR key ordering.
- Implement:
  - `encode_canonical_preimage()` (omits sha256/prev_sha256)
  - `encode_canonical_envelope()` (includes sha256/prev_sha256)
  - `compute_sha256()` hashes preimage bytes
  - WAL + wire always carry envelope bytes

### 3.3 Why not minicbor_serde for hashing
- Serde does not guarantee canonical key ordering.
- Canonical ordering is required for tamper-evident hashes.

---

## 4) Deterministic event apply semantics (core)

Create a pure apply path so local mutations and remote events converge identically.

### 4.1 Apply module (`src/core/apply.rs`)

Add:
- `apply_event(ns_state: &mut CanonicalState, event: &EventEnvelope)
   -> Result<ApplyOutcome, ApplyError>`

Add shared gating helpers:
- `should_apply(applied_seq: u64, incoming_seq: u64) -> ApplyGate`
  where ApplyGate is one of:
  - AlreadyApplied (no-op; do not call apply_event)
  - Gap (buffer/request; do not call apply_event)
  - ApplyNow (call apply_event, then advance applied watermark if contiguous)

ApplyOutcome should include:
- changed bead ids
- changed deps
- new notes
- (no watermark mutation; watermark advancement is owned by StoreRuntime)
- derived_dirty_shards (optional helper): the set of checkpoint shard ids (00..ff)
  affected by this event for beads/tombstones/deps in the namespace

### 4.2 Merge rules (explicit, idempotent)

- bead_upsert:
  - Apply each field using LWW stamps.
  - If patch omits a field, leave it unchanged.
- bead_delete:
  - Merge tombstone with LWW semantics.
  - Ignore delete if older than live stamp (resurrection rule).
- dep_upsert / dep_delete:
  - Treat as LWW on DepLife.
  - Deletions should carry a delete stamp.
- note_append:
  - Insert note if id not present.
  - If same id exists with different content, treat as corruption.
- namespace_gc_marker:
  - Update per-namespace GC floor and enforce safety margin logic.
  - Actual deletion can be deferred to a background task.

Idempotency:
- Idempotency MUST be enforced by StoreRuntime via watermarks before calling apply_event.
- apply_event MUST remain deterministic and side-effect-free with respect to watermarks.

Rationale:
- Event apply must be deterministic and idempotent across local and remote lanes.

---

## 5) Event WAL redesign (`src/daemon/wal/*`)

Replace the snapshot WAL with event WAL per namespace. Keep legacy WAL read-only.

### 5.1 File move: free the wal module name

Current `src/daemon/wal.rs` conflicts with planned `src/daemon/wal/` directory.
Rename to `src/daemon/wal_legacy_snapshot.rs`, update imports, keep read-only
for migration and one-release compatibility.

### 5.2 File layout and framing (spec-aligned)

Directory layout under store dir:
- `wal/<namespace>/segment-<created_at_ms>-<segment_id>.wal`
- `index/wal.sqlite`
- `snap/` for snapshot temp files

Segment header (raw bytes, little-endian where applicable):
- magic: 5 bytes = ASCII "BDWAL"
- wal_format_version: u32_le
- header_len: u32_le  // total header bytes including header_crc32c
- store_id: 16 bytes (UUID bytes)
- store_epoch: u64_le
- namespace_len: u32_le
- namespace_bytes: namespace_len bytes (UTF-8)
- created_at_ms: u64_le
- segment_id: 16 bytes (UUID bytes) // stable identity for indexing/repair
- flags: u32_le
- header_crc32c: u32_le  // crc32c (Castagnoli) of header bytes excluding this field

Record framing (wal_format_version = 2):
- `u32_le record_magic` = ASCII "BDR2" (0x42445232)
- `u32_le length`  // total bytes of (record_header + payload)
- `u32_le crc32c`  // Castagnoli over bytes from record_header_v through end of payload
- record_header_v: u16_le (currently 1)
- record_header_len: u16_le  // bytes of record header starting at record_header_v
- flags: u16_le
- reserved: u16_le  // must be 0 for now; future use
  - bit0: has_prev_sha
  - bit1: has_client_request_id
  - bit2: has_request_sha256   // local-only idempotency digest (see 6.2 + 8.2)
- origin_replica_id: 16 bytes
- origin_seq: u64_le
- event_time_ms: u64_le
- txn_id: 16 bytes
- client_request_id: 16 bytes (if flag set)
- request_sha256: 32 bytes (if flag set)  // sha256 of canonicalized mutation request (retry-stable)
- sha256: 32 bytes
- prev_sha256: 32 bytes (if flag set)
- payload: canonical CBOR bytes of EventEnvelope (includes sha256/prev_sha256)

Header consistency (normative):
- record_header fields MUST match the decoded envelope fields.
- On mismatch, treat as corruption (fail unless repair mode).

Forward compatibility rule (normative):
- Readers MUST skip any unknown trailing bytes in the record header using record_header_len.
- Writers MUST set reserved=0 and MUST NOT rely on readers interpreting unknown fields.

Rotation triggers:
- max bytes (default 32 MiB)
- max age (default 60s)

Max record size:
- Must enforce MAX_WAL_RECORD_BYTES (default <= 16 MiB).
- Records larger than limit must be rejected at write time and error at read time.

Tail recovery:
- If EOF occurs mid-record, ignore partial tail and truncate to last valid boundary.
- If crc mismatch at tail, truncate to last valid boundary.
- If corruption is detected not at tail, fail with explicit error by default.
- Repair mode MAY attempt resynchronization by scanning forward for the next
  `record_magic` within a bounded window, then continue indexing. Apply MUST still
  enforce per-(ns, origin) contiguity and prev_sha continuity.

### 5.3 WAL segment naming and meaning of first_seq

Segment filename: `segment-<created_at_ms>-<segment_id>.wal`:
- `created_at_ms` from segment header.
- `segment_id` from segment header.

`first_seq` is the origin_seq of the first record written to the segment (from any origin).
It SHOULD be cached in SQLite (segments table) for operator visibility.
Offline tools can derive it cheaply from the first record_header without decoding CBOR.

WAL index is authoritative for range serving and should not rely on filename
semantics beyond ordering.

### 5.4 WAL module structure

New files:
- `src/daemon/wal/mod.rs` (public API)
- `src/daemon/wal/segment.rs` (segment header and rotation)
- `src/daemon/wal/frame.rs` (crc32c framing)
- `src/daemon/wal/index.rs` (WalIndex trait + SQLite implementation)
- `src/daemon/wal/replay.rs` (scan, rebuild, truncate)
- `src/daemon/wal/error.rs` (WalError types)

Legacy:
- `src/daemon/wal_legacy_snapshot.rs` (read-only snapshot WAL)

### 5.5 EventWal API (example shape, locked)

```rust
pub struct EventWal { /* store_dir, index, open segments */ }

pub struct EventBytes {
  pub eid: EventId,
  pub sha: [u8; 32],
  pub prev_sha: Option<[u8; 32]>, // fast-path chain link (also present in envelope bytes)
  pub bytes: bytes::Bytes, // canonical CBOR bytes; MAY be a zero-copy view into an mmapped segment
}

/// Locally-produced event input. MUST omit origin_seq/sha/prev_sha.
pub struct EventDraft {
  pub envelope_without_ids: EventEnvelope, // origin_seq=0, sha256/prev_sha256 unset
}

pub struct AppendResult {
  pub event_ids: Vec<EventId>,
  pub shas: Vec<[u8; 32]>,
  pub bytes_written: u64,
}

impl EventWal {
  pub fn open(store_dir: &Path, meta: &StoreMeta) -> Result<Self, WalError>;
  /// Append locally-authored events (origin_replica_id == meta.replica_id).
  /// Allocates origin_seq and fills prev_sha256/sha256 deterministically.
  pub fn append_local(&mut self, ns: &NamespaceId, drafts: &[EventDraft])
    -> Result<AppendResult, WalError>;
  /// Ingest remote-authored events as-is using canonical bytes received from the origin.
  /// The caller MUST enforce contiguity and prev_sha continuity before calling.
  pub fn ingest_remote_bytes(&mut self, ns: &NamespaceId, origin: &ReplicaId,
                             frames: &[EventBytes])
    -> Result<AppendResult, WalError>;
  pub fn fsync_namespace(&mut self, ns: &NamespaceId) -> Result<(), WalError>;
  pub fn stream_range_bytes(&self, ns: &NamespaceId, origin: &ReplicaId,
                            from_seq_exclusive: u64, max_bytes: usize)
    -> Result<Vec<EventBytes>, WalError>;
  pub fn stream_range(&self, ns: &NamespaceId, origin: &ReplicaId,
                      from_seq_exclusive: u64, max_bytes: usize)
    -> Result<Vec<EventEnvelope>, WalError>; // convenience wrapper
  pub fn max_origin_seq(&self, ns: &NamespaceId, origin: &ReplicaId) -> Result<u64, WalError>;
  pub fn replay(&self, ns: &NamespaceId) -> Result<(), WalError>;
}
```

Implementation details:
- `append_local` allocates origin_seq (SQLite counter), sets prev_sha256 from a
  durable head cache (or index lookup on cold start), computes canonical bytes + sha,
  writes frames, fsyncs per durability, records index entries, and returns assigned ids.
- `ingest_remote_bytes` does NOT allocate origin_seq and MUST NOT re-encode bytes.
  It writes validated canonical bytes, records index entries, and returns ids.
- `stream_range` reads canonical bytes from WAL and decodes (or streams bytes
  directly; still must validate hash).
- `replay` rebuilds in-memory state or index from WAL segments.

### 5.6 WAL append flow (authoritative)

Local append (authoritative):
1) Acquire per-namespace append lock.
2) Allocate origin_seq inside SQLite transaction (table-backed counter).
3) Resolve prev_sha256 for (ns, local_replica_id, origin_seq-1) from an in-memory
   head cache (populate at open; update on each successful fsync+commit).
4) Encode canonical envelope bytes and compute sha256.
5) Write framed record(s) to active segment.
6) If new segment created, fsync directory once.
7) For LocalFsync or stronger, fsync active segment file.
8) Record event offsets + sha + prev_sha in SQLite and commit transaction.
9) Update in-memory head cache and return assigned EventIds.

Remote ingest (authoritative):
1) Caller enforces contiguity: origin_seq == durable_seen+1 and prev_sha matches
   the current head sha for (ns, origin).
2) Acquire per-namespace append lock.
3) Write framed record(s) using provided canonical bytes (no re-encode).
4) Fsync per durability policy used for remote ingest (durable means "on disk here").
5) Record offsets + sha + prev_sha in SQLite and commit.
6) Advance durable/applied watermarks via StoreRuntime and update head cache.

### 5.7 WAL pruning and local snapshots

- WAL segments are authoritative; pruning only when a local snapshot `included`
  watermarks cover the segment and retention policy allows it.
- Pruning MUST NOT unlink a segment file while it is actively referenced by any
  replication/WANT reader (e.g., mmapped or otherwise pinned). Implementations SHOULD
  use refcounts or epoch-based deferral.
- Keep WAL indefinitely on anchors if configured for audit.
- Local snapshots (compaction) are optional but must include included watermarks
  and GC floors, and must be versioned.

Retention + pruning gates (normative):
- Implement a background GC/retention worker that:
  - evaluates namespace retention policies (ttl/size/forever),
  - emits NamespaceGcMarker events when policy requires (only by gc_authority),
  - applies GC floors and schedules actual deletion/compaction work.
- WAL segment pruning MUST be gated by BOTH:
  - safety for replication: segment contains no events above the minimum durable
    watermark required by any eligible replica still tracked by policy, AND
  - replay safety: either (a) the segment's events are included in a durable
    checkpoint for that namespace/group, or (b) policy explicitly allows "WAL-only"
    durability without Git replay.
  - operational safety: if Git is the only checkpoint lane in use,
    the daemon MUST maintain a verified local checkpoint cache before allowing pruning.
    Otherwise a temporary Git outage can make the store unrecoverable after restart.
- Idempotency receipts MUST be garbage collected:
  - default window recommendation: keep >= 7 days of client_request_id receipts
    (configurable per namespace), OR cap by total rows/bytes to prevent DB bloat.

---

## 6) WAL index with SQLite (`src/daemon/wal/index.rs`)

SQLite is used as a rebuildable index, not the source of truth.

### 6.1 WalIndex trait (shape to lock)

```rust
pub trait WalIndex: Send + Sync {
  /// Returns a writer handle bound to the coordinator thread (single writer connection).
  fn writer(&self) -> Box<dyn WalIndexWriter>;
  /// Returns a reader handle usable by replication/admin threads (pooled read-only connections).
  fn reader(&self) -> Box<dyn WalIndexReader>;
}

pub trait WalIndexWriter {
  /// Begin a single atomic index update transaction (pairs with WAL append critical section).
  fn begin_txn(&self) -> Result<Box<dyn WalIndexTxn>, WalError>;
}

pub trait WalIndexTxn {
  fn next_origin_seq(&mut self, ns: &NamespaceId, origin: &ReplicaId) -> Result<u64, WalError>;
  fn record_event(&mut self, ns: &NamespaceId, eid: &EventId, sha: [u8; 32],
                  prev_sha: Option<[u8; 32]>,
                  segment_id: SegmentId, offset: u64, len: u32,
                  event_time_ms: u64, txn_id: TxnId,
                  client_request_id: Option<ClientRequestId>,
                  request_sha256: Option<[u8; 32]>)
    -> Result<(), WalError>;
  fn update_watermark(&mut self, ns: &NamespaceId, origin: &ReplicaId,
                      applied: u64, durable: u64,
                      applied_head_sha: Option<[u8; 32]>,
                      durable_head_sha: Option<[u8; 32]>) -> Result<(), WalError>;
  fn upsert_client_request_pending(&mut self, ns: &NamespaceId, origin: &ReplicaId,
                                   client_request_id: ClientRequestId,
                                   request_sha256: [u8; 32],
                                   txn_id: TxnId, requested_class: DurabilityClass,
                                   event_ids: &[EventId]) -> Result<(), WalError>;
  fn finalize_client_request_committed(&mut self, receipt: &DurabilityReceipt) -> Result<(), WalError>;
  fn commit(self: Box<Self>) -> Result<(), WalError>;
  fn rollback(self: Box<Self>) -> Result<(), WalError>;
}

pub trait WalIndexReader {
  fn lookup_event_sha(&self, ns: &NamespaceId, eid: &EventId)
    -> Result<Option<[u8; 32]>, WalError>;
  fn iter_from(&self, ns: &NamespaceId, origin: &ReplicaId, from_seq_excl: u64,
               max_bytes: usize) -> Result<Vec<IndexedRangeItem>, WalError>;
  fn lookup_client_request(&self, ns: &NamespaceId, origin: &ReplicaId,
                           client_request_id: ClientRequestId)
    -> Result<Option<DurabilityReceipt>, WalError>;
  fn max_origin_seq(&self, ns: &NamespaceId, origin: &ReplicaId) -> Result<u64, WalError>;
}
```

### 6.2 Minimal schema (example, spec-aligned)

```
CREATE TABLE events (
  namespace TEXT NOT NULL,
  origin_replica_id BLOB NOT NULL,
  origin_seq INTEGER NOT NULL,
  sha BLOB NOT NULL,
  prev_sha BLOB, -- nullable; present for seq>1
  segment_id BLOB NOT NULL,    -- SegmentId UUID bytes (stable identity)
  segment_offset INTEGER NOT NULL,
  len INTEGER NOT NULL,
  event_time_ms INTEGER NOT NULL,
  txn_id BLOB NOT NULL,
  client_request_id BLOB,
  PRIMARY KEY (namespace, origin_replica_id, origin_seq)
);

CREATE INDEX events_by_client_request
  ON events (namespace, origin_replica_id, client_request_id);

CREATE INDEX events_by_origin_seq
  ON events (namespace, origin_replica_id, origin_seq);

CREATE TABLE watermarks (
  namespace TEXT NOT NULL,
  origin_replica_id BLOB NOT NULL,
  applied_seq INTEGER NOT NULL,
  durable_seq INTEGER NOT NULL,
  applied_head_sha BLOB,  -- sha256 at applied_seq (nullable if applied_seq==0)
  durable_head_sha BLOB,  -- sha256 at durable_seq (nullable if durable_seq==0)
  PRIMARY KEY (namespace, origin_replica_id)
);

CREATE TABLE client_requests (
  namespace TEXT NOT NULL,
  origin_replica_id BLOB NOT NULL,
  client_request_id BLOB NOT NULL,
  state INTEGER NOT NULL,       -- 0=PENDING, 1=COMMITTED
  created_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  request_sha256 BLOB NOT NULL, -- 32 bytes, see idempotency safety refinement
  txn_id BLOB NOT NULL,
  requested_class INTEGER NOT NULL,
  durability_proof BLOB,        -- nullable while PENDING
  achieved_class INTEGER,       -- optional derived cache
  min_seen BLOB,                -- nullable while PENDING
  event_ids BLOB NOT NULL,      -- known at PENDING time
  PRIMARY KEY (namespace, origin_replica_id, client_request_id)
);

CREATE TABLE origin_seq (
  namespace TEXT NOT NULL,
  origin_replica_id BLOB NOT NULL,
  next_seq INTEGER NOT NULL,
  PRIMARY KEY (namespace, origin_replica_id)
);

CREATE TABLE meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- HLC persistence (minimal)
CREATE TABLE hlc (
  actor_id TEXT PRIMARY KEY,
  last_physical_ms INTEGER NOT NULL,
  last_logical INTEGER NOT NULL
);

-- Segment bookkeeping to support incremental recovery without full scans
CREATE TABLE segments (
  namespace TEXT NOT NULL,
  segment_id BLOB NOT NULL,
  segment_path TEXT NOT NULL,      -- relative to store_dir
  created_at_ms INTEGER NOT NULL,
  last_indexed_offset INTEGER NOT NULL,
  PRIMARY KEY (namespace, segment_id)
);
CREATE INDEX segments_by_ns_created
  ON segments (namespace, created_at_ms);
-- Optional FK for integrity (defensive):
-- FOREIGN KEY(namespace, segment_id) REFERENCES segments(namespace, segment_id)

-- Replica liveness cache (rebuildable; improves pruning and observability)
CREATE TABLE replica_liveness (
  replica_id BLOB PRIMARY KEY,
  last_seen_ms INTEGER NOT NULL,
  last_handshake_ms INTEGER NOT NULL,
  role TEXT NOT NULL,
  durability_eligible INTEGER NOT NULL
);
```

Notes:
- UUIDs stored as 16-byte BLOBs.
- min_seen and event_ids can be CBOR-encoded blobs.
  Under the "one request → one event" rule, event_ids is typically length 1,
  but remains a Vec for forward compatibility.
- meta should store schema version and store_id for sanity checks.

Startup sanity checks (normative):
- On every store open (even when index exists), the daemon MUST verify:
  - meta.store_id/store_epoch match StoreMeta (fail fast if mismatch).
  - For each segments row: last_indexed_offset <= current file length, else treat
    as index corruption and force rebuild for that namespace.
  - origin_seq.next_seq >= (max(origin_seq) observed in events table) + 1, else
    recompute origin_seq from events table (or full WAL scan if events table missing).
- If any check fails, rebuild the index from WAL segments before allowing appends.

Pending receipt reconciliation (normative):
- On store open after WAL replay, scan client_requests WHERE state=0 (PENDING) and:
  - validate referenced event_ids exist in events table (or can be found via WAL scan)
  - compute whether events are applied and what min_seen should be
  - update to COMMITTED with deterministic proof fields where possible
  - if events are missing/corrupt, leave PENDING for operator review via admin.doctor

### 6.3 Semantics

- WAL is authoritative; SQLite can be dropped and rebuilt.
- On startup, if index is missing or version mismatched, scan WAL segments to rebuild.
- On startup, if index exists, perform incremental catch-up:
  for each segment, if file_len > segments.last_indexed_offset,
  scan frames from last_indexed_offset to EOF and index any valid records.
  Apply tail-truncation rules if partial/corrupt tail is encountered.
- `client_request_id` lookups provide idempotency without scanning WAL.
- Equivocation: if same EventId exists with different sha, treat as protocol error.
- origin_seq allocation is authoritative in SQLite but rebuildable from WAL.

### 6.4 Transaction boundaries

- origin_seq allocation, WAL write, and record_event indexing must happen in a single
  critical section and be committed together.
- SQLite transactions must not be committed before WAL fsync (for LocalFsync).

SQLite PRAGMA choices (normative):
- `journal_mode = WAL` for concurrent reads during writes.
- Add explicit `index_durability_mode = cache | durable` (store-local setting).
  - cache (default): `synchronous = NORMAL`. SQLite is treated as rebuildable cache.
    Correctness and client durability promises derive from WAL segments + replay.
  - durable (opt-in): `synchronous = FULL`. Stronger persistence for receipts/index,
    at a cost in tail latency.
  In both modes, open-time index catch-up + origin_seq recompute is mandatory.
- `cache_size = -16000` (16 MiB cache) or configurable.
- `busy_timeout = 5000` (or configurable) to avoid spurious SQLITE_BUSY under load.
- `foreign_keys = ON` (defensive, even if schema uses few FKs initially).
- Document these choices explicitly; do not rely on SQLite defaults.

### 6.5 Crash-consistency contract (normative)

The daemon MUST preserve the following ordering for any locally-authored mutation:

Local append ordering:
1) Allocate origin_seq inside a SQLite write transaction.
2) Write WAL record bytes to the active segment.
3) If durability >= LocalFsync: fsync the segment file (and fsync directory only on new segment creation).
4) Record event offsets + sha + prev_sha in SQLite (same transaction) and COMMIT.
5) Apply event to in-memory state.
6) Update applied watermark (in memory) and persist watermarks + receipt finalization in SQLite.
7) Only after step (6) may a success response be returned.

Failure handling:
- If failure occurs before step (3): no durability is promised; WAL tail may be truncated on restart.
- If failure occurs after step (3) but before step (4): WAL may contain a durable record not indexed.
  This is expected in `index_durability_mode=cache`. Startup MUST catch up/rebuild the index
  from WAL before accepting writes to avoid origin_seq reuse and to restore idempotency lookups.
- If failure occurs after step (4) but before step (5): restart replay MUST apply the event.
- If failure occurs after step (5) but before step (6): event is applied in memory but receipt may be missing.
  Startup reconciliation MUST finalize receipts and watermarks deterministically.

Serve-traffic gate:
- The daemon MUST NOT accept mutation requests until WAL verification + index catch-up + replay complete,
  unless explicitly started in read-only / maintenance mode.

---

## 7) Store runtime in daemon (`src/daemon/*`)

Add a per-store runtime struct that owns state, WAL, watermarks, and coordination.

### 7.1 StoreRuntime shape (locked)

```rust
pub struct StoreRuntime {
  meta: StoreMeta,
  policies: NamespacePolicySet,
  state: StoreState,
  wal: EventWal,
  wal_index_reader: Box<dyn WalIndexReader>, // fast WANT/admin reads without blocking coordinator
  watermarks_applied: WatermarkMap,
  watermarks_durable: WatermarkMap,
  /// Fast chain heads: last durable sha per (ns, origin). Used for prev_sha fill
  /// and for gap validation without decoding envelope bytes.
  head_sha: BTreeMap<NamespaceId, BTreeMap<ReplicaId, [u8; 32]>>,
  gc: GcRuntime, // retention enforcement + WAL pruning + receipt GC
  gap_buffer: GapBufferByNsOrigin,
  peer_acks: PeerAckTable,
  broadcaster: EventBroadcaster,
  idempotency: IdempotencyStore,
  checkpoint: CheckpointRuntime, // worker thread + job queue + last results
  scrubber: ScrubberRuntime,     // periodic WAL/index health checks + metrics
  admission: AdmissionController, // global priority + overload shedding (new)
}
```

### 7.2 Daemon integration

- `Daemon` becomes keyed by StoreId instead of RemoteUrl.
- Maintain `path_to_store_id` mapping for repo-bound operations.
- Maintain `remote_to_store_id` mapping for Git lane compatibility.
- Keep git-specific RepoState keyed by store id and checkpoint group.
- `core.rs` remains the serialization point for apply/ingest.

### 7.3 Startup flow (store open)

1) Determine StoreId using discovery order (spec 2.1.1):
   - local store dir meta.json
   - refs/beads/meta store_meta.json
   - refs/beads/<store_id>/* listing (manual selection if multiple)
   - UUIDv5(normalized_remote_url) fallback for migration only
2) Ensure store dir exists and acquire store-global lock.
3) Load or create StoreMeta (replica_id stable on disk).
4) Load namespaces.toml and validate policies (fail fast).
4.1) Compute policy_hash and roster_hash (if replicas.toml exists) and store in StoreRuntime.
5) Open WAL and SQLite index; rebuild index if needed.
6) Verify filesystem WAL segments before serving traffic (normative):
   - For every wal/<namespace>/segment-*.wal file, read the segment header and verify:
     - store_id/store_epoch match StoreMeta,
     - namespace in header matches directory,
     - wal_format_version supported.
   - On mismatch, fail fast with an operator-facing remediation (archive or delete).
7) Recover baseline state (normative, deterministic):
   - Preferred: import latest verified local checkpoint cache for each group (if present).
     If cache meta.json policy_hash/roster_hash mismatch local, surface warning before serving writes.
   - Else: import latest Git checkpoint heads for configured groups (if available).
   - Else: migration import (legacy git ref / legacy snapshot WAL) into namespace "core".
   - Else: start empty.
   - Merge multiple imported baselines deterministically via CRDT join per namespace.
   - Initialize applied+durable watermarks to the merged max(meta.included) for each
     imported namespace/origin.
8) Replay WAL to reach "now" (normative):
   - For each namespace, scan/index segments as needed, then apply only events with
     origin_seq > baseline watermark (per origin) in contiguous order.
   - Advance applied and durable watermarks as events are applied and fsync-confirmed.
9) Start IPC server, replication listener, replication manager, checkpoint scheduler.

### 7.4 Identity discovery and repo binding

Replace or augment current resolve_remote():
- Primary: fetch refs/beads/meta and read store_meta.json.
- Fallback: list refs/beads/<store_id>/*.
- Migration-only fallback: UUIDv5(normalized_remote_url).
- Persist mapping path -> StoreId; do not rekey on URL changes.

---

## 8) Mutation pipeline refactor (events-first)

Add a MutationEngine to transform ops into events.

### 8.1 Mutation planning

MutationEngine responsibilities:
- Validate inputs and CAS conditions.
- Determine effective ActorId:
  - request.actor_id if present (and permitted by IPC policy),
  - else daemon default actor id.
- Mint txn_id and stamps using daemon clock.
- Mint stamps using per-ActorId HLC state and populate EventEnvelope.hlc_max.
- Build exactly one EventEnvelope (kind=TxnV1) per client mutation request.
  If the request would exceed MAX_WAL_RECORD_BYTES, return an explicit error
  instructing the client to split the operation into multiple requests.
- Enforce content-level limits before encoding:
  - reject note_appends with content > MAX_NOTE_BYTES,
  - reject txns with total ops > MAX_OPS_PER_TXN,
  - enforce per-bead label count and other bounded fields.
- Support client_request_id idempotency.

### 8.2 Event sequencing and idempotency

- origin_seq allocated at WAL append, not in memory.
- If client_request_id exists in WAL index, return prior receipt without new events.
- MutationEngine MUST compute request_sha256 from a canonicalized mutation request representation
  (retry-stable; excludes durability wait knobs) and pass it to WAL append so it is persisted in:
  - SQLite client_requests.request_sha256
  - WAL record header (has_request_sha256)

### 8.3 Apply pipeline (ordered steps)

1) Validate input and namespace policy.
2) Idempotency check (client_request_id).
3) Build one event draft (no seq, no hash).
4) WAL append + fsync (per durability class).
5) Apply events to state via core::apply_event.
6) Update query indexes and in-memory derived indexes.
7) Notify subscribers (if enabled).
8) Trigger replication and checkpoint scheduling.

### 8.4 Write stamp generation (explicit)

Write stamps are minted from HLC clock:
- monotonic per actor
- total order: (physical_ms, logical_counter, actor_id)

event_time_ms MUST equal physical wall time used to mint the stamp.

HLC persistence integration:
- When a mutation is appended (LocalFsync boundary), persist updated HLC state
  in the same critical section as WAL append/index commit.

---

## 9) Replication subsystem (`src/daemon/repl/*`)

Replication uses framed CBOR messages (same framing as WAL).

### 9.1 Modules

- frame.rs: length + crc32c framing (shared with WAL format)
- proto.rs: typed message schemas (HELLO, WELCOME, EVENTS, ACK, WANT, ERROR, SNAPSHOT_*)
- session.rs: per-connection state machine
- manager.rs: outbound peers, reconnect/backoff, event fanout
- server.rs: listener and accept loop
- snapshot.rs: snapshot request/response handling

### 9.2 Message envelope

CBOR map `{ v, type, body }` with `minicbor`.
Event payloads SHOULD embed canonical EventEnvelope bytes (see EventBytes/EventFrameV1);
decoding into EventEnvelope structs is required only for apply and validation.
Hash verification remains mandatory.

### 9.3 Handshake and protocol negotiation

HELLO body:
- protocol_version
- min_protocol_version
- store_id
- store_epoch
- sender_replica_id
- hello_nonce
- max_frame_bytes (sender limit)
- requested_namespaces
- offered_namespaces
- seen_durable (per requested namespace)  // contiguous durable watermark map
- seen_applied? (per requested namespace) // optional; helps peers reason about read lag
- capabilities (supports_snapshots, supports_live_stream, supports_compression)

WELCOME body:
- protocol_version (negotiated)
- store_id
- store_epoch
- receiver_replica_id
- welcome_nonce
- accepted_namespaces
- receiver_seen_durable (per accepted namespace)
- receiver_seen_applied? (per accepted namespace)
- live_stream_enabled
- max_frame_bytes (effective limit)

Version negotiation:
- negotiated version v = min(max_a, max_b) if v >= max(min_a, min_b); else incompatible.
- effective frame limit = min(sender.max_frame_bytes, receiver.max_frame_bytes).

### 9.4 EVENTS

Body:
- events: [EventFrameV1]

EventFrameV1:
- eid: EventId
- sha256: bytes(32)
- prev_sha256?: bytes(32)  (omitted for seq==1)
- bytes: bytes  (canonical CBOR EventEnvelope bytes, including sha256/prev_sha256)

Rules:
- Sender MAY batch.
- Sender MUST send events for a given (namespace, origin_replica_id) in strictly
  increasing origin_seq order.
- Receiver MUST validate:
  - store_id/store_epoch match
  - sha256 matches envelope content
  - if prev_sha256 present, it MUST match the receiver's current head sha for that
    (namespace, origin_replica_id) before buffering/appending
  - if event_id already exists, sha256 matches prior observed sha256
- Receiver applies idempotently, buffers gaps, and advances seen_map only when
  contiguous.

Ingestion ordering rule (clarified, normative):
- Receiver MUST NOT append to WAL unless the event is contiguous for that
  (namespace, origin_replica_id): origin_seq == durable_seen + 1.
- If origin_seq > durable_seen + 1, receiver MUST buffer (bounded) and issue WANT.
- Only after missing prefix arrives and prev_sha256 continuity can be verified
  may buffered events be appended and applied in order.
- Remote ingest MUST call EventWal::ingest_remote_bytes (not append_local) so
  origin_seq is preserved and canonical bytes are persisted unchanged.

### 9.5 ACK

Body:
- durable: { namespace -> { origin_replica_id -> u64 } }
- applied?: { namespace -> { origin_replica_id -> u64 } }

Rules:
- durable advances only after WAL fsync or snapshot/checkpoint that guarantees replay.
- applied advances only after apply_event.
- Both vectors monotonic per (namespace, origin).
- ACKs may be standalone or piggybacked.

### 9.6 WANT

Body:
- want: { namespace -> { origin_replica_id -> u64 } } meaning "send seq > this"

Rules:
- Respond with EVENTS until satisfied.
- Must use WAL index to avoid O(total WAL) scans.
- If WAL cannot serve the requested range (pruned or missing), sender MUST respond with
  SNAPSHOT_REQUIRED (new) and MUST NOT silently stall.

SNAPSHOT_REQUIRED body (new):
- namespaces
- reason: "range_pruned" | "range_missing" | "over_limit"
- suggested_included: { namespace -> { origin_replica_id -> u64 } }  // best-known durable included
- suggested_included_heads: { namespace -> { origin_replica_id -> sha256_hex } }  // chain anchors
- max_snapshot_bytes (sender limit)

### 9.7 Snapshot messages (recommended; normative for anchors and for WANT range gaps)

SNAPSHOT_REQUEST body:
- namespaces
- min_included? (watermarks)
- resume_from_chunk? (default 0)

SNAPSHOT_BEGIN body:
- snapshot_id
- namespaces
- included (watermarks)
- encoding: "checkpoint_tar_zstd_v1"
- chunk_size_bytes (default 1 MiB)
- total_chunks
- sha256 (hash of full snapshot byte stream)

SNAPSHOT_CHUNK body:
- snapshot_id
- chunk_index
- bytes

SNAPSHOT_END body:
- snapshot_id

Rules:
- Hash must be verified before applying snapshot.
- Resumability rule (normative):
  - If resume_from_chunk is supported, the sender MUST materialize the full snapshot
    byte stream once (temp file under store_dir/snap/) and stream chunks from that file.
  - The sender MUST NOT regenerate a new tar/zstd stream for the same snapshot_id.
  - If the temp artifact is missing/expired, the sender MUST respond with
    ERROR(code="snapshot_expired") and require restart from chunk 0.
- Resource limits (normative):
  - The sender MUST enforce MAX_SNAPSHOT_BYTES before announcing total_chunks.
  - The receiver MUST enforce MAX_SNAPSHOT_BYTES and reject early (before allocation)
    if total_chunks * chunk_size_bytes exceeds limit.

### 9.8 Gap handling and flow control

Gap handling:
- seen_durable_map tracks highest contiguous durable seq per (namespace, origin).
- If origin_seq > seen_durable_map + 1, buffer and request WANT.
- Gap buffer MUST be bounded (defaults: 10k events or 10 MiB per connection).
- If bounds exceeded or gaps do not resolve in time, drop and re-handshake.

Flow control:
- Sender must bound in-flight events (max 10k or 10 MiB).
- ACKs advance sender windows.
- Keep fairness across namespaces to avoid starvation.

Fair scheduling (normative defaults):
- Add bounds:
  - MAX_EVENTS_PER_ORIGIN_PER_BATCH default 1024
  - MAX_BYTES_PER_ORIGIN_PER_BATCH default 1 MiB
- Sender MUST construct each EVENTS batch by round-robin over eligible (namespace, origin_replica_id)
  pairs, respecting per-origin caps, so that a single hot origin cannot starve others.

Rate limiting (normative):
- Receiver MUST enforce MAX_REPL_INGEST_BYTES_PER_SEC if configured, and apply backpressure by
  stopping reads or responding ERROR(code="overloaded") and closing the connection.

Keepalive:
- Send PING if no traffic for KEEPALIVE_MS.
- Drop connection after DEAD_MS of silence.

PING/PONG:
- PING body: nonce
- PONG body: nonce

Compression safety (normative):
- If supports_compression is enabled (e.g., zstd), every compressed frame MUST
  carry its uncompressed_length, and the receiver MUST enforce:
  - uncompressed_length <= MAX_FRAME_BYTES
  - cumulative uncompressed bytes per batch <= MAX_EVENT_BATCH_BYTES
  Reject frames that violate bounds before allocating full buffers.

### 9.9 Replication scope and policies

Replicate only namespaces where policy requires it:
- replicate=none: no send/receive
- replicate=anchors: only anchors
- replicate=peers: connected peers
- replicate=p2p: discovered peers (transport only; correctness unchanged)

### 9.10 Error handling (explicit)

- Store id mismatch: close connection with ERROR(code="wrong_store").
- Store epoch mismatch: reject and require snapshot/bootstrap; do not merge state.
- Equivocation: same EventId with different sha is protocol corruption; terminate session.

---

## 10) Durability coordination

Add a DurabilityCoordinator to track pending txns and completion conditions.

### 10.1 Durability classes

- LocalFsync: complete after WAL fsync + apply.
- ReplicatedFsync(k): wait for durable ACKs from k eligible replicas.
- GitCheckpointed: wait for a pushed checkpoint that includes events.

### 10.2 Replica selection for k

- replicate=anchors: choose anchors
- replicate=peers: choose connected peers
- replicate=none: ReplicatedFsync invalid (fail fast)

Fail-fast availability (normative):
- If requested ReplicatedFsync(k) and the eligible configured replica set size < k,
  return ERROR(code="durability_unavailable") immediately (do not wait).
- If eligible set size >= k but fewer than k are currently connected, the daemon MAY wait
  up to timeout and then return retryable timeout with receipt.

### 10.3 Container rule (client mode)

Ephemeral containers:
- Must submit mutations to durable replica.
- Must not be counted as durable copies.

### 10.4 Optional Git durable copy

If durable_copy_via_git=true:
- Git checkpoint push may count as one durable copy if:
  - namespace persist_to_git=true
  - coordinator is primary checkpoint writer
  - push succeeds and ref advanced
  - checkpoint meta.included covers events
- Must record local inclusion proof mapping txn_id (or event range) to commit id
  and included watermarks to avoid double counting on retries.

### 10.5 Receipt and retry semantics

- On success: return DurabilityReceipt.
- On timeout: return retryable error including receipt.
- require_min_seen for reads and subscriptions MUST compare against applied watermarks.

---

## 11) Local subscriptions (IPC streaming)

Add Subscribe IPC request:
- streams EventEnvelope objects (canonical bytes optional)
- supports require_min_seen (wait or return retryable error)
- separate from materialized query responses (views remain the query path)

---

## 12) Snapshot bootstrap (realtime)

Use checkpoint layout for realtime snapshots:
- Build checkpoint tree in temp dir (shared builder).
- Package as tar + zstd.
- Send SNAPSHOT_BEGIN with hash and chunk count.
- Stream SNAPSHOT_CHUNK frames.
- Verify hash, extract, import, merge, advance watermarks.
- Keep the snapshot artifact until completion (SNAPSHOT_END) or TTL expiry so resume works.

Deterministic snapshot artifact (recommended, correctness-preserving):
- The sender SHOULD reuse a cached deterministic tar.zst produced at checkpoint export time.
- Deterministic tar requirements:
  - files enumerated in UTF-8 path byte order ascending,
  - mtime = 0 for all entries,
  - uid/gid = 0, uname/gname empty,
  - stable permission bits (e.g., 0644 for files, 0755 for dirs),
  - no PAX time fields.
- The snapshot sha256 in SNAPSHOT_BEGIN SHOULD equal the cached artifact hash.

---

## 13) Git lane becomes checkpoint lane (`src/git/checkpoint/*`)

### 13.1 New checkpoint modules

Add `src/git/checkpoint/`:
- layout.rs: sharded JSONL layout for state/tombstones/deps per namespace
- manifest.rs: deterministic file list and hashes
- meta.rs: checkpoint meta (included watermarks, content hash, manifest hash)
- export.rs: build checkpoint tree from StoreState
- import.rs: parse, verify, and merge into StoreState

### 13.2 Git refs

- Discovery ref: `refs/beads/meta` (contains store_meta.json)
- Checkpoint refs: `refs/beads/<store_id>/<group>`
- Legacy ref: `refs/heads/beads/store` (read-only during migration)

### 13.3 Checkpoint content format (normative)

Each checkpoint commit tree contains:
- meta.json (checkpoint metadata)
- manifest.json (deterministic list + hashes)
- namespaces/<ns>/state/<shard>.jsonl
- namespaces/<ns>/tombstones/<shard>.jsonl
- namespaces/<ns>/deps/<shard>.jsonl

Sharding rule:
- 256 shards `00.jsonl` .. `ff.jsonl` under each directory.
- Missing shards are empty.
- Shard assignment:
  - beads: first byte of sha256(id_bytes)
  - tombstones: first byte of sha256(id_bytes)
  - deps: first byte of sha256(dep_key_bytes) where dep_key_bytes = from "\0" to "\0" kind
- Within each shard, JSONL lines sorted by key:
  - beads by id
  - tombstones by id
  - deps by (from,to,kind)

JSON encoding requirements:
- UTF-8
- stable key ordering for objects (canonical JSON, see below)
- no extra whitespace
- each JSONL line ends with "\n"

Canonical JSON (normative):
- All JSON objects MUST be serialized with keys sorted by UTF-8 byte order ascending,
  recursively for nested objects.
- Writers MUST NOT rely on HashMap iteration order at any layer.
- Introduce a shared helper used by checkpoint export and manifest/meta writers:
  - `src/core/json_canon.rs` (or `src/git/checkpoint/json_canon.rs`)
  - API sketch: `fn to_canon_json_bytes<T: Serialize>(v: &T) -> Vec<u8>`
- Canonical JSON MUST emit:
  - no insignificant whitespace,
  - deterministic escaping as per serde_json,
  - no NaN/Infinity floats (reject if encountered).

### 13.4 manifest.json (normative)

manifest.json fields:
- checkpoint_group
- store_id
- store_epoch
- namespaces (sorted)
- files: { "<path>": { "sha256": "<sha256-hex>", "bytes": <u64> } }
  for each file present excluding meta.json and manifest.json

Missing shards are treated as empty and therefore do not appear in files.
manifest_hash MUST be computed over canonical JSON bytes (not "pretty" JSON).

### 13.5 meta.json (required fields)

meta.json fields:
- checkpoint_format_version
- store_id
- store_epoch
- checkpoint_group
- namespaces (included)
- created_at_ms
- created_by_replica_id
- policy_hash  // sha256 over canonical policy representation (namespaces.toml after validation)
- roster_hash? // sha256 over canonical replicas.toml if present (else omitted)
- included: { namespace -> { origin_replica_id -> u64 } }
- included_heads: { namespace -> { origin_replica_id -> sha256_hex } }
  // sha256 at the included watermark for chain continuity after WAL pruning.
  // Required for namespaces where WAL pruning is enabled.
- content_hash (sha256 of canonical meta preimage bytes; see algorithm below)
- manifest_hash (sha256 of manifest.json bytes)

content_hash algorithm (non-recursive, normative):
1) Write all shard files. Compute each file's sha256 and byte length.
2) Write manifest.json (Canonical JSON) and compute:
   manifest_hash = sha256(manifest.json bytes) in lowercase hex.
3) Build a meta preimage object equal to meta.json but with `content_hash` omitted.
4) Serialize the meta preimage using Canonical JSON.
5) content_hash = sha256(meta_preimage_bytes) in lowercase hex.

Import rule:
- Recompute manifest_hash; verify each file hash (and optionally size) listed in manifest.
- Recompute content_hash from the meta preimage; reject if mismatch.
- If local policy_hash differs from checkpoint policy_hash, daemon MUST emit a warning
  and SHOULD surface it in admin.status/admin.doctor. Strict mode MAY reject import.
- If roster_hash is present and differs, daemon MUST emit a warning (durability semantics may differ).

### 13.6 Checkpoint writer selection

Each group defines:
- checkpoint_writers (replica_id list)
- primary_checkpoint_writer

Only primary writer must auto-push.
Other writers may push only on override or failover configuration.

### 13.7 Checkpoint scheduling (normative defaults)

Each group has:
- debounce_ms default 200
- max_interval_ms default 1000
- max_events default 2000

Rules:
- When an event is applied in any included namespace, group becomes dirty.
- Dirty groups schedule a checkpoint:
  - after debounce_ms since last event, or
  - when max_interval_ms since first dirtied elapses, or
  - when max_events new events included
- Only one checkpoint push in flight per group. If new events arrive during push,
  they are included in the next scheduled checkpoint.

### 13.8 Export algorithm (normative)

1) Materialize converged state for included namespaces.
2) Incremental export (normative):
   - Track dirty shards per namespace in StoreRuntime using ApplyOutcome.
   - Regenerate only dirty shard files since the last exported checkpoint for the group.
   - Reuse previous checkpoint blobs for unchanged shard paths.
   - Determinism rule remains: regenerated shard JSONL lines MUST be sorted as specified.
3) Snapshot barrier (normative):
   - The coordinator thread MUST capture:
     - included watermarks (durable) for the group
     - a consistent view of the shard contents to be written (full or dirty-only)
   - This captured payload (CheckpointSnapshot) MUST be immutable and handed to a
     checkpoint worker thread for file I/O, hashing, commit creation, and push.
   - The snapshot MUST NOT claim inclusion of watermarks that are not represented
     by the shard contents in that snapshot.
4) Write manifest.json.
5) Write meta.json including included watermarks (durable), manifest_hash, content_hash.
6) Create commit on group ref.
7) Push.
8) Cache the exported checkpoint locally (recommended, operational):
   - Store a verified copy (or hardlinks) under the store dir cache path.
   - Keep only the last N checkpoints per group (default N=3), prune older caches
     after successful newer writes.
   - Cache publish MUST be atomic:
     - write into `checkpoint_cache/<group>/.tmp/<checkpoint_id>/`
     - fsync files, then rename to `checkpoint_cache/<group>/<checkpoint_id>/`
     - write `<checkpoint_id>.tar.zst` as `<checkpoint_id>.tar.zst.tmp` then rename
     - fsync `checkpoint_cache/<group>/` directory after publishes
     - update `checkpoint_cache/<group>/CURRENT` (write tmp + rename) to point to the
       newest verified checkpoint_id
9) Also materialize a deterministic snapshot artifact (recommended):
   - Write `<checkpoint_id>.tar.zst` alongside the cached tree.
   - Record its sha256 in meta.json (optional field `snapshot_hash`).
   - This artifact is used by SNAPSHOT_* serving to avoid re-tarring/re-compressing.

### 13.9 Import / merge algorithm (normative)

1) Fetch remote head for each checkpoint group.
2) Parse checkpoint snapshot.
3) Verify manifest_hash and content_hash; reject if mismatch.
3.1) Verify included_heads is present for any namespace where pruning is enabled; reject if missing.
3.2) Seed StoreRuntime head_sha for included origins from included_heads where WAL boundary
     events are not available locally (post-prune replay anchor).
4) Merge into local state deterministically.
5) Advance durable watermarks to at least meta.included for included namespaces.
6) If local state changed and replica is checkpoint writer, export new checkpoint.

### 13.9.1 Import hardening (normative)

Snapshot tar extraction safety:
- MUST enforce MAX_SNAPSHOT_BYTES on total extracted bytes (not just archive size).
- MUST enforce MAX_SNAPSHOT_ENTRIES (default 200,000) on entry count.
- MUST enforce MAX_SNAPSHOT_ENTRY_BYTES (default 64 MiB) per entry.
- MUST reject entries with path traversal (../) or absolute paths.
- MUST reject symlinks (do not follow; reject archive).
- MUST reject device files, fifos, or other non-regular files.
- MUST extract to temp directory first, then validate manifest/hashes before moving.

JSONL shard parsing safety:
- MUST use streaming JSONL parser (line-by-line, not full-file buffer).
- MUST enforce MAX_JSONL_LINE_BYTES (default 4 MiB) per line.
- MUST enforce MAX_JSONL_SHARD_BYTES (default 256 MiB) per shard file.
- MUST reject lines that are not valid UTF-8 or valid JSON.
- MUST reject JSON objects exceeding schema-expected nesting depth (default 32).
- MUST count and bound total entities per shard (implicit via MAX_SNAPSHOT_ENTRIES overall).

Validation sequence:
1) Extract to temp dir with all safety checks above.
2) Verify manifest_hash over extracted manifest.json bytes.
3) Verify each file hash in manifest.
4) Verify content_hash over entire tree.
5) Parse and validate each JSONL shard within limits.
6) Only then move/merge into store state.

### 13.10 Multi-writer safety

If push fails (non-FF):
1) fetch latest head
2) import + merge
3) re-export new checkpoint commit
4) retry push with bounded attempts/backoff

Correctness must not depend on a single writer, even if recommended.

---

## 14) Scheduler integration

Add a checkpoint scheduler:
- Debounce after local mutations.
- Max interval and max events per spec.
- One in-flight push per checkpoint group.
- Heavy checkpoint work (export, hash, commit, push) SHOULD run in a dedicated
  worker thread. The coordinator owns the snapshot barrier and only submits
  immutable jobs, so mutation/replication latency remains stable.

Separate replication reconnect/backoff logic from checkpoint scheduling.

---

## 15) Config and path layout

### 15.1 Store directory layout

```
$BD_DATA_DIR/stores/<store_id>/
  meta.json
  namespaces.toml
  replicas.toml
  wal/<namespace>/*.wal
  index/wal.sqlite
  snap/
  checkpoint_cache/<group>/<checkpoint_id>/
  checkpoint_cache/<group>/<checkpoint_id>.tar.zst
  checkpoint_cache/<group>/CURRENT
```

Update `src/paths.rs`:
- add helpers for stores_dir(), store_dir(store_id), store_meta_path(store_id)
- maintain BD_DATA_DIR override

Filesystem safety rules (new, normative):
- Store directories MUST be created with mode 0700 (or platform equivalent).
- Sensitive files (meta.json, replicas.toml, namespaces.toml, wal.sqlite, PSK material) MUST be 0600.
- When opening critical paths, daemon MUST reject symlinks:
  - meta.json
  - index/wal.sqlite
  - wal/<namespace>/
  This prevents writing outside store_dir via symlink attacks/misconfig.

### 15.2 Config additions

Add configuration for:
- replica_id stability and store identity persistence
- namespace policy defaults
- checkpoint groups
- replication peers, listen address, max frame size, timeouts

---

## 16) IPC, API, and CLI changes

### 16.1 IPC (`src/daemon/ipc.rs`)

Add to all mutation requests:
- namespace (optional, default "core")
- durability (optional, default LocalFsync)
- client_request_id (optional UUID)
- actor_id (optional, default daemon actor; used for stamps and attribution)

Response changes:
- include DurabilityReceipt on success
- on timeout waiting for remote durability, return retryable error including receipt

IPC_PROTOCOL_VERSION bump only when adding streaming responses.

Add to all read/query requests (normative):
- require_min_seen: Option<WatermarkMap>  // applied watermark gating for read-your-writes
- wait_timeout_ms: Option<u64>            // bounded wait; default small; 0 = no wait

Read gating semantics (normative):
- If require_min_seen is not satisfied:
  - the daemon MAY wait up to wait_timeout_ms for apply to catch up, else
  - return a retryable error including current applied watermarks.

Add admin/introspection ops:
- `admin.status`: returns store_id, replica_id, namespaces, applied/durable watermarks,
  WAL segment stats (count/bytes), index health (last_indexed_offset per segment),
  replication sessions (peers, last_ack, lag), checkpoint groups (dirty/in-flight/last push).
- `admin.flush`: forces fsync for a namespace (and optionally triggers checkpoint now).
- `admin.rebuild_index`: rebuilds SQLite index from WAL (explicit operator action).
- `admin.metrics`: returns a simple text or JSON snapshot of key counters and
  latencies (append/fsync p50/p95, queue depths, replication lag per peer/ns,
  checkpoint durations, GC sweep stats).
- `admin.verify_store`: runs offline-ish verification checks (segment headers match
  meta, content_hash recompute for last checkpoint, index vs WAL spot checks).
- `admin.scrub_now`: performs a bounded integrity scrub while the daemon is live:
  - samples up to N events per namespace (default N=200, configurable),
  - verifies WAL CRC32c and sha256 (preimage) correctness,
  - verifies SQLite offsets point to valid record_magic and header_len,
  - optionally verifies the newest checkpoint_cache entries (manifest_hash + content_hash).
  Returns structured pass/warn/fail results similar to admin.doctor.
- `admin.doctor`: runs a bounded health scrub and returns a deterministic report:
  - machine-readable JSON with:
    - checks: [{ id, status, severity, evidence, suggested_actions }]
    - summary: { risk: low|medium|high|critical,
                 safe_to_accept_writes: bool,
                 safe_to_prune_wal: bool,
                 safe_to_rebuild_index: bool }
  - MUST include explicit remediation actions (exact admin ops to run, in order).
- `admin.gc_now`: triggers retention enforcement + receipt GC for selected namespaces.
- `admin.prune_wal`: runs WAL pruning evaluation with dry_run option and explicit
  acknowledgement gates (never prune implicitly via IPC).
- `admin.reload_policies`: reloads namespaces.toml, validates, applies safe live changes,
  and returns a diff summary (applied vs requires_restart).
- `admin.rotate_replica_id`: generates a new replica_id, updates meta.json, logs the
  old -> new mapping. Used for recovery from duplicated store directories.
- `admin.fingerprint`: returns stable convergence fingerprints:
  - per-namespace: { namespace -> { state_sha256, tombstones_sha256, deps_sha256 } }
  - includes watermarks_applied and watermarks_durable used as the basis
  - supports modes:
    - full (may be expensive)
    - sample(N) (bounded; good for periodic monitoring)
  Intended use: quickly detect divergence across replicas and validate repairs.

Observability requirements (normative):
- Every mutation MUST be traceable end-to-end via txn_id and (if present) client_request_id.
- Replication sessions MUST log peer identity (replica_id + authenticated identity) and
  namespace authorization decisions.
- Emit at minimum:
  - counters: wal_append_ok/err, wal_fsync_ok/err, apply_ok/err, repl_events_in/out,
    checkpoint_export_ok/err, snapshot_send_ok/err, scrub_ok/err, scrub_records_checked
  - gauges: IPC inflight, repl ingest queue bytes/events, checkpoint job queue depth,
    per-peer lag (durable watermark diff)
  - histograms: wal_append_duration, wal_fsync_duration, apply_duration, checkpoint_duration

Graceful shutdown (normative):
- On SIGTERM / shutdown request: stop accepting new mutation requests, drain in-flight
  durability waits (bounded), flush WAL, close segments, then exit.

### 16.2 API (`src/api/mod.rs`)

Expose:
- namespace in issue summaries and issue views
- durability receipt types in JSON mode
- replication status fields (watermarks, warnings)

### 16.3 CLI

Add global:
- --namespace

Add mutation flags:
- --durability
- --client-request-id

Update handlers to pass through IPC.

---

## 17) Migration strategy

### 17.1 Legacy WAL snapshot

- Rename old wal.rs to wal_legacy_snapshot.
- On store open during migration:
  - read legacy snapshot WAL (if present)
  - merge into StoreState["core"]
  - proceed with new event WAL thereafter

### 17.2 Legacy Git ref

- Import refs/heads/beads/store into StoreState["core"].
- Export first checkpoint to new ref.
- Mirror legacy ref for one release if needed.
- Keep legacy wire format reader for migration only.

### 17.3 Identity migration

- Prefer refs/beads/meta for store identity.
- Only fall back to URL-derived identity as migration bootstrap.
- Persist store_id and replica_id locally; do not re-derive if remote URL changes.

Initial watermarks:
- After baseline import, set applied and durable to included watermarks
  for origins represented in baseline.

---

## 18) Testing and validation

Add deterministic tests for:
- Canonical CBOR hashing stability (bytes identical across runs)
- WAL framing and tail truncation
- WAL index rebuild from segments
- origin_seq allocation and idempotency receipts
- apply idempotence and note collision detection
- replication ACK/WANT behavior and backpressure
- checkpoint export/import determinism, manifest and content hashes
- store discovery order and identity persistence
- store-global lock enforcement
- admin.status correctness under load (watermarks/segment stats are consistent)
- graceful shutdown does not lose locally durable events
- fingerprint determinism (same state -> same sha256 across runs/platforms)

Add performance benchmarks (recommended, prevents regressions):
- WAL append throughput and tail latency under:
  - fsync per record
  - group commit (vary max latency/events/bytes)
- WAL index rebuild time vs WAL size (GiB scale)
- Replication WANT servicing latency under concurrent writes
- Checkpoint export wall time and CPU usage vs state size (beads/deps/notes)

Add rolling-upgrade compatibility tests (recommended):
- WAL replay compatibility across wal_format_version N-1 -> N
- checkpoint import compatibility across checkpoint_format_version N-1 -> N
- replication protocol negotiation matrix:
  - min/max protocol versions
  - capability mismatches (compression, snapshots)

Upgrade/rollback policy (normative):
- WAL format changes MUST support reading N-1 records during transition.
- Rollback is safe if no new-format records have been written:
  - Check wal_format_version in meta.json before downgrade.
  - If current WAL contains only N-1 compatible records, rollback is allowed.
  - If any N-only records exist, rollback requires WAL truncation or checkpoint restore.
- Checkpoint format N MUST import N-1 checkpoints without data loss.
- Replication peers negotiate min(max_version) >= max(min_version); incompatible peers
  disconnect cleanly with ERROR(code="version_incompatible").
- Document upgrade sequence in release notes: stop daemon, upgrade binary, start daemon.
  Rolling upgrades (mixed-version cluster) are supported if all versions share
  overlapping protocol_version ranges.

Add fuzzing + crash safety validation (recommended):
- cargo-fuzz targets:
  - WAL frame decode (length/crc/tail truncation)
  - CBOR EventEnvelope decode with bounds (depth, map entries, array entries)
  - replication message decode (HELLO/WELCOME/EVENTS)
- crash tests (integration):
  - inject SIGKILL during append (after write before fsync, after fsync before index commit, etc)
  - assert recovery behavior: no duplicate origin_seq, tail truncation correct, index rebuild correct

---

## 19) Recommended phase ordering

1) Core types + StoreState + receipts
2) Canonical CBOR + EventEnvelope + apply path
3) Event WAL + SQLite index + replay
4) Mutation engine (events-first)
5) Replication subsystem + ACK semantics
6) Git checkpoint lane
7) IPC streaming subscriptions + snapshot bootstrap

This ordering minimizes scope collapse and keeps the system operable as each layer lands.

---

## Appendix: Modules impacted (high-level map)

Core:
- `src/core/identity.rs` (newtypes)
- `src/core/namespace.rs`
- `src/core/watermark.rs`
- `src/core/event.rs`
- `src/core/durability.rs`
- `src/core/apply.rs`
- `src/core/state.rs` or `src/core/store_state.rs`
- `src/core/store_meta.rs`

Daemon:
- `src/daemon/core.rs` (store runtime, identity discovery, scheduler integration)
- `src/daemon/executor.rs` (thin wrapper over mutation engine)
- `src/daemon/ipc.rs` (namespace/durability/client_request_id and receipts)
- `src/daemon/wal_legacy_snapshot.rs` (renamed from wal.rs)
- `src/daemon/wal/*` (new)
- `src/daemon/repl/*` (new)
- `src/daemon/mutation_engine.rs` (new)
- `src/daemon/query.rs` (namespace-aware query routing)
- `src/daemon/store_runtime.rs` (store open + lock + runtime)

Git:
- `src/git/checkpoint/*` (new)
- `src/git/sync.rs` (legacy import path during migration)
- `src/git/wire.rs` (legacy wire; checkpoint wire may reuse or supersede)

CLI/API:
- `src/cli/mod.rs` + command handlers
- `src/api/mod.rs`

Config/paths:
- `src/config.rs`
- `src/paths.rs`

---

## Deferred: Authenticated replication (next phase)

Authenticated replication (ed25519 handshake signatures, replica roster authorization) is explicitly deferred to the next phase. The current plan assumes a trusted network boundary (tailnet). Auth will be added after the distributed database correctness is validated.
