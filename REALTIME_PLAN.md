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
name segments with a sortable prefix plus first seq: `segment-<created_at_ms>-<first_seq>.wal`
(and maybe a random suffix if you ever allow multiple writers, but you probably will not).

Rationale:
- Rebuild path needs deterministic ordering, and operators will look at filenames
  to sanity-check state. Including first seq makes range serving and tail recovery
  easier to debug.

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

CBOR decoding limits (normative):
- MAX_CBOR_DEPTH default 32 (prevents pathological nesting).
- MAX_CBOR_MAP_ENTRIES default 10,000 (prevents resource exhaustion during decode).
- MAX_CBOR_ARRAY_ENTRIES default 10,000.
- Reject payloads exceeding these limits before fully decoding.

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
- kind, delta
- sha256, prev_sha256

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

Event-time rule:
- `event_time_ms` MUST equal the physical wall time used to mint the write stamp
  for the mutation that produced this event.
- If delta contains multiple new stamps, event_time_ms MUST be >= the max wall time
  and SHOULD equal it.

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
- `DurabilityReceipt`:
  - store_id, store_epoch, txn_id, events: Vec<EventId>
  - requested_class, achieved_class
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
- `wal/<namespace>/segment-<created_at_ms>-<first_seq>.wal`
- `index/wal.sqlite`
- `snap/` for snapshot temp files

Segment header (raw bytes, little-endian where applicable):
- magic: 5 bytes = ASCII "BDWAL"
- wal_format_version: u32_le
- store_id: 16 bytes (UUID bytes)
- store_epoch: u64_le
- namespace_len: u32_le
- namespace_bytes: namespace_len bytes (UTF-8)
- created_at_ms: u64_le
- segment_id: 16 bytes (UUID bytes) // stable identity for indexing/repair
- flags: u32_le

Record framing:
- `u32_le length`
- `u32_le crc32c` (Castagnoli) computed over payload bytes
- payload: canonical CBOR bytes of EventEnvelope

Rotation triggers:
- max bytes (default 32 MiB)
- max age (default 60s)

Max record size:
- Must enforce MAX_WAL_RECORD_BYTES (default <= 16 MiB).
- Records larger than limit must be rejected at write time and error at read time.

Tail recovery:
- If EOF occurs mid-record, ignore partial tail and truncate to last valid boundary.
- If crc mismatch at tail, truncate to last valid boundary.
- If corruption is detected not at tail, fail with explicit error unless repair mode.

### 5.3 WAL segment naming and meaning of first_seq

Segment filename: `segment-<created_at_ms>-<first_seq>.wal`:
- `created_at_ms` from segment header.
- `first_seq` is the origin_seq of the first record written to the segment
  (from any origin). It is for operator visibility only.
- WAL index is authoritative for range serving and should not rely on filename
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

impl EventWal {
  pub fn open(store_dir: &Path, meta: &StoreMeta) -> Result<Self, WalError>;
  pub fn append(&mut self, ns: &NamespaceId, events: &[EventEnvelope]) -> Result<(), WalError>;
  pub fn fsync_namespace(&mut self, ns: &NamespaceId) -> Result<(), WalError>;
  pub fn stream_range(&self, ns: &NamespaceId, origin: &ReplicaId,
                      from_seq_exclusive: u64, max_bytes: usize)
    -> Result<Vec<EventEnvelope>, WalError>;
  pub fn max_origin_seq(&self, ns: &NamespaceId, origin: &ReplicaId) -> Result<u64, WalError>;
  pub fn replay(&self, ns: &NamespaceId) -> Result<(), WalError>;
}
```

Implementation details:
- `append` allocates origin_seq, computes canonical bytes + sha, writes frames,
  fsyncs per durability, records index entries, and returns assigned ids.
- `stream_range` reads canonical bytes from WAL and decodes (or streams bytes
  directly; still must validate hash).
- `replay` rebuilds in-memory state or index from WAL segments.

### 5.6 WAL append flow (authoritative)

1) Acquire per-namespace append lock.
2) Build canonical bytes and sha256 for each event.
3) Allocate origin_seq inside SQLite transaction (table-backed counter).
4) Write framed records to active segment.
5) If new segment created, fsync directory once.
6) For LocalFsync or stronger, fsync active segment file.
7) Record event offsets and metadata in SQLite and commit transaction.
8) Return assigned EventIds (and optionally updated watermarks).

### 5.7 WAL pruning and local snapshots

- WAL segments are authoritative; pruning only when a local snapshot `included`
  watermarks cover the segment and retention policy allows it.
- Keep WAL indefinitely on anchors if configured for audit.
- Local snapshots (compaction) are optional but must include included watermarks
  and GC floors, and must be versioned.

---

## 6) WAL index with SQLite (`src/daemon/wal/index.rs`)

SQLite is used as a rebuildable index, not the source of truth.

### 6.1 WalIndex trait (shape to lock)

```rust
pub trait WalIndex {
  fn record_event(&self, ns: &NamespaceId, eid: &EventId, sha: [u8; 32],
                  segment_path: &Path, offset: u64, len: u32,
                  event_time_ms: u64, txn_id: TxnId,
                  client_request_id: Option<ClientRequestId>)
    -> Result<(), WalError>;
  fn lookup_event_sha(&self, ns: &NamespaceId, eid: &EventId)
    -> Result<Option<[u8; 32]>, WalError>;
  fn iter_from(&self, ns: &NamespaceId, origin: &ReplicaId, from_seq_excl: u64,
               max_bytes: usize)
    -> Result<Vec<IndexedRangeItem>, WalError>;
  fn lookup_client_request(&self, ns: &NamespaceId, origin: &ReplicaId,
                           client_request_id: ClientRequestId)
    -> Result<Option<DurabilityReceipt>, WalError>;
  fn update_watermark(&self, ns: &NamespaceId, origin: &ReplicaId,
                      applied: u64, durable: u64) -> Result<(), WalError>;
}
```

Additional required index behaviors:
- `next_origin_seq(ns, local_replica_id)` for allocation inside append.
- Optional `record_receipt` for idempotency storage.

### 6.2 Minimal schema (example, spec-aligned)

```
CREATE TABLE events (
  namespace TEXT NOT NULL,
  origin_replica_id BLOB NOT NULL,
  origin_seq INTEGER NOT NULL,
  sha BLOB NOT NULL,
  segment_path TEXT NOT NULL, -- MUST be a store-relative path, not absolute
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
  PRIMARY KEY (namespace, origin_replica_id)
);

CREATE TABLE client_requests (
  namespace TEXT NOT NULL,
  origin_replica_id BLOB NOT NULL,
  client_request_id BLOB NOT NULL,
  txn_id BLOB NOT NULL,
  requested_class INTEGER NOT NULL,
  achieved_class INTEGER NOT NULL,
  min_seen BLOB NOT NULL,
  event_ids BLOB NOT NULL,
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
```

Notes:
- UUIDs stored as 16-byte BLOBs.
- min_seen and event_ids can be CBOR-encoded blobs.
  Under the "one request → one event" rule, event_ids is typically length 1,
  but remains a Vec for forward compatibility.
- meta should store schema version and store_id for sanity checks.

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
- `synchronous = NORMAL` for WAL mode (fsync at checkpoint, not every commit).
- `cache_size = -16000` (16 MiB cache) or configurable.
- Document these choices explicitly; do not rely on SQLite defaults.

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
  watermarks_applied: WatermarkMap,
  watermarks_durable: WatermarkMap,
  gap_buffer: GapBufferByNsOrigin,
  peer_acks: PeerAckTable,
  broadcaster: EventBroadcaster,
  idempotency: IdempotencyStore,
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
5) Open WAL and SQLite index; rebuild index if needed.
6) Load baseline state (legacy WAL snapshot, checkpoint import, or empty).
7) Start IPC server, replication listener, replication manager, checkpoint scheduler.

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
- Mint txn_id and stamps using daemon clock.
- Build exactly one EventEnvelope (kind=TxnV1) per client mutation request.
  If the request would exceed MAX_WAL_RECORD_BYTES, return an explicit error
  instructing the client to split the operation into multiple requests.
- Support client_request_id idempotency.

### 8.2 Event sequencing and idempotency

- origin_seq allocated at WAL append, not in memory.
- If client_request_id exists in WAL index, return prior receipt without new events.

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
Event payloads embed EventEnvelope (canonical bytes or decoded objects);
hash verification is mandatory.

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
- seen (per requested namespace)
- capabilities (supports_snapshots, supports_live_stream, supports_compression)

WELCOME body:
- protocol_version (negotiated)
- store_id
- store_epoch
- receiver_replica_id
- welcome_nonce
- accepted_namespaces
- receiver_seen (per accepted namespace)
- live_stream_enabled
- max_frame_bytes (effective limit)

Version negotiation:
- negotiated version v = min(max_a, max_b) if v >= max(min_a, min_b); else incompatible.
- effective frame limit = min(sender.max_frame_bytes, receiver.max_frame_bytes).

### 9.4 EVENTS

Body:
- events: [EventEnvelope]

Rules:
- Sender MAY batch.
- Sender MUST send events for a given (namespace, origin_replica_id) in strictly
  increasing origin_seq order.
- Receiver MUST validate:
  - store_id/store_epoch match
  - sha256 matches envelope content
  - if event_id already exists, sha256 matches prior observed sha256
- Receiver applies idempotently, buffers gaps, and advances seen_map only when
  contiguous.

Ingestion ordering rule (clarified, normative):
- Receiver MUST NOT append to WAL unless the event is contiguous for that
  (namespace, origin_replica_id): origin_seq == durable_seen + 1.
- If origin_seq > durable_seen + 1, receiver MUST buffer (bounded) and issue WANT.
- Only after missing prefix arrives and prev_sha256 continuity can be verified
  may buffered events be appended and applied in order.

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
- If WAL cannot serve the requested range, trigger snapshot bootstrap for the
  requested namespaces.

### 9.7 Snapshot messages (optional but recommended)

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

### 9.8 Gap handling and flow control

Gap handling:
- seen_map tracks highest contiguous seq per (namespace, origin).
- If origin_seq > seen_map + 1, buffer and request WANT.
- Gap buffer MUST be bounded (defaults: 10k events or 10 MiB per connection).
- If bounds exceeded or gaps do not resolve in time, drop and re-handshake.

Flow control:
- Sender must bound in-flight events (max 10k or 10 MiB).
- ACKs advance sender windows.
- Keep fairness across namespaces to avoid starvation.

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
- replicate=none: ReplicatedFsync invalid

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
- require_min_seen for reads/subscriptions must compare against applied map.

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
- stable key ordering for objects
- no extra whitespace
- each JSONL line ends with "\n"

### 13.4 manifest.json (normative)

manifest.json fields:
- checkpoint_group
- store_id
- store_epoch
- namespaces (sorted)
- files: { "<path>": "<sha256-hex>" } for each file present excluding meta.json and manifest.json

Missing shards are treated as empty and therefore do not appear in files.

### 13.5 meta.json (required fields)

meta.json fields:
- checkpoint_format_version
- store_id
- store_epoch
- checkpoint_group
- namespaces (included)
- created_at_ms
- created_by_replica_id
- included: { namespace -> { origin_replica_id -> u64 } }
- content_hash (sha256 of canonical checkpoint tree)
- manifest_hash (sha256 of manifest.json bytes)

content_hash algorithm:
1) Enumerate all checkpoint files including meta.json and manifest.json, sorted by
   UTF-8 path bytes ascending.
2) For each file, compute file_hash = sha256(file_bytes).
3) Build byte stream: path_bytes + "\0" + file_hash_hex + "\n" for each file.
4) content_hash = sha256(stream) in lowercase hex.

Import rule:
- Recompute manifest_hash and content_hash; reject if mismatch.

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
3) Write manifest.json.
4) Write meta.json including included watermarks (durable), manifest_hash, content_hash.
5) Create commit on group ref.
6) Push.

### 13.9 Import / merge algorithm (normative)

1) Fetch remote head for each checkpoint group.
2) Parse checkpoint snapshot.
3) Verify manifest_hash and content_hash; reject if mismatch.
4) Merge into local state deterministically.
5) Advance durable watermarks to at least meta.included for included namespaces.
6) If local state changed and replica is checkpoint writer, export new checkpoint.

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

Separate replication reconnect/backoff logic from checkpoint scheduling.

---

## 15) Config and path layout

### 15.1 Store directory layout

```
$BD_DATA_DIR/stores/<store_id>/
  meta.json
  namespaces.toml
  wal/<namespace>/*.wal
  index/wal.sqlite
  snap/
```

Update `src/paths.rs`:
- add helpers for stores_dir(), store_dir(store_id), store_meta_path(store_id)
- maintain BD_DATA_DIR override

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

Response changes:
- include DurabilityReceipt on success
- on timeout waiting for remote durability, return retryable error including receipt

IPC_PROTOCOL_VERSION bump only when adding streaming responses.

Add admin/introspection ops:
- `admin.status`: returns store_id, replica_id, namespaces, applied/durable watermarks,
  WAL segment stats (count/bytes), index health (last_indexed_offset per segment),
  replication sessions (peers, last_ack, lag), checkpoint groups (dirty/in-flight/last push).
- `admin.flush`: forces fsync for a namespace (and optionally triggers checkpoint now).
- `admin.rebuild_index`: rebuilds SQLite index from WAL (explicit operator action).

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
