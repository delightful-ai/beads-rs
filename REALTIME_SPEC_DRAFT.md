# Beads Replication and Persistence Spec

**Version:** v0.5 (supersedes v0.4)
**Status:** Normative unless marked "Implementation-defined"
**Audience:** You, future contributors, and "future-you at 3am"
**Scope:** Store format, WAL/event model, daemon-to-daemon replication protocol, client durability/ACK policies, Git checkpoint export/import, namespace policy semantics, and bootstrap/GC flows.

---

## 0. What changed from v0.4 (why these updates)

1. Hashing model simplified: events are hashed over raw `EventBody` CBOR bytes; `sha256`/`prev_sha256` live in WAL/replication headers (not inside the hashed CBOR payload).
2. Idempotency simplified: stable `(namespace, origin_replica_id, client_request_id)` mapping + receipt reconstruction (no PENDING/COMMITTED state machine).
3. Durability scope tightened: v0.5 supports `LOCAL_FSYNC` and `REPLICATED_FSYNC(k)` only; Git checkpoints are not a client-requestable wait target in v0.5.
4. Snapshot bootstrap over realtime is deferred in v0.5; bootstrap relies on checkpoints (Git or local cache).
5. Retention/GC markers and WAL pruning are deferred in v0.5 (policies are parsed/validated; enforcement is future work).
6. Checkpoint hashing clarified: `manifest.json` lists per-file sha256 + byte size and `content_hash` is computed over a canonical meta preimage (non-recursive). `included_heads` is optional in v0.5 (becomes required once WAL pruning is enabled).

---

## 0.1 Design goals (non-normative)

The system aims to be "better than Git" in semantics, not only in latency:

* Realtime push (no polling required for live updates).
* Durability knobs (ack after local fsync, or after multiple durable replicas).
* Selective replication by namespace, with clean opt-in later.
* Containers and ephemeral clients that never need to touch Git directly.
* Transport-agnostic realtime lane (tailnet now, p2p later).

## 0.2 Lane model (non-normative)

Git is a lane, not a temperature. For a given namespace, Git persistence and realtime streaming are orthogonal:

* Git lane: deterministic checkpoints for durability and bootstrap, optionally at high cadence.
* Realtime lane: event streaming with explicit ACK semantics and subscriptions.

A namespace can use either or both lanes depending on policy. This keeps Git strong where it already performs well, while enabling a realtime path that can beat Git on latency and semantics when available.

---

## 0.3 Decisions (locked for v0.5)

These decisions are now considered locked for v0.5 planning and implementation.

1. Store identity and discovery:
   - `store_id` remains a UUID.
   - `store_epoch` exists and starts at 0.
   - Repo-backed stores MUST publish `refs/beads/meta` with `store_meta.json` (section 8.2.1).
   - Remote URL normalization is a legacy fallback only (Appendix A).
   - `store_slug` is a human-friendly alias and is not a stable identifier.

2. Event identity rules:
   - `event_id = (origin_replica_id, namespace, origin_seq)`.
   - `origin_seq` is monotonic per `(origin_replica_id, namespace)`.
   - `seen_map` tracks highest contiguous sequence and must handle gaps (buffer + WANT).

3. Event kind + delta v1 format:
   - Base event kind is `txn_v1`.
   - Delta is `TxnDeltaV1` containing operation lists: `bead_upserts`, `bead_deletes`, `dep_upserts`, `dep_deletes`, `note_appends`.
   - Operation schemas (WireBead, dep edge, tombstone, note) keep the v0.4 field/stamp semantics; v0.5 bundles them into one txn event.

4. Refs and migration:
   - New canonical Git ref: `refs/beads/<store_id>/<group_name>`.
   - Keep `refs/heads/beads/store` as a compatibility alias during migration.

5. Snapshot bootstrap (realtime):
   - Deferred in v0.5. Bootstrap uses checkpoints (Git or local checkpoint cache).
   - When reintroduced, snapshot payloads will reuse the Git checkpoint layout (meta.json + sharded JSONL), optionally packaged (tar/zstd).

6. Git checkpoints:
   - Git checkpoints remain the deterministic checkpoint lane for bootstrap and recovery.
   - v0.5 does not count Git checkpoints toward mutation durability acks (no `GIT_CHECKPOINTED` wait target).

---

## 0.4 Additional defaults (locked for v0.5)

1. Event WAL compaction and pruning:
   - WAL pruning is deferred in v0.5; retain WAL segments. Retention policies are parsed and validated, but enforcement is future work.

2. Gap handling persistence:
   - Buffer out-of-order events in memory up to a strict bound and issue WANT.
   - If bounds are exceeded or gaps do not resolve within a timeout, drop and re-handshake.

3. ACK durability semantics:
   - ACKs carry `durable` and `applied` watermarks; durability classes use `durable`.

4. Snapshot transport framing:
   - Deferred in v0.5 (snapshot bootstrap is not supported).

5. Store directory location:
   - `$BD_DATA_DIR/stores/<store_id>/` for repo-backed stores, with an implementation-defined fallback for local-only stores.

6. Ref migration behavior:
   - Read both legacy and new refs; write new canonical refs; mirror legacy refs for one release. If both exist and diverge, prefer the new ref and emit a warning.

7. Canonical CBOR encoding:
   - Locally-authored `EventBody` bytes MUST use RFC 8949 canonical CBOR (definite lengths, sorted map keys, no duplicate keys, no floats).
   - Receivers MUST reject indefinite-length encodings and floats for hashed structures.
   - Receivers MAY enforce full RFC 8949 canonical CBOR for `event_body_bytes` (rejecting non-canonical bytes).

8. Safety bounds (defaults; implementation-defined but MUST exist):
   - `MAX_FRAME_BYTES` default 16 MiB.
   - `MAX_EVENT_BATCH_EVENTS` default 10_000.
   - `MAX_EVENT_BATCH_BYTES` default 10 MiB.
   - `KEEPALIVE_MS` default 5s; `DEAD_MS` default 30s.

---

## 1. Definitions

### 1.1 Store

A **Store** is the unit of durability and replication hosted by a daemon. A store contains one or more **namespaces**.

* Stores are identified by a stable `store_id` (UUID).
* A store may be **repo-backed** (has an associated Git repo remote) or **local-only**.

### 1.2 Namespace

A **Namespace** is a logical partition within a store with independent policies:

* persistence to Git: on/off
* replication: none / anchors / peers / p2p
* retention: forever / TTL / size budget
* visibility rules (e.g. pinned/system)

Namespaces are isolation boundaries:

* Namespaces MUST NOT reference each other (no cross-namespace deps).
* Mutations are namespace-scoped; cross-namespace atomic mutations are not supported.

### 1.3 Replica

A **Replica** is a daemon instance hosting a local copy of a store.

* Each replica has a stable `replica_id` (UUID).
* Each replica maintains a local sequence counter per store **per namespace**: `origin_seq[namespace]`.

### 1.4 Event

An **Event** is an immutable record appended to the WAL describing one mutation (delta) to the store state.

* Events are identified by `event_id = (origin_replica_id, namespace, origin_seq)`.
* Events are **idempotent**: applying the same event twice MUST have no effect after first application.

### 1.5 Anchor

An **Anchor** is a replica configured to be always-on and used as a durability/replication hub. There may be zero, one, or many anchors.

### 1.6 Git Checkpoint

A **Checkpoint** is a deterministic snapshot export of one or more namespaces, committed to a Git ref.

* Git checkpoints are deterministic snapshots for distribution and bootstrap.
* Git MAY be used at high cadence (hot path), but realtime durability and ACK semantics are defined by WAL + the replication protocol.
* Git-only deployments are valid for persisted namespaces; when no realtime peers are connected, replicas continue operating locally and sync via Git when available.

### 1.7 Object identity and human aliases (normative)

This spec distinguishes between:

1) Canonical IDs (machine identity, correctness primitive)
   - Beads MUST have `id: string` unique within their namespace.
   - IDs are namespace-scoped; collisions across namespaces are allowed and have no semantic meaning.
   - Dependency edges reference beads by canonical IDs within the same namespace.
   - Notes are uniquely identified by `(bead_id, note.id)`.

2) Human aliases (ergonomic only, not a correctness primitive)
   - Beads MAY have `key: string` as a short alias for display and CLI lookup.
   - `key` MAY collide. Collisions MUST be handled as an ambiguity at lookup time, not as a merge or correctness failure.

All replication, durability, checkpointing, and dependency semantics operate on canonical IDs.

### 1.8 Transaction and idempotency identifiers (normative)

* `txn_id: uuid` groups all event(s) created by a single mutation request (trace and receipt handle).
* All events in a `txn_id` MUST share the same `namespace`.
* `client_request_id: uuid` is an optional idempotency token supplied by clients.
  - If provided, a daemon MUST ensure the request is applied at most once.
  - Retrying with the same `client_request_id` MUST return the original receipt and MUST NOT mint new write stamps.

---

## 2. Identity and configuration (locked)

### 2.1 Store identity

* `store_id` MUST be generated once at store initialization.
* For repo-backed stores, `store_id` MUST be stored in the Git checkpoint metadata so clones inherit the same store identity.
* `store_epoch` MUST exist and MUST start at 0.
  - `store_epoch` MUST be incremented only by an explicit administrative store reset operation.
  - Replicas MUST NOT merge events or checkpoints across differing `store_epoch` without an explicit reseed/reset flow (section 9).

### 2.1.1 Store identity discovery order (normative)

When initializing or opening a repo-backed store without an existing local store directory:

1. Fetch `refs/beads/meta` and read `store_meta.json`. This is the primary mechanism.
2. If `refs/beads/meta` is missing, attempt to discover `store_id` by listing remote refs matching `refs/beads/<store_id>/*`.
   - If multiple store_ids are present, the replica MUST require explicit operator selection.
3. If neither exists, a replica MAY fall back to UUIDv5(normalized_remote_url) as a migration bootstrap only (Appendix A).

Once established, replicas MUST persist and reuse `store_id` and `store_epoch`. They MUST NOT change identity solely because a remote URL string changes.

### 2.2 Replica identity

* `replica_id` MUST be stable across daemon restarts on the same machine.
* `replica_id` MUST be unique across replicas participating in a store.

### 2.3 Namespace identifiers

* Namespaces are identified by ASCII strings matching: `[a-z][a-z0-9_]{0,31}`.
* Reserved conventional namespaces (default meanings):

  * `core` - normal beads ("project truth")
  * `sys` - pinned/system beads (identities/hooks/roles/config)
  * `wf` - workflow/orchestration beads (high churn)
  * `tmp` - local-only scratch (never replicated by default)

Implementations MAY allow additional namespaces.

### 2.4 Namespace policy config

Each namespace MUST have a policy block:

* `persist_to_git: bool`
* `replicate: none | anchors | peers | p2p`
* `retention: forever | ttl:<duration> | size:<bytes>`
* `ready_eligible: bool` (default true for `core`, false for `sys`, configurable)
* `visibility: normal | pinned` (pinned affects ready/search defaults)
* `gc_authority: checkpoint_writer | explicit_replica:<replica_id> | none`
* `ttl_basis: last_mutation_stamp | event_time | explicit_field`

If unset:

* `core`: `persist_to_git=true`, `replicate=peers`, `retention=forever`, `ready_eligible=true`, `visibility=normal`, `gc_authority=checkpoint_writer`, `ttl_basis=last_mutation_stamp`
* `sys`: `persist_to_git=true`, `replicate=anchors`, `retention=forever`, `ready_eligible=false`, `visibility=pinned`, `gc_authority=checkpoint_writer`, `ttl_basis=last_mutation_stamp`
* `wf`: `persist_to_git=false`, `replicate=anchors`, `retention=ttl:7d`, `ready_eligible=false`, `visibility=normal`, `gc_authority=checkpoint_writer`, `ttl_basis=last_mutation_stamp`
* `tmp`: `persist_to_git=false`, `replicate=none`, `retention=ttl:24h`, `ready_eligible=false`, `visibility=normal`, `gc_authority=none`

(These defaults are pragmatic; changeable via config, but the semantics are fixed.)

Default policy summary:

| namespace | persist_to_git | replicate | retention | ready_eligible | visibility | gc_authority | ttl_basis |
| --- | --- | --- | --- | --- | --- | --- | --- |
| core | true | peers | forever | true | normal | checkpoint_writer | last_mutation_stamp |
| sys | true | anchors | forever | false | pinned | checkpoint_writer | last_mutation_stamp |
| wf | false | anchors | ttl:7d | false | normal | checkpoint_writer | last_mutation_stamp |
| tmp | false | none | ttl:24h | false | normal | none | last_mutation_stamp |

Namespace policy replication:

* Replication of namespace policies is implementation-defined but MUST be deterministic and durable if enabled.
* The `namespaces.toml` file is a local projection and may be regenerated.

### 2.5 Checkpoint groups

Git persistence is done per **checkpoint group**.

* A **checkpoint group** is a named set of namespaces exported atomically to one Git ref.
* Default group: `main = {core, sys}`.
* Optional: `wf` can be added to `main` or exported to its own ref, but default is not persisted.

Each persisted group MUST specify:

* `git_remote` (default: repo's `origin`)
* `git_ref` (default naming in section 8.2)
* `checkpoint_writers: [replica_id]`
* `primary_checkpoint_writer: replica_id`
* `durable_copy_via_git: bool` (default false)

---

## 3. On-disk store format (locked)

### 3.1 Store directory layout

Each replica MUST store each store under a directory containing:

* `meta.json` (replica + store identity, format version)
* `namespaces.toml` (namespace policies + checkpoint groups)
* `wal/` (append-only segments, partitioned by namespace)
  * `wal/<namespace>/` (one WAL stream per namespace)
* `snap/` (local snapshots)
* `index/` (required for performant replication; rebuildable)

### 3.2 meta.json (required fields)

`meta.json` MUST include:

* `store_format_version: int` (this spec implies `5`)
* `wal_format_version: int` (this spec implies `2`)
* `checkpoint_format_version: int` (this spec implies `5`)
* `replication_protocol_version: int` (this spec implies `1`)
* `store_id: uuid`
* `store_epoch: u64`
* `replica_id: uuid`
* `created_at_ms: int`

### 3.3 WAL stream and segmenting

There is exactly one WAL stream per namespace per store. Events are tagged with namespaces and originate from per-namespace sequences.
Physically, segments live under `wal/<namespace>/`.

WAL MUST be stored as a sequence of **segments**:

* Exactly one segment is **active** for appends.
* Segments are immutable once sealed.
* Segment rotation MUST occur when **either**:

  * active segment size exceeds `WAL_SEGMENT_MAX_BYTES` (default 32 MiB), or
  * active segment age exceeds `WAL_SEGMENT_MAX_AGE` (default 60 seconds)

Sealed segments MAY be compressed on disk (recommended), but the active segment MUST remain writable.

### 3.4 WAL record framing and encoding (normative)

### 3.4.1 Segment header (normative)

Each segment MUST begin with the following header (raw bytes, little-endian where applicable):

* `magic: 5 bytes` = ASCII "BDWAL"
* `wal_format_version: u32_le`
* `header_len: u32_le`
* `store_id: 16 bytes` (UUID bytes)
* `store_epoch: u64_le`
* `namespace_len: u32_le`
* `namespace_bytes: namespace_len bytes` (UTF-8)
* `created_at_ms: u64_le`
* `segment_id: 16 bytes` (UUID bytes)
* `flags: u32_le`
* `header_crc32c: u32_le` (Castagnoli over header bytes excluding this field)

Readers MUST validate header fields. A mismatched `store_id` or `store_epoch` MUST be treated as store corruption or misconfiguration.

### 3.4.2 Record framing (normative)

Each record is:

* `u32_le record_magic` = ASCII "BDR2"
* `u32_le length` (bytes of `record_header` + `payload`)
* `u32_le crc32c` (Castagnoli) computed over bytes from `record_header` through end of payload
* `record_header` (versioned + length-delimited; includes ids + sha/prev and optional request metadata)
* `payload` = CBOR bytes of `EventBody` (locally-authored bytes are canonical; receivers MAY enforce canonical). The payload does NOT include `sha256` / `prev_sha256`.

Payload encoding MUST be canonical CBOR for locally-authored events. Receivers MUST reject
indefinite-length encodings and floats for hashed structures, and MAY reject non-canonical bytes.

Hashing rule (v0.5):
* `sha256 = SHA256(payload_bytes)` where `payload_bytes` are the raw `EventBody` CBOR bytes as stored/transmitted.

### 3.4.3 Crash recovery and corruption handling (normative)

On WAL replay, a replica MUST scan records sequentially:

* If EOF occurs mid-record, the partial tail MUST be ignored and the segment MUST be truncated to the last valid record boundary.
* If `crc32c` does not match the payload, the segment tail MUST be treated as corrupted and MUST be truncated to the last valid record boundary.
* If corruption is detected not at the tail, the replica MUST fail with an explicit error unless an operator opts into a repair mode (section 14).

### 3.4.4 Maximum record size safety (normative)

Implementations MUST enforce a maximum record size (`MAX_WAL_RECORD_BYTES`). Records larger than this MUST be rejected at write time and MUST cause an explicit error at read time.
Default is implementation-defined but SHOULD be <= 16 MiB.

### 3.5 Local snapshots (compaction)

Local snapshots are implementation-defined binary or structured files, BUT MUST satisfy:

1. Snapshot is self-contained for the included namespaces.
2. Snapshot includes a **watermark vector** `included` with the same shape as `seen_map` (see section 4.3).
3. Snapshot format is versioned and validated.
4. Snapshot SHOULD record `gc_floor_ms` per namespace when GC floors are in use (section 10).

Local snapshots are an optimization and MUST NOT be required for correctness.

### 3.6 WAL index and range serving (normative requirement)

Replicas MUST maintain a rebuildable index that can retrieve event ranges efficiently:

* Given `(namespace, origin_replica_id, from_seq_exclusive)`, the replica MUST be able to stream events
  with `origin_seq > from_seq_exclusive` in strictly increasing order without scanning unrelated data.
* The concrete index file format is implementation-defined, but it MUST be rebuildable solely by scanning WAL segments.

Replicas SHOULD maintain a lightweight segment manifest to support fast WANT planning and bounded catch-up.

---

## 4. Event model (WAL contents)

### 4.1 Event body schema (v0.5)

Each WAL record / replication frame carries:

* `event_body_bytes` (CBOR bytes of `EventBody`; locally-authored bytes are canonical)
* `sha256: bytes32 = SHA256(event_body_bytes)`
* `prev_sha256?: bytes32` (optional chain link per `(origin_replica_id, namespace)`)

**EventBody**

* `envelope_v: int` (this spec implies `1`)
* `store_id: uuid`
* `store_epoch: u64`
* `namespace: string`
* `origin_replica_id: uuid`
* `origin_seq: u64` (monotonic per origin replica per namespace)
* `event_time_ms: u64` (origin wall clock)
* `txn_id: uuid` (required; groups all event(s) created by one mutation request)
* `client_request_id: uuid?` (optional; idempotency key from the client)
* `kind: enum` (see section 4.2)
* `delta: map` (CBOR map; delta v1 MUST include `v: 1`)
* `hlc_max?: map` (optional; used for HLC recovery; implementation-defined in v0.5)

`event_time_ms` MUST equal the physical wall time used to mint the write stamp for the mutation that produced this event. If the delta contains multiple new write stamps, `event_time_ms` MUST be >= the max of their wall times and SHOULD be equal to it.

### 4.2 Event kinds (v0.5 normative base)

The following event kind MUST exist:

1. `txn_v1`
   Delta contains a `TxnDeltaV1` (v=1) with operation lists:
   `bead_upserts`, `bead_deletes`, `dep_upserts`, `dep_deletes`, `note_appends`.

Reserved (deferred in v0.5):
* `namespace_gc_marker` is reserved for future GC/retention enforcement once that work lands.

### 4.3 Event ordering, gaps, and idempotence

* The authoritative identity is `(origin_replica_id, namespace, origin_seq)`.
* A replica MUST maintain `seen_map[namespace][origin_replica_id] = highest_contiguous_origin_seq_applied`.
* Applying an event with `origin_seq <= seen_map[namespace][origin_replica_id]` MUST be a no-op (idempotent).
* If a replica observes the same event_id with a different `sha256`, it MUST treat this as corruption or equivocation and MUST fail the replication session with an explicit error.
* If `origin_seq == seen_map + 1`, apply and advance the contiguous watermark.
* If `origin_seq > seen_map + 1`, the replica MUST buffer the event (or persist it in a gap set) and SHOULD request the missing range via `WANT`. It MUST NOT advance `seen_map` past the gap.
* ACKs MUST NOT advance past gaps; only contiguous sequences are acked.
* The gap buffer MUST be bounded (defaults: 10k events or 10 MiB per connection, whichever comes first).
  - If bounds are exceeded or gaps do not resolve within a timeout, the replica MAY drop and re-handshake.
  - Disconnect/re-handshake is a pressure valve, not the steady-state strategy.
Replicas MUST tolerate events arriving out of order across origins and across namespaces.

### 4.4 Tamper-evidence hash (required; chain optional)

* `sha256` MUST be present on every event frame/record.
* `sha256` MUST be computed over the raw `event_body_bytes` exactly as stored/transmitted.
* If `prev_sha256` is present, it MUST equal the previous event hash for the same `(origin_replica_id, namespace)` to form a hash chain.
  Receivers MUST validate `prev_sha256` when appending contiguously; for out-of-order events, validation may be deferred until the predecessor is known.

The optional chain does not affect merge correctness, but provides flight recorder integrity without Git mirroring.

---

### 4.5 Delta payload schemas (TxnDeltaV1, v1, normative)

TxnDeltaV1 is a CBOR map with a required `v: 1` version field and (optionally) any of:
* `bead_upserts: [ { bead: WireBead } ]`
* `bead_deletes: [ { id, deleted_at, deleted_by, ... } ]`
* `dep_upserts: [ { from, to, kind, created_at, created_by, deleted_at?, deleted_by? } ]`
* `dep_deletes: [ { from, to, kind, deleted_at, deleted_by } ]`
* `note_appends: [ { bead_id, note } ]`

Write stamps are encoded as `[wall_ms, counter]`. Actor attribution is carried in a separate field (e.g., `created_by`).

The CBOR encoding MUST use deterministic ordering (sorted map keys, definite lengths).

#### 4.5.1 `bead_upsert` (TxnDeltaV1 entry)

Entry:

* `bead: WireBead`

`WireBead` uses the same field names as the Git checkpoint wire format:

* `id`, `key?`, `created_at`, `created_by`, `created_on_branch?`
* `title`, `description`, `design?`, `acceptance_criteria?`, `priority`, `type`, `labels`, `external_ref?`, `source_repo?`, `estimated_minutes?`
* `status`, `closed_at?`, `closed_by?`, `closed_reason?`, `closed_on_branch?`
* `assignee?`, `assignee_at?`, `assignee_expires?`
* `notes?` (each note has `id`, `content`, `author`, `at`)
* `_at`, `_by`, `_v?`

Notes replication rule (normative):
* In WAL/realtime `bead_upserts`, `notes` SHOULD be omitted.
* New notes MUST be replicated via `note_appends`.
* A `bead_upsert` that includes `notes` MUST be interpreted as "at least these notes exist" (set-union), never as truncation or deletion of notes.

#### 4.5.2 `bead_delete` (TxnDeltaV1 entry)

Entry:

* `id`
* `deleted_at`
* `deleted_by`
* `reason?`
* `lineage_created_at?`
* `lineage_created_by?`

#### 4.5.3 `dep_upsert` (TxnDeltaV1 entry)

Entry:

* `from`
* `to`
* `kind`
* `created_at`
* `created_by`
* `deleted_at?`
* `deleted_by?`

Rules (normative):
* Dependency edges are intra-namespace:
  - `from` and `to` MUST refer to beads in the same event namespace (`EventBody.namespace`).
  - Cross-namespace dependencies are not defined and MUST be rejected.

#### 4.5.4 `dep_delete` (TxnDeltaV1 entry)

Entry:

* `from`
* `to`
* `kind`
* `deleted_at`
* `deleted_by`

#### 4.5.5 `note_append` (TxnDeltaV1 entry)

Entry:

* `bead_id`
* `note` with fields: `id`, `content`, `author`, `at`

Note immutability rules (normative):
* Notes are append-only. A note id collision where content differs MUST be treated as corruption and MUST be rejected.

#### 4.5.6 `namespace_gc_marker` (reserved)

Reserved for future GC/retention enforcement. Namespace GC markers are deferred in v0.5 and not valid in this version.

---

## 5. Mutation and durability pipeline (local)

### 5.1 Write stamp generation

Write stamps are used ONLY for conflict resolution (not replication progress).

Write stamps MUST satisfy:

* monotonic per actor on a replica
* total order via `(physical_ms, logical_counter, actor_id tie-break)`

### 5.2 Mutation transaction (normative)

A mutation request MUST:

1. Validate input (schema constraints)
2. If `client_request_id` is provided, perform an idempotency check:
   - If an event with this `client_request_id` is already committed, return the prior receipt (same `txn_id` and event_id list) and do not mint new write stamps.
3. Mint a fresh `txn_id` for this mutation request (unless returning an idempotent replay).
4. Construct one or more Events (usually one) that all share the same `txn_id`.
5. Append event(s) to WAL for the relevant namespace.
6. fsync according to durability class (section 6).
7. Apply delta(s) to in-memory state via deterministic merge logic.
8. Update indexes (indexes may lag visibility, but canonical state MUST reflect applied events).
9. Notify subscribers.
10. Replicate to peers per policy (async unless required by durability class).

Step (5) and fsync (6) MUST happen before acknowledging success.

Monotonic `origin_seq` minting (normative):
* `origin_seq` MUST be monotonic per `(origin_replica_id, namespace)` across daemon restarts.
* The next `origin_seq` MUST be derived from durable local state, typically `max_origin_seq_seen_in_wal(namespace) + 1`.

---

## 6. Durability / ACK semantics (client-facing)

We support two durability classes for mutations in v0.5.

### 6.1 Durability classes

Each mutation request MUST specify a durability class, or the daemon MUST apply a configured default by client type/namespace:

1. `LOCAL_FSYNC`
   ACK after local WAL append + fsync + apply.

2. `REPLICATED_FSYNC(k)`
   ACK after local fsync+apply PLUS durable acknowledgements from at least `k` additional replicas for the exact event(s).

Implementations MAY expose `ANCHOR_FSYNC` as a convenience alias for `REPLICATED_FSYNC(1)` with anchor-only selection.

### 6.2 ACK enforcement

For `REPLICATED_FSYNC(k)`:

* The coordinator MUST wait for ACKs from >=`k` distinct replicas where:
  `durable[namespace][origin_replica_id=self] >= origin_seq_of_event`.
* The mutation MUST fail if it cannot achieve the required `k` within a configured timeout (default 5s), unless policy explicitly allows fallback.

### 6.3 Replica selection for k

Replica selection is policy-driven:

* if `replicate=anchors`, choose anchors first
* if `replicate=peers`, choose connected peers
* if `replicate=none`, `REPLICATED_FSYNC` is invalid and MUST fail

### 6.4 Container (client mode) rule

Ephemeral containers SHOULD run in client mode:

* They MUST submit mutations to a durable replica.
* They MUST NOT be the sole durable holder of new events.

This is the main "ephemeral is safe" guarantee.

### 6.4.1 Eligible replicas for durability counting (normative)

Replicas counted toward `REPLICATED_FSYNC(k)` MUST be eligible durable replicas:
* They MUST have local durable storage (not ephemeral-only).
* Client-mode ephemeral replicas MUST NOT be counted as durable copies.
Eligibility is policy-driven and SHOULD prioritize anchors.

### 6.5 (Deferred) Git as a counted durable copy

v0.5 does not count Git checkpoints toward mutation durability acks. Git is the checkpoint/bootstrap lane, not a client wait target in v0.5.

### 6.6 Durability receipts and consistency tokens (normative API concept)

On success, the daemon SHOULD return a `durability_receipt`:
* `store_id`
* `store_epoch`
* `txn_id`
* `events: [event_id]`
* `requested_class`
* `achieved_class`
* `min_seen: { namespace -> { origin_replica_id -> u64 } }`

If a request times out waiting for remote durable ACKs, but the event(s) were committed locally, the daemon SHOULD return a retryable error that includes the receipt.

Reads and subscriptions MAY accept `require_min_seen`:
* If the serving replica has applied at least `require_min_seen`, it MAY respond immediately.
* Otherwise it SHOULD wait until it catches up, or return a retryable error that includes current `seen_map`.

---

## 7. Daemon-to-daemon replication protocol (realtime + catch-up)

### 7.1 Transport requirements

Replication runs over any bidirectional reliable byte stream.

Transport MUST provide:

* confidentiality and peer authenticity OR be deployed on a trusted network boundary (tailnet)
* ordered delivery within a connection
* reconnection support

The protocol does NOT assume HTTP; it can run over TCP, WebSocket, or libp2p streams.

### 7.2 Message framing and encoding (normative)

Replication messages use length+crc32c framing shared with WAL tooling:

* `u32_le length` + `u32_le crc32c` + `CBOR payload`

Receivers MUST reject frames larger than `MAX_FRAME_BYTES` and MAY drop the connection.

### 7.3 Message types (normative)

All messages are CBOR maps with:
* `v: u32` (protocol version; this spec implies `1`)
* `type: string`
* `body: map`

#### 1) `HELLO`

Sent by initiator upon connection.

Body:

* `protocol_version: u32` (must equal outer `v`)
* `min_protocol_version: u32` (minimum acceptable)
* `store_id`
* `store_epoch`
* `sender_replica_id`
* `hello_nonce: u64`
* `max_frame_bytes: u32` (sender limit; receiver MUST NOT exceed its own)
* `requested_namespaces: [string]`
* `offered_namespaces: [string]`
* `seen: { namespace -> { origin_replica_id -> u64 } }` (only for requested namespaces)
* `capabilities`:

  * `supports_snapshots: bool` (reserved; MUST be false in v0.5)
  * `supports_live_stream: bool` (must be true for realtime)
  * `supports_compression: bool` (optional)

Rules:

* Receiver MUST reply with `WELCOME` or `ERROR`.
* If protocol versions are incompatible, receiver MUST reply `ERROR(code="version_incompatible")`.
* If `store_id` mismatches, receiver MUST reply `ERROR(code="wrong_store")`.
* If `store_id` matches but `store_epoch` mismatches, receiver MUST reply `ERROR(code="store_epoch_mismatch")`.
* Version compatibility rule:
  - Let `max_a = sender.protocol_version`, `min_a = sender.min_protocol_version`.
  - Let `max_b = receiver.protocol_version`, `min_b = receiver.min_protocol_version`.
  - The negotiated version is `v = min(max_a, max_b)` if `v >= max(min_a, min_b)`, otherwise incompatible.
* The effective frame limit is `min(sender.max_frame_bytes, receiver.max_frame_bytes)`. Senders MUST NOT exceed this limit after `WELCOME`.

#### 2) `WELCOME`

Body:

* `protocol_version: u32` (negotiated)
* `store_id`
* `store_epoch`
* `receiver_replica_id`
* `welcome_nonce: u64`
* `accepted_namespaces: [string]`
* `receiver_seen: { namespace -> { origin_replica_id -> u64 } }` (only for accepted namespaces)
* `live_stream_enabled: bool`
* `max_frame_bytes: u32`

#### 3) `ERROR`

Body:

* ErrorPayload as defined in `REALTIME_ERRORS.md` (code, message, retryable, retry_after_ms?, details?, receipt?)

#### 4) `EVENTS`

A stream message carrying one or more EventFrameV1 objects.

Body:

* `events: [EventFrameV1]`

EventFrameV1:
* `eid: { origin_replica_id, namespace, origin_seq }`
* `sha256: bytes32`
* `prev_sha256?: bytes32`
* `bytes: bytes` (raw CBOR bytes of `EventBody`; locally-authored bytes are canonical)

Rules:

* Sender MAY batch.
* Sender MUST send events for a given `(namespace, origin_replica_id)` in strictly increasing `origin_seq` order.
* Receiver MUST apply idempotently, buffer gaps, and update `seen_map` only when contiguous.
* Receiver MUST validate:
  - `store_id` and `store_epoch` match (from decoded EventBody)
  - `sha256(bytes) == frame.sha256`
  - if event_id already exists, sha256 matches prior observed sha256

#### 5) `ACK`

A cumulative acknowledgment message.

Body:

* `durable: { namespace -> { origin_replica_id -> u64 } }`
* `applied?: { namespace -> { origin_replica_id -> u64 } }`

Rules:

* Receiver MUST ONLY advance `durable[ns][origin]` after the corresponding events are durably represented and guaranteed to be replayed after restart.
  - This is satisfied by WAL append + fsync, or by persisting a snapshot/checkpoint base that guarantees replay and records included watermarks.
* Receiver MUST ONLY advance `applied[ns][origin]` after applying events to canonical in-memory state.
* Both vectors MUST be monotonic per `(namespace, origin_replica_id)`.
* ACKs MAY be sent as standalone messages or piggybacked with other messages.

#### 6) `WANT`

Used to request missing events explicitly (optional optimization).

Body:

* `want: { namespace -> { origin_replica_id -> u64 } }` meaning "send events with seq > this".

Rules:

* Receiver SHOULD respond with `EVENTS` until satisfied.
* A responder MUST use its index to avoid O(total WAL) scans when serving WANT.
* If a responder cannot serve the requested range and snapshot bootstrap is not supported (v0.5), it MUST reply `ERROR(code="bootstrap_required")`.

#### 7) Snapshot messages (deferred in v0.5)

Snapshot bootstrap over realtime is deferred in v0.5. Bootstrap relies on checkpoints (Git or local cache). Snapshot messages may be introduced in a later version.

### 7.4 Standard sync flow (normative)

Upon connection:

1. Initiator sends `HELLO`.
2. Receiver responds `WELCOME`.
3. Both sides determine missing ranges per namespace per origin.
4. Each side streams `EVENTS` for missing events.
5. Receiver emits `ACK` updates as it durably persists and applies events.
6. If `live_stream_enabled`, each side continues to push new events and ACK them.

### 7.5 Flow control

* A sender MUST bound in-flight events (default: max 10k events or 10 MiB buffered).
* If receiver is slow, sender MUST backpressure rather than dropping events.
* ACKs SHOULD be used to advance sender windows and avoid unbounded buffering.
* On disconnect, reconnect MUST resume via `HELLO(seen)`.

Fairness requirement (normative):
* Implementations SHOULD schedule sending across namespaces to avoid starvation.
  A high-churn namespace MUST NOT indefinitely delay replication of `core`/`sys` when they share a connection.

#### 7.5.1 Keepalive (normative)
* Peers SHOULD send `PING` if no traffic is observed for `KEEPALIVE_MS`.
* If no traffic (including PONG) is observed for `DEAD_MS`, the connection SHOULD be dropped and re-handshaken.

#### 7.5.2 `PING` / `PONG` (normative)
`PING` body:
* `nonce: u64`
`PONG` body:
* `nonce: u64`

### 7.6 Replication scope and namespace policies

A replica MUST only replicate namespaces whose policy requires it:

* if `replicate=none`, do not send/receive for that namespace
* if `replicate=anchors`, only replicate with configured anchors
* if `replicate=peers`, replicate with configured peer set
* if `replicate=p2p`, replicate with discovered peers (see section 11)

If no eligible peers are connected, replication for that namespace is idle; local mutations still apply immediately and are durable via WAL.

---

## 8. Git checkpoints (deterministic, hot or cold path)

### 8.1 Core rule (reaffirmed)

Git stores deterministic **state checkpoints**, not the WAL, by default.

Git MAY be used at high cadence as a hot path. Realtime durability and ACK semantics are still defined by WAL + the replication protocol.

### 8.2 Git ref naming (normative)

For repo-backed stores, each checkpoint group MUST map to a single Git ref:

Default pattern:

* `refs/beads/<store_id>/<group_name>`

Example:

* `refs/beads/550e8400-e29b-41d4-a716-446655440000/main`

(You MAY alias this to `beads/store` for ergonomics, but the canonical identity is store_id + group.)

### 8.2.1 Discovery ref (required for repo-backed stores)

The Git remote MUST contain `refs/beads/meta`.

This ref MUST contain a deterministic file `store_meta.json` with:
* `checkpoint_format_version: int`
* `store_id: uuid`
* `store_epoch: u64`
* `checkpoint_groups: { group_name -> git_ref }` (map keys sorted)

Bootstrapping MUST fetch this ref first to discover store identity and group refs.

### 8.3 Checkpoint content format (normative)

Each checkpoint commit tree MUST contain:

* `meta.json` (checkpoint metadata)
* `manifest.json` (deterministic listing of shard files present; required)
* `namespaces/` directory containing per-namespace shard trees:
  * `namespaces/<ns>/state/` shard files
  * `namespaces/<ns>/tombstones/` shard files
  * `namespaces/<ns>/deps/` shard files

#### Sharding rule (fixed)

For each included namespace `<ns>`:

* There are 256 shard slots named `00.jsonl` .. `ff.jsonl` under each of:
  `namespaces/<ns>/state/`, `namespaces/<ns>/tombstones/`, `namespaces/<ns>/deps/`.
* Shard files with zero records MAY be omitted. Missing shard files MUST be interpreted as empty.
* Shard assignment is:
  - beads: shard = first byte of `sha256(id_bytes)`
  - tombstones: shard = first byte of `sha256(id_bytes)`
  - deps: shard = first byte of `sha256(dep_key_bytes)` where `dep_key_bytes = from "\0" to "\0" kind` (UTF-8)
  - `id_bytes` are the UTF-8 bytes of the `id` string
* Within each shard, JSONL lines MUST be sorted by key:
  - beads by `id`
  - tombstones by `id`
  - deps by `(from,to,kind)`

JSON encoding requirements:

* UTF-8
* stable key ordering for objects
* no extra whitespace
* each JSONL line ends with `\n`

(Human diff does not matter; determinism does.)

#### 8.3.1 manifest.json (normative)

`manifest.json` MUST be a deterministically-serialized (canonical) JSON object containing:
* `checkpoint_group: string`
* `store_id: uuid`
* `store_epoch: u64`
* `namespaces: [string]` (sorted)
* `files: { "<path>": { "sha256": "<sha256-hex>", "bytes": <u64> } }` for each file path present excluding `meta.json` and `manifest.json` (paths sorted)

Missing shards are treated as empty and therefore do not appear in `files`.

### 8.4 Checkpoint meta.json (required fields)

Checkpoint `meta.json` MUST include:

* `checkpoint_format_version: int`
* `store_id: uuid`
* `store_epoch: u64`
* `checkpoint_group: string`
* `namespaces: [string]` (included)
* `created_at_ms: u64`
* `created_by_replica_id: uuid`
* `policy_hash: hex` (sha256 of canonical validated namespace policy representation)
* `roster_hash?: hex` (sha256 of canonical validated replicas roster if present)
* `included: { namespace -> { origin_replica_id -> u64 } }`
* `included_heads?: { namespace -> { origin_replica_id -> sha256_hex } }` (optional in v0.5; required once WAL pruning is enabled)
* `content_hash: hex` (REQUIRED; sha256 of canonical checkpoint tree content)
* `manifest_hash: hex` (REQUIRED; sha256 of canonical manifest.json bytes)

`content_hash` algorithm (non-recursive, normative):
1. Write all shard files; compute each file's `sha256` and byte size.
2. Write `manifest.json` (canonical JSON) and compute `manifest_hash = sha256(manifest.json bytes)` (lowercase hex).
3. Build a meta preimage object equal to `meta.json` but with `content_hash` omitted.
4. Serialize the meta preimage using canonical JSON bytes.
5. `content_hash = sha256(meta_preimage_bytes)` (lowercase hex).

Import rule (normative):
* A replica importing a checkpoint MUST recompute `manifest_hash` and `content_hash` and reject the checkpoint if either mismatches.

### 8.5 Checkpoint writer selection (normative)

Each checkpoint group MUST define:

* `checkpoint_writers: [replica_id]` (recommended anchors)
* `primary_checkpoint_writer: replica_id`

Only the primary writer MUST auto-push checkpoints.
Other writers MAY push only on manual override or explicit failover configuration.

### 8.6 Checkpoint scheduling algorithm (normative defaults)

Each checkpoint group has:

* `debounce_ms` (default 200ms)
* `max_interval_ms` (default 1000ms)
* `max_events` (default 2000)

Rules:

* When an event is applied in any namespace included in a persisted group, the group becomes "dirty".
* Dirty groups schedule a checkpoint:

  * after `debounce_ms` since the last new event in that group, **or**
  * when `max_interval_ms` since first dirtied elapses, **or**
  * when `max_events` new events are included,

  whichever happens first.
* Only one checkpoint push may be "in flight" per group. If new events arrive during a push, they are included in the next scheduled checkpoint.

These defaults are aggressive to keep Git viable as a hot path; operators MAY raise or lower them.

### 8.7 Checkpoint export algorithm (normative)

When exporting a checkpoint:

1. Materialize the current converged state for included namespaces.
2. Write deterministic per-namespace sharded snapshot files (empty shards may be omitted).
3. Write `manifest.json`.
4. Write `meta.json` including:
   - `included` as the replica's current durable seen map at time of snapshot.
   - `manifest_hash` and `content_hash`.
5. Create a commit on the group ref.
6. Push.

### 8.8 Checkpoint import / merge algorithm (normative)

We do NOT rely on Git textual merges.

On receiving new Git commits:

1. Fetch remote head for each checkpoint group.
2. Parse checkpoint snapshot into in-memory model.
3. Verify `manifest_hash` and `content_hash`. Reject if mismatch.
4. Merge into local state using deterministic merge logic.
5. Advance the replica's durable watermark vector to at least `meta.included` for every included namespace and origin.
6. If local state changed and this replica is a configured checkpoint writer, it SHOULD export a new checkpoint.

### 8.9 Multi-writer safety (normative)

If multiple replicas write checkpoints to the same ref:

* Push may fail due to non-fast-forward.
* On failure, the replica MUST:

  1. fetch latest head
  2. import + merge
  3. re-export new checkpoint commit
  4. retry push with bounded attempts/backoff

Correctness MUST NOT depend on "only one writer", though single-writer is recommended operationally.

---

## 9. Bootstrap and recovery flows (no ambiguity)

### 9.1 Replica startup (order of preference)

On daemon startup for a store:

1. Load local state from latest local snapshot + replay WAL tail
2. Connect to configured peers/anchors, run replication handshake
3. If repo-backed and Git is configured, fetch and import latest checkpoints (can happen in parallel)

A replica MUST be able to start and serve reads and writes even if network and Git are unavailable (using local state).

### 9.2 New replica bootstrap (no local state)

If a replica has no local snapshot/WAL:

1. If repo-backed: fetch/import latest Git checkpoint(s) for groups.
2. If an anchor can export a local checkpoint cache, import that checkpoint and then subscribe to events.
3. Else: start empty (only allowed if store init explicitly permits it).

Snapshot bootstrap over realtime is deferred in v0.5. If WAL ranges are missing, peers return `bootstrap_required` and the replica must bootstrap from a checkpoint (Git or local cache).

### 9.3 Client (container) bootstrap

Client mode:

* Client fetches a checkpoint from an anchor (or Git if allowed)
* Then subscribes to live updates
* Mutations are submitted to anchor with required durability

Client mode MUST NOT assume local durability.

### 9.4 Store epoch mismatch handling (normative)

If a replica observes a checkpoint or replication peer with the same `store_id` but a different `store_epoch`:
* It MUST refuse to merge state.
* It MUST require an explicit reseed/reset action (operator or higher-level orchestrator).

---

## 10. Retention and GC (deferred in v0.5)

v0.5 parses and validates retention policies but does not emit GC markers or prune WAL.
The rules below are reserved for a future version and should not be treated as implemented behavior in v0.5.

### 10.1 Retention is namespace-scoped

Each namespace policy defines retention. GC only applies to namespaces with TTL or size policy.

### 10.2 GC authority (deterministic deletes)

Replicas MUST NOT independently delete replicated data based on local TTL.
Instead, a GC authority emits GC decisions as events so all replicas converge.

Each namespace has `gc_authority`:

* `checkpoint_writer` (default for replicated namespaces)
* `explicit_replica:<replica_id>` (static)
* `none` (no GC; keep forever)

If `replicate != none` and `retention != forever`, then `gc_authority` MUST NOT be `none`.

### 10.3 TTL basis (what timestamp does "age" use?)

Each namespace defines `ttl_basis`:

1. `last_mutation_stamp` (default)
   Age is based on the physical component of the latest write stamp for that object.
   For beads: bead-level `_at[0]` physical ms.
   For notes: note `at[0]`.
   For deps: use edge's last mutation stamp (created or deleted).

2. `event_time`
   Age based on the `event_time_ms` of the latest event affecting that object.

3. `explicit_field`
   Age based on a named field defined in the namespace schema (implementation-defined, but the field name and units MUST be specified in config).

### 10.4 Authoritative "now"

The authoritative time base for TTL evaluation is the GC authority's wall clock time at the moment it decides to GC.

Other replicas MUST NOT use their local "now" to decide deletions for replicated namespaces.

### 10.5 GC safety margin

To reduce risk from clock skew and late arrivals, GC authority MUST use:

`cutoff = now_ms - TTL - SAFETY_MARGIN`

Default:

* `SAFETY_MARGIN = 24h` for safety-first
* MAY be configured lower (e.g. 5 minutes) for trusted tailnet deployments

### 10.5.1 GC authority clock sanity check

GC authority MUST sanity check its clock against the latest `event_time_ms` seen in the WAL for that namespace. If `now_ms + MAX_CLOCK_SKEW_MS < max_event_time_ms`, GC MUST halt and emit a warning (no GC events). Default `MAX_CLOCK_SKEW_MS` is 24h unless configured.

### 10.6 GC floors (deferred in v0.5)

Deferred in v0.5. Namespace GC marker events and GC floor enforcement are reserved for a future version.

### 10.7 WAL pruning (deferred in v0.5)

Deferred in v0.5. WAL segments are retained; pruning safety gates and included_heads requirements are future work.

### 10.8 Offline guarantees (explicit)

If a replica is offline longer than retention:

* it is NOT guaranteed to catch up via missing events alone
* it MUST bootstrap via checkpoint (Git or local cache)

This mirrors your tombstone TTL philosophy and is acceptable behavior.

---

## 11. P2P integration requirements (kept simple)

### 11.1 P2P is a transport + discovery swap

To "turn on p2p", an implementation MUST ONLY need to supply:

* a discovery mechanism that yields peer endpoints/streams for a store_id
* a transport that provides reliable bidirectional streams

The replication protocol (HELLO/WELCOME/EVENTS/WANT/ACK) is unchanged.

### 11.2 Store advertisement

In p2p mode, peers MUST advertise:

* `store_id`
* namespaces offered
* optional role (anchor-like) tags

### 11.3 No new correctness rules

P2P mode MUST NOT change:

* event identity
* WAL format
* merge semantics
* checkpoint semantics
* ACK semantics
* GC authority semantics

---

## 12. Operational recommendations (non-normative, but actionable)

1. **Tailnet deployments**: start with hub-and-spoke anchored replication for `core/sys/wf`.
2. **For other people**:

   * Git-only mode is supported but is eventual, not realtime.
   * Single-anchor mode is the minimal "realtime" upgrade.
3. **Keep wf out of Git by default**; if a user wants wf across machines without anchors, they can set `wf.persist_to_git=true` and accept Git-only semantics (polling).
4. **Git can be hot or cold**: choose cadence based on latency vs churn tolerance.
5. **Avoid WAL-in-Git by default**; if later you need a "Git flight recorder", enable optional tamper-evidence chains first, then consider an archive repo.

---

## 13. Compliance checklist (what "done" means for this spec)

An implementation conforms to this spec when:

* It writes WAL events in framed canonical CBOR with the required envelope fields and checksums.
* It sequences events per-namespace and maintains contiguous `seen_map` watermarks with gap handling.
* It replicates events between daemons via HELLO/WELCOME/EVENTS/ACK with idempotence and version negotiation.
* It supports durability classes and enforces ACK semantics.
* It exports/imports deterministic Git checkpoints with per-namespace shard trees, manifest, hash verification, and per-namespace watermarks.
* It supports namespaces with independent persistence/replication/retention/GC policies.
* It can operate in:

  * single-node
  * single-anchor
  * multi-anchor
  * Git-only modes
* Adding p2p does not change storage or merge logic, only discovery/transport.

---

## 14. Validation and repair (recommended, normative shape)

Implementations SHOULD provide a validation operation that checks:
* WAL segments parse, have valid headers, and record checksums pass
* `store_id` and `store_epoch` consistency across local state, checkpoints, and peers
* `seen_map` is monotonic and does not advance past gaps
* no equivocation: event_id must not map to different sha256 values
* dependency edges reference valid bead IDs (dangling edges reported as warnings)
* checkpoint determinism invariants (sorted JSONL, stable encoding, manifest consistency, hash verification)

Any destructive repair action MUST be explicit and auditable (emit a report and/or write a backup snapshot).

## Appendix A: Remote URL normalization (legacy fallback, normative)

If UUIDv5(remote_url) is used as a migration fallback, normalization MUST produce a stable ASCII string:
* Strip credentials/userinfo.
* Canonicalize scp-like forms (`git@host:org/repo.git`) into URL form (`ssh://git@host/org/repo.git`).
* Lowercase host.
* Remove trailing `.git`.
* Remove trailing slashes.
* Normalize default ports (22 for ssh, 443 for https) by omission.
* Preserve path case.
The resulting string is the input to UUIDv5.
