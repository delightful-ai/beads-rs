# Realtime Error Codes

This document defines the canonical error codes for the beads realtime protocol.
It is referenced by REALTIME_PLAN.md and serves as the single source of truth for
error semantics.

## Stability Rules

- Error codes are **stable once added**: they are never removed or renamed.
- New codes may be added at any time.
- Clients MUST handle unknown codes gracefully (treat as non-retryable unless `retryable=true`).

## ErrorPayload Shape

All errors use this payload structure:

```
{
  code: string,           // one of the codes below
  message: string,        // human-readable description
  retryable: bool,        // true if client can retry without human intervention
  retry_after_ms?: u64,   // suggested wait before retry (avoid thundering herds)
  details?: object,       // error-specific structured data
  receipt?: DurabilityReceipt  // present on partial success (e.g., durability timeout)
}
```

## Error Codes

### Protocol and Identity

| Code | Retryable | Description |
|------|-----------|-------------|
| `wrong_store` | no | store_id mismatch; connection to wrong store |
| `store_epoch_mismatch` | no | store_epoch differs; requires checkpoint/bootstrap |
| `replica_id_collision` | no | peer has same replica_id as local (duplicate store dir) |
| `version_incompatible` | no | protocol version negotiation failed |
| `diverged` | no | same seq but different head sha; chain has forked |

### Operational

| Code | Retryable | Description |
|------|-----------|-------------|
| `overloaded` | yes | server is at capacity; try again later |
| `maintenance_mode` | yes | server in maintenance mode; mutations rejected |
| `durability_timeout` | yes | durability wait timed out; receipt included |
| `durability_unavailable` | no | requested durability class cannot be satisfied (e.g., k > eligible replicas) |

### Replication

| Code | Retryable | Description |
|------|-----------|-------------|
| `snapshot_required` | yes | WAL range pruned or missing; need bootstrap (snapshot later; checkpoint in v0.5) |
| `snapshot_expired` | yes | snapshot temp file expired; restart from chunk 0 |
| `bootstrap_required` | no | WAL range missing; peer must bootstrap from a checkpoint (v0.5) |
| `unknown_replica` | no | replica_id not in roster (if replicas.toml exists) |
| `subscriber_lagged` | yes | subscribe stream fell too far behind; reconnect |

### Client Request

| Code | Retryable | Description |
|------|-----------|-------------|
| `client_request_id_reuse_mismatch` | no | same client_request_id with different request_sha256 |
| `request_too_large` | no | request exceeds MAX_WAL_RECORD_BYTES; split into multiple requests |
| `invalid_request` | no | malformed or invalid request |

### Data Integrity

| Code | Retryable | Description |
|------|-----------|-------------|
| `corruption` | no | data corruption detected (CRC, hash, or structural) |
| `non_canonical` | no | received bytes are not canonical CBOR for a hashed structure |
| `equivocation` | no | same EventId with different sha256 (protocol violation) |

---

## Adding New Codes

When adding a new error code:

1. Add it to the appropriate section in this document
2. Set `retryable` based on whether automated retry is safe
3. Document any `details` fields the error provides
4. Ensure code name is descriptive and uses snake_case


## Error registry v1

This is a canonical, machine-stable list of error codes for **IPC** responses and **replication** `ERROR` frames. Treat it as **append-only**: you can add new codes, but you should never change the meaning of an existing code.

### Error envelope shape

Use the same logical shape everywhere (JSON for IPC, CBOR map for replication):

```text
ErrorPayload {
  code: string,              // stable, snake_case
  message: string,           // human-readable; not stable; never parse
  retryable: bool,           // safe for clients to retry (often with same client_request_id)
  retry_after_ms?: u64,      // suggested wait before retry (avoid thundering herds)
  details?: map<string, any>, // structured debugging/handling data; optional
  receipt?: DurabilityReceipt // included on partial success (e.g., durability_timeout)
}
```

**Normative rules**

* `code` MUST match `[a-z][a-z0-9_]{0,63}`.
* `message` MAY change between versions; clients MUST NOT parse it.
* `details` is forward-compatible: unknown keys MUST be ignored.
* `retry_after_ms` MAY be provided for retryable errors; clients SHOULD respect it with jitter.
* `retryable=true` means:

  * it is safe to retry the same operation, and
  * if `client_request_id` was provided, the daemon SHOULD respond idempotently (same txn/event ids) when possible.

---

## Registry

### A) Generic request/transport errors (IPC + REPL)

#### `invalid_request`

* **retryable:** false
* **Meaning:** Request body is syntactically valid but semantically invalid (unknown op, missing required fields, invalid enum, etc.).
* **details:** `{ field?: string, reason?: string }`

#### `malformed_payload`

* **retryable:** false
* **Meaning:** Could not parse request payload (invalid JSON/CBOR, truncated data).
* **details:** `{ parser: "json"|"cbor"|"ndjson", reason?: string }`

#### `frame_too_large`

* **retryable:** false
* **Meaning:** Single incoming frame exceeds `MAX_FRAME_BYTES` (or negotiated max).
* **details:** `{ max_frame_bytes: u64, got_bytes: u64 }`

#### `batch_too_large`

* **retryable:** false
* **Meaning:** Batch exceeds `MAX_EVENT_BATCH_EVENTS` or `MAX_EVENT_BATCH_BYTES`.
* **details:** `{ max_events: u64, max_bytes: u64, got_events: u64, got_bytes: u64 }`

#### `rate_limited`

* **retryable:** true
* **Meaning:** Token bucket / ingest budget exceeded (e.g., `MAX_REPL_INGEST_BYTES_PER_SEC`).
* **details:** `{ retry_after_ms?: u64, limit_bytes_per_sec: u64 }`

#### `overloaded`

* **retryable:** true
* **Meaning:** Server is shedding load (queue full, memory pressure, background IO budget exhausted, etc.).
* **details:** `{ subsystem?: "ipc"|"repl"|"checkpoint"|"wal", retry_after_ms?: u64, queue_bytes?: u64, queue_events?: u64 }`

#### `maintenance_mode`

* **retryable:** true
* **Meaning:** Server is in maintenance/read-only mode; mutations/replication ingest rejected.
* **details:** `{ reason?: string, until_ms?: u64 }`

#### `internal_error`

* **retryable:** maybe (default false unless you’re confident it’s transient)
* **Meaning:** Unexpected server-side failure.
* **details:** `{ trace_id?: string, component?: string }`

---

### B) Store identity / compatibility (mostly REPL, sometimes IPC)

#### `wrong_store`

* **retryable:** false
* **Meaning:** Peer/store_id mismatch (connecting the wrong cluster/store).
* **details:** `{ expected_store_id: uuid, got_store_id: uuid }`

#### `store_epoch_mismatch`

* **retryable:** false (the session should reconnect and bootstrap)
* **Meaning:** Same store_id but different store_epoch; requires checkpoint/bootstrap, never merge.
* **details:** `{ store_id: uuid, expected_epoch: u64, got_epoch: u64 }`

#### `version_incompatible`

* **retryable:** false
* **Meaning:** No overlapping replication protocol versions.
* **details:** `{ local_min: u32, local_max: u32, peer_min: u32, peer_max: u32 }`

#### `auth_failed`

* **retryable:** false (unless operator fixes secret)
* **Meaning:** Replication authentication failed (e.g., PSK proof mismatch).
* **details:** `{ mode: "psk_v1"|"other", reason?: string }`

#### `unknown_replica`

* **retryable:** false (until roster updated)
* **Meaning:** `replicas.toml` exists and inbound peer replica_id is not allowed.
* **details:** `{ replica_id: uuid, roster_hash?: hex32 }`

#### `replica_id_collision`

* **retryable:** false
* **Meaning:** Peer replica_id equals local replica_id; must rotate or fix copied store dir.
* **details:** `{ replica_id: uuid }`

#### `diverged`

* **retryable:** false
* **Meaning:** Same seq but different head sha; chain has forked/corrupted.
* **details:** `{ namespace: string, origin_replica_id: uuid, seq: u64, expected_sha256: hex32, got_sha256: hex32 }`

---

### C) Namespace / policy / authorization (IPC + REPL ingest gating)

#### `namespace_invalid`

* **retryable:** false
* **Meaning:** Namespace id fails validation (`[a-z][a-z0-9_]{0,31}`).
* **details:** `{ namespace: string, pattern: string }`

#### `namespace_unknown`

* **retryable:** false
* **Meaning:** Namespace not configured/allowed by policies.
* **details:** `{ namespace: string }`

#### `namespace_policy_violation`

* **retryable:** false
* **Meaning:** Operation disallowed by namespace policy (e.g., replicate=none, persist_to_git=false for a requested checkpoint operation, etc.).
* **details:** `{ namespace: string, rule: string, reason?: string }`

#### `cross_namespace_dependency`

* **retryable:** false
* **Meaning:** dep_upsert/dep_delete attempted across namespaces (forbidden).
* **details:** `{ from_namespace: string, to_namespace: string }`

---

### D) Mutation planning / request constraints (IPC)

#### `cas_failed`

* **retryable:** true (often)
* **Meaning:** Compare-and-set / precondition failed (stale read, conflicting write).
* **details:** `{ expected?: any, actual?: any, key?: string }`

#### `client_request_id_reuse_mismatch`

* **retryable:** false
* **Meaning:** Same `(namespace, origin_replica_id, client_request_id)` reused with a different request digest.
* **details:** `{ namespace: string, client_request_id: uuid, expected_request_sha256: hex32, got_request_sha256: hex32 }`

#### `payload_too_large`

* **retryable:** false
* **Meaning:** Mutation request too large to encode/apply within limits (before WAL).
* **details:** `{ limit_bytes: u64, got_bytes: u64 }`

#### `wal_record_too_large`

* **retryable:** false
* **Meaning:** Event would exceed `MAX_WAL_RECORD_BYTES`.
* **details:** `{ max_wal_record_bytes: u64, estimated_bytes: u64 }`

#### `request_too_large` (deprecated)

* **retryable:** false
* **Meaning:** Compatibility alias for `wal_record_too_large` (kept for older clients).
* **details:** `{ max_wal_record_bytes: u64, estimated_bytes: u64 }`

#### `ops_too_many`

* **retryable:** false
* **Meaning:** Txn exceeds `MAX_OPS_PER_TXN`.
* **details:** `{ max_ops_per_txn: u64, got_ops: u64 }`

#### `note_too_large`

* **retryable:** false
* **Meaning:** A note_append content exceeds `MAX_NOTE_BYTES`.
* **details:** `{ max_note_bytes: u64, got_bytes: u64 }`

#### `labels_too_many`

* **retryable:** false
* **Meaning:** Bead exceeds `MAX_LABELS_PER_BEAD`.
* **details:** `{ max_labels_per_bead: u64, got_labels: u64, bead_id?: string }`

---

### E) Durability, waiting, and read-your-writes (IPC)

#### `durability_unavailable`

* **retryable:** false (until topology/policy changes)
* **Meaning:** Requested durability cannot be satisfied (e.g., ReplicatedFsync(k) but eligible replicas < k).
* **details:** `{ requested: string, k?: u32, eligible_total: u32, eligible_replica_ids?: [uuid] }`

#### `durability_timeout`

* **retryable:** true
* **Meaning:** Local append succeeded, but waiting for requested durability proof timed out.
* **details:** `{ requested: string, waited_ms: u64, pending_replica_ids?: [uuid] }`
* **receipt:** MUST be included (since local append succeeded) per your plan.

#### `require_min_seen_timeout`

* **retryable:** true
* **Meaning:** `require_min_seen` not satisfied within `wait_timeout_ms`.
* **details:** `{ waited_ms: u64, required: WatermarkMap, current_applied: WatermarkMap }`

#### `require_min_seen_unsatisfied`

* **retryable:** true
* **Meaning:** `wait_timeout_ms=0` (or omitted default=0) and requirement not currently satisfied.
* **details:** `{ required: WatermarkMap, current_applied: WatermarkMap }`

---

### F) WAL / hashing / contiguity / corruption (primarily REPL ingest + admin)

These codes generally indicate corruption or protocol violations and should be treated as serious.

#### `non_canonical`

* **retryable:** false
* **Meaning:** Received bytes are not canonical CBOR for a hashed structure (e.g., EventBody). Enforcement is optional in v0.5.
* **details:** `{ format: "cbor", reason?: string }`

#### `hash_mismatch`

* **retryable:** false
* **Meaning:** Event sha256 does not match `event_body_bytes`.
* **details:** `{ eid: {ns, origin, seq}, expected_sha256: hex32, got_sha256: hex32 }`

#### `prev_sha_mismatch`

* **retryable:** false (peer should resync/bootstrap)
* **Meaning:** Incoming event prev_sha256 does not match receiver head for that (ns,origin).
* **details:** `{ eid: {...}, expected_prev_sha256: hex32, got_prev_sha256: hex32, head_seq: u64 }`

#### `gap_detected`

* **retryable:** true (session should WANT)
* **Meaning:** Received event is not contiguous (origin_seq > durable_seen+1); receiver will buffer/issue WANT.
* **details:** `{ namespace: string, origin_replica_id: uuid, durable_seen: u64, got_seq: u64 }`

#### `equivocation`

* **retryable:** false
* **Meaning:** Same EventId observed with different sha256 (protocol-level corruption).
* **details:** `{ eid: {...}, existing_sha256: hex32, new_sha256: hex32 }`

#### `wal_corrupt`

* **retryable:** false by default (repair mode may proceed)
* **Meaning:** WAL record/segment corruption not limited to tail truncation.
* **details:** `{ namespace: string, segment_id?: uuid, offset?: u64, reason: string }`

#### `corruption`

* **retryable:** false
* **Meaning:** Generic corruption detected (CRC/hash/structural). Prefer more specific codes when possible.
* **details:** `{ reason: string }`

#### `wal_tail_truncated`

* **retryable:** true (typically; informs operators)
* **Meaning:** Recovery truncated partial/corrupt tail record(s).
* **details:** `{ namespace: string, segment_id?: uuid, truncated_from_offset: u64 }`

#### `segment_header_mismatch`

* **retryable:** false
* **Meaning:** WAL segment header store_id/store_epoch/namespace mismatch.
* **details:** `{ path: string, expected_store_id: uuid, got_store_id: uuid, expected_epoch: u64, got_epoch: u64 }`

#### `wal_format_unsupported`

* **retryable:** false
* **Meaning:** wal_format_version not supported by this binary.
* **details:** `{ wal_format_version: u32, supported: [u32] }`

#### `index_corrupt`

* **retryable:** true (after rebuild)
* **Meaning:** SQLite index inconsistent with WAL (offsets out of bounds, meta mismatch).
* **details:** `{ reason: string }`

#### `index_rebuild_required`

* **retryable:** true (after rebuild)
* **Meaning:** Server is refusing writes/replication until index rebuild/catch-up completes.
* **details:** `{ namespace?: string, reason: string }`

---

### G) Checkpoint / manifest / snapshot (IPC admin + REPL snapshot path)

Snapshot bootstrap is deferred in v0.5; snapshot_* codes are reserved for a future version.

#### `checkpoint_hash_mismatch`

* **retryable:** false
* **Meaning:** content_hash or manifest_hash mismatch on import.
* **details:** `{ which: "content_hash"|"manifest_hash", expected: string, got: string }`

#### `checkpoint_format_unsupported`

* **retryable:** false
* **Meaning:** checkpoint_format_version not supported.
* **details:** `{ checkpoint_format_version: u32, supported: [u32] }`

#### `snapshot_required`

* **retryable:** true
* **Meaning:** WAL cannot serve requested range; peer must bootstrap (snapshot/checkpoint). v0.5 uses `bootstrap_required` when snapshots are unsupported.
* **details:** `{ namespaces: [string], reason: "range_pruned"|"range_missing"|"over_limit" }`

#### `bootstrap_required`

* **retryable:** false
* **Meaning:** WAL cannot serve the requested range and snapshot bootstrap is not supported (v0.5); peer must bootstrap via checkpoint (Git or local cache).
* **details:** `{ namespaces: [string], reason: "range_pruned"|"range_missing"|"over_limit" }`

#### `snapshot_expired`

* **retryable:** true (restart from chunk 0)
* **Meaning:** Snapshot artifact for resume is missing/expired; sender cannot resume.
* **details:** `{ snapshot_id: uuid, restart_from_chunk: u64 }`

#### `snapshot_too_large`

* **retryable:** false
* **Meaning:** Snapshot exceeds `MAX_SNAPSHOT_BYTES` (sender or receiver enforcement).
* **details:** `{ max_snapshot_bytes: u64, announced_bytes?: u64 }`

#### `snapshot_corrupt`

* **retryable:** false (usually)
* **Meaning:** Snapshot hash mismatch or unsafe archive content.
* **details:** `{ reason: string }`

#### `archive_unsafe`

* **retryable:** false
* **Meaning:** Tar extraction safety violation (path traversal, symlink, device file, too many entries, etc.).
* **details:** `{ reason: string, path?: string }`

#### `jsonl_parse_error`

* **retryable:** false
* **Meaning:** Invalid UTF-8/JSON or line/shard limits exceeded during checkpoint import.
* **details:** `{ path: string, line?: u64, reason: string }`

---

### H) Locking / filesystem safety (IPC + startup)

#### `lock_held`

* **retryable:** false
* **Meaning:** Store-global lock is held by another process.
* **details:** `{ store_id: uuid, holder_pid?: u32, holder_replica_id?: uuid, started_at_ms?: u64, daemon_version?: string }`

#### `lock_stale`

* **retryable:** false (requires operator action)
* **Meaning:** Lock appears stale; daemon will not auto-break.
* **details:** `{ store_id: uuid, holder_pid?: u32, started_at_ms?: u64, suggested_action: string }`

#### `path_symlink_rejected`

* **retryable:** false
* **Meaning:** Critical store path is a symlink (rejected for safety).
* **details:** `{ path: string }`

#### `permission_denied`

* **retryable:** false
* **Meaning:** Cannot read/write required store files due to permissions.
* **details:** `{ path: string, operation: "read"|"write"|"fsync" }`

---

### I) Streaming / subscription

#### `subscriber_lagged`

* **retryable:** true
* **Meaning:** Subscriber fell too far behind (bounded buffers exceeded); reconnect required.
* **details:** `{ max_queue_bytes?: u64, max_queue_events?: u64 }`

---

## Suggested “do not invent new codes ad hoc” rule

When you add a new error condition, you should:

1. pick an existing code if it truly matches, otherwise
2. add a new code here with retryability + required details keys.

If you tell me what your current IPC error payload looks like (fields + existing codes), I can map this registry directly onto your structs/enums so it drops in cleanly without churn.

---

## CLI error codes (beads v0.x)

These codes are retained for compatibility with the current CLI/IPC surface.
They are not part of the realtime replication protocol registry. Some may be
promoted into the protocol namespace or renamed over time as the realtime
surface stabilizes.

| Code | Retryable | Meaning |
|------|-----------|---------|
| `not_found` | no | requested bead does not exist |
| `already_exists` | no | attempted to create a bead that already exists |
| `already_claimed` | yes | bead is already claimed by another actor |
| `cas_mismatch` | no | compare-and-set precondition failed |
| `invalid_transition` | no | workflow transition is invalid |
| `validation_failed` | no | field validation failed |
| `not_a_git_repo` | no | path is not a git repository |
| `no_remote` | no | repo has no configured remote |
| `repo_not_initialized` | no | beads ref not initialized in repo |
| `sync_failed` | maybe | git sync failed |
| `bead_deleted` | no | bead is tombstoned |
| `wal_error` | maybe | WAL persistence failed |
| `wal_merge_conflict` | no | merge conflict during WAL apply |
| `not_claimed_by_you` | no | unclaim attempted by non-claimer |
| `dep_not_found` | no | dependency edge not found |
| `load_timeout` | yes | initial fetch timed out |
| `internal` | maybe | CLI internal error |
| `parse_error` | no | CLI IPC parse error |
| `io_error` | yes | CLI IPC I/O error |
| `invalid_id` | no | identifier failed validation |
| `disconnected` | yes | client disconnected |
| `daemon_unavailable` | yes | daemon not reachable |
| `daemon_version_mismatch` | yes | client/server protocol mismatch |
| `init_failed` | maybe | initialization failed |
