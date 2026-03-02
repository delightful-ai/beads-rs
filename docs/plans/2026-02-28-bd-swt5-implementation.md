# bd-swt5 Implementation Plan

**Goal:** Ensure replica-id rotation cannot leave a stale replication runtime active; old runtime bindings must be rejected and runtime must rebind before replication ingest/serve resumes.
**Architecture:** Hard cutover to a typed replication-runtime binding version carried end-to-end (runtime state -> replication handles -> ingest requests). Ingest gating is driven by currently bound `ReplicationHandles` state (presence + version), not store metadata. Replica-id rotation bumps store binding version and triggers runtime rebind, making stale/missing bindings invalid by construction.
**Tech Stack:** Rust (`beads-daemon` runtime core/admin/repl/store modules), existing runtime unit tests.

## Locked Decisions

1. Hard cutover only: no compatibility shim for old replication runtime bindings, no dual-path acceptance.
2. Replication runtime validity is defined by a typed binding version on currently bound replication handles, not by best-effort handle replacement.
3. `rotate_replica_id` is the only place that advances replication binding version.
4. `ReplicationHandles.runtime_version` is the authoritative source for the currently bound runtime version.
5. Ingest must reject unless replication handles are present and `request.runtime_version == repl_handles.runtime_version`; stale or missing binding is retryable rejection.
6. Rotation path performs mandatory rebind transition (invalidate old runtime, then rebind) before serving with the new replica identity.

## Dependency Readiness Policy

1. Policy used: **closed or open-parent-only**.
2. `bd-9hym` is **closed** (direct prerequisite satisfied).
3. `bd-70pc` is **open** but is the parent epic (allowed under open-parent-only policy).
4. `bd-swt5` is implementation-ready under this policy.

## 1) Exact Files, Types, and API Changes

### A. `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/store/runtime.rs`

1. Add typed binding-version model for replication runtime identity:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ReplicationRuntimeVersion(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReplicaIdRotation {
    pub old_replica_id: ReplicaId,
    pub new_replica_id: ReplicaId,
    pub runtime_version: ReplicationRuntimeVersion,
}
```

2. Extend `StoreRuntime`:

```rust
pub struct StoreRuntime {
    // ...existing fields...
    replication_runtime_version: ReplicationRuntimeVersion,
}
```

3. Add accessor and change rotate API:

```rust
impl StoreRuntime {
    pub(crate) fn replication_runtime_version(&self) -> ReplicationRuntimeVersion;

    pub fn rotate_replica_id(&mut self) -> Result<ReplicaIdRotation, StoreRuntimeError>;
}
```

4. `rotate_replica_id` behavior change:
- Generate new replica id.
- Persist new `meta.replica_id` to `store_meta.json`.
- Increment `replication_runtime_version`.
- Return `ReplicaIdRotation { old_replica_id, new_replica_id, runtime_version }`.

### B. `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/replication.rs`

1. Make runtime binding version part of repl ingest plumbing:

```rust
let session_store = SharedSessionStore::new(ReplSessionStore::new(
    session.token(),
    store.replication_runtime_version(),
    wal_index,
    ingest_tx,
));
```

2. Strengthen `ensure_replication_runtime` rebind rules:
- If handles exist and `handles.runtime_version == store.replication_runtime_version()`, return early.
- If handles exist but version mismatches, shutdown old handles and continue rebind.
- Store new handles with current `runtime_version`; this field is the authoritative currently-bound runtime version source for ingest gating.

3. Strengthen `handle_repl_ingest` gate:
- Keep session-token validation.
- Authoritative rule: reject ingest unless replication handles are present and `request.runtime_version == repl_handles.runtime_version`.
- Do not gate ingest by store metadata version (`store.replication_runtime_version()`); that value may lead/lag until rebind finishes.
- Reject stale version with retryable `ReplError` (same stale-runtime class; no fallback path).

4. Update `reload_replication_runtime` to remain the canonical explicit rebind transition used by admin rotation.

### C. `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/admin/online.rs`

1. Update `admin_rotate_replica_id` for mandatory rebind flow:

```rust
let rotation = store.rotate_replica_id()?;
drop(proof);
self.reload_replication_runtime(store_id)?;
```

2. Use `rotation.old_replica_id` / `rotation.new_replica_id` in response payload.
3. Include runtime-version in trace logging for post-rotation observability.

### D. `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/repl/runtime.rs` (required plumbing)

1. Extend ingest request payload:

```rust
pub(crate) struct ReplIngestRequest {
    pub(crate) session: StoreSessionToken,
    pub(crate) runtime_version: ReplicationRuntimeVersion,
    pub(crate) batch: ContiguousBatch,
    pub(crate) now_ms: u64,
    pub(crate) respond: Sender<Result<IngestOutcome, ReplError>>,
}
```

2. Extend `ReplSessionStore`:

```rust
pub struct ReplSessionStore {
    session: StoreSessionToken,
    runtime_version: ReplicationRuntimeVersion,
    wal_index: Arc<dyn WalIndex>,
    ingest_tx: Sender<ReplIngestRequest>,
}
```

3. Update constructor:

```rust
pub(crate) fn new(
    session: StoreSessionToken,
    runtime_version: ReplicationRuntimeVersion,
    wal_index: Arc<dyn WalIndex>,
    ingest_tx: Sender<ReplIngestRequest>,
) -> Self;
```

4. Populate `runtime_version` on every outbound `ReplIngestRequest`.

### E. `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs` (supporting type change)

1. Add runtime version to replication handles:

```rust
pub(crate) struct ReplicationHandles {
    runtime_version: ReplicationRuntimeVersion,
    manager: Option<ReplicationManagerHandle>,
    server: Option<ReplicationServerHandle>,
}
```

2. Update all `ReplicationHandles` construction sites accordingly.

## 2) Mandatory Test Scenario and Test List

## Required scenario

Add test in `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs`:

`replica_id_rotation_invalidates_old_replication_runtime_and_requires_rebind_before_ingest`

Scenario assertions:
1. Seed store with runtime binding version `v1` and bound handles at `v1`; ingest request tagged `v1` succeeds.
2. Rotate replica id so store runtime version becomes `v2`; pre-rebind handles are invalidated/dropped so no replication binding is present.
3. Before rebind, both ingest tagged `v1` and ingest tagged `v2` are rejected with the same missing-binding retryable class because no handles are bound yet.
4. After explicit rebind (`reload_replication_runtime` or equivalent bound-handle transition), bound handles move to `v2` and ingest tagged `v2` succeeds.

## Additional tests

1. Add store-runtime unit test in `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/store/runtime.rs`:
`rotate_replica_id_bumps_replication_runtime_version`

2. Add runtime-core test in `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs`:
`ensure_replication_runtime_rebinds_when_binding_version_changes`

3. Update existing ingest tests in `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs` to set `runtime_version` on `ReplIngestRequest`:
- `reload_invalidates_generation_atomically_and_rejects_stale_handles`
- `repl_ingest_accepts_non_core_namespace`

## 3) Execution Order

1. Implement typed binding version + rotate API changes in `store/runtime.rs` with unit test.
2. Plumb `runtime_version` through `ReplIngestRequest` and `ReplSessionStore` (`repl/runtime.rs`).
3. Update core replication bind/gates (`core/replication.rs`) and `ReplicationHandles` metadata (`core/mod.rs`).
4. Update admin rotate flow to enforce rebind transition (`admin/online.rs`).
5. Add mandatory scenario test and adjust existing ingest tests.
6. Run full verification gate.

## 4) Verification Commands

```bash
cargo test -p beads-daemon rotate_replica_id_bumps_replication_runtime_version
cargo test -p beads-daemon replica_id_rotation_invalidates_old_replication_runtime_and_requires_rebind_before_ingest
cargo test -p beads-daemon ensure_replication_runtime_rebinds_when_binding_version_changes
cargo test -p beads-daemon reload_invalidates_generation_atomically_and_rejects_stale_handles
cargo test -p beads-daemon repl_ingest_accepts_non_core_namespace

cargo check
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

## 5) Risk Notes

1. Risk: false stale-rejects if binding version propagation is incomplete.  
Mitigation: compile-enforced constructor/signature changes (`ReplSessionStore::new`, `ReplIngestRequest`) with no optional fields.

2. Risk: rotation leaves runtime down if rebind sequencing is wrong.  
Mitigation: admin rotate path explicitly calls `reload_replication_runtime(store_id)` and test asserts post-rotation rebind behavior.

3. Risk: test flakiness from live network listeners during rebind tests.  
Mitigation: keep mandatory scenario focused on ingest binding checks and deterministic in-process state transitions.
