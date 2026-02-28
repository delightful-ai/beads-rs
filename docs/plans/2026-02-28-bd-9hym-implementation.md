# bd-9hym Implementation Plan

**Goal:** Unify per-store runtime state under a generation-scoped `StoreSession` so refresh/reload invalidates all store subsystems atomically and stale handles/results cannot be applied.  
**Architecture:** Hard cutover from split `stores`/`git_lanes`/`repl_handles` maps to a single `StoreSession` aggregate keyed by `StoreId`, with a monotonic generation token propagated through async boundaries (git ops/results and replication ingest).  
**Tech Stack:** Rust, `beads-daemon` runtime core/coord/server/repl/git-worker modules, existing runtime unit tests.

## Locked Decisions

1. Hard cutover only: remove split-map ownership model in one change; no compatibility shims.
2. `StoreSession` is the only owner of per-store runtime + lane + replication handles.
3. Every async callback path that can outlive a reload carries `StoreSessionToken { store_id, generation }`.
4. Reload/refresh invalidation is token-gated: stale generation callbacks are ignored/rejected deterministically.
5. `drop_store_state` becomes session-scoped teardown and must shutdown replication handles before removal.
6. Visibility contract is runtime-internal: `StoreSessionToken`/`StoreGeneration` remain `pub(crate)` and every interface carrying them is narrowed to `pub(crate)` or `pub(in crate::runtime)` (no public re-export leaks).

## Dependency Readiness

1. bd-9hym is execution-ready under dependency policy: **closed or open-parent-only**.
2. bd-70pc is the parent epic for bd-9hym and may remain open; it does **not** block implementation or closure of bd-9hym.

## 1) Exact Type/Signature/File Changes

1. Add [store_session.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/store_session.rs) and wire it from [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs).

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StoreGeneration(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct StoreSessionToken {
    store_id: StoreId,
    generation: StoreGeneration,
}

impl StoreSessionToken {
    pub(crate) fn store_id(self) -> StoreId;
    pub(crate) fn generation(self) -> StoreGeneration;
}

pub(crate) struct StoreSession {
    token: StoreSessionToken,
    runtime: StoreRuntime,
    lane: GitLaneState,
    repl_handles: Option<ReplicationHandles>,
}
```

```rust
impl StoreSession {
    pub(crate) fn new(
        token: StoreSessionToken,
        runtime: StoreRuntime,
        lane: GitLaneState,
    ) -> Self;

    pub(crate) fn token(&self) -> StoreSessionToken;
    pub(crate) fn runtime(&self) -> &StoreRuntime;
    pub(crate) fn runtime_mut(&mut self) -> &mut StoreRuntime;
    pub(crate) fn lane(&self) -> &GitLaneState;
    pub(crate) fn lane_mut(&mut self) -> &mut GitLaneState;
    pub(crate) fn split_mut(&mut self) -> (&mut StoreRuntime, &mut GitLaneState);
    pub(crate) fn repl_handles(&self) -> Option<&ReplicationHandles>;
    pub(crate) fn take_repl_handles(&mut self) -> Option<ReplicationHandles>;
    pub(crate) fn set_repl_handles(&mut self, handles: ReplicationHandles);
}
```

2. Update [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs), [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mod.rs), and [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/repl/mod.rs) for daemon fields, helpers, and visibility cutover.

```rust
pub struct Daemon {
    layout: DaemonLayout,
    store_sessions: BTreeMap<StoreId, StoreSession>,
    next_store_generation: u64,
    store_caches: StoreCaches,
    // ...existing non-store fields...
}
```

```rust
impl Daemon {
    fn alloc_store_session_token(&mut self, store_id: StoreId) -> StoreSessionToken;
    pub(crate) fn session_token_for_store(&self, store_id: StoreId) -> Option<StoreSessionToken>;
    pub(crate) fn session_matches(&self, token: StoreSessionToken) -> bool;
}
```

Visibility changes in these files (hard cutover, no shims):
- [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs): change `pub use loaded_store::LoadedStore;` to `pub(crate) use loaded_store::LoadedStore;`.
- [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/mod.rs): stop publicly re-exporting `LoadedStore`, `GitOp`, and `GitResult`; keep only runtime-internal re-exports (`pub(crate)` or `pub(in crate::runtime)`).
- [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/repl/mod.rs): change `ReplIngestRequest` and `ReplSessionStore` re-export to crate-private visibility while keeping protocol/session types public.
- [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/testkit/mod.rs): remove `pub use crate::runtime::GitOp;` and do not re-export runtime `GitOp`/`GitResult` from the public test-harness module.
- [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-rs/src/test_harness/mod.rs): remove explicit public typing on `beads_daemon::testkit::GitOp` channels from helper signatures; keep git channel internals opaque to cross-crate harness callers.
- [lib.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/lib.rs): keep `pub mod testkit` feature gate, but ensure no alternate `daemon`/`runtime` re-export path leaks `GitOp`/`GitResult` when building with `--all-features`.

3. Replace [loaded_store.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/loaded_store.rs) internals to borrow session, not split references.

```rust
pub(crate) struct LoadedStore<'a> {
    store_id: StoreId,
    remote: RemoteUrl,
    session: &'a mut StoreSession,
}
```

```rust
impl LoadedStore<'_> {
    pub(crate) fn session_token(&self) -> StoreSessionToken;
    pub(crate) fn generation(&self) -> StoreGeneration;
    pub(crate) fn runtime(&self) -> &StoreRuntime;
    pub(crate) fn runtime_mut(&mut self) -> &mut StoreRuntime;
    pub(crate) fn lane(&self) -> &GitLaneState;
    pub(crate) fn lane_mut(&mut self) -> &mut GitLaneState;
    pub(crate) fn split_mut(&mut self) -> (&mut StoreRuntime, &mut GitLaneState);
}
```

4. Update [repo_access.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repo_access.rs) to session-centric access and invalidation.

```rust
pub(crate) fn store_session_by_id_mut(
    &mut self,
    store_id: StoreId,
) -> Option<&mut StoreSession>;

pub(crate) fn drop_store_state(&mut self, store_id: StoreId);
```

`drop_store_state` behavior becomes: cancel scheduler, drop checkpoint scheduler store data, shutdown/take replication handles from session, remove session, clear export pending for that store.

5. Update [repo_load.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repo_load.rs) creation/load path to allocate new session token on initial open/reopen and insert one `StoreSession` per store ID.

6. Update [sync.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/coord/sync.rs) callback signatures and token checks.

```rust
pub(in crate::runtime) fn complete_refresh(
    &mut self,
    session: StoreSessionToken,
    remote: &RemoteUrl,
    result: Result<LoadResult, SyncError>,
);

pub(in crate::runtime) fn complete_sync(
    &mut self,
    session: StoreSessionToken,
    remote: &RemoteUrl,
    result: Result<SyncOutcome, SyncError>,
);
```

`ensure_repo_fresh` and sync start paths send git ops with `session: loaded.session_token()`.

7. Update [checkpoint_scheduling.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/checkpoint_scheduling.rs) checkpoint completion signature and stale-token gate.

```rust
pub(in crate::runtime) fn complete_checkpoint(
    &mut self,
    session: StoreSessionToken,
    checkpoint_group: &str,
    result: Result<CheckpointPublishOutcome, CheckpointPublishError>,
);
```

`complete_checkpoint` must do `if !self.session_matches(session) { return; }` before mutating scheduler/store state, then derive `store_id` from `session.store_id()`.

8. Update [git_worker.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/git_worker.rs) op/result payloads and runtime-internal visibility.

```rust
pub(in crate::runtime) enum GitOp {
    Refresh { repo: PathBuf, remote: RemoteUrl, session: StoreSessionToken },
    Sync { repo: PathBuf, remote: RemoteUrl, session: StoreSessionToken, store_id: StoreId, state: CanonicalState, actor: ActorId },
    Checkpoint { repo: PathBuf, session: StoreSessionToken, store_id: StoreId, checkpoint_group: String, git_ref: String, snapshot: CheckpointSnapshot, checkpoint_groups: BTreeMap<String, String> },
    // ...
}

pub(in crate::runtime) enum GitResult {
    Sync(StoreSessionToken, RemoteUrl, SyncResult),
    Refresh(StoreSessionToken, RemoteUrl, Result<LoadResult, SyncError>),
    Checkpoint(StoreSessionToken, String, CheckpointResult),
}
```

Test-harness visibility cutover requirement:
- treat `GitOp`/`GitResult` as runtime-internal only and compile-check with `--all-features` after removing public testkit export leaks;
- any harness API that previously exposed those concrete enums must switch to opaque helper boundaries in `testkit`/`test_harness` instead of re-exporting runtime internals.

9. Update [daemon_api.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/server/daemon_api.rs) and [state_loop.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/server/state_loop.rs) for new `GitResult` variants and token-aware completion calls.

Both handlers must call:
- `complete_refresh(session, &remote, result)`
- `complete_sync(session, &remote, result)`
- `complete_checkpoint(session, &group, result)`

10. Update [replication.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/replication.rs), [repl_ingest.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repl_ingest.rs), and [runtime.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/repl/runtime.rs).

```rust
pub(crate) struct ReplIngestRequest {
    pub(crate) session: StoreSessionToken,
    pub(crate) batch: ContiguousBatch,
    pub(crate) now_ms: u64,
    pub(crate) respond: Sender<Result<IngestOutcome, ReplError>>,
}
```

```rust
impl ReplSessionStore {
    pub(crate) fn new(
        session: StoreSessionToken,
        wal_index: Arc<dyn WalIndex>,
        ingest_tx: Sender<ReplIngestRequest>,
    ) -> Self;
}
```

`handle_repl_ingest` rejects stale `session` token before touching store state; `ingest_remote_batch` accepts/uses token or resolved store ID after token validation.

11. Update session-backed store/lane consumers:
- [housekeeping.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/housekeeping.rs)
- [checkpoint_scheduling.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/checkpoint_scheduling.rs)
- [checkpoint_import.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/checkpoint_import.rs)
- [helpers.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/helpers.rs)
- [executor.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/executor.rs) (only where map field access changed)

## 2) Migration of Split Maps/Handles to Session-Scoped Invalidation

1. Replace daemon ownership:
`stores + git_lanes + repl_handles` → `store_sessions: BTreeMap<StoreId, StoreSession>`.

2. Centralize per-store teardown in `drop_store_state(store_id)`:
must remove one `StoreSession`, shutdown its replication handles, drop checkpoint scheduler store data, cancel scheduler, clear export pending.

3. Load/reload path behavior:
`ensure_repo_loaded*` inserts a new `StoreSession` with fresh generation only when store is absent; `force_reload` always drops session first, then strict reload creates new generation.

4. Async generation gates:
`GitOp` and `GitResult` carry token; completion methods early-return if `!session_matches(token)`.
Checkpoint path is token-gated end-to-end: `start_checkpoint_job` sends `GitOp::Checkpoint { session, ... }`, git worker returns `GitResult::Checkpoint(session, ...)`, server handlers call `complete_checkpoint(session, ...)`, and stale checkpoints are ignored before scheduler/store mutation.
`ReplIngestRequest` carries token; stale token returns retryable error and is discarded.

5. API cleanup:
remove `store_and_lane_by_id_mut` and direct `self.git_lanes`/`self.repl_handles` usage; replace with `StoreSession` accessors.

## 3) Tests (Mandatory Scenario Included)

Modify/add tests in [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs) and update affected signatures.

1. Add `reload_invalidates_generation_atomically_and_rejects_stale_handles`.
Scenario:
- Load store, capture `token_v1`.
- Reload (drop + strict load) to produce `token_v2`.
- Deliver stale `complete_refresh(token_v1, ...)` result and assert active session/lane state is unchanged.
- Send stale `ReplIngestRequest { session: token_v1, ... }` and assert error.
- Send request with `token_v2` and assert success.
This is the mandatory acceptance test for atomic generation invalidation + stale-handle rejection.

2. Add `complete_sync_ignores_stale_session_token`.
Ensure stale sync result cannot overwrite current session state after reload.

3. Add `complete_checkpoint_ignores_stale_session_token_after_reload`.
Scenario:
- Load store, capture `token_v1`, and stage checkpoint dirty shards for a checkpoint group.
- Reload to produce `token_v2`.
- Deliver stale `complete_checkpoint(token_v1, group, Ok(...))`.
- Assert no mutation to active session (`token_v2`) checkpoint shard state and no success transition for the new session’s scheduler key.

4. Update existing refresh tests to pass valid token explicitly:
`complete_refresh_*` tests now call `complete_refresh(current_token, ...)`.

5. Update existing repl ingest tests to populate `ReplIngestRequest.session` with current token.

6. Update test helpers in [helpers.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/helpers.rs):
`insert_store_for_tests` creates/inserts `StoreSession` with generated token.

## 4) Verification Commands

```bash
cargo test -p beads-daemon reload_invalidates_generation_atomically_and_rejects_stale_handles
cargo test -p beads-daemon complete_sync_ignores_stale_session_token
cargo test -p beads-daemon complete_checkpoint_ignores_stale_session_token_after_reload
cargo test -p beads-daemon complete_refresh_clears_in_progress_flag
cargo test -p beads-daemon repl_ingest_accepts_non_core_namespace

cargo check
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

## 5) Risks and Mitigations

1. Risk: broad compile fallout from field/type replacement across runtime modules.  
Mitigation: do structural cutover first (`StoreSession` + daemon fields + accessors), then async token plumbing, then test updates.

2. Risk: stale async results still applied if any callback path misses token propagation.  
Mitigation: require token in all `GitOp` variants that return later (`Refresh`, `Sync`, `Checkpoint`) and in `ReplIngestRequest`; no fallback overloads.

3. Risk: borrow-checker regressions from session aggregate mutation patterns.  
Mitigation: use `LoadedStore::split_mut()` and `StoreSession::split_mut()` as the only dual mutable borrow entrypoints.

4. Risk: hidden stale state from non-session caches/schedulers.  
Mitigation: enforce `drop_store_state` as single invalidation gate and clear scheduler/checkpoint/export pending for the store during teardown.

5. Risk: behavior drift in shutdown path with updated `GitResult` signatures.  
Mitigation: update both normal and shutdown `GitResult` handling paths in state loop and keep explicit no-op on stale tokens.
