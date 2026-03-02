# bd-642h Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make WAL replay carry checkpoint-dirty effects through a mandatory typed boundary so recovered events cannot be installed without registering dirty shards.

**Architecture:** Hard cutover from replay returning `bool` plus mutating external state, to replay returning a `#[must_use]` pending replay object that owns recovered state and checkpoint-dirty effects. Every `replay_event_wal` callsite must pass state by value and acknowledge effects to install state and dirty shards; no compatibility shim.

**Tech Stack:** Rust, `beads-daemon` runtime core/store modules, `beads-rs` test harness, checkpoint shard derivation utilities, unit tests, compile-fail doctest.

---

## Dependency Note

1. `bd-70pc` is the open parent epic for `bd-642h`.
2. Per the open-parent-only rule, this open parent is non-blocking for `bd-642h`.

## Hard-Cutover Decisions

1. Remove `replay_event_wal(..., state: &mut StoreState) -> Result<bool, ...>` in one change.
2. Replay output becomes pending/acknowledged boundary types with private fields.
3. Recovered state installation and checkpoint-dirty registration happen only via `acknowledge_checkpoint_dirty`.
4. Replay dirty-path derivation uses one source of truth shared with store runtime checkpoint logic.
5. No backward-compatible overloads or adapters.
6. Both replay callsites migrate in the same bead: daemon repo load and `beads-rs` test harness restart.

## 1) Exact Signatures/Types/Files

1. In [runtime.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/store/runtime.rs), extract reusable dirty-path logic and add path recorder:
```rust
pub(crate) fn checkpoint_dirty_paths_for_outcome(
    namespace: &NamespaceId,
    state: &CanonicalState,
    outcome: &ApplyOutcome,
) -> BTreeSet<CheckpointShardPath>;

pub(crate) fn record_checkpoint_dirty_paths(
    &mut self,
    namespace: &NamespaceId,
    paths: BTreeSet<CheckpointShardPath>,
);
```
Refactor `record_checkpoint_dirty_shards` to call these helpers.

2. In [helpers.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/helpers.rs), replace replay seam with pending/acknowledged replay types:
```rust
#[must_use = "must acknowledge replay checkpoint-dirty effects before using recovered replay state"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingReplayApply {
    state: StoreState,
    replayed_any: bool,
    checkpoint_dirty_paths: BTreeMap<NamespaceId, BTreeSet<CheckpointShardPath>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayApplyOutcome {
    pub replayed_any: bool,
    pub max_write_stamp: Option<WriteStamp>,
}

impl PendingReplayApply {
    pub fn acknowledge_checkpoint_dirty(self, store: &mut StoreRuntime) -> ReplayApplyOutcome;
}

pub fn replay_event_wal(
    store_dir: &Path,
    wal_index: &dyn WalIndex,
    state: StoreState,
    limits: &Limits,
) -> Result<PendingReplayApply, StoreRuntimeError>;
```
Replay loop derives dirty paths from each applied event and unions by namespace.

3. In [repo_load.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repo_load.rs), update `apply_loaded_repo_state`:
```rust
let pending_replay = replay_event_wal(..., state, self.limits())?;
// keep apply_checkpoint_watermarks(...) before ack
let replay = pending_replay.acknowledge_checkpoint_dirty(store);
if replay.replayed_any {
    needs_sync = true;
}
last_seen_stamp = max_write_stamp(last_seen_stamp, replay.max_write_stamp);
```
Remove direct `store.state = state` assignment.

4. In [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs), export new replay boundary types:
```rust
pub use helpers::{PendingReplayApply, ReplayApplyOutcome, replay_event_wal};
```

5. In [testkit/mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/testkit/mod.rs), keep replay seam re-export aligned with new signature; add explicit boundary type re-exports only if callsites need named type access.

6. In the missed callsite [test_harness/mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-rs/src/test_harness/mod.rs), migrate `restart()` replay usage with explicit required order:
   1. Move `store.state` by value (`let state = std::mem::take(&mut store.state);`).
   2. Call `replay_event_wal(..., state, ...)`.
   3. Immediately call `.acknowledge_checkpoint_dirty(store)` on the returned pending value.
   4. Do not drop pending replay unacknowledged.
```rust
let state = std::mem::take(&mut store.state);
let _replay = replay_event_wal(
    &store_dir,
    store.wal_index.as_ref(),
    state,
    &limits,
)
.expect("replay wal")
.acknowledge_checkpoint_dirty(store);
```

## 2) Callsite Propagation Details

1. Replay propagation shape:
```text
StoreState (owned)
  -> replay_event_wal(...)
  -> PendingReplayApply { private state + dirty paths }
  -> acknowledge_checkpoint_dirty(store)
  -> ReplayApplyOutcome { replayed_any, max_write_stamp }
```

2. `apply_loaded_repo_state` ordering in [repo_load.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repo_load.rs):
   1. Merge git-loaded state and checkpoint imports.
   2. Build `PendingReplayApply` from owned `state`.
   3. Apply checkpoint watermarks.
   4. Acknowledge replay to install state and dirty paths.
   5. Derive `needs_sync` and `last_seen_stamp` from `ReplayApplyOutcome`.

3. `restart()` ordering in [test_harness/mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-rs/src/test_harness/mod.rs):
   1. Move `store.state` out by value.
   2. Replay with owned state.
   3. Immediately acknowledge against `store`.
   4. Never leave `PendingReplayApply` unused.

4. `replay_event_wal` callsites to migrate in this bead:
   1. [repo_load.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repo_load.rs)
   2. [test_harness/mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-rs/src/test_harness/mod.rs)

## 3) Tests (Mandatory Scenario Included)

1. Add compile-fail doctest in [helpers.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/helpers.rs) proving pending replay state cannot be accessed directly.
2. Add runtime regression in [mod.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs):
   `apply_loaded_repo_state_replay_marks_checkpoint_dirty_shards`, asserting replay-restored state and expected dirty shard tracking.
3. Keep [runtime.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/store/runtime.rs) checkpoint-dirty roundtrip test to guard refactor equivalence.
4. Ensure `beads-rs` harness path compiles under feature-gated build by running explicit all-features workspace/package checks in verification.

## 4) Verification Commands

```bash
cargo fmt --all

# Required: explicit all-features workspace coverage (includes feature-gated harness paths)
cargo check --workspace --all-features
cargo test --workspace --all-features --no-run
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Explicit harness package coverage
cargo check -p beads-rs --all-features
cargo test -p beads-rs --all-features --no-run

# Replay boundary + checkpoint dirty targeted validation
cargo test -p beads-daemon --all-features --doc
cargo test -p beads-daemon runtime::core::tests::apply_loaded_repo_state_replay_marks_checkpoint_dirty_shards
cargo test -p beads-daemon runtime::store::runtime::tests::checkpoint_dirty_shards_roundtrip

# Required project gates
just dylint
cargo test
cargo test --features slow-tests
```

## Key Risks

1. Replay install timing in `apply_loaded_repo_state` could change failure semantics.
   Mitigation: keep `apply_checkpoint_watermarks(...)` before replay acknowledgment.

2. Dirty-path derivation drift could reintroduce skipped shards.
   Mitigation: one shared `checkpoint_dirty_paths_for_outcome(...)` helper used by both normal mutation and replay.

3. Pending replay might be dropped at a callsite.
   Mitigation: `#[must_use]`, private fields, explicit callsite migration in both files, and all-features workspace lint/check gates.

4. Feature-gated harness migration could regress silently without feature-enabled builds.
   Mitigation: explicit `--workspace --all-features` verification commands plus direct `-p beads-rs --all-features` checks.
