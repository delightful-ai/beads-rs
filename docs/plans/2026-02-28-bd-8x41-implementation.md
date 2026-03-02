# bd-8x41 Implementation Plan

**Goal:** Enforce checkpoint policy/roster compatibility as a hard pre-merge gate so incompatible checkpoints are never merged into daemon state.
**Architecture:** Replace warning-only hash mismatch checks with a typed compatibility validator in checkpoint import flow. `apply_loaded_repo_state` must fail before any checkpoint merge/replay side effects when compatibility fails.
**Tech Stack:** Rust (`beads-daemon` runtime core), existing runtime unit tests in `core/mod.rs`.

## Locked Decisions

1. Hard cutover only: remove warning-only mismatch handling; no dual-path behavior.
2. Compatibility is enforced before `merge_store_states` is called.
3. Compatibility mismatches are represented as typed values (not log-string checks).
4. First incompatible checkpoint import aborts load for that repo open; no best-effort merge of remaining imports.
5. Successful imports continue to merge exactly as today.
6. Checkpoint parse/read failures stay best-effort skip (existing behavior); only policy/roster incompatibility becomes fatal.

## Dependency Readiness

1. Policy used: **closed-or-open-parent-only**.
2. Direct dependency `core/bd-70pc` is **open** and is the parent epic (allowed by policy).
3. `bd-8x41` is ready to execute under this policy.

## Exact File/API/Type Changes

Compile-safety decision for this blocker: **Approach A**. Define `CheckpointCompatibilityError` with `thiserror::Error` so `Display` is guaranteed and `repo_load` can safely use `err.to_string()`.

### 1) `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/checkpoint_import.rs`

1. Replace `warn_on_checkpoint_hash_mismatch` with typed compatibility API:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(super) enum CheckpointCompatibilityError {
    #[error(
        "checkpoint policy hash mismatch for group {checkpoint_group}: local={local_policy_hash:?} checkpoint={checkpoint_policy_hash}"
    )]
    PolicyHashMismatch {
        checkpoint_group: String,
        local_policy_hash: Option<ContentHash>,
        checkpoint_policy_hash: ContentHash,
    },
    #[error(
        "checkpoint roster hash mismatch for group {checkpoint_group}: local={local_roster_hash:?} checkpoint={checkpoint_roster_hash}"
    )]
    RosterHashMismatch {
        checkpoint_group: String,
        local_roster_hash: Option<ContentHash>,
        checkpoint_roster_hash: ContentHash,
    },
}
```

2. Add gate function (name fixed in plan for consistency):

```rust
fn ensure_checkpoint_compatible(
    import: &CheckpointImport,
    local_policy_hash: Option<ContentHash>,
    local_roster_hash: Option<ContentHash>,
) -> Result<(), CheckpointCompatibilityError>
```

Also add the required import in this file:

```rust
use thiserror::Error;
```

Compatibility rules:
- Policy mismatch: if local policy hash exists and differs from checkpoint policy hash -> error.
- Roster mismatch: if checkpoint has `Some(roster_hash)` and differs from local roster hash (including local `None`) -> error.
- Otherwise pass.

3. Change loader signature to hard-gate mismatches:

```rust
pub(super) fn load_checkpoint_imports(
    &self,
    store_id: StoreId,
    repo: &Path,
) -> Result<Vec<CheckpointImport>, CheckpointCompatibilityError>
```

4. Update both cache and git import branches to call `ensure_checkpoint_compatible(...)` and return `Err(...)` immediately on mismatch.

5. Delete `warn_on_checkpoint_hash_mismatch` entirely and remove all mismatch warning log messages (`"checkpoint policy hash mismatch"`, `"checkpoint roster hash mismatch"`) from this path.

### 2) `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/repo_load.rs`

1. Update `apply_loaded_repo_state` to consume the new `Result` from `load_checkpoint_imports`:

```rust
let checkpoint_imports = self
    .load_checkpoint_imports(store_id, repo)
    .map_err(Self::checkpoint_compatibility_op_error)?;
```

2. Add mapping helper in this file:

```rust
fn checkpoint_compatibility_op_error(err: CheckpointCompatibilityError) -> OpError
```

Planned mapping:
- `OpError::ValidationFailed { field: "checkpoint_compatibility".into(), reason: err.to_string() }`
- This is compile-safe because `CheckpointCompatibilityError` derives `thiserror::Error`, which provides `Display`.

3. Ensure failure happens before this loop executes:

```rust
for import in &checkpoint_imports {
    state = merge_store_states(&state, &import.state)?;
}
```

Result: incompatible checkpoints are rejected pre-merge, and no warning-only merge path remains.

## Mandatory Test Scenario (Required)

### `/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/core/mod.rs`

Replace warning test coverage with pre-merge rejection coverage.

1. Remove/replace:
- `checkpoint_roster_hash_mismatch_warns`

2. Add:
- `load_from_checkpoint_ref_rejects_policy_hash_mismatch_pre_merge`
- `load_from_checkpoint_ref_rejects_roster_hash_mismatch_pre_merge`

3. Required assertions in both tests:
- `apply_loaded_repo_state(...)` returns `Err(OpError::ValidationFailed { .. })`.
- Error `field == "checkpoint_compatibility"`.
- Error `reason` identifies policy vs roster mismatch.
- Imported checkpoint state is **not** merged (e.g., bead present in checkpoint is absent from runtime state after failure).
- No test asserts on warning logs for mismatch anymore.

Implementation shape (reuse existing `load_from_checkpoint_ref_sets_watermarks` fixture style):
- Initialize daemon/store/repo.
- Publish checkpoint containing known bead.
- Force mismatch in checkpoint meta:
  - policy test: checkpoint `policy_hash` set to non-local hash.
  - roster test: checkpoint `roster_hash = Some(non_local_hash)` while local roster is `None` or different.
- Call `apply_loaded_repo_state` and assert rejection before merge.

## Additional Cleanup in Same Test File

1. Remove now-unused tracing capture scaffolding tied to warning test only:
- `TestWriter`, `TestWriterGuard`
- `Dispatch`, `Level`, `tracing_subscriber::fmt::MakeWriter` imports
- any now-unused std imports (`Write`, etc.)

## Execution Order

1. Introduce `CheckpointCompatibilityError` as `thiserror::Error` (with explicit `#[error(...)]` messages) plus `ensure_checkpoint_compatible` in `checkpoint_import.rs`.
2. Change `load_checkpoint_imports` to return `Result<_, CheckpointCompatibilityError>` and remove warning-only function.
3. Wire `repo_load.rs` to map compatibility failures to `OpError::ValidationFailed` and abort pre-merge.
4. Replace warning-oriented runtime test with policy+roster pre-merge rejection tests in `core/mod.rs`.
5. Remove obsolete logging test harness code.
6. Run targeted and full verification gates.

## Verification Commands

```bash
cargo test -p beads-daemon load_from_checkpoint_ref_rejects_policy_hash_mismatch_pre_merge
cargo test -p beads-daemon load_from_checkpoint_ref_rejects_roster_hash_mismatch_pre_merge
cargo test -p beads-daemon load_from_checkpoint_ref_sets_watermarks

cargo check
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

## Risk Notes

1. Risk: accidentally keeping a non-fatal mismatch path in one import branch (cache vs git).
Mitigation: enforce identical gate call in both branches and remove old warn function entirely.

2. Risk: rejection occurs after partial side effects.
Mitigation: keep compatibility check in `load_checkpoint_imports` and return error before any merge/replay/apply path in `apply_loaded_repo_state`.

3. Risk: brittle test coupling to logging internals.
Mitigation: assert on returned `OpError` and state non-merge invariants only.
