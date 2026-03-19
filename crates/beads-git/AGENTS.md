## Boundary
This crate owns git-backed store loading/sync plus checkpoint format, cache, import/export, and publish behavior. Keep daemon runtime orchestration out of here even if `beads-daemon/src/runtime/git_worker.rs` is the main caller.

## Canonical Ownership
- `src/sync.rs` owns store-branch loading, migration, non-fast-forward recovery, backup refs, and deterministic commit/push behavior. Do not regrow git-owned store loading in daemon or assembly code.
- `src/wire.rs` owns the canonical store-branch wire format. Treat it as a compatibility boundary and make additive changes.
- `src/checkpoint/layout.rs` owns shard names and file placement. Never hand-roll filenames, shard paths, or directory scans.
- `src/checkpoint/export.rs` and `src/checkpoint/import.rs` own checkpoint serialization and verification. `import.rs` is the verifier boundary, not a permissive deserializer.
- `src/checkpoint/cache.rs` owns atomic local publication: temp-dir write, fsync, `CURRENT` swap, prune, and full hash recheck on load.
- `src/checkpoint/publish.rs` owns checkpoint ref publication and non-fast-forward merge/retry logic, including `STORE_META_REF`.

## Test Placement
- Many git-owned tests stay inline next to source: `src/sync.rs`, `src/wire.rs`, and the checkpoint source files already carry the owner-local regressions and property checks.
- `tests/checkpoint.rs` and `tests/checkpoint_diff.rs` are the crate-level checkpoint seams. Use them when the public checkpoint export/import surface changes, not as a dumping ground for every git-owned regression.

## Keep Out
- Do not add daemon session/runtime coordination here because sync/checkpoint hooks are nearby. That belongs in `crates/beads-daemon/src/runtime/**`.
- Do not duplicate snapshot serialization or typed digest logic with ad-hoc JSON/path code. Reuse the checkpoint helpers and `beads-core` typed hashes/IDs.
- Do not expose raw test path overrides from `src/paths.rs`. Use `crate::test_support::{DataDirOverride, override_data_dir_for_tests}`; the raw helper is hidden on purpose.
- Do not turn `CheckpointImportError::IncompatibleDepsFormat` into silent acceptance. Daemon runtime uses that error to rebuild checkpoints instead of importing stale legacy data.
- Do not invent new store or checkpoint refs. `refs/heads/beads/store` and `STORE_META_REF` are compatibility surfaces.

## Proof Loops
- Run `cargo test -p beads-git checkpoint` for checkpoint export/import/diff/layout changes.
- Run `cargo test -p beads-git` for sync, wire, migration, or publish changes.
- Run `cargo test -p beads-git --features slow-tests -- --list` when touching sync/recovery races, migration retry paths, or other `#[cfg(feature = "slow-tests")]` coverage in `src/sync.rs`.
- Run `cargo check -p beads-git` when you only need a fast compile proof on API moves before the full test run.
