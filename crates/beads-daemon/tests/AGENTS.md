## Boundary
This tree proves daemon crate seams, not assembly flows. The layout is intentional: focused top-level config/layout/privacy tests, `repl/**`, `wal/**`, `ui/**`, and shared fixtures in `support/**`.

## Canonical Helper Stack
- Start REPL coverage from `repl.rs` plus `support/repl_peer.rs`, `support/repl_frames.rs`, and `support/repl_transport.rs`. Those helpers already drive `beads_daemon::testkit::repl::*` rather than private runtime modules.
- Use `repl/e2e.rs` plus `beads_daemon::testkit::e2e::ReplicationRig` for deterministic multi-node replication, restart, and link-fault coverage that no longer needs the shipped `bd` process seam.
- Start WAL coverage from `wal.rs` plus `support/wal.rs` and `support/wal_corrupt.rs`. `TempWalDir`, `record_for_seq`, and the corruption helpers are the owning fixture stack.
- Keep privacy/acknowledgement checks in `private_field_compile_fail.rs` and `ui/*_private.rs`. Those tests guard `PendingReplayApply` and `PlannedMutation` from field-peeking.

## Keep / Avoid
- Keep new coverage in the existing seam buckets. The REPL and WAL suites were intentionally moved down from `crates/beads-rs/tests`; do not bounce daemon-owned protocol/WAL tests back up to assembly.
- When a tailnet or restart scenario can run against `ReplicationRig`, keep it here and leave `crates/beads-rs/tests/integration/daemon/repl_e2e.rs` for external-process smoke or package-owned seams only.
- Use `beads_daemon::paths::override_data_dir_for_tests(...)` when a test needs daemon store paths and git checkpoint-cache paths under one temp root. That wrapper exists so tests do not depend on raw `beads_git::paths` helpers.
- Do not reach into `beads_daemon::runtime::*` from these tests. Go through `beads_daemon::testkit::*` or the public crate surface.
- Do not build one-off checkpoint-cache validators here before checking `wal/fsck.rs`; that suite already proves invalid `checkpoint_cache/CURRENT` handling through the fsck surface.
- Keep WAL fixtures and REPL fixtures separate. The split between `support/wal*.rs` and `support/repl*.rs` is part of the proof model.

## Proof Loops
- Run `cargo test -p beads-daemon --features test-harness repl:: -- --list` for the cheap proof that the REPL seam compiles and is wired into the harness.
- Run `cargo test -p beads-daemon --features test-harness wal:: -- --list` for the WAL seam, including fsck/receipt/index coverage under `tests/wal.rs`.
- Run `cargo test -p beads-daemon --features test-harness private_field_compile_fail_guards_acknowledgement_apis -- --list` when touching `PendingReplayApply`, `PlannedMutation`, or the UI privacy boundary.
- Run `cargo test -p beads-daemon --features test-harness` before calling this subtree done.
