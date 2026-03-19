## Boundary
This directory proves crate-level checkpoint seams only: deterministic export, import verification, multi-namespace layout/manifest behavior, and diff diagnostics. Keep checkpoint compatibility coverage here instead of pushing it up into daemon or assembly tests.

## Canonical Stack
- Use `checkpoint_support.rs` for fixture snapshots, manifest/hash assertions, and shared state builders.
- Put export/import/roundtrip/layout cases in `checkpoint.rs`.
- Put mismatch-reporting cases in `checkpoint_diff.rs`.

## Keep / Avoid
- If the behavior is local to `src/checkpoint/cache.rs` or `src/checkpoint/publish.rs`, prefer unit tests next to that source file. Use this directory when the public checkpoint export/import surface changes.
- Do not pull in daemon runtime, `beads_daemon::testkit`, or assembly fixtures. These tests stay below that layer on purpose.
- Do not create alternate fixture builders when `fixture_small_state()`, `fixture_multi_namespace()`, `assert_manifest_files()`, and `assert_meta_hashes()` already cover the common checkpoint shapes.

## Proof Loops
- Run `cargo test -p beads-git checkpoint` as the cheap loop for this directory.
- Run `cargo test -p beads-git` before declaring checkpoint compatibility changes done.
