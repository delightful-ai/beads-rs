## Boundary
`src/lib.rs` is the public daemon seam. Keep public additions in top-level modules such as `admin.rs`, `config.rs`, `layout.rs`, `paths.rs`, `scheduler.rs`, `telemetry.rs`, `test_utils.rs`, and feature-gated `testkit/mod.rs`; keep `src/runtime/**` crate-private.

## Route Work
- Add cross-crate daemon test seams through `src/testkit/mod.rs` behind the `test-harness` feature. Those surfaces were intentionally moved out of `beads-rs`; do not reopen `runtime::*` just to make tests compile.
- Keep daemon-specific path/runtime wiring in `src/paths.rs` and `src/layout.rs`. Repo discovery, config precedence, and path derivation still belong to `beads-bootstrap`.
- Treat `src/compat/**` as compatibility-only surface. It is nearby legacy bait, not the pattern for new daemon features.
- Treat the private `mod git` shim in `src/lib.rs` as local organization for runtime code, not a public dependency route.

## Keep Out
- Do not regrow checkpoint export/import, checkpoint cache, store-branch loading, or wire-format logic here because `runtime/git_worker.rs` calls it. That ownership sits in `crates/beads-git/src/sync.rs` and `crates/beads-git/src/checkpoint/**`.
- Do not add new validated-ID parsing, CRDT merge rules, or typed error payload logic here because daemon code already handles requests. Those belong in `beads-core`.
- Do not add CLI-facing formatting or command branching to daemon admin/status/config code. Return typed data and let CLI/package layers render it.
- Do not bypass `beads_git::test_support` when adding data-dir overrides. `src/paths.rs::DataDirOverride` deliberately keeps daemon and git test roots coupled instead of exposing raw override helpers.

## Tests
- `tests/repl.rs`, `tests/wal.rs`, and `tests/private_field_compile_fail.rs` are the crate seams. The REPL and WAL suites were intentionally re-homed here from `crates/beads-rs/tests`.
- Keep one-subsystem implementation tests next to the owning module under `src/**`.
- Keep assembly/product seams in `crates/beads-rs/tests`, not here.

## Verification
- Run `cargo test -p beads-daemon --features test-harness` for the full crate seam. `tests/repl.rs`, `tests/wal.rs`, and the `ui/*_private.rs` compile-fail checks import `beads_daemon::testkit::*`, so plain `cargo test -p beads-daemon` does not compile that surface.
- Use `cargo test -p beads-daemon --features test-harness -- --list` if you need to prove the test-harness-gated seam is present before running the full suite.
- Run `cargo check -p beads-daemon --all-features` when touching `test-harness`, `model-testing`, or other gated surfaces in `Cargo.toml`.
- Run `cargo test -p beads-rs --test public_boundary` when changing cross-crate boundary rules around `beads_daemon::runtime::*`, `beads_daemon::git::*`, or the assembly-owned test markers in `crates/beads-rs/tests`. It does not prove the full `beads-daemon` export list or `testkit` API shape.
