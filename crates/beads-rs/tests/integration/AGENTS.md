## Boundary
This directory owns process-level assembly scenarios for the shipped `bd` product surface.
Current subtrees are `cli/`, `daemon/`, and shared setup in `fixtures/`, with additional cross-cutting realtime failures in `realtime_errors.rs`.
NEVER: use this tree as overflow for core/git/daemon owner coverage or for a second in-memory e2e harness.

## Local rules
- Put CLI-path coverage in `cli/` and daemon/runtime-path coverage in `daemon/`.
- `cli/` now has its own child `AGENTS.md`; use it for command-family routing, helper stacks, and narrow proof loops instead of bloating this parent.
- Reuse `fixtures/` for repo/runtime/process setup instead of open-coding setup inside individual tests. If more than one test wants the helper, it probably belongs there.
- Keep cross-cutting realtime failure cases in `realtime_errors.rs` only when they truly span CLI/daemon boundaries; otherwise push them down.
- The ownership markers in `daemon/mod.rs` and `fixtures/mod.rs` are enforced by `tests/public_boundary.rs`. Update both together when you deliberately change what remains assembly-owned.
- Remaining daemon cases here are assembly-owned product seams; if a case no longer needs the package/runtime seam, move it to `crates/beads-daemon/tests`.
- Do not recreate `core/` or `repl/` subtrees here. Those suites were moved out after ownership cleanup.
- Don't re-test core/git/daemon internals here just because the end-to-end fixtures are nearby.

## Verification
- Start with `cargo xtest` for changes in this subtree.
- If the changed file is guarded by `#[cfg(feature = "slow-tests")]` or depends on tailnet/load timing, append `cargo nextest run --profile slow --workspace --all-features --features slow-tests`.
