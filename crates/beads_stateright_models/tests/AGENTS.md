## Boundary
This directory holds crate regression tests.
NEVER: use this directory as a substitute for the example-based model checks.

## Routing
- Put focused regression tests here.
- Keep reusable model code in `../src/`.
- Keep model-running entrypoints in `../examples/`.

## Local rules
- Keep each test focused on one regression or invariant.
- Treat these tests as narrow regressions layered on the example/model seams, not as a second home for model implementations.
- Reuse the example modules via `#[path = "../examples/..."]` when you need model logic in a regression test.
- Use the crate’s model-facing seams instead of reimplementing behavior in the test.
- Keep the drift guard in `src/drift_guard.rs` green; do not fork example/model code into `tests/`.
- Run `cargo test -p beads_stateright_models` after changes, and use the parent crate's `repl_core_machine` example checks as the cheap broader sanity pass.
