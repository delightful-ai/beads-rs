# beads_stateright_models

This crate hosts Stateright models for Beads realtime correctness.

## Routing
- Keep model code and adapters under `src/`.
- Keep model-running entrypoints under `examples/`.
- Keep focused regression tests under `tests/`.
- Keep local helper scripts under `scripts/`.

## Non‑negotiables
- Models must stay in lockstep with production code. Start from the production-backed adapters in `beads_rs::model` before inventing parallel model logic.
- If the drift guard fails to compile, fix the model adapters first. Do not delete the guard.
- Sanity‑check models after changes (at minimum the repl core model).

## Local workflow
- Baseline compile/drift guard check: `cargo check -p beads_stateright_models`
- Sanity run repl core:
  - `cargo run --example repl_core_machine -- check --network unordered_duplicating`
  - `cargo run --example repl_core_machine -- check --network ordered`
- If the bug is narrow, follow with a focused regression like `tests/durability_idempotency_regression.rs` rather than turning `tests/` into a second home for broad model exploration.

## Scope
- Keep models small but production‑faithful.
