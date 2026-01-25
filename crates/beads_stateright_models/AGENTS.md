# beads_stateright_models

This crate hosts Stateright models for Beads realtime correctness.

## Non‑negotiables
- Models must stay in lockstep with production code. Use `beads_rs::model` adapters; do not reimplement logic.
- If the drift guard fails to compile, fix the model adapters first. Do not delete the guard.
- Sanity‑check models after changes (at minimum the repl core model).

## Local workflow
- Build models: `cargo check`
- Sanity run repl core:
  - `cargo run --example repl_core_machine -- check --network unordered_duplicating`
  - `cargo run --example repl_core_machine -- check --network ordered`

## Scope
- Prefer ActorModel + Stateright network semantics over custom state machines.
- Keep models small but production‑faithful.
