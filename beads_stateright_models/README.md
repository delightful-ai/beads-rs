# Stateright models for beads-rs

This crate contains Stateright model-checking examples for the realtime plan.
Dependencies are pinned via crates.io (see `Cargo.toml`).

## Run a model

```bash
cargo run --example repl_core_machine
cargo run --example durability_quorum_machine
cargo run --example idempotency_receipt_machine
```

Most examples accept optional subcommands:

```bash
cargo run --example repl_core_machine -- check
cargo run --example repl_core_machine -- explore localhost:3000
```

## Notes

- Use `RUST_LOG=info` (or higher) for checker output details.
- Examples are intentionally small; adjust constants in each example if you want larger searches.
