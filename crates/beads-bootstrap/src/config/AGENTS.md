## Boundary
This subtree owns config schema, layer merge rules, file IO, and env overrides for bootstrap config.
NEVER: collapse schema, precedence, env probing, and file IO back into one call-site helper.

## Routing
- New settings start in `schema.rs`; add both the concrete `Config` field and the override-layer field that `ConfigLayer::apply_to` merges.
- `merge.rs` owns the effective order: defaults, then user layer, then repo layer, then env overrides. `BD_TEST_FAST` belongs here because it changes effective config, not file parsing.
- `env.rs` is the source of truth for supported env keys. Do not parse undeclared env vars in `load.rs`, `merge.rs`, or callers.
- `load.rs` owns config file location, TOML reads, atomic writes, and `load_for_repo` as the canonical effective-config entrypoint.

## Verification
- `cargo test -p beads-bootstrap` exercises config roundtrips, precedence, env overrides, and atomic write paths in this subtree.
