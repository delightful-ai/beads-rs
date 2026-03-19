## Boundary
This crate owns validated IDs, CRDT/apply semantics, event validation, and canonical wire helpers.
NEVER: solve CLI, package, or IPC needs here with host-specific wrappers or surface-specific serde hacks just because this crate is reused everywhere.

## Routing
- New domain invariants usually need changes in two places: the typed boundary in `validated.rs`, `event.rs`, or another owner module, and the semantic owner in `apply.rs`, `state/`, or the relevant CRDT type.
- `state/` and `CanonicalState` are the single source of truth for live/tombstone, label, note, and dep visibility rules. Do not re-derive those semantics in higher crates.
- `event.rs` and `wire_bead.rs` own canonical encoding, hashing, and validated event/wire helpers. Emitted JSON belongs in `beads-api` or `beads-surface`, not here.
- Core-only proof families live under `tests/`: apply semantics, canonical CBOR, and event validation.
- If the change is about product/package behavior rather than domain semantics, it does not belong here.

## Verification
- `cargo test -p beads-core --test apply_semantics` proves cross-module apply and visibility rules.
- `cargo test -p beads-core --test canonical_cbor` proves byte-stable canonical wire output.
- `cargo test -p beads-core --test event_validation` proves fixture and validation boundaries still line up.
- `cargo test -p beads-core` is the right loop when a change crosses more than one proof family or touches public reexports.
