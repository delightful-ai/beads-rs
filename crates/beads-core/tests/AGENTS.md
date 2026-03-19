## Boundary
This directory proves three core families: apply semantics, canonical CBOR, and event validation.
NEVER: drop assembly or package regressions here just because they exercise core semantics; these tests were moved out of `beads-rs/tests` to keep pure core proof local.

## Local rules
- `support/event_body.rs` is the fixture source for valid event bodies; start there instead of hand-assembling raw event structs in each test.
- Copy `apply_semantics.rs` plus `support/apply_harness.rs` for apply/state invariants and idempotence assertions.
- Copy `canonical_cbor.rs` plus `support/cbor.rs` for byte-for-byte wire proofs.
- Reuse `tests/support/` instead of rebuilding apply, CBOR, or event harness code inline.
- Keep this directory for cross-module core proofs. Small owner-local invariants still belong beside the owning module in `src/`.

## Verification
- `cargo test -p beads-core --test apply_semantics` for apply/state changes.
- `cargo test -p beads-core --test canonical_cbor` for canonical encoding changes.
- `cargo test -p beads-core --test event_validation` for validation-fixture changes.
- `cargo test -p beads-core` when one change spans multiple proof families.
