## Boundary
This crate is the assembly/package seam for beads-rs.
It owns product-facing glue in `src/`, the shipped `bd` entrypoint in `src/bin/main.rs`, the fault-injection support binary in `src/bin/tailnet_proxy.rs`, and the assembly/product proof in `tests/`.
The test side is intentionally narrow: `tests/public_boundary.rs` guards the remaining package-boundary exceptions, `tests/core_state_fingerprint.rs` proves CRDT fingerprint equivalence at the core+git serialization seam, `tests/e2e.rs` exercises the daemon-owned in-memory testkit through the shipped package, and `tests/integration/**` covers process-level product seams.
NEVER: treat this crate as a convenience umbrella for bootstrap, command-language, daemon runtime, or protocol ownership. Those surfaces were peeled out on purpose.

## Routing
- `src/cli/` is the host adapter over `beads-cli`; top-level parse/dispatch, command grammar, and rendering live in `crates/beads-cli`.
- Canonical repo/config/path logic lives in `crates/beads-bootstrap`; the similarly named files under `src/` are assembly consumers and should stay thin.
- Upgrade/install behavior that depends on the shipped package stays here. That surface moved out of `beads-cli`; do not push it back down just because command handlers are nearby.
- `tests/core_state_fingerprint.rs` is the legitimate non-product top-level proof lane here because it crosses `beads-core` CRDT semantics with `beads-git::wire` serialization. Do not misroute that seam test to `beads-core/tests` unless the serialization dependency disappears.
- `tests/` is for package behavior and cross-crate seams; pure owner-crate coverage belongs with the owning crate. If a test stops needing the package seam, move it out of `beads-rs` and tighten `tests/public_boundary.rs` in the same change.

## Verification
- For changes in this crate, start with `cargo check -p beads-rs`.
- If you touch package-boundary assertions or ownership markers, run `cargo test -p beads-rs --test public_boundary`.
- If you touch the core/git fingerprint seam, run `cargo test -p beads-rs --test core_state_fingerprint`.
- If you touch shipped behavior or assembly seams, append `cargo xtest`.
