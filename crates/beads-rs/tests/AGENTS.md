## Boundary
This directory owns assembly/product tests for the `beads-rs` package.
Current coverage is intentionally split four ways: package-boundary checks in `public_boundary.rs`, CRDT fingerprint seam proof in `core_state_fingerprint.rs`, daemon-testkit package proof in `e2e.rs`, and process-level product scenarios routed through `integration.rs`.
NEVER: keep a test here once it mostly proves core, git, daemon, or protocol owner behavior just because the assembly harness and binaries are convenient. History already moved core-only, WAL, and generic REPL suites out of this crate.

## Local rules
- Route new package-boundary and ownership assertions to `public_boundary.rs` first. When you re-home tests or move a package seam, update that file in the same change.
- Keep `core_state_fingerprint.rs` here while it depends on `beads_git::wire` serialization to fingerprint `CanonicalState`. That file is the live exception to the otherwise product-facing top-level test split.
- Treat `e2e.rs` as a consumer of `beads_daemon::testkit::e2e`; do not grow a second in-memory replication harness under `beads-rs/tests`.
- `integration.rs` is the process-level assembly router. New subtrees should be rare; do not recreate deleted `integration/core` or `integration/repl` buckets just because they are easy names.
- Reuse shared setup from `integration/fixtures/` instead of creating one-off harness code at this level.
- Treat checked-in compatibility corpora such as `integration/fixtures/legacy_store_corpus/` as provenance fixtures, not a scratch area for ad-hoc test state.

## Verification
- Use `cargo test -p beads-rs --test public_boundary` for package-boundary and ownership assertions.
- Use `cargo test -p beads-rs --test core_state_fingerprint` for the core/git fingerprint seam.
- Use `cargo xtest` for broader assembly/package behavior in this subtree.
