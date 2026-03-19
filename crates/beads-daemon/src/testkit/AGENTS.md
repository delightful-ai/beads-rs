## Boundary
This subtree owns daemon-only in-memory harnesses and helpers used to prove runtime, replication, and durability behavior without external processes or assembly-level fixtures.
Do not reintroduce wall-clock waits, process spawning, or runtime-dir ownership into these helpers just because an end-to-end test is nearby.

## Routing
- `e2e.rs` owns `TestWorld`, `ReplicationRig`, deterministic network fault injection, and request/wait loops for in-memory daemon proofs.
- If a test needs daemon internals, simulated links, or deterministic multi-node time control, extend this harness here instead of creating another bespoke rig under `crates/beads-rs/tests` or `crates/beads-daemon/tests`.
- External-process lifecycle, Unix sockets, runtime dirs, and product-boundary assertions still belong in `crates/beads-rs/tests/integration/**`, not here.

## Determinism
- If the harness advances time with `TestClock`, `pump()`, or `advance_ms()`, timeout logic must use that same simulated clock. Do not mix in `Instant`, sleeps, or other host wall-clock checks for deterministic wait paths.
- Prefer bounded harness loops that return protocol responses or receipts over host-speed-sensitive panic paths.
- Keep network fault profiles explicit and reproducible; seed randomized behavior through the shared harness APIs instead of ad hoc per-test RNG state.

## Verification
- `cargo test -p beads-rs --features e2e-tests --test e2e -- e2e_replicated_fsync_timeout_receipt --exact --nocapture` proves the deterministic durability-timeout path in this subtree.
- `cargo test -p beads-daemon --features test-harness` exercises the daemon-owned `testkit` seam more broadly.
