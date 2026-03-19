## Boundary
This subtree holds daemon-facing assembly tests that still need the shipped product seam: external daemon processes, runtime/data/log wiring, admin IPC as seen through the packaged runtime dirs, streaming subscriptions, and multi-daemon replication through real sockets/proxies.
NEVER: keep a case here once it only proves daemon-owned runtime behavior, WAL internals, or generic REPL protocol contracts. History already moved the reusable REPL/WAL suites into `crates/beads-daemon/tests`.

## Routing
- Copy `admin.rs` for fast typed admin IPC, maintenance, and fingerprint assertions that stay in the default suite.
- Copy `admin_status.rs` or `subscribe.rs` for slow/time-based status and streaming proofs; both are `slow-tests`, so `cargo xtest` is not enough.
- Copy `lifecycle.rs`, `crash_recovery.rs`, or `store_lock.rs` for daemon ownership, stale socket/meta, restart, and operator-facing store-lock behavior.
- Copy `logging.rs` or `realtime_smoke.rs` for runtime/log-dir/realtime wiring at the product seam.
- Copy `repl_e2e.rs` only for multi-daemon external-process replication smoke, tailnet-proxy product coverage, or checkpoint/WAL behavior that truly needs the assembly harness.
- Keep exactly one thin external restart-under-tailnet/proxy smoke in `repl_e2e.rs` for the shipped daemon restart + IPC reconnect + proxy-process seam; deeper backlog/fresh-handshake semantics stay in `crates/beads-daemon/tests/repl/e2e.rs`.
- Deterministic restart or fault-profile replication proofs belong in `crates/beads-daemon/tests/repl/e2e.rs` via `beads_daemon::testkit::e2e::ReplicationRig`, not here.
- If a case no longer needs the package/runtime seam, move it to `crates/beads-daemon/tests`.

## Local rules
- Reuse `../fixtures/` first; this subtree should describe assembly-owned proof, not grow its own harness stack.
- For convergence polling in replication tests, prefer the `AdminFingerprint` helpers in `fixtures::repl_rig` unless you are asserting WAL bytes, liveness, or other status-only fields.
- Do not call `reload_replication()` after ordinary replicated writes when the roster/config has not changed; wait on the existing readiness/convergence helpers instead.
- Treat `repl_e2e.rs` as the constrained external-process slow zone, not the default template for ordinary daemon tests or deterministic fault semantics.

## Verification
- Use `cargo xtest` for default-suite files such as `admin.rs`.
- For any `slow-tests` file here, or any tailnet/proxy change, append `cargo nextest run --profile slow --workspace --all-features --features slow-tests`.
