## Boundary
This directory contains executable entrypoints, not command logic.
Right now that is the shipped `bd` bootstrap in `main.rs` and the separate `tailnet_proxy.rs` binary used for fault-injection support in slow replication tests.
NEVER: route ordinary CLI behavior, daemon runtime logic, or test harness setup into binaries just because a `main` function can reach everything.

## Local rules
- Keep `main.rs` thin: do the minimal process/env setup there, then hand off to `beads_rs::run_cli_entrypoint`.
- New user-facing commands belong in `beads-cli` plus the `beads-rs/src/cli` host seam, not in a new binary.
- Keep `tailnet_proxy.rs` isolated as a dedicated tool; do not mix its behavior into the normal `bd` path.
- Do not copy `tailnet_proxy.rs` argument/process logic into `main.rs` or new entrypoints; it is a fault-injection helper for `tests/integration/daemon/repl_e2e.rs`, not the template for shipped binaries.
- If you need more fault-injection behavior, extend `tailnet_proxy.rs` and the consuming fixture/tests together so the binary stays single-purpose.

## Verification
- For `main.rs` changes, run `cargo run --bin bd -- --help` to prove parse/bootstrap handoff still works.
- For `tailnet_proxy.rs` changes, run the affected slow `daemon::repl_e2e` coverage. `cargo xtest` will not prove that binary.
