## Boundary
This directory owns the shared assembly-test harness used by `tests/integration/**`.
It currently contains the reusable product-level harness stack: temp roots, `bd` runtime/env shaping, owned daemon lifecycle, waits/polling, realtime/load fixtures, multi-daemon repl rigs, checked-in migration/legacy store corpora, WAL corruption helpers, and tailnet proxy setup.
NEVER: hide owner-crate logic or one-off test behavior here. If a helper stops being about the shipped package/process seam, move it to the owner crate or keep it local to one test.

## Helper stack
- Start with `bd_runtime.rs` for repo/runtime dirs, `bd` command shaping, autostart control, and `BdCommandProfile`. Copy `BdCommandProfile::cli()` or `fast_daemon()` instead of inventing ad-hoc `BD_*` env sets.
- Use `temp.rs` for short deterministic temp roots, owner metadata, stale fixture cleanup, and Unix socket path checks. Do not derive temp paths from `current_dir()/tmp`.
- Use `wait.rs` for polling and child lifecycle. `wait_for_child_exit()` and `kill_child_and_wait()` reap `Child`s; bare `kill()` and PID-only checks leak processes.
- Use `daemon_runtime.rs` when a fixture owns daemon shutdown/crash semantics. Pass the owning fixture's `runtime_dir` and `data_dir`; do not assume `data/` lives under the runtime tree.
- Owned daemons should be driven through `IpcClient::for_runtime_dir(...).with_autostart(false)` so autostart helpers do not steal lifecycle ownership.
- Use `realtime.rs` or `BdRuntimeRepo` for single-daemon tests. Reach for `repl_rig.rs` only when you truly need multi-daemon replication, peer config churn, or tailnet proxy faults.
- Use `admin_status.rs` when you need repeated admin-status sampling under load instead of open-coding status collectors in individual tests. Bind those helpers to the owning runtime dir; do not introduce ambient `IpcClient::new()` shortcuts there or in `ipc_stream.rs` / `load_gen.rs`.
- `tailnet_proxy.rs` is the external fault-injection harness consumed by `repl_rig.rs` and `daemon/repl_e2e.rs`; keep those surfaces aligned.
- Treat `legacy_store.rs`, `migration_store.rs`, and the checked-in corpora under `legacy_store_corpus/` as provenance fixtures, not scratch space.

## Local rules
- Add reusable setup here instead of creating per-test harness code in `cli/` or `daemon/`, but only when more than one test needs it.
- `temp.rs` owns `BD_TEST_KEEP_TMP`; `timing.rs` owns `BD_TEST_TIMING_DIR`. Reuse those hooks instead of inventing new debug env vars.
- Don't open-code repo/runtime discovery, wait loops, or temp-root paths in tests; extend `bd_runtime.rs`, `wait.rs`, or `temp.rs` instead.
- Keep daemon lifecycle ownership explicit in fixtures; do not hide it behind unrelated helper flows or helper CLI autostart paths.

## Verification
- After fixture changes, run the narrowest dependent integration test first.
- If you touched `repl_rig.rs`, `tailnet_proxy.rs`, or another slow-only harness surface, append `cargo nextest run --profile slow --workspace --all-features --features slow-tests`.
- Then run `cargo xtest`.
