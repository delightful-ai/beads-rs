# Runtime Config Boundary And Env Isolation Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve launch-time env compatibility for `bd` while centralizing runtime behavior behind explicit typed config and eliminating same-process env mutation plus late ambient env reads.

**Architecture:** Use a delete-first beat structure. In each beat, lock the current contract with one focused test, delete one ambient env read or write, thread an explicit typed value only as far as needed, fix the failing tests and fallout immediately, then commit before starting the next seam. For bootstrap helpers that intentionally model env semantics (`BD_CONFIG_DIR`, `GIT_DIR`, `GIT_WORK_TREE`, XDG paths), add lookup-injected helpers so unit tests can exercise the behavior without mutating process-global env. Do not move test-only or process-only knobs into `beads.toml`; keep persistent config and runtime/test overrides separate.

**Tech Stack:** Rust workspace crates, `clap`, `serde`, `git2`, `tracing`, `tempfile`, `assert_cmd`, `cargo`, `jj`, `bd`

---

## Scope

**In scope**

- CLI and daemon startup wiring
- Bootstrap config/repo discovery
- IPC socket/runtime-dir selection
- Daemon scheduler and load timing knobs
- Store discovery overrides
- Git sync and checkpoint runtime knobs
- WAL runtime/test-hook knobs
- Telemetry/log filter selection
- Unit and integration tests that currently require `unsafe { set_var/remove_var }`

**Out of scope for this plan**

- Shell scripts under `scripts/`
- Bench harness env variables
- Self-upgrade fixture env knobs in `crates/beads-rs/src/upgrade*.rs`
- Example binaries and model-checking examples

Those can stay env-driven until the core runtime boundary is clean.

## Execution Style

- Prefer vertical delete beats over a broad framework-first pass.
- One beat should remove one ambient env seam and fix its direct fallout.
- Write or tighten the failing test first, delete the seam second, repair fallout third, then commit.
- Only introduce shared runtime assembly code when two or more beats need the same wiring.
- If a seam turns out to be bootstrap-only, stop there and do not generalize it into a larger config object.

## File Structure

### Central runtime assembly

- Create: `crates/beads-rs/src/runtime_config.rs`
- Modify: `crates/beads-rs/src/lib.rs`
- Modify: `crates/beads-rs/src/bin/main.rs`
- Modify: `crates/beads-rs/src/cli/mod.rs`
- Modify: `crates/beads-rs/src/cli/backend.rs`
- Modify: `crates/beads-rs/src/telemetry.rs`
- Modify: `crates/beads-rs/src/paths.rs`

### Bootstrap env lookup injection

- Modify: `crates/beads-bootstrap/src/config/env.rs`
- Modify: `crates/beads-bootstrap/src/config/load.rs`
- Modify: `crates/beads-bootstrap/src/config/merge.rs`
- Modify: `crates/beads-bootstrap/src/repo.rs`

### Owner-crate runtime consumers

- Modify: `crates/beads-cli/src/runtime.rs`
- Modify: `crates/beads-surface/src/ipc/client.rs`
- Modify: `crates/beads-daemon/src/config.rs`
- Modify: `crates/beads-daemon/src/runtime/core/mod.rs`
- Modify: `crates/beads-daemon/src/runtime/core/helpers.rs`
- Modify: `crates/beads-daemon/src/runtime/coord.rs`
- Modify: `crates/beads-daemon/src/runtime/store/discovery.rs`
- Modify: `crates/beads-daemon/src/scheduler.rs`
- Modify: `crates/beads-daemon/src/git_lane.rs`
- Modify: `crates/beads-daemon/src/telemetry.rs`
- Modify: `crates/beads-daemon-core/src/wal/segment.rs`
- Modify: `crates/beads-git/src/sync.rs`
- Modify: `crates/beads-git/src/checkpoint/publish.rs`
- Modify: `crates/beads-git/src/paths.rs`

### Tests and docs

- Modify: `crates/beads-daemon/tests/support/env.rs`
- Modify: `crates/beads-daemon/tests/config_precedence.rs`
- Modify: `crates/beads-bootstrap/tests/repo_discovery.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/bd_runtime.rs`
- Modify: `README.md` only if user-facing env semantics need clarification
- Modify: `CLI_SPEC.md` only if launch-time env precedence wording changes

## Chunk 1: Freeze Compatibility And Build The Assembly Point

### Task 1: Document The Env Contract With Red Tests

**Files:**

- Create: `crates/beads-rs/tests/integration/cli/runtime_env_contract.rs`
- Modify: `crates/beads-rs/tests/integration/mod.rs` if needed
- Test: `cargo test -p beads-rs --test integration cli::runtime_env_contract::launch_time_env_overrides_are_respected -- --exact`

- [ ] Add a new integration test module that launches `bd` as a child process and proves launch-time env still works for:
  - `BD_CONFIG_DIR`
  - `BD_RUNTIME_DIR`
  - `BD_TEST_FAST`
  - `LOG`
- [ ] Add one negative contract test that proves CLI-local env does not retroactively reconfigure an already-running daemon.
- [ ] Keep these tests at the child-process boundary. Do not use `std::env::set_var` inside the test process.
- [ ] Record the intended rule in a module comment:

```text
Env passed to the bd process at launch is supported.
Mutating process-global env after startup is not part of the runtime contract.
```

- [ ] Run the targeted test and capture the current behavior before changing startup wiring.
- [ ] Describe and checkpoint the contract tests before larger refactors.

### Task 2: Add Only The Minimal Runtime Assembly Needed For The First Deleted Seams

**Files:**

- Create: `crates/beads-rs/src/runtime_config.rs`
- Modify: `crates/beads-rs/src/lib.rs`
- Modify: `crates/beads-rs/src/cli/mod.rs`
- Modify: `crates/beads-rs/src/telemetry.rs`
- Test: `cargo test -p beads-rs cli::tests::resolve_defaults_use_config_when_no_flags -- --exact`

- [ ] Add `runtime_config.rs` only if the first deleted seams expose repeated startup wiring. Keep the shape small and explicit; do not start with a giant umbrella type. A reasonable ceiling is:

```rust
pub struct RuntimeConfig {
    pub app: crate::config::Config,
    pub actor: beads_core::ActorId,
    pub test_mode: TestMode,
    pub telemetry: TelemetryRuntimeConfig,
    pub daemon: beads_daemon::config::DaemonRuntimeConfig,
    pub git: beads_git::sync::SyncConfig,
    pub checkpoint_publish_max_retries: usize,
    pub wal: WalRuntimeConfig,
    pub store_discovery: StoreDiscoveryOverrides,
}
```

- [ ] Build any new assembly object in one place from:
  - loaded `Config`
  - CLI flags
  - a single env snapshot taken at startup
- [ ] Keep persistent config (`Config`) separate from process-only/test-only overrides.
- [ ] Update `run_cli_entrypoint()` and `run_daemon_command()` only as far as needed for the seams being deleted in this chunk.
- [ ] Keep the startup assembly in `beads-rs`; do not push daemon-specific or git-specific policy decisions back into `beads-bootstrap`.
- [ ] Re-run the targeted CLI tests plus `cargo check -p beads-rs -p beads-daemon -p beads-git -p beads-surface`.

### Task 3: Remove The CLI `BD_ACTOR` In-Process Env Mutation

**Files:**

- Modify: `crates/beads-rs/src/bin/main.rs`
- Modify: `crates/beads-cli/src/runtime.rs`
- Modify: `crates/beads-rs/src/cli/mod.rs`
- Test: `cargo test -p beads-cli current_actor_id -- --nocapture`
- Test: `cargo test -p beads-rs cli::tests::resolve_defaults_cli_overrides_config -- --exact`

- [ ] Delete the `std::env::set_var("BD_ACTOR", ...)` bridge from `crates/beads-rs/src/bin/main.rs`.
- [ ] Change CLI runtime assembly so actor resolution happens once in host code:
  - CLI `--actor`
  - then config default actor
  - then `user@hostname`
- [ ] Change `CliRuntimeCtx` so command handlers consume a resolved actor value instead of falling back to `current_actor_id()` reading env mid-flight.
- [ ] Keep `BD_ACTOR` as a launch-time input by resolving it during runtime assembly, not by mutating process env.
- [ ] Remove or narrow `current_actor_id()` so it no longer reads ambient env during ordinary command handling.
- [ ] Re-run the targeted tests and `cargo check -p beads-cli -p beads-rs`.

## Chunk 2: Replace Same-Process Env Mutation In Bootstrap Tests

### Task 4: Add Lookup-Injected Config Loading APIs

**Files:**

- Modify: `crates/beads-bootstrap/src/config/env.rs`
- Modify: `crates/beads-bootstrap/src/config/load.rs`
- Modify: `crates/beads-bootstrap/src/config/merge.rs`
- Modify: `crates/beads-daemon/tests/config_precedence.rs`
- Modify: `crates/beads-daemon/tests/support/env.rs`
- Test: `cargo test -p beads-daemon --test config_precedence`

- [ ] Add bootstrap helpers that accept an env lookup function or snapshot map instead of hardcoding `std::env::var(...)`.
- [ ] Follow the existing `apply_env_overrides_from(...)` pattern instead of inventing a second test seam.
- [ ] Add parallel helpers for config loading where needed, for example:

```rust
pub fn config_path_with<F>(lookup: F) -> PathBuf
where
    F: FnMut(&str) -> Option<String>;
```

```rust
pub fn load_for_repo_with<F>(repo_root: Option<&Path>, lookup: F) -> Result<Config>
where
    F: FnMut(&str) -> Option<String>;
```

- [ ] Rewrite `config_precedence.rs` to use a deterministic env snapshot or map rather than `unsafe { set_var/remove_var }`.
- [ ] Delete the test helper behavior that mutates global env after `capture()`.
- [ ] Keep the production wrappers (`load_for_repo`, `config_path`) so shell users still get the same env behavior.
- [ ] Re-run `cargo test -p beads-daemon --test config_precedence`.

### Task 5: Add Lookup-Injected Repo Discovery APIs

**Files:**

- Modify: `crates/beads-bootstrap/src/repo.rs`
- Modify: `crates/beads-bootstrap/tests/repo_discovery.rs`
- Test: `cargo test -p beads-bootstrap --test repo_discovery`

- [ ] Add repo discovery variants that accept an env lookup function for `GIT_DIR` and `GIT_WORK_TREE`.
- [ ] Keep the public production entrypoints delegating to `std::env`.
- [ ] Rewrite `repo_discovery.rs` to exercise `GIT_DIR` and `GIT_WORK_TREE` semantics without mutating process-global env.
- [ ] Remove the `EnvGuard` implementation that depends on `unsafe { set_var/remove_var }`.
- [ ] Re-run the targeted bootstrap test and ensure the launch-time semantics still match git's behavior.

### Task 6: Tighten Path And IPC Bootstrap Boundaries

**Files:**

- Modify: `crates/beads-rs/src/paths.rs`
- Modify: `crates/beads-daemon/src/paths.rs`
- Modify: `crates/beads-git/src/paths.rs`
- Modify: `crates/beads-surface/src/ipc/client.rs`
- Modify: `crates/beads-rs/src/lib.rs`
- Test: `cargo test -p beads-surface`
- Test: `cargo test -p beads-rs paths::tests::init_from_config_updates_overrides -- --exact`

- [ ] Make path and socket helpers consume the already-initialized runtime/path overrides instead of rereading `BD_DATA_DIR`, `BD_LOG_DIR`, and `BD_RUNTIME_DIR` late in the process.
- [ ] Keep env-derived path compatibility by resolving those vars during startup config assembly and then calling:
  - `paths::init_from_config(...)`
  - `beads_surface::ipc::set_runtime_dir_override(...)`
  - `beads_git::init_data_dir_override(...)`
- [ ] Remove the late `BD_RUNTIME_DIR` fallback from `beads-surface/src/ipc/client.rs` once startup initialization is authoritative.
- [ ] Keep `XDG_RUNTIME_DIR`, `HOME`, and per-user temp fallback behavior for true bootstrap/default lookup.
- [ ] Re-run targeted tests plus `cargo check -p beads-rs -p beads-daemon -p beads-git -p beads-surface`.

## Chunk 3: Thread Explicit Runtime Knobs Into Owner Crates

### Task 7: Expand `DaemonRuntimeConfig` To Own Daemon Timing And Discovery Knobs

**Files:**

- Modify: `crates/beads-daemon/src/config.rs`
- Modify: `crates/beads-daemon/src/runtime/core/mod.rs`
- Modify: `crates/beads-daemon/src/runtime/core/helpers.rs`
- Modify: `crates/beads-daemon/src/runtime/coord.rs`
- Modify: `crates/beads-daemon/src/runtime/store/discovery.rs`
- Modify: `crates/beads-daemon/src/scheduler.rs`
- Modify: `crates/beads-daemon/src/git_lane.rs`
- Test: `cargo test -p beads-daemon`

- [ ] Extend `DaemonRuntimeConfig` so it owns the knobs currently read ad hoc from env:
  - git sync enabled/disabled
  - checkpoint scheduling enabled/disabled
  - load timeout
  - sync debounce delay
  - git backoff base delay
  - optional store discovery overrides (`remote_url`, `store_id`)
- [ ] Change daemon constructors so convenience entrypoints call the explicit config builder rather than `GitSyncPolicy::from_env()` or `CheckpointPolicy::from_env()`.
- [ ] Replace `load_timeout()` env reads with config access.
- [ ] Replace `SyncScheduler::new()` and `GitLaneState::new()` implicit env behavior with explicit constructor parameters.
- [ ] Replace `BD_REMOTE_URL` and `BD_STORE_ID` late reads in store discovery with values carried in `DaemonRuntimeConfig`.
- [ ] Re-run `cargo test -p beads-daemon` and `cargo check -p beads-daemon -p beads-rs`.

### Task 8: Expand Git And WAL Runtime Config Instead Of Reading Env Inside The Crates

**Files:**

- Modify: `crates/beads-git/src/sync.rs`
- Modify: `crates/beads-git/src/checkpoint/publish.rs`
- Modify: `crates/beads-daemon-core/src/wal/segment.rs`
- Modify: `crates/beads-rs/tests/integration/fixtures/bd_runtime.rs`
- Test: `cargo test -p beads-git`
- Test: `cargo test -p beads-daemon-core`

- [ ] Extend `beads_git::sync::SyncConfig` so it is fully constructed by the caller and no longer uses `from_env()`.
- [ ] Move `BD_TOMBSTONE_TTL_MS` handling into startup/runtime assembly, keeping the same validation and clamp behavior.
- [ ] Change checkpoint publish retry selection so the caller passes `max_retries` explicitly instead of `publish_checkpoint()` reading env.
- [ ] Replace `BD_WAL_SYNC_MODE` and `BD_TEST_FAST` logic in `SegmentConfig::from_limits(...)` with an explicit WAL runtime config passed from the caller or fixture.
- [ ] Keep crash/hang fault injection test hooks explicit in fixtures rather than ambient where feasible. If full hook extraction is too large for this pass, isolate it behind one owner-owned typed hook object instead of raw env reads.
- [ ] Update integration fixtures so child-process tests still pass env to spawned binaries, but in-process tests construct typed config directly.
- [ ] Re-run targeted git/WAL tests plus affected integration slices.

### Task 9: Remove Telemetry Env Reads From Deep Runtime Paths

**Files:**

- Modify: `crates/beads-rs/src/telemetry.rs`
- Modify: `crates/beads-daemon/src/telemetry.rs`
- Modify: `crates/beads-rs/src/runtime_config.rs`
- Test: `cargo test -p beads-rs telemetry`
- Test: `cargo test -p beads-daemon telemetry`

- [ ] Extend `TelemetryConfig` to carry everything telemetry needs from startup:
  - effective log filter override
  - whether test mode is active
  - whether daemon logging defaults should auto-enable file output
- [ ] Resolve `LOG`, `BD_TESTING`, and `BD_LOG_FILE` during startup assembly instead of reading them inside telemetry initialization.
- [ ] Keep user-facing behavior the same: launch-time `LOG=... bd ...` must still work.
- [ ] Re-run targeted telemetry tests and `cargo check -p beads-rs -p beads-daemon`.

## Chunk 4: Audit, Cleanup, And Verification

### Task 10: Audit Remaining Runtime Env Reads And Fence Off The Rest

**Files:**

- Modify only as needed based on audit results

- [ ] Run the runtime env audit and classify each remaining read as one of:
  - supported bootstrap input
  - child-process test/setup only
  - intentionally deferred out-of-scope
  - bug to remove now
- [ ] Use this exact audit command:

```bash
rg -n "std::env::var\\(|std::env::var_os\\(|std::env::set_var\\(|std::env::remove_var\\(|from_env\\(" \
  crates/beads-rs/src \
  crates/beads-daemon/src \
  crates/beads-daemon-core/src \
  crates/beads-git/src \
  crates/beads-surface/src \
  crates/beads-bootstrap/src \
  crates/beads-cli/src \
  crates/*/tests \
  --glob '!**/target/**'
```

- [ ] For any remaining in-process `set_var/remove_var`, either delete it or move the test to child-process execution.
- [ ] For any intentionally deferred env read in upgrade/scripts/examples, add a short comment or doc note so it is clear that it is not part of the main runtime boundary.

### Task 11: Final Verification And Documentation

**Files:**

- Modify: `README.md` only if the env contract wording changed
- Modify: `CLI_SPEC.md` only if precedence wording changed

- [ ] Run `cargo fmt --all`.
- [ ] Run `just dylint`.
- [ ] Run `cargo clippy --all-features -- -D warnings`.
- [ ] Run `cargo xtest`.
- [ ] If this refactor touches slow integration behavior, run:

```bash
cargo nextest run --profile slow --workspace --all-features --features slow-tests
```

- [ ] Re-run the env audit from Task 10 and confirm:
  - no same-process `std::env::set_var` or `remove_var` remains in normal runtime code
  - no `unsafe` env mutation remains in the daemon/bootstrap tests that were targeted by this plan
  - remaining runtime env reads are bootstrap-only or explicitly deferred
- [ ] Update user-facing docs only if needed to clarify the contract:
  - launch-time env is supported
  - already-running daemons are not reconfigured by later CLI env
  - same-process env mutation is not a supported extension mechanism

## Execution Notes

- Keep the refactor staged. Do not try to delete every env read in one commit.
- Land the compatibility tests before deleting runtime env reads.
- Prefer lookup injection for bootstrap semantics and explicit config threading for runtime semantics.
- Prefer "delete and kill" beats:
  - delete one env seam
  - fix the directly failing tests
  - repair nearby docs or fixtures
  - commit
- Do not widen `beads.toml` with test-only toggles just to replace env.
- Child-process integration tests using `Command::env(...)` are valid and should stay.
