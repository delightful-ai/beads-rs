## Boundary
This subtree owns CLI-facing assembly tests for the shipped `bd` binary.
The proof families here are distinct: `critical_path.rs` covers end-user command workflows and operator-facing CLI seams, `migration.rs` covers Go/store-ref migration behavior at the CLI boundary, and `upgrade.rs` covers package install/update behavior that only exists because `beads-rs` ships the binary.
NEVER: dump generic core/daemon behavior here just because invoking `bd` is convenient. These tests should prove command-surface behavior, package seams, or CLI-owned compatibility exceptions.

## Routing
- Copy `critical_path.rs` for ordinary command workflows, autostart/store-admin seams, sync-from-CLI behavior, and the remaining CLI compatibility aliases such as `comment`.
- Copy `migration.rs` for `bd migrate ...` behavior, legacy store corpora, backup-ref/push-race proofs, and store-ref rewrites that must be verified through the CLI surface.
- Copy `upgrade.rs` for `bd upgrade` package/install behavior, fake release assets, and checksum/install-path proofs.
- If a case only needs typed runtime hooks without invoking the shipped binary, it probably belongs in an owner crate or a lower-level test, not here.

## Helper stack
- Start with `fixtures::bd_runtime::BdRuntimeRepo` for repo/runtime setup and `bd` command shaping. `critical_path.rs` is the canonical example.
- Use `fixtures::legacy_store` for migration/store-ref fixtures instead of inventing ad-hoc git-object surgery in individual tests.
- Use `fixtures::bd_runtime::{wait_for_daemon_pid, wait_for_store_id}` when a CLI case needs operator-visible runtime/store discovery after autostart.
- `upgrade.rs` intentionally scrubs inherited config/runtime env with `scrub_assert_test_env()` and builds fake release assets locally; copy that pattern for package-update proofs, not for normal repo-scoped CLI tests.

## Verification
- Start with `cargo test -p beads-rs --test integration cli::critical_path` for default-suite command-surface changes only.
- Use `cargo test -p beads-rs --test integration cli::migration` for default-suite migration work only.
- `critical_path.rs` and `migration.rs` both contain substantial `#[cfg(feature = "slow-tests")]` coverage. If the touched seam is exercised there, the real proof loop is `cargo nextest run --profile slow --workspace --all-features --features slow-tests`, not the default `cargo test` filter alone.
- Use `cargo test -p beads-rs --test integration cli::upgrade` for upgrade/install behavior.
- Append `cargo xtest` once the narrow loop passes.
