# Non-CLI Daemon Coupling Inventory (`beads-rs-gou3.8`)

Audit date: 2026-03-13
Scope audited:
- `crates/beads-rs/src/model/**`
- `crates/beads_stateright_models/**`
- `crates/fuzz/**`
- `crates/beads-rs/tests/**`
- `crates/beads-rs/src/bin/**`

Method: searched Rust sources for current daemon crate paths (`beads_daemon::...`, `beads_daemon_core::...`)
and legacy pre-split paths (`crate::daemon::...`, `beads_rs::daemon::...`), then reviewed each
remaining callsite.

```bash
rg -n --glob '*.rs' 'beads_daemon(::|_core::)|beads_daemon_core::|crate::daemon::|beads_rs::daemon::' \
  crates/beads-rs/src/model \
  crates/beads_stateright_models \
  crates/fuzz \
  crates/beads-rs/tests \
  crates/beads-rs/src/bin
```

Legacy/runtime-public check results:
- no remaining `crate::daemon::...` / `beads_rs::daemon::...` references in scope
- no remaining `beads_daemon::runtime::...` references in scope
- no remaining `beads_daemon::git::...` references in scope

## Coupling Entries

| File path | Daemon symbol/module used | Classification | Migration note |
| --- | --- | --- | --- |
| `crates/beads-rs/src/model/{mod.rs,durability.rs}` | `beads_daemon_core::{durability,repl,wal}` | `shared-core boundary` | Intentional model surface depends on daemon-core protocol/runtime traits; keep constrained to daemon-core rather than `beads_daemon`. |
| `crates/beads-rs/src/bin/tailnet_proxy.rs` | `beads_daemon_core::repl::frame::*` | `shared-core boundary` | Tailnet proxy consumes framing primitives directly; if a narrower wire crate is extracted, migrate there. |
| `crates/beads_stateright_models/examples/durability_idempotency_machine.rs` | `beads_daemon_core::repl::WatermarkState` | `shared-core candidate` | Model checking should continue to consume protocol/state types from daemon-core or a future extracted protocol crate. |
| `crates/fuzz/fuzz_targets/repl_message_decode.rs` | `beads_daemon_core::repl::proto::decode_envelope` | `shared-core candidate` | Keep fuzz target wired to protocol decode API, not daemon runtime exports. |
| `crates/fuzz/fuzz_targets/wal_decode.rs` | `beads_daemon_core::wal::FrameReader` | `shared-core candidate` | Keep fuzz target wired to WAL frame decode API, not daemon runtime exports. |
| `crates/beads-rs/tests/e2e.rs` | `beads_daemon::testkit::e2e::*` | `daemon-owned e2e testkit` | Assembly end-to-end tests intentionally consume the daemon-owned testkit seam rather than reimplementing orchestration in `beads-rs`. |
| `crates/beads-rs/tests/integration/fixtures/{daemon_runtime,realtime}.rs` | `beads_daemon::test_utils::*` | `daemon-internal (product test support)` | Runtime orchestration helpers remain daemon-owned test support used by product-level integration tests. |
| `crates/beads-rs/tests/integration/fixtures/daemon_boundary.rs` | `beads_daemon::testkit::wal::*` | `daemon-internal (narrow WAL bridge)` | Assembly tests keep only the WAL-facing bridge needed to inspect product crash-recovery and replication artifacts. |
| `crates/beads-rs/tests/integration/daemon/crash_recovery.rs` | `beads_daemon::testkit::wal::{IndexDurabilityMode, SqliteWalIndex, WalIndex}` | `assembly crash-recovery e2e` | Product crash-recovery tests verify the shipped daemon via IPC/process seams and use WAL testkit types only to inspect resulting on-disk state. |
| `crates/beads-rs/tests/integration/daemon/store_lock.rs` | `beads_daemon::{daemon_layout_from_paths,testkit::store::*}` | `assembly path/lock integration` | Product path/layout tests intentionally compare `beads-rs` assembly helpers against daemon-owned lock/layout behavior. |

## Audited Paths With No Direct Coupling Entries

| Path | Result |
| --- | --- |
| `crates/beads-rs/tests/integration/repl/**` | Moved to `crates/beads-daemon/tests/repl/**`. |
| `crates/beads-rs/tests/integration/wal/**` | Moved to `crates/beads-daemon/tests/wal/**`. |
| `crates/beads-rs/tests/integration/core/{deps,identity}.rs` | Removed from `beads-rs`; owner coverage now lives in `beads-core` / `beads-git` / `beads-daemon`. |
| `crates/beads-rs/src/bin/**` | No direct `beads_daemon::...` runtime imports beyond `tailnet_proxy.rs` frame protocol usage via `beads_daemon_core`. |

## Count By Classification

| Classification | Count |
| --- | ---: |
| `shared-core boundary` | 2 |
| `shared-core candidate` | 3 |
| `daemon-owned e2e testkit` | 1 |
| `daemon-internal (product test support)` | 1 |
| `daemon-internal (narrow WAL bridge)` | 1 |
| `assembly crash-recovery e2e` | 1 |
| `assembly path/lock integration` | 1 |
| **Total coupling entries** | **10** |

## Current Residuals

1. Product-level `beads-rs` integration and e2e coverage still intentionally depends on
   `beads_daemon::testkit::*` / `beads_daemon::test_utils::*`; those are deliberate test-only seams,
   not public runtime boundary leaks.
2. Protocol consumers are correctly constrained to `beads_daemon_core`; extracting a narrower wire
   or protocol crate could reduce this further if it becomes worthwhile.
3. There are no remaining non-test couplings to `beads_daemon::runtime::*` or `beads_daemon::git::*`.
