# Non-CLI Daemon Coupling Inventory (`bd-6ea1.1`)

Audit date: 2026-02-26
Scope audited:
- `crates/beads-rs/src/model/**`
- `crates/beads_stateright_models/**`
- `crates/fuzz/**`
- `crates/beads-rs/tests/**`
- `crates/beads-rs/src/bin/**`

Method: searched Rust sources for current daemon crate paths (`beads_daemon::...`, `beads_daemon_core::...`) and legacy pre-split paths (`crate::daemon::...`, `beads_rs::daemon::...`), then reviewed each callsite.

```bash
rg -n --glob '*.rs' 'beads_daemon(::|_core::)|beads_daemon_core::|crate::daemon::|beads_rs::daemon::' \
  crates/beads-rs/src/model \
  crates/beads_stateright_models \
  crates/fuzz \
  crates/beads-rs/tests \
  crates/beads-rs/src/bin
```

Legacy check result: no remaining `crate::daemon::...` / `beads_rs::daemon::...` references in scope.

## Coupling Entries

| File path | Daemon symbol/module used | Classification | Migration note |
| --- | --- | --- | --- |
| `crates/beads-rs/src/model/{mod.rs,durability.rs}` | `beads_daemon_core::{durability,repl,wal}` | `shared-core boundary` | Intentional model surface depends on daemon-core protocol/runtime traits; keep constrained to daemon-core (not `beads_daemon`). |
| `crates/beads-rs/src/bin/tailnet_proxy.rs` | `beads_daemon_core::repl::frame::*` | `shared-core boundary` | Tailnet proxy consumes framing primitives directly; if a narrower wire crate is extracted, migrate there. |
| `crates/beads_stateright_models/examples/durability_idempotency_machine.rs` | `beads_daemon_core::repl::WatermarkState` | `shared-core candidate` | Model checking should continue to consume protocol/state types from daemon-core (or a future extracted protocol crate). |
| `crates/fuzz/fuzz_targets/repl_message_decode.rs` | `beads_daemon_core::repl::proto::decode_envelope` | `shared-core candidate` | Keep fuzz target wired to protocol decode API, not daemon runtime exports. |
| `crates/fuzz/fuzz_targets/wal_decode.rs` | `beads_daemon_core::wal::FrameReader` | `shared-core candidate` | Keep fuzz target wired to WAL frame decode API, not daemon runtime exports. |
| `crates/beads-rs/tests/integration/daemon/store_lock.rs` | `beads_daemon::testkit::store::{StoreLock, StoreLockError, read_lock_meta}` | `daemon-internal (testkit)` | Store lock behavior remains daemon-owned; testkit is the correct boundary. |
| `crates/beads-rs/tests/integration/fixtures/daemon_boundary.rs` | `beads_daemon::{admission,testkit::repl,testkit::wal}` | `daemon-internal (testkit)` | Fixture intentionally centralizes deep daemon test plumbing via testkit exports. |
| `crates/beads-rs/tests/integration/repl/{ack,backpressure,range}.rs` | `beads_daemon::{admission,testkit::repl,testkit::wal}` | `daemon-internal (testkit)` | Repl/session behavior tests remain daemon runtime policy tests. |
| `crates/beads-rs/tests/integration/wal/{fsck,idempotency,index,receipts,seq,wal_tests}.rs` | `beads_daemon::testkit::wal::*` (+ `fsck::*`) | `daemon-internal (testkit)` | WAL correctness tests intentionally validate daemon WAL internals through testkit seam. |
| `crates/beads-rs/tests/integration/fixtures/{repl_frames,repl_peer,repl_transport}.rs` | `beads_daemon_core::repl::{frame,proto}` | `shared-core (test fixture)` | Wire/protocol fixtures depend on daemon-core protocol types only. |
| `crates/beads-rs/tests/integration/fixtures/{daemon_runtime,realtime}.rs` | `beads_daemon::test_utils::*` | `daemon-internal (test support)` | Runtime orchestration helpers are daemon-owned integration test utilities. |
| `crates/beads-rs/tests/integration/core/identity.rs` | `beads_daemon::remote::normalize_url` | `daemon utility coupling` | Consider extracting URL normalization to shared utility crate if non-daemon consumers grow. |

## Audited Paths With No Direct Coupling Entries

| Path | Result |
| --- | --- |
| `crates/beads-rs/src/bin/**` | No direct `beads_daemon::...` runtime imports beyond `tailnet_proxy.rs` frame protocol usage via `beads_daemon_core`. |

## Count By Classification

| Classification | Count |
| --- | ---: |
| `shared-core boundary` | 2 |
| `shared-core candidate` | 3 |
| `shared-core (test fixture)` | 1 |
| `daemon-internal (testkit)` | 4 |
| `daemon-internal (test support)` | 1 |
| `daemon utility coupling` | 1 |
| **Total coupling entries** | **12** |

## Current Blockers For Further Daemon Split

1. Integration test suites still depend on daemon-owned testkit and test utility surfaces.
2. `core/identity.rs` currently reaches into a daemon utility (`remote::normalize_url`) instead of a neutral shared utility.
3. Protocol consumers are correctly on `beads_daemon_core`, but protocol extraction into a narrower crate could further reduce coupling if desired.
