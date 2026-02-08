# Non-CLI Daemon Coupling Inventory (`bd-21eg.3`)

Audit date: 2026-02-07  
Scope audited:
- `crates/beads-rs/src/model/**`
- `beads_stateright_models/**`
- `fuzz/**`
- `crates/beads-rs/tests/**`
- `crates/beads-rs/src/bin/**` (no top-level `bins/` directory present)

Method: searched Rust sources for direct `crate::daemon::...` / `beads_rs::daemon::...` references and reviewed callsites/imports.

## Coupling Entries

| File path | Daemon symbol/module used | Classification | Migration note |
| --- | --- | --- | --- |
| `crates/beads-rs/src/model/durability.rs` | `crate::daemon::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll}`, `crate::daemon::ops::OpError` | `shared-core candidate` | Move durability coordinator contract and non-daemon error surface to shared crate for model usage. |
| `crates/beads-rs/src/model/mod.rs` | `crate::daemon::repl::*`, `crate::daemon::repl::gap_buffer::*`, `crate::daemon::wal::{MemoryWalIndex, MemoryWalIndexSnapshot}` | `shared-core candidate` | Extract pure repl/wal state types behind a shared module, then re-export from `model` without `daemon` path dependency. |
| `crates/beads-rs/src/bin/tailnet_proxy.rs` | `beads_rs::daemon::repl::frame::{FrameError, FrameLimitState, FrameReader, FrameWriter}` | `shared-core candidate` | Split frame codec into a reusable wire/protocol crate (`repl-wire` style) consumed by daemon and proxy binary. |
| `crates/beads-rs/tests/e2e.rs` | `beads_rs::daemon::ipc::*`, `beads_rs::daemon::ops::OpResult` | `shared-core candidate` | Route tests through stable API crate types (`beads-api`) instead of daemon internals. |
| `crates/beads-rs/tests/integration/cli/critical_path.rs` | `beads_rs::daemon::ipc::*`, `beads_rs::daemon::ops::OpResult`, `beads_rs::daemon::remote::normalize_url` | `shared-core candidate` | Move IPC/Op types to API surface and URL normalization to shared identity helper. |
| `crates/beads-rs/tests/integration/core/identity.rs` | `beads_rs::daemon::remote::normalize_url` | `shared-core candidate` | Promote normalization helper to core/git identity utility module. |
| `crates/beads-rs/tests/integration/daemon/admin.rs` | `beads_rs::daemon::ipc::*`, `beads_rs::daemon::ops::OpResult` | `shared-core candidate` | Keep daemon behavior tests but consume protocol via API crate boundary. |
| `crates/beads-rs/tests/integration/daemon/admin_status.rs` | `beads_rs::daemon::ipc::IpcClient` | `shared-core candidate` | Expose test-safe IPC client via API/test-support layer. |
| `crates/beads-rs/tests/integration/daemon/crash_recovery.rs` | `beads_rs::daemon::ipc::*` | `shared-core candidate` | Use stable request/response contract crate to avoid daemon module import. |
| `crates/beads-rs/tests/integration/daemon/lifecycle.rs` | `beads_rs::daemon::remote::normalize_url` | `shared-core candidate` | Relocate URL normalization into non-daemon identity utility. |
| `crates/beads-rs/tests/integration/daemon/repl_e2e.rs` | `beads_rs::daemon::ipc::*`, `beads_rs::daemon::ops::OpResult` | `shared-core candidate` | Replace daemon-path protocol imports with API crate protocol imports. |
| `crates/beads-rs/tests/integration/daemon/subscribe.rs` | `beads_rs::daemon::ipc::*`, `beads_rs::daemon::ipc::IpcClient` | `shared-core candidate` | IPC interfaces should be imported from shared API package, not daemon module. |
| `crates/beads-rs/tests/integration/fixtures/admin_status.rs` | `beads_rs::daemon::ipc::*` | `shared-core candidate` | Move fixture protocol types to API-facing test utilities. |
| `crates/beads-rs/tests/integration/fixtures/daemon_runtime.rs` | `beads_rs::daemon::ipc::{Request, Response}` | `shared-core candidate` | Fixture transport helpers should depend on API request/response types. |
| `crates/beads-rs/tests/integration/fixtures/ipc_stream.rs` | `beads_rs::daemon::ipc::*`, `beads_rs::daemon::ipc::ResponsePayload` | `shared-core candidate` | Stream helpers can target shared IPC framing/types once extracted. |
| `crates/beads-rs/tests/integration/fixtures/load_gen.rs` | `beads_rs::daemon::ipc::*` | `shared-core candidate` | Load generator should import protocol from shared API boundary. |
| `crates/beads-rs/tests/integration/fixtures/realtime.rs` | `beads_rs::daemon::ipc::*` | `shared-core candidate` | Realtime fixture should target API IPC client/types rather than daemon module. |
| `crates/beads-rs/tests/integration/fixtures/repl_frames.rs` | `beads_rs::daemon::repl::frame::*`, `beads_rs::daemon::repl::proto::*` | `shared-core candidate` | Repl wire framing/proto are cross-process contracts; extract to shared protocol crate. |
| `crates/beads-rs/tests/integration/fixtures/repl_transport.rs` | `beads_rs::daemon::repl::proto::*` | `shared-core candidate` | Repl envelope/message types should be shared protocol definitions. |
| `crates/beads-rs/tests/integration/daemon/store_lock.rs` | `beads_rs::daemon::store_lock::{StoreLock, StoreLockError, read_lock_meta}` | `daemon-internal` | Keep with daemon runtime ownership/locking internals; expose only behavioral outcomes externally. |
| `crates/beads-rs/tests/integration/fixtures/mutation.rs` | `beads_rs::daemon::mutation_engine::*`, `beads_rs::daemon::ops::*`, `beads_rs::daemon::ipc::*` | `daemon-internal` | Mutation engine orchestration is daemon implementation detail; keep as daemon-scoped tests. |
| `crates/beads-rs/tests/integration/fixtures/repl_peer.rs` | `beads_rs::daemon::admission::*`, `beads_rs::daemon::repl::*`, `beads_rs::daemon::repl::session::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | Session/admission state machines are daemon internals; preserve in daemon test harness crate/module. |
| `crates/beads-rs/tests/integration/fixtures/repl_rig.rs` | `beads_rs::daemon::wal::*`, `beads_rs::daemon::ipc::*`, `beads_rs::daemon::ops::*` | `daemon-internal` | Rig reaches deep WAL/session internals; keep with daemon internals or split into daemon-testkit. |
| `crates/beads-rs/tests/integration/fixtures/store_lock.rs` | `beads_rs::daemon::store_lock::{StoreLockError, remove_lock_file}` | `daemon-internal` | File lock semantics belong to daemon process coordination internals. |
| `crates/beads-rs/tests/integration/fixtures/wal.rs` | `beads_rs::daemon::wal::*`, `beads_rs::daemon::wal::frame::encode_frame` | `daemon-internal` | WAL record/frame encoding currently implementation-specific; keep internal unless protocolized. |
| `crates/beads-rs/tests/integration/fixtures/wal_corrupt.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Corruption/recovery helpers validate daemon WAL implementation details. |
| `crates/beads-rs/tests/integration/repl/ack.rs` | `beads_rs::daemon::admission::*`, `beads_rs::daemon::repl::*`, `beads_rs::daemon::repl::session::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | Ack/session flow control logic is daemon runtime behavior, not shared core contract. |
| `crates/beads-rs/tests/integration/repl/backpressure.rs` | `beads_rs::daemon::admission::*`, `beads_rs::daemon::repl::*`, `beads_rs::daemon::repl::session::*` | `daemon-internal` | Backpressure control is daemon execution policy; keep internal with daemon test suite. |
| `crates/beads-rs/tests/integration/repl/range.rs` | `beads_rs::daemon::repl::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | Range scans are tied to daemon WAL/read path internals. |
| `crates/beads-rs/tests/integration/wal/fsck.rs` | `beads_rs::daemon::wal::fsck::*`, `beads_rs::daemon::wal::*` | `daemon-internal` | FSCK implementation and row-level internals are daemon-owned storage logic. |
| `crates/beads-rs/tests/integration/wal/idempotency.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Idempotency checks are over internal WAL index implementation. |
| `crates/beads-rs/tests/integration/wal/index.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Index rebuild internals remain daemon storage engine implementation detail. |
| `crates/beads-rs/tests/integration/wal/receipts.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Receipt materialization from WAL index is currently daemon-internal storage behavior. |
| `crates/beads-rs/tests/integration/wal/seq.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Sequence validation depends on daemon WAL internals. |
| `crates/beads-rs/tests/integration/wal/wal_tests.rs` | `beads_rs::daemon::wal::*` | `daemon-internal` | Core WAL replay and frame tests are implementation tests for daemon internals. |

## Audited Paths With No Direct Coupling Entries

| Path | Result |
| --- | --- |
| `beads_stateright_models/**` | No Rust source files present (only `.DS_Store`), so no direct daemon symbol references detected. |
| `fuzz/**` | No Rust source files present under current tree (`corpus/`, `artifacts/` only), so no direct daemon symbol references detected. |
| `crates/beads-rs/src/model/event_factory.rs` | No direct `daemon` path reference. |
| `crates/beads-rs/src/model/repl_ingest.rs` | No direct `daemon` path reference. |
| `crates/beads-rs/src/model/digest.rs` | No direct `daemon` path reference. |
| `crates/beads-rs/src/bin/main.rs` | No direct `daemon` module import; daemon coupling is command dispatch/logging behavior only. |

## Count By Classification

| Classification | Count |
| --- | ---: |
| `shared-core candidate` | 19 |
| `daemon-internal` | 16 |
| **Total coupling entries** | **35** |

## Blockers For Daemon Split

1. IPC contract types are imported from `daemon::ipc` across tests/fixtures; there is no fully adopted non-daemon contract path, so split work blocks on API boundary extraction/adoption.
2. Model test adapters in `crates/beads-rs/src/model` re-export daemon repl/wal/durability types directly; this prevents isolating daemon internals without first extracting pure state/algorithm types.
3. URL normalization (`daemon::remote::normalize_url`) is used by non-daemon test paths, indicating identity/git normalization logic is stranded inside daemon module.
4. Repl wire/frame/proto definitions are consumed by non-daemon binary/fixtures (`tailnet_proxy`, repl transport/frame fixtures) but live under daemon module.
5. WAL/repl/admission tests are heavily coupled to daemon implementation types; after split they need either a dedicated daemon-internal test crate or clearly segmented public testkit APIs.
6. Store lock and mutation engine internals are directly referenced by integration fixtures/tests, which hardens implementation details as de-facto API surface unless explicitly fenced.
