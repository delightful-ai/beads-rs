# CLI -> Daemon Direct Import Inventory

## Scope

- Tree scanned: `crates/beads-rs/src/cli/**/*.rs`
- Inventory rule: direct imports matching `use crate::daemon::...`
- Goal: replace CLI-to-daemon imports with boundary-safe targets (`beads-surface`, `beads-api`, wrapper, or missing API)

## Inventory (Direct Imports)

| # | File | Symbol/import | Replacement target | Boundary note |
|---|---|---|---|---|
| 1 | `crates/beads-rs/src/cli/commands/admin.rs:13` | `crate::daemon::ipc::{AdminDoctorPayload, AdminFingerprintPayload, AdminFlushPayload, AdminMaintenanceModePayload, AdminOp, AdminScrubPayload, EmptyPayload, Request}` | `beads-surface` | IPC payload/enum types are surface IPC schema. |
| 2 | `crates/beads-rs/src/cli/commands/blocked.rs:4` | `crate::daemon::ipc::{EmptyPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 3 | `crates/beads-rs/src/cli/commands/claim.rs:6` | `crate::daemon::ipc::{ClaimPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 4 | `crates/beads-rs/src/cli/commands/close.rs:6` | `crate::daemon::ipc::{ClosePayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 5 | `crates/beads-rs/src/cli/commands/comments.rs:8` | `crate::daemon::ipc::{AddNotePayload, IdPayload, Request, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 6 | `crates/beads-rs/src/cli/commands/count.rs:6` | `crate::daemon::ipc::{CountPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 7 | `crates/beads-rs/src/cli/commands/count.rs:7` | `crate::daemon::query::Filters` | `beads-surface` | Query filter type is surface query schema. |
| 8 | `crates/beads-rs/src/cli/commands/create.rs:13` | `crate::daemon::ipc::{CreatePayload, Request, Response, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 9 | `crates/beads-rs/src/cli/commands/create.rs:14` | `crate::daemon::ops::OpResult` | `beads-surface` | Operation result enum is surface ops schema. |
| 10 | `crates/beads-rs/src/cli/commands/delete.rs:7` | `crate::daemon::ipc::{DeletePayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 11 | `crates/beads-rs/src/cli/commands/deleted.rs:8` | `crate::daemon::ipc::{DeletedPayload, Request, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 12 | `crates/beads-rs/src/cli/commands/dep.rs:8` | `crate::daemon::ipc::{DepPayload, EmptyPayload, IdPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 13 | `crates/beads-rs/src/cli/commands/epic.rs:9` | `crate::daemon::ipc::{ClosePayload, EpicStatusPayload, Request, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 14 | `crates/beads-rs/src/cli/commands/init.rs:3` | `crate::daemon::ipc::{EmptyPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 15 | `crates/beads-rs/src/cli/commands/label.rs:10` | `crate::daemon::query::Filters` | `beads-surface` | Query filter type is surface query schema. |
| 16 | `crates/beads-rs/src/cli/commands/label.rs:9` | `crate::daemon::ipc::{LabelsPayload, ListPayload, Request, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 17 | `crates/beads-rs/src/cli/commands/list.rs:10` | `crate::daemon::query::Filters` | `beads-surface` | Query filter type is surface query schema. |
| 18 | `crates/beads-rs/src/cli/commands/list.rs:9` | `crate::daemon::ipc::{ListPayload, Request, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 19 | `crates/beads-rs/src/cli/commands/migrate.rs:7` | `crate::daemon::ipc::{EmptyPayload, Request, send_request}` | `wrapper` | Keep raw send policy in one CLI transport wrapper over surface client. |
| 20 | `crates/beads-rs/src/cli/commands/onboard.rs:14` | `crate::daemon::ipc::{EmptyPayload, RepoCtx, Request, Response, send_request}` | `wrapper` | Bootstrapping send path should go through a CLI wrapper, not direct transport calls. |
| 21 | `crates/beads-rs/src/cli/commands/ready.rs:7` | `crate::daemon::ipc::{ReadyPayload, Request, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 22 | `crates/beads-rs/src/cli/commands/reopen.rs:6` | `crate::daemon::ipc::{IdPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 23 | `crates/beads-rs/src/cli/commands/search.rs:5` | `crate::daemon::ipc::{ListPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 24 | `crates/beads-rs/src/cli/commands/search.rs:6` | `crate::daemon::query::Filters` | `beads-surface` | Query filter type is surface query schema. |
| 25 | `crates/beads-rs/src/cli/commands/show.rs:10` | `crate::daemon::Filters` | `beads-surface` | Alias currently re-exported from daemon; import query type directly from surface. |
| 26 | `crates/beads-rs/src/cli/commands/show.rs:11` | `crate::daemon::ipc::{IdPayload, ListPayload, Request, ResponsePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 27 | `crates/beads-rs/src/cli/commands/stale.rs:7` | `crate::daemon::ipc::{Request, ResponsePayload, StalePayload}` | `beads-surface` | IPC request/response envelope is surface-owned. |
| 28 | `crates/beads-rs/src/cli/commands/status.rs:4` | `crate::daemon::ipc::{EmptyPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 29 | `crates/beads-rs/src/cli/commands/store.rs:16` | `crate::daemon::ipc::{Response, ResponsePayload, send_request_no_autostart}` | `wrapper` | No-autostart transport policy should be centralized in a CLI wrapper. |
| 30 | `crates/beads-rs/src/cli/commands/store.rs:578` | `crate::daemon::wal::fsck::{FsckCheck, FsckCheckId, FsckEvidence, FsckEvidenceCode, FsckRepair, FsckRepairKind, FsckReport, FsckRisk, FsckSeverity, FsckStats, FsckStatus, FsckSummary}` | `beads-api` | Test fixture can be expressed in stable API output types; avoid daemon WAL internals in CLI tests. |
| 31 | `crates/beads-rs/src/cli/commands/subscribe.rs:6` | `crate::daemon::ipc::{EmptyPayload, Request, Response, subscribe_stream}` | `wrapper` | Streaming transport lifecycle belongs in one wrapper over surface client. |
| 32 | `crates/beads-rs/src/cli/commands/sync.rs:3` | `crate::daemon::ipc::{EmptyPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 33 | `crates/beads-rs/src/cli/commands/unclaim.rs:6` | `crate::daemon::ipc::{IdPayload, Request}` | `beads-surface` | Request payload types are in surface IPC boundary. |
| 34 | `crates/beads-rs/src/cli/commands/update.rs:12` | `crate::daemon::ipc::{AddNotePayload, ClaimPayload, ClosePayload, DepPayload, IdPayload, LabelsPayload, ParentPayload, Request, ResponsePayload, UpdatePayload}` | `beads-surface` | IPC payload and envelope types are surface-owned. |
| 35 | `crates/beads-rs/src/cli/commands/update.rs:16` | `crate::daemon::ops::BeadPatchDaemonExt` | `missing API` | CLI uses daemon-only patch validation trait. Boundary-safe validator is not exposed in surface/api. |
| 36 | `crates/beads-rs/src/cli/filters.rs:6` | `crate::daemon::query::Filters` | `beads-surface` | Query filter type is surface query schema. |
| 37 | `crates/beads-rs/src/cli/mod.rs:20` | `crate::daemon::ipc::{IdPayload, IpcClient, IpcConnection, MutationCtx, MutationMeta, ReadConsistency, ReadCtx, RepoCtx, Request, Response, ResponsePayload}` | `wrapper` | Connection reuse/autostart/error-retry behavior should be encapsulated behind a CLI transport wrapper. |
| 38 | `crates/beads-rs/src/cli/parsers.rs:5` | `crate::daemon::query::SortField` | `beads-surface` | Sort enum is surface query schema. |

## Missing Boundary APIs

### 1) Surface patch validation for CLI updates

- Import currently used: `crate::daemon::ops::BeadPatchDaemonExt` (`crates/beads-rs/src/cli/commands/update.rs:16`)
- Missing boundary: a non-daemon validator for `beads_surface::ops::BeadPatch`
- Why this is missing: CLI currently calls `patch.validate_for_daemon()` through a daemon extension trait; this leaks daemon-only logic into CLI.
- Replacement shape (boundary-safe):
  - Either `beads-surface` exposes validation (for example, `BeadPatch::validate_for_update_request()`), or
  - `beads-api`/CLI adds a typed wrapper validator that performs the same constraints before emitting IPC payloads.

## Replacement Breakdown

- `beads-surface`: 31 imports
- `wrapper`: 5 imports
- `beads-api`: 1 import
- `missing API`: 1 import
- **Total direct daemon imports inventoried:** 38
