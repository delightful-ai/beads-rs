# Boundary API Migration Matrix (`bd-21eg.4`)

Date: 2026-02-07  
Scope: missing boundary APIs for `patch` / `fsck` / `lock` plus CLI migration targets.

## Proposed Boundary Signatures

### `bd-21eg.12` Patch Validation API (`beads-surface`)

```rust
use beads_surface::ops::BeadPatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredPatchField {
    Title,
    Description,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PatchValidationError {
    #[error("cannot clear required field: {field:?}")]
    CannotClearRequiredField { field: RequiredPatchField },
    #[error("cannot set required field to empty: {field:?}")]
    EmptyRequiredField { field: RequiredPatchField },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedBeadPatch {
    inner: BeadPatch,
}

pub fn validate_bead_patch(patch: &BeadPatch) -> Result<(), PatchValidationError>;
pub fn normalize_bead_patch(patch: BeadPatch) -> Result<BeadPatch, PatchValidationError>;

impl TryFrom<BeadPatch> for ValidatedBeadPatch {
    type Error = PatchValidationError;
    fn try_from(patch: BeadPatch) -> Result<Self, Self::Error>;
}

impl ValidatedBeadPatch {
    pub fn as_patch(&self) -> &BeadPatch;
    pub fn into_patch(self) -> BeadPatch;
}
```

### `bd-21eg.13` / `bd-21eg.14` Store Admin Surface API (`beads-surface`)

```rust
use beads_api::{AdminFsckOutput, AdminStoreLockInfoOutput, AdminStoreUnlockOutput};
use beads_core::{ErrorPayload, StoreId};
use beads_surface::ipc::payload::{
    AdminStoreFsckPayload, AdminStoreLockInfoPayload, AdminStoreUnlockPayload,
};
use beads_surface::ipc::{IpcClient, IpcError, ResponsePayload};

#[derive(Debug, thiserror::Error)]
pub enum StoreAdminError {
    #[error(transparent)]
    Ipc(#[from] IpcError),
    #[error("daemon returned error payload")]
    Daemon(ErrorPayload),
    #[error("unexpected response payload for {op}")]
    UnexpectedPayload {
        op: &'static str,
        payload: ResponsePayload,
    },
}

pub trait OfflineStoreAdmin {
    fn fsck_offline(
        &self,
        payload: AdminStoreFsckPayload,
    ) -> Result<AdminFsckOutput, StoreAdminError>;

    fn lock_info_offline(
        &self,
        payload: AdminStoreLockInfoPayload,
    ) -> Result<AdminStoreLockInfoOutput, StoreAdminError>;

    fn unlock_offline(
        &self,
        payload: AdminStoreUnlockPayload,
    ) -> Result<AdminStoreUnlockOutput, StoreAdminError>;
}

pub fn store_fsck(
    client: &IpcClient,
    offline: &impl OfflineStoreAdmin,
    payload: AdminStoreFsckPayload,
) -> Result<AdminFsckOutput, StoreAdminError>;

pub fn store_lock_info(
    client: &IpcClient,
    offline: &impl OfflineStoreAdmin,
    payload: AdminStoreLockInfoPayload,
) -> Result<AdminStoreLockInfoOutput, StoreAdminError>;

pub fn store_unlock(
    client: &IpcClient,
    offline: &impl OfflineStoreAdmin,
    payload: AdminStoreUnlockPayload,
) -> Result<AdminStoreUnlockOutput, StoreAdminError>;
```

## Migration Matrix

| # | Current daemon-internal symbol | Proposed boundary API | Target bead |
| --- | --- | --- | --- |
| 1 | `crate::daemon::ops::BeadPatchDaemonExt::validate_for_daemon` (`crates/beads-rs/src/daemon/ops.rs:636`) | `validate_bead_patch(patch: &BeadPatch) -> Result<(), PatchValidationError>` | `bd-21eg.12` |
| 2 | `crate::daemon::ops::normalize_required_patch` (`crates/beads-rs/src/daemon/ops.rs:737`) | `normalize_bead_patch(patch: BeadPatch) -> Result<BeadPatch, PatchValidationError>` | `bd-21eg.12` |
| 3 | `crate::daemon::ops::ValidatedSurfaceBeadPatch::try_from` (`crates/beads-rs/src/daemon/ops.rs:670`) | `impl TryFrom<BeadPatch> for ValidatedBeadPatch` | `bd-21eg.12` |
| 4 | CLI daemon coupling at `patch.validate_for_daemon()` (`crates/beads-rs/src/cli/commands/update.rs:173`) | CLI uses `validate_bead_patch` / `ValidatedBeadPatch::try_from` only | `bd-21eg.15` |
| 5 | `crate::daemon::wal::fsck::fsck_store` (`crates/beads-rs/src/daemon/wal/fsck.rs:221`) | `OfflineStoreAdmin::fsck_offline(payload) -> Result<AdminFsckOutput, StoreAdminError>` | `bd-21eg.13` |
| 6 | `crate::daemon::admin::fsck_report_to_output` (`crates/beads-rs/src/daemon/admin.rs:750`) | folded into `fsck_offline(...)` return contract (`AdminFsckOutput`) | `bd-21eg.13` |
| 7 | `Daemon::admin_store_fsck` (`crates/beads-rs/src/daemon/admin.rs:603`) | `store_fsck(client, offline, payload) -> Result<AdminFsckOutput, StoreAdminError>` | `bd-21eg.13` |
| 8 | CLI offline fsck fallback (`crates/beads-rs/src/cli/commands/store.rs:111`) calling daemon fsck internals | CLI calls `store_fsck(...)` boundary API | `bd-21eg.15` |
| 9 | `crate::daemon::store::lock::read_lock_meta` (`crates/beads-rs/src/daemon/store/lock.rs:141`) | `OfflineStoreAdmin::lock_info_offline(...)` and `OfflineStoreAdmin::unlock_offline(...)` | `bd-21eg.14` |
| 10 | `crate::daemon::store::lock::remove_lock_file` (`crates/beads-rs/src/daemon/store/lock.rs:155`) | `OfflineStoreAdmin::unlock_offline(...) -> Result<AdminStoreUnlockOutput, StoreAdminError>` | `bd-21eg.14` |
| 11 | `Daemon::admin_store_lock_info` (`crates/beads-rs/src/daemon/admin.rs:630`) | `store_lock_info(client, offline, payload) -> Result<AdminStoreLockInfoOutput, StoreAdminError>` | `bd-21eg.14` |
| 12 | `Daemon::admin_store_unlock` (`crates/beads-rs/src/daemon/admin.rs:650`) | `store_unlock(client, offline, payload) -> Result<AdminStoreUnlockOutput, StoreAdminError>` | `bd-21eg.14` |
| 13 | CLI offline unlock path (`crates/beads-rs/src/cli/commands/store.rs:422`) calling daemon lock internals | CLI calls `store_unlock(...)` boundary API | `bd-21eg.15` |

## Section Summary

- `.12` patch signatures remove `daemon::ops` exposure from CLI and make required-field errors typed (`RequiredPatchField`) instead of stringly.
- `.13` fsck signature set provides one typed entrypoint (`store_fsck`) and one typed offline hook (`fsck_offline`) that returns `beads_api::AdminFsckOutput`.
- `.14` lock signature set mirrors fsck shape for lock-info/unlock with typed `beads_api` outputs.
- `.15` migration rows are strictly callsite rewires in CLI to consume the new boundary APIs; no daemon modules imported from handlers.

## Counts

- Total mapped symbols: **13**
- By domain:
  - Patch: **4**
  - Fsck: **4**
  - Lock: **5**
- By target bead:
  - `bd-21eg.12`: **3**
  - `bd-21eg.13`: **3**
  - `bd-21eg.14`: **4**
  - `bd-21eg.15`: **3**
