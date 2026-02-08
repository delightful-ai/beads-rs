use beads_cli::backend::{
    CliHostBackend, MigrateRefreshRequest, StoreFsckRequest, StoreUnlockRequest,
    UpgradeMethod as CliUpgradeMethod, UpgradeOutcome as CliUpgradeOutcome, UpgradeRequest,
};
use beads_surface::ipc::{EmptyPayload, Request, send_request};
use beads_surface::store_admin::{
    StoreAdminCall, StoreAdminCallError, call_store_fsck_no_autostart,
    call_store_unlock_no_autostart, daemon_pid_no_autostart,
};

use crate::config::load_or_init;
use crate::upgrade::{UpgradeMethod as HostUpgradeMethod, run_upgrade};
use crate::{Error, OpError, Result};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct BeadsRsCliBackend;

impl CliHostBackend for BeadsRsCliBackend {
    type Error = Error;

    fn run_upgrade(&self, request: UpgradeRequest) -> Result<CliUpgradeOutcome> {
        let cfg = load_or_init();
        let outcome = run_upgrade(cfg, request.background)?;
        Ok(CliUpgradeOutcome {
            updated: outcome.updated,
            from_version: outcome.from_version,
            to_version: outcome.to_version,
            install_path: outcome.install_path,
            method: map_upgrade_method(outcome.method),
        })
    }

    fn run_store_fsck(&self, request: StoreFsckRequest) -> Result<beads_api::AdminFsckOutput> {
        let output = match call_store_fsck_no_autostart(request.store_id, request.repair)
            .map_err(map_store_admin_call_error)?
        {
            StoreAdminCall::Output(output) => output,
            StoreAdminCall::OfflineFallback => {
                crate::store_admin::offline_store_fsck_output(request.store_id, request.repair)?
            }
        };
        Ok(output)
    }

    fn run_store_unlock(
        &self,
        request: StoreUnlockRequest,
    ) -> Result<beads_api::AdminStoreUnlockOutput> {
        let output = match call_store_unlock_no_autostart(request.store_id, request.force)
            .map_err(map_store_admin_call_error)?
        {
            StoreAdminCall::Output(output) => output,
            StoreAdminCall::OfflineFallback => crate::store_admin::offline_store_unlock_output(
                request.store_id,
                request.force,
                daemon_pid_no_autostart(),
            )?,
        };
        Ok(output)
    }

    fn notify_migrate_refresh(&self, request: MigrateRefreshRequest) -> Result<()> {
        send_request(&Request::Refresh {
            ctx: request.repo_ctx,
            payload: EmptyPayload {},
        })
        .map(|_| ())
        .map_err(Error::from)
    }
}

fn map_upgrade_method(method: HostUpgradeMethod) -> CliUpgradeMethod {
    match method {
        HostUpgradeMethod::Prebuilt => CliUpgradeMethod::Prebuilt,
        HostUpgradeMethod::Cargo => CliUpgradeMethod::Cargo,
        HostUpgradeMethod::None => CliUpgradeMethod::None,
    }
}

fn map_store_admin_call_error(err: StoreAdminCallError) -> Error {
    match err {
        StoreAdminCallError::Ipc(err) => Error::Ipc(err),
        StoreAdminCallError::InvalidRequest { field, reason } => {
            Error::Op(OpError::InvalidRequest { field, reason })
        }
    }
}
