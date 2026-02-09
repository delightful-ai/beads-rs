use beads_cli::backend::{
    CliHostBackend, MigrateApplyImportOutcome, MigrateApplyImportRequest, MigrateDetectRequest,
    MigrateRefreshRequest, StoreFsckRequest, StoreUnlockRequest, UpgradeMethod as CliUpgradeMethod,
    UpgradeOutcome as CliUpgradeOutcome, UpgradeRequest,
};
use beads_surface::ipc::{EmptyPayload, Request, send_request};
use beads_surface::store_admin::{
    StoreAdminCall, StoreAdminCallError, call_store_fsck_no_autostart,
    call_store_unlock_no_autostart, daemon_pid_no_autostart,
};

use crate::config::load_or_init;
use crate::git::SyncProcess;
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

    fn run_migrate_detect(&self, request: MigrateDetectRequest) -> Result<u32> {
        let repo = git2::Repository::discover(&request.repo)
            .map_err(|err| crate::git::SyncError::OpenRepo(request.repo, err))?;
        read_current_format_version(&repo)
    }

    fn run_migrate_apply_import(
        &self,
        request: MigrateApplyImportRequest,
    ) -> Result<MigrateApplyImportOutcome> {
        let repo = git2::Repository::discover(&request.repo)
            .map_err(|err| crate::git::SyncError::OpenRepo(request.repo.clone(), err))?;

        if repo.refname_to_id("refs/heads/beads/store").is_ok() && !request.force {
            return Err(Error::Op(OpError::ValidationFailed {
                field: "migrate".into(),
                reason: "beads/store already exists; use --force to overwrite via merge".into(),
            }));
        }

        let committed = SyncProcess::new(request.repo)
            .fetch(&repo)?
            .merge(&request.imported)?
            .commit(&repo)?;
        let commit_oid = committed.commit_oid().to_string();
        let pushed = if !request.no_push {
            let _ = committed.push(&repo)?;
            true
        } else {
            false
        };

        Ok(MigrateApplyImportOutcome { commit_oid, pushed })
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

fn read_current_format_version(repo: &git2::Repository) -> Result<u32> {
    use git2::ObjectType;

    let oid = repo
        .refname_to_id("refs/heads/beads/store")
        .map_err(|_| crate::git::SyncError::NoLocalRef("refs/heads/beads/store".into()))?;
    let commit = repo.find_commit(oid).map_err(crate::git::SyncError::from)?;
    let tree = commit.tree().map_err(crate::git::SyncError::from)?;
    let meta_entry = tree
        .get_name("meta.json")
        .ok_or_else(|| crate::git::SyncError::MissingFile("meta.json".into()))?;
    let meta_obj = repo
        .find_object(meta_entry.id(), Some(ObjectType::Blob))
        .map_err(crate::git::SyncError::from)?;
    let meta_blob = meta_obj
        .peel_to_blob()
        .map_err(|_| crate::git::SyncError::NotABlob("meta.json"))?;
    let parsed = crate::git::wire::parse_supported_meta(meta_blob.content())
        .map_err(crate::git::SyncError::from)?;
    match parsed.meta() {
        crate::git::wire::StoreMeta::V1 { .. } => Ok(1),
        crate::git::wire::StoreMeta::Legacy => Ok(0),
    }
}
