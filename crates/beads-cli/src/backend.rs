use std::path::PathBuf;

use beads_api::{AdminFsckOutput, AdminStoreUnlockOutput};
use beads_core::StoreId;
use beads_surface::ipc::RepoCtx;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpgradeRequest {
    pub background: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpgradeOutcome {
    pub updated: bool,
    pub from_version: String,
    pub to_version: Option<String>,
    pub install_path: PathBuf,
    pub method: UpgradeMethod,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpgradeMethod {
    Prebuilt,
    Cargo,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreFsckRequest {
    pub store_id: StoreId,
    pub repair: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreUnlockRequest {
    pub store_id: StoreId,
    pub force: bool,
}

#[derive(Debug, Clone)]
pub struct MigrateRefreshRequest {
    pub repo_ctx: RepoCtx,
}

impl MigrateRefreshRequest {
    pub fn new(repo_ctx: RepoCtx) -> Self {
        Self { repo_ctx }
    }
}

pub trait CliHostBackend {
    type Error;

    fn run_upgrade(
        &self,
        request: UpgradeRequest,
    ) -> std::result::Result<UpgradeOutcome, Self::Error>;

    fn run_store_fsck(
        &self,
        request: StoreFsckRequest,
    ) -> std::result::Result<AdminFsckOutput, Self::Error>;

    fn run_store_unlock(
        &self,
        request: StoreUnlockRequest,
    ) -> std::result::Result<AdminStoreUnlockOutput, Self::Error>;

    fn notify_migrate_refresh(
        &self,
        request: MigrateRefreshRequest,
    ) -> std::result::Result<(), Self::Error>;
}
