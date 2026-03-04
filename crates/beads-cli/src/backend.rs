use std::path::PathBuf;

use beads_api::{AdminFsckOutput, AdminStoreUnlockOutput};
use beads_core::{CanonicalState, StoreId};
use beads_surface::ipc::RepoCtx;
use serde::Serialize;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateDetectRequest {
    pub repo: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum DepsFormat {
    #[serde(rename = "orset_v1")]
    OrSetV1,
    #[serde(rename = "legacy_edges")]
    LegacyEdges,
    #[serde(rename = "missing")]
    Missing,
    #[serde(rename = "invalid")]
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MigrateDetectOutcome {
    pub meta_format_version: Option<u32>,
    pub effective_format_version: u32,
    pub latest_format_version: u32,
    pub deps_format: DepsFormat,
    pub notes_present: bool,
    pub checksums_present: bool,
    pub needs_migration: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateToRequest {
    pub repo: PathBuf,
    pub to: u32,
    pub dry_run: bool,
    pub force: bool,
    pub no_push: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MigrateToOutcome {
    pub dry_run: bool,
    pub from_effective_version: u32,
    pub to_version: u32,
    pub deps_format_before: DepsFormat,
    pub converted_deps: bool,
    pub added_notes_file: bool,
    pub wrote_checksums: bool,
    pub commit_oid: Option<String>,
    pub pushed: bool,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct MigrateApplyImportRequest {
    pub repo: PathBuf,
    pub imported: CanonicalState,
    pub force: bool,
    pub no_push: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateApplyImportOutcome {
    pub commit_oid: String,
    pub pushed: bool,
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

    fn run_migrate_detect(
        &self,
        request: MigrateDetectRequest,
    ) -> std::result::Result<MigrateDetectOutcome, Self::Error>;

    fn run_migrate_to(
        &self,
        request: MigrateToRequest,
    ) -> std::result::Result<MigrateToOutcome, Self::Error>;

    fn run_migrate_apply_import(
        &self,
        request: MigrateApplyImportRequest,
    ) -> std::result::Result<MigrateApplyImportOutcome, Self::Error>;

    fn notify_migrate_refresh(
        &self,
        request: MigrateRefreshRequest,
    ) -> std::result::Result<(), Self::Error>;
}
