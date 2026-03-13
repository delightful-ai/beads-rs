use beads_cli::backend::{
    CliHostBackend, DepsFormat, MigrateApplyImportOutcome, MigrateApplyImportRequest,
    MigrateDetectOutcome, MigrateDetectRequest, MigrateRefreshRequest, MigrateToOutcome,
    MigrateToRequest, PushDisposition, StoreFsckRequest, StoreUnlockRequest,
    UpgradeMethod as CliUpgradeMethod, UpgradeOutcome as CliUpgradeOutcome, UpgradeRequest,
};
use beads_core::FormatVersion;
use beads_surface::ipc::{EmptyPayload, Request, send_request};
use beads_surface::store_admin::{
    StoreAdminCall, StoreAdminCallError, call_store_fsck_no_autostart,
    call_store_unlock_no_autostart, daemon_pid_no_autostart,
};
use git2::{ErrorCode, ObjectType, Oid, Repository};

use crate::config::load_or_init;
use crate::upgrade::{UpgradeMethod as HostUpgradeMethod, run_upgrade};
use crate::{Error, OpError, Result};
use beads_git::{SyncError, SyncProcess, sync, wire};

const DEFAULT_MIGRATE_MAX_RETRIES: usize = 3;
const ENV_TESTING: &str = "BD_TESTING";
const ENV_TEST_MIGRATE_MAX_RETRIES: &str = "BD_TEST_MIGRATE_MAX_RETRIES";

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

    fn run_migrate_detect(&self, request: MigrateDetectRequest) -> Result<MigrateDetectOutcome> {
        let repo = Repository::discover(&request.repo)
            .map_err(|err| beads_git::SyncError::OpenRepo(request.repo, err))?;
        detect_store_migration_state(&repo)
    }

    fn run_migrate_to(&self, request: MigrateToRequest) -> Result<MigrateToOutcome> {
        let latest = FormatVersion::CURRENT.get();
        if request.to != latest {
            return Err(Error::Op(OpError::ValidationFailed {
                field: "to".into(),
                reason: format!(
                    "unsupported migration target {} (latest supported is {latest})",
                    request.to
                ),
            }));
        }

        let repo = Repository::discover(&request.repo)
            .map_err(|err| beads_git::SyncError::OpenRepo(request.repo.clone(), err))?;

        let migrated = match sync::migrate_store_ref_to_v1(
            &repo,
            &request.repo,
            request.dry_run,
            request.force,
            request.no_push,
            migrate_max_retries(),
        ) {
            Ok(outcome) => outcome,
            Err(SyncError::NoLocalRef(ref_name)) => {
                return Err(Error::Op(OpError::ValidationFailed {
                    field: "migrate".into(),
                    reason: format!(
                        "{ref_name} is missing; run `bd migrate from-go --input <path>` or sync from remote first"
                    ),
                }));
            }
            Err(SyncError::MigrationWarnings(warnings)) => {
                let summary = warnings
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "legacy deps parse emitted warnings".into());
                return Err(Error::Op(OpError::ValidationFailed {
                    field: "migrate".into(),
                    reason: format!(
                        "legacy deps parse produced {} warning(s); rerun with --force to proceed. first warning: {summary}",
                        warnings.len()
                    ),
                }));
            }
            Err(err) => return Err(err.into()),
        };

        Ok(MigrateToOutcome {
            dry_run: request.dry_run,
            from_effective_version: migrated.from_effective_version,
            to_version: request.to,
            deps_format_before: map_deps_format(migrated.deps_format_before),
            converted_deps: migrated.converted_deps,
            added_notes_file: migrated.added_notes_file,
            wrote_checksums: migrated.wrote_checksums,
            commit_oid: migrated.commit_oid.map(|oid| oid.to_string()),
            push: match migrated.push {
                sync::MigratePushDisposition::Pushed => PushDisposition::Pushed,
                sync::MigratePushDisposition::SkippedNoPush => PushDisposition::SkippedNoPush,
                sync::MigratePushDisposition::SkippedNoRemote => PushDisposition::SkippedNoRemote,
            },
            warnings: migrated.warnings,
        })
    }

    fn run_migrate_apply_import(
        &self,
        request: MigrateApplyImportRequest,
    ) -> Result<MigrateApplyImportOutcome> {
        let repo = git2::Repository::discover(&request.repo)
            .map_err(|err| beads_git::SyncError::OpenRepo(request.repo.clone(), err))?;

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
        let pushed = if !request.no_push && repo.find_remote("origin").is_ok() {
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

fn migrate_max_retries() -> usize {
    if std::env::var_os(ENV_TESTING).as_deref() != Some(std::ffi::OsStr::new("1")) {
        return DEFAULT_MIGRATE_MAX_RETRIES;
    }
    std::env::var(ENV_TEST_MIGRATE_MAX_RETRIES)
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MIGRATE_MAX_RETRIES)
}

fn map_upgrade_method(method: HostUpgradeMethod) -> CliUpgradeMethod {
    match method {
        HostUpgradeMethod::Prebuilt => CliUpgradeMethod::Prebuilt,
        HostUpgradeMethod::Cargo => CliUpgradeMethod::Cargo,
        HostUpgradeMethod::None => CliUpgradeMethod::None,
    }
}

fn map_deps_format(format: beads_git::wire::DepsFormat) -> DepsFormat {
    match format {
        beads_git::wire::DepsFormat::OrSetV1 => DepsFormat::OrSetV1,
        beads_git::wire::DepsFormat::LegacyEdges => DepsFormat::LegacyEdges,
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

fn detect_store_migration_state(repo: &Repository) -> Result<MigrateDetectOutcome> {
    let latest = FormatVersion::CURRENT.get();
    let local_oid = resolve_ref_oid(repo, "refs/heads/beads/store")?;
    let live_remote_preview = match sync::open_remote_store_preview(repo) {
        Ok(preview) => preview,
        Err(beads_git::SyncError::Fetch(err)) if local_oid.is_some() => {
            tracing::warn!(
                error = ?err,
                "remote store preview failed during detect; continuing with local store only"
            );
            None
        }
        Err(err) => return Err(Error::from(err)),
    };
    let remote_oid = live_remote_preview
        .as_ref()
        .map(sync::RemoteStorePreview::oid);

    let mut probes = Vec::new();
    match (local_oid, remote_oid) {
        (None, None) => {}
        (Some(local_oid), Some(remote_oid)) if local_oid == remote_oid => {
            probes.push(probe_store_tree_in_repo(repo, local_oid, None)?);
        }
        (Some(local_oid), Some(remote_oid)) => {
            probes.push(probe_store_tree_in_repo(repo, local_oid, Some("local"))?);
            let remote_repo = live_remote_preview
                .as_ref()
                .map(sync::RemoteStorePreview::repo)
                .unwrap_or(repo);
            probes.push(probe_store_tree_in_repo(
                remote_repo,
                remote_oid,
                Some("remote"),
            )?);
        }
        (Some(local_oid), None) => probes.push(probe_store_tree_in_repo(repo, local_oid, None)?),
        (None, Some(remote_oid)) => {
            let remote_repo = live_remote_preview
                .as_ref()
                .map(sync::RemoteStorePreview::repo)
                .unwrap_or(repo);
            probes.push(probe_store_tree_in_repo(remote_repo, remote_oid, None)?);
        }
    }

    if probes.is_empty() {
        return Ok(MigrateDetectOutcome {
            meta_format_version: None,
            effective_format_version: 0,
            latest_format_version: latest,
            deps_format: DepsFormat::Missing,
            notes_present: false,
            checksums_present: false,
            needs_migration: true,
            reasons: vec!["refs/heads/beads/store is missing".into()],
        });
    }
    let meta_format_version = aggregate_meta_format_version(&probes, latest);
    let deps_format = probes
        .iter()
        .map(|probe| probe.deps_format)
        .max_by_key(|format| deps_format_rank(*format))
        .unwrap_or(DepsFormat::Missing);
    let notes_present = probes.iter().all(|probe| probe.notes_present);
    let checksums_present = probes.iter().all(|probe| probe.checksums_present);
    let mut reasons: Vec<String> = probes.into_iter().flat_map(|probe| probe.reasons).collect();

    let effective_format_version = if meta_format_version == Some(latest)
        && matches!(deps_format, DepsFormat::OrSetV1)
        && notes_present
        && checksums_present
    {
        latest
    } else {
        0
    };
    let needs_migration = effective_format_version != latest;
    reasons.sort();
    reasons.dedup();

    Ok(MigrateDetectOutcome {
        meta_format_version,
        effective_format_version,
        latest_format_version: latest,
        deps_format,
        notes_present,
        checksums_present,
        needs_migration,
        reasons,
    })
}

#[derive(Debug, Clone)]
struct StoreMigrationProbe {
    meta_format_version: Option<u32>,
    deps_format: DepsFormat,
    notes_present: bool,
    checksums_present: bool,
    reasons: Vec<String>,
}

fn probe_store_tree_in_repo(
    repo: &Repository,
    oid: Oid,
    reason_prefix: Option<&str>,
) -> Result<StoreMigrationProbe> {
    let commit = repo.find_commit(oid).map_err(beads_git::SyncError::from)?;
    let tree = commit.tree().map_err(beads_git::SyncError::from)?;

    let notes_present = tree.get_name("notes.jsonl").is_some();
    let mut reasons = Vec::new();

    let (meta_format_version, checksums_present) = match read_meta_probe(repo, &tree)? {
        Some((version, checksums_present)) => (Some(version), checksums_present),
        None => (None, false),
    };
    if !notes_present {
        reasons.push("notes.jsonl missing".into());
    }
    if !checksums_present {
        reasons.push("meta checksums missing".into());
    }

    let deps_format = match read_blob_from_tree(repo, &tree, "deps.jsonl")? {
        Some(deps_bytes) => match wire::parse_deps_wire(&deps_bytes) {
            Ok(_) => DepsFormat::OrSetV1,
            Err(strict_err) => match wire::parse_legacy_deps_edges(&deps_bytes) {
                Ok((_wire, warnings)) => {
                    reasons.extend(warnings);
                    DepsFormat::LegacyEdges
                }
                Err(legacy_err) => {
                    reasons.push(format!(
                        "deps.jsonl parse failed (strict: {strict_err}; legacy: {legacy_err})"
                    ));
                    DepsFormat::Invalid
                }
            },
        },
        None => DepsFormat::Missing,
    };

    match deps_format {
        DepsFormat::OrSetV1 => {}
        DepsFormat::LegacyEdges => {
            reasons.push("deps.jsonl is legacy line-per-edge (missing cc)".into())
        }
        DepsFormat::Missing => reasons.push("deps.jsonl missing".into()),
        DepsFormat::Invalid => {}
    }

    if let Some(prefix) = reason_prefix {
        reasons = reasons
            .into_iter()
            .map(|reason| format!("{prefix}: {reason}"))
            .collect();
    }

    Ok(StoreMigrationProbe {
        meta_format_version,
        deps_format,
        notes_present,
        checksums_present,
        reasons,
    })
}

fn resolve_ref_oid(repo: &Repository, refname: &str) -> Result<Option<Oid>> {
    match repo.refname_to_id(refname) {
        Ok(oid) => Ok(Some(oid)),
        Err(err) if err.code() == ErrorCode::NotFound => Ok(None),
        Err(err) => Err(beads_git::SyncError::Git(err).into()),
    }
}

fn aggregate_meta_format_version(probes: &[StoreMigrationProbe], latest: u32) -> Option<u32> {
    if probes.is_empty() {
        return None;
    }
    if probes
        .iter()
        .all(|probe| probe.meta_format_version == Some(latest))
    {
        return Some(latest);
    }
    if probes
        .iter()
        .any(|probe| probe.meta_format_version == Some(0))
    {
        return Some(0);
    }
    probes.iter().find_map(|probe| probe.meta_format_version)
}

fn deps_format_rank(format: DepsFormat) -> u8 {
    match format {
        DepsFormat::OrSetV1 => 0,
        DepsFormat::Missing => 1,
        DepsFormat::LegacyEdges => 2,
        DepsFormat::Invalid => 3,
    }
}

fn read_meta_probe(repo: &Repository, tree: &git2::Tree<'_>) -> Result<Option<(u32, bool)>> {
    let Some(bytes) = read_blob_from_tree(repo, tree, "meta.json")? else {
        return Ok(None);
    };
    let parsed = wire::parse_supported_meta(&bytes).map_err(beads_git::SyncError::from)?;
    let format_version = match parsed.meta() {
        wire::StoreMeta::Legacy => 0,
        wire::StoreMeta::V1 { .. } => 1,
    };
    let checksums_present = parsed
        .checksums()
        .is_some_and(|checksums| checksums.notes.is_some());
    Ok(Some((format_version, checksums_present)))
}

fn read_blob_from_tree(
    repo: &Repository,
    tree: &git2::Tree<'_>,
    name: &'static str,
) -> Result<Option<Vec<u8>>> {
    let Some(entry) = tree.get_name(name) else {
        return Ok(None);
    };
    let obj = repo
        .find_object(entry.id(), Some(ObjectType::Blob))
        .map_err(beads_git::SyncError::from)?;
    let blob = obj
        .peel_to_blob()
        .map_err(|_| beads_git::SyncError::NotABlob(name))?;
    Ok(Some(blob.content().to_vec()))
}
