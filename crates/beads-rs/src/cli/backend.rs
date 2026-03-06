use beads_cli::backend::{
    CliHostBackend, DepsFormat, MigrateApplyImportOutcome, MigrateApplyImportRequest,
    MigrateDetectOutcome, MigrateDetectRequest, MigrateRefreshRequest, MigrateToOutcome,
    MigrateToRequest, StoreFsckRequest, StoreUnlockRequest, UpgradeMethod as CliUpgradeMethod,
    UpgradeOutcome as CliUpgradeOutcome, UpgradeRequest,
};
use beads_core::FormatVersion;
use beads_surface::ipc::{EmptyPayload, Request, send_request};
use beads_surface::store_admin::{
    StoreAdminCall, StoreAdminCallError, call_store_fsck_no_autostart,
    call_store_unlock_no_autostart, daemon_pid_no_autostart,
};
use git2::{ObjectType, Oid, Repository};

use crate::config::load_or_init;
use crate::upgrade::{UpgradeMethod as HostUpgradeMethod, run_upgrade};
use crate::{Error, OpError, Result};
use beads_git::{
    SyncError, SyncProcess, fetch_store_ref, migrate_store_ref_to_v1, refname_to_id_optional, wire,
};

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
        let before = detect_store_migration_state(&repo)?;

        let migrated = match migrate_store_ref_to_v1(
            &repo,
            &request.repo,
            request.dry_run,
            request.force,
            request.no_push,
            3,
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
            from_effective_version: before.effective_format_version,
            to_version: request.to,
            deps_format_before: before.deps_format,
            converted_deps: migrated.converted_deps,
            added_notes_file: migrated.added_notes_file,
            wrote_checksums: migrated.wrote_checksums,
            commit_oid: migrated.commit_oid.map(|oid| oid.to_string()),
            pushed: migrated.pushed,
            warnings: migrated.warnings,
        })
    }

    fn run_migrate_apply_import(
        &self,
        request: MigrateApplyImportRequest,
    ) -> Result<MigrateApplyImportOutcome> {
        let repo = Repository::discover(&request.repo)
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

#[derive(Clone, Debug)]
struct StoreRefProbe {
    meta_format_version: Option<u32>,
    deps_format: DepsFormat,
    notes_present: bool,
    checksums_present: bool,
    reasons: Vec<String>,
}

fn detect_store_migration_state(repo: &Repository) -> Result<MigrateDetectOutcome> {
    let latest = FormatVersion::CURRENT.get();
    let local_oid = refname_to_id_optional(repo, "refs/heads/beads/store")?;

    let mut reasons = Vec::new();
    if let Err(err) = fetch_store_ref(repo) {
        reasons.push(format!(
            "remote beads/store fetch failed before detect: {err}"
        ));
    }
    let remote_oid = refname_to_id_optional(repo, "refs/remotes/origin/beads/store")?;
    let local_probe = match local_oid {
        Some(oid) => Some(probe_store_ref(repo, oid, "local")?),
        None => {
            reasons.push("refs/heads/beads/store is missing".into());
            None
        }
    };
    let remote_probe = match remote_oid {
        Some(oid) => Some(probe_store_ref(repo, oid, "remote")?),
        None => None,
    };

    if local_probe.is_none() && remote_probe.is_none() {
        return Ok(MigrateDetectOutcome {
            meta_format_version: None,
            effective_format_version: 0,
            latest_format_version: latest,
            deps_format: DepsFormat::Missing,
            notes_present: false,
            checksums_present: false,
            needs_migration: true,
            reasons,
        });
    }

    let probes = [local_probe.as_ref(), remote_probe.as_ref()];
    let existing: Vec<&StoreRefProbe> = probes.into_iter().flatten().collect();
    for probe in &existing {
        reasons.extend(probe.reasons.clone());
    }

    let meta_format_version = existing
        .iter()
        .map(|probe| probe.meta_format_version)
        .min()
        .flatten();
    let notes_present = existing.iter().all(|probe| probe.notes_present);
    let checksums_present = existing.iter().all(|probe| probe.checksums_present);
    let deps_format = aggregate_deps_format(existing.iter().map(|probe| probe.deps_format));
    let effective_format_version = if !existing.is_empty()
        && existing
            .iter()
            .all(|probe| probe.meta_format_version == Some(latest))
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

fn probe_store_ref(repo: &Repository, oid: Oid, label: &str) -> Result<StoreRefProbe> {
    let commit = repo.find_commit(oid).map_err(beads_git::SyncError::from)?;
    let tree = commit.tree().map_err(beads_git::SyncError::from)?;
    let notes_present = tree.get_name("notes.jsonl").is_some();
    let mut reasons = Vec::new();

    let (meta_format_version, checksums_present) = match read_meta_probe(repo, &tree)? {
        Some((version, checksums_present)) => (Some(version), checksums_present),
        None => {
            reasons.push(format!("{label} meta.json missing"));
            (None, false)
        }
    };
    if !notes_present {
        reasons.push(format!("{label} notes.jsonl missing"));
    }
    if !checksums_present {
        reasons.push(format!("{label} meta checksums missing"));
    }

    let deps_format = match read_blob_from_tree(repo, &tree, "deps.jsonl")? {
        Some(deps_bytes) => match wire::parse_deps_wire(&deps_bytes) {
            Ok(_) => DepsFormat::OrSetV1,
            Err(strict_err) => match wire::parse_legacy_deps_edges(&deps_bytes) {
                Ok((_wire, warnings)) => {
                    reasons.extend(
                        warnings
                            .into_iter()
                            .map(|warning| format!("{label} {warning}")),
                    );
                    reasons.push(format!(
                        "{label} deps.jsonl is legacy line-per-edge (missing cc)"
                    ));
                    DepsFormat::LegacyEdges
                }
                Err(legacy_err) => {
                    reasons.push(format!(
                        "{label} deps.jsonl parse failed (strict: {strict_err}; legacy: {legacy_err})"
                    ));
                    DepsFormat::Invalid
                }
            },
        },
        None => {
            reasons.push(format!("{label} deps.jsonl missing"));
            DepsFormat::Missing
        }
    };

    Ok(StoreRefProbe {
        meta_format_version,
        deps_format,
        notes_present,
        checksums_present,
        reasons,
    })
}

fn aggregate_deps_format(formats: impl Iterator<Item = DepsFormat>) -> DepsFormat {
    let mut saw_any = false;
    let mut aggregate = DepsFormat::OrSetV1;
    for format in formats {
        saw_any = true;
        aggregate = match (aggregate, format) {
            (_, DepsFormat::Invalid) => DepsFormat::Invalid,
            (DepsFormat::Invalid, _) => DepsFormat::Invalid,
            (_, DepsFormat::LegacyEdges) => DepsFormat::LegacyEdges,
            (DepsFormat::LegacyEdges, _) => DepsFormat::LegacyEdges,
            (_, DepsFormat::Missing) => DepsFormat::Missing,
            (DepsFormat::Missing, DepsFormat::OrSetV1) => DepsFormat::Missing,
            (_, DepsFormat::OrSetV1) => aggregate,
        };
    }
    if saw_any {
        aggregate
    } else {
        DepsFormat::Missing
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
