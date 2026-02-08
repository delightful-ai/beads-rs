use clap::{Args, Subcommand};

use super::super::{Ctx, print_json};
use crate::Result;
use crate::core::FormatVersion;
use beads_cli::backend::{CliHostBackend, MigrateRefreshRequest};
use beads_cli::validation::{normalize_bead_slug_for, validation_error};
use std::path::PathBuf;

#[derive(Subcommand, Debug)]
pub enum MigrateCmd {
    /// Show current store format and whether migration is needed.
    Detect,

    /// Migrate canonical store to a target format version.
    To(MigrateToArgs),

    /// Import/migrate from beads-go export.
    FromGo(MigrateFromGoArgs),
}

#[derive(Args, Debug)]
pub struct MigrateToArgs {
    /// Target format version.
    pub to: u32,

    /// Preview changes without writing commits.
    #[arg(long)]
    pub dry_run: bool,

    /// Skip safety checks.
    #[arg(long)]
    pub force: bool,

    /// Do not push to remote.
    #[arg(long)]
    pub no_push: bool,
}

#[derive(Args, Debug)]
pub struct MigrateFromGoArgs {
    /// Path to beads-go issues.jsonl (or export bundle).
    #[arg(long, value_name = "PATH")]
    pub input: PathBuf,

    /// Override the bead ID root slug during import (e.g. `myrepo` for `myrepo-abc123`).
    ///
    /// When omitted, the importer preserves whatever slug is present in the export IDs.
    #[arg(long, value_name = "SLUG")]
    pub root_slug: Option<String>,

    /// Preview without writing.
    #[arg(long)]
    pub dry_run: bool,

    /// Skip safety checks.
    #[arg(long)]
    pub force: bool,

    /// Do not push to remote.
    #[arg(long)]
    pub no_push: bool,
}

pub(crate) fn handle(ctx: &Ctx, cmd: MigrateCmd) -> Result<()> {
    match cmd {
        MigrateCmd::Detect => {
            let repo = git2::Repository::discover(&ctx.repo)
                .map_err(|e| crate::git::SyncError::OpenRepo(ctx.repo.clone(), e))?;
            let current = read_current_format_version(&repo)?;
            let latest = FormatVersion::CURRENT.get();
            let payload = serde_json::json!({
                "current_format_version": current,
                "latest_format_version": latest,
                "needs_migration": current != latest,
            });
            print_json(&payload)?;
            Ok(())
        }
        MigrateCmd::To(args) => Err(validation_error(
            "migrate",
            format!(
                "migration to format {} not implemented yet (dry_run={}, force={}, no_push={})",
                args.to, args.dry_run, args.force, args.no_push
            ),
        )
        .into()),
        MigrateCmd::FromGo(args) => {
            use crate::git::SyncProcess;

            let actor = ctx.actor_id()?;
            let root_slug = args
                .root_slug
                .as_deref()
                .map(|slug| normalize_bead_slug_for("root_slug", slug))
                .transpose()?;
            let (imported, report) =
                beads_cli::migrate::import_go_export(&args.input, &actor, root_slug)?;

            if args.dry_run {
                let payload = serde_json::json!({
                    "dry_run": true,
                    "root_slug": report.root_slug,
                    "live_beads": report.live_beads,
                    "tombstones": report.tombstones,
                    "deps": report.deps,
                    "notes": report.notes,
                    "warnings": report.warnings,
                });
                print_json(&payload)?;
                return Ok(());
            }

            let repo = git2::Repository::discover(&ctx.repo)
                .map_err(|e| crate::git::SyncError::OpenRepo(ctx.repo.clone(), e))?;

            if repo.refname_to_id("refs/heads/beads/store").is_ok() && !args.force {
                return Err(validation_error(
                    "migrate",
                    "beads/store already exists; use --force to overwrite via merge",
                )
                .into());
            }

            let committed = SyncProcess::new(ctx.repo.clone())
                .fetch(&repo)?
                .merge(&imported)?
                .commit(&repo)?;
            let commit_oid = committed.commit_oid().to_string();
            let pushed = if !args.no_push {
                let _ = committed.push(&repo)?;
                true
            } else {
                false
            };

            if !report.warnings.is_empty() {
                tracing::warn!("warnings:");
                for w in &report.warnings {
                    tracing::warn!("  - {w}");
                }
            }

            // Notify daemon to refresh its cached state (if running).
            // Ignore errors - daemon may not be running.
            let _ = ctx
                .backend()
                .notify_migrate_refresh(MigrateRefreshRequest::new(ctx.repo_ctx()));

            let payload = serde_json::json!({
                "dry_run": false,
                "root_slug": report.root_slug,
                "live_beads": report.live_beads,
                "tombstones": report.tombstones,
                "deps": report.deps,
                "notes": report.notes,
                "warnings": report.warnings,
                "commit": commit_oid,
                "pushed": pushed,
            });
            print_json(&payload)?;
            Ok(())
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
