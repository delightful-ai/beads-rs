use clap::{Args, Subcommand};
use std::path::PathBuf;

use super::CommandError;
use crate::backend::{
    CliHostBackend, MigrateApplyImportRequest, MigrateDetectRequest, MigrateRefreshRequest,
    MigrateToRequest,
};
use crate::migrate::{GoImportError, import_go_export};
use crate::render::print_json;
use crate::runtime::CliRuntimeCtx;
use crate::validation::{normalize_bead_slug_for, validation_error};
use beads_core::FormatVersion;

#[derive(Subcommand, Debug)]
pub enum MigrateCmd {
    /// Show current store format and whether migration is needed.
    Detect,

    /// Migrate canonical store to the latest supported format version.
    To(MigrateToArgs),

    /// Import/migrate from beads-go export.
    FromGo(MigrateFromGoArgs),
}

#[derive(Args, Debug)]
pub struct MigrateToArgs {
    /// Target format version (must match the latest supported version).
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

pub fn handle<B>(
    ctx: &CliRuntimeCtx,
    cmd: MigrateCmd,
    backend: &B,
) -> std::result::Result<(), B::Error>
where
    B: CliHostBackend,
    B::Error: From<CommandError> + From<GoImportError>,
{
    match cmd {
        MigrateCmd::Detect => {
            let outcome = backend.run_migrate_detect(MigrateDetectRequest {
                repo: ctx.repo.clone(),
            })?;
            print_json_as_backend::<B, _>(&outcome)
        }
        MigrateCmd::To(args) => {
            let latest = FormatVersion::CURRENT.get();
            if args.to != latest {
                return Err(B::Error::from(CommandError::from(validation_error(
                    "to",
                    format!(
                        "unsupported migration target {} (latest supported is {latest})",
                        args.to
                    ),
                ))));
            }

            let outcome = backend.run_migrate_to(MigrateToRequest {
                repo: ctx.repo.clone(),
                to: args.to,
                dry_run: args.dry_run,
                force: args.force,
                no_push: args.no_push,
            })?;

            if !args.dry_run {
                let _ = backend.notify_migrate_refresh(MigrateRefreshRequest::new(ctx.repo_ctx()));
            }

            print_json_as_backend::<B, _>(&outcome)
        }
        MigrateCmd::FromGo(args) => {
            let actor = ctx
                .actor_id()
                .map_err(|err| B::Error::from(CommandError::from(err)))?;
            let root_slug = args
                .root_slug
                .as_deref()
                .map(|slug| normalize_bead_slug_for("root_slug", slug))
                .transpose()
                .map_err(|err| B::Error::from(CommandError::from(err)))?;
            let (imported, report) =
                import_go_export(&args.input, &actor, root_slug).map_err(B::Error::from)?;

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
                print_json_as_backend::<B, _>(&payload)?;
                return Ok(());
            }

            let outcome = backend.run_migrate_apply_import(MigrateApplyImportRequest {
                repo: ctx.repo.clone(),
                imported,
                force: args.force,
                no_push: args.no_push,
            })?;

            if !report.warnings.is_empty() {
                tracing::warn!("warnings:");
                for warning in &report.warnings {
                    tracing::warn!("  - {warning}");
                }
            }

            // Notify daemon to refresh its cached state (if running).
            // Ignore errors: daemon may not be running.
            let _ = backend.notify_migrate_refresh(MigrateRefreshRequest::new(ctx.repo_ctx()));

            let payload = serde_json::json!({
                "dry_run": false,
                "root_slug": report.root_slug,
                "live_beads": report.live_beads,
                "tombstones": report.tombstones,
                "deps": report.deps,
                "notes": report.notes,
                "warnings": report.warnings,
                "commit": outcome.commit_oid,
                "pushed": outcome.pushed,
            });
            print_json_as_backend::<B, _>(&payload)
        }
    }
}

fn print_json_as_backend<B, T: serde::Serialize>(value: &T) -> std::result::Result<(), B::Error>
where
    B: CliHostBackend,
    B::Error: From<CommandError>,
{
    print_json(value).map_err(|err| B::Error::from(CommandError::from(err)))
}
