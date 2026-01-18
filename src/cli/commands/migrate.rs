use super::super::{Ctx, MigrateCmd, normalize_bead_slug_for, print_json};
use crate::core::FormatVersion;
use crate::daemon::ipc::{Request, send_request};
use crate::{Error, Result};

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
        MigrateCmd::To(args) => Err(Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "migrate".into(),
            reason: format!(
                "migration to format {} not implemented yet (dry_run={}, force={}, no_push={})",
                args.to, args.dry_run, args.force, args.no_push
            ),
        })),
        MigrateCmd::FromGo(args) => {
            use std::time::{SystemTime, UNIX_EPOCH};

            use crate::git::SyncProcess;

            let actor = ctx.actor_id()?;
            let root_slug = args
                .root_slug
                .as_deref()
                .map(|slug| normalize_bead_slug_for("root_slug", slug))
                .transpose()?
                .map(|slug| slug.as_str().to_string());
            let (imported, report) =
                crate::migrate::import_go_export(&args.input, &actor, root_slug)?;

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
                return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                    field: "migrate".into(),
                    reason: "beads/store already exists; use --force to overwrite via merge".into(),
                }));
            }

            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let resolution_stamp =
                crate::core::Stamp::new(crate::core::WriteStamp::new(now_ms, 0), actor.clone());

            let committed = SyncProcess::new(ctx.repo.clone())
                .fetch(&repo)?
                .merge(&imported, resolution_stamp)?
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
            let _ = send_request(&Request::Refresh {
                repo: ctx.repo.clone(),
            });

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
    let parsed =
        crate::git::wire::parse_meta(meta_blob.content()).map_err(crate::git::SyncError::from)?;
    Ok(parsed.format_version)
}
