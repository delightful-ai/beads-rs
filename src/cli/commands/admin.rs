use clap::{Args, Subcommand};

use super::super::{Ctx, print_ok, send};
use crate::api::{AdminFingerprintMode, AdminFingerprintSample};
use crate::daemon::ipc::Request;
use crate::{Result, WallClock};

#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// Show admin status snapshot.
    Status,
    /// Show admin metrics snapshot.
    Metrics,
    /// Run admin doctor checks.
    Doctor(AdminDoctorArgs),
    /// Run admin scrub now checks.
    #[command(name = "scrub")]
    Scrub(AdminScrubArgs),
    /// Flush WAL for a namespace.
    Flush(AdminFlushArgs),
    /// Show admin fingerprint for divergence detection.
    Fingerprint(AdminFingerprintArgs),
    /// Reload namespace policies from namespaces.toml.
    #[command(name = "reload-policies")]
    ReloadPolicies,
    /// Reload limits from config.toml.
    #[command(name = "reload-limits")]
    ReloadLimits,
    /// Rotate the local replica id.
    #[command(name = "rotate-replica-id")]
    RotateReplicaId,
    /// Toggle maintenance mode.
    Maintenance {
        #[command(subcommand)]
        cmd: AdminMaintenanceCmd,
    },
    /// Rebuild WAL index from segments.
    #[command(name = "rebuild-index")]
    RebuildIndex,
}

#[derive(Subcommand, Debug)]
pub enum AdminMaintenanceCmd {
    /// Enable maintenance mode.
    On,
    /// Disable maintenance mode.
    Off,
}

#[derive(Args, Debug)]
pub struct AdminDoctorArgs {
    /// Max records to sample per namespace.
    #[arg(long = "max-records", default_value_t = 200)]
    pub max_records: u64,
}

#[derive(Args, Debug)]
pub struct AdminScrubArgs {
    /// Max records to sample per namespace.
    #[arg(long = "max-records", default_value_t = 200)]
    pub max_records: u64,
    /// Verify checkpoint cache entries.
    #[arg(long)]
    pub verify_checkpoint_cache: bool,
}

#[derive(Args, Debug)]
pub struct AdminFlushArgs {
    /// Trigger checkpoint immediately for matching groups.
    #[arg(long = "checkpoint-now")]
    pub checkpoint_now: bool,
}

#[derive(Args, Debug)]
pub struct AdminFingerprintArgs {
    /// Sample N shard indices per namespace.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=256))]
    pub sample: Option<u16>,
    /// Sampling nonce (auto-generated if omitted).
    #[arg(long, requires = "sample")]
    pub nonce: Option<String>,
}

pub(crate) fn handle(ctx: &Ctx, cmd: AdminCmd) -> Result<()> {
    match cmd {
        AdminCmd::Status => {
            let req = Request::AdminStatus {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Metrics => {
            let req = Request::AdminMetrics {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Doctor(args) => {
            let req = Request::AdminDoctor {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
                max_records_per_namespace: Some(args.max_records),
                verify_checkpoint_cache: true,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Scrub(args) => {
            let req = Request::AdminScrub {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
                max_records_per_namespace: Some(args.max_records),
                verify_checkpoint_cache: args.verify_checkpoint_cache,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Flush(args) => {
            let req = Request::AdminFlush {
                repo: ctx.repo.clone(),
                namespace: ctx.namespace.as_ref().map(|ns| ns.as_str().to_string()),
                checkpoint_now: args.checkpoint_now,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Fingerprint(args) => {
            let (mode, sample) = match args.sample {
                Some(shard_count) => {
                    let nonce = args
                        .nonce
                        .unwrap_or_else(|| format!("auto-{}", WallClock::now().0));
                    (
                        AdminFingerprintMode::Sample,
                        Some(AdminFingerprintSample { shard_count, nonce }),
                    )
                }
                None => (AdminFingerprintMode::Full, None),
            };
            let req = Request::AdminFingerprint {
                repo: ctx.repo.clone(),
                read: ctx.read_consistency(),
                mode,
                sample,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::ReloadPolicies => {
            let req = Request::AdminReloadPolicies {
                repo: ctx.repo.clone(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::ReloadLimits => {
            let req = Request::AdminReloadLimits {
                repo: ctx.repo.clone(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::RotateReplicaId => {
            let req = Request::AdminRotateReplicaId {
                repo: ctx.repo.clone(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::Maintenance { cmd } => {
            let enabled = matches!(cmd, AdminMaintenanceCmd::On);
            let req = Request::AdminMaintenanceMode {
                repo: ctx.repo.clone(),
                enabled,
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
        AdminCmd::RebuildIndex => {
            let req = Request::AdminRebuildIndex {
                repo: ctx.repo.clone(),
            };
            let ok = send(&req)?;
            print_ok(&ok, ctx.json)
        }
    }
}
