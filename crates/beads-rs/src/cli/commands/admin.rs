use clap::{Args, Subcommand};

use super::super::{Ctx, print_ok, send};
use super::{fmt_metric_labels, fmt_wall_ms};
use crate::api::{
    AdminCheckpointOutput, AdminClockAnomalyKind, AdminDoctorOutput, AdminFingerprintKind,
    AdminFingerprintMode, AdminFingerprintOutput, AdminFingerprintSample, AdminFlushOutput,
    AdminHealthReport, AdminHealthStatus, AdminMaintenanceModeOutput, AdminMetricsOutput,
    AdminRebuildIndexOutput, AdminReloadLimitsOutput, AdminReloadPoliciesOutput,
    AdminReloadReplicationOutput, AdminRotateReplicaIdOutput, AdminScrubOutput, AdminStatusOutput,
};
use crate::core::{HeadStatus, ReplicaRole, Watermarks};
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

pub(crate) fn render_admin_status(status: &AdminStatusOutput) -> String {
    let mut out = String::new();
    out.push_str("Admin Status\n============\n\n");
    out.push_str(&format!("Store:   {}\n", status.store_id));
    out.push_str(&format!("Replica: {}\n", status.replica_id));
    if !status.namespaces.is_empty() {
        let ns = status
            .namespaces
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("Namespaces: {}\n", ns));
    }
    render_watermarks_section("applied", &status.watermarks_applied, &mut out);
    render_watermarks_section("durable", &status.watermarks_durable, &mut out);
    if let Some(anomaly) = &status.last_clock_anomaly {
        out.push_str(&format!(
            "Clock anomaly: {} delta={}ms at {}\n",
            clock_anomaly_kind_str(&anomaly.kind),
            anomaly.delta_ms,
            fmt_wall_ms(anomaly.at_wall_ms)
        ));
    }

    if !status.wal.is_empty() {
        out.push_str("\nWAL:\n");
        for ns in &status.wal {
            if ns.growth.window_ms > 0 {
                out.push_str(&format!(
                    "  {}: {} segments, {} bytes (growth {} bytes/s, {} segs/s over {}ms)\n",
                    ns.namespace.as_str(),
                    ns.segment_count,
                    ns.total_bytes,
                    ns.growth.bytes_per_sec,
                    ns.growth.segments_per_sec,
                    ns.growth.window_ms
                ));
            } else {
                out.push_str(&format!(
                    "  {}: {} segments, {} bytes\n",
                    ns.namespace.as_str(),
                    ns.segment_count,
                    ns.total_bytes
                ));
            }
        }
    }

    if !status.wal_warnings.is_empty() {
        out.push_str("\nWAL Warnings:\n");
        for warning in &status.wal_warnings {
            out.push_str(&format!(
                "  {}: {}={} limit={}",
                warning.namespace.as_str(),
                wal_warning_kind_str(warning.kind),
                warning.observed,
                warning.limit
            ));
            if let Some(window_ms) = warning.window_ms {
                out.push_str(&format!(" window={}ms", window_ms));
            }
            out.push('\n');
        }
    }

    if !status.replication.is_empty() {
        out.push_str("\nReplication:\n");
        for peer in &status.replication {
            let last_ack = if peer.last_ack_at_ms == 0 {
                "never".to_string()
            } else {
                fmt_wall_ms(peer.last_ack_at_ms)
            };
            out.push_str(&format!(
                "  {} (last_ack={}, diverged={})\n",
                peer.peer, last_ack, peer.diverged
            ));
            for ns in &peer.lag_by_namespace {
                out.push_str(&format!(
                    "    {}: durable_lag={}, applied_lag={}\n",
                    ns.namespace.as_str(),
                    ns.durable_lag,
                    ns.applied_lag
                ));
            }
        }
    }

    if !status.replica_liveness.is_empty() {
        out.push_str("\nReplica Liveness:\n");
        for entry in &status.replica_liveness {
            let last_seen = if entry.last_seen_ms == 0 {
                "never".to_string()
            } else {
                fmt_wall_ms(entry.last_seen_ms)
            };
            let last_handshake = if entry.last_handshake_ms == 0 {
                "never".to_string()
            } else {
                fmt_wall_ms(entry.last_handshake_ms)
            };
            out.push_str(&format!(
                "  {} (role={}, durability_eligible={}, last_seen={}, last_handshake={})\n",
                entry.replica_id,
                replica_role_str(entry.role),
                entry.durability_eligible,
                last_seen,
                last_handshake
            ));
        }
    }

    if !status.checkpoints.is_empty() {
        out.push_str("\nCheckpoints:\n");
        for group in &status.checkpoints {
            let last = group
                .last_checkpoint_wall_ms
                .map(fmt_wall_ms)
                .unwrap_or_else(|| "never".to_string());
            out.push_str(&format!(
                "  {} (dirty={}, in_flight={}, last={})\n",
                group.group, group.dirty, group.in_flight, last
            ));
        }
    }

    out.trim_end().into()
}

pub(crate) fn render_admin_metrics(metrics: &AdminMetricsOutput) -> String {
    let mut out = String::new();
    out.push_str("Admin Metrics\n=============\n");

    out.push_str("\nCounters:\n");
    if metrics.counters.is_empty() {
        out.push_str("  (none)\n");
    } else {
        for metric in &metrics.counters {
            out.push_str(&format!(
                "  {}{} {}\n",
                metric.name,
                fmt_metric_labels(&metric.labels),
                metric.value
            ));
        }
    }

    out.push_str("\nGauges:\n");
    if metrics.gauges.is_empty() {
        out.push_str("  (none)\n");
    } else {
        for metric in &metrics.gauges {
            out.push_str(&format!(
                "  {}{} {}\n",
                metric.name,
                fmt_metric_labels(&metric.labels),
                metric.value
            ));
        }
    }

    out.push_str("\nHistograms:\n");
    if metrics.histograms.is_empty() {
        out.push_str("  (none)\n");
    } else {
        for metric in &metrics.histograms {
            let p50 = metric
                .p50
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".to_string());
            let p95 = metric
                .p95
                .map(|v| v.to_string())
                .unwrap_or_else(|| "n/a".to_string());
            out.push_str(&format!(
                "  {}{} count={} p50={} p95={}\n",
                metric.name,
                fmt_metric_labels(&metric.labels),
                metric.count,
                p50,
                p95
            ));
        }
    }

    out.trim_end().into()
}

pub(crate) fn render_admin_doctor(out: &AdminDoctorOutput) -> String {
    render_admin_health("Admin Doctor", &out.report)
}

pub(crate) fn render_admin_scrub(out: &AdminScrubOutput) -> String {
    render_admin_health("Admin Scrub", &out.report)
}

pub(crate) fn render_admin_flush(out: &AdminFlushOutput) -> String {
    let mut out_str = String::new();
    out_str.push_str("Admin Flush\n");
    out_str.push_str("===========\n\n");
    out_str.push_str(&format!("Namespace: {}\n", out.namespace.as_str()));
    out_str.push_str(&format!("Flushed at: {}\n", fmt_wall_ms(out.flushed_at_ms)));
    match &out.segment {
        Some(segment) => {
            out_str.push_str("Segment:\n");
            out_str.push_str(&format!("  id:         {}\n", segment.segment_id));
            out_str.push_str(&format!(
                "  created_at: {}\n",
                fmt_wall_ms(segment.created_at_ms)
            ));
            out_str.push_str(&format!("  path:       {}\n", segment.path));
        }
        None => out_str.push_str("Segment: none\n"),
    }
    if out.checkpoint_now {
        out_str.push_str("Checkpoint now: yes\n");
    } else {
        out_str.push_str("Checkpoint now: no\n");
    }
    if !out.checkpoint_groups.is_empty() {
        out_str.push_str("Checkpoint groups:\n");
        for group in &out.checkpoint_groups {
            out_str.push_str(&format!("  {group}\n"));
        }
    }
    out_str.trim_end().into()
}

pub(crate) fn render_admin_checkpoint(out: &AdminCheckpointOutput) -> String {
    let mut out_str = String::new();
    out_str.push_str("Admin Checkpoint\n");
    out_str.push_str("================\n\n");
    out_str.push_str(&format!("Namespace: {}\n", out.namespace.as_str()));
    if out.checkpoint_groups.is_empty() {
        out_str.push_str("Checkpoint groups: none\n");
        return out_str.trim_end().into();
    }
    out_str.push_str("Checkpoint groups:\n");
    for group in &out.checkpoint_groups {
        let last = match group.last_checkpoint_wall_ms {
            Some(ms) => fmt_wall_ms(ms),
            None => "n/a".to_string(),
        };
        out_str.push_str(&format!(
            "  {} dirty={} in_flight={} last={}\n",
            group.group, group.dirty, group.in_flight, last
        ));
    }
    out_str.trim_end().into()
}

pub(crate) fn render_admin_fingerprint(out: &AdminFingerprintOutput) -> String {
    let mut out_str = String::new();
    out_str.push_str("Admin Fingerprint\n");
    out_str.push_str("=================\n\n");
    out_str.push_str(&format!(
        "mode: {}\n",
        fingerprint_mode_str(&out.mode, out.sample.as_ref())
    ));
    if !out.namespaces.is_empty() {
        let ns_list = out
            .namespaces
            .iter()
            .map(|ns| ns.namespace.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        out_str.push_str(&format!("namespaces: {}\n", ns_list));
    }
    for namespace in &out.namespaces {
        out_str.push_str(&format!("\n{}:\n", namespace.namespace.as_str()));
        out_str.push_str(&format!(
            "  roots: state={} tombstones={} deps={} namespace={}\n",
            namespace.state_sha256,
            namespace.tombstones_sha256,
            namespace.deps_sha256,
            namespace.namespace_root
        ));
        if namespace.shards.is_empty() {
            continue;
        }
        for kind in [
            AdminFingerprintKind::State,
            AdminFingerprintKind::Tombstones,
            AdminFingerprintKind::Deps,
        ] {
            let shards = namespace
                .shards
                .iter()
                .filter(|shard| shard.kind == kind)
                .collect::<Vec<_>>();
            if shards.is_empty() {
                continue;
            }
            out_str.push_str(&format!("  {} shards:\n", fingerprint_kind_str(&kind)));
            for shard in shards {
                out_str.push_str(&format!("    {:02x}: {}\n", shard.index, shard.sha256));
            }
        }
    }
    out_str.trim_end().into()
}

pub(crate) fn render_admin_reload_policies(out: &AdminReloadPoliciesOutput) -> String {
    let mut out_str = String::new();
    out_str.push_str("Admin Reload Policies\n");
    out_str.push_str("=====================\n\n");
    if out.applied.is_empty() {
        out_str.push_str("applied: (none)\n");
    } else {
        out_str.push_str("applied:\n");
        for diff in &out.applied {
            out_str.push_str(&format!("  {}:\n", diff.namespace.as_str()));
            for change in &diff.changes {
                out_str.push_str(&format!(
                    "    {}: {} -> {}\n",
                    change.field, change.before, change.after
                ));
            }
        }
    }
    if out.requires_restart.is_empty() {
        out_str.push_str("\nrequires_restart: (none)\n");
    } else {
        out_str.push_str("\nrequires_restart:\n");
        for diff in &out.requires_restart {
            out_str.push_str(&format!("  {}:\n", diff.namespace.as_str()));
            for change in &diff.changes {
                out_str.push_str(&format!(
                    "    {}: {} -> {}\n",
                    change.field, change.before, change.after
                ));
            }
        }
    }
    out_str.trim_end().into()
}

pub(crate) fn render_admin_reload_replication(out: &AdminReloadReplicationOutput) -> String {
    let mut out_str = String::new();
    out_str.push_str("Admin Reload Replication\n");
    out_str.push_str("========================\n\n");
    out_str.push_str(&format!("store_id: {}\n", out.store_id));
    out_str.push_str(&format!(
        "roster_present: {}\n",
        if out.roster_present { "true" } else { "false" }
    ));
    out_str.trim_end().into()
}

pub(crate) fn render_admin_reload_limits(out: &AdminReloadLimitsOutput) -> String {
    let mut out_str = String::new();
    out_str.push_str("Admin Reload Limits\n");
    out_str.push_str("===================\n\n");
    out_str.push_str(&format!("store_id: {}\n", out.store_id));
    out_str.push_str(&format!(
        "requires_restart: {}\n",
        if out.requires_restart {
            "true"
        } else {
            "false"
        }
    ));
    match out.checkpoint_groups_reloaded {
        Some(count) => out_str.push_str(&format!("checkpoint_groups_reloaded: {}\n", count)),
        None => out_str.push_str("checkpoint_groups_reloaded: failed\n"),
    }
    out_str.trim_end().into()
}

pub(crate) fn render_admin_rotate_replica_id(out: &AdminRotateReplicaIdOutput) -> String {
    format!(
        "replica_id rotated: {} -> {}",
        out.old_replica_id, out.new_replica_id
    )
}

pub(crate) fn render_admin_maintenance(out: &AdminMaintenanceModeOutput) -> String {
    if out.enabled {
        "maintenance mode enabled".to_string()
    } else {
        "maintenance mode disabled".to_string()
    }
}

pub(crate) fn render_admin_rebuild_index(out: &AdminRebuildIndexOutput) -> String {
    let stats = &out.stats;
    let mut out = String::new();
    out.push_str("rebuild index complete\n");
    out.push_str(&format!("segments_scanned: {}\n", stats.segments_scanned));
    out.push_str(&format!("records_indexed: {}\n", stats.records_indexed));
    out.push_str(&format!(
        "segments_truncated: {}\n",
        stats.segments_truncated
    ));
    if !stats.tail_truncations.is_empty() {
        out.push_str("tail_truncations:\n");
        for truncation in &stats.tail_truncations {
            out.push_str(&format!(
                "  {} {} @{}\n",
                truncation.namespace.as_str(),
                truncation.segment_id,
                truncation.truncated_from_offset
            ));
        }
    }
    out.trim_end().into()
}

fn render_watermarks_section<K>(label: &str, watermarks: &Watermarks<K>, out: &mut String) {
    if watermarks.namespaces().next().is_none() {
        return;
    }
    out.push_str(&format!("\nWatermarks ({label}):\n"));
    for namespace in watermarks.namespaces() {
        out.push_str(&format!("  {}:\n", namespace.as_str()));
        for (origin, watermark) in watermarks.origins(namespace) {
            out.push_str(&format!(
                "    {}: seq={} head={}\n",
                origin,
                watermark.seq().get(),
                head_status_str(watermark.head())
            ));
        }
    }
}

fn head_status_str(head: HeadStatus) -> &'static str {
    match head {
        HeadStatus::Genesis => "genesis",
        HeadStatus::Known(_) => "known",
        HeadStatus::Unknown => "unknown",
    }
}

fn fingerprint_mode_str(
    mode: &AdminFingerprintMode,
    sample: Option<&AdminFingerprintSample>,
) -> String {
    match mode {
        AdminFingerprintMode::Full => "full".to_string(),
        AdminFingerprintMode::Sample => sample
            .map(|sample| {
                format!(
                    "sample (shards={}, nonce={})",
                    sample.shard_count, sample.nonce
                )
            })
            .unwrap_or_else(|| "sample".to_string()),
    }
}

fn fingerprint_kind_str(kind: &AdminFingerprintKind) -> &'static str {
    match kind {
        AdminFingerprintKind::State => "state",
        AdminFingerprintKind::Tombstones => "tombstones",
        AdminFingerprintKind::Deps => "deps",
    }
}

fn clock_anomaly_kind_str(kind: &AdminClockAnomalyKind) -> &'static str {
    match kind {
        AdminClockAnomalyKind::ForwardJumpClamped => "forward_jump_clamped",
    }
}

fn replica_role_str(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Anchor => "anchor",
        ReplicaRole::Peer => "peer",
        ReplicaRole::Observer => "observer",
    }
}

fn wal_warning_kind_str(kind: crate::api::AdminWalWarningKind) -> &'static str {
    match kind {
        crate::api::AdminWalWarningKind::TotalBytesExceeded => "total_bytes",
        crate::api::AdminWalWarningKind::SegmentCountExceeded => "segment_count",
        crate::api::AdminWalWarningKind::GrowthBytesExceeded => "growth_bytes",
    }
}

fn render_admin_health(title: &str, report: &AdminHealthReport) -> String {
    let mut out = String::new();
    out.push_str(title);
    out.push('\n');
    out.push_str(&"=".repeat(title.len()));
    out.push('\n');
    out.push('\n');

    out.push_str(&format!("checked_at_ms: {}\n", report.checked_at_ms));
    out.push_str(&format!(
        "summary: risk={} safe_to_accept_writes={} safe_to_prune_wal={} safe_to_rebuild_index={}\n",
        health_risk_str(&report.summary.risk),
        report.summary.safe_to_accept_writes,
        report.summary.safe_to_prune_wal,
        report.summary.safe_to_rebuild_index
    ));
    out.push_str(&format!(
        "stats: namespaces={} segments={} records={} index_offsets={} checkpoint_groups={}\n",
        report.stats.namespaces,
        report.stats.segments_checked,
        report.stats.records_checked,
        report.stats.index_offsets_checked,
        report.stats.checkpoint_groups_checked
    ));
    if let Some(anomaly) = &report.last_clock_anomaly {
        out.push_str(&format!(
            "clock_anomaly: {} delta={}ms at {}\n",
            clock_anomaly_kind_str(&anomaly.kind),
            anomaly.delta_ms,
            fmt_wall_ms(anomaly.at_wall_ms)
        ));
    }

    out.push_str("\nchecks:\n");
    for check in &report.checks {
        out.push_str(&format!(
            "  - {}: {} (severity={}, issues={})\n",
            health_check_id_str(&check.id),
            health_status_str(&check.status),
            health_severity_str(&check.severity),
            check.evidence.len()
        ));
        if check.status != AdminHealthStatus::Pass {
            for evidence in &check.evidence {
                let path = evidence
                    .path
                    .as_ref()
                    .map(|p| format!(" path={p}"))
                    .unwrap_or_default();
                let namespace = evidence
                    .namespace
                    .as_ref()
                    .map(|ns| format!(" namespace={}", ns.as_str()))
                    .unwrap_or_default();
                let origin = evidence
                    .origin
                    .as_ref()
                    .map(|id| format!(" origin={id}"))
                    .unwrap_or_default();
                let seq = evidence
                    .seq
                    .map(|seq| format!(" seq={seq}"))
                    .unwrap_or_default();
                let offset = evidence
                    .offset
                    .map(|offset| format!(" offset={offset}"))
                    .unwrap_or_default();
                let segment = evidence
                    .segment_id
                    .map(|segment| format!(" segment_id={segment}"))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "      * {}: {}{path}{namespace}{origin}{seq}{offset}{segment}\n",
                    health_evidence_code_str(&evidence.code),
                    evidence.message
                ));
            }
            if !check.suggested_actions.is_empty() {
                out.push_str("      actions:\n");
                for action in &check.suggested_actions {
                    out.push_str(&format!("        - {action}\n"));
                }
            }
        }
    }

    out.trim_end().into()
}

fn health_status_str(status: &AdminHealthStatus) -> &'static str {
    match status {
        AdminHealthStatus::Pass => "pass",
        AdminHealthStatus::Warn => "warn",
        AdminHealthStatus::Fail => "fail",
    }
}

fn health_severity_str(severity: &crate::api::AdminHealthSeverity) -> &'static str {
    match severity {
        crate::api::AdminHealthSeverity::Low => "low",
        crate::api::AdminHealthSeverity::Medium => "medium",
        crate::api::AdminHealthSeverity::High => "high",
        crate::api::AdminHealthSeverity::Critical => "critical",
    }
}

fn health_risk_str(risk: &crate::api::AdminHealthRisk) -> &'static str {
    match risk {
        crate::api::AdminHealthRisk::Low => "low",
        crate::api::AdminHealthRisk::Medium => "medium",
        crate::api::AdminHealthRisk::High => "high",
        crate::api::AdminHealthRisk::Critical => "critical",
    }
}

fn health_check_id_str(id: &crate::api::AdminHealthCheckId) -> &'static str {
    match id {
        crate::api::AdminHealthCheckId::WalFrames => "wal_frames",
        crate::api::AdminHealthCheckId::WalHashes => "wal_hashes",
        crate::api::AdminHealthCheckId::IndexOffsets => "index_offsets",
        crate::api::AdminHealthCheckId::CheckpointCache => "checkpoint_cache",
    }
}

fn health_evidence_code_str(code: &crate::api::AdminHealthEvidenceCode) -> &'static str {
    match code {
        crate::api::AdminHealthEvidenceCode::SegmentHeaderInvalid => "segment_header_invalid",
        crate::api::AdminHealthEvidenceCode::FrameHeaderInvalid => "frame_header_invalid",
        crate::api::AdminHealthEvidenceCode::FrameTruncated => "frame_truncated",
        crate::api::AdminHealthEvidenceCode::FrameCrcMismatch => "frame_crc_mismatch",
        crate::api::AdminHealthEvidenceCode::RecordDecodeInvalid => "record_decode_invalid",
        crate::api::AdminHealthEvidenceCode::EventBodyDecodeInvalid => "event_body_decode_invalid",
        crate::api::AdminHealthEvidenceCode::RecordHeaderMismatch => "record_header_mismatch",
        crate::api::AdminHealthEvidenceCode::RecordShaMismatch => "record_sha_mismatch",
        crate::api::AdminHealthEvidenceCode::IndexOffsetInvalid => "index_offset_invalid",
        crate::api::AdminHealthEvidenceCode::IndexSegmentMissing => "index_segment_missing",
        crate::api::AdminHealthEvidenceCode::IndexOpenFailed => "index_open_failed",
        crate::api::AdminHealthEvidenceCode::CheckpointCacheInvalid => "checkpoint_cache_invalid",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    use crate::core::{Applied, Durable, NamespaceId, ReplicaId, Seq0, StoreId};

    #[test]
    fn render_admin_status_includes_watermarks() {
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let origin = ReplicaId::new(Uuid::from_bytes([3u8; 16]));

        let mut applied = Watermarks::<Applied>::new();
        applied
            .observe_at_least(
                &NamespaceId::core(),
                &origin,
                Seq0::new(1),
                HeadStatus::Known([1u8; 32]),
            )
            .expect("applied watermark");

        let mut durable = Watermarks::<Durable>::new();
        durable
            .observe_at_least(
                &NamespaceId::core(),
                &origin,
                Seq0::new(2),
                HeadStatus::Known([2u8; 32]),
            )
            .expect("durable watermark");

        let status = AdminStatusOutput {
            store_id,
            replica_id,
            namespaces: vec![NamespaceId::core()],
            watermarks_applied: applied,
            watermarks_durable: durable,
            last_clock_anomaly: None,
            wal: Vec::new(),
            wal_warnings: Vec::new(),
            replication: Vec::new(),
            replica_liveness: Vec::new(),
            checkpoints: Vec::new(),
        };

        let output = render_admin_status(&status);
        let expected = concat!(
            "Admin Status\n",
            "============\n\n",
            "Store:   01010101-0101-0101-0101-010101010101\n",
            "Replica: 02020202-0202-0202-0202-020202020202\n",
            "Namespaces: core\n",
            "\nWatermarks (applied):\n",
            "  core:\n",
            "    03030303-0303-0303-0303-030303030303: seq=1 head=known\n",
            "\nWatermarks (durable):\n",
            "  core:\n",
            "    03030303-0303-0303-0303-030303030303: seq=2 head=known",
        );
        assert_eq!(output, expected);
    }
}
