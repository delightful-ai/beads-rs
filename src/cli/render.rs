//! Human/markdown renderer for CLI outputs.
//!
//! Parity target: beads-go default (non-`--json`) formatting.
//! This module is pure formatting; handlers gather any extra data needed.

use crate::api::{
    AdminClockAnomalyKind, AdminDoctorOutput, AdminFingerprintKind, AdminFingerprintMode,
    AdminFingerprintOutput, AdminFingerprintSample, AdminFlushOutput, AdminHealthReport,
    AdminHealthStatus, AdminMaintenanceModeOutput, AdminMetricsOutput, AdminRebuildIndexOutput,
    AdminReloadPoliciesOutput, AdminRotateReplicaIdOutput, AdminScrubOutput, AdminStatusOutput,
    BlockedIssue, CountResult, DaemonInfo, DeletedLookup, DepEdge, EpicStatus, Issue, IssueSummary,
    Note, StatusOutput, SyncWarning, Tombstone,
};
use crate::core::ReplicaRole;
use crate::daemon::ipc::ResponsePayload;
use crate::daemon::ops::OpResult;
use crate::daemon::query::QueryResult;
use std::sync::LazyLock;

/// Render a daemon response for human output.
pub fn render_human(payload: &ResponsePayload) -> String {
    match payload {
        ResponsePayload::Op(op) => render_op(&op.result),
        ResponsePayload::Query(q) => render_query(q),
        ResponsePayload::Synced(_) => "synced".into(),
        ResponsePayload::Refreshed(_) => "refreshed".into(),
        ResponsePayload::Initialized(_) => "initialized".into(),
        ResponsePayload::ShuttingDown(_) => "shutting down".into(),
        ResponsePayload::Subscribed(sub) => {
            format!("subscribed to {}", sub.subscribed.namespace.as_str())
        }
        ResponsePayload::Event(ev) => {
            format!("event {}", ev.event.event_id.origin_seq.get())
        }
    }
}

// -----------------------------------------------------------------------------
// Command-specific helpers (used by handlers)
// -----------------------------------------------------------------------------

pub fn render_create(issue: &Issue) -> String {
    let mut out = String::new();
    out.push_str(&format!("✓ Created issue: {}\n", issue.id));
    out.push_str(&format!("  Title: {}\n", issue.title));
    out.push_str(&format!("  Priority: P{}\n", issue.priority));
    out.push_str(&format!("  Status: {}", issue.status));
    out
}

pub fn render_updated(id: &str) -> String {
    format!("✓ Updated issue: {id}")
}

pub fn render_ready(views: &[IssueSummary], blocked_count: usize, closed_count: usize) -> String {
    let mut out = String::new();
    if views.is_empty() {
        out.push_str("\n✨ No ready work found\n");
    } else {
        out.push_str(&format!(
            "\n📋 Ready work ({} issues with no blockers):\n\n",
            views.len()
        ));
        for (i, v) in views.iter().enumerate() {
            out.push_str(&format!(
                "{}. [P{}] {}: {}\n",
                i + 1,
                v.priority,
                v.id,
                v.title
            ));
            if let Some(m) = v.estimated_minutes {
                out.push_str(&format!("   Estimate: {} min\n", m));
            }
            if let Some(a) = &v.assignee
                && !a.is_empty()
            {
                out.push_str(&format!("   Assignee: {}\n", a));
            }
        }
        out.push('\n');
    }
    // Always show summary footer so agents understand context.
    out.push_str(&format!(
        "{} blocked, {} closed — run `bd blocked` to see what's stuck\n",
        blocked_count, closed_count
    ));
    out
}

pub fn render_stale(issues: &[IssueSummary], threshold_days: u32) -> String {
    if issues.is_empty() {
        return "\n✨ No stale issues found (all active)\n".into();
    }

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let mut out = format!(
        "\n⏰ Stale issues ({} not updated in {}+ days):\n\n",
        issues.len(),
        threshold_days
    );
    for (i, issue) in issues.iter().enumerate() {
        let updated_ms = issue.updated_at.wall_ms;
        let days_stale = now_ms
            .saturating_sub(updated_ms)
            .saturating_div(24 * 60 * 60 * 1000);

        out.push_str(&format!(
            "{}. [P{}] {}: {}\n",
            i + 1,
            issue.priority,
            issue.id,
            issue.title
        ));
        out.push_str(&format!(
            "   Status: {}, Last updated: {} days ago\n",
            issue.status, days_stale
        ));
        if let Some(a) = &issue.assignee
            && !a.is_empty()
        {
            out.push_str(&format!("   Assignee: {}\n", a));
        }
        out.push('\n');
    }

    out.trim_end().into()
}

pub fn render_comments_list(issue_id: &str, notes: &[Note]) -> String {
    if notes.is_empty() {
        return format!("No comments on {issue_id}");
    }

    let mut out = format!("\nComments on {issue_id}:\n\n");
    for n in notes {
        out.push_str(&format!(
            "[{}] {} at {}\n\n",
            n.author,
            n.content,
            fmt_wall_ms(n.at.wall_ms),
        ));
    }
    out.trim_end().into()
}

pub fn render_label_list(issue_id: &str, labels: &[String]) -> String {
    if labels.is_empty() {
        return format!("\n{issue_id} has no labels\n");
    }
    let mut out = format!("\n🏷 Labels for {issue_id}:\n");
    for l in labels {
        out.push_str(&format!("  - {l}\n"));
    }
    out.push('\n');
    out
}

pub fn render_label_list_all(counts: &std::collections::BTreeMap<String, usize>) -> String {
    if counts.is_empty() {
        return "\nNo labels found in database".into();
    }

    let max_len = counts.keys().map(|s| s.len()).max().unwrap_or(0);
    let mut out = format!("\n🏷 All labels ({} unique):\n", counts.len());
    for (label, count) in counts {
        let padding = " ".repeat(max_len.saturating_sub(label.len()));
        out.push_str(&format!("  {label}{padding}  ({count} issues)\n"));
    }
    out.push('\n');
    out
}

pub fn render_deleted_list(tombs: &[Tombstone], since: &str, all: bool) -> String {
    if tombs.is_empty() {
        if all {
            return "\n✨ No deletions tracked\n".into();
        }
        return format!("\n✨ No deletions in the last {since}\n");
    }

    let mut out = if all {
        format!("\n🗑️ All tracked deletions ({} total):\n\n", tombs.len())
    } else {
        format!(
            "\n🗑️ Deletions in the last {since} ({} total):\n\n",
            tombs.len()
        )
    };

    for t in tombs {
        let ts = fmt_wall_ms(t.deleted_at.wall_ms);
        let reason = t.reason.as_deref().unwrap_or("");
        let reason = if reason.is_empty() {
            "".to_string()
        } else {
            format!("  {reason}")
        };
        out.push_str(&format!(
            "  {:<12}  {}  {:<12}{}\n",
            t.id, ts, t.deleted_by, reason
        ));
    }

    out.trim_end().into()
}

pub fn render_epic_statuses(statuses: &[EpicStatus]) -> String {
    if statuses.is_empty() {
        return "No open epics found".into();
    }

    let mut out = String::new();
    for s in statuses {
        let pct = if s.total_children > 0 {
            (s.closed_children * 100) / s.total_children
        } else {
            0
        };
        let icon = if s.eligible_for_close { "✓" } else { "○" };
        out.push_str(&format!("{icon} {} {}\n", s.epic.id, s.epic.title));
        out.push_str(&format!(
            "   Progress: {}/{} children closed ({}%)\n",
            s.closed_children, s.total_children, pct
        ));
        if s.eligible_for_close {
            out.push_str("   Eligible for closure\n");
        }
        out.push('\n');
    }

    out.trim_end().into()
}

pub fn render_epic_close_dry_run(statuses: &[EpicStatus]) -> String {
    let mut out = format!("Would close {} epic(s):\n", statuses.len());
    for s in statuses {
        out.push_str(&format!("  - {}: {}\n", s.epic.id, s.epic.title));
    }
    out
}

pub fn render_epic_close_result(closed: &[String]) -> String {
    let mut out = format!("✓ Closed {} epic(s)\n", closed.len());
    for id in closed {
        out.push_str(&format!("  - {id}\n"));
    }
    out.trim_end().into()
}

pub struct IncomingGroups {
    pub children: Vec<IssueSummary>,
    pub blocks: Vec<IssueSummary>,
    pub related: Vec<IssueSummary>,
    pub discovered: Vec<IssueSummary>,
}

pub fn render_show(
    bead: &Issue,
    outgoing: &[IssueSummary],
    incoming: &IncomingGroups,
    notes: &[Note],
) -> String {
    let mut out = String::new();
    out.push_str(&format!("\n{}: {}\n", bead.id, bead.title));
    out.push_str(&format!("Status: {}\n", bead.status));
    out.push_str(&format!("Priority: P{}\n", bead.priority));
    out.push_str(&format!("Type: {}\n", bead.issue_type));
    if let Some(a) = &bead.assignee
        && !a.is_empty()
    {
        out.push_str(&format!("Assignee: {}\n", a));
    }
    out.push_str(&format!(
        "Created: {}\n",
        fmt_wall_ms(bead.created_at.wall_ms)
    ));
    out.push_str(&format!(
        "Updated: {}\n",
        fmt_wall_ms(bead.updated_at.wall_ms)
    ));

    if !bead.description.is_empty() {
        out.push_str(&format!("\nDescription:\n{}\n", bead.description));
    }
    if let Some(d) = &bead.design
        && !d.is_empty()
    {
        out.push_str(&format!("\nDesign:\n{}\n", d));
    }
    if let Some(a) = &bead.acceptance_criteria
        && !a.is_empty()
    {
        out.push_str(&format!("\nAcceptance Criteria:\n{}\n", a));
    }

    if !bead.labels.is_empty() {
        out.push_str(&format!("\nLabels: {}\n", fmt_labels(&bead.labels)));
    }

    if !outgoing.is_empty() {
        out.push_str(&format!("\nDepends on ({}):\n", outgoing.len()));
        for dep in outgoing {
            out.push_str(&format!(
                "  → {}: {} [P{}]\n",
                dep.id, dep.title, dep.priority
            ));
        }
    }

    if !incoming.children.is_empty() {
        // For epics, show detailed progress with done/remaining breakdown
        if bead.issue_type == "epic" {
            render_epic_children(&mut out, &incoming.children);
        } else {
            out.push_str(&format!("\nChildren ({}):\n", incoming.children.len()));
            for dep in &incoming.children {
                out.push_str(&format!(
                    "  ↳ {}: {} [P{}]\n",
                    dep.id, dep.title, dep.priority
                ));
            }
        }
    }
    if !incoming.blocks.is_empty() {
        out.push_str(&format!("\nBlocks ({}):\n", incoming.blocks.len()));
        for dep in &incoming.blocks {
            out.push_str(&format!(
                "  ← {}: {} [P{}]\n",
                dep.id, dep.title, dep.priority
            ));
        }
    }
    if !incoming.related.is_empty() {
        out.push_str(&format!("\nRelated ({}):\n", incoming.related.len()));
        for dep in &incoming.related {
            out.push_str(&format!(
                "  ↔ {}: {} [P{}]\n",
                dep.id, dep.title, dep.priority
            ));
        }
    }
    if !incoming.discovered.is_empty() {
        out.push_str(&format!("\nDiscovered ({}):\n", incoming.discovered.len()));
        for dep in &incoming.discovered {
            out.push_str(&format!(
                "  ◊ {}: {} [P{}]\n",
                dep.id, dep.title, dep.priority
            ));
        }
    }

    if !notes.is_empty() {
        out.push_str(&format!("\nComments ({}):\n", notes.len()));
        for n in notes {
            out.push_str(&format!(
                "  [{} at {}]\n  {}\n\n",
                n.author,
                fmt_wall_ms(n.at.wall_ms),
                n.content
            ));
        }
    }

    out.push('\n');
    out
}

/// Render epic children with progress breakdown and priority sorting.
fn render_epic_children(out: &mut String, children: &[IssueSummary]) {
    let mut done: Vec<&IssueSummary> = Vec::new();
    let mut remaining: Vec<&IssueSummary> = Vec::new();

    for child in children {
        if child.status == "closed" {
            done.push(child);
        } else {
            remaining.push(child);
        }
    }

    // Sort remaining by priority (P0 first), then by status (in_progress before open)
    remaining.sort_by(|a, b| {
        a.priority.cmp(&b.priority).then_with(|| {
            // in_progress before open
            let a_prog = a.status == "in_progress";
            let b_prog = b.status == "in_progress";
            b_prog.cmp(&a_prog)
        })
    });

    // Sort done by updated_at (most recent first)
    done.sort_by(|a, b| b.updated_at.wall_ms.cmp(&a.updated_at.wall_ms));

    let total = children.len();
    let done_count = done.len();
    let pct = if total > 0 {
        (done_count * 100) / total
    } else {
        0
    };

    out.push_str(&format!(
        "\nProgress: {}/{} done ({}%)\n",
        done_count, total, pct
    ));

    if !remaining.is_empty() {
        out.push_str(&format!("\nRemaining ({}):\n", remaining.len()));
        for child in &remaining {
            let status_marker = if child.status == "in_progress" {
                ">"
            } else {
                " "
            };
            let assignee = child
                .assignee
                .as_ref()
                .filter(|a| !a.is_empty())
                .map(|a| format!(" @{}", a))
                .unwrap_or_default();
            out.push_str(&format!(
                " {}[P{}] {}: {}{}\n",
                status_marker, child.priority, child.id, child.title, assignee
            ));
        }
    }

    if !done.is_empty() {
        out.push_str(&format!("\nDone ({}):\n", done.len()));
        for child in &done {
            out.push_str(&format!("  [x] {}: {}\n", child.id, child.title));
        }
    }
}

// -----------------------------------------------------------------------------
// Generic response rendering (list/search/dep/status/etc)
// -----------------------------------------------------------------------------

fn render_op(op: &OpResult) -> String {
    match op {
        OpResult::Created { id } => format!("✓ Created issue: {}", id.as_str()),
        OpResult::Updated { id } => format!("✓ Updated issue: {}", id.as_str()),
        OpResult::Closed { id } => format!("✓ Closed {}", id.as_str()),
        OpResult::Reopened { id } => format!("↻ Reopened {}", id.as_str()),
        OpResult::Deleted { id } => format!("✓ Deleted {}", id.as_str()),
        OpResult::DepAdded { from, to } => {
            format!(
                "✓ Added dependency: {} depends on {}",
                from.as_str(),
                to.as_str()
            )
        }
        OpResult::DepRemoved { from, to } => {
            format!(
                "✓ Removed dependency: {} no longer depends on {}",
                from.as_str(),
                to.as_str()
            )
        }
        OpResult::NoteAdded { bead_id, .. } => {
            format!("Comment added to {}", bead_id.as_str())
        }
        OpResult::Claimed { id, expires } => {
            format!("✓ Claimed {} until {}", id.as_str(), expires.0)
        }
        OpResult::Unclaimed { id } => format!("✓ Unclaimed {}", id.as_str()),
        OpResult::ClaimExtended { id, expires } => {
            format!("✓ Claim extended for {} until {}", id.as_str(), expires.0)
        }
    }
}

fn render_query(q: &QueryResult) -> String {
    match q {
        QueryResult::Issue(issue) => render_issue_detail(issue),
        QueryResult::Issues(views) => render_issue_list(views),
        QueryResult::DepTree { root, edges } => render_dep_tree(root.as_str(), edges),
        QueryResult::Deps { incoming, outgoing } => render_deps(incoming, outgoing),
        QueryResult::DepCycles(out) => render_dep_cycles(out),
        QueryResult::Notes(notes) => render_notes(notes),
        QueryResult::Status(out) => render_status(out),
        QueryResult::Blocked(blocked) => render_blocked(blocked),
        QueryResult::Ready(result) => {
            render_ready(&result.issues, result.blocked_count, result.closed_count)
        }
        QueryResult::Stale(issues) => render_issue_list(issues),
        QueryResult::Count(result) => render_count(result),
        QueryResult::Deleted(tombs) => render_deleted(tombs),
        QueryResult::DeletedLookup(out) => render_deleted_lookup(out),
        QueryResult::EpicStatus(statuses) => render_epic_statuses(statuses),
        QueryResult::DaemonInfo(info) => render_daemon_info(info),
        QueryResult::AdminStatus(status) => render_admin_status(status),
        QueryResult::AdminMetrics(metrics) => render_admin_metrics(metrics),
        QueryResult::AdminDoctor(out) => render_admin_doctor(out),
        QueryResult::AdminScrub(out) => render_admin_scrub(out),
        QueryResult::AdminFlush(out) => render_admin_flush(out),
        QueryResult::AdminFingerprint(out) => render_admin_fingerprint(out),
        QueryResult::AdminReloadPolicies(out) => render_admin_reload_policies(out),
        QueryResult::AdminRotateReplicaId(out) => render_admin_rotate_replica_id(out),
        QueryResult::AdminMaintenanceMode(out) => render_admin_maintenance(out),
        QueryResult::AdminRebuildIndex(out) => render_admin_rebuild_index(out),
        QueryResult::Validation { warnings } => {
            if warnings.is_empty() {
                "ok".into()
            } else {
                warnings.join("\n")
            }
        }
    }
}

fn render_dep_cycles(out: &crate::api::DepCycles) -> String {
    if out.cycles.is_empty() {
        return "no dependency cycles found".into();
    }
    let mut lines = Vec::new();
    for cycle in &out.cycles {
        lines.push(format!("cycle: {}", cycle.join(" -> ")));
    }
    lines.join("\n")
}

fn render_daemon_info(info: &DaemonInfo) -> String {
    format!(
        "daemon {} (protocol {}, pid {})",
        info.version, info.protocol_version, info.pid
    )
}

fn render_admin_status(status: &AdminStatusOutput) -> String {
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
            out.push_str(&format!(
                "  {}: {} segments, {} bytes\n",
                ns.namespace.as_str(),
                ns.segment_count,
                ns.total_bytes
            ));
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

fn render_admin_metrics(metrics: &AdminMetricsOutput) -> String {
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

fn render_admin_doctor(out: &AdminDoctorOutput) -> String {
    render_admin_health("Admin Doctor", &out.report)
}

fn render_admin_scrub(out: &AdminScrubOutput) -> String {
    render_admin_health("Admin Scrub", &out.report)
}

fn render_admin_flush(out: &AdminFlushOutput) -> String {
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

fn render_admin_fingerprint(out: &AdminFingerprintOutput) -> String {
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

fn render_admin_reload_policies(out: &AdminReloadPoliciesOutput) -> String {
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

fn render_admin_rotate_replica_id(out: &AdminRotateReplicaIdOutput) -> String {
    format!(
        "replica_id rotated: {} -> {}",
        out.old_replica_id, out.new_replica_id
    )
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

fn render_admin_maintenance(out: &AdminMaintenanceModeOutput) -> String {
    if out.enabled {
        "maintenance mode enabled".to_string()
    } else {
        "maintenance mode disabled".to_string()
    }
}

fn render_admin_rebuild_index(out: &AdminRebuildIndexOutput) -> String {
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

fn render_status(out: &StatusOutput) -> String {
    let s = &out.summary;
    let mut buf = String::new();
    buf.push_str("\nIssue Database Status\n=====================\n\nSummary:\n");
    buf.push_str(&format!("  Total Issues:      {}\n", s.total_issues));
    buf.push_str(&format!("  Open:              {}\n", s.open_issues));
    buf.push_str(&format!("  In Progress:       {}\n", s.in_progress_issues));
    buf.push_str(&format!("  Blocked:           {}\n", s.blocked_issues));
    buf.push_str(&format!("  Closed:            {}\n", s.closed_issues));
    buf.push_str(&format!("  Ready to Work:     {}\n", s.ready_issues));
    if let Some(t) = s.tombstone_issues
        && t > 0
    {
        buf.push_str(&format!("  Deleted:           {} (tombstones)\n", t));
    }
    if let Some(e) = s.epics_eligible_for_closure
        && e > 0
    {
        buf.push_str(&format!("  Epics Ready to Close: {}\n", e));
    }
    if let Some(sync) = &out.sync {
        let last = sync
            .last_sync_wall_ms
            .map(fmt_wall_ms)
            .unwrap_or_else(|| "never".into());
        buf.push_str("\nSync:\n");
        buf.push_str(&format!("  dirty:             {}\n", sync.dirty));
        buf.push_str(&format!("  in_progress:       {}\n", sync.sync_in_progress));
        buf.push_str(&format!("  last_sync:         {}\n", last));
        if let Some(next_retry) = sync.next_retry_wall_ms {
            let mut line = format!("  next_retry:       {}", fmt_wall_ms(next_retry));
            if let Some(in_ms) = sync.next_retry_in_ms {
                line.push_str(&format!(" (in {})", fmt_duration_ms(in_ms)));
            }
            line.push('\n');
            buf.push_str(&line);
        }
        buf.push_str(&format!(
            "  consecutive_failures: {}\n",
            sync.consecutive_failures
        ));
        if !sync.warnings.is_empty() {
            buf.push_str("  warnings:\n");
            for warning in &sync.warnings {
                match warning {
                    SyncWarning::Fetch {
                        message,
                        at_wall_ms,
                    } => {
                        buf.push_str(&format!(
                            "    fetch_error: {} (at {})\n",
                            message,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::Diverged {
                        local_oid,
                        remote_oid,
                        at_wall_ms,
                    } => {
                        buf.push_str(&format!(
                            "    divergence: local {} remote {} (at {})\n",
                            local_oid,
                            remote_oid,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::ForcePush {
                        previous_remote_oid,
                        remote_oid,
                        at_wall_ms,
                    } => {
                        buf.push_str(&format!(
                            "    force_push: {} -> {} (at {})\n",
                            previous_remote_oid,
                            remote_oid,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::ClockSkew {
                        delta_ms,
                        at_wall_ms,
                    } => {
                        let direction = if *delta_ms >= 0 { "ahead" } else { "behind" };
                        let abs_ms = delta_ms.unsigned_abs();
                        buf.push_str(&format!(
                            "    clock_skew: {} ms {} (at {})\n",
                            abs_ms,
                            direction,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                    SyncWarning::WalTailTruncated {
                        namespace,
                        segment_id,
                        truncated_from_offset,
                        at_wall_ms,
                    } => {
                        let segment = segment_id
                            .map(|id| id.to_string())
                            .unwrap_or_else(|| "unknown".to_string());
                        buf.push_str(&format!(
                            "    wal_tail_truncated: {} segment {} offset {} (at {})\n",
                            namespace,
                            segment,
                            truncated_from_offset,
                            fmt_wall_ms(*at_wall_ms)
                        ));
                    }
                }
            }
        }
    }
    buf.push('\n');
    buf
}

fn render_blocked(blocked: &[BlockedIssue]) -> String {
    if blocked.is_empty() {
        return "\n✨ No blocked issues\n".into();
    }

    let mut out = format!("\n🚫 Blocked issues ({}):\n\n", blocked.len());
    for b in blocked {
        out.push_str(&format!(
            "[P{}] {}: {}\n",
            b.issue.priority, b.issue.id, b.issue.title
        ));
        out.push_str(&format!(
            "  Blocked by {} open dependencies: {:?}\n\n",
            b.blocked_by_count, b.blocked_by
        ));
    }
    out.trim_end().into()
}

fn render_count(result: &CountResult) -> String {
    match result {
        CountResult::Simple { count } => format!("{count}"),
        CountResult::Grouped { total, groups } => {
            let mut out = format!("Total: {total}\n\n");
            for g in groups {
                out.push_str(&format!("{}: {}\n", g.group, g.count));
            }
            out.trim_end().into()
        }
    }
}

fn render_deleted(tombs: &[Tombstone]) -> String {
    if tombs.is_empty() {
        return "\n✨ No deletions tracked\n".into();
    }

    let mut out = format!("\n🗑️ Deleted issues ({}):\n\n", tombs.len());
    for t in tombs {
        let ts = fmt_wall_ms(t.deleted_at.wall_ms);
        let reason = t.reason.as_deref().unwrap_or("");
        if reason.is_empty() {
            out.push_str(&format!("  {:<12}  {}  {}\n", t.id, ts, t.deleted_by));
        } else {
            out.push_str(&format!(
                "  {:<12}  {}  {}  {}\n",
                t.id, ts, t.deleted_by, reason
            ));
        }
    }
    out.trim_end().into()
}

fn render_deleted_lookup(out: &DeletedLookup) -> String {
    if !out.found {
        return format!(
            "Issue {} not found in tombstones\n(This could mean the issue was never deleted, or the record was pruned)",
            out.id
        );
    }
    let record = match &out.record {
        Some(r) => r,
        None => {
            return format!(
                "Issue {} not found in tombstones\n(This could mean the issue was never deleted, or the record was pruned)",
                out.id
            );
        }
    };

    let mut buf = format!("\n🗑️ Deletion record for {}:\n\n", out.id);
    buf.push_str(&format!("  ID:        {}\n", record.id));
    buf.push_str(&format!(
        "  Deleted:   {}\n",
        fmt_wall_ms(record.deleted_at.wall_ms)
    ));
    buf.push_str(&format!("  By:        {}\n", record.deleted_by));
    if let Some(reason) = &record.reason
        && !reason.is_empty()
    {
        buf.push_str(&format!("  Reason:    {}\n", reason));
    }
    buf.push('\n');
    buf
}

fn render_notes(notes: &[Note]) -> String {
    if notes.is_empty() {
        return "No comments".into();
    }
    let mut out = String::new();
    out.push_str("\nComments:\n\n");
    for n in notes {
        out.push_str(&format!(
            "[{}] {} at {}\n\n",
            n.author,
            n.content,
            fmt_wall_ms(n.at.wall_ms)
        ));
    }
    out.trim_end().into()
}

fn render_issue_list(views: &[IssueSummary]) -> String {
    render_issue_list_opts(views, false)
}

/// Render issue list with options.
pub fn render_issue_list_opts(views: &[IssueSummary], show_labels: bool) -> String {
    let mut out = String::new();
    for v in views {
        out.push_str(&render_issue_summary_opts(v, show_labels));
        out.push('\n');
    }
    out.trim_end().into()
}

fn render_issue_summary_opts(v: &IssueSummary, show_labels: bool) -> String {
    let mut s = format!("{} [P{}] [{}] {}", v.id, v.priority, v.issue_type, v.status);
    if let Some(a) = &v.assignee
        && !a.is_empty()
    {
        s.push_str(&format!(" @{}", a));
    }
    if show_labels && !v.labels.is_empty() {
        s.push_str(&format!(" {}", fmt_labels(&v.labels)));
    }
    s.push_str(&format!(" - {}", v.title));
    s
}

fn render_issue_detail(v: &Issue) -> String {
    // Default detail renderer (used for `show --json=false` fallback).
    let mut out = String::new();
    out.push_str(&format!("\n{}: {}\n", v.id, v.title));
    out.push_str(&format!("Status: {}\n", v.status));
    out.push_str(&format!("Priority: P{}\n", v.priority));
    out.push_str(&format!("Type: {}\n", v.issue_type));
    if let Some(a) = &v.assignee
        && !a.is_empty()
    {
        out.push_str(&format!("Assignee: {}\n", a));
    }
    out.push_str(&format!("Created: {}\n", fmt_wall_ms(v.created_at.wall_ms)));
    out.push_str(&format!("Updated: {}\n", fmt_wall_ms(v.updated_at.wall_ms)));

    if !v.description.is_empty() {
        out.push_str(&format!("\nDescription:\n{}\n", v.description));
    }
    if let Some(d) = &v.design
        && !d.is_empty()
    {
        out.push_str(&format!("\nDesign:\n{}\n", d));
    }
    if let Some(a) = &v.acceptance_criteria
        && !a.is_empty()
    {
        out.push_str(&format!("\nAcceptance Criteria:\n{}\n", a));
    }
    if !v.labels.is_empty() {
        out.push_str(&format!("\nLabels: {}\n", fmt_labels(&v.labels)));
    }
    if !v.notes.is_empty() {
        out.push_str("\nComments:\n\n");
        for n in &v.notes {
            out.push_str(&format!(
                "[{}] {} at {}\n\n",
                n.author,
                n.content,
                fmt_wall_ms(n.at.wall_ms)
            ));
        }
    }
    out.push('\n');
    out
}

fn render_dep_tree(root: &str, edges: &[DepEdge]) -> String {
    if edges.is_empty() {
        return format!("\n{root} has no dependencies\n");
    }
    let mut out = format!("\n🌲 Dependency tree for {root}:\n\n");
    for e in edges {
        out.push_str(&format!("{} → {} ({})\n", e.from, e.to, e.kind));
    }
    out.push('\n');
    out
}

fn render_deps(incoming: &[DepEdge], outgoing: &[DepEdge]) -> String {
    let mut out = String::new();
    if !outgoing.is_empty() {
        out.push_str(&format!("\nDepends on ({}):\n", outgoing.len()));
        for e in outgoing {
            out.push_str(&format!("  → {} ({})\n", e.to, e.kind));
        }
    }
    if !incoming.is_empty() {
        out.push_str(&format!("\nBlocks ({}):\n", incoming.len()));
        for e in incoming {
            out.push_str(&format!("  ← {} ({})\n", e.from, e.kind));
        }
    }
    if out.is_empty() {
        "no deps".into()
    } else {
        out.trim_end().into()
    }
}

fn fmt_labels(labels: &[String]) -> String {
    let mut out = String::from("[");
    for (i, l) in labels.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(l);
    }
    out.push(']');
    out
}

fn fmt_metric_labels(labels: &[crate::api::AdminMetricLabel]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut out = String::from(" {");
    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(label.key.as_str());
        out.push('=');
        out.push_str(label.value.as_str());
    }
    out.push('}');
    out
}

static WALL_MS_FORMAT: LazyLock<Option<Vec<time::format_description::FormatItem<'static>>>> =
    LazyLock::new(|| time::format_description::parse("[year]-[month]-[day] [hour]:[minute]").ok());

fn fmt_wall_ms(ms: u64) -> String {
    use time::OffsetDateTime;

    let dt = OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    match WALL_MS_FORMAT.as_deref() {
        Some(fmt) => dt.format(fmt).unwrap_or_else(|_| ms.to_string()),
        None => ms.to_string(),
    }
}

fn fmt_duration_ms(ms: u64) -> String {
    if ms < 1000 {
        return format!("{ms}ms");
    }
    let secs = ms as f64 / 1000.0;
    if secs < 60.0 {
        return format!("{secs:.1}s");
    }
    let mins = secs / 60.0;
    if mins < 60.0 {
        return format!("{mins:.1}m");
    }
    let hours = mins / 60.0;
    format!("{hours:.1}h")
}
