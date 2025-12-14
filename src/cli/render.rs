//! Human/markdown renderer for CLI outputs.
//!
//! Parity target: beads-go default (non-`--json`) formatting.
//! This module is pure formatting; handlers gather any extra data needed.

use crate::api::{
    BlockedIssue, CountResult, DeletedLookup, DepEdge, EpicStatus, Issue, IssueSummary, Note,
    StatusOutput, Tombstone,
};
use crate::daemon::ipc::ResponsePayload;
use crate::daemon::ops::OpResult;
use crate::daemon::query::QueryResult;

/// Render a daemon response for human output.
pub fn render_human(payload: &ResponsePayload) -> String {
    match payload {
        ResponsePayload::Op(op) => render_op(op),
        ResponsePayload::Query(q) => render_query(q),
        ResponsePayload::Synced => "synced".into(),
        ResponsePayload::Initialized => "initialized".into(),
        ResponsePayload::Pong => "pong".into(),
        ResponsePayload::ShuttingDown => "shutting down".into(),
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

pub fn render_ready(views: &[IssueSummary]) -> String {
    if views.is_empty() {
        return "\n✨ No ready work found\n".into();
    }
    let mut out = String::new();
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
        out.push_str(&format!("\nChildren ({}):\n", incoming.children.len()));
        for dep in &incoming.children {
            out.push_str(&format!(
                "  ↳ {}: {} [P{}]\n",
                dep.id, dep.title, dep.priority
            ));
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
        QueryResult::Notes(notes) => render_notes(notes),
        QueryResult::Status(out) => render_status(out),
        QueryResult::Blocked(blocked) => render_blocked(blocked),
        QueryResult::Stale(issues) => render_issue_list(issues),
        QueryResult::Count(result) => render_count(result),
        QueryResult::Deleted(tombs) => render_deleted(tombs),
        QueryResult::DeletedLookup(out) => render_deleted_lookup(out),
        QueryResult::EpicStatus(statuses) => render_epic_statuses(statuses),
        QueryResult::Validation { warnings } => {
            if warnings.is_empty() {
                "ok".into()
            } else {
                warnings.join("\n")
            }
        }
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
        buf.push_str(&format!(
            "  consecutive_failures: {}\n",
            sync.consecutive_failures
        ));
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
    let mut out = String::new();
    for v in views {
        out.push_str(&render_issue_summary(v));
        out.push('\n');
    }
    out.trim_end().into()
}

fn render_issue_summary(v: &IssueSummary) -> String {
    let mut s = format!("{} [P{}] [{}] {}", v.id, v.priority, v.issue_type, v.status);
    if let Some(a) = &v.assignee
        && !a.is_empty()
    {
        s.push_str(&format!(" @{}", a));
    }
    if !v.labels.is_empty() {
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

fn fmt_wall_ms(ms: u64) -> String {
    use time::OffsetDateTime;
    use time::format_description::parse;

    let fmt = parse("[year]-[month]-[day] [hour]:[minute]").unwrap();
    let dt = OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    dt.format(&fmt).unwrap_or_else(|_| ms.to_string())
}
