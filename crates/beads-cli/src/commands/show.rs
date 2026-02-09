use clap::Args;
use std::sync::LazyLock;

use super::common::{fmt_issue_ref, fmt_labels};
use super::{CommandResult, print_ok};
use crate::render::print_line;
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::normalize_bead_id;
use beads_api::{Issue, IssueSummary, Note, QueryResult};
use beads_core::{BeadId, BeadType, WorkflowStatus};
use beads_surface::Filters;
use beads_surface::ipc::{IdPayload, ListPayload, Request, ResponsePayload};
use std::collections::{BTreeSet, HashMap};

static WALL_MS_FORMAT: LazyLock<Option<Vec<time::format_description::FormatItem<'static>>>> =
    LazyLock::new(|| time::format_description::parse("[year]-[month]-[day] [hour]:[minute]").ok());

#[derive(Args, Debug)]
pub struct ShowArgs {
    pub id: String,

    /// No-op for compatibility (children are always shown).
    #[arg(long, hide = true)]
    pub children: bool,
}

pub fn handle(ctx: &CliRuntimeCtx, args: ShowArgs) -> CommandResult<()> {
    let id = normalize_bead_id(&args.id)?;
    let req = Request::Show {
        ctx: ctx.read_ctx(),
        payload: IdPayload { id: id.clone() },
    };
    let ok = send(&req)?;

    match ok {
        ResponsePayload::Query(QueryResult::Issue(mut view)) => {
            // Fetch deps for richer show output.
            let deps_payload = send(&Request::Deps {
                ctx: ctx.read_ctx(),
                payload: IdPayload { id: id.clone() },
            })?;
            let (incoming_edges, outgoing_edges) = match deps_payload {
                ResponsePayload::Query(QueryResult::Deps { incoming, outgoing }) => {
                    (incoming, outgoing)
                }
                _ => (Vec::new(), Vec::new()),
            };

            // For JSON mode, include deps in the response and return early
            if ctx.json {
                view.deps_incoming = incoming_edges;
                view.deps_outgoing = outgoing_edges;
                return print_ok(&ResponsePayload::Query(QueryResult::Issue(view)), true);
            }

            // Human mode: fetch notes and build richer display
            let notes_payload = send(&Request::Notes {
                ctx: ctx.read_ctx(),
                payload: IdPayload { id: id.clone() },
            })?;
            let notes = match notes_payload {
                ResponsePayload::Query(QueryResult::Notes(n)) => n,
                _ => Vec::new(),
            };

            // Collect IDs for outgoing deps
            let outgoing_ids: BTreeSet<String> =
                outgoing_edges.iter().map(|e| e.to.clone()).collect();

            // Categorize incoming deps by kind
            let mut blocks_ids: BTreeSet<String> = BTreeSet::new();
            let mut children_ids: BTreeSet<String> = BTreeSet::new();
            let mut related_ids: BTreeSet<String> = BTreeSet::new();
            let mut discovered_ids: BTreeSet<String> = BTreeSet::new();
            for e in &incoming_edges {
                match e.kind.as_str() {
                    "parent" => {
                        children_ids.insert(e.from.clone());
                    }
                    "related" => {
                        related_ids.insert(e.from.clone());
                    }
                    "discovered_from" => {
                        discovered_ids.insert(e.from.clone());
                    }
                    _ => {
                        blocks_ids.insert(e.from.clone());
                    }
                }
            }

            let mut all_ids = BTreeSet::new();
            all_ids.extend(outgoing_ids.iter().cloned());
            all_ids.extend(blocks_ids.iter().cloned());
            all_ids.extend(children_ids.iter().cloned());
            all_ids.extend(related_ids.iter().cloned());
            all_ids.extend(discovered_ids.iter().cloned());
            let summary_map = fetch_summary_map(ctx, &all_ids)?;

            let outgoing_views = summaries_for(&outgoing_ids, &summary_map);
            let blocks = summaries_for(&blocks_ids, &summary_map);
            let children = summaries_for(&children_ids, &summary_map);
            let related = summaries_for(&related_ids, &summary_map);
            let discovered = summaries_for(&discovered_ids, &summary_map);

            let incoming = IncomingGroups {
                children,
                blocks,
                related,
                discovered,
            };

            print_line(&render_show(&view, &outgoing_views, &incoming, &notes))?;
            Ok(())
        }
        other => print_ok(&other, false),
    }
}

fn fetch_summary_map(
    ctx: &CliRuntimeCtx,
    ids: &BTreeSet<String>,
) -> CommandResult<HashMap<String, IssueSummary>> {
    if ids.is_empty() {
        return Ok(HashMap::new());
    }
    let bead_ids = ids
        .iter()
        .map(|id| BeadId::parse(id))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let filters = Filters {
        ids: Some(bead_ids),
        ..Filters::default()
    };
    let req = Request::List {
        ctx: ctx.read_ctx(),
        payload: ListPayload { filters },
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issues(summaries)) => Ok(summaries
            .into_iter()
            .map(|summary| (summary.id.clone(), summary))
            .collect()),
        _ => Ok(HashMap::new()),
    }
}

fn summaries_for(
    ids: &BTreeSet<String>,
    summaries: &HashMap<String, IssueSummary>,
) -> Vec<IssueSummary> {
    ids.iter()
        .filter_map(|id| summaries.get(id).cloned())
        .collect()
}

pub struct IncomingGroups {
    pub children: Vec<IssueSummary>,
    pub blocks: Vec<IssueSummary>,
    pub related: Vec<IssueSummary>,
    pub discovered: Vec<IssueSummary>,
}

fn render_show(
    bead: &Issue,
    outgoing: &[IssueSummary],
    incoming: &IncomingGroups,
    notes: &[Note],
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "\n{}: {}\n",
        fmt_issue_ref(&bead.namespace, &bead.id),
        bead.title
    ));
    out.push_str(&format!("Namespace: {}\n", bead.namespace.as_str()));
    out.push_str(&format!("Status: {}\n", bead.status.as_str()));
    out.push_str(&format!("Priority: P{}\n", bead.priority));
    out.push_str(&format!("Type: {}\n", bead.issue_type.as_str()));
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
                fmt_issue_ref(&dep.namespace, &dep.id),
                dep.title,
                dep.priority
            ));
        }
    }

    if !incoming.children.is_empty() {
        // For epics, show detailed progress with done/remaining breakdown
        if bead.issue_type == BeadType::Epic {
            render_epic_children(&mut out, &incoming.children);
        } else {
            out.push_str(&format!("\nChildren ({}):\n", incoming.children.len()));
            for dep in &incoming.children {
                out.push_str(&format!(
                    "  ↳ {}: {} [P{}]\n",
                    fmt_issue_ref(&dep.namespace, &dep.id),
                    dep.title,
                    dep.priority
                ));
            }
        }
    }
    if !incoming.blocks.is_empty() {
        out.push_str(&format!("\nBlocks ({}):\n", incoming.blocks.len()));
        for dep in &incoming.blocks {
            out.push_str(&format!(
                "  ← {}: {} [P{}]\n",
                fmt_issue_ref(&dep.namespace, &dep.id),
                dep.title,
                dep.priority
            ));
        }
    }
    if !incoming.related.is_empty() {
        out.push_str(&format!("\nRelated ({}):\n", incoming.related.len()));
        for dep in &incoming.related {
            out.push_str(&format!(
                "  ↔ {}: {} [P{}]\n",
                fmt_issue_ref(&dep.namespace, &dep.id),
                dep.title,
                dep.priority
            ));
        }
    }
    if !incoming.discovered.is_empty() {
        out.push_str(&format!("\nDiscovered ({}):\n", incoming.discovered.len()));
        for dep in &incoming.discovered {
            out.push_str(&format!(
                "  ◊ {}: {} [P{}]\n",
                fmt_issue_ref(&dep.namespace, &dep.id),
                dep.title,
                dep.priority
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

pub fn render_issue_detail(v: &Issue) -> String {
    // Default detail renderer (used for `show --json=false` fallback).
    let mut out = String::new();
    out.push_str(&format!(
        "\n{}: {}\n",
        fmt_issue_ref(&v.namespace, &v.id),
        v.title
    ));
    out.push_str(&format!("Namespace: {}\n", v.namespace.as_str()));
    out.push_str(&format!("Status: {}\n", v.status.as_str()));
    out.push_str(&format!("Priority: P{}\n", v.priority));
    out.push_str(&format!("Type: {}\n", v.issue_type.as_str()));
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

/// Render epic children with progress breakdown and priority sorting.
fn render_epic_children(out: &mut String, children: &[IssueSummary]) {
    let mut done: Vec<&IssueSummary> = Vec::new();
    let mut remaining: Vec<&IssueSummary> = Vec::new();

    for child in children {
        if child.status == WorkflowStatus::Closed {
            done.push(child);
        } else {
            remaining.push(child);
        }
    }

    // Sort remaining by priority (P0 first), then by status (in_progress before open)
    remaining.sort_by_key(|child| {
        (
            child.priority,
            std::cmp::Reverse(child.status == WorkflowStatus::InProgress),
        )
    });

    // Sort done by updated_at (most recent first)
    done.sort_by_key(|child| std::cmp::Reverse(child.updated_at.wall_ms));

    let total = children.len();
    let done_count = done.len();
    let pct = done_count
        .saturating_mul(100)
        .checked_div(total)
        .unwrap_or(0);

    out.push_str(&format!(
        "\nProgress: {}/{} done ({}%)\n",
        done_count, total, pct
    ));

    if !remaining.is_empty() {
        out.push_str(&format!("\nRemaining ({}):\n", remaining.len()));
        for child in &remaining {
            let status_marker = if child.status == WorkflowStatus::InProgress {
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
                status_marker,
                child.priority,
                fmt_issue_ref(&child.namespace, &child.id),
                child.title,
                assignee
            ));
        }
    }

    if !done.is_empty() {
        out.push_str(&format!("\nDone ({}):\n", done.len()));
        for child in &done {
            out.push_str(&format!(
                "  [x] {}: {}\n",
                fmt_issue_ref(&child.namespace, &child.id),
                child.title
            ));
        }
    }
}

fn fmt_wall_ms(ms: u64) -> String {
    use time::OffsetDateTime;

    let dt = OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    match WALL_MS_FORMAT.as_deref() {
        Some(format) => dt.format(format).unwrap_or_else(|_| ms.to_string()),
        None => ms.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_core::{BeadType, NamespaceId, WorkflowStatus, WriteStamp};

    fn sample_issue(namespace: &str, id: &str) -> Issue {
        Issue {
            id: id.to_string(),
            namespace: NamespaceId::parse(namespace).expect("namespace"),
            title: "Title".to_string(),
            description: String::new(),
            design: None,
            acceptance_criteria: None,
            status: WorkflowStatus::Open,
            priority: 1,
            issue_type: BeadType::Task,
            labels: Vec::new(),
            assignee: None,
            assignee_at: None,
            assignee_expires: None,
            created_at: WriteStamp::new(0, 0),
            created_by: "tester".to_string(),
            created_on_branch: None,
            updated_at: WriteStamp::new(0, 0),
            updated_by: "tester".to_string(),
            closed_at: None,
            closed_by: None,
            closed_reason: None,
            closed_on_branch: None,
            external_ref: None,
            source_repo: None,
            estimated_minutes: None,
            content_hash: "hash".to_string(),
            notes: Vec::new(),
            deps_incoming: Vec::new(),
            deps_outgoing: Vec::new(),
        }
    }

    #[test]
    fn render_show_includes_namespace() {
        let issue = sample_issue("wf", "bd-123");
        let incoming = IncomingGroups {
            children: Vec::new(),
            blocks: Vec::new(),
            related: Vec::new(),
            discovered: Vec::new(),
        };

        let output = render_show(&issue, &[], &incoming, &[]);
        let expected = concat!(
            "\nwf/bd-123: Title\n",
            "Namespace: wf\n",
            "Status: open\n",
            "Priority: P1\n",
            "Type: task\n",
            "Created: 1970-01-01 00:00\n",
            "Updated: 1970-01-01 00:00\n",
            "\n",
        );
        assert_eq!(output, expected);
    }
}
