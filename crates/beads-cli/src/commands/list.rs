use beads_api::QueryResult;
use beads_core::BeadId;
use beads_surface::Filters;
use beads_surface::ipc::{ListPayload, Request, ResponsePayload};
use clap::Args;

use super::common::{fmt_issue_ref, fmt_labels, fmt_wall_ms};
use super::{CommandResult, print_ok};
use crate::filters::{CommonFilterArgs, apply_common_filters};
use crate::parsers::{parse_sort, parse_status};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{normalize_bead_id_for, validation_error};

#[derive(Args, Debug)]
pub struct ListArgs {
    #[command(flatten)]
    pub common: CommonFilterArgs,

    /// Include closed beads instead of defaulting to open work.
    #[arg(long)]
    pub all: bool,

    /// Show detailed multi-line output for each bead.
    #[arg(long, conflicts_with_all = ["tree", "flat"])]
    pub long: bool,

    /// Render a hierarchical tree of descendants (requires --parent).
    #[arg(long, conflicts_with = "flat")]
    pub tree: bool,

    /// Force the legacy flat list output.
    #[arg(long, conflicts_with = "tree")]
    pub flat: bool,

    /// Show labels in output.
    #[arg(short = 'L', long = "show-labels")]
    pub show_labels: bool,

    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,

    /// Sort field (priority|created|updated|title) with optional :asc/:desc.
    #[arg(long)]
    pub sort: Option<String>,

    /// Filter by parent epic ID (shows children of this epic).
    #[arg(long)]
    pub parent: Option<String>,

    /// Optional text query (matches title/description).
    #[arg(value_name = "QUERY", num_args = 0..)]
    pub query: Vec<String>,
}

pub fn handle_list(ctx: &CliRuntimeCtx, args: ListArgs) -> CommandResult<()> {
    let search = if args.query.is_empty() {
        None
    } else {
        Some(args.query.join(" "))
    };
    let parent = args
        .parent
        .as_deref()
        .map(|raw| normalize_bead_id_for("parent", raw))
        .transpose()?;

    let mut filters = Filters::default();
    apply_common_filters(&args.common, &mut filters)?;

    if args.all && args.common.status.is_some() {
        return Err(validation_error("all", "--all cannot be combined with --status").into());
    }

    if let Some(status) = args.common.status.as_deref() {
        filters.status = Some(parse_status(status));
    } else if args.all {
        filters.status = Some(String::from("all"));
    } else if ctx.json {
        filters.status = None;
    } else {
        filters.status = Some(String::from("open"));
    }

    filters.limit = args.limit;
    filters.search = search;
    filters.parent = parent;

    if let Some(sort) = args.sort {
        let (field, ascending) =
            parse_sort(&sort).map_err(|err| validation_error("sort", err.to_string()))?;
        filters.sort_by = Some(field);
        filters.ascending = ascending;
    }

    let tree_parent = filters.parent.clone();
    if !ctx.json && args.tree {
        let Some(parent) = tree_parent.as_ref() else {
            return Err(validation_error("tree", "--tree requires --parent").into());
        };
        let tree = fetch_child_tree(ctx, &filters, parent)?;
        let output = render_issue_tree(parent.as_str(), &tree, args.show_labels);
        crate::render::print_line(&output)?;
        return Ok(());
    }

    let req = Request::List {
        ctx: ctx.read_ctx(),
        payload: ListPayload {
            filters: filters.clone(),
        },
    };
    let ok = send(&req)?;

    if !ctx.json
        && let ResponsePayload::Query(QueryResult::Issues(ref views)) = ok
    {
        let output = if args.long {
            render_issue_list_long(views, args.show_labels)
        } else if args.flat {
            render_issue_list_flat(views, args.show_labels)
        } else {
            render_issue_list_opts(views, args.show_labels)
        };
        crate::render::print_line(&output)?;
        return Ok(());
    }

    print_ok(&ok, ctx.json)
}

pub fn render_issue_list_opts(views: &[beads_api::IssueSummary], show_labels: bool) -> String {
    let mut out = String::new();
    for view in views {
        out.push_str(&render_issue_summary_opts(view, show_labels));
        out.push('\n');
    }
    out.trim_end().into()
}

fn render_issue_list_flat(views: &[beads_api::IssueSummary], show_labels: bool) -> String {
    render_issue_list_opts(views, show_labels)
}

fn render_issue_summary_opts(view: &beads_api::IssueSummary, show_labels: bool) -> String {
    let mut summary = format!(
        "{} [P{}] [{}] {}",
        fmt_issue_ref(&view.namespace, &view.id),
        view.priority,
        view.issue_type.as_str(),
        view.status.as_str()
    );
    if let Some(assignee) = &view.assignee
        && !assignee.is_empty()
    {
        summary.push_str(&format!(" @{}", assignee));
    }
    if show_labels && !view.labels.is_empty() {
        summary.push_str(&format!(" {}", fmt_labels(&view.labels)));
    }
    summary.push_str(&format!(" - {}", view.title));
    summary
}

fn render_issue_list_long(views: &[beads_api::IssueSummary], show_labels: bool) -> String {
    let mut out = String::new();
    for (idx, view) in views.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        out.push_str(&render_issue_summary_opts(view, show_labels));
        if !view.description.trim().is_empty() {
            out.push_str(&format!("\n  Description: {}", view.description.trim()));
        }
        if let Some(design) = &view.design
            && !design.trim().is_empty()
        {
            out.push_str(&format!("\n  Design: {}", design.trim()));
        }
        if let Some(acceptance) = &view.acceptance_criteria
            && !acceptance.trim().is_empty()
        {
            out.push_str(&format!("\n  Acceptance: {}", acceptance.trim()));
        }
        out.push_str(&format!(
            "\n  Updated: {}",
            fmt_wall_ms(view.updated_at.wall_ms)
        ));
    }
    out
}

#[derive(Clone)]
struct TreeNode {
    issue: beads_api::IssueSummary,
    children: Vec<TreeNode>,
}

fn fetch_child_tree(
    ctx: &CliRuntimeCtx,
    base_filters: &Filters,
    parent: &BeadId,
) -> CommandResult<Vec<TreeNode>> {
    let mut filters = base_filters.clone();
    filters.parent = Some(parent.clone());
    let children = list_issue_summaries(ctx, filters)?;

    let mut nodes = Vec::with_capacity(children.len());
    for child in children {
        let child_id = BeadId::parse(&child.id)?;
        let descendants = fetch_child_tree(ctx, base_filters, &child_id)?;
        nodes.push(TreeNode {
            issue: child,
            children: descendants,
        });
    }
    Ok(nodes)
}

fn list_issue_summaries(
    ctx: &CliRuntimeCtx,
    filters: Filters,
) -> CommandResult<Vec<beads_api::IssueSummary>> {
    let req = Request::List {
        ctx: ctx.read_ctx(),
        payload: ListPayload { filters },
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issues(views)) => Ok(views),
        other => Err(validation_error(
            "list",
            format!("unexpected response while building tree view: {other:?}"),
        )
        .into()),
    }
}

fn render_issue_tree(parent: &str, nodes: &[TreeNode], show_labels: bool) -> String {
    let mut out = String::new();
    out.push_str(&format!("Children of {parent}:\n"));
    if nodes.is_empty() {
        out.push_str("  (none)");
        return out;
    }
    for node in nodes {
        render_tree_node(&mut out, node, show_labels, 0);
    }
    out.trim_end().to_string()
}

fn render_tree_node(out: &mut String, node: &TreeNode, show_labels: bool, depth: usize) {
    let indent = "  ".repeat(depth);
    out.push_str(&format!(
        "{}- {}\n",
        indent,
        render_issue_summary_opts(&node.issue, show_labels)
    ));
    for child in &node.children {
        render_tree_node(out, child, show_labels, depth + 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_core::{BeadType, NamespaceId, WorkflowStatus, WriteStamp};

    fn sample_summary(namespace: &str, id: &str) -> beads_api::IssueSummary {
        beads_api::IssueSummary {
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
            assignee_expires: None,
            created_at: WriteStamp::new(0, 0),
            created_by: "tester".to_string(),
            updated_at: WriteStamp::new(0, 0),
            updated_by: "tester".to_string(),
            estimated_minutes: None,
            content_hash: "hash".to_string(),
            note_count: 0,
        }
    }

    #[test]
    fn render_issue_list_includes_namespace() {
        let summary = sample_summary("wf", "bd-123");
        let output = render_issue_list_opts(&[summary], false);
        assert_eq!(output, "wf/bd-123 [P1] [task] open - Title");
    }

    #[test]
    fn render_issue_list_long_includes_description() {
        let mut summary = sample_summary("core", "bd-123");
        summary.description = "Detailed description".to_string();
        let output = render_issue_list_long(&[summary], false);
        assert!(output.contains("bd-123 [P1] [task] open - Title"));
        assert!(output.contains("Description: Detailed description"));
    }

    #[test]
    fn render_issue_tree_indents_nested_children() {
        let child = TreeNode {
            issue: sample_summary("core", "bd-123.1"),
            children: vec![TreeNode {
                issue: sample_summary("core", "bd-123.1.1"),
                children: Vec::new(),
            }],
        };
        let output = render_issue_tree("bd-123", &[child], false);
        assert!(output.contains("Children of bd-123:"));
        assert!(output.contains("- core/bd-123.1 [P1] [task] open - Title"));
        assert!(output.contains("  - core/bd-123.1.1 [P1] [task] open - Title"));
    }
}
