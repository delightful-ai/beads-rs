use beads_api::QueryResult;
use beads_surface::Filters;
use beads_surface::ipc::{ListPayload, Request, ResponsePayload};
use clap::Args;

use super::common::{fmt_issue_ref, fmt_labels};
use super::{CommandResult, print_ok};
use crate::filters::{CommonFilterArgs, apply_common_filters};
use crate::parsers::{parse_sort, parse_status};
use crate::runtime::{CliRuntimeCtx, send};
use crate::validation::{normalize_bead_id_for, validation_error};

#[derive(Args, Debug)]
pub struct ListArgs {
    #[command(flatten)]
    pub common: CommonFilterArgs,

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

    if let Some(status) = args.common.status.as_deref() {
        filters.status = Some(parse_status(status));
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

    let req = Request::List {
        ctx: ctx.read_ctx(),
        payload: ListPayload { filters },
    };
    let ok = send(&req)?;

    if !ctx.json
        && let ResponsePayload::Query(QueryResult::Issues(ref views)) = ok
    {
        let output = render_issue_list_opts(views, args.show_labels);
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
}
