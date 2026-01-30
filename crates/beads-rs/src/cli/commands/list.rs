use clap::Args;

use super::super::{
    CommonFilterArgs, Ctx, normalize_bead_id_for, print_line, print_ok, send, validation_error,
};
use super::{fmt_issue_ref, fmt_labels};
use crate::Result;
use crate::api::QueryResult;
use crate::cli::parsers::{parse_sort, parse_status};
use crate::daemon::ipc::{ListPayload, Request, ResponsePayload};
use crate::daemon::query::Filters;

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

pub(crate) fn handle_list(ctx: &Ctx, args: ListArgs) -> Result<()> {
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
    args.common.apply(&mut filters)?;
    if let Some(status) = args.common.status.as_deref() {
        let status = parse_status(status).map_err(|msg| validation_error("status", msg))?;
        filters.status = Some(status);
    }
    filters.limit = args.limit;
    filters.search = search;
    filters.parent = parent;
    if let Some(sort) = args.sort {
        let (field, ascending) = parse_sort(&sort).map_err(|msg| validation_error("sort", msg))?;
        filters.sort_by = Some(field);
        filters.ascending = ascending;
    }

    let req = Request::List {
        ctx: ctx.read_ctx(),
        payload: ListPayload { filters },
    };
    let ok = send(&req)?;

    // Handle show_labels flag for human output
    if !ctx.json
        && let ResponsePayload::Query(QueryResult::Issues(ref views)) = ok
    {
        let output = render_issue_list_opts(views, args.show_labels);
        return print_line(&output);
    }

    print_ok(&ok, ctx.json)
}

pub(crate) fn render_issue_list_opts(
    views: &[crate::api::IssueSummary],
    show_labels: bool,
) -> String {
    let mut out = String::new();
    for v in views {
        out.push_str(&render_issue_summary_opts(v, show_labels));
        out.push('\n');
    }
    out.trim_end().into()
}

fn render_issue_summary_opts(v: &crate::api::IssueSummary, show_labels: bool) -> String {
    let mut s = format!(
        "{} [P{}] [{}] {}",
        fmt_issue_ref(&v.namespace, &v.id),
        v.priority,
        v.issue_type,
        v.status
    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{NamespaceId, WriteStamp};

    fn sample_summary(namespace: &str, id: &str) -> crate::api::IssueSummary {
        crate::api::IssueSummary {
            id: id.to_string(),
            namespace: NamespaceId::parse(namespace).expect("namespace"),
            title: "Title".to_string(),
            description: String::new(),
            design: None,
            acceptance_criteria: None,
            status: "open".to_string(),
            priority: 1,
            issue_type: "task".to_string(),
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
