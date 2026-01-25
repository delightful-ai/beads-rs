use clap::Args;

use super::super::render;
use super::super::{
    Ctx, apply_common_filters, normalize_bead_id_for, parse_bead_type, parse_priority, parse_sort,
    parse_status, parse_time_ms_opt, print_ok, send,
};
use crate::api::QueryResult;
use crate::core::{BeadType, Priority};
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::Filters;
use crate::{Error, Result};

#[derive(Args, Debug)]
pub struct ListArgs {
    /// Status filter (open, in_progress, closed).
    #[arg(short = 's', long, value_parser = parse_status)]
    pub status: Option<String>,

    /// Type filter.
    #[arg(short = 't', long = "type", alias = "issue-type", value_parser = parse_bead_type)]
    pub bead_type: Option<BeadType>,

    /// Priority filter.
    #[arg(short = 'p', long, value_parser = parse_priority)]
    pub priority: Option<Priority>,

    /// Minimum priority (inclusive).
    #[arg(long = "priority-min", value_parser = parse_priority)]
    pub priority_min: Option<Priority>,

    /// Maximum priority (inclusive).
    #[arg(long = "priority-max", value_parser = parse_priority)]
    pub priority_max: Option<Priority>,

    /// Assignee filter.
    #[arg(short = 'a', long)]
    pub assignee: Option<String>,

    /// Label filter (repeat or comma-separated).
    #[arg(short = 'l', long = "label", alias = "labels", value_delimiter = ',', num_args = 0..)]
    pub labels: Vec<String>,

    /// Label filter (OR: must have AT LEAST ONE). Repeat or comma-separated.
    #[arg(long = "label-any", value_delimiter = ',', num_args = 0..)]
    pub labels_any: Vec<String>,

    /// Filter by title substring.
    #[arg(long = "title-contains")]
    pub title_contains: Option<String>,

    /// Filter by description substring.
    #[arg(long = "desc-contains")]
    pub desc_contains: Option<String>,

    /// Filter by notes substring.
    #[arg(long = "notes-contains")]
    pub notes_contains: Option<String>,

    /// Filter issues created after date (YYYY-MM-DD or RFC3339).
    #[arg(long = "created-after")]
    pub created_after: Option<String>,

    /// Filter issues created before date (YYYY-MM-DD or RFC3339).
    #[arg(long = "created-before")]
    pub created_before: Option<String>,

    /// Filter issues updated after date (YYYY-MM-DD or RFC3339).
    #[arg(long = "updated-after")]
    pub updated_after: Option<String>,

    /// Filter issues updated before date (YYYY-MM-DD or RFC3339).
    #[arg(long = "updated-before")]
    pub updated_before: Option<String>,

    /// Filter issues closed after date (YYYY-MM-DD or RFC3339).
    #[arg(long = "closed-after")]
    pub closed_after: Option<String>,

    /// Filter issues closed before date (YYYY-MM-DD or RFC3339).
    #[arg(long = "closed-before")]
    pub closed_before: Option<String>,

    /// Filter issues with empty description.
    #[arg(long = "empty-description")]
    pub empty_description: bool,

    /// Filter issues with no assignee.
    #[arg(long = "no-assignee")]
    pub no_assignee: bool,

    /// Filter issues with no labels.
    #[arg(long = "no-labels")]
    pub no_labels: bool,

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

#[derive(Args, Debug)]
pub struct SearchArgs {
    /// Search query (multiple words allowed).
    #[arg(num_args = 1..)]
    pub query: Vec<String>,

    /// Limit results.
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
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
    apply_common_filters(
        &mut filters,
        args.status.clone(),
        args.priority,
        args.bead_type,
        args.assignee.clone(),
        args.labels.clone(),
    )?;
    filters.priority_min = args.priority_min;
    filters.priority_max = args.priority_max;
    if !args.labels_any.is_empty() {
        filters.labels_any = Some(args.labels_any.clone());
    }
    filters.title_contains = args.title_contains.clone().filter(|s| !s.trim().is_empty());
    filters.desc_contains = args.desc_contains.clone().filter(|s| !s.trim().is_empty());
    filters.notes_contains = args.notes_contains.clone().filter(|s| !s.trim().is_empty());
    filters.created_after = parse_time_ms_opt(args.created_after.as_deref())?;
    filters.created_before = parse_time_ms_opt(args.created_before.as_deref())?;
    filters.updated_after = parse_time_ms_opt(args.updated_after.as_deref())?;
    filters.updated_before = parse_time_ms_opt(args.updated_before.as_deref())?;
    filters.closed_after = parse_time_ms_opt(args.closed_after.as_deref())?;
    filters.closed_before = parse_time_ms_opt(args.closed_before.as_deref())?;
    filters.empty_description = args.empty_description;
    filters.no_assignee = args.no_assignee;
    filters.no_labels = args.no_labels;
    filters.limit = args.limit;
    filters.search = search;
    filters.parent = parent;
    if let Some(sort) = args.sort {
        let (field, ascending) = parse_sort(&sort).map_err(|msg| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "sort".into(),
                reason: msg,
            })
        })?;
        filters.sort_by = Some(field);
        filters.ascending = ascending;
    }

    let req = Request::List {
        repo: ctx.repo.clone(),
        filters,
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;

    // Handle show_labels flag for human output
    if !ctx.json
        && let ResponsePayload::Query(QueryResult::Issues(ref views)) = ok
    {
        let output = render::render_issue_list_opts(views, args.show_labels);
        println!("{}", output);
        return Ok(());
    }

    print_ok(&ok, ctx.json)
}

pub(crate) fn handle_search(ctx: &Ctx, args: SearchArgs) -> Result<()> {
    let filters = Filters {
        search: Some(args.query.join(" ")),
        limit: args.limit,
        ..Default::default()
    };
    let req = Request::List {
        repo: ctx.repo.clone(),
        filters,
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
