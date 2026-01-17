use super::super::render;
use super::super::{
    Ctx, ListArgs, SearchArgs, apply_common_filters, normalize_bead_id_for, parse_sort,
    parse_time_ms_opt, print_ok, send,
};
use crate::api::QueryResult;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::Filters;
use crate::{Error, Result};

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
