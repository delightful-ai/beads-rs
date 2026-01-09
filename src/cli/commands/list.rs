use super::super::render;
use super::super::{Ctx, ListArgs, SearchArgs, apply_common_filters, parse_sort, print_ok, send};
use crate::core::BeadId;
use crate::daemon::ipc::{Request, ResponsePayload};
use crate::daemon::query::{Filters, QueryResult};
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
        .map(BeadId::parse)
        .transpose()
        .map_err(|e| {
            Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "parent".into(),
                reason: e.to_string(),
            })
        })?;
    let mut filters = Filters::default();
    apply_common_filters(
        &mut filters,
        args.status.clone(),
        args.priority,
        args.bead_type,
        args.assignee.clone(),
        args.labels.clone(),
    )?;
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
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
