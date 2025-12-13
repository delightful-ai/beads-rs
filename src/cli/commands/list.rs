use crate::{Error, Result};

use crate::core::ActorId;
use crate::daemon::ipc::Request;
use crate::daemon::query::Filters;

use super::super::{parse_sort, print_ok, send, Ctx, ListArgs, SearchArgs};

pub(crate) fn handle_list(ctx: &Ctx, args: ListArgs) -> Result<()> {
    let mut filters = Filters::default();
    filters.status = args.status;
    filters.priority = args.priority;
    filters.bead_type = args.bead_type;
    filters.assignee = args.assignee.map(ActorId::new).transpose()?;
    if !args.labels.is_empty() {
        filters.labels = Some(args.labels);
    }
    filters.limit = args.limit;
    if !args.query.is_empty() {
        filters.search = Some(args.query.join(" "));
    }
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
    print_ok(&ok, ctx.json)
}

pub(crate) fn handle_search(ctx: &Ctx, args: SearchArgs) -> Result<()> {
    let mut filters = Filters::default();
    filters.search = Some(args.query.join(" "));
    filters.limit = args.limit;
    let req = Request::List {
        repo: ctx.repo.clone(),
        filters,
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}
