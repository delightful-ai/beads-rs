use super::super::{
    CountArgs, Ctx, apply_common_filters, normalize_bead_id_for, parse_time_ms_opt, print_ok, send,
};
use crate::daemon::ipc::Request;
use crate::daemon::query::Filters;
use crate::{Error, Result};

pub(crate) fn handle(ctx: &Ctx, args: CountArgs) -> Result<()> {
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

    if let Some(title) = args.title.clone()
        && !title.trim().is_empty()
    {
        filters.title = Some(title);
    }

    if let Some(ids_raw) = args.id.clone() {
        let mut ids = Vec::new();
        for part in ids_raw.split(',') {
            let s = part.trim();
            if s.is_empty() {
                continue;
            }
            ids.push(normalize_bead_id_for("id", s)?);
        }
        if !ids.is_empty() {
            filters.ids = Some(ids);
        }
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

    let group_by = resolve_group_by(&args)?;

    let req = Request::Count {
        repo: ctx.repo.clone(),
        filters,
        group_by,
        read: ctx.read_consistency(),
    };
    let ok = send(&req)?;
    print_ok(&ok, ctx.json)
}

fn resolve_group_by(args: &CountArgs) -> Result<Option<String>> {
    let mut group_by: Option<&'static str> = None;

    let candidates = [
        (args.by_status, "status"),
        (args.by_priority, "priority"),
        (args.by_type, "type"),
        (args.by_assignee, "assignee"),
        (args.by_label, "label"),
    ];

    for (flag, name) in candidates {
        if !flag {
            continue;
        }
        if group_by.is_some() {
            return Err(Error::Op(crate::daemon::OpError::ValidationFailed {
                field: "by-*".into(),
                reason: "only one --by-* flag can be specified".into(),
            }));
        }
        group_by = Some(name);
    }

    Ok(group_by.map(|s| s.to_string()))
}
