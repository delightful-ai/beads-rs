use time::format_description::well_known::Rfc3339;
use time::{Date, OffsetDateTime, Time};

use super::super::{CountArgs, Ctx, print_ok, send};
use crate::core::{ActorId, BeadId};
use crate::daemon::ipc::Request;
use crate::daemon::query::Filters;
use crate::{Error, Result};

pub(crate) fn handle(ctx: &Ctx, args: CountArgs) -> Result<()> {
    let mut filters = Filters {
        status: args.status.clone(),
        priority: args.priority,
        priority_min: args.priority_min,
        priority_max: args.priority_max,
        bead_type: args.bead_type,
        assignee: args.assignee.as_deref().map(ActorId::new).transpose()?,
        ..Default::default()
    };

    if !args.labels.is_empty() {
        filters.labels = Some(args.labels.clone());
    }
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
            ids.push(BeadId::parse(s)?);
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

fn parse_time_ms_opt(s: Option<&str>) -> Result<Option<u64>> {
    let Some(s) = s else { return Ok(None) };
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }

    Ok(Some(parse_time_ms(s).map_err(|msg| {
        Error::Op(crate::daemon::OpError::ValidationFailed {
            field: "date".into(),
            reason: msg,
        })
    })?))
}

fn parse_time_ms(s: &str) -> std::result::Result<u64, String> {
    // RFC3339
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    // YYYY-MM-DD (midnight UTC)
    let fmt_date =
        time::format_description::parse("[year]-[month]-[day]").map_err(|e| e.to_string())?;
    if let Ok(date) = Date::parse(s, &fmt_date) {
        let dt = date.with_time(Time::MIDNIGHT).assume_utc();
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    // YYYY-MM-DD HH:MM:SS (UTC)
    let fmt_dt = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
        .map_err(|e| e.to_string())?;
    if let Ok(dt) = time::PrimitiveDateTime::parse(s, &fmt_dt) {
        let dt = dt.assume_utc();
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    Err(format!(
        "unsupported date format: {s:?} (use YYYY-MM-DD or RFC3339)"
    ))
}
