use time::format_description::well_known::Rfc3339;
use time::{Date, OffsetDateTime, Time};

use crate::core::{
    BeadId, BeadType, DepKind, Priority, ValidatedBeadId, ValidatedDepKind,
};
use crate::daemon::query::SortField;
use crate::{Error, Result};

pub(crate) fn parse_bead_type(raw: &str) -> std::result::Result<BeadType, String> {
    let s = raw.trim().to_lowercase();
    match s.as_str() {
        "bug" | "bugs" => Ok(BeadType::Bug),
        "feature" | "feat" | "features" => Ok(BeadType::Feature),
        "task" | "todo" | "tasks" => Ok(BeadType::Task),
        "epic" | "epics" => Ok(BeadType::Epic),
        "chore" | "chores" | "maintenance" => Ok(BeadType::Chore),
        _ => Err(format!("unknown bead type `{raw}`")),
    }
}

pub(crate) fn parse_priority(raw: &str) -> std::result::Result<Priority, String> {
    let s = raw.trim().to_lowercase();

    // Numeric, allow p1/P2 forms.
    let num_str = s.trim_start_matches('p');
    if let Ok(n) = num_str.parse::<u8>() {
        return Priority::new(n).map_err(|e| e.to_string());
    }

    match s.as_str() {
        "critical" | "crit" => Ok(Priority::CRITICAL),
        "high" => Ok(Priority::HIGH),
        "medium" | "med" => Ok(Priority::MEDIUM),
        "low" => Ok(Priority::LOW),
        "backlog" | "lowest" => Ok(Priority::LOWEST),
        _ => Err(format!("invalid priority `{raw}`")),
    }
}

pub(crate) fn parse_status(raw: &str) -> std::result::Result<String, String> {
    let s = raw.trim().to_lowercase().replace(['-', ' '], "_");
    let canon = match s.as_str() {
        "open" | "todo" => "open",
        "inprogress" | "in_progress" | "doing" | "wip" => "in_progress",
        "closed" | "done" | "complete" => "closed",
        other => other,
    };
    Ok(canon.to_string())
}

pub(crate) fn parse_dep_kind(raw: &str) -> std::result::Result<DepKind, String> {
    ValidatedDepKind::parse(raw)
        .map(Into::into)
        .map_err(|e| e.to_string())
}

pub(crate) fn parse_sort(raw: &str) -> std::result::Result<(SortField, bool), String> {
    let mut s = raw.trim().to_lowercase();
    let mut ascending = false;

    if s.starts_with('-') {
        s = s.trim_start_matches('-').to_string();
        ascending = false;
    }

    let tmp = s.clone();
    if let Some((field, dir)) = tmp.split_once(':') {
        s = field.to_string();
        ascending = matches!(dir, "asc" | "ascending");
    }

    let field = match s.as_str() {
        "priority" | "prio" => SortField::Priority,
        "created" | "created_at" | "createdat" => SortField::CreatedAt,
        "updated" | "updated_at" | "updatedat" => SortField::UpdatedAt,
        "title" | "name" => SortField::Title,
        _ => return Err(format!("invalid sort field `{raw}`")),
    };
    Ok((field, ascending))
}

pub(crate) fn parse_time_ms_opt(s: Option<&str>) -> Result<Option<u64>> {
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

pub(crate) fn parse_time_ms(s: &str) -> std::result::Result<u64, String> {
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

pub(crate) fn parse_dep_edge(
    kind_flag: Option<DepKind>,
    from_raw: &str,
    to_raw: &str,
) -> std::result::Result<(DepKind, BeadId, BeadId), String> {
    let (kind_from, from_raw) = split_kind_id(from_raw)?;
    let (kind_to, to_raw) = split_kind_id(to_raw)?;

    let kind = kind_flag
        .or(kind_from)
        .or(kind_to)
        .unwrap_or(DepKind::Blocks);

    let from = ValidatedBeadId::parse(&from_raw)
        .map(Into::into)
        .map_err(|e| format!("invalid from id {from_raw:?}: {e}"))?;
    let to = ValidatedBeadId::parse(&to_raw)
        .map(Into::into)
        .map_err(|e| format!("invalid to id {to_raw:?}: {e}"))?;

    Ok((kind, from, to))
}

fn split_kind_id(raw: &str) -> std::result::Result<(Option<DepKind>, String), String> {
    if let Some((k, id)) = raw.split_once(':') {
        Ok((Some(parse_dep_kind(k)?), id.trim().to_string()))
    } else {
        Ok((None, raw.trim().to_string()))
    }
}
