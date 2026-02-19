use beads_core::{BeadId, BeadType, DepKind, Priority, ValidatedBeadId, ValidatedDepKind};
use beads_surface::SortField;
use time::format_description::well_known::Rfc3339;
use time::{Date, OffsetDateTime, Time};

pub type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[error("unknown bead type `{raw}`")]
    UnknownBeadType { raw: String },

    #[error("invalid priority `{raw}`")]
    InvalidPriority { raw: String },

    #[error("invalid sort field `{raw}`")]
    InvalidSortField { raw: String },

    #[error("unsupported date format: {raw:?} (use YYYY-MM-DD or RFC3339)")]
    UnsupportedDateFormat { raw: String },

    #[error("invalid from id {raw:?}: {reason}")]
    InvalidFromId { raw: String, reason: String },

    #[error("invalid to id {raw:?}: {reason}")]
    InvalidToId { raw: String, reason: String },

    #[error("{reason}")]
    Reason { reason: String },
}

impl ParseError {
    fn reason(reason: impl Into<String>) -> Self {
        Self::Reason {
            reason: reason.into(),
        }
    }
}

pub fn parse_bead_type(raw: &str) -> Result<BeadType> {
    let s = raw.trim().to_lowercase();
    match s.as_str() {
        "bug" | "bugs" => Ok(BeadType::Bug),
        "feature" | "feat" | "features" => Ok(BeadType::Feature),
        "task" | "todo" | "tasks" => Ok(BeadType::Task),
        "epic" | "epics" => Ok(BeadType::Epic),
        "chore" | "chores" | "maintenance" => Ok(BeadType::Chore),
        _ => Err(ParseError::UnknownBeadType {
            raw: raw.to_string(),
        }),
    }
}

pub fn parse_priority(raw: &str) -> Result<Priority> {
    let s = raw.trim().to_lowercase();

    // Numeric, allow p1/P2 forms.
    let num_str = s.trim_start_matches('p');
    if let Ok(n) = num_str.parse::<u8>() {
        return Priority::new(n).map_err(|err| ParseError::reason(err.to_string()));
    }

    match s.as_str() {
        "critical" | "crit" => Ok(Priority::CRITICAL),
        "high" => Ok(Priority::HIGH),
        "medium" | "med" => Ok(Priority::MEDIUM),
        "low" => Ok(Priority::LOW),
        "backlog" | "lowest" => Ok(Priority::LOWEST),
        _ => Err(ParseError::InvalidPriority {
            raw: raw.to_string(),
        }),
    }
}

pub fn parse_status(raw: &str) -> String {
    let s = raw.trim().to_lowercase().replace(['-', ' '], "_");
    let canon = match s.as_str() {
        "open" | "todo" => "open",
        "inprogress" | "in_progress" | "doing" | "wip" => "in_progress",
        "closed" | "done" | "complete" => "closed",
        other => other,
    };
    canon.to_string()
}

pub fn parse_dep_kind(raw: &str) -> Result<DepKind> {
    ValidatedDepKind::parse(raw)
        .map(Into::into)
        .map_err(|err| ParseError::reason(err.to_string()))
}

pub fn parse_sort(raw: &str) -> Result<(SortField, bool)> {
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
        _ => {
            return Err(ParseError::InvalidSortField {
                raw: raw.to_string(),
            });
        }
    };
    Ok((field, ascending))
}

pub fn parse_time_ms_opt(s: Option<&str>) -> Result<Option<u64>> {
    let Some(s) = s else { return Ok(None) };
    let s = s.trim();
    if s.is_empty() {
        return Ok(None);
    }

    parse_time_ms(s).map(Some)
}

pub fn parse_time_ms(s: &str) -> Result<u64> {
    // RFC3339
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    // YYYY-MM-DD (midnight UTC)
    let fmt_date = time::format_description::parse("[year]-[month]-[day]")
        .map_err(|err| ParseError::reason(err.to_string()))?;
    if let Ok(date) = Date::parse(s, &fmt_date) {
        let dt = date.with_time(Time::MIDNIGHT).assume_utc();
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    // YYYY-MM-DD HH:MM:SS (UTC)
    let fmt_dt = time::format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]")
        .map_err(|err| ParseError::reason(err.to_string()))?;
    if let Ok(dt) = time::PrimitiveDateTime::parse(s, &fmt_dt) {
        let dt = dt.assume_utc();
        return Ok(dt.unix_timestamp_nanos() as u64 / 1_000_000);
    }

    Err(ParseError::UnsupportedDateFormat { raw: s.to_string() })
}

pub fn parse_dep_edge(
    kind_flag: Option<DepKind>,
    from_raw: &str,
    to_raw: &str,
) -> Result<(DepKind, BeadId, BeadId)> {
    let (kind_from, from_raw) = split_kind_id(from_raw)?;
    let (kind_to, to_raw) = split_kind_id(to_raw)?;

    let kind = kind_flag
        .or(kind_from)
        .or(kind_to)
        .unwrap_or(DepKind::Blocks);

    let from = ValidatedBeadId::parse(&from_raw)
        .map(Into::into)
        .map_err(|err| ParseError::InvalidFromId {
            raw: from_raw,
            reason: err.to_string(),
        })?;
    let to = ValidatedBeadId::parse(&to_raw)
        .map(Into::into)
        .map_err(|err| ParseError::InvalidToId {
            raw: to_raw,
            reason: err.to_string(),
        })?;

    Ok((kind, from, to))
}

fn split_kind_id(raw: &str) -> Result<(Option<DepKind>, String)> {
    if let Some((k, id)) = raw.split_once(':') {
        Ok((Some(parse_dep_kind(k)?), id.trim().to_string()))
    } else {
        Ok((None, raw.trim().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bead_type_valid() {
        let cases = vec![
            ("bug", BeadType::Bug),
            ("bugs", BeadType::Bug),
            ("BUG", BeadType::Bug),
            ("  bug  ", BeadType::Bug),
            ("feature", BeadType::Feature),
            ("feat", BeadType::Feature),
            ("features", BeadType::Feature),
            ("task", BeadType::Task),
            ("todo", BeadType::Task),
            ("tasks", BeadType::Task),
            ("epic", BeadType::Epic),
            ("epics", BeadType::Epic),
            ("chore", BeadType::Chore),
            ("chores", BeadType::Chore),
            ("maintenance", BeadType::Chore),
        ];

        for (input, expected) in cases {
            assert_eq!(
                parse_bead_type(input).unwrap(),
                expected,
                "Failed to parse '{}'",
                input
            );
        }
    }

    #[test]
    fn test_parse_bead_type_invalid() {
        let cases = vec!["invalid", "random", "", "123"];

        for input in cases {
            match parse_bead_type(input) {
                Err(ParseError::UnknownBeadType { raw }) => {
                    assert_eq!(raw, input.to_string());
                }
                _ => panic!("Expected UnknownBeadType error for '{}'", input),
            }
        }
    }
}
