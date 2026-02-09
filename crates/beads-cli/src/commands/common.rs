use super::{CommandError, CommandResult};
use crate::runtime::{CliRuntimeCtx, send};
use beads_api::{Issue, QueryResult};
use beads_core::{BeadId, NamespaceId};
use beads_surface::ipc::{IdPayload, IpcError, Request, ResponsePayload};
use std::sync::LazyLock;

pub(crate) fn fmt_issue_ref(namespace: &NamespaceId, id: &str) -> String {
    format!("{}/{}", namespace.as_str(), id)
}

pub(crate) fn fmt_labels(labels: &[String]) -> String {
    let mut out = String::from("[");
    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(label);
    }
    out.push(']');
    out
}

pub(crate) fn fmt_metric_labels(labels: &[beads_api::AdminMetricLabel]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut out = String::from(" {");
    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(label.key.as_str());
        out.push('=');
        out.push_str(label.value.as_str());
    }
    out.push('}');
    out
}

static WALL_MS_FORMAT: LazyLock<Option<Vec<time::format_description::FormatItem<'static>>>> =
    LazyLock::new(|| time::format_description::parse("[year]-[month]-[day] [hour]:[minute]").ok());

pub(crate) fn fmt_wall_ms(ms: u64) -> String {
    use time::OffsetDateTime;

    let dt = OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    match WALL_MS_FORMAT.as_deref() {
        Some(fmt) => dt.format(fmt).unwrap_or_else(|_| ms.to_string()),
        None => ms.to_string(),
    }
}

pub(crate) fn fetch_issue(ctx: &CliRuntimeCtx, id: &BeadId) -> CommandResult<Issue> {
    let req = Request::Show {
        ctx: ctx.read_ctx(),
        payload: IdPayload { id: id.clone() },
    };
    match send(&req)? {
        ResponsePayload::Query(QueryResult::Issue(issue)) => Ok(issue),
        other => Err(CommandError::Ipc(IpcError::DaemonUnavailable(format!(
            "unexpected response for show: {other:?}"
        )))),
    }
}
