use std::time::Instant;

use tracing::Span;

use super::super::ipc::{ReadConsistency, RequestInfo};
use crate::daemon::metrics;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ReadConsistencyTag {
    Default,
    RequireMinSeen,
}

impl ReadConsistencyTag {
    fn as_str(self) -> &'static str {
        match self {
            ReadConsistencyTag::Default => "default",
            ReadConsistencyTag::RequireMinSeen => "require_min_seen",
        }
    }
}

pub(super) fn read_consistency_tag(read: &ReadConsistency) -> ReadConsistencyTag {
    if read.require_min_seen.is_some() {
        ReadConsistencyTag::RequireMinSeen
    } else {
        ReadConsistencyTag::Default
    }
}

pub(super) fn request_span(info: &RequestInfo<'_>) -> Span {
    let span = tracing::info_span!(
        "ipc_request",
        request_type = info.op,
        repo = tracing::field::Empty,
        namespace = tracing::field::Empty,
        actor_id = tracing::field::Empty,
        client_request_id = tracing::field::Empty,
        read_consistency = tracing::field::Empty,
    );
    if let Some(repo) = info.repo {
        let repo_display = repo.display();
        span.record("repo", tracing::field::display(repo_display));
    }
    if let Some(namespace) = info.namespace {
        span.record("namespace", tracing::field::display(namespace));
    }
    if let Some(actor_id) = info.actor_id {
        span.record("actor_id", tracing::field::display(actor_id));
    }
    if let Some(client_request_id) = info.client_request_id {
        span.record(
            "client_request_id",
            tracing::field::display(client_request_id),
        );
    }
    if let Some(read) = info.read {
        let read_consistency = read_consistency_tag(read);
        span.record(
            "read_consistency",
            tracing::field::display(read_consistency.as_str()),
        );
    }
    span
}

pub(super) fn record_ipc_request_metric(
    request_type: &'static str,
    started_at: Instant,
    outcome: &'static str,
) {
    metrics::ipc_request_completed(request_type, outcome, started_at.elapsed());
}
