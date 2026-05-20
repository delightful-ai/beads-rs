//! Query executors - read operations against state.
//!
//! Queries are pure reads - no clock, no dirty, no scheduling.

use std::path::Path;
use std::time::Instant;

use crossbeam::channel::Sender;

use super::core::{Daemon, ReadScope};
use super::git_worker::GitOp;
use super::ipc::{ReadConsistency, Response, ResponseExt, ResponsePayload};
use super::ops::{MapLiveError, OpError};
use super::query::{Filters, QueryResult};
use super::store::runtime::StoreRuntime;
use crate::core::{
    BeadId, BeadRef, CanonicalState, NamespaceId, NamespacePolicy, StoreState, WallClock,
};
use crate::git_lane::GitLaneState;
use crate::remote::RemoteUrl;
use beads_api::{
    BlockedIssue, CountGroup, CountResult, DeletedLookup, DepCycles, DepEdge, EpicStatus, Issue,
    IssueSummary, Note, ReadyResult, ShowDetails, StatusOutput, StatusSummary, SyncStatus,
    SyncWarning, Tombstone,
};

mod helpers;
use helpers::*;

struct ReadCtx<'a> {
    remote: RemoteUrl,
    read: ReadScope,
    store: &'a StoreRuntime,
    store_state: &'a StoreState,
    state: &'a CanonicalState,
    repo_state: Option<&'a GitLaneState>,
}

struct StatusParts {
    summary: StatusSummary,
    warnings: Vec<SyncWarning>,
    sync_dirty: bool,
    sync_in_progress: bool,
    last_sync_wall_ms: Option<u64>,
    consecutive_failures: u32,
    remote_url: RemoteUrl,
}

fn issue_summary_for_ref(store_state: &StoreState, bead_ref: &BeadRef) -> Option<IssueSummary> {
    let state = store_state.get(bead_ref.namespace())?;
    let view = state.bead_view(bead_ref.id())?;
    Some(IssueSummary::from_view(bead_ref.namespace(), &view))
}

fn require_live_store_ref(
    store_state: &StoreState,
    namespace: &NamespaceId,
    id: &BeadId,
) -> Result<(), OpError> {
    let Some(state) = store_state.get(namespace) else {
        return Err(OpError::NotFound(id.clone()));
    };
    state.require_live(id).map_live_err(id)?;
    Ok(())
}

fn show_details_for_store(
    read_namespace: &NamespaceId,
    id: &BeadId,
    state: &CanonicalState,
    store_state: &StoreState,
) -> Result<ShowDetails, OpError> {
    require_live_store_ref(store_state, read_namespace, id)?;

    let view = state.bead_view(id).expect("live bead should have view");
    let mut issue = Issue::from_view(read_namespace, &view);
    let (incoming, outgoing) = deps_for_store(read_namespace, id, store_state)?;

    issue.deps_incoming = incoming.clone();
    issue.deps_outgoing = outgoing.clone();

    let mut related_refs = std::collections::BTreeSet::new();
    for edge in &incoming {
        if let (Ok(namespace), Ok(id)) = (
            NamespaceId::parse(&edge.from_namespace),
            BeadId::parse(&edge.from),
        ) {
            related_refs.insert(BeadRef::new(namespace, id));
        }
    }
    for edge in &outgoing {
        if let (Ok(namespace), Ok(id)) = (
            NamespaceId::parse(&edge.to_namespace),
            BeadId::parse(&edge.to),
        ) {
            related_refs.insert(BeadRef::new(namespace, id));
        }
    }

    let summaries = related_refs
        .into_iter()
        .filter_map(|bead_ref| issue_summary_for_ref(store_state, &bead_ref))
        .collect();

    Ok(ShowDetails {
        issue,
        incoming,
        outgoing,
        summaries,
    })
}

fn deps_for_store(
    read_namespace: &NamespaceId,
    id: &BeadId,
    store_state: &StoreState,
) -> Result<(Vec<DepEdge>, Vec<DepEdge>), OpError> {
    require_live_store_ref(store_state, read_namespace, id)?;
    let root_ref = BeadRef::new(read_namespace.clone(), id.clone());

    let incoming = store_state
        .deps_to(&root_ref)
        .into_iter()
        .map(|key| DepEdge::from(&key))
        .collect();
    let outgoing = store_state
        .deps_from(&root_ref)
        .into_iter()
        .map(|key| DepEdge::from(&key))
        .collect();
    Ok((incoming, outgoing))
}

fn dep_tree_for_store(
    read_namespace: &NamespaceId,
    id: &BeadId,
    store_state: &StoreState,
) -> Result<Vec<DepEdge>, OpError> {
    require_live_store_ref(store_state, read_namespace, id)?;
    let root_ref = BeadRef::new(read_namespace.clone(), id.clone());

    let mut edges = Vec::new();
    let mut visited = std::collections::HashSet::new();
    let mut queue = std::collections::VecDeque::new();
    queue.push_back(root_ref);

    while let Some(current) = queue.pop_front() {
        if !visited.insert(current.clone()) {
            continue;
        }

        for key in store_state.deps_from(&current) {
            edges.push(DepEdge::from(&key));
            if !visited.contains(key.to_ref()) {
                queue.push_back(key.to_ref().clone());
            }
        }
    }

    Ok(edges)
}

fn format_bead_ref_for_namespace(read_namespace: &NamespaceId, bead_ref: &BeadRef) -> String {
    if bead_ref.namespace() == read_namespace {
        bead_ref.id().as_str().to_string()
    } else {
        bead_ref.to_string()
    }
}

fn namespace_ready_eligible(
    namespace: &NamespaceId,
    policies: &std::collections::BTreeMap<NamespaceId, NamespacePolicy>,
) -> bool {
    policies
        .get(namespace)
        .map(|policy| policy.ready_eligible)
        .unwrap_or(false)
}

fn validation_warnings_for_namespace(
    read_namespace: &NamespaceId,
    state: &CanonicalState,
    store_state: &StoreState,
) -> Vec<String> {
    let mut errors = Vec::new();

    for key in state.dep_store().values() {
        if key.from_ref().namespace() != read_namespace {
            errors.push(format!(
                "invalid dep owner: edge {} -> {} is stored in namespace {}",
                key.from_ref(),
                key.to_ref(),
                read_namespace
            ));
        }
        if store_state.resolve_ref(key.from_ref()).is_none() {
            errors.push(format!(
                "orphan dep: {} depends on {} but {} doesn't exist",
                key.from_ref(),
                key.to_ref(),
                key.from_ref()
            ));
        }
        if store_state.resolve_ref(key.to_ref()).is_none() {
            errors.push(format!(
                "orphan dep: {} depends on {} but {} doesn't exist",
                key.from_ref(),
                key.to_ref(),
                key.to_ref()
            ));
        }
    }

    for cycle in store_state.dependency_cycles() {
        if cycle
            .iter()
            .any(|bead_ref| bead_ref.namespace() == read_namespace)
            && let Some(first) = cycle.first()
        {
            errors.push(format!("dependency cycle involving {first}"));
        }
    }

    errors
}

fn ready_for_namespace(
    namespace: &NamespaceId,
    state: &CanonicalState,
    store_state: &StoreState,
    policies: &std::collections::BTreeMap<NamespaceId, NamespacePolicy>,
    limit: Option<usize>,
) -> ReadyResult {
    if !namespace_ready_eligible(namespace, policies) {
        return ReadyResult {
            issues: Vec::new(),
            blocked_count: 0,
            closed_count: 0,
        };
    }

    let blocked_by = compute_blocked_by_store(store_state);
    let mut blocked_count = 0usize;
    let mut closed_count = 0usize;

    let mut views: Vec<IssueSummary> = Vec::new();
    for (id, bead) in state.iter_live() {
        if bead.fields.workflow.value.is_closed() {
            closed_count += 1;
            continue;
        }

        let bead_ref = BeadRef::new(namespace.clone(), id.clone());
        if blocked_by.contains_key(&bead_ref) {
            blocked_count += 1;
            continue;
        }
        if let Some(view) = state.bead_view(id) {
            views.push(IssueSummary::from_view(namespace, &view));
        }
    }

    sort_ready_issues(&mut views);

    if let Some(limit) = limit {
        views.truncate(limit);
    }

    ReadyResult {
        issues: views,
        blocked_count,
        closed_count,
    }
}

fn blocked_for_namespace(
    namespace: &NamespaceId,
    state: &CanonicalState,
    store_state: &StoreState,
) -> Vec<BlockedIssue> {
    let blocked_by = compute_blocked_by_store(store_state);
    let mut out: Vec<BlockedIssue> = Vec::new();

    for (from_ref, deps) in blocked_by {
        if from_ref.namespace() != namespace {
            continue;
        }

        let view = match state.bead_view(from_ref.id()) {
            Some(view) => view,
            None => continue,
        };
        if view.bead.fields.workflow.value.is_closed() {
            continue;
        }

        let mut blocked_by_ids: Vec<String> = deps
            .into_iter()
            .map(|bead_ref| format_bead_ref_for_namespace(namespace, &bead_ref))
            .collect();
        blocked_by_ids.sort();
        blocked_by_ids.dedup();

        out.push(BlockedIssue {
            issue: IssueSummary::from_view(namespace, &view),
            blocked_by_count: blocked_by_ids.len(),
            blocked_by: blocked_by_ids,
        });
    }

    out.sort_by(|a, b| {
        b.issue
            .priority
            .cmp(&a.issue.priority)
            .then_with(|| b.issue.updated_at.cmp(&a.issue.updated_at))
    });

    out
}

impl Daemon {
    fn with_read_ctx<F, T>(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
        want_repo_state: bool,
        f: F,
    ) -> Result<T, OpError>
    where
        F: for<'a> FnOnce(ReadCtx<'a>) -> Result<T, OpError>,
    {
        let loaded = self.ensure_repo_fresh(repo, git_tx)?;
        let read = loaded.read_scope(read)?;
        loaded.check_read_gate(&read)?;
        let store = loaded.runtime();
        let state = Self::namespace_state(&loaded, read.namespace());
        let repo_state = want_repo_state.then(|| loaded.lane());
        let remote = loaded.remote().clone();

        f(ReadCtx {
            remote,
            read,
            store,
            store_state: &store.state,
            state,
            repo_state,
        })
    }

    fn with_read_ctx_without_gate<F, T>(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
        want_repo_state: bool,
        f: F,
    ) -> Result<T, OpError>
    where
        F: for<'a> FnOnce(ReadCtx<'a>) -> Result<T, OpError>,
    {
        let loaded = self.ensure_repo_fresh(repo, git_tx)?;
        let read = loaded.read_scope(read)?;
        let store = loaded.runtime();
        let state = Self::namespace_state(&loaded, read.namespace());
        let repo_state = want_repo_state.then(|| loaded.lane());
        let remote = loaded.remote().clone();

        f(ReadCtx {
            remote,
            read,
            store,
            store_state: &store.state,
            state,
            repo_state,
        })
    }

    fn with_read_ctx_response_without_gate<F>(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
        want_repo_state: bool,
        f: F,
    ) -> Response
    where
        F: for<'a> FnOnce(ReadCtx<'a>) -> Result<ResponsePayload, OpError>,
    {
        match self.with_read_ctx_without_gate(repo, read, git_tx, want_repo_state, f) {
            Ok(payload) => Response::ok(payload),
            Err(err) => Response::err_from(err),
        }
    }

    fn with_read_ctx_response<F>(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
        want_repo_state: bool,
        f: F,
    ) -> Response
    where
        F: for<'a> FnOnce(ReadCtx<'a>) -> Result<ResponsePayload, OpError>,
    {
        match self.with_read_ctx(repo, read, git_tx, want_repo_state, f) {
            Ok(payload) => Response::ok(payload),
            Err(err) => Response::err_from(err),
        }
    }

    /// Get a single bead.
    pub(in crate::runtime) fn query_show(
        &mut self,
        repo: &Path,
        id: &BeadId,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            match ctx.state.require_live(id).map_live_err(id) {
                Ok(_) => {
                    let view = ctx.state.bead_view(id).expect("live bead should have view");
                    let issue = Issue::from_view(ctx.read.namespace(), &view);
                    Ok(ResponsePayload::query(QueryResult::Issue(issue)))
                }
                Err(e) => Err(e),
            }
        })
    }

    /// Get multiple beads (batch fetch for summaries).
    pub(in crate::runtime) fn query_show_multiple(
        &mut self,
        repo: &Path,
        ids: &[BeadId],
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let mut summaries = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some(view) = ctx.state.bead_view(id) {
                    summaries.push(IssueSummary::from_view(ctx.read.namespace(), &view));
                }
            }

            Ok(ResponsePayload::query(QueryResult::Issues(summaries)))
        })
    }

    /// Get a single bead with dependency edges and dependency summaries.
    pub(in crate::runtime) fn query_show_details(
        &mut self,
        repo: &Path,
        id: &BeadId,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let details =
                show_details_for_store(ctx.read.namespace(), id, ctx.state, ctx.store_state)?;
            Ok(ResponsePayload::query(QueryResult::ShowDetails(details)))
        })
    }

    /// List beads with optional filters.
    pub(in crate::runtime) fn query_list(
        &mut self,
        repo: &Path,
        filters: &Filters,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let state = ctx.state;
            let namespace = ctx.read.namespace();

            // Build children set if parent filter is specified.
            // Parent deps: from=child, to=parent, kind=Parent.
            let children_of_parent: Option<std::collections::HashSet<BeadId>> =
                filters.parent.as_ref().map(|parent_ref| {
                    ctx.store_state
                        .deps_to(parent_ref)
                        .into_iter()
                        .filter(|key| key.kind() == crate::core::DepKind::Parent)
                        .filter(|key| key.from_ref().namespace() == namespace)
                        .map(|key| key.from_ref().id().clone())
                        .collect()
                });

            let mut views: Vec<IssueSummary> = state
                .iter_live()
                .filter_map(|(id, _)| {
                    // If parent filter specified, only include children of that parent.
                    if let Some(ref children) = children_of_parent
                        && !children.contains(id)
                    {
                        return None;
                    }
                    let view = state.bead_view(id)?;
                    if !filters.matches(&view) {
                        return None;
                    }
                    Some(IssueSummary::from_view(ctx.read.namespace(), &view))
                })
                .collect();

            // Sort
            if let Some(sort_by) = filters.sort_by {
                use super::query::SortField;
                match sort_by {
                    SortField::Priority => {
                        views.sort_by(|a, b| {
                            if filters.ascending {
                                a.priority.cmp(&b.priority)
                            } else {
                                b.priority.cmp(&a.priority)
                            }
                        });
                    }
                    SortField::CreatedAt => {
                        views.sort_by(|a, b| {
                            if filters.ascending {
                                a.created_at.cmp(&b.created_at)
                            } else {
                                b.created_at.cmp(&a.created_at)
                            }
                        });
                    }
                    SortField::UpdatedAt => {
                        views.sort_by(|a, b| {
                            if filters.ascending {
                                a.updated_at.cmp(&b.updated_at)
                            } else {
                                b.updated_at.cmp(&a.updated_at)
                            }
                        });
                    }
                    SortField::Title => {
                        views.sort_by(|a, b| {
                            if filters.ascending {
                                a.title.cmp(&b.title)
                            } else {
                                b.title.cmp(&a.title)
                            }
                        });
                    }
                }
            } else {
                // Default: sort by priority (high to low), then updated_at (newest first)
                views.sort_by(|a, b| {
                    b.priority
                        .cmp(&a.priority)
                        .then_with(|| b.updated_at.cmp(&a.updated_at))
                });
            }

            // Apply limit
            if let Some(limit) = filters.limit {
                views.truncate(limit);
            }

            Ok(ResponsePayload::query(QueryResult::Issues(views)))
        })
    }

    /// Get ready beads (no blockers, open status).
    pub(in crate::runtime) fn query_ready(
        &mut self,
        repo: &Path,
        limit: Option<usize>,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let result = ready_for_namespace(
                ctx.read.namespace(),
                ctx.state,
                ctx.store_state,
                &ctx.store.policies,
                limit,
            );
            Ok(ResponsePayload::query(QueryResult::Ready(result)))
        })
    }

    /// Get dependency tree for a bead.
    pub(in crate::runtime) fn query_dep_tree(
        &mut self,
        repo: &Path,
        id: &BeadId,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let edges = dep_tree_for_store(ctx.read.namespace(), id, ctx.store_state)?;

            Ok(ResponsePayload::query(QueryResult::DepTree {
                root: BeadRef::new(ctx.read.namespace().clone(), id.clone()),
                edges,
            }))
        })
    }

    /// Get direct dependencies for a bead.
    pub(in crate::runtime) fn query_deps(
        &mut self,
        repo: &Path,
        id: &BeadId,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let (incoming, outgoing) = deps_for_store(ctx.read.namespace(), id, ctx.store_state)?;

            Ok(ResponsePayload::query(QueryResult::Deps {
                incoming,
                outgoing,
            }))
        })
    }

    /// Get notes for a bead.
    pub(in crate::runtime) fn query_notes(
        &mut self,
        repo: &Path,
        id: &BeadId,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let state = ctx.state;

            if state.get_live(id).is_none() {
                return Err(OpError::NotFound(id.clone()));
            }

            let notes: Vec<Note> = state.notes_for(id).into_iter().map(Note::from).collect();

            Ok(ResponsePayload::query(QueryResult::Notes(notes)))
        })
    }

    /// Get sync status for a repo.
    pub(in crate::runtime) fn query_status(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let parts = match self.with_read_ctx(repo, read, git_tx, true, |ctx| {
            let ReadCtx {
                remote,
                read,
                store,
                store_state,
                state,
                repo_state,
            } = ctx;
            let repo_state = repo_state.expect("repo state requested");

            let blocked_by = compute_blocked_by_store(store_state);
            let ready_eligible = namespace_ready_eligible(read.namespace(), &store.policies);

            let mut open_issues = 0usize;
            let mut in_progress_issues = 0usize;
            let mut closed_issues = 0usize;
            let mut blocked_issues = 0usize;
            let mut ready_issues = 0usize;

            for (id, bead) in state.iter_live() {
                if bead.fields.workflow.value.is_closed() {
                    closed_issues += 1;
                    continue;
                }

                match bead.fields.workflow.value.status() {
                    "open" => open_issues += 1,
                    "in_progress" => in_progress_issues += 1,
                    _ => {}
                }

                let id_ref = BeadRef::new(read.namespace().clone(), id.clone());
                if blocked_by.contains_key(&id_ref) {
                    blocked_issues += 1;
                } else if ready_eligible {
                    ready_issues += 1;
                }
            }

            let epics_eligible_for_closure =
                compute_epic_statuses(read.namespace(), state, store_state, true).len();

            let summary = StatusSummary {
                total_issues: state.live_count(),
                open_issues,
                in_progress_issues,
                blocked_issues,
                closed_issues,
                ready_issues,
                tombstone_issues: Some(state.tombstone_count()),
                epics_eligible_for_closure: Some(epics_eligible_for_closure),
            };

            let mut warnings = Vec::new();
            if let Some(fetch) = &repo_state.last_fetch_error {
                warnings.push(SyncWarning::Fetch {
                    message: fetch.message.clone(),
                    at_wall_ms: fetch.wall_ms,
                });
            }
            if let Some(diverged) = &repo_state.last_divergence {
                warnings.push(SyncWarning::Diverged {
                    local_oid: diverged.local_oid.clone(),
                    remote_oid: diverged.remote_oid.clone(),
                    at_wall_ms: diverged.wall_ms,
                });
            }
            if let Some(force_push) = &repo_state.last_force_push {
                warnings.push(SyncWarning::ForcePush {
                    previous_remote_oid: force_push.previous_remote_oid.clone(),
                    remote_oid: force_push.remote_oid.clone(),
                    at_wall_ms: force_push.wall_ms,
                });
            }
            if let Some(skew) = &repo_state.last_clock_skew {
                warnings.push(SyncWarning::ClockSkew {
                    delta_ms: skew.delta_ms,
                    at_wall_ms: skew.wall_ms,
                });
            }
            if let Some(repair) = &store.last_wal_tail_truncated {
                warnings.push(SyncWarning::WalTailTruncated {
                    namespace: repair.namespace.clone(),
                    segment_id: repair.segment_id,
                    truncated_from_offset: repair.truncated_from_offset,
                    at_wall_ms: repair.wall_ms,
                });
            }

            Ok(StatusParts {
                summary,
                warnings,
                sync_dirty: repo_state.dirty,
                sync_in_progress: repo_state.sync_in_progress,
                last_sync_wall_ms: repo_state.last_sync_wall_ms,
                consecutive_failures: repo_state.consecutive_failures,
                remote_url: remote,
            })
        }) {
            Ok(parts) => parts,
            Err(err) => return Response::err_from(err),
        };

        let next_retry = self.next_sync_deadline_for(&parts.remote_url);
        let (next_retry_wall_ms, next_retry_in_ms) = match next_retry {
            Some(deadline) => {
                let now = Instant::now();
                let now_wall = WallClock::now().0;
                let delta = deadline.saturating_duration_since(now);
                let delta_ms = delta.as_millis() as u64;
                (Some(now_wall.saturating_add(delta_ms)), Some(delta_ms))
            }
            None => (None, None),
        };

        let sync = SyncStatus {
            dirty: parts.sync_dirty,
            sync_in_progress: parts.sync_in_progress,
            last_sync_wall_ms: parts.last_sync_wall_ms,
            next_retry_wall_ms,
            next_retry_in_ms,
            consecutive_failures: parts.consecutive_failures,
            warnings: parts.warnings,
        };

        let out = StatusOutput {
            summary: parts.summary,
            sync: Some(sync),
        };

        Response::ok(ResponsePayload::query(QueryResult::Status(out)))
    }

    /// Get blocked issues.
    pub(in crate::runtime) fn query_blocked(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let out = blocked_for_namespace(ctx.read.namespace(), ctx.state, ctx.store_state);
            Ok(ResponsePayload::query(QueryResult::Blocked(out)))
        })
    }

    /// Get stale issues (not updated recently).
    pub(in crate::runtime) fn query_stale(
        &mut self,
        repo: &Path,
        days: u32,
        status: Option<&str>,
        limit: Option<usize>,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let cutoff_ms = self
            .clock()
            .wall_ms()
            .saturating_sub(days as u64 * 24 * 60 * 60 * 1000);
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let status = status.map(|s| s.trim()).filter(|s| !s.is_empty());
            if let Some(s) = status
                && s != "open"
                && s != "in_progress"
                && s != "blocked"
            {
                return Err(OpError::ValidationFailed {
                    field: "status".into(),
                    reason: "valid values: open, in_progress, blocked".into(),
                });
            }

            let state = ctx.state;
            let blocked_by = compute_blocked_by_store(ctx.store_state);

            let mut out: Vec<IssueSummary> = Vec::new();

            for (id, bead) in state.iter_live() {
                if bead.fields.workflow.value.is_closed() {
                    continue;
                }

                let Some(view) = state.bead_view(id) else {
                    continue;
                };

                // Stale check.
                let updated_ms = view.updated_stamp().at.wall_ms;
                if updated_ms > cutoff_ms {
                    continue;
                }

                // Status filter:
                // - open/in_progress: stored workflow status
                // - blocked: derived via active `blocks` deps to open issues
                if let Some(s) = status {
                    match s {
                        "open" | "in_progress" => {
                            if bead.fields.workflow.value.status() != s {
                                continue;
                            }
                        }
                        "blocked" => {
                            let id_ref = BeadRef::new(ctx.read.namespace().clone(), id.clone());
                            if !blocked_by.contains_key(&id_ref) {
                                continue;
                            }
                        }
                        _ => {}
                    }
                }

                out.push(IssueSummary::from_view(ctx.read.namespace(), &view));
            }

            // Stalest first (oldest updated_at).
            out.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));

            // Apply limit (go default is 50; CLI should set it, but be defensive).
            let limit = limit.unwrap_or(50);
            out.truncate(limit);

            Ok(ResponsePayload::query(QueryResult::Stale(out)))
        })
    }

    /// Count issues matching filters.
    pub(in crate::runtime) fn query_count(
        &mut self,
        repo: &Path,
        filters: &Filters,
        group_by: Option<&str>,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let state = ctx.state;
            let blocked_by = compute_blocked_by_store(ctx.store_state);

            let status_filter = filters
                .status
                .as_deref()
                .map(|s| s.trim())
                .filter(|s| !s.is_empty());
            let mut base_filters = filters.clone();
            base_filters.status = None;

            // Validate group_by (if any).
            let group_by = group_by.map(|s| s.trim()).filter(|s| !s.is_empty());
            if let Some(g) = group_by {
                match g {
                    "status" | "priority" | "type" | "assignee" | "label" => {}
                    _ => {
                        return Err(OpError::ValidationFailed {
                            field: "group_by".into(),
                            reason: "valid values: status, priority, type, assignee, label".into(),
                        });
                    }
                }
            }

            let mut matched = Vec::new();

            for (id, bead) in state.iter_live() {
                let Some(view) = state.bead_view(id) else {
                    continue;
                };
                if !base_filters.matches(&view) {
                    continue;
                }

                // Status filter has a derived "blocked" meaning in beads-rs.
                if let Some(s) = status_filter {
                    match s {
                        "open" | "in_progress" | "closed" => {
                            if bead.fields.workflow.value.status() != s {
                                continue;
                            }
                        }
                        "blocked" => {
                            let id_ref = BeadRef::new(ctx.read.namespace().clone(), id.clone());
                            if bead.fields.workflow.value.is_closed()
                                || !blocked_by.contains_key(&id_ref)
                            {
                                continue;
                            }
                        }
                        "all" => {}
                        _ => {
                            return Err(OpError::ValidationFailed {
                                field: "status".into(),
                                reason: "valid values: open, in_progress, blocked, closed".into(),
                            });
                        }
                    }
                }

                matched.push(view);
            }

            let Some(group_by) = group_by else {
                return Ok(ResponsePayload::query(QueryResult::Count(
                    CountResult::Simple {
                        count: matched.len(),
                    },
                )));
            };

            let mut counts: std::collections::BTreeMap<String, usize> =
                std::collections::BTreeMap::new();
            for view in &matched {
                let bead = &view.bead;
                let id = bead.id();
                match group_by {
                    "status" => {
                        let id_ref = BeadRef::new(ctx.read.namespace().clone(), id.clone());
                        let group = if bead.fields.workflow.value.is_closed() {
                            "closed".to_string()
                        } else if blocked_by.contains_key(&id_ref) {
                            "blocked".to_string()
                        } else {
                            bead.fields.workflow.value.status().to_string()
                        };
                        *counts.entry(group).or_insert(0) += 1;
                    }
                    "priority" => {
                        let group = format!("P{}", bead.fields.priority.value.value());
                        *counts.entry(group).or_insert(0) += 1;
                    }
                    "type" => {
                        let group = bead.fields.bead_type.value.as_str().to_string();
                        *counts.entry(group).or_insert(0) += 1;
                    }
                    "assignee" => {
                        let group = match &bead.fields.claim.value {
                            crate::core::Claim::Claimed { assignee, .. } => {
                                assignee.as_str().to_string()
                            }
                            crate::core::Claim::Unclaimed => "(unassigned)".to_string(),
                        };
                        *counts.entry(group).or_insert(0) += 1;
                    }
                    "label" => {
                        if view.labels.is_empty() {
                            *counts.entry("(no labels)".to_string()).or_insert(0) += 1;
                        } else {
                            for l in view.labels.iter() {
                                *counts.entry(l.as_str().to_string()).or_insert(0) += 1;
                            }
                        }
                    }
                    _ => {}
                }
            }

            let groups: Vec<CountGroup> = counts
                .into_iter()
                .map(|(group, count)| CountGroup { group, count })
                .collect();

            Ok(ResponsePayload::query(QueryResult::Count(
                CountResult::Grouped {
                    total: matched.len(),
                    groups,
                },
            )))
        })
    }

    /// Show deleted (tombstoned) issues.
    pub(in crate::runtime) fn query_deleted(
        &mut self,
        repo: &Path,
        since_ms: Option<u64>,
        id: Option<&BeadId>,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let cutoff_ms = since_ms.map(|d| self.clock().wall_ms().saturating_sub(d));
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let state = ctx.state;

            if let Some(id) = id {
                let record = state.get_tombstone(id).map(Tombstone::from);
                let out = DeletedLookup {
                    found: record.is_some(),
                    id: id.as_str().to_string(),
                    record,
                };
                return Ok(ResponsePayload::query(QueryResult::DeletedLookup(out)));
            }

            let mut tombs: Vec<Tombstone> = state
                .iter_tombstones()
                .filter(|(_, t)| {
                    t.lineage.is_none()
                        && cutoff_ms.map(|c| t.deleted.at.wall_ms >= c).unwrap_or(true)
                })
                .map(|(_, t)| Tombstone::from(t))
                .collect();

            // Most recent first.
            tombs.sort_by(|a, b| b.deleted_at.cmp(&a.deleted_at));

            Ok(ResponsePayload::query(QueryResult::Deleted(tombs)))
        })
    }

    /// Epic completion status.
    pub(in crate::runtime) fn query_epic_status(
        &mut self,
        repo: &Path,
        eligible_only: bool,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let statuses = compute_epic_statuses(
                ctx.read.namespace(),
                ctx.state,
                ctx.store_state,
                eligible_only,
            );
            Ok(ResponsePayload::query(QueryResult::EpicStatus(statuses)))
        })
    }

    /// Validate state.
    pub(in crate::runtime) fn query_validate(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        self.with_read_ctx_response(repo, read, git_tx, false, |ctx| {
            let errors =
                validation_warnings_for_namespace(ctx.read.namespace(), ctx.state, ctx.store_state);

            Ok(ResponsePayload::query(QueryResult::Validation {
                warnings: errors,
            }))
        })
    }

    /// Dependency cycles.
    pub(in crate::runtime) fn query_dep_cycles(
        &mut self,
        repo: &Path,
        read: ReadConsistency,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        // `dep_cycles` is diagnostic/structural and should remain queryable even
        // when min-seen watermarks are not yet satisfied.
        self.with_read_ctx_response_without_gate(repo, read, git_tx, false, |ctx| {
            let cycles = dep_cycles_from_store_state(ctx.store_state);
            Ok(ResponsePayload::query(QueryResult::DepCycles(cycles)))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Issue, IssueSummary, blocked_for_namespace, dep_tree_for_store, deps_for_store,
        ready_for_namespace, show_details_for_store, validation_warnings_for_namespace,
    };
    use super::{
        compute_blocked_by, compute_blocked_by_store, dep_cycles_from_state,
        dep_cycles_from_store_state, sort_ready_issues,
    };
    use crate::core::{
        ActorId, Bead, BeadCore, BeadFields, BeadId, BeadRef, BeadType, CanonicalState, Claim,
        Closure, DepKey, DepKind, DepStore, Dot, Lww, NamespaceId, NamespacePolicy, OrSet,
        Priority, ReplicaId, Stamp, StoreState, Tombstone, Workflow, WorkflowStatus, WriteStamp,
    };
    use crate::runtime::ops::OpError;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    fn issue_summary(id: &str, priority: u8, created_at_ms: u64) -> IssueSummary {
        let stamp = WriteStamp::new(created_at_ms, 0);
        IssueSummary {
            id: id.to_string(),
            namespace: NamespaceId::core(),
            title: format!("title-{id}"),
            description: "desc".to_string(),
            design: None,
            acceptance_criteria: None,
            status: WorkflowStatus::Open,
            priority,
            issue_type: BeadType::Task,
            labels: Vec::new(),
            assignee: None,
            assignee_expires: None,
            created_at: stamp.clone(),
            created_by: "tester".to_string(),
            updated_at: stamp,
            updated_by: "tester".to_string(),
            estimated_minutes: None,
            content_hash: "hash".to_string(),
            note_count: 0,
        }
    }

    fn make_bead(id: &str, stamp: &Stamp) -> Bead {
        let id = BeadId::parse(id).expect("bead id");
        let title = format!("title-{}", id.as_str());
        let core = BeadCore::new(id, stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new(title, stamp.clone()),
            description: Lww::new("desc".to_string(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::default(), stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn ready_sort_orders_by_priority_then_created_at() {
        let mut issues = vec![
            issue_summary("a", 2, 30),
            issue_summary("b", 0, 20),
            issue_summary("c", 1, 10),
            issue_summary("d", 0, 5),
        ];

        sort_ready_issues(&mut issues);

        let ids: Vec<&str> = issues.iter().map(|issue| issue.id.as_str()).collect();
        assert_eq!(ids, vec!["d", "b", "c", "a"]);
    }

    #[test]
    fn issue_summary_uses_read_namespace() {
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1_000, 0), actor);
        let bead = make_bead("bd-123", &stamp);
        let namespace = NamespaceId::parse("wf").unwrap();
        let mut state = CanonicalState::new();

        state.insert(bead).expect("insert bead");
        let view = state
            .bead_view(&BeadId::parse("bd-123").unwrap())
            .expect("bead view");

        let summary = IssueSummary::from_view(&namespace, &view);

        assert_eq!(summary.namespace, namespace);
    }

    #[test]
    fn issue_view_uses_read_namespace() {
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(2_000, 0), actor);
        let bead = make_bead("bd-456", &stamp);
        let namespace = NamespaceId::parse("wf").unwrap();
        let mut state = CanonicalState::new();

        state.insert(bead).expect("insert bead");
        let view = state
            .bead_view(&BeadId::parse("bd-456").unwrap())
            .expect("bead view");

        let issue = Issue::from_view(&namespace, &view);

        assert_eq!(issue.namespace, namespace);
    }

    fn add_dep(state: &mut CanonicalState, from: &str, to: &str, kind: DepKind, counter: u64) {
        let key = DepKey::new_local(
            &NamespaceId::core(),
            BeadId::parse(from).expect("from id"),
            BeadId::parse(to).expect("to id"),
            kind,
        )
        .expect("dep key");
        let dep_key = state.check_dep_add_key(key).expect("checked dep key");
        let dot = Dot {
            replica: ReplicaId::new(Uuid::from_u128(counter as u128 + 1)),
            counter,
        };
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(10_000 + counter, 0), actor);
        state.apply_dep_add(dep_key, dot, stamp);
    }

    fn ref_in(namespace: &NamespaceId, id: &str) -> BeadRef {
        BeadRef::new(namespace.clone(), BeadId::parse(id).expect("bead id"))
    }

    fn add_store_dep(
        store: &mut StoreState,
        owner_namespace: &NamespaceId,
        from_namespace: &NamespaceId,
        from: &str,
        to_namespace: &NamespaceId,
        to: &str,
        counter: u64,
    ) {
        add_store_dep_kind(
            store,
            owner_namespace,
            from_namespace,
            from,
            to_namespace,
            to,
            DepKind::Blocks,
            counter,
        );
    }

    fn add_store_dep_kind(
        store: &mut StoreState,
        owner_namespace: &NamespaceId,
        from_namespace: &NamespaceId,
        from: &str,
        to_namespace: &NamespaceId,
        to: &str,
        kind: DepKind,
        counter: u64,
    ) {
        let key = DepKey::new(ref_in(from_namespace, from), ref_in(to_namespace, to), kind)
            .expect("dep key");
        let dep_key = store.check_dep_add_key(key).expect("checked dep key");
        let dot = Dot {
            replica: ReplicaId::new(Uuid::from_u128(counter as u128 + 1)),
            counter,
        };
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(20_000 + counter, 0), actor);
        store
            .get_mut(owner_namespace)
            .expect("owner namespace")
            .apply_dep_add(dep_key, dot, stamp);
    }

    fn add_store_dep_unchecked(
        store: &mut StoreState,
        owner_namespace: &NamespaceId,
        from_namespace: &NamespaceId,
        from: &str,
        to_namespace: &NamespaceId,
        to: &str,
        counter: u64,
    ) {
        let key = DepKey::new(
            ref_in(from_namespace, from),
            ref_in(to_namespace, to),
            DepKind::Blocks,
        )
        .expect("dep key");
        let dot = Dot {
            replica: ReplicaId::new(Uuid::from_u128(counter as u128 + 1)),
            counter,
        };
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(20_000 + counter, 0), actor);
        let state = store.get_mut(owner_namespace).expect("owner namespace");
        let mut set = OrSet::new();
        set.apply_add(dot, key);
        state.set_dep_store(DepStore::from_parts(set, Some(stamp)));
    }

    fn cross_namespace_store() -> (StoreState, NamespaceId, NamespaceId, NamespaceId) {
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(4_000, 0), actor);
        let sessions = NamespaceId::parse("sessions").unwrap();
        let extmsg = NamespaceId::parse("extmsg").unwrap();
        let core = NamespaceId::core();
        let mut store = StoreState::new();
        store
            .core_mut()
            .insert(make_bead("bd-core", &stamp))
            .expect("core bead");
        store
            .ensure_namespace(sessions.clone())
            .insert(make_bead("bd-session", &stamp))
            .expect("session bead");
        store
            .ensure_namespace(extmsg.clone())
            .insert(make_bead("bd-extmsg", &stamp))
            .expect("extmsg bead");
        add_store_dep(
            &mut store,
            &sessions,
            &sessions,
            "bd-session",
            &core,
            "bd-core",
            1,
        );
        add_store_dep(&mut store, &core, &core, "bd-core", &extmsg, "bd-extmsg", 2);
        (store, sessions, core, extmsg)
    }

    fn close_bead(store: &mut StoreState, namespace: &NamespaceId, id: &str, stamp: Stamp) {
        let bead = store
            .get_mut(namespace)
            .and_then(|state| state.get_mut(&BeadId::parse(id).expect("bead id")))
            .expect("live bead");
        bead.fields.workflow = Lww::new(
            Workflow::Closed(Closure::new(Some("done".to_string()), None)),
            stamp,
        );
    }

    fn policies_for(namespaces: &[NamespaceId]) -> BTreeMap<NamespaceId, NamespacePolicy> {
        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        for namespace in namespaces {
            policies
                .entry(namespace.clone())
                .or_insert_with(NamespacePolicy::core_default);
        }
        policies
    }

    #[test]
    fn blocked_by_uses_live_ready_work_dep_kinds() {
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(3_000, 0), actor);
        let mut state = CanonicalState::new();

        for id in ["bd-cond", "bd-waits", "bd-parent", "bd-track", "bd-blocker"] {
            state.insert(make_bead(id, &stamp)).expect("insert bead");
        }

        add_dep(
            &mut state,
            "bd-cond",
            "bd-blocker",
            DepKind::ConditionalBlocks,
            1,
        );
        add_dep(&mut state, "bd-waits", "bd-blocker", DepKind::WaitsFor, 2);
        add_dep(&mut state, "bd-parent", "bd-blocker", DepKind::Parent, 3);
        add_dep(&mut state, "bd-track", "bd-blocker", DepKind::Tracks, 4);

        let blocked_by = compute_blocked_by(&state);

        assert_eq!(
            blocked_by
                .get(&BeadId::parse("bd-cond").unwrap())
                .map(Vec::as_slice),
            Some(&[BeadId::parse("bd-blocker").unwrap()][..])
        );
        assert_eq!(
            blocked_by
                .get(&BeadId::parse("bd-waits").unwrap())
                .map(Vec::as_slice),
            Some(&[BeadId::parse("bd-blocker").unwrap()][..])
        );
        assert!(
            !blocked_by.contains_key(&BeadId::parse("bd-parent").unwrap()),
            "parent-child is structural and must not block ready work"
        );
        assert!(
            !blocked_by.contains_key(&BeadId::parse("bd-track").unwrap()),
            "tracks must remain non-blocking"
        );
    }

    #[test]
    fn blocked_by_store_uses_cross_namespace_ready_work_dep_kinds() {
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(3_500, 0), actor);
        let sessions = NamespaceId::parse("sessions").unwrap();
        let core = NamespaceId::core();
        let mut store = StoreState::new();

        for id in ["bd-cond", "bd-waits", "bd-parent", "bd-track"] {
            store
                .ensure_namespace(sessions.clone())
                .insert(make_bead(id, &stamp))
                .expect("session bead");
        }
        store
            .core_mut()
            .insert(make_bead("bd-blocker", &stamp))
            .expect("core blocker");

        add_store_dep_kind(
            &mut store,
            &sessions,
            &sessions,
            "bd-cond",
            &core,
            "bd-blocker",
            DepKind::ConditionalBlocks,
            1,
        );
        add_store_dep_kind(
            &mut store,
            &sessions,
            &sessions,
            "bd-waits",
            &core,
            "bd-blocker",
            DepKind::WaitsFor,
            2,
        );
        add_store_dep_kind(
            &mut store,
            &sessions,
            &sessions,
            "bd-parent",
            &core,
            "bd-blocker",
            DepKind::Parent,
            3,
        );
        add_store_dep_kind(
            &mut store,
            &sessions,
            &sessions,
            "bd-track",
            &core,
            "bd-blocker",
            DepKind::Tracks,
            4,
        );

        let blocked_by = compute_blocked_by_store(&store);

        assert_eq!(
            blocked_by
                .get(&ref_in(&sessions, "bd-cond"))
                .map(Vec::as_slice),
            Some(&[ref_in(&core, "bd-blocker")][..])
        );
        assert_eq!(
            blocked_by
                .get(&ref_in(&sessions, "bd-waits"))
                .map(Vec::as_slice),
            Some(&[ref_in(&core, "bd-blocker")][..])
        );
        assert!(
            !blocked_by.contains_key(&ref_in(&sessions, "bd-parent")),
            "parent-child is structural and must not block ready work"
        );
        assert!(
            !blocked_by.contains_key(&ref_in(&sessions, "bd-track")),
            "tracks must remain non-blocking"
        );
    }

    #[test]
    fn ready_for_namespace_respects_cross_namespace_open_and_closed_blockers() {
        let (mut store, sessions, core, extmsg) = cross_namespace_store();
        let policies = policies_for(&[sessions.clone(), core.clone(), extmsg]);
        let session_state = store.get(&sessions).expect("sessions state");

        let blocked_ready = ready_for_namespace(&sessions, session_state, &store, &policies, None);

        assert!(blocked_ready.issues.is_empty());
        assert_eq!(blocked_ready.blocked_count, 1);
        assert_eq!(blocked_ready.closed_count, 0);

        let close_stamp = Stamp::new(
            WriteStamp::new(5_000, 0),
            ActorId::new("closer").expect("actor"),
        );
        close_bead(&mut store, &core, "bd-core", close_stamp);
        let session_state = store.get(&sessions).expect("sessions state");

        let unblocked_ready =
            ready_for_namespace(&sessions, session_state, &store, &policies, None);

        assert_eq!(unblocked_ready.blocked_count, 0);
        assert_eq!(unblocked_ready.closed_count, 0);
        assert_eq!(
            unblocked_ready
                .issues
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["bd-session"]
        );
    }

    #[test]
    fn ready_for_namespace_suppresses_non_ready_namespaces() {
        let (store, sessions, core, extmsg) = cross_namespace_store();
        let mut policies = policies_for(&[sessions.clone(), core, extmsg]);
        policies
            .get_mut(&sessions)
            .expect("sessions policy")
            .ready_eligible = false;
        let session_state = store.get(&sessions).expect("sessions state");

        let ready = ready_for_namespace(&sessions, session_state, &store, &policies, None);

        assert!(ready.issues.is_empty());
        assert_eq!(ready.blocked_count, 0);
        assert_eq!(ready.closed_count, 0);
    }

    #[test]
    fn blocked_for_namespace_formats_cross_namespace_blockers_compactly() {
        let (store, sessions, _, _) = cross_namespace_store();
        let session_state = store.get(&sessions).expect("sessions state");

        let blocked = blocked_for_namespace(&sessions, session_state, &store);

        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].issue.namespace, sessions);
        assert_eq!(blocked[0].issue.id, "bd-session");
        assert_eq!(blocked[0].blocked_by, vec!["core/bd-core"]);
    }

    #[test]
    fn deps_for_store_returns_cross_namespace_edges_from_either_endpoint() {
        let (store, sessions, core, _) = cross_namespace_store();
        let session_id = BeadId::parse("bd-session").unwrap();
        let core_id = BeadId::parse("bd-core").unwrap();

        let (_, outgoing) = deps_for_store(&sessions, &session_id, &store).unwrap();
        assert_eq!(outgoing.len(), 1);
        assert_eq!(outgoing[0].from_namespace, "sessions");
        assert_eq!(outgoing[0].from, "bd-session");
        assert_eq!(outgoing[0].to_namespace, "core");
        assert_eq!(outgoing[0].to, "bd-core");

        let (incoming, _) = deps_for_store(&core, &core_id, &store).unwrap();
        assert_eq!(incoming.len(), 1);
        assert_eq!(incoming[0].from_namespace, "sessions");
        assert_eq!(incoming[0].from, "bd-session");
        assert_eq!(incoming[0].to_namespace, "core");
        assert_eq!(incoming[0].to, "bd-core");
    }

    #[test]
    fn show_details_for_store_summarizes_cross_namespace_targets() {
        let (store, sessions, _, _) = cross_namespace_store();
        let session_state = store.get(&sessions).expect("sessions state");
        let details = show_details_for_store(
            &sessions,
            &BeadId::parse("bd-session").unwrap(),
            session_state,
            &store,
        )
        .unwrap();

        assert_eq!(details.issue.namespace, sessions);
        assert_eq!(details.outgoing.len(), 1);
        assert_eq!(details.outgoing[0].to_namespace, "core");
        assert_eq!(details.summaries.len(), 1);
        assert_eq!(details.summaries[0].namespace, NamespaceId::core());
        assert_eq!(details.summaries[0].id, "bd-core");
    }

    #[test]
    fn validation_accepts_cross_namespace_dep_with_live_target() {
        let (store, sessions, _, _) = cross_namespace_store();
        let session_state = store.get(&sessions).expect("sessions state");

        let warnings = validation_warnings_for_namespace(&sessions, session_state, &store);

        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn show_details_for_store_preserves_deleted_lookup_error() {
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(4_500, 0), actor);
        let id = BeadId::parse("bd-deleted").unwrap();
        let mut store = StoreState::new();
        store
            .core_mut()
            .insert(make_bead(id.as_str(), &stamp))
            .expect("core bead");
        store
            .core_mut()
            .delete(Tombstone::new(id.clone(), stamp, None));

        let err = show_details_for_store(&NamespaceId::core(), &id, store.core(), &store)
            .expect_err("deleted bead should not hydrate");

        assert!(matches!(err, OpError::BeadDeleted(found) if found == id));
    }

    #[test]
    fn dep_tree_for_store_traverses_cross_namespace_edges() {
        let (store, sessions, _, _) = cross_namespace_store();
        let edges =
            dep_tree_for_store(&sessions, &BeadId::parse("bd-session").unwrap(), &store).unwrap();

        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].from_namespace, "sessions");
        assert_eq!(edges[0].to_namespace, "core");
        assert_eq!(edges[1].from_namespace, "core");
        assert_eq!(edges[1].to_namespace, "extmsg");
    }

    #[test]
    fn epic_status_counts_cross_namespace_parent_children() {
        let (mut store, sessions, core, _) = cross_namespace_store();
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(5_000, 0), actor);
        store
            .core_mut()
            .get_mut(&BeadId::parse("bd-core").unwrap())
            .expect("core bead")
            .fields
            .bead_type = Lww::new(BeadType::Epic, stamp.clone());
        close_bead(&mut store, &sessions, "bd-session", stamp);
        add_store_dep_kind(
            &mut store,
            &sessions,
            &sessions,
            "bd-session",
            &core,
            "bd-core",
            DepKind::Parent,
            9,
        );

        let statuses = super::helpers::compute_epic_statuses(&core, store.core(), &store, false);

        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].total_children, 1);
        assert_eq!(statuses[0].closed_children, 1);
        assert!(statuses[0].eligible_for_close);
    }

    #[test]
    fn epic_status_ignores_tombstoned_children_with_retained_parent_edges() {
        let (mut store, sessions, core, _) = cross_namespace_store();
        let actor = ActorId::new("tester").unwrap();
        let stamp = Stamp::new(WriteStamp::new(5_000, 0), actor);
        store
            .core_mut()
            .get_mut(&BeadId::parse("bd-core").unwrap())
            .expect("core bead")
            .fields
            .bead_type = Lww::new(BeadType::Epic, stamp.clone());
        close_bead(&mut store, &sessions, "bd-session", stamp.clone());
        add_store_dep_kind(
            &mut store,
            &sessions,
            &sessions,
            "bd-session",
            &core,
            "bd-core",
            DepKind::Parent,
            9,
        );
        let deleted_id = BeadId::parse("bd-deleted").unwrap();
        store
            .ensure_namespace(sessions.clone())
            .insert(make_bead(deleted_id.as_str(), &stamp))
            .expect("deleted child");
        add_store_dep_kind(
            &mut store,
            &sessions,
            &sessions,
            deleted_id.as_str(),
            &core,
            "bd-core",
            DepKind::Parent,
            10,
        );
        store
            .get_mut(&sessions)
            .expect("sessions namespace")
            .delete(Tombstone::new(deleted_id, stamp, None));

        let statuses = super::helpers::compute_epic_statuses(&core, store.core(), &store, false);

        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].total_children, 1);
        assert_eq!(statuses[0].closed_children, 1);
        assert!(statuses[0].eligible_for_close);
    }

    #[test]
    fn dep_cycles_from_state_reports_cycle() {
        let mut state = CanonicalState::new();
        let actor = ActorId::new("alice").unwrap();
        let stamp = Stamp::new(WriteStamp::new(1000, 0), actor);

        let a = "bd-aaa";
        let b = "bd-bbb";
        state.insert(make_bead(a, &stamp)).expect("insert bead a");
        state.insert(make_bead(b, &stamp)).expect("insert bead b");

        let ab = DepKey::new_local(
            &NamespaceId::core(),
            crate::core::BeadId::parse(a).unwrap(),
            crate::core::BeadId::parse(b).unwrap(),
            DepKind::Blocks,
        )
        .expect("dep key");
        let ba = DepKey::new_local(
            &NamespaceId::core(),
            crate::core::BeadId::parse(b).unwrap(),
            crate::core::BeadId::parse(a).unwrap(),
            DepKind::Blocks,
        )
        .expect("dep key");
        let dep_dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let dep_dot_2 = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([2u8; 16])),
            counter: 1,
        };
        let ab = state.check_dep_add_key(ab).expect("dep key");
        let ba = state.check_dep_add_key(ba).expect("dep key");
        state.apply_dep_add(ab, dep_dot, stamp.clone());
        state.apply_dep_add(ba, dep_dot_2, stamp);

        let cycles = dep_cycles_from_state(&state);
        assert_eq!(
            cycles.cycles,
            vec![vec![a.to_string(), b.to_string(), a.to_string()]]
        );
    }

    #[test]
    fn dep_cycles_from_store_state_reports_cross_namespace_cycle() {
        let (mut store, sessions, core, _) = cross_namespace_store();
        add_store_dep_unchecked(
            &mut store,
            &core,
            &core,
            "bd-core",
            &sessions,
            "bd-session",
            10,
        );

        let cycles = dep_cycles_from_store_state(&store);

        assert_eq!(cycles.cycles.len(), 1);
        let cycle = &cycles.cycles[0];
        assert_eq!(cycle.first(), cycle.last());
        assert!(cycle.iter().any(|entry| entry == "core/bd-core"));
        assert!(cycle.iter().any(|entry| entry == "sessions/bd-session"));
    }
}
