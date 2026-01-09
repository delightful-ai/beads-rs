//! Query executors - read operations against state.
//!
//! Queries are pure reads - no clock, no dirty, no scheduling.

use std::path::Path;
use std::time::Instant;

use crossbeam::channel::Sender;

use super::core::Daemon;
use super::git_worker::GitOp;
use super::ipc::{Response, ResponsePayload};
use super::ops::{MapLiveError, OpError};
use super::query::{Filters, QueryResult};
use crate::api::{
    BlockedIssue, CountGroup, CountResult, DeletedLookup, DepEdge, EpicStatus, Issue, IssueSummary,
    Note, StatusOutput, StatusSummary, SyncStatus, SyncWarning, Tombstone,
};
use crate::core::{BeadId, DepKind, DepLife, WallClock};

impl Daemon {
    /// Get a single bead.
    pub fn query_show(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        match repo_state.state.require_live(id).map_live_err(id) {
            Ok(bead) => {
                let issue = Issue::from_bead(bead);
                Response::ok(ResponsePayload::Query(QueryResult::Issue(issue)))
            }
            Err(e) => Response::err(e),
        }
    }

    /// List beads with optional filters.
    pub fn query_list(
        &mut self,
        repo: &Path,
        filters: &Filters,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        // Build children set if parent filter is specified.
        // Parent deps: from=child, to=parent, kind=Parent.
        let children_of_parent: Option<std::collections::HashSet<&BeadId>> =
            filters.parent.as_ref().map(|parent_id| {
                repo_state
                    .state
                    .iter_deps()
                    .filter(|(key, edge)| {
                        edge.life.value == DepLife::Active
                            && key.kind() == DepKind::Parent
                            && key.to() == parent_id
                    })
                    .map(|(key, _)| key.from())
                    .collect()
            });

        let mut views: Vec<IssueSummary> = repo_state
            .state
            .iter_live()
            .filter(|(id, bead)| {
                // If parent filter specified, only include children of that parent.
                if let Some(ref children) = children_of_parent
                    && !children.contains(id)
                {
                    return false;
                }
                filters.matches(bead)
            })
            .map(|(_, bead)| IssueSummary::from_bead(bead))
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

        Response::ok(ResponsePayload::Query(QueryResult::Issues(views)))
    }

    /// Get ready beads (no blockers, open status).
    pub fn query_ready(
        &mut self,
        repo: &Path,
        limit: Option<usize>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        // Collect IDs that are blocked by *blocking* deps (kind=blocks).
        let mut blocked: std::collections::HashSet<&BeadId> = std::collections::HashSet::new();
        for (key, edge) in repo_state.state.iter_deps() {
            // Only count active `blocks` edges where the target is not closed.
            if edge.life.value == DepLife::Active
                && key.kind() == crate::core::DepKind::Blocks
                && let Some(to_bead) = repo_state.state.get_live(key.to())
                && !to_bead.fields.workflow.value.is_closed()
            {
                blocked.insert(key.from());
            }
        }

        // Count blocked and closed issues for summary.
        let mut blocked_count = 0usize;
        let mut closed_count = 0usize;

        let mut views: Vec<IssueSummary> = repo_state
            .state
            .iter_live()
            .filter(|(id, bead)| {
                if bead.fields.workflow.value.is_closed() {
                    closed_count += 1;
                    return false;
                }
                if blocked.contains(id) {
                    blocked_count += 1;
                    return false;
                }
                true
            })
            .map(|(_, bead)| IssueSummary::from_bead(bead))
            .collect();

        // Sort by priority (low to high), then created_at (oldest first).
        sort_ready_issues(&mut views);

        // Apply limit
        if let Some(limit) = limit {
            views.truncate(limit);
        }

        let result = crate::api::ReadyResult {
            issues: views,
            blocked_count,
            closed_count,
        };

        Response::ok(ResponsePayload::Query(QueryResult::Ready(result)))
    }

    /// Get dependency tree for a bead.
    pub fn query_dep_tree(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        // Check if bead exists
        if let Err(e) = repo_state.state.require_live(id).map_live_err(id) {
            return Response::err(e);
        }

        // Collect all edges in the transitive closure
        let mut edges = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(id.clone());

        while let Some(current) = queue.pop_front() {
            if !visited.insert(current.clone()) {
                continue;
            }

            for (key, edge) in repo_state.state.iter_deps() {
                if key.from() == &current && edge.life.value == DepLife::Active {
                    edges.push(DepEdge::from((key, edge)));
                    if !visited.contains(key.to()) {
                        queue.push_back(key.to().clone());
                    }
                }
            }
        }

        Response::ok(ResponsePayload::Query(QueryResult::DepTree {
            root: id.clone(),
            edges,
        }))
    }

    /// Get direct dependencies for a bead.
    pub fn query_deps(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        // Check if bead exists
        if let Err(e) = repo_state.state.require_live(id).map_live_err(id) {
            return Response::err(e);
        }

        let mut incoming = Vec::new();
        let mut outgoing = Vec::new();

        for (key, edge) in repo_state.state.iter_deps() {
            if edge.life.value != DepLife::Active {
                continue;
            }

            if key.from() == id {
                outgoing.push(DepEdge::from((key, edge)));
            }
            if key.to() == id {
                incoming.push(DepEdge::from((key, edge)));
            }
        }

        Response::ok(ResponsePayload::Query(QueryResult::Deps {
            incoming,
            outgoing,
        }))
    }

    /// Get notes for a bead.
    pub fn query_notes(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        let bead = match repo_state.state.get_live(id) {
            Some(b) => b,
            None => return Response::err(OpError::NotFound(id.clone())),
        };

        let notes: Vec<Note> = bead.notes.sorted().into_iter().map(Note::from).collect();

        Response::ok(ResponsePayload::Query(QueryResult::Notes(notes)))
    }

    /// Get sync status for a repo.
    pub fn query_status(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        let blocked_by = compute_blocked_by(repo_state);
        let blocked_set: std::collections::HashSet<&BeadId> = blocked_by.keys().collect();

        let mut open_issues = 0usize;
        let mut in_progress_issues = 0usize;
        let mut closed_issues = 0usize;
        let mut blocked_issues = 0usize;
        let mut ready_issues = 0usize;

        for (id, bead) in repo_state.state.iter_live() {
            if bead.fields.workflow.value.is_closed() {
                closed_issues += 1;
                continue;
            }

            match bead.fields.workflow.value.status() {
                "open" => open_issues += 1,
                "in_progress" => in_progress_issues += 1,
                _ => {}
            }

            if blocked_set.contains(id) {
                blocked_issues += 1;
            } else {
                ready_issues += 1;
            }
        }

        let epics_eligible_for_closure = compute_epic_statuses(repo_state, true).len();

        let summary = StatusSummary {
            total_issues: repo_state.state.live_count(),
            open_issues,
            in_progress_issues,
            blocked_issues,
            closed_issues,
            ready_issues,
            tombstone_issues: Some(repo_state.state.tombstone_count()),
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

        let next_retry = self.next_sync_deadline_for(remote.remote());
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
            dirty: repo_state.dirty,
            sync_in_progress: repo_state.sync_in_progress,
            last_sync_wall_ms: repo_state.last_sync_wall_ms,
            next_retry_wall_ms,
            next_retry_in_ms,
            consecutive_failures: repo_state.consecutive_failures,
            warnings,
        };

        let out = StatusOutput {
            summary,
            sync: Some(sync),
        };

        Response::ok(ResponsePayload::Query(QueryResult::Status(out)))
    }

    /// Get blocked issues.
    pub fn query_blocked(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        let blocked_by = compute_blocked_by(repo_state);
        let mut out: Vec<BlockedIssue> = Vec::new();

        for (from, deps) in blocked_by {
            let bead = match repo_state.state.get_live(&from) {
                Some(b) => b,
                None => continue,
            };
            if bead.fields.workflow.value.is_closed() {
                continue;
            }

            let mut blocked_by_ids: Vec<String> =
                deps.into_iter().map(|id| id.as_str().to_string()).collect();
            blocked_by_ids.sort();
            blocked_by_ids.dedup();

            out.push(BlockedIssue {
                issue: IssueSummary::from_bead(bead),
                blocked_by_count: blocked_by_ids.len(),
                blocked_by: blocked_by_ids,
            });
        }

        // Sort by priority (high to low), then updated_at (newest first).
        out.sort_by(|a, b| {
            b.issue
                .priority
                .cmp(&a.issue.priority)
                .then_with(|| b.issue.updated_at.cmp(&a.issue.updated_at))
        });

        Response::ok(ResponsePayload::Query(QueryResult::Blocked(out)))
    }

    /// Get stale issues (not updated recently).
    pub fn query_stale(
        &mut self,
        repo: &Path,
        days: u32,
        status: Option<&str>,
        limit: Option<usize>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        let status = status.map(|s| s.trim()).filter(|s| !s.is_empty());
        if let Some(s) = status
            && s != "open"
            && s != "in_progress"
            && s != "blocked"
        {
            return Response::err(OpError::ValidationFailed {
                field: "status".into(),
                reason: "valid values: open, in_progress, blocked".into(),
            });
        }

        let blocked_by = compute_blocked_by(repo_state);
        let cutoff_ms = self
            .clock()
            .wall_ms()
            .saturating_sub(days as u64 * 24 * 60 * 60 * 1000);

        let mut out: Vec<IssueSummary> = Vec::new();

        for (id, bead) in repo_state.state.iter_live() {
            if bead.fields.workflow.value.is_closed() {
                continue;
            }

            // Stale check.
            let updated_ms = bead.updated_stamp().at.wall_ms;
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
                        if !blocked_by.contains_key(id) {
                            continue;
                        }
                    }
                    _ => {}
                }
            }

            out.push(IssueSummary::from_bead(bead));
        }

        // Stalest first (oldest updated_at).
        out.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));

        // Apply limit (go default is 50; CLI should set it, but be defensive).
        let limit = limit.unwrap_or(50);
        out.truncate(limit);

        Response::ok(ResponsePayload::Query(QueryResult::Stale(out)))
    }

    /// Count issues matching filters.
    pub fn query_count(
        &mut self,
        repo: &Path,
        filters: &Filters,
        group_by: Option<&str>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        let blocked_by = compute_blocked_by(repo_state);

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
                    return Response::err(OpError::ValidationFailed {
                        field: "group_by".into(),
                        reason: "valid values: status, priority, type, assignee, label".into(),
                    });
                }
            }
        }

        let mut matched: Vec<(&BeadId, &crate::core::Bead)> = Vec::new();

        for (id, bead) in repo_state.state.iter_live() {
            if !base_filters.matches(bead) {
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
                        if bead.fields.workflow.value.is_closed() || !blocked_by.contains_key(id) {
                            continue;
                        }
                    }
                    "all" => {}
                    _ => {
                        return Response::err(OpError::ValidationFailed {
                            field: "status".into(),
                            reason: "valid values: open, in_progress, blocked, closed".into(),
                        });
                    }
                }
            }

            matched.push((id, bead));
        }

        let Some(group_by) = group_by else {
            return Response::ok(ResponsePayload::Query(QueryResult::Count(
                CountResult::Simple {
                    count: matched.len(),
                },
            )));
        };

        let mut counts: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for (id, bead) in &matched {
            match group_by {
                "status" => {
                    let group = if bead.fields.workflow.value.is_closed() {
                        "closed".to_string()
                    } else if blocked_by.contains_key(*id) {
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
                    if bead.fields.labels.value.is_empty() {
                        *counts.entry("(no labels)".to_string()).or_insert(0) += 1;
                    } else {
                        for l in bead.fields.labels.value.iter() {
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

        Response::ok(ResponsePayload::Query(QueryResult::Count(
            CountResult::Grouped {
                total: matched.len(),
                groups,
            },
        )))
    }

    /// Show deleted (tombstoned) issues.
    pub fn query_deleted(
        &mut self,
        repo: &Path,
        since_ms: Option<u64>,
        id: Option<&BeadId>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        if let Some(id) = id {
            let record = repo_state.state.get_tombstone(id).map(Tombstone::from);
            let out = DeletedLookup {
                found: record.is_some(),
                id: id.as_str().to_string(),
                record,
            };
            return Response::ok(ResponsePayload::Query(QueryResult::DeletedLookup(out)));
        }

        let cutoff_ms = since_ms.map(|d| self.clock().wall_ms().saturating_sub(d));
        let mut tombs: Vec<Tombstone> = repo_state
            .state
            .iter_tombstones()
            .filter(|(_, t)| {
                t.lineage.is_none() && cutoff_ms.map(|c| t.deleted.at.wall_ms >= c).unwrap_or(true)
            })
            .map(|(_, t)| Tombstone::from(t))
            .collect();

        // Most recent first.
        tombs.sort_by(|a, b| b.deleted_at.cmp(&a.deleted_at));

        Response::ok(ResponsePayload::Query(QueryResult::Deleted(tombs)))
    }

    /// Epic completion status.
    pub fn query_epic_status(
        &mut self,
        repo: &Path,
        eligible_only: bool,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        let statuses = compute_epic_statuses(repo_state, eligible_only);
        Response::ok(ResponsePayload::Query(QueryResult::EpicStatus(statuses)))
    }

    /// Validate state.
    pub fn query_validate(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state,
            Err(e) => return Response::err(e),
        };

        // Run validation checks
        let mut errors = Vec::new();

        // Check for orphan dependencies
        for (key, edge) in repo_state.state.iter_deps() {
            if edge.life.value != DepLife::Active {
                continue;
            }
            if repo_state.state.get_live(key.from()).is_none() {
                errors.push(format!(
                    "orphan dep: {} depends on {} but {} doesn't exist",
                    key.from().as_str(),
                    key.to().as_str(),
                    key.from().as_str()
                ));
            }
            if repo_state.state.get_live(key.to()).is_none() {
                errors.push(format!(
                    "orphan dep: {} depends on {} but {} doesn't exist",
                    key.from().as_str(),
                    key.to().as_str(),
                    key.to().as_str()
                ));
            }
        }

        // Check for dependency cycles
        // (simplified - just report if found, don't enumerate all)
        for (id, _) in repo_state.state.iter_live() {
            let mut visited = std::collections::HashSet::new();
            let mut queue = std::collections::VecDeque::new();
            queue.push_back(id.clone());

            while let Some(current) = queue.pop_front() {
                if current == *id && !visited.is_empty() {
                    errors.push(format!("dependency cycle involving {}", id.as_str()));
                    break;
                }
                if !visited.insert(current.clone()) {
                    continue;
                }
                for (key, edge) in repo_state.state.iter_deps() {
                    if key.from() == &current && edge.life.value == DepLife::Active {
                        queue.push_back(key.to().clone());
                    }
                }
            }
        }

        Response::ok(ResponsePayload::Query(QueryResult::Validation {
            warnings: errors,
        }))
    }
}

fn compute_blocked_by(
    repo_state: &super::repo::RepoState,
) -> std::collections::BTreeMap<BeadId, Vec<BeadId>> {
    let mut blocked: std::collections::BTreeMap<BeadId, Vec<BeadId>> =
        std::collections::BTreeMap::new();

    for (key, edge) in repo_state.state.iter_deps() {
        if edge.life.value != DepLife::Active {
            continue;
        }
        if key.kind() != DepKind::Blocks {
            continue;
        }

        // Only count blockers that are currently open (not closed).
        if let Some(to_bead) = repo_state.state.get_live(key.to()) {
            if to_bead.fields.workflow.value.is_closed() {
                continue;
            }
        } else {
            continue;
        }

        blocked
            .entry(key.from().clone())
            .or_default()
            .push(key.to().clone());
    }

    blocked
}

fn compute_epic_statuses(
    repo_state: &super::repo::RepoState,
    eligible_only: bool,
) -> Vec<EpicStatus> {
    // Build epic -> children mapping from parent edges.
    let mut children: std::collections::BTreeMap<BeadId, Vec<BeadId>> =
        std::collections::BTreeMap::new();
    for (key, edge) in repo_state.state.iter_deps() {
        if edge.life.value != DepLife::Active {
            continue;
        }
        if key.kind() != DepKind::Parent {
            continue;
        }
        children
            .entry(key.to().clone())
            .or_default()
            .push(key.from().clone());
    }

    let mut out = Vec::new();
    for (id, bead) in repo_state.state.iter_live() {
        if bead.fields.bead_type.value != crate::core::BeadType::Epic {
            continue;
        }
        if bead.fields.workflow.value.is_closed() {
            continue;
        }

        let child_ids = children.get(id).cloned().unwrap_or_default();
        let total_children = child_ids.len();
        let closed_children = child_ids
            .iter()
            .filter(|cid| {
                repo_state
                    .state
                    .get_live(cid)
                    .map(|b| b.fields.workflow.value.is_closed())
                    .unwrap_or(false)
            })
            .count();

        let eligible_for_close = total_children > 0 && closed_children == total_children;
        if eligible_only && !eligible_for_close {
            continue;
        }

        out.push(EpicStatus {
            epic: IssueSummary::from_bead(bead),
            total_children,
            closed_children,
            eligible_for_close,
        });
    }

    out.sort_by(|a, b| a.epic.id.cmp(&b.epic.id));
    out
}

fn sort_ready_issues(issues: &mut [IssueSummary]) {
    issues.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| a.created_at.cmp(&b.created_at))
    });
}

#[cfg(test)]
mod tests {
    use super::sort_ready_issues;
    use crate::api::IssueSummary;
    use crate::core::WriteStamp;

    fn issue_summary(id: &str, priority: u8, created_at_ms: u64) -> IssueSummary {
        let stamp = WriteStamp::new(created_at_ms, 0);
        IssueSummary {
            id: id.to_string(),
            title: format!("title-{id}"),
            description: "desc".to_string(),
            design: None,
            acceptance_criteria: None,
            status: "open".to_string(),
            priority,
            issue_type: "task".to_string(),
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
}
