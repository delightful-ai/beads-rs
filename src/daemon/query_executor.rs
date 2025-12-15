//! Query executors - read operations against state.
//!
//! Queries are pure reads - no clock, no dirty, no scheduling.

use std::path::Path;

use crossbeam::channel::Sender;

use super::core::Daemon;
use super::git_worker::GitOp;
use super::ipc::{Response, ResponsePayload};
use super::ops::OpError;
use super::query::{Filters, QueryResult};
use crate::api::{
    BlockedIssue, CountGroup, CountResult, DeletedLookup, DepEdge, EpicStatus, Issue, IssueSummary,
    Note, StatusOutput, StatusSummary, SyncStatus, Tombstone,
};
use crate::core::{BeadId, DepKind, DepLife};

impl Daemon {
    /// Get a single bead.
    pub fn query_show(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = self.repo_state(&remote).unwrap();

        match repo_state.state.get_live(id) {
            Some(bead) => {
                let issue = Issue::from_bead(bead);
                Response::ok(ResponsePayload::Query(QueryResult::Issue(issue)))
            }
            None => {
                // Check if deleted
                if repo_state.state.get_tombstone(id).is_some() {
                    return Response::err(OpError::BeadDeleted(id.clone()));
                }
                Response::err(OpError::NotFound(id.clone()))
            }
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
        let repo_state = self.repo_state(&remote).unwrap();

        let mut views: Vec<IssueSummary> = repo_state
            .state
            .iter_live()
            .filter(|(_, bead)| filters.matches(bead))
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
        let repo_state = self.repo_state(&remote).unwrap();

        // Collect IDs that are blocked by *blocking* deps (kind=blocks).
        let mut blocked: std::collections::HashSet<&BeadId> = std::collections::HashSet::new();
        for (_, edge) in repo_state.state.iter_deps() {
            // Only count active `blocks` edges where the target is not closed.
            if edge.life.value == DepLife::Active
                && edge.key.kind == crate::core::DepKind::Blocks
                && let Some(to_bead) = repo_state.state.get_live(&edge.key.to)
                && !to_bead.fields.workflow.value.is_closed()
            {
                blocked.insert(&edge.key.from);
            }
        }

        let mut views: Vec<IssueSummary> = repo_state
            .state
            .iter_live()
            .filter(|(id, bead)| {
                // Must be open
                !bead.fields.workflow.value.is_closed()
                    // Must not be blocked
                    && !blocked.contains(id)
            })
            .map(|(_, bead)| IssueSummary::from_bead(bead))
            .collect();

        // Sort by priority (high to low), then created_at (oldest first)
        views.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });

        // Apply limit
        if let Some(limit) = limit {
            views.truncate(limit);
        }

        Response::ok(ResponsePayload::Query(QueryResult::Issues(views)))
    }

    /// Get dependency tree for a bead.
    pub fn query_dep_tree(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = self.repo_state(&remote).unwrap();

        // Check if bead exists
        if repo_state.state.get_live(id).is_none() {
            return Response::err(OpError::NotFound(id.clone()));
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

            for (_, edge) in repo_state.state.iter_deps() {
                if edge.key.from == current && edge.life.value == DepLife::Active {
                    edges.push(DepEdge::from(edge));
                    if !visited.contains(&edge.key.to) {
                        queue.push_back(edge.key.to.clone());
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
        let repo_state = self.repo_state(&remote).unwrap();

        // Check if bead exists
        if repo_state.state.get_live(id).is_none() {
            return Response::err(OpError::NotFound(id.clone()));
        }

        let mut incoming = Vec::new();
        let mut outgoing = Vec::new();

        for (_, edge) in repo_state.state.iter_deps() {
            if edge.life.value != DepLife::Active {
                continue;
            }

            if edge.key.from == *id {
                outgoing.push(DepEdge::from(edge));
            }
            if edge.key.to == *id {
                incoming.push(DepEdge::from(edge));
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
        let repo_state = self.repo_state(&remote).unwrap();

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
        let repo_state = self.repo_state(&remote).unwrap();

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

        let sync = SyncStatus {
            dirty: repo_state.dirty,
            sync_in_progress: repo_state.sync_in_progress,
            last_sync_wall_ms: repo_state.last_sync_wall_ms,
            consecutive_failures: repo_state.consecutive_failures,
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
        let repo_state = self.repo_state(&remote).unwrap();

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
        let repo_state = self.repo_state(&remote).unwrap();

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
        let repo_state = self.repo_state(&remote).unwrap();

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

        if group_by.is_none() {
            return Response::ok(ResponsePayload::Query(QueryResult::Count(
                CountResult::Simple {
                    count: matched.len(),
                },
            )));
        }

        let mut counts: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for (id, bead) in &matched {
            match group_by.unwrap() {
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
        let repo_state = self.repo_state(&remote).unwrap();

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
        let repo_state = self.repo_state(&remote).unwrap();

        let statuses = compute_epic_statuses(repo_state, eligible_only);
        Response::ok(ResponsePayload::Query(QueryResult::EpicStatus(statuses)))
    }

    /// Validate state.
    pub fn query_validate(&mut self, repo: &Path, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_fresh(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let repo_state = self.repo_state(&remote).unwrap();

        // Run validation checks
        let mut errors = Vec::new();

        // Check for orphan dependencies
        for (_, edge) in repo_state.state.iter_deps() {
            if edge.life.value != DepLife::Active {
                continue;
            }
            if repo_state.state.get_live(&edge.key.from).is_none() {
                errors.push(format!(
                    "orphan dep: {} depends on {} but {} doesn't exist",
                    edge.key.from.as_str(),
                    edge.key.to.as_str(),
                    edge.key.from.as_str()
                ));
            }
            if repo_state.state.get_live(&edge.key.to).is_none() {
                errors.push(format!(
                    "orphan dep: {} depends on {} but {} doesn't exist",
                    edge.key.from.as_str(),
                    edge.key.to.as_str(),
                    edge.key.to.as_str()
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
                for (_, edge) in repo_state.state.iter_deps() {
                    if edge.key.from == current && edge.life.value == DepLife::Active {
                        queue.push_back(edge.key.to.clone());
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

    for (_, edge) in repo_state.state.iter_deps() {
        if edge.life.value != DepLife::Active {
            continue;
        }
        if edge.key.kind != DepKind::Blocks {
            continue;
        }

        // Only count blockers that are currently open (not closed).
        if let Some(to_bead) = repo_state.state.get_live(&edge.key.to) {
            if to_bead.fields.workflow.value.is_closed() {
                continue;
            }
        } else {
            continue;
        }

        blocked
            .entry(edge.key.from.clone())
            .or_default()
            .push(edge.key.to.clone());
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
    for (_, edge) in repo_state.state.iter_deps() {
        if edge.life.value != DepLife::Active {
            continue;
        }
        if edge.key.kind != DepKind::Parent {
            continue;
        }
        children
            .entry(edge.key.to.clone())
            .or_default()
            .push(edge.key.from.clone());
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
