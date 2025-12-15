//! Operation executors - apply mutations to state.
//!
//! Each mutation:
//! 1. Ensures repo is loaded
//! 2. Advances the clock
//! 3. Mutates state
//! 4. Marks dirty and schedules sync
//! 5. Returns response

use std::path::Path;

use crossbeam::channel::Sender;

use super::core::Daemon;
use super::git_worker::GitOp;
use super::ipc::{Response, ResponsePayload};
use super::ops::{BeadPatch, MapLiveError, OpError, OpResult, Patch};
use crate::core::{
    Bead, BeadCore, BeadFields, BeadId, BeadType, Claim, Closure, DepEdge, DepKey, DepKind,
    DepLife, Labels, Lww, Note, NoteId, Priority, Stamp, Tombstone, WallClock, Workflow,
};

impl Daemon {
    /// Create a new bead.
    #[allow(clippy::too_many_arguments)]
    pub fn apply_create(
        &mut self,
        repo: &Path,
        requested_id: Option<String>,
        parent: Option<String>,
        title: String,
        bead_type: BeadType,
        priority: Priority,
        description: Option<String>,
        design: Option<String>,
        acceptance_criteria: Option<String>,
        assignee: Option<String>,
        external_ref: Option<String>,
        estimated_minutes: Option<u32>,
        labels: Vec<String>,
        dependencies: Vec<String>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let requested_id = requested_id
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let parent = parent
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        if requested_id.is_some() && parent.is_some() {
            return Response::err(OpError::ValidationFailed {
                field: "create".into(),
                reason: "cannot specify both --id and --parent".into(),
            });
        }

        // Validate title is not empty or whitespace-only
        let title = title.trim().to_string();
        if title.is_empty() {
            return Response::err(OpError::ValidationFailed {
                field: "title".into(),
                reason: "title cannot be empty".into(),
            });
        }

        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Advance clock (needs &mut self, so do this before re-borrowing repo)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();
        let stamp = Stamp {
            at: write_stamp.clone(),
            by: actor.clone(),
        };

        // Re-borrow repo_state for mutations
        let repo_state = self.repo_state_mut(&remote);

        // Parse dependency specs first (for validation + source_repo inheritance).
        let parsed_deps = match parse_dep_specs(&dependencies) {
            Ok(v) => v,
            Err(e) => return Response::err(e),
        };

        // Determine ID.
        let desc_str = description.unwrap_or_default();
        let (id, parent_id) = match (requested_id, parent) {
            (Some(raw), None) => {
                let id = match BeadId::parse(&raw) {
                    Ok(id) => id,
                    Err(e) => {
                        return Response::err(OpError::ValidationFailed {
                            field: "id".into(),
                            reason: e.to_string(),
                        });
                    }
                };
                if repo_state.state.get_live(&id).is_some()
                    || repo_state.state.get_tombstone(&id).is_some()
                {
                    return Response::err(OpError::AlreadyExists(id));
                }
                (id, None)
            }
            (None, Some(raw)) => {
                let parent_id = match BeadId::parse(&raw) {
                    Ok(id) => id,
                    Err(e) => {
                        return Response::err(OpError::ValidationFailed {
                            field: "parent".into(),
                            reason: e.to_string(),
                        });
                    }
                };
                let child = match next_child_id(&repo_state.state, &parent_id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(e),
                };
                (child, Some(parent_id))
            }
            (None, None) => (
                generate_unique_id(
                    &repo_state.state,
                    repo_state.root_slug.as_deref(),
                    &title,
                    &desc_str,
                    &actor,
                    &write_stamp,
                    remote.remote(),
                ),
                None,
            ),
            (Some(_), Some(_)) => unreachable!("guarded above"),
        };

        // Validate dep targets exist (parity with beads-go daemon).
        for (_, to) in &parsed_deps {
            if repo_state.state.get_live(to).is_none() {
                return Response::err(OpError::NotFound(to.clone()));
            }
        }

        // Inherit source_repo from discovered-from parent if present.
        let mut source_repo_value: Option<String> = None;
        if let Some((_, from_id)) = parsed_deps
            .iter()
            .find(|(k, _)| matches!(k, DepKind::DiscoveredFrom))
            && let Some(parent_bead) = repo_state.state.get_live(from_id)
            && let Some(sr) = &parent_bead.fields.source_repo.value
            && !sr.trim().is_empty()
        {
            source_repo_value = Some(sr.clone());
        }

        // Validate + canonicalize labels.
        let mut label_set = Labels::new();
        for raw in labels {
            let label = match crate::core::Label::parse(raw) {
                Ok(l) => l,
                Err(e) => {
                    return Response::err(OpError::ValidationFailed {
                        field: "labels".into(),
                        reason: e.to_string(),
                    });
                }
            };
            label_set.insert(label);
        }

        let design = design.and_then(|s| {
            let t = s.trim().to_string();
            if t.is_empty() { None } else { Some(t) }
        });
        let acceptance_criteria = acceptance_criteria.and_then(|s| {
            let t = s.trim().to_string();
            if t.is_empty() { None } else { Some(t) }
        });
        let external_ref = external_ref.and_then(|s| {
            let t = s.trim().to_string();
            if t.is_empty() { None } else { Some(t) }
        });

        // Create claim at creation time if assignee specified (only current actor).
        let claim_value = match assignee.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            None => Claim::Unclaimed,
            Some("me") | Some("self") => {
                let expires = WallClock(write_stamp.wall_ms + 3600 * 1000);
                Claim::claimed(actor.clone(), Some(expires))
            }
            Some(raw) => {
                if raw != actor.as_str() {
                    return Response::err(OpError::ValidationFailed {
                        field: "assignee".into(),
                        reason: "cannot assign other actors; run bd as that actor".into(),
                    });
                }
                let expires = WallClock(write_stamp.wall_ms + 3600 * 1000);
                Claim::claimed(actor.clone(), Some(expires))
            }
        };

        // Create bead
        let core = BeadCore::new(id.clone(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new(title, stamp.clone()),
            description: Lww::new(desc_str, stamp.clone()),
            design: Lww::new(design, stamp.clone()),
            acceptance_criteria: Lww::new(acceptance_criteria, stamp.clone()),
            priority: Lww::new(priority, stamp.clone()),
            bead_type: Lww::new(bead_type, stamp.clone()),
            labels: Lww::new(label_set, stamp.clone()),
            external_ref: Lww::new(external_ref, stamp.clone()),
            source_repo: Lww::new(source_repo_value, stamp.clone()),
            estimated_minutes: Lww::new(estimated_minutes, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(claim_value, stamp.clone()),
        };
        let bead = Bead::new(core, fields);

        // Insert into state
        repo_state.state.insert_live(bead);

        // Parent edge (from child -> parent).
        if let Some(parent_id) = parent_id {
            if repo_state.state.get_live(&parent_id).is_none() {
                return Response::err(OpError::NotFound(parent_id));
            }
            let key = match DepKey::new(id.clone(), parent_id, DepKind::Parent) {
                Ok(k) => k,
                Err(e) => {
                    return Response::err(OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: e.reason,
                    });
                }
            };
            repo_state
                .state
                .insert_dep(DepEdge::new(key, stamp.clone()));
        }

        // Dependency edges.
        for (kind, to) in parsed_deps {
            let key = match DepKey::new(id.clone(), to, kind) {
                Ok(k) => k,
                Err(e) => {
                    return Response::err(OpError::ValidationFailed {
                        field: "dependency".into(),
                        reason: e.reason,
                    });
                }
            };
            repo_state
                .state
                .insert_dep(DepEdge::new(key, stamp.clone()));
        }

        // Mark dirty and schedule sync
        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::Created { id }))
    }

    /// Update an existing bead.
    pub fn apply_update(
        &mut self,
        repo: &Path,
        id: &BeadId,
        patch: BeadPatch,
        cas: Option<String>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        // Validate patch
        if let Err(e) = patch.validate() {
            return Response::err(e);
        }

        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead existence and CAS (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            let bead = match repo_state.state.require_live(id).map_live_err(id) {
                Ok(b) => b,
                Err(e) => return Response::err(e),
            };

            // CAS check
            if let Some(expected) = cas {
                let actual = bead.content_hash().to_hex();
                if expected != actual {
                    return Response::err(OpError::CasMismatch { expected, actual });
                }
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();
        let stamp = Stamp {
            at: write_stamp,
            by: actor,
        };

        // Re-borrow for mutations (safe: bead confirmed to exist above, no deletions between)
        let repo_state = self.repo_state_mut(&remote);
        let bead = match repo_state.state.require_live_mut(id).map_live_err(id) {
            Ok(b) => b,
            Err(e) => return Response::err(e),
        };

        // Apply patch
        if let Patch::Set(v) = patch.title {
            bead.fields.title = Lww::new(v, stamp.clone());
        }
        if let Patch::Set(v) = patch.description {
            bead.fields.description = Lww::new(v, stamp.clone());
        }
        match patch.design {
            Patch::Set(v) => bead.fields.design = Lww::new(Some(v), stamp.clone()),
            Patch::Clear => bead.fields.design = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        match patch.acceptance_criteria {
            Patch::Set(v) => bead.fields.acceptance_criteria = Lww::new(Some(v), stamp.clone()),
            Patch::Clear => bead.fields.acceptance_criteria = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        if let Patch::Set(v) = patch.priority {
            bead.fields.priority = Lww::new(v, stamp.clone());
        }
        if let Patch::Set(v) = patch.bead_type {
            bead.fields.bead_type = Lww::new(v, stamp.clone());
        }
        if let Patch::Set(v) = patch.labels {
            let mut labels = Labels::new();
            for raw in v {
                let label = crate::core::Label::parse(raw).map_err(|e| OpError::ValidationFailed {
                    field: "labels".into(),
                    reason: e.to_string(),
                });
                let label = match label {
                    Ok(l) => l,
                    Err(e) => return Response::err(e),
                };
                labels.insert(label);
            }
            bead.fields.labels = Lww::new(labels, stamp.clone());
        }
        match patch.external_ref {
            Patch::Set(v) => bead.fields.external_ref = Lww::new(Some(v), stamp.clone()),
            Patch::Clear => bead.fields.external_ref = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        match patch.source_repo {
            Patch::Set(v) => bead.fields.source_repo = Lww::new(Some(v), stamp.clone()),
            Patch::Clear => bead.fields.source_repo = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        match patch.estimated_minutes {
            Patch::Set(v) => bead.fields.estimated_minutes = Lww::new(Some(v), stamp.clone()),
            Patch::Clear => bead.fields.estimated_minutes = Lww::new(None, stamp.clone()),
            Patch::Keep => {}
        }
        // Handle status changes via workflow transitions
        if let Patch::Set(status) = patch.status {
            match status.as_str() {
                "open" => bead.fields.workflow = Lww::new(Workflow::Open, stamp.clone()),
                "in_progress" => {
                    bead.fields.workflow = Lww::new(Workflow::InProgress, stamp.clone())
                }
                "closed" => {
                    let closure = Closure::new(None, None);
                    bead.fields.workflow = Lww::new(Workflow::Closed(closure), stamp.clone());
                }
                other => {
                    return Response::err(OpError::ValidationFailed {
                        field: "status".into(),
                        reason: format!("unknown status {other:?}"),
                    });
                }
            }
        }

        // Mark dirty and schedule sync
        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::Updated { id: id.clone() }))
    }

    /// Close a bead.
    pub fn apply_close(
        &mut self,
        repo: &Path,
        id: &BeadId,
        reason: Option<String>,
        on_branch: Option<String>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead state (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            let bead = match repo_state.state.require_live(id).map_live_err(id) {
                Ok(b) => b,
                Err(e) => return Response::err(e),
            };

            // Check if already closed
            if bead.fields.workflow.value.is_closed() {
                return Response::err(OpError::InvalidTransition {
                    from: "closed".into(),
                    to: "closed".into(),
                });
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();
        let stamp = Stamp {
            at: write_stamp,
            by: actor,
        };

        // Re-borrow for mutations (safe: bead confirmed to exist above, no deletions between)
        let repo_state = self.repo_state_mut(&remote);
        let bead = match repo_state.state.require_live_mut(id).map_live_err(id) {
            Ok(b) => b,
            Err(e) => return Response::err(e),
        };
        let closure = Closure::new(reason, on_branch);
        bead.fields.workflow = Lww::new(Workflow::Closed(closure), stamp);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::Closed { id: id.clone() }))
    }

    /// Reopen a closed bead.
    pub fn apply_reopen(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead state (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            let bead = match repo_state.state.require_live(id).map_live_err(id) {
                Ok(b) => b,
                Err(e) => return Response::err(e),
            };

            // Check if actually closed
            if !bead.fields.workflow.value.is_closed() {
                let from_status = bead.fields.workflow.value.status().to_string();
                return Response::err(OpError::InvalidTransition {
                    from: from_status,
                    to: "open".into(),
                });
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();
        let stamp = Stamp {
            at: write_stamp,
            by: actor,
        };

        // Re-borrow for mutations (safe: bead confirmed to exist above, no deletions between)
        let repo_state = self.repo_state_mut(&remote);
        let bead = match repo_state.state.require_live_mut(id).map_live_err(id) {
            Ok(b) => b,
            Err(e) => return Response::err(e),
        };
        bead.fields.workflow = Lww::new(Workflow::Open, stamp);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::Reopened { id: id.clone() }))
    }

    /// Delete a bead (soft delete via tombstone).
    pub fn apply_delete(
        &mut self,
        repo: &Path,
        id: &BeadId,
        reason: Option<String>,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead existence (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            if let Err(e) = repo_state.state.require_live(id).map_live_err(id) {
                return Response::err(e);
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();
        let stamp = Stamp {
            at: write_stamp,
            by: actor,
        };

        // Re-borrow for mutations
        let repo_state = self.repo_state_mut(&remote);

        // Create tombstone
        let tombstone = Tombstone::new(id.clone(), stamp, reason);

        // Remove from live, add to tombstones
        repo_state.state.remove_live(id);
        repo_state.state.insert_tombstone(tombstone);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::Deleted { id: id.clone() }))
    }

    /// Add a dependency.
    pub fn apply_add_dep(
        &mut self,
        repo: &Path,
        from: &BeadId,
        to: &BeadId,
        kind: DepKind,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        // Validate DepKey (rejects self-dependencies)
        let key = match DepKey::new(from.clone(), to.clone(), kind) {
            Ok(k) => k,
            Err(e) => {
                return Response::err(OpError::ValidationFailed {
                    field: "dependency".into(),
                    reason: e.reason,
                });
            }
        };

        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check both beads exist and detect cycles (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            if repo_state.state.get_live(from).is_none() {
                return Response::err(OpError::NotFound(from.clone()));
            }
            if repo_state.state.get_live(to).is_none() {
                return Response::err(OpError::NotFound(to.clone()));
            }

            // Check for circular dependency: if there's a path from `to` to `from`,
            // adding `from -> to` would create a cycle.
            // Only DAG-enforced kinds (Blocks, Parent) reject cycles.
            if would_create_cycle(&repo_state.state, from, to, kind) {
                return Response::err(OpError::ValidationFailed {
                    field: "dependency".into(),
                    reason: format!(
                        "circular dependency: {} already depends on {} (directly or transitively)",
                        to, from
                    ),
                });
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();
        let stamp = Stamp {
            at: write_stamp,
            by: actor,
        };

        // Re-borrow for mutations
        let repo_state = self.repo_state_mut(&remote);

        // Create dependency edge (new edges are active by default)
        let edge = DepEdge::new(key, stamp);

        repo_state.state.insert_dep(edge);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::DepAdded {
            from: from.clone(),
            to: to.clone(),
        }))
    }

    /// Remove a dependency (soft delete).
    pub fn apply_remove_dep(
        &mut self,
        repo: &Path,
        from: &BeadId,
        to: &BeadId,
        kind: DepKind,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        // Validate DepKey (self-deps can't exist, so can't be removed)
        let key = match DepKey::new(from.clone(), to.clone(), kind) {
            Ok(k) => k,
            Err(e) => {
                return Response::err(OpError::ValidationFailed {
                    field: "dependency".into(),
                    reason: e.reason,
                });
            }
        };

        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();
        let stamp = Stamp {
            at: write_stamp,
            by: actor,
        };

        // Re-borrow for mutations
        let repo_state = self.repo_state_mut(&remote);

        // Create a deleted edge (will merge with existing)
        let life = Lww::new(DepLife::Deleted, stamp.clone());
        let edge = DepEdge::with_life(key, stamp, life);

        repo_state.state.insert_dep(edge);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::DepRemoved {
            from: from.clone(),
            to: to.clone(),
        }))
    }

    /// Add a note to a bead.
    pub fn apply_add_note(
        &mut self,
        repo: &Path,
        id: &BeadId,
        content: String,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead existence (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            if let Err(e) = repo_state.state.require_live(id).map_live_err(id) {
                return Response::err(e);
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let actor = self.actor().clone();

        // Re-borrow for mutations (safe: bead confirmed to exist above, no deletions between)
        let repo_state = self.repo_state_mut(&remote);
        let bead = match repo_state.state.require_live_mut(id).map_live_err(id) {
            Ok(b) => b,
            Err(e) => return Response::err(e),
        };

        // Generate unique note ID
        let note_id = NoteId::generate();

        let note = Note::new(note_id.clone(), content, actor, write_stamp);
        bead.notes.insert(note);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::NoteAdded {
            bead_id: id.clone(),
            note_id: note_id.as_str().to_string(),
        }))
    }

    /// Claim a bead.
    pub fn apply_claim(
        &mut self,
        repo: &Path,
        id: &BeadId,
        lease_secs: u64,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Get actor and current time for pre-check
        let actor = self.actor().clone();
        let now_ms = self.clock().wall_ms();

        let result = self.with_mutation(&remote, |repo_state, stamp| {
            let bead = repo_state
                .state
                .get_live_mut(id)
                .ok_or_else(|| OpError::NotFound(id.clone()))?;

            // Check if already claimed (and not expired)
            if let Claim::Claimed {
                ref assignee,
                expires: Some(exp),
            } = bead.fields.claim.value
            {
                let now = WallClock(now_ms);
                if exp >= now && assignee != &actor {
                    return Err(OpError::AlreadyClaimed {
                        by: assignee.clone(),
                        expires: Some(exp),
                    });
                }
            }

            let expires = WallClock(stamp.at.wall_ms + lease_secs * 1000);
            bead.fields.claim = Lww::new(Claim::claimed(actor, Some(expires)), stamp.clone());
            // Claiming also transitions to in_progress
            bead.fields.workflow = Lww::new(Workflow::InProgress, stamp);

            Ok(expires)
        });

        match result {
            Ok(expires) => {
                self.mark_dirty_and_schedule(&remote, git_tx);
                Response::ok(ResponsePayload::Op(OpResult::Claimed {
                    id: id.clone(),
                    expires,
                }))
            }
            Err(e) => Response::err(e),
        }
    }

    /// Release a claim.
    pub fn apply_unclaim(&mut self, repo: &Path, id: &BeadId, git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Get actor for checks
        let actor = self.actor().clone();

        // Check bead state and claim ownership (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            let bead = match repo_state.state.require_live(id).map_live_err(id) {
                Ok(b) => b,
                Err(e) => return Response::err(e),
            };

            // Check if claimed by this actor
            if let Claim::Claimed { ref assignee, .. } = bead.fields.claim.value
                && assignee != &actor
            {
                return Response::err(OpError::NotClaimedByYou);
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let stamp = Stamp {
            at: write_stamp,
            by: actor,
        };

        // Re-borrow for mutations (safe: bead confirmed to exist above, no deletions between)
        let repo_state = self.repo_state_mut(&remote);
        let bead = match repo_state.state.require_live_mut(id).map_live_err(id) {
            Ok(b) => b,
            Err(e) => return Response::err(e),
        };
        bead.fields.claim = Lww::new(Claim::Unclaimed, stamp.clone());
        // Unclaiming transitions back to open
        bead.fields.workflow = Lww::new(Workflow::Open, stamp);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::Unclaimed { id: id.clone() }))
    }

    /// Extend an existing claim.
    pub fn apply_extend_claim(
        &mut self,
        repo: &Path,
        id: &BeadId,
        lease_secs: u64,
        git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded(repo, git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Get actor for checks
        let actor = self.actor().clone();

        // Check bead state and claim ownership (scope to drop borrow)
        {
            let repo_state = self.repo_state_mut(&remote);
            let bead = match repo_state.state.require_live(id).map_live_err(id) {
                Ok(b) => b,
                Err(e) => return Response::err(e),
            };

            // Check if claimed by this actor
            if let Claim::Claimed { ref assignee, .. } = bead.fields.claim.value {
                if assignee != &actor {
                    return Response::err(OpError::NotClaimedByYou);
                }
            } else {
                return Response::err(OpError::NotClaimedByYou);
            }
        }

        // Advance clock (needs &mut self)
        let write_stamp = self.clock_mut().tick();
        let expires = WallClock(write_stamp.wall_ms + lease_secs * 1000);
        let stamp = Stamp {
            at: write_stamp,
            by: actor.clone(),
        };

        // Re-borrow for mutations (safe: bead confirmed to exist above, no deletions between)
        let repo_state = self.repo_state_mut(&remote);
        let bead = match repo_state.state.require_live_mut(id).map_live_err(id) {
            Ok(b) => b,
            Err(e) => return Response::err(e),
        };
        bead.fields.claim = Lww::new(Claim::claimed(actor, Some(expires)), stamp);

        self.mark_dirty_and_schedule(&remote, git_tx);

        Response::ok(ResponsePayload::Op(OpResult::ClaimExtended {
            id: id.clone(),
            expires,
        }))
    }
}

fn parse_dep_specs(raw: &[String]) -> Result<Vec<(DepKind, BeadId)>, OpError> {
    let mut out = Vec::new();
    for spec in raw {
        for part in spec.split(',') {
            let p = part.trim();
            if p.is_empty() {
                continue;
            }

            let (kind, id_raw) = if let Some((k, id)) = p.split_once(':') {
                (parse_dep_kind_str(k)?, id.trim())
            } else {
                (DepKind::Blocks, p)
            };

            let to = BeadId::parse(id_raw).map_err(|e| OpError::ValidationFailed {
                field: "deps".into(),
                reason: e.to_string(),
            })?;
            out.push((kind, to));
        }
    }
    Ok(out)
}

fn parse_dep_kind_str(raw: &str) -> Result<DepKind, OpError> {
    let s = raw.trim().to_lowercase().replace('-', "_");
    match s.as_str() {
        "blocks" | "block" => Ok(DepKind::Blocks),
        "parent" | "parent_child" | "parentchild" => Ok(DepKind::Parent),
        "related" | "relates" => Ok(DepKind::Related),
        "discovered_from" | "discoveredfrom" => Ok(DepKind::DiscoveredFrom),
        _ => Err(OpError::ValidationFailed {
            field: "deps".into(),
            reason: format!("invalid dependency type {raw:?}"),
        }),
    }
}

fn next_child_id(state: &crate::core::CanonicalState, parent: &BeadId) -> Result<BeadId, OpError> {
    if state.get_live(parent).is_none() {
        return Err(OpError::NotFound(parent.clone()));
    }

    let depth = parent.as_str().chars().filter(|c| *c == '.').count();
    if depth >= 3 {
        return Err(OpError::ValidationFailed {
            field: "parent".into(),
            reason: format!(
                "maximum hierarchy depth (3) exceeded for parent {}",
                parent.as_str()
            ),
        });
    }

    let prefix = format!("{}.", parent.as_str());
    let mut max_child: u32 = 0;

    for id in state
        .iter_live()
        .map(|(id, _)| id)
        .chain(state.iter_tombstones().map(|(k, _)| &k.id))
    {
        let Some(rest) = id.as_str().strip_prefix(&prefix) else {
            continue;
        };
        let first = rest.split('.').next().unwrap_or("");
        if first.is_empty() {
            continue;
        }
        if let Ok(n) = first.parse::<u32>() {
            max_child = max_child.max(n);
        }
    }

    let mut next = max_child + 1;
    loop {
        let candidate = BeadId::parse(&format!("{}.{}", parent.as_str(), next)).map_err(|e| {
            OpError::ValidationFailed {
                field: "parent".into(),
                reason: e.to_string(),
            }
        })?;

        if !state.contains(&candidate) {
            return Ok(candidate);
        }
        next += 1;
    }
}

/// Generate a unique bead ID that doesn't collide with existing ones.
///
/// Parity with beads-go:
/// - Content-hash IDs (SHA256) encoded base36
/// - Adaptive base length 3→8 based on birthday-paradox threshold
/// - Progressive extension on collision using a nonce
fn generate_unique_id(
    state: &crate::core::CanonicalState,
    root_slug: Option<&str>,
    title: &str,
    description: &str,
    actor: &crate::core::ActorId,
    stamp: &crate::core::WriteStamp,
    remote: &crate::daemon::RemoteUrl,
) -> BeadId {
    let slug = root_slug
        .map(|s| s.to_string())
        .unwrap_or_else(|| infer_bead_slug(state));
    let num_top_level = state
        .iter_live()
        .filter(|(id, _)| id.is_top_level())
        .count();

    let base_len = compute_adaptive_length(num_top_level);

    for len in base_len..=8 {
        for nonce in 0..10 {
            let short = generate_hash_suffix(title, description, actor, stamp, remote, len, nonce);
            let candidate =
                BeadId::parse(&format!("{}-{}", slug, short)).expect("generated id must be valid");
            if !state.contains(&candidate) {
                return candidate;
            }
        }
    }

    // Extremely unlikely fallback.
    BeadId::generate_with_slug(&slug, 8)
}

fn compute_adaptive_length(num_issues: usize) -> usize {
    for len in 3..=8 {
        if collision_probability(num_issues, len) <= 0.25 {
            return len;
        }
    }
    8
}

fn collision_probability(num_issues: usize, id_len: usize) -> f64 {
    let base = 36.0f64;
    let total = base.powi(id_len as i32);
    let n = num_issues as f64;
    1.0 - (-n * n / (2.0 * total)).exp()
}

fn generate_hash_suffix(
    title: &str,
    description: &str,
    actor: &crate::core::ActorId,
    stamp: &crate::core::WriteStamp,
    remote: &crate::daemon::RemoteUrl,
    len: usize,
    nonce: usize,
) -> String {
    use sha2::{Digest, Sha256};

    let content = format!(
        "{}|{}|{}|{}|{}|{}",
        title,
        description,
        actor.as_str(),
        stamp.wall_ms,
        stamp.counter,
        remote.as_str(),
    );
    let content = format!("{}|{}", content, nonce);
    let hash = Sha256::digest(content.as_bytes());

    let num_bytes = match len {
        3 => 2,
        4 => 3,
        5 | 6 => 4,
        7 | 8 => 5,
        _ => 3,
    };

    encode_base36(&hash[..num_bytes], len)
}

/// Infer slug from existing beads (fallback when root_slug not set).
fn infer_bead_slug(state: &crate::core::CanonicalState) -> String {
    use std::collections::BTreeMap;

    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for id in state
        .iter_live()
        .map(|(id, _)| id)
        .chain(state.iter_tombstones().map(|(k, _)| &k.id))
    {
        *counts.entry(id.slug()).or_default() += 1;
    }
    let mut best_slug: Option<&str> = None;
    let mut best_count: usize = 0;
    for (slug, count) in counts {
        if count > best_count {
            best_slug = Some(slug);
            best_count = count;
        }
    }
    best_slug
        .map(|s| s.to_string())
        .unwrap_or_else(|| "bd".to_string())
}

fn encode_base36(bytes: &[u8], len: usize) -> String {
    let mut num: u64 = 0;
    for &b in bytes {
        num = (num << 8) | b as u64;
    }
    let alphabet = b"0123456789abcdefghijklmnopqrstuvwxyz";

    let mut chars = Vec::new();
    let mut n = num;
    while n > 0 {
        let rem = (n % 36) as usize;
        chars.push(alphabet[rem] as char);
        n /= 36;
    }
    chars.reverse();

    let mut s: String = chars.into_iter().collect();
    if s.len() < len {
        s = "0".repeat(len - s.len()) + &s;
    }
    if s.len() > len {
        s = s[s.len() - len..].to_string();
    }
    s
}

/// Check if adding a dependency from `from` to `to` would create a cycle.
///
/// Only checks for cycles when the dependency kind requires DAG enforcement
/// (i.e., `Blocks` and `Parent`). `Related` and `DiscoveredFrom` are
/// informational links that can form cycles.
///
/// Returns true if there's already a path from `to` back to `from` via
/// DAG-enforced edges.
fn would_create_cycle(
    state: &crate::core::CanonicalState,
    from: &BeadId,
    to: &BeadId,
    kind: DepKind,
) -> bool {
    use std::collections::HashSet;

    // Only DAG-enforced kinds need cycle checking
    if !kind.requires_dag() {
        return false;
    }

    // BFS from `to` following DAG-enforced deps to see if we can reach `from`
    let mut visited = HashSet::new();
    let mut queue = vec![to.clone()];

    while let Some(current) = queue.pop() {
        if &current == from {
            return true; // Found a path from `to` to `from`
        }
        if visited.contains(&current) {
            continue;
        }
        visited.insert(current.clone());

        // Follow outgoing DAG-enforced deps only
        for edge in state.deps_from(&current) {
            if edge.key.kind().requires_dag() && !visited.contains(edge.key.to()) {
                queue.push(edge.key.to().clone());
            }
        }
    }

    false
}
