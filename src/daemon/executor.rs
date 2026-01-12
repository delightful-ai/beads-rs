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
use uuid::Uuid;

use super::core::{Daemon, LoadedStore};
use super::git_worker::GitOp;
use super::ipc::{OpResponse, Response, ResponsePayload};
use super::ops::{BeadPatch, MapLiveError, OpError, OpResult};
use crate::core::{
    Bead, BeadCore, BeadFields, BeadId, BeadSlug, BeadType, Claim, Closure, DepEdge, DepKey,
    DepKind, DepLife, DepSpec, DurabilityReceipt, Label, Labels, Limits, Lww, Note, NoteId,
    NoteLog, Priority, StoreIdentity, Tombstone, TxnId, WallClock, Workflow, WriteStamp,
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
        _git_tx: &Sender<GitOp>,
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

        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        let actor = self.actor().clone();
        let remote_url = remote.remote().clone();
        let root_slug = match self.repo_state(&remote) {
            Ok(repo_state) => repo_state.root_slug.clone(),
            Err(e) => return Response::err(e),
        };

        // Parse dependency specs first (for validation + source_repo inheritance).
        let parsed_deps = match DepSpec::parse_list(&dependencies) {
            Ok(v) => v,
            Err(e) => {
                return Response::err(OpError::ValidationFailed {
                    field: "deps".into(),
                    reason: e.to_string(),
                });
            }
        };

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
        if let Err(err) = enforce_label_limit(&label_set, self.limits(), None) {
            return Response::err(err);
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

        let desc_str = description.unwrap_or_default();
        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            // Determine ID.
            let (id, parent_id) = match (requested_id, parent) {
                (Some(raw), None) => {
                    let id = BeadId::parse(&raw).map_err(|e| OpError::ValidationFailed {
                        field: "id".into(),
                        reason: e.to_string(),
                    })?;
                    if state.get_live(&id).is_some() || state.get_tombstone(&id).is_some() {
                        return Err(OpError::AlreadyExists(id));
                    }
                    (id, None)
                }
                (None, Some(raw)) => {
                    let parent_id = BeadId::parse(&raw).map_err(|e| OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: e.to_string(),
                    })?;
                    let child = next_child_id(state, &parent_id)?;
                    (child, Some(parent_id))
                }
                (None, None) => (
                    generate_unique_id(
                        state,
                        root_slug.as_deref(),
                        &title,
                        &desc_str,
                        &actor,
                        &stamp.at,
                        &remote_url,
                    )?,
                    None,
                ),
                (Some(_), Some(_)) => unreachable!("guarded above"),
            };

            // Validate dep targets exist (parity with beads-go daemon).
            for spec in &parsed_deps {
                if state.get_live(spec.id()).is_none() {
                    return Err(OpError::NotFound(spec.id().clone()));
                }
            }

            // Inherit source_repo from discovered-from parent if present.
            let mut source_repo_value: Option<String> = None;
            if let Some(spec) = parsed_deps
                .iter()
                .find(|spec| matches!(spec.kind(), DepKind::DiscoveredFrom))
                && let Some(parent_bead) = state.get_live(spec.id())
                && let Some(sr) = &parent_bead.fields.source_repo.value
                && !sr.trim().is_empty()
            {
                source_repo_value = Some(sr.clone());
            }

            // Create claim at creation time if assignee specified (only current actor).
            let claim_value = match assignee.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
                None => Claim::Unclaimed,
                Some("me") | Some("self") => {
                    let expires = WallClock(stamp.at.wall_ms + 3600 * 1000);
                    Claim::claimed(actor.clone(), Some(expires))
                }
                Some(raw) => {
                    if raw != actor.as_str() {
                        return Err(OpError::ValidationFailed {
                            field: "assignee".into(),
                            reason: "cannot assign other actors; run bd as that actor".into(),
                        });
                    }
                    let expires = WallClock(stamp.at.wall_ms + 3600 * 1000);
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
            state.insert_live(bead);

            // Parent edge (from child -> parent).
            if let Some(parent_id) = parent_id {
                if state.get_live(&parent_id).is_none() {
                    return Err(OpError::NotFound(parent_id));
                }
                let key = DepKey::new(id.clone(), parent_id, DepKind::Parent).map_err(|e| {
                    OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: e.reason,
                    }
                })?;
                state.insert_dep(key, DepEdge::new(stamp.clone()));
            }

            // Dependency edges.
            for spec in parsed_deps {
                let key = DepKey::new(id.clone(), spec.id().clone(), spec.kind()).map_err(|e| {
                    OpError::ValidationFailed {
                        field: "dependency".into(),
                        reason: e.reason,
                    }
                })?;
                state.insert_dep(key, DepEdge::new(stamp.clone()));
            }

            Ok(id)
        });

        match result {
            Ok(id) => self.op_response(&remote, OpResult::Created { id }),
            Err(e) => Response::err(e),
        }
    }

    /// Update an existing bead.
    pub fn apply_update(
        &mut self,
        repo: &Path,
        id: &BeadId,
        patch: BeadPatch,
        cas: Option<String>,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        // Validate patch
        if let Err(e) = patch.validate() {
            return Response::err(e);
        }

        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead existence and CAS (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
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

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state.require_live_mut(id).map_live_err(id)?;

            // Apply patch
            patch.apply_to_fields(&mut bead.fields, &stamp)?;

            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Updated { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Add labels to a bead.
    pub fn apply_add_labels(
        &mut self,
        repo: &Path,
        id: &BeadId,
        labels: Vec<String>,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let parsed = match parse_label_list(labels) {
            Ok(v) => v,
            Err(e) => return Response::err(e),
        };

        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        let limits = self.limits().clone();

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state.require_live_mut(id).map_live_err(id)?;
            let mut labels = bead.fields.labels.value.clone();
            for label in parsed {
                labels.insert(label);
            }
            enforce_label_limit(&labels, &limits, Some(id.clone()))?;
            bead.fields.labels = Lww::new(labels, stamp.clone());
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Updated { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Remove labels from a bead.
    pub fn apply_remove_labels(
        &mut self,
        repo: &Path,
        id: &BeadId,
        labels: Vec<String>,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let parsed = match parse_label_list(labels) {
            Ok(v) => v,
            Err(e) => return Response::err(e),
        };

        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state.require_live_mut(id).map_live_err(id)?;
            let mut labels = bead.fields.labels.value.clone();
            for label in parsed {
                labels.remove(label.as_str());
            }
            bead.fields.labels = Lww::new(labels, stamp.clone());
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Updated { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Replace the parent relationship for a bead.
    pub fn apply_set_parent(
        &mut self,
        repo: &Path,
        id: &BeadId,
        parent: Option<BeadId>,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        let existing_parents = {
            let repo_state = match self.repo_state(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };

            if repo_state.state.get_live(id).is_none() {
                return Response::err(OpError::NotFound(id.clone()));
            }

            let existing: Vec<BeadId> = repo_state
                .state
                .deps_from(id)
                .into_iter()
                .filter(|(key, _)| key.kind() == DepKind::Parent)
                .map(|(key, _)| key.to().clone())
                .collect();

            match &parent {
                Some(desired) => {
                    if repo_state.state.get_live(desired).is_none() {
                        return Response::err(OpError::NotFound(desired.clone()));
                    }
                    if existing.len() == 1 && existing[0] == *desired {
                        return self
                            .op_response(&remote, OpResult::Updated { id: id.clone() });
                    }
                    if would_create_cycle(&repo_state.state, id, desired, DepKind::Parent) {
                        return Response::err(OpError::ValidationFailed {
                            field: "parent".into(),
                            reason: format!(
                                "circular dependency: {} already depends on {} (directly or transitively)",
                                desired, id
                            ),
                        });
                    }
                }
                None => {
                    if existing.is_empty() {
                        return self
                            .op_response(&remote, OpResult::Updated { id: id.clone() });
                    }
                }
            }

            existing
        };

        let new_parent = parent.clone();
        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            for existing_parent in existing_parents {
                let key =
                    DepKey::new(id.clone(), existing_parent, DepKind::Parent).map_err(|e| {
                        OpError::ValidationFailed {
                            field: "parent".into(),
                            reason: e.reason,
                        }
                    })?;
                let life = Lww::new(DepLife::Deleted, stamp.clone());
                let edge = DepEdge::with_life(stamp.clone(), life);
                state.insert_dep(key, edge);
            }

            if let Some(parent_id) = new_parent {
                let key = DepKey::new(id.clone(), parent_id, DepKind::Parent).map_err(|e| {
                    OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: e.reason,
                    }
                })?;
                state.insert_dep(key, DepEdge::new(stamp.clone()));
            }

            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Updated { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Close a bead.
    pub fn apply_close(
        &mut self,
        repo: &Path,
        id: &BeadId,
        reason: Option<String>,
        on_branch: Option<String>,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead state (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
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

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state.require_live_mut(id).map_live_err(id)?;
            let closure = Closure::new(reason, on_branch);
            bead.fields.workflow = Lww::new(Workflow::Closed(closure), stamp);
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Closed { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Reopen a closed bead.
    pub fn apply_reopen(&mut self, repo: &Path, id: &BeadId, _git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead state (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
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

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state.require_live_mut(id).map_live_err(id)?;
            bead.fields.workflow = Lww::new(Workflow::Open, stamp);
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Reopened { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Delete a bead (soft delete via tombstone).
    pub fn apply_delete(
        &mut self,
        repo: &Path,
        id: &BeadId,
        reason: Option<String>,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check bead existence (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
            if let Err(e) = repo_state.state.require_live(id).map_live_err(id) {
                return Response::err(e);
            }
        }

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let tombstone = Tombstone::new(id.clone(), stamp, reason);
            state.remove_live(id);
            state.insert_tombstone(tombstone);
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Deleted { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Add a dependency.
    pub fn apply_add_dep(
        &mut self,
        repo: &Path,
        from: &BeadId,
        to: &BeadId,
        kind: DepKind,
        _git_tx: &Sender<GitOp>,
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

        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Check both beads exist and detect cycles (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
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

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let edge = DepEdge::new(stamp);
            state.insert_dep(key, edge);
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(
                &remote,
                OpResult::DepAdded {
                    from: from.clone(),
                    to: to.clone(),
                },
            ),
            Err(e) => Response::err(e),
        }
    }

    /// Remove a dependency (soft delete).
    pub fn apply_remove_dep(
        &mut self,
        repo: &Path,
        from: &BeadId,
        to: &BeadId,
        kind: DepKind,
        _git_tx: &Sender<GitOp>,
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

        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let life = Lww::new(DepLife::Deleted, stamp.clone());
            let edge = DepEdge::with_life(stamp, life);
            state.insert_dep(key, edge);
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(
                &remote,
                OpResult::DepRemoved {
                    from: from.clone(),
                    to: to.clone(),
                },
            ),
            Err(e) => Response::err(e),
        }
    }

    /// Add a note to a bead.
    pub fn apply_add_note(
        &mut self,
        repo: &Path,
        id: &BeadId,
        content: String,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };
        if let Err(err) = enforce_note_limit(&content, self.limits()) {
            return Response::err(err);
        }

        // Check bead existence (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
            if let Err(e) = repo_state.state.require_live(id).map_live_err(id) {
                return Response::err(e);
            }
        }

        let actor = self.actor().clone();
        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state.require_live_mut(id).map_live_err(id)?;
            let note_id = generate_unique_note_id(&bead.notes, NoteId::generate);
            let note = Note::new(note_id.clone(), content, actor, stamp.at);
            bead.notes.insert(note);
            Ok(note_id)
        });

        match result {
            Ok(note_id) => self.op_response(
                &remote,
                OpResult::NoteAdded {
                    bead_id: id.clone(),
                    note_id: note_id.as_str().to_string(),
                },
            ),
            Err(e) => Response::err(e),
        }
    }

    /// Claim a bead.
    pub fn apply_claim(
        &mut self,
        repo: &Path,
        id: &BeadId,
        lease_secs: u64,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Get actor and current time for pre-check
        let actor = self.actor().clone();
        let now_ms = self.clock().wall_ms();

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state
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
            Ok(expires) => self.op_response(
                &remote,
                OpResult::Claimed {
                    id: id.clone(),
                    expires,
                },
            ),
            Err(e) => Response::err(e),
        }
    }

    /// Release a claim.
    pub fn apply_unclaim(&mut self, repo: &Path, id: &BeadId, _git_tx: &Sender<GitOp>) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Get actor for checks
        let actor = self.actor().clone();

        // Check bead state and claim ownership (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
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

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let bead = state.require_live_mut(id).map_live_err(id)?;
            bead.fields.claim = Lww::new(Claim::Unclaimed, stamp.clone());
            // Unclaiming transitions back to open
            bead.fields.workflow = Lww::new(Workflow::Open, stamp);
            Ok(())
        });

        match result {
            Ok(()) => self.op_response(&remote, OpResult::Unclaimed { id: id.clone() }),
            Err(e) => Response::err(e),
        }
    }

    /// Extend an existing claim.
    pub fn apply_extend_claim(
        &mut self,
        repo: &Path,
        id: &BeadId,
        lease_secs: u64,
        _git_tx: &Sender<GitOp>,
    ) -> Response {
        let remote = match self.ensure_repo_loaded_strict(repo, _git_tx) {
            Ok(r) => r,
            Err(e) => return Response::err(e),
        };

        // Get actor for checks
        let actor = self.actor().clone();

        // Check bead state and claim ownership (scope to drop borrow)
        {
            let repo_state = match self.repo_state_mut(&remote) {
                Ok(repo_state) => repo_state,
                Err(e) => return Response::err(e),
            };
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

        let result = self.apply_wal_mutation(&remote, |state, stamp| {
            let expires = WallClock(stamp.at.wall_ms + lease_secs * 1000);
            let bead = state.require_live_mut(id).map_live_err(id)?;
            bead.fields.claim = Lww::new(Claim::claimed(actor.clone(), Some(expires)), stamp);
            Ok(expires)
        });

        match result {
            Ok(expires) => self.op_response(
                &remote,
                OpResult::ClaimExtended {
                    id: id.clone(),
                    expires,
                },
            ),
            Err(e) => Response::err(e),
        }
    }

    fn op_response(&self, proof: &LoadedStore, result: OpResult) -> Response {
        match self.build_receipt(proof) {
            Ok(receipt) => Response::ok(ResponsePayload::Op(OpResponse::new(result, receipt))),
            Err(e) => Response::err(e),
        }
    }

    fn build_receipt(&self, proof: &LoadedStore) -> Result<DurabilityReceipt, OpError> {
        let store = self.store_identity(proof)?;
        let stamp = self
            .repo_state(proof)?
            .last_seen_stamp
            .clone()
            .unwrap_or_else(|| WriteStamp::new(self.clock().wall_ms(), 0));
        let txn_id = txn_id_for_stamp(&store, &stamp);
        Ok(DurabilityReceipt::local_fsync_defaults(
            store,
            txn_id,
            Vec::new(),
            stamp.wall_ms,
        ))
    }
}

fn parse_label_list(labels: Vec<String>) -> Result<Vec<Label>, OpError> {
    let mut parsed = Vec::new();
    for raw in labels {
        let label = Label::parse(raw).map_err(|e| OpError::ValidationFailed {
            field: "labels".into(),
            reason: e.to_string(),
        })?;
        parsed.push(label);
    }
    Ok(parsed)
}

fn enforce_label_limit(
    labels: &Labels,
    limits: &Limits,
    bead_id: Option<BeadId>,
) -> Result<(), OpError> {
    if labels.len() > limits.max_labels_per_bead {
        return Err(OpError::LabelsTooMany {
            max_labels: limits.max_labels_per_bead,
            got_labels: labels.len(),
            bead_id,
        });
    }
    Ok(())
}

fn enforce_note_limit(content: &str, limits: &Limits) -> Result<(), OpError> {
    let got_bytes = content.len();
    if got_bytes > limits.max_note_bytes {
        return Err(OpError::NoteTooLarge {
            max_bytes: limits.max_note_bytes,
            got_bytes,
        });
    }
    Ok(())
}

fn txn_id_for_stamp(store: &StoreIdentity, stamp: &WriteStamp) -> TxnId {
    let namespace = store.store_id.as_uuid();
    let name = format!("{}:{}", stamp.wall_ms, stamp.counter);
    TxnId::new(Uuid::new_v5(&namespace, name.as_bytes()))
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
) -> Result<BeadId, OpError> {
    let slug = match root_slug {
        Some(raw) => BeadSlug::parse(raw).map_err(|e| OpError::ValidationFailed {
            field: "root_slug".into(),
            reason: e.to_string(),
        })?,
        None => infer_bead_slug(state)?,
    };
    let num_top_level = state
        .iter_live()
        .filter(|(id, _)| id.is_top_level())
        .count();

    let base_len = compute_adaptive_length(num_top_level);

    for len in base_len..=8 {
        for nonce in 0..10 {
            let short = generate_hash_suffix(title, description, actor, stamp, remote, len, nonce);
            let candidate = BeadId::parse(&format!("{}-{}", slug.as_str(), short))
                .map_err(|_| OpError::Internal("generated id must be valid"))?;
            if !state.contains(&candidate) {
                return Ok(candidate);
            }
        }
    }

    // Extremely unlikely fallback.
    Ok(BeadId::generate_with_slug(&slug, 8))
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
fn infer_bead_slug(state: &crate::core::CanonicalState) -> Result<BeadSlug, OpError> {
    use std::collections::BTreeMap;

    let mut counts: BTreeMap<BeadSlug, usize> = BTreeMap::new();
    for id in state
        .iter_live()
        .map(|(id, _)| id)
        .chain(state.iter_tombstones().map(|(k, _)| &k.id))
    {
        *counts.entry(id.slug_value()).or_default() += 1;
    }
    let mut best_slug: Option<BeadSlug> = None;
    let mut best_count: usize = 0;
    for (slug, count) in counts {
        if count > best_count {
            best_slug = Some(slug);
            best_count = count;
        }
    }
    if let Some(slug) = best_slug {
        Ok(slug)
    } else {
        BeadSlug::parse("bd").map_err(|_| OpError::Internal("default slug invalid"))
    }
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
        for (key, _) in state.deps_from(&current) {
            if key.kind().requires_dag() && !visited.contains(key.to()) {
                queue.push(key.to().clone());
            }
        }
    }

    false
}

fn generate_unique_note_id<F>(notes: &NoteLog, mut next_id: F) -> NoteId
where
    F: FnMut() -> NoteId,
{
    let mut note_id = next_id();
    while notes.contains(&note_id) {
        note_id = next_id();
    }
    note_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, BeadCore, BeadFields, BeadType, Claim, Lww, Note, NoteLog, Priority, Stamp,
        Workflow, WriteStamp,
    };

    fn make_bead(id: &str, wall_ms: u64, actor: &ActorId) -> Bead {
        let stamp = Stamp::new(WriteStamp::new(wall_ms, 0), actor.clone());
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::new(2).unwrap(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Default::default(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    #[test]
    fn generate_unique_id_rejects_invalid_root_slug() {
        let state = crate::core::CanonicalState::new();
        let actor = ActorId::new("tester").unwrap();
        let stamp = WriteStamp::new(1000, 0);
        let remote = crate::daemon::RemoteUrl("example.com/test".to_string());

        let err = generate_unique_id(
            &state,
            Some("bad slug"),
            "title",
            "",
            &actor,
            &stamp,
            &remote,
        )
        .unwrap_err();
        match err {
            OpError::ValidationFailed { field, .. } => assert_eq!(field, "root_slug"),
            other => panic!("expected validation error, got {other:?}"),
        }
    }

    #[test]
    fn infer_bead_slug_prefers_most_common() {
        let actor = ActorId::new("tester").unwrap();
        let mut state = crate::core::CanonicalState::new();
        state.insert(make_bead("foo-abc", 1000, &actor)).unwrap();
        state.insert(make_bead("foo-def", 1100, &actor)).unwrap();
        state.insert(make_bead("bar-xyz", 1200, &actor)).unwrap();

        let slug = infer_bead_slug(&state).unwrap();
        assert_eq!(slug.as_str(), "foo");
    }

    #[test]
    fn generate_unique_id_uses_root_slug() {
        let state = crate::core::CanonicalState::new();
        let actor = ActorId::new("tester").unwrap();
        let stamp = WriteStamp::new(1000, 0);
        let remote = crate::daemon::RemoteUrl("example.com/test".to_string());

        let id = generate_unique_id(&state, Some("myrepo"), "title", "", &actor, &stamp, &remote)
            .unwrap();
        assert!(id.as_str().starts_with("myrepo-"));
    }

    #[test]
    fn generate_unique_note_id_retries_on_collision() {
        let actor = ActorId::new("tester").unwrap();
        let stamp = WriteStamp::new(1000, 0);
        let note_id = NoteId::new("dup").unwrap();
        let note = Note::new(note_id.clone(), "hi".to_string(), actor, stamp);

        let mut notes = NoteLog::new();
        notes.insert(note);

        let ids = vec!["dup", "dup", "uniq"];
        let mut idx = 0usize;
        let generated = generate_unique_note_id(&notes, || {
            let id = ids[idx];
            idx += 1;
            NoteId::new(id).unwrap()
        });

        assert_eq!(generated.as_str(), "uniq");
    }

    #[test]
    fn enforce_label_limit_rejects_overage() {
        let limits = Limits {
            max_labels_per_bead: 1,
            ..Limits::default()
        };
        let mut labels = Labels::new();
        labels.insert(Label::parse("alpha").unwrap());
        labels.insert(Label::parse("beta").unwrap());

        let bead_id = BeadId::parse("test-1").unwrap();
        let err = enforce_label_limit(&labels, &limits, Some(bead_id.clone())).unwrap_err();
        match err {
            OpError::LabelsTooMany {
                max_labels,
                got_labels,
                bead_id: Some(id),
            } => {
                assert_eq!(max_labels, 1);
                assert_eq!(got_labels, 2);
                assert_eq!(id, bead_id);
            }
            other => panic!("expected LabelsTooMany, got {other:?}"),
        }
    }

    #[test]
    fn enforce_note_limit_rejects_overage() {
        let limits = Limits {
            max_note_bytes: 2,
            ..Limits::default()
        };
        let err = enforce_note_limit("hey", &limits).unwrap_err();
        assert!(matches!(
            err,
            OpError::NoteTooLarge {
                max_bytes: 2,
                got_bytes: 3
            }
        ));
    }
}
