//! Mutation planning for realtime event generation.

use std::collections::BTreeMap;

use uuid::Uuid;

use super::clock::Clock;
use super::ops::{BeadPatch, MapLiveError, OpError, Patch};
use super::remote::RemoteUrl;
use crate::core::{
    ActorId, BeadId, BeadSlug, BeadType, CanonicalState, ClientRequestId, CoreError, DepKey,
    DepKind, DepSpec, EventBody, EventBytes, EventKindV1, HlcMax, Label, Labels, Limits,
    NamespaceId, NoteAppendV1, NoteId, NoteLog, Priority, ReplicaId, Seq1, Stamp, StoreIdentity,
    TxnDeltaError, TxnDeltaV1, TxnId, TxnOpV1, TxnV1, WallClock, WireBeadPatch, WireDepDeleteV1,
    WireDepV1, WireNoteV1, WirePatch, WireStamp, WireTombstoneV1, WorkflowStatus,
    encode_event_body_canonical, sha256_bytes, to_canon_json_bytes,
};
use crate::daemon::wal::record::RECORD_HEADER_BASE_LEN;

#[derive(Clone, Debug)]
pub struct MutationContext {
    pub namespace: NamespaceId,
    pub actor_id: ActorId,
    pub client_request_id: Option<ClientRequestId>,
}

#[derive(Clone, Debug)]
pub struct IdContext {
    pub root_slug: Option<String>,
    pub remote_url: RemoteUrl,
}

#[derive(Clone, Debug)]
pub struct EventDraft {
    pub delta: TxnDeltaV1,
    pub hlc_max: HlcMax,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    pub request_sha256: [u8; 32],
    pub client_request_id: Option<ClientRequestId>,
}

#[derive(Clone, Debug)]
pub struct SequencedEvent {
    pub event_body: EventBody,
    pub event_bytes: EventBytes<crate::core::Canonical>,
    pub request_sha256: [u8; 32],
    pub client_request_id: Option<ClientRequestId>,
}

#[derive(Clone, Debug)]
pub enum MutationRequest {
    Create {
        id: Option<String>,
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
    },
    Update {
        id: String,
        patch: BeadPatch,
        cas: Option<String>,
    },
    AddLabels {
        id: String,
        labels: Vec<String>,
    },
    RemoveLabels {
        id: String,
        labels: Vec<String>,
    },
    SetParent {
        id: String,
        parent: Option<String>,
    },
    Close {
        id: String,
        reason: Option<String>,
        on_branch: Option<String>,
    },
    Reopen {
        id: String,
    },
    Delete {
        id: String,
        reason: Option<String>,
    },
    AddDep {
        from: String,
        to: String,
        kind: DepKind,
    },
    RemoveDep {
        from: String,
        to: String,
        kind: DepKind,
    },
    AddNote {
        id: String,
        content: String,
    },
    Claim {
        id: String,
        lease_secs: u64,
    },
    Unclaim {
        id: String,
    },
    ExtendClaim {
        id: String,
        lease_secs: u64,
    },
}

#[derive(Clone, Debug)]
pub struct ParsedBeadPatch {
    pub title: Patch<String>,
    pub description: Patch<String>,
    pub design: Patch<String>,
    pub acceptance_criteria: Patch<String>,
    pub priority: Patch<Priority>,
    pub bead_type: Patch<BeadType>,
    pub labels: Patch<Labels>,
    pub external_ref: Patch<String>,
    pub source_repo: Patch<String>,
    pub estimated_minutes: Patch<u32>,
    pub status: Patch<WorkflowStatus>,
}

impl ParsedBeadPatch {
    fn parse(raw: BeadPatch) -> Result<Self, OpError> {
        let BeadPatch {
            title,
            description,
            design,
            acceptance_criteria,
            priority,
            bead_type,
            labels,
            external_ref,
            source_repo,
            estimated_minutes,
            status,
        } = raw;

        if matches!(title, Patch::Clear) {
            return Err(OpError::ValidationFailed {
                field: "title".into(),
                reason: "cannot clear required field".into(),
            });
        }
        if matches!(description, Patch::Clear) {
            return Err(OpError::ValidationFailed {
                field: "description".into(),
                reason: "cannot clear required field".into(),
            });
        }

        let labels = match labels {
            Patch::Set(values) => Patch::Set(parse_labels(values)?),
            Patch::Clear => Patch::Clear,
            Patch::Keep => Patch::Keep,
        };

        Ok(Self {
            title,
            description,
            design,
            acceptance_criteria,
            priority,
            bead_type,
            labels,
            external_ref,
            source_repo,
            estimated_minutes,
            status,
        })
    }
}

#[derive(Clone, Debug)]
pub enum ParsedMutationRequest {
    Create {
        id: Option<BeadId>,
        parent: Option<BeadId>,
        title: String,
        bead_type: BeadType,
        priority: Priority,
        description: Option<String>,
        design: Option<String>,
        acceptance_criteria: Option<String>,
        assignee: Option<ActorId>,
        external_ref: Option<String>,
        estimated_minutes: Option<u32>,
        labels: Labels,
        dependencies: Vec<DepSpec>,
    },
    Update {
        id: BeadId,
        patch: ParsedBeadPatch,
        cas: Option<String>,
    },
    AddLabels {
        id: BeadId,
        labels: Labels,
    },
    RemoveLabels {
        id: BeadId,
        labels: Labels,
    },
    SetParent {
        id: BeadId,
        parent: Option<BeadId>,
    },
    Close {
        id: BeadId,
        reason: Option<String>,
        on_branch: Option<String>,
    },
    Reopen {
        id: BeadId,
    },
    Delete {
        id: BeadId,
        reason: Option<String>,
    },
    AddDep {
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    },
    RemoveDep {
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    },
    AddNote {
        id: BeadId,
        content: String,
    },
    Claim {
        id: BeadId,
        lease_secs: u64,
    },
    Unclaim {
        id: BeadId,
    },
    ExtendClaim {
        id: BeadId,
        lease_secs: u64,
    },
}

impl ParsedMutationRequest {
    pub fn parse(req: MutationRequest, actor: &ActorId) -> Result<Self, OpError> {
        match req {
            MutationRequest::Create {
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
            } => {
                let requested_id = normalize_optional_string(id);
                let parent = normalize_optional_string(parent);
                if requested_id.is_some() && parent.is_some() {
                    return Err(OpError::ValidationFailed {
                        field: "create".into(),
                        reason: "cannot specify both id and parent".into(),
                    });
                }

                let id = match requested_id {
                    Some(raw) => Some(parse_bead_id_validation("id", raw)?),
                    None => None,
                };
                let parent = match parent {
                    Some(raw) => Some(parse_bead_id_validation("parent", raw)?),
                    None => None,
                };

                let labels = parse_labels(labels)?;
                let dependencies = parse_dep_specs(&dependencies)?;
                let assignee = normalize_assignee(assignee, actor)?;
                let design = normalize_optional_string(design);
                let acceptance_criteria = normalize_optional_string(acceptance_criteria);
                let external_ref = normalize_optional_string(external_ref);

                Ok(ParsedMutationRequest::Create {
                    id,
                    parent,
                    title,
                    bead_type,
                    priority,
                    description,
                    design,
                    acceptance_criteria,
                    assignee,
                    external_ref,
                    estimated_minutes,
                    labels,
                    dependencies,
                })
            }
            MutationRequest::Update { id, patch, cas } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                let patch = ParsedBeadPatch::parse(patch)?;
                Ok(ParsedMutationRequest::Update { id, patch, cas })
            }
            MutationRequest::AddLabels { id, labels } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                let labels = parse_labels(labels)?;
                Ok(ParsedMutationRequest::AddLabels { id, labels })
            }
            MutationRequest::RemoveLabels { id, labels } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                let labels = parse_labels(labels)?;
                Ok(ParsedMutationRequest::RemoveLabels { id, labels })
            }
            MutationRequest::SetParent { id, parent } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                let parent = match parent {
                    Some(raw) => Some(parse_bead_id_invalid_id("parent", raw)?),
                    None => None,
                };
                Ok(ParsedMutationRequest::SetParent { id, parent })
            }
            MutationRequest::Close {
                id,
                reason,
                on_branch,
            } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                Ok(ParsedMutationRequest::Close {
                    id,
                    reason,
                    on_branch,
                })
            }
            MutationRequest::Reopen { id } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                Ok(ParsedMutationRequest::Reopen { id })
            }
            MutationRequest::Delete { id, reason } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                Ok(ParsedMutationRequest::Delete { id, reason })
            }
            MutationRequest::AddDep { from, to, kind } => {
                let from = parse_bead_id_invalid_id("from", from)?;
                let to = parse_bead_id_invalid_id("to", to)?;
                Ok(ParsedMutationRequest::AddDep { from, to, kind })
            }
            MutationRequest::RemoveDep { from, to, kind } => {
                let from = parse_bead_id_invalid_id("from", from)?;
                let to = parse_bead_id_invalid_id("to", to)?;
                Ok(ParsedMutationRequest::RemoveDep { from, to, kind })
            }
            MutationRequest::AddNote { id, content } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                Ok(ParsedMutationRequest::AddNote { id, content })
            }
            MutationRequest::Claim { id, lease_secs } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                Ok(ParsedMutationRequest::Claim { id, lease_secs })
            }
            MutationRequest::Unclaim { id } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                Ok(ParsedMutationRequest::Unclaim { id })
            }
            MutationRequest::ExtendClaim { id, lease_secs } => {
                let id = parse_bead_id_invalid_id("id", id)?;
                Ok(ParsedMutationRequest::ExtendClaim { id, lease_secs })
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MutationEngine {
    limits: Limits,
}

impl MutationEngine {
    pub fn new(limits: Limits) -> Self {
        Self { limits }
    }

    pub fn request_sha256_for(
        &self,
        ctx: &MutationContext,
        req: &ParsedMutationRequest,
    ) -> Result<[u8; 32], OpError> {
        let canonical = self.canonicalize_request(req)?;
        request_sha256(ctx, &canonical)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan(
        &self,
        state: &CanonicalState,
        clock: &mut Clock,
        store: StoreIdentity,
        id_ctx: Option<&IdContext>,
        ctx: MutationContext,
        req: ParsedMutationRequest,
    ) -> Result<EventDraft, OpError> {
        let now_ms = clock.wall_ms();
        let write_stamp = clock.tick();
        let stamp = Stamp::new(write_stamp.clone(), ctx.actor_id.clone());

        let planned = match req {
            ParsedMutationRequest::Create {
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
            } => self.plan_create(
                state,
                &stamp,
                id_ctx,
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
            )?,
            ParsedMutationRequest::Update { id, patch, cas } => {
                self.plan_update(state, id, patch, cas)?
            }
            ParsedMutationRequest::AddLabels { id, labels } => {
                self.plan_add_labels(state, id, labels)?
            }
            ParsedMutationRequest::RemoveLabels { id, labels } => {
                self.plan_remove_labels(state, id, labels)?
            }
            ParsedMutationRequest::SetParent { id, parent } => {
                self.plan_set_parent(state, id, parent, &stamp)?
            }
            ParsedMutationRequest::Close {
                id,
                reason,
                on_branch,
            } => self.plan_close(state, id, reason, on_branch)?,
            ParsedMutationRequest::Reopen { id } => self.plan_reopen(state, id)?,
            ParsedMutationRequest::Delete { id, reason } => {
                self.plan_delete(state, &stamp, id, reason)?
            }
            ParsedMutationRequest::AddDep { from, to, kind } => {
                self.plan_add_dep(state, &stamp, from, to, kind)?
            }
            ParsedMutationRequest::RemoveDep { from, to, kind } => {
                self.plan_remove_dep(&stamp, from, to, kind)?
            }
            ParsedMutationRequest::AddNote { id, content } => {
                self.plan_add_note(state, &stamp, id, content)?
            }
            ParsedMutationRequest::Claim { id, lease_secs } => {
                self.plan_claim(state, &stamp, now_ms, id, lease_secs)?
            }
            ParsedMutationRequest::Unclaim { id } => self.plan_unclaim(state, &stamp, id)?,
            ParsedMutationRequest::ExtendClaim { id, lease_secs } => {
                self.plan_extend_claim(state, &stamp, id, lease_secs)?
            }
        };

        self.enforce_delta_limits(&planned.delta)?;

        let request_sha256 = request_sha256(&ctx, &planned.canonical)?;
        let MutationContext {
            actor_id,
            client_request_id,
            ..
        } = ctx;

        Ok(EventDraft {
            delta: planned.delta,
            hlc_max: HlcMax {
                actor_id,
                physical_ms: write_stamp.wall_ms,
                logical: write_stamp.counter,
            },
            event_time_ms: write_stamp.wall_ms,
            txn_id: txn_id_for_stamp(&store, &stamp),
            request_sha256,
            client_request_id,
        })
    }

    pub fn build_event(
        &self,
        draft: EventDraft,
        store: StoreIdentity,
        namespace: NamespaceId,
        origin_replica_id: ReplicaId,
        origin_seq: Seq1,
    ) -> Result<SequencedEvent, OpError> {
        let event_body = EventBody {
            envelope_v: 1,
            store,
            namespace,
            origin_replica_id,
            origin_seq,
            event_time_ms: draft.event_time_ms,
            txn_id: draft.txn_id,
            client_request_id: draft.client_request_id,
            trace_id: None,
            kind: EventKindV1::TxnV1(TxnV1 {
                delta: draft.delta,
                hlc_max: draft.hlc_max,
            }),
        };

        let event_bytes = encode_event_body_canonical(&event_body)
            .map_err(|_| OpError::Internal("event_body encode failed"))?;
        self.enforce_record_size(&event_bytes, draft.client_request_id)?;

        Ok(SequencedEvent {
            event_body,
            event_bytes,
            request_sha256: draft.request_sha256,
            client_request_id: draft.client_request_id,
        })
    }

    fn canonicalize_request(
        &self,
        req: &ParsedMutationRequest,
    ) -> Result<CanonicalMutationOp, OpError> {
        match req {
            ParsedMutationRequest::Create {
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
            } => {
                if id.is_some() && parent.is_some() {
                    return Err(OpError::ValidationFailed {
                        field: "create".into(),
                        reason: "cannot specify both id and parent".into(),
                    });
                }

                let title = title.trim().to_string();
                if title.is_empty() {
                    return Err(OpError::ValidationFailed {
                        field: "title".into(),
                        reason: "title cannot be empty".into(),
                    });
                }

                enforce_label_limit(labels, &self.limits, None)?;
                let canonical_deps = canonical_deps(dependencies);
                let description = description.clone().unwrap_or_default();

                Ok(CanonicalMutationOp::Create {
                    id: id.clone(),
                    parent: parent.clone(),
                    title,
                    bead_type: *bead_type,
                    priority: *priority,
                    description,
                    design: design.clone(),
                    acceptance_criteria: acceptance_criteria.clone(),
                    assignee: assignee.clone(),
                    external_ref: external_ref.clone(),
                    estimated_minutes: *estimated_minutes,
                    labels: canonical_labels(labels),
                    dependencies: canonical_deps,
                })
            }
            ParsedMutationRequest::Update { id, patch, cas } => {
                let (_, canonical_patch) = normalize_patch(id, patch)?;
                Ok(CanonicalMutationOp::Update {
                    id: id.clone(),
                    patch: canonical_patch,
                    cas: cas.clone(),
                })
            }
            ParsedMutationRequest::AddLabels { id, labels } => Ok(CanonicalMutationOp::AddLabels {
                id: id.clone(),
                labels: canonical_labels(labels),
            }),
            ParsedMutationRequest::RemoveLabels { id, labels } => {
                Ok(CanonicalMutationOp::RemoveLabels {
                    id: id.clone(),
                    labels: canonical_labels(labels),
                })
            }
            ParsedMutationRequest::SetParent { id, parent } => Ok(CanonicalMutationOp::SetParent {
                id: id.clone(),
                parent: parent.clone(),
            }),
            ParsedMutationRequest::Close {
                id,
                reason,
                on_branch,
            } => Ok(CanonicalMutationOp::Close {
                id: id.clone(),
                reason: reason.clone(),
                on_branch: on_branch.clone(),
            }),
            ParsedMutationRequest::Reopen { id } => {
                Ok(CanonicalMutationOp::Reopen { id: id.clone() })
            }
            ParsedMutationRequest::Delete { id, reason } => Ok(CanonicalMutationOp::Delete {
                id: id.clone(),
                reason: reason.clone(),
            }),
            ParsedMutationRequest::AddDep { from, to, kind } => Ok(CanonicalMutationOp::AddDep {
                from: from.clone(),
                to: to.clone(),
                kind: *kind,
            }),
            ParsedMutationRequest::RemoveDep { from, to, kind } => {
                Ok(CanonicalMutationOp::RemoveDep {
                    from: from.clone(),
                    to: to.clone(),
                    kind: *kind,
                })
            }
            ParsedMutationRequest::AddNote { id, content } => {
                enforce_note_limit(content, &self.limits)?;
                Ok(CanonicalMutationOp::AddNote {
                    id: id.clone(),
                    content: content.clone(),
                })
            }
            ParsedMutationRequest::Claim { id, lease_secs } => Ok(CanonicalMutationOp::Claim {
                id: id.clone(),
                lease_secs: *lease_secs,
            }),
            ParsedMutationRequest::Unclaim { id } => {
                Ok(CanonicalMutationOp::Unclaim { id: id.clone() })
            }
            ParsedMutationRequest::ExtendClaim { id, lease_secs } => {
                Ok(CanonicalMutationOp::ExtendClaim {
                    id: id.clone(),
                    lease_secs: *lease_secs,
                })
            }
        }
    }

    fn enforce_delta_limits(&self, delta: &TxnDeltaV1) -> Result<(), OpError> {
        let total_ops = delta.total_ops();
        if total_ops > self.limits.max_ops_per_txn {
            return Err(OpError::OpsTooMany {
                max_ops: self.limits.max_ops_per_txn,
                got_ops: total_ops,
            });
        }

        let mut note_appends = 0usize;
        for op in delta.iter() {
            match op {
                TxnOpV1::BeadUpsert(patch) => {
                    if let Some(labels) = &patch.labels {
                        enforce_label_limit(labels, &self.limits, Some(patch.id.clone()))?;
                    }
                    if let crate::core::NotesPatch::AtLeast(notes) = &patch.notes {
                        for note in notes {
                            enforce_note_limit(&note.content, &self.limits)?;
                        }
                    }
                }
                TxnOpV1::BeadDelete(_) => {}
                TxnOpV1::DepUpsert(_) => {}
                TxnOpV1::DepDelete(_) => {}
                TxnOpV1::NoteAppend(append) => {
                    note_appends += 1;
                    enforce_note_limit(&append.note.content, &self.limits)?;
                }
            }
        }

        if note_appends > self.limits.max_note_appends_per_txn {
            return Err(OpError::InvalidRequest {
                field: Some("note_appends".into()),
                reason: format!(
                    "note_appends {note_appends} exceeds max {}",
                    self.limits.max_note_appends_per_txn
                ),
            });
        }

        Ok(())
    }

    fn enforce_record_size(
        &self,
        event_bytes: &EventBytes<crate::core::Canonical>,
        client_request_id: Option<ClientRequestId>,
    ) -> Result<(), OpError> {
        let max_payload = self
            .limits
            .max_wal_record_bytes
            .min(self.limits.max_frame_bytes);
        if event_bytes.len() > max_payload {
            return Err(OpError::WalRecordTooLarge {
                max_wal_record_bytes: max_payload,
                estimated_bytes: event_bytes.len(),
            });
        }

        let estimated = estimated_record_bytes(event_bytes.len(), client_request_id.is_some());
        if estimated > self.limits.max_wal_record_bytes {
            return Err(OpError::WalRecordTooLarge {
                max_wal_record_bytes: self.limits.max_wal_record_bytes,
                estimated_bytes: estimated,
            });
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_create(
        &self,
        state: &CanonicalState,
        stamp: &Stamp,
        id_ctx: Option<&IdContext>,
        id: Option<BeadId>,
        parent: Option<BeadId>,
        title: String,
        bead_type: BeadType,
        priority: Priority,
        description: Option<String>,
        design: Option<String>,
        acceptance_criteria: Option<String>,
        assignee: Option<ActorId>,
        external_ref: Option<String>,
        estimated_minutes: Option<u32>,
        labels: Labels,
        dependencies: Vec<DepSpec>,
    ) -> Result<PlannedDelta, OpError> {
        if id.is_some() && parent.is_some() {
            return Err(OpError::ValidationFailed {
                field: "create".into(),
                reason: "cannot specify both id and parent".into(),
            });
        }

        let requested_id = id.clone();
        let requested_parent = parent.clone();

        let title = title.trim().to_string();
        if title.is_empty() {
            return Err(OpError::ValidationFailed {
                field: "title".into(),
                reason: "title cannot be empty".into(),
            });
        }

        enforce_label_limit(&labels, &self.limits, None)?;

        let mut parsed_deps = dependencies;
        sort_dedup_dep_specs(&mut parsed_deps);
        let canonical_deps = canonical_deps(&parsed_deps);

        let description = description.unwrap_or_default();

        let id_ctx = id_ctx.ok_or_else(|| OpError::InvalidRequest {
            field: Some("id".into()),
            reason: "id context missing for create".into(),
        })?;

        let (id, parent_id) = match (requested_id.as_ref(), requested_parent.as_ref()) {
            (Some(requested_id), None) => {
                if state.get_live(requested_id).is_some()
                    || state.get_tombstone(requested_id).is_some()
                {
                    return Err(OpError::AlreadyExists(requested_id.clone()));
                }
                (requested_id.clone(), None)
            }
            (None, Some(parent_id)) => {
                let child = next_child_id(state, parent_id)?;
                (child, Some(parent_id.clone()))
            }
            (None, None) => (
                generate_unique_id(
                    state,
                    id_ctx.root_slug.as_deref(),
                    &title,
                    &description,
                    &stamp.by,
                    &stamp.at,
                    &id_ctx.remote_url,
                )?,
                None,
            ),
            (Some(_), Some(_)) => unreachable!("guarded above"),
        };

        for spec in &parsed_deps {
            if state.get_live(spec.id()).is_none() {
                return Err(OpError::NotFound(spec.id().clone()));
            }
            DepKey::new(id.clone(), spec.id().clone(), spec.kind()).map_err(|e| {
                OpError::ValidationFailed {
                    field: "dependency".into(),
                    reason: e.reason,
                }
            })?;
        }

        let mut source_repo_value = None;
        if let Some(spec) = parsed_deps
            .iter()
            .find(|spec| matches!(spec.kind(), DepKind::DiscoveredFrom))
            && let Some(parent_bead) = state.get_live(spec.id())
            && let Some(sr) = &parent_bead.fields.source_repo.value
            && !sr.trim().is_empty()
        {
            source_repo_value = Some(sr.clone());
        }

        let mut patch = WireBeadPatch::new(id.clone());
        patch.title = Some(title.clone());
        patch.description = Some(description.clone());
        if let Some(design) = design.clone() {
            patch.design = WirePatch::Set(design);
        }
        if let Some(acceptance_criteria) = acceptance_criteria.clone() {
            patch.acceptance_criteria = WirePatch::Set(acceptance_criteria);
        }
        patch.priority = Some(priority);
        patch.bead_type = Some(bead_type);
        if !labels.is_empty() {
            patch.labels = Some(labels.clone());
        }
        if let Some(external_ref) = external_ref.clone() {
            patch.external_ref = WirePatch::Set(external_ref);
        }
        if let Some(source_repo) = source_repo_value.clone() {
            patch.source_repo = WirePatch::Set(source_repo);
        }
        if let Some(estimated_minutes) = estimated_minutes {
            patch.estimated_minutes = WirePatch::Set(estimated_minutes);
        }
        if let Some(assignee) = assignee.clone() {
            let expires = WallClock(stamp.at.wall_ms.saturating_add(3600 * 1000));
            patch.assignee = WirePatch::Set(assignee);
            patch.assignee_expires = WirePatch::Set(expires);
        }

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        if let Some(parent_id) = parent_id {
            delta
                .insert(TxnOpV1::DepUpsert(WireDepV1 {
                    from: id.clone(),
                    to: parent_id,
                    kind: DepKind::Parent,
                    created_at: WireStamp::from(&stamp.at),
                    created_by: stamp.by.clone(),
                    deleted_at: None,
                    deleted_by: None,
                }))
                .map_err(delta_error_to_op)?;
        }

        for spec in parsed_deps {
            delta
                .insert(TxnOpV1::DepUpsert(WireDepV1 {
                    from: id.clone(),
                    to: spec.id().clone(),
                    kind: spec.kind(),
                    created_at: WireStamp::from(&stamp.at),
                    created_by: stamp.by.clone(),
                    deleted_at: None,
                    deleted_by: None,
                }))
                .map_err(delta_error_to_op)?;
        }

        let canonical = CanonicalMutationOp::Create {
            id: requested_id,
            parent: requested_parent,
            title,
            bead_type,
            priority,
            description,
            design,
            acceptance_criteria,
            assignee,
            external_ref,
            estimated_minutes,
            labels: canonical_labels(&labels),
            dependencies: canonical_deps,
        };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_update(
        &self,
        state: &CanonicalState,
        id: BeadId,
        patch: ParsedBeadPatch,
        cas: Option<String>,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;

        if let Some(expected) = cas.as_ref() {
            let actual = bead.content_hash().to_hex();
            if expected != &actual {
                return Err(OpError::CasMismatch {
                    expected: expected.clone(),
                    actual,
                });
            }
        }

        let (wire_patch, canonical_patch) = normalize_patch(&id, &patch)?;

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(wire_patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::Update {
            id,
            patch: canonical_patch,
            cas,
        };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_add_labels(
        &self,
        state: &CanonicalState,
        id: BeadId,
        labels: Labels,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;
        let mut merged = bead.fields.labels.value.clone();
        for label in labels.iter() {
            merged.insert(label.clone());
        }
        enforce_label_limit(&merged, &self.limits, Some(id.clone()))?;

        let mut patch = WireBeadPatch::new(id.clone());
        patch.labels = Some(merged.clone());

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::AddLabels {
            id,
            labels: canonical_labels(&labels),
        };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_remove_labels(
        &self,
        state: &CanonicalState,
        id: BeadId,
        labels: Labels,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;
        let mut merged = bead.fields.labels.value.clone();
        for label in labels.iter() {
            merged.remove(label.as_str());
        }

        let mut patch = WireBeadPatch::new(id.clone());
        patch.labels = Some(merged);

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::RemoveLabels {
            id,
            labels: canonical_labels(&labels),
        };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_close(
        &self,
        state: &CanonicalState,
        id: BeadId,
        reason: Option<String>,
        on_branch: Option<String>,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;
        if bead.fields.workflow.value.is_closed() {
            return Err(OpError::InvalidTransition {
                from: "closed".into(),
                to: "closed".into(),
            });
        }

        let mut patch = WireBeadPatch::new(id.clone());
        patch.status = Some(WorkflowStatus::Closed);
        if let Some(reason) = reason.clone() {
            patch.closed_reason = WirePatch::Set(reason);
        }
        if let Some(branch) = on_branch.clone() {
            patch.closed_on_branch = WirePatch::Set(branch);
        }

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::Close {
            id,
            reason,
            on_branch,
        };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_reopen(&self, state: &CanonicalState, id: BeadId) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;
        if !bead.fields.workflow.value.is_closed() {
            let from_status = bead.fields.workflow.value.status().to_string();
            return Err(OpError::InvalidTransition {
                from: from_status,
                to: "open".into(),
            });
        }

        let mut patch = WireBeadPatch::new(id.clone());
        patch.status = Some(WorkflowStatus::Open);

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::Reopen { id };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_delete(
        &self,
        state: &CanonicalState,
        stamp: &Stamp,
        id: BeadId,
        reason: Option<String>,
    ) -> Result<PlannedDelta, OpError> {
        state.require_live(&id).map_live_err(&id)?;

        let reason = normalize_optional_string(reason);
        let tombstone = WireTombstoneV1 {
            id: id.clone(),
            deleted_at: WireStamp::from(&stamp.at),
            deleted_by: stamp.by.clone(),
            reason: reason.clone(),
            lineage_created_at: None,
            lineage_created_by: None,
        };

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadDelete(tombstone))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::Delete { id, reason };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_add_dep(
        &self,
        state: &CanonicalState,
        stamp: &Stamp,
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    ) -> Result<PlannedDelta, OpError> {
        DepKey::new(from.clone(), to.clone(), kind).map_err(|e| OpError::ValidationFailed {
            field: "dependency".into(),
            reason: e.reason,
        })?;

        if state.get_live(&from).is_none() {
            return Err(OpError::NotFound(from));
        }
        if state.get_live(&to).is_none() {
            return Err(OpError::NotFound(to));
        }
        if would_create_cycle(state, &from, &to, kind) {
            return Err(OpError::ValidationFailed {
                field: "dependency".into(),
                reason: format!(
                    "circular dependency: {} already depends on {} (directly or transitively)",
                    to, from
                ),
            });
        }

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepUpsert(WireDepV1 {
                from: from.clone(),
                to: to.clone(),
                kind,
                created_at: WireStamp::from(&stamp.at),
                created_by: stamp.by.clone(),
                deleted_at: None,
                deleted_by: None,
            }))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::AddDep { from, to, kind };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_remove_dep(
        &self,
        stamp: &Stamp,
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    ) -> Result<PlannedDelta, OpError> {
        DepKey::new(from.clone(), to.clone(), kind).map_err(|e| OpError::ValidationFailed {
            field: "dependency".into(),
            reason: e.reason,
        })?;

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepDelete(WireDepDeleteV1 {
                from: from.clone(),
                to: to.clone(),
                kind,
                deleted_at: WireStamp::from(&stamp.at),
                deleted_by: stamp.by.clone(),
            }))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::RemoveDep { from, to, kind };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_set_parent(
        &self,
        state: &CanonicalState,
        id: BeadId,
        parent: Option<BeadId>,
        stamp: &Stamp,
    ) -> Result<PlannedDelta, OpError> {
        state.require_live(&id).map_live_err(&id)?;

        let parent_id = match parent.as_ref() {
            Some(parent_id) => {
                DepKey::new(id.clone(), parent_id.clone(), DepKind::Parent).map_err(|e| {
                    OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: e.reason,
                    }
                })?;
                if state.get_live(parent_id).is_none() {
                    return Err(OpError::NotFound(parent_id.clone()));
                }
                if would_create_cycle(state, &id, parent_id, DepKind::Parent) {
                    return Err(OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: format!(
                            "circular dependency: {} already depends on {} (directly or transitively)",
                            parent_id, id
                        ),
                    });
                }
                Some(parent_id.clone())
            }
            None => None,
        };

        let existing_parents: Vec<BeadId> = state
            .deps_from(&id)
            .into_iter()
            .filter(|(key, _)| key.kind() == DepKind::Parent)
            .map(|(key, _)| key.to().clone())
            .collect();

        let mut delta = TxnDeltaV1::new();
        for existing_parent in existing_parents {
            delta
                .insert(TxnOpV1::DepDelete(WireDepDeleteV1 {
                    from: id.clone(),
                    to: existing_parent,
                    kind: DepKind::Parent,
                    deleted_at: WireStamp::from(&stamp.at),
                    deleted_by: stamp.by.clone(),
                }))
                .map_err(delta_error_to_op)?;
        }

        if let Some(parent_id) = parent_id.clone() {
            delta
                .insert(TxnOpV1::DepUpsert(WireDepV1 {
                    from: id.clone(),
                    to: parent_id,
                    kind: DepKind::Parent,
                    created_at: WireStamp::from(&stamp.at),
                    created_by: stamp.by.clone(),
                    deleted_at: None,
                    deleted_by: None,
                }))
                .map_err(delta_error_to_op)?;
        }

        let canonical = CanonicalMutationOp::SetParent {
            id,
            parent: parent_id,
        };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_add_note(
        &self,
        state: &CanonicalState,
        stamp: &Stamp,
        id: BeadId,
        content: String,
    ) -> Result<PlannedDelta, OpError> {
        enforce_note_limit(&content, &self.limits)?;
        let bead = state.require_live(&id).map_live_err(&id)?;
        let note_id = generate_unique_note_id(&bead.notes, NoteId::generate);

        let note = WireNoteV1 {
            id: note_id.clone(),
            content: content.clone(),
            author: stamp.by.clone(),
            at: WireStamp::from(&stamp.at),
        };

        let append = NoteAppendV1 {
            bead_id: id.clone(),
            note,
        };

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::NoteAppend(append))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::AddNote { id, content };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_claim(
        &self,
        state: &CanonicalState,
        stamp: &Stamp,
        now_ms: u64,
        id: BeadId,
        lease_secs: u64,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;

        if let crate::core::Claim::Claimed {
            assignee,
            expires: Some(exp),
        } = &bead.fields.claim.value
        {
            let now = WallClock(now_ms);
            if *exp >= now && assignee != &stamp.by {
                return Err(OpError::AlreadyClaimed {
                    by: assignee.clone(),
                    expires: Some(*exp),
                });
            }
        }

        let expires = WallClock(
            stamp
                .at
                .wall_ms
                .saturating_add(lease_secs.saturating_mul(1000)),
        );

        let mut patch = WireBeadPatch::new(id.clone());
        patch.assignee = WirePatch::Set(stamp.by.clone());
        patch.assignee_expires = WirePatch::Set(expires);
        patch.status = Some(WorkflowStatus::InProgress);

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::Claim { id, lease_secs };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_unclaim(
        &self,
        state: &CanonicalState,
        stamp: &Stamp,
        id: BeadId,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;

        if let crate::core::Claim::Claimed { assignee, .. } = &bead.fields.claim.value {
            if assignee != &stamp.by {
                return Err(OpError::NotClaimedByYou);
            }
        } else {
            return Err(OpError::NotClaimedByYou);
        }

        let mut patch = WireBeadPatch::new(id.clone());
        patch.assignee = WirePatch::Clear;
        patch.status = Some(WorkflowStatus::Open);

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::Unclaim { id };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_extend_claim(
        &self,
        state: &CanonicalState,
        stamp: &Stamp,
        id: BeadId,
        lease_secs: u64,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;

        if let crate::core::Claim::Claimed { assignee, .. } = &bead.fields.claim.value {
            if assignee != &stamp.by {
                return Err(OpError::NotClaimedByYou);
            }
        } else {
            return Err(OpError::NotClaimedByYou);
        }

        let expires = WallClock(
            stamp
                .at
                .wall_ms
                .saturating_add(lease_secs.saturating_mul(1000)),
        );

        let mut patch = WireBeadPatch::new(id.clone());
        patch.assignee = WirePatch::Keep;
        patch.assignee_expires = WirePatch::Set(expires);

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::ExtendClaim { id, lease_secs };

        Ok(PlannedDelta { delta, canonical })
    }
}

struct PlannedDelta {
    delta: TxnDeltaV1,
    canonical: CanonicalMutationOp,
}

#[derive(Clone, Debug, serde::Serialize)]
struct CanonicalMutation {
    namespace: NamespaceId,
    actor_id: ActorId,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_request_id: Option<ClientRequestId>,
    #[serde(flatten)]
    op: CanonicalMutationOp,
}

#[derive(Clone, Debug, serde::Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum CanonicalMutationOp {
    Create {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<BeadId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent: Option<BeadId>,
        title: String,
        bead_type: BeadType,
        priority: Priority,
        description: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        design: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        acceptance_criteria: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        assignee: Option<ActorId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        external_ref: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        estimated_minutes: Option<u32>,
        labels: Vec<String>,
        dependencies: Vec<String>,
    },
    Update {
        id: BeadId,
        patch: CanonicalBeadPatch,
        #[serde(skip_serializing_if = "Option::is_none")]
        cas: Option<String>,
    },
    AddLabels {
        id: BeadId,
        labels: Vec<String>,
    },
    RemoveLabels {
        id: BeadId,
        labels: Vec<String>,
    },
    Close {
        id: BeadId,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        on_branch: Option<String>,
    },
    Reopen {
        id: BeadId,
    },
    Delete {
        id: BeadId,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    SetParent {
        id: BeadId,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent: Option<BeadId>,
    },
    AddDep {
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    },
    RemoveDep {
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    },
    AddNote {
        id: BeadId,
        content: String,
    },
    Claim {
        id: BeadId,
        lease_secs: u64,
    },
    Unclaim {
        id: BeadId,
    },
    ExtendClaim {
        id: BeadId,
        lease_secs: u64,
    },
}

#[derive(Clone, Debug, serde::Serialize)]
struct CanonicalBeadPatch {
    #[serde(skip_serializing_if = "Patch::is_keep")]
    title: Patch<String>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    description: Patch<String>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    design: Patch<String>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    acceptance_criteria: Patch<String>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    priority: Patch<Priority>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    bead_type: Patch<BeadType>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    labels: Patch<Vec<String>>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    external_ref: Patch<String>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    source_repo: Patch<String>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    estimated_minutes: Patch<u32>,
    #[serde(skip_serializing_if = "Patch::is_keep")]
    status: Patch<WorkflowStatus>,
}

fn request_sha256(ctx: &MutationContext, op: &CanonicalMutationOp) -> Result<[u8; 32], OpError> {
    let canon = CanonicalMutation {
        namespace: ctx.namespace.clone(),
        actor_id: ctx.actor_id.clone(),
        client_request_id: ctx.client_request_id,
        op: op.clone(),
    };
    let bytes = to_canon_json_bytes(&canon).map_err(|err| OpError::InvalidRequest {
        field: Some("request".into()),
        reason: err.to_string(),
    })?;
    Ok(sha256_bytes(&bytes).0)
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

fn normalize_assignee(value: Option<String>, actor: &ActorId) -> Result<Option<ActorId>, OpError> {
    let Some(raw) = normalize_optional_string(value) else {
        return Ok(None);
    };
    match raw.as_str() {
        "me" | "self" => Ok(Some(actor.clone())),
        _ if raw == actor.as_str() => Ok(Some(actor.clone())),
        _ => Err(OpError::ValidationFailed {
            field: "assignee".into(),
            reason: "cannot assign other actors; run bd as that actor".into(),
        }),
    }
}

fn parse_bead_id_validation(field: &str, raw: String) -> Result<BeadId, OpError> {
    BeadId::parse(&raw).map_err(|err| OpError::ValidationFailed {
        field: field.into(),
        reason: err.to_string(),
    })
}

fn parse_bead_id_invalid_id(field: &str, raw: String) -> Result<BeadId, OpError> {
    match BeadId::parse(&raw) {
        Ok(id) => Ok(id),
        Err(CoreError::InvalidId(id)) => Err(OpError::InvalidId(id)),
        Err(other) => Err(OpError::InvalidRequest {
            field: Some(field.into()),
            reason: other.to_string(),
        }),
    }
}

fn parse_labels(labels: Vec<String>) -> Result<Labels, OpError> {
    let mut parsed = Labels::new();
    for raw in labels {
        let label = Label::parse(raw).map_err(|e| OpError::ValidationFailed {
            field: "labels".into(),
            reason: e.to_string(),
        })?;
        parsed.insert(label);
    }
    Ok(parsed)
}

fn parse_dep_specs(deps: &[String]) -> Result<Vec<DepSpec>, OpError> {
    DepSpec::parse_list(deps).map_err(|e| OpError::ValidationFailed {
        field: "dependencies".into(),
        reason: e.to_string(),
    })
}

fn canonical_labels(labels: &Labels) -> Vec<String> {
    labels
        .iter()
        .map(|label| label.as_str().to_string())
        .collect()
}

fn sort_dedup_dep_specs(specs: &mut Vec<DepSpec>) {
    specs.sort_by(|a, b| {
        a.kind()
            .as_str()
            .cmp(b.kind().as_str())
            .then_with(|| a.id().as_str().cmp(b.id().as_str()))
    });
    specs.dedup_by(|a, b| a.kind() == b.kind() && a.id() == b.id());
}

fn canonical_deps(deps: &[DepSpec]) -> Vec<String> {
    let mut parsed = deps.to_vec();
    sort_dedup_dep_specs(&mut parsed);
    parsed
        .into_iter()
        .map(|spec| spec.to_spec_string())
        .collect()
}

fn next_child_id(state: &CanonicalState, parent: &BeadId) -> Result<BeadId, OpError> {
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
        .chain(state.iter_tombstones().map(|(_, t)| &t.id))
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

fn would_create_cycle(state: &CanonicalState, from: &BeadId, to: &BeadId, kind: DepKind) -> bool {
    use std::collections::HashSet;

    if !kind.requires_dag() {
        return false;
    }

    let mut visited = HashSet::new();
    let mut queue = vec![to.clone()];

    while let Some(current) = queue.pop() {
        if &current == from {
            return true;
        }
        if !visited.insert(current.clone()) {
            continue;
        }
        for (key, _) in state.deps_from(&current) {
            if key.kind().requires_dag() && !visited.contains(key.to()) {
                queue.push(key.to().clone());
            }
        }
    }

    false
}

fn normalize_patch(
    id: &BeadId,
    patch: &ParsedBeadPatch,
) -> Result<(WireBeadPatch, CanonicalBeadPatch), OpError> {
    let mut wire = WireBeadPatch::new(id.clone());

    if let Patch::Set(title) = &patch.title {
        wire.title = Some(title.clone());
    }
    if let Patch::Set(description) = &patch.description {
        wire.description = Some(description.clone());
    }
    wire.design = patch_to_wire(&patch.design);
    wire.acceptance_criteria = patch_to_wire(&patch.acceptance_criteria);
    if let Patch::Set(priority) = &patch.priority {
        wire.priority = Some(*priority);
    }
    if let Patch::Set(bead_type) = &patch.bead_type {
        wire.bead_type = Some(*bead_type);
    }

    let canon_labels = match &patch.labels {
        Patch::Set(labels) => {
            wire.labels = Some(labels.clone());
            Patch::Set(canonical_labels(labels))
        }
        Patch::Clear => Patch::Clear,
        Patch::Keep => Patch::Keep,
    };

    wire.external_ref = patch_to_wire(&patch.external_ref);
    wire.source_repo = patch_to_wire(&patch.source_repo);
    wire.estimated_minutes = patch_to_wire_u32(&patch.estimated_minutes);

    if let Patch::Set(status) = &patch.status {
        wire.status = Some(*status);
    }

    let canonical = CanonicalBeadPatch {
        title: patch.title.clone(),
        description: patch.description.clone(),
        design: patch.design.clone(),
        acceptance_criteria: patch.acceptance_criteria.clone(),
        priority: patch.priority.clone(),
        bead_type: patch.bead_type.clone(),
        labels: canon_labels,
        external_ref: patch.external_ref.clone(),
        source_repo: patch.source_repo.clone(),
        estimated_minutes: patch.estimated_minutes.clone(),
        status: patch.status.clone(),
    };

    Ok((wire, canonical))
}

fn patch_to_wire(patch: &Patch<String>) -> WirePatch<String> {
    match patch {
        Patch::Keep => WirePatch::Keep,
        Patch::Clear => WirePatch::Clear,
        Patch::Set(value) => WirePatch::Set(value.clone()),
    }
}

fn patch_to_wire_u32(patch: &Patch<u32>) -> WirePatch<u32> {
    match patch {
        Patch::Keep => WirePatch::Keep,
        Patch::Clear => WirePatch::Clear,
        Patch::Set(value) => WirePatch::Set(*value),
    }
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

fn delta_error_to_op(err: TxnDeltaError) -> OpError {
    OpError::ValidationFailed {
        field: "delta".into(),
        reason: err.to_string(),
    }
}

fn estimated_record_bytes(payload_len: usize, has_client_request_id: bool) -> usize {
    let mut len = RECORD_HEADER_BASE_LEN + payload_len + 32 + 32;
    if has_client_request_id {
        len += 16;
    }
    len
}

fn txn_id_for_stamp(store: &StoreIdentity, stamp: &crate::core::Stamp) -> TxnId {
    let namespace = store.store_id.as_uuid();
    let name = format!(
        "{}:{}:{}",
        stamp.by.as_str(),
        stamp.at.wall_ms,
        stamp.at.counter
    );
    TxnId::new(Uuid::new_v5(&namespace, name.as_bytes()))
}

fn generate_unique_id(
    state: &CanonicalState,
    root_slug: Option<&str>,
    title: &str,
    description: &str,
    actor: &ActorId,
    stamp: &crate::core::WriteStamp,
    remote: &RemoteUrl,
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
    actor: &ActorId,
    stamp: &crate::core::WriteStamp,
    remote: &RemoteUrl,
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

fn infer_bead_slug(state: &CanonicalState) -> Result<BeadSlug, OpError> {
    let mut counts: BTreeMap<BeadSlug, usize> = BTreeMap::new();
    for id in state
        .iter_live()
        .map(|(id, _)| id)
        .chain(state.iter_tombstones().map(|(_, t)| &t.id))
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
        Bead, BeadCore, BeadFields, Claim, Lww, Stamp, StoreId, Workflow, WriteStamp,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap()
    }

    fn make_state_with_bead(id: &str, actor: &ActorId) -> CanonicalState {
        let stamp = Stamp::new(WriteStamp::new(10, 0), actor.clone());
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("t".to_string(), stamp.clone()),
            description: Lww::new("d".to_string(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::default(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            labels: Lww::new(Labels::new(), stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::Unclaimed, stamp),
        };
        let bead = Bead::new(core, fields);
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();
        state
    }

    struct FixedTimeSource {
        now: Arc<AtomicU64>,
    }

    impl crate::daemon::clock::TimeSource for FixedTimeSource {
        fn now_ms(&self) -> u64 {
            self.now.load(Ordering::SeqCst)
        }
    }

    fn fixed_clock(now: u64) -> Clock {
        let source = Box::new(FixedTimeSource {
            now: Arc::new(AtomicU64::new(now)),
        });
        Clock::with_time_source(source)
    }

    #[test]
    fn request_hash_is_stable_with_label_ordering() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([1u8; 16])), 0.into());
        let state = make_state_with_bead("bd-123", &actor);

        let req_a = ParsedMutationRequest::parse(
            MutationRequest::AddLabels {
                id: "bd-123".into(),
                labels: vec!["b".into(), "a".into()],
            },
            &actor,
        )
        .unwrap();
        let req_b = ParsedMutationRequest::parse(
            MutationRequest::AddLabels {
                id: "bd-123".into(),
                labels: vec!["a".into(), "b".into()],
            },
            &actor,
        )
        .unwrap();

        let mut clock_a = fixed_clock(1_000);
        let mut clock_b = fixed_clock(1_000);

        let draft_a = engine
            .plan(&state, &mut clock_a, store, None, ctx.clone(), req_a)
            .unwrap();
        let draft_b = engine
            .plan(&state, &mut clock_b, store, None, ctx, req_b)
            .unwrap();

        assert_eq!(draft_a.request_sha256, draft_b.request_sha256);
        assert_eq!(draft_a.delta, draft_b.delta);
    }

    #[test]
    fn rejects_note_over_limit() {
        let limits = Limits {
            max_note_bytes: 2,
            ..Default::default()
        };
        let engine = MutationEngine::new(limits);
        let actor = actor_id("alice");
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([3u8; 16])), 0.into());
        let state = make_state_with_bead("bd-456", &actor);

        let req = ParsedMutationRequest::parse(
            MutationRequest::AddNote {
                id: "bd-456".into(),
                content: "hey".into(),
            },
            &actor,
        )
        .unwrap();

        let mut clock = fixed_clock(1_000);
        let err = engine
            .plan(&state, &mut clock, store, None, ctx, req)
            .unwrap_err();

        assert!(matches!(err, OpError::NoteTooLarge { .. }));
    }

    #[test]
    fn rejects_ops_over_limit() {
        let limits = Limits {
            max_ops_per_txn: 0,
            ..Default::default()
        };
        let engine = MutationEngine::new(limits);
        let actor = actor_id("alice");
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([5u8; 16])), 0.into());
        let state = make_state_with_bead("bd-789", &actor);

        let req = ParsedMutationRequest::parse(
            MutationRequest::AddLabels {
                id: "bd-789".into(),
                labels: vec!["alpha".into()],
            },
            &actor,
        )
        .unwrap();

        let mut clock = fixed_clock(1_000);
        let err = engine
            .plan(&state, &mut clock, store, None, ctx, req)
            .unwrap_err();

        assert!(matches!(err, OpError::OpsTooMany { .. }));
    }

    #[test]
    fn planning_does_not_mutate_state() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([7u8; 16])), 0.into());
        let state = make_state_with_bead("bd-000", &actor);
        let before = serde_json::to_string(&state).unwrap();

        let req = ParsedMutationRequest::parse(
            MutationRequest::AddLabels {
                id: "bd-000".into(),
                labels: vec!["alpha".into()],
            },
            &actor,
        )
        .unwrap();

        let mut clock = fixed_clock(1_000);
        let _ = engine
            .plan(&state, &mut clock, store, None, ctx, req)
            .unwrap();

        let after = serde_json::to_string(&state).unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn txn_id_includes_actor_identity() {
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([9u8; 16])), 0.into());
        let stamp_a = Stamp::new(WriteStamp::new(42, 7), actor_id("alice"));
        let stamp_b = Stamp::new(WriteStamp::new(42, 7), actor_id("bob"));

        let id_a = txn_id_for_stamp(&store, &stamp_a);
        let id_b = txn_id_for_stamp(&store, &stamp_b);

        assert_ne!(id_a, id_b);
    }

    #[test]
    fn parsed_request_rejects_invalid_id() {
        let actor = actor_id("alice");
        let err = ParsedMutationRequest::parse(
            MutationRequest::AddLabels {
                id: "bad".into(),
                labels: vec![],
            },
            &actor,
        )
        .unwrap_err();
        assert!(matches!(err, OpError::InvalidId(_)));
    }

    #[test]
    fn parsed_request_rejects_invalid_label() {
        let actor = actor_id("alice");
        let err = ParsedMutationRequest::parse(
            MutationRequest::AddLabels {
                id: "bd-123".into(),
                labels: vec!["bad\nlabel".into()],
            },
            &actor,
        )
        .unwrap_err();
        assert!(matches!(err, OpError::ValidationFailed { field, .. } if field == "labels"));
    }
}
