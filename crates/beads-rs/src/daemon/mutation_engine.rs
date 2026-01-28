//! Mutation planning for realtime event generation.

use std::collections::BTreeMap;

use uuid::Uuid;

use super::ops::{MapLiveError, OpError};
use super::remote::RemoteUrl;
use crate::core::event::ValidatedBeadPatch;
use crate::core::{
    ActorId, BeadId, BeadSlug, BeadType, CanonicalState, ClientRequestId, DepKey, DepKind, DepSpec,
    Dot, EventBody, EventBytes, EventKindV1, HlcMax, Label, Labels, Limits, NamespaceId,
    NoteAppendV1, NoteId, Priority, ReplicaId, Seq1, Stamp, StoreIdentity, TraceId, TxnDeltaError,
    TxnDeltaV1, TxnId, TxnOpV1, TxnV1, ValidatedEventBody, WallClock, WireBeadPatch, WireDepAddV1,
    WireDepRemoveV1, WireDotV1, WireDvvV1, WireLabelAddV1, WireLabelRemoveV1, WireNoteV1,
    WirePatch, WireStamp, WireTombstoneV1, WorkflowStatus, encode_event_body_canonical,
    sha256_bytes, to_canon_json_bytes,
};
use crate::daemon::ipc::{
    AddNotePayload, ClaimPayload, ClosePayload, CreatePayload, DeletePayload, DepPayload,
    IdPayload, LabelsPayload, LeasePayload, ParentPayload, UpdatePayload,
};
use crate::daemon::wal::record::RECORD_HEADER_BASE_LEN;
use beads_surface::ops::{BeadPatch, Patch};

#[derive(Clone, Debug)]
pub struct MutationContext {
    pub namespace: NamespaceId,
    pub actor_id: ActorId,
    pub client_request_id: Option<ClientRequestId>,
    pub trace_id: TraceId,
}

pub trait DotAllocator {
    fn next_dot(&mut self) -> Result<Dot, OpError>;
}

#[derive(Clone, Debug)]
pub struct IdContext {
    pub root_slug: Option<BeadSlug>,
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
    pub trace_id: TraceId,
}

#[derive(Clone, Debug)]
pub struct SequencedEvent {
    pub event_body: ValidatedEventBody,
    pub event_bytes: EventBytes<crate::core::Canonical>,
    pub request_sha256: [u8; 32],
    pub client_request_id: Option<ClientRequestId>,
}

#[derive(Clone, Debug)]
pub struct ParsedBeadPatch {
    pub title: Patch<String>,
    pub description: Patch<String>,
    pub design: Patch<String>,
    pub acceptance_criteria: Patch<String>,
    pub priority: Patch<Priority>,
    pub bead_type: Patch<BeadType>,
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

        Ok(Self {
            title,
            description,
            design,
            acceptance_criteria,
            priority,
            bead_type,
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
    pub fn parse_create(payload: CreatePayload, actor: &ActorId) -> Result<Self, OpError> {
        let CreatePayload {
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
        } = payload;
        if id.is_some() && parent.is_some() {
            return Err(OpError::ValidationFailed {
                field: "create".into(),
                reason: "cannot specify both id and parent".into(),
            });
        }

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

    pub fn parse_update(payload: UpdatePayload) -> Result<Self, OpError> {
        let UpdatePayload { id, patch, cas } = payload;
        let patch = ParsedBeadPatch::parse(patch)?;
        Ok(ParsedMutationRequest::Update { id, patch, cas })
    }

    pub fn parse_add_labels(payload: LabelsPayload) -> Result<Self, OpError> {
        let LabelsPayload { id, labels } = payload;
        let labels = parse_labels(labels)?;
        Ok(ParsedMutationRequest::AddLabels { id, labels })
    }

    pub fn parse_remove_labels(payload: LabelsPayload) -> Result<Self, OpError> {
        let LabelsPayload { id, labels } = payload;
        let labels = parse_labels(labels)?;
        Ok(ParsedMutationRequest::RemoveLabels { id, labels })
    }

    pub fn parse_set_parent(payload: ParentPayload) -> Result<Self, OpError> {
        let ParentPayload { id, parent } = payload;
        Ok(ParsedMutationRequest::SetParent { id, parent })
    }

    pub fn parse_close(payload: ClosePayload) -> Result<Self, OpError> {
        let ClosePayload {
            id,
            reason,
            on_branch,
        } = payload;
        Ok(ParsedMutationRequest::Close {
            id,
            reason,
            on_branch: on_branch.map(String::from),
        })
    }

    pub fn parse_reopen(payload: IdPayload) -> Result<Self, OpError> {
        let IdPayload { id } = payload;
        Ok(ParsedMutationRequest::Reopen { id })
    }

    pub fn parse_delete(payload: DeletePayload) -> Result<Self, OpError> {
        let DeletePayload { id, reason } = payload;
        Ok(ParsedMutationRequest::Delete { id, reason })
    }

    pub fn parse_add_dep(payload: DepPayload) -> Result<Self, OpError> {
        let DepPayload { from, to, kind } = payload;
        Ok(ParsedMutationRequest::AddDep { from, to, kind })
    }

    pub fn parse_remove_dep(payload: DepPayload) -> Result<Self, OpError> {
        let DepPayload { from, to, kind } = payload;
        Ok(ParsedMutationRequest::RemoveDep { from, to, kind })
    }

    pub fn parse_add_note(payload: AddNotePayload) -> Result<Self, OpError> {
        let AddNotePayload { id, content } = payload;
        Ok(ParsedMutationRequest::AddNote { id, content })
    }

    pub fn parse_claim(payload: ClaimPayload) -> Result<Self, OpError> {
        let ClaimPayload { id, lease_secs } = payload;
        Ok(ParsedMutationRequest::Claim { id, lease_secs })
    }

    pub fn parse_unclaim(payload: IdPayload) -> Result<Self, OpError> {
        let IdPayload { id } = payload;
        Ok(ParsedMutationRequest::Unclaim { id })
    }

    pub fn parse_extend_claim(payload: LeasePayload) -> Result<Self, OpError> {
        let LeasePayload { id, lease_secs } = payload;
        Ok(ParsedMutationRequest::ExtendClaim { id, lease_secs })
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
        now_ms: u64,
        stamp: Stamp,
        store: StoreIdentity,
        id_ctx: Option<&IdContext>,
        ctx: MutationContext,
        req: ParsedMutationRequest,
        dot_alloc: &mut dyn DotAllocator,
    ) -> Result<EventDraft, OpError> {
        let write_stamp = stamp.at.clone();

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
                dot_alloc,
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
                self.plan_add_labels(state, id, labels, dot_alloc)?
            }
            ParsedMutationRequest::RemoveLabels { id, labels } => {
                self.plan_remove_labels(state, id, labels)?
            }
            ParsedMutationRequest::SetParent { id, parent } => {
                self.plan_set_parent(state, id, parent, &stamp, dot_alloc)?
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
                self.plan_add_dep(state, &stamp, from, to, kind, dot_alloc)?
            }
            ParsedMutationRequest::RemoveDep { from, to, kind } => {
                self.plan_remove_dep(state, &stamp, from, to, kind)?
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
            trace_id,
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
            trace_id,
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
            trace_id: Some(draft.trace_id),
            kind: EventKindV1::TxnV1(TxnV1 {
                delta: draft.delta,
                hlc_max: draft.hlc_max,
            }),
        };

        let event_body =
            event_body
                .into_validated(&self.limits)
                .map_err(|err| OpError::ValidationFailed {
                    field: "event".into(),
                    reason: err.to_string(),
                })?;

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
                TxnOpV1::BeadUpsert(_) => {}
                TxnOpV1::BeadDelete(_) => {}
                TxnOpV1::LabelAdd(_) => {}
                TxnOpV1::LabelRemove(_) => {}
                TxnOpV1::DepAdd(_) => {}
                TxnOpV1::DepRemove(_) => {}
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
        dot_alloc: &mut dyn DotAllocator,
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
                    id_ctx.root_slug.as_ref(),
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

        let patch = validate_wire_patch(patch)?;
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
            .map_err(delta_error_to_op)?;

        for label in labels.iter() {
            let dot = dot_alloc.next_dot()?;
            delta
                .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
                    bead_id: id.clone(),
                    label: label.clone(),
                    dot: WireDotV1::from(dot),
                    lineage: Some(stamp.clone().into()),
                }))
                .map_err(delta_error_to_op)?;
        }

        if let Some(parent_id) = parent_id {
            delta
                .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                    key: DepKey::new(id.clone(), parent_id, DepKind::Parent).map_err(|e| {
                        OpError::ValidationFailed {
                            field: "parent".into(),
                            reason: e.reason,
                        }
                    })?,
                    dot: WireDotV1::from(dot_alloc.next_dot()?),
                }))
                .map_err(delta_error_to_op)?;
        }

        for spec in parsed_deps {
            delta
                .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                    key: DepKey::new(id.clone(), spec.id().clone(), spec.kind()).map_err(|e| {
                        OpError::ValidationFailed {
                            field: "dependency".into(),
                            reason: e.reason,
                        }
                    })?,
                    dot: WireDotV1::from(dot_alloc.next_dot()?),
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
        state.require_live(&id).map_live_err(&id)?;

        if let Some(expected) = cas.as_ref() {
            let view = state.bead_view(&id).expect("live bead should have view");
            let actual = view.content_hash().to_hex();
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
        dot_alloc: &mut dyn DotAllocator,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;
        let lineage = bead.core.created().clone();
        let mut merged = state.labels_for(&id);
        for label in labels.iter() {
            merged.insert(label.clone());
        }
        enforce_label_limit(&merged, &self.limits, Some(id.clone()))?;

        let mut delta = TxnDeltaV1::new();
        for label in labels.iter() {
            let dot = dot_alloc.next_dot()?;
            delta
                .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
                    bead_id: id.clone(),
                    label: label.clone(),
                    dot: WireDotV1::from(dot),
                    lineage: Some(lineage.clone().into()),
                }))
                .map_err(delta_error_to_op)?;
        }

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
        let lineage = bead.core.created().clone();
        let mut delta = TxnDeltaV1::new();
        for label in labels.iter() {
            let ctx = state.label_dvv(&id, label, Some(&lineage));
            delta
                .insert(TxnOpV1::LabelRemove(WireLabelRemoveV1 {
                    bead_id: id.clone(),
                    label: label.clone(),
                    ctx: WireDvvV1::from(&ctx),
                    lineage: Some(lineage.clone().into()),
                }))
                .map_err(delta_error_to_op)?;
        }

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
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;
        if let Some(reason) = reason.clone() {
            patch.closed_reason = WirePatch::Set(reason);
        }
        if let Some(branch) = on_branch.clone() {
            patch.closed_on_branch = WirePatch::Set(branch);
        }

        let patch = validate_wire_patch(patch)?;
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
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;

        let patch = validate_wire_patch(patch)?;
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
            lineage: None,
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
        _stamp: &Stamp,
        from: BeadId,
        to: BeadId,
        kind: DepKind,
        dot_alloc: &mut dyn DotAllocator,
    ) -> Result<PlannedDelta, OpError> {
        let key =
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
        if kind.requires_dag()
            && let Err(err) = state.check_no_cycle(&from, &to, kind)
        {
            return Err(OpError::ValidationFailed {
                field: "dependency".into(),
                reason: err.reason,
            });
        }

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: key.clone(),
                dot: WireDotV1::from(dot_alloc.next_dot()?),
            }))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::AddDep { from, to, kind };

        Ok(PlannedDelta { delta, canonical })
    }

    fn plan_remove_dep(
        &self,
        state: &CanonicalState,
        _stamp: &Stamp,
        from: BeadId,
        to: BeadId,
        kind: DepKind,
    ) -> Result<PlannedDelta, OpError> {
        let key =
            DepKey::new(from.clone(), to.clone(), kind).map_err(|e| OpError::ValidationFailed {
                field: "dependency".into(),
                reason: e.reason,
            })?;

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
                key: key.clone(),
                ctx: WireDvvV1::from(&state.dep_dvv(&key)),
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
        _stamp: &Stamp,
        dot_alloc: &mut dyn DotAllocator,
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
                if let Err(err) = state.check_no_cycle(&id, parent_id, DepKind::Parent) {
                    return Err(OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: err.reason,
                    });
                }
                Some(parent_id.clone())
            }
            None => None,
        };

        let existing_parents: Vec<BeadId> = state
            .deps_from(&id)
            .into_iter()
            .filter(|key| key.kind() == DepKind::Parent)
            .map(|key| key.to().clone())
            .collect();

        let mut delta = TxnDeltaV1::new();
        for existing_parent in existing_parents {
            let key =
                DepKey::new(id.clone(), existing_parent.clone(), DepKind::Parent).map_err(|e| {
                    OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: e.reason,
                    }
                })?;
            delta
                .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
                    key: key.clone(),
                    ctx: WireDvvV1::from(&state.dep_dvv(&key)),
                }))
                .map_err(delta_error_to_op)?;
        }

        if let Some(parent_id) = parent_id.clone() {
            delta
                .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                    key: DepKey::new(id.clone(), parent_id, DepKind::Parent).map_err(|e| {
                        OpError::ValidationFailed {
                            field: "parent".into(),
                            reason: e.reason,
                        }
                    })?,
                    dot: WireDotV1::from(dot_alloc.next_dot()?),
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
        let lineage = bead.core.created().clone();
        let note_id = generate_unique_note_id(state, &id, NoteId::generate);

        let note = WireNoteV1 {
            id: note_id.clone(),
            content: content.clone(),
            author: stamp.by.clone(),
            at: WireStamp::from(&stamp.at),
        };

        let append = NoteAppendV1 {
            bead_id: id.clone(),
            note,
            lineage: Some(lineage.into()),
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
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;

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
        patch.assignee_expires = WirePatch::Clear;
        patch.status = Some(WorkflowStatus::Open);
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;

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

        let assignee = match &bead.fields.claim.value {
            crate::core::Claim::Claimed { assignee, .. } => {
                if assignee != &stamp.by {
                    return Err(OpError::NotClaimedByYou);
                }
                assignee.clone()
            }
            crate::core::Claim::Unclaimed => {
                return Err(OpError::NotClaimedByYou);
            }
        };

        let expires = WallClock(
            stamp
                .at
                .wall_ms
                .saturating_add(lease_secs.saturating_mul(1000)),
        );

        let mut patch = WireBeadPatch::new(id.clone());
        patch.assignee = WirePatch::Set(assignee);
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

fn validate_wire_patch(patch: WireBeadPatch) -> Result<WireBeadPatch, OpError> {
    ValidatedBeadPatch::try_from(patch)
        .map(ValidatedBeadPatch::into_inner)
        .map_err(|err| OpError::ValidationFailed {
            field: "bead_patch".into(),
            reason: err.to_string(),
        })
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

    wire.external_ref = patch_to_wire(&patch.external_ref);
    wire.source_repo = patch_to_wire(&patch.source_repo);
    wire.estimated_minutes = patch_to_wire_u32(&patch.estimated_minutes);

    if let Patch::Set(status) = &patch.status {
        wire.status = Some(*status);
        wire.closed_reason = WirePatch::Clear;
        wire.closed_on_branch = WirePatch::Clear;
    }

    let canonical = CanonicalBeadPatch {
        title: patch.title.clone(),
        description: patch.description.clone(),
        design: patch.design.clone(),
        acceptance_criteria: patch.acceptance_criteria.clone(),
        priority: patch.priority.clone(),
        bead_type: patch.bead_type.clone(),
        external_ref: patch.external_ref.clone(),
        source_repo: patch.source_repo.clone(),
        estimated_minutes: patch.estimated_minutes.clone(),
        status: patch.status.clone(),
    };

    let wire = validate_wire_patch(wire)?;
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
    root_slug: Option<&BeadSlug>,
    title: &str,
    description: &str,
    actor: &ActorId,
    stamp: &crate::core::WriteStamp,
    remote: &RemoteUrl,
) -> Result<BeadId, OpError> {
    let slug = match root_slug {
        Some(slug) => slug.clone(),
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

fn generate_unique_note_id<F>(state: &CanonicalState, bead_id: &BeadId, mut next_id: F) -> NoteId
where
    F: FnMut() -> NoteId,
{
    let mut note_id = next_id();
    while state.note_id_exists(bead_id, &note_id) {
        note_id = next_id();
    }
    note_id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        Bead, BeadCore, BeadFields, Claim, Dot, Lww, ReplicaId, Stamp, StoreId, Workflow,
        WriteStamp,
    };

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

    fn make_stamp(now_ms: u64, actor: &ActorId) -> Stamp {
        Stamp::new(WriteStamp::new(now_ms, 0), actor.clone())
    }

    struct TestDotAllocator {
        replica_id: ReplicaId,
        counter: u64,
    }

    impl TestDotAllocator {
        fn new(replica_id: ReplicaId) -> Self {
            Self {
                replica_id,
                counter: 0,
            }
        }
    }

    impl DotAllocator for TestDotAllocator {
        fn next_dot(&mut self) -> Result<Dot, OpError> {
            self.counter += 1;
            Ok(Dot {
                replica: self.replica_id,
                counter: self.counter,
            })
        }
    }

    #[test]
    fn request_hash_is_stable_with_label_ordering() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([1u8; 16])), 0.into());
        let state = make_state_with_bead("bd-123", &actor);

        let req_a = ParsedMutationRequest::parse_add_labels(LabelsPayload {
            id: BeadId::parse("bd-123").expect("bead id"),
            labels: vec!["b".into(), "a".into()],
        })
        .unwrap();
        let req_b = ParsedMutationRequest::parse_add_labels(LabelsPayload {
            id: BeadId::parse("bd-123").expect("bead id"),
            labels: vec!["a".into(), "b".into()],
        })
        .unwrap();

        let now_ms = 1_000;
        let stamp_a = make_stamp(now_ms, &actor);
        let stamp_b = make_stamp(now_ms, &actor);
        let replica_id = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let mut dots_a = TestDotAllocator::new(replica_id);
        let mut dots_b = TestDotAllocator::new(replica_id);

        let draft_a = engine
            .plan(
                &state,
                now_ms,
                stamp_a,
                store,
                None,
                ctx.clone(),
                req_a,
                &mut dots_a,
            )
            .unwrap();
        let draft_b = engine
            .plan(
                &state,
                now_ms,
                stamp_b,
                store,
                None,
                ctx,
                req_b,
                &mut dots_b,
            )
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
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([3u8; 16])), 0.into());
        let state = make_state_with_bead("bd-456", &actor);

        let req = ParsedMutationRequest::parse_add_note(AddNotePayload {
            id: BeadId::parse("bd-456").expect("bead id"),
            content: "hey".into(),
        })
        .unwrap();

        let now_ms = 1_000;
        let stamp = make_stamp(now_ms, &actor);
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([2u8; 16])));
        let err = engine
            .plan(&state, now_ms, stamp, store, None, ctx, req, &mut dots)
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
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([5u8; 16])), 0.into());
        let state = make_state_with_bead("bd-789", &actor);

        let req = ParsedMutationRequest::parse_add_labels(LabelsPayload {
            id: BeadId::parse("bd-789").expect("bead id"),
            labels: vec!["alpha".into()],
        })
        .unwrap();

        let now_ms = 1_000;
        let stamp = make_stamp(now_ms, &actor);
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));
        let err = engine
            .plan(&state, now_ms, stamp, store, None, ctx, req, &mut dots)
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
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([7u8; 16])), 0.into());
        let state = make_state_with_bead("bd-000", &actor);
        let before = serde_json::to_string(&state).unwrap();

        let req = ParsedMutationRequest::parse_add_labels(LabelsPayload {
            id: BeadId::parse("bd-000").expect("bead id"),
            labels: vec!["alpha".into()],
        })
        .unwrap();

        let now_ms = 1_000;
        let stamp = make_stamp(now_ms, &actor);
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([4u8; 16])));
        let _ = engine
            .plan(&state, now_ms, stamp, store, None, ctx, req, &mut dots)
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
    fn parsed_request_rejects_invalid_label() {
        let err = ParsedMutationRequest::parse_add_labels(LabelsPayload {
            id: BeadId::parse("bd-123").expect("bead id"),
            labels: vec!["bad\nlabel".into()],
        })
        .unwrap_err();
        assert!(matches!(err, OpError::ValidationFailed { field, .. } if field == "labels"));
    }
}
