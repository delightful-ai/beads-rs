//! Mutation planning for realtime event generation.

use std::collections::BTreeMap;

use uuid::Uuid;

use super::ops::{MapLiveError, OpError, ValidatedSurfaceBeadPatch};
use crate::core::limits::LimitViolation;
use crate::core::{
    ActorId, Bead, BeadId, BeadPatchWireV1, BeadRef, BeadSlug, BeadType, BranchName,
    CanonicalState, ClientRequestId, DepAddKey, DepKey, DepKind, Dot, EventBody, EventBytes,
    EventKindV1, HlcMax, Label, Labels, Limits, NamespaceId, NamespacePolicy, NoteAppendV1, NoteId,
    ParentEdge, Priority, ReplicaId, Seq1, Stamp, StoreIdentity, StoreState, TraceId,
    TxnDeltaError, TxnDeltaV1, TxnId, TxnOpV1, TxnV1, ValidatedBeadPatch, ValidatedEventBody,
    ValidatedEventKindV1, ValidatedMutationCommand, ValidatedTxnV1, WallClock, WireDepAddV1,
    WireDepRemoveV1, WireDotV1, WireDvvV1, WireLabelAddV1, WireLabelRemoveV1, WireNoteV1,
    WireParentAddV1, WireParentRemoveV1, WirePatch, WireStamp, WireTombstoneV1, WorkflowStatus,
    encode_event_body_canonical, sha256_bytes, to_canon_json_bytes,
};
use crate::remote::RemoteUrl;
use crate::runtime::ipc::{
    AddNotePayload, ClaimPayload, ClosePayload, CreatePayload, DeletePayload, DepPayload,
    IdPayload, LabelsPayload, LeasePayload, ParentPayload, UpdatePayload,
};
use crate::runtime::wal::record::RECORD_HEADER_BASE_LEN;
use beads_surface::ops::{OpenInProgress, Patch};

#[derive(Clone, Debug)]
pub struct MutationContext {
    pub namespace: NamespaceId,
    pub actor_id: ActorId,
    pub client_request_id: Option<ClientRequestId>,
    pub trace_id: TraceId,
}

#[derive(Clone, Debug)]
pub struct StampedContext {
    ctx: MutationContext,
    stamp: Stamp,
}

impl StampedContext {
    pub fn new(ctx: MutationContext, stamp: Stamp) -> Result<Self, OpError> {
        if stamp.by != ctx.actor_id {
            return Err(OpError::ValidationFailed {
                field: "stamp".into(),
                reason: format!(
                    "stamp actor {} does not match context actor {}",
                    stamp.by, ctx.actor_id
                ),
            });
        }

        Ok(Self { ctx, stamp })
    }

    pub fn into_parts(self) -> (MutationContext, Stamp) {
        (self.ctx, self.stamp)
    }
}

pub trait DotAllocator {
    fn next_dot(&mut self) -> Result<DotAllocation, OpError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub enum DotDurabilityEffect {
    StoreMetaSyncBoundary,
    WalIndexTxnMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DotAllocation {
    pub dot: Dot,
    pub durability: DotDurabilityEffect,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingPlanningDurabilityEffects {
    pub dot_meta_sync_boundaries: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcknowledgedPlanningDurabilityEffects {
    pub dot_meta_sync_boundaries: u32,
}

#[derive(Clone, Debug)]
pub struct IdContext {
    pub root_slug: Option<BeadSlug>,
    pub remote_url: RemoteUrl,
}

#[derive(Clone, Debug)]
pub struct EventDraft {
    pub command: ValidatedMutationCommand,
    pub hlc_max: HlcMax,
    pub event_time_ms: u64,
    pub txn_id: TxnId,
    pub request_sha256: [u8; 32],
    pub client_request_id: Option<ClientRequestId>,
    pub trace_id: TraceId,
}

/// ```compile_fail,E0616
/// use beads_daemon::__doctest::mutation_engine::PlannedMutation;
///
/// fn bypass_ack(planned: PlannedMutation) {
///     let _ = planned.draft;
/// }
/// ```
#[must_use = "planned mutation has unacknowledged durability effects"]
#[derive(Debug)]
pub struct PlannedMutation {
    draft: EventDraft,
    durability: PendingPlanningDurabilityEffects,
}

impl PlannedMutation {
    pub fn acknowledge_durability(self) -> (EventDraft, AcknowledgedPlanningDurabilityEffects) {
        (
            self.draft,
            AcknowledgedPlanningDurabilityEffects {
                dot_meta_sync_boundaries: self.durability.dot_meta_sync_boundaries,
            },
        )
    }
}

#[derive(Clone, Debug)]
pub struct SequencedEvent {
    pub event_body: ValidatedEventBody,
    pub event_bytes: EventBytes<crate::core::Canonical>,
    pub request_sha256: [u8; 32],
    pub client_request_id: Option<ClientRequestId>,
}

#[derive(Clone, Debug)]
pub enum ParsedMutationRequest {
    Create {
        id: Option<BeadId>,
        parent: Option<String>,
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
        dependencies: Vec<String>,
    },
    Update {
        id: BeadId,
        patch: ValidatedSurfaceBeadPatch,
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
        parent: Option<String>,
    },
    Close {
        id: BeadId,
        reason: Option<String>,
        on_branch: Option<BranchName>,
    },
    Reopen {
        id: BeadId,
    },
    Delete {
        id: BeadId,
        reason: Option<String>,
    },
    AddDep {
        from_namespace: Option<NamespaceId>,
        from: BeadId,
        to_namespace: Option<NamespaceId>,
        to: BeadId,
        kind: DepKind,
    },
    RemoveDep {
        from_namespace: Option<NamespaceId>,
        from: BeadId,
        to_namespace: Option<NamespaceId>,
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
        let labels = parse_labels(labels)?;
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
        let patch = ValidatedSurfaceBeadPatch::try_from(patch)?;
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
            on_branch,
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
        let DepPayload {
            from_namespace,
            from,
            to_namespace,
            to,
            kind,
        } = payload;
        if kind == DepKind::Parent {
            return Err(OpError::ValidationFailed {
                field: "dependency".into(),
                reason: "parent edges must use set-parent".into(),
            });
        }
        Ok(ParsedMutationRequest::AddDep {
            from_namespace,
            from,
            to_namespace,
            to,
            kind,
        })
    }

    pub fn parse_remove_dep(payload: DepPayload) -> Result<Self, OpError> {
        let DepPayload {
            from_namespace,
            from,
            to_namespace,
            to,
            kind,
        } = payload;
        if kind == DepKind::Parent {
            return Err(OpError::ValidationFailed {
                field: "dependency".into(),
                reason: "parent edges must use set-parent".into(),
            });
        }
        Ok(ParsedMutationRequest::RemoveDep {
            from_namespace,
            from,
            to_namespace,
            to,
            kind,
        })
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
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn plan(
        &self,
        state: &CanonicalState,
        now_ms: u64,
        stamped: StampedContext,
        store: StoreIdentity,
        id_ctx: Option<&IdContext>,
        req: ParsedMutationRequest,
        dot_alloc: &mut dyn DotAllocator,
    ) -> Result<PlannedMutation, OpError> {
        self.plan_inner(
            state, None, None, now_ms, stamped, store, id_ctx, req, dot_alloc,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn plan_for_store(
        &self,
        store_state: &StoreState,
        namespace_policies: &BTreeMap<NamespaceId, NamespacePolicy>,
        now_ms: u64,
        stamped: StampedContext,
        store: StoreIdentity,
        id_ctx: Option<&IdContext>,
        req: ParsedMutationRequest,
        dot_alloc: &mut dyn DotAllocator,
    ) -> Result<PlannedMutation, OpError> {
        let namespace = stamped.ctx.namespace.clone();
        let state = store_state.get_or_default(&namespace);
        self.plan_inner(
            &state,
            Some(store_state),
            Some(namespace_policies),
            now_ms,
            stamped,
            store,
            id_ctx,
            req,
            dot_alloc,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_inner(
        &self,
        state: &CanonicalState,
        store_state: Option<&StoreState>,
        namespace_policies: Option<&BTreeMap<NamespaceId, NamespacePolicy>>,
        now_ms: u64,
        stamped: StampedContext,
        store: StoreIdentity,
        id_ctx: Option<&IdContext>,
        req: ParsedMutationRequest,
        dot_alloc: &mut dyn DotAllocator,
    ) -> Result<PlannedMutation, OpError> {
        let (ctx, stamp) = stamped.into_parts();
        let namespace = ctx.namespace.clone();
        let write_stamp = stamp.at.clone();
        let mut durability = PendingPlanningDurabilityEffects::default();

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
                store_state,
                namespace_policies,
                &stamp,
                &namespace,
                dot_alloc,
                &mut durability,
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
                self.plan_add_labels(state, id, labels, dot_alloc, &mut durability)?
            }
            ParsedMutationRequest::RemoveLabels { id, labels } => {
                self.plan_remove_labels(state, id, labels)?
            }
            ParsedMutationRequest::SetParent { id, parent } => self.plan_set_parent(
                state,
                store_state,
                namespace_policies,
                &namespace,
                id,
                parent,
                dot_alloc,
                &mut durability,
            )?,
            ParsedMutationRequest::Close {
                id,
                reason,
                on_branch,
            } => self.plan_close(state, id, reason, on_branch)?,
            ParsedMutationRequest::Reopen { id } => self.plan_reopen(state, id)?,
            ParsedMutationRequest::Delete { id, reason } => {
                self.plan_delete(state, &stamp, id, reason)?
            }
            ParsedMutationRequest::AddDep {
                from_namespace,
                from,
                to_namespace,
                to,
                kind,
            } => self.plan_add_dep(
                state,
                store_state,
                namespace_policies,
                &namespace,
                &stamp,
                from_namespace,
                from,
                to_namespace,
                to,
                kind,
                dot_alloc,
                &mut durability,
            )?,
            ParsedMutationRequest::RemoveDep {
                from_namespace,
                from,
                to_namespace,
                to,
                kind,
            } => self.plan_remove_dep(
                state,
                store_state,
                namespace_policies,
                &namespace,
                &stamp,
                from_namespace,
                from,
                to_namespace,
                to,
                kind,
            )?,
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

        let PlannedDelta { delta, canonical } = planned;
        self.enforce_delta_limits(&delta)?;
        let command = ValidatedMutationCommand::try_from_delta(delta).map_err(|err| {
            OpError::ValidationFailed {
                field: "event".into(),
                reason: err.to_string(),
            }
        })?;

        let request_sha256 = request_sha256(&ctx, &canonical)?;
        let MutationContext {
            actor_id,
            client_request_id,
            trace_id,
            ..
        } = ctx;

        Ok(PlannedMutation {
            draft: EventDraft {
                command,
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
            },
            durability,
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
        let EventDraft {
            command,
            hlc_max,
            event_time_ms,
            txn_id,
            request_sha256,
            client_request_id,
            trace_id,
        } = draft;
        let (delta, validated_delta) = command.into_parts();
        let event_body = EventBody {
            envelope_v: 1,
            store,
            namespace,
            origin_replica_id,
            origin_seq,
            event_time_ms,
            txn_id,
            client_request_id,
            trace_id: Some(trace_id),
            kind: EventKindV1::TxnV1(TxnV1 {
                delta,
                hlc_max: hlc_max.clone(),
            }),
        };

        let validated_kind = ValidatedEventKindV1::TxnV1(ValidatedTxnV1 {
            hlc_max,
            delta: validated_delta,
        });
        let event_body =
            ValidatedEventBody::try_from_validated(event_body, validated_kind, &self.limits)
                .map_err(|err| OpError::ValidationFailed {
                    field: "event".into(),
                    reason: err.to_string(),
                })?;

        let event_bytes = encode_event_body_canonical(&event_body)
            .map_err(|_| OpError::Internal("event_body encode failed"))?;
        self.enforce_record_size(&event_bytes, client_request_id)?;

        Ok(SequencedEvent {
            event_body,
            event_bytes,
            request_sha256,
            client_request_id,
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
                let title = title.trim().to_string();
                if title.is_empty() {
                    return Err(OpError::ValidationFailed {
                        field: "title".into(),
                        reason: "title cannot be empty".into(),
                    });
                }

                enforce_label_limit(labels, &self.limits, None)?;
                let mut canonical_deps = dependencies.clone();
                canonical_deps.sort();
                canonical_deps.dedup();
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
            ParsedMutationRequest::AddDep {
                from_namespace,
                from,
                to_namespace,
                to,
                kind,
            } => Ok(CanonicalMutationOp::AddDep {
                from_namespace: from_namespace.clone(),
                from: from.clone(),
                to_namespace: to_namespace.clone(),
                to: to.clone(),
                kind: *kind,
            }),
            ParsedMutationRequest::RemoveDep {
                from_namespace,
                from,
                to_namespace,
                to,
                kind,
            } => Ok(CanonicalMutationOp::RemoveDep {
                from_namespace: from_namespace.clone(),
                from: from.clone(),
                to_namespace: to_namespace.clone(),
                to: to.clone(),
                kind: *kind,
            }),
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
        self.limits
            .policy()
            .txn_delta(delta)
            .map_err(map_txn_limit_violation)?;
        Ok(())
    }

    fn enforce_record_size(
        &self,
        event_bytes: &EventBytes<crate::core::Canonical>,
        client_request_id: Option<ClientRequestId>,
    ) -> Result<(), OpError> {
        self.limits
            .policy()
            .wal_record_payload(event_bytes.len())
            .map_err(map_wal_payload_violation)?;
        let estimated = estimated_record_bytes(event_bytes.len(), client_request_id.is_some());
        self.limits
            .policy()
            .wal_record_len(estimated)
            .map_err(map_wal_record_violation)?;

        Ok(())
    }

    fn allocate_dot(
        &self,
        dot_alloc: &mut dyn DotAllocator,
        durability: &mut PendingPlanningDurabilityEffects,
    ) -> Result<Dot, OpError> {
        let allocation = dot_alloc.next_dot()?;
        if matches!(
            allocation.durability,
            DotDurabilityEffect::StoreMetaSyncBoundary
        ) {
            durability.dot_meta_sync_boundaries =
                durability.dot_meta_sync_boundaries.saturating_add(1);
        }
        Ok(allocation.dot)
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_create(
        &self,
        state: &CanonicalState,
        store_state: Option<&StoreState>,
        namespace_policies: Option<&BTreeMap<NamespaceId, NamespacePolicy>>,
        stamp: &Stamp,
        namespace: &NamespaceId,
        dot_alloc: &mut dyn DotAllocator,
        durability: &mut PendingPlanningDurabilityEffects,
        id_ctx: Option<&IdContext>,
        id: Option<BeadId>,
        parent: Option<String>,
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
        dependencies: Vec<String>,
    ) -> Result<PlannedDelta, OpError> {
        let requested_id = id.clone();
        let requested_parent = parent.clone();
        let parent_ref = requested_parent
            .as_deref()
            .map(|raw| parse_bead_ref("parent", raw, namespace))
            .transpose()?;

        let title = title.trim().to_string();
        if title.is_empty() {
            return Err(OpError::ValidationFailed {
                field: "title".into(),
                reason: "title cannot be empty".into(),
            });
        }

        enforce_label_limit(&labels, &self.limits, None)?;

        let dependencies = parse_create_dep_specs(&dependencies, namespace)?;
        let canonical_deps = canonical_create_deps(&dependencies, namespace);

        let description = description.unwrap_or_default();

        let id_ctx = id_ctx.ok_or_else(|| OpError::InvalidRequest {
            field: Some("id".into()),
            reason: "id context missing for create".into(),
        })?;

        let (id, parent_ref) = match (requested_id.as_ref(), parent_ref.as_ref()) {
            (Some(requested_id), parent_ref) => {
                if state.get_live(requested_id).is_some()
                    || state.get_tombstone(requested_id).is_some()
                {
                    return Err(OpError::AlreadyExists(requested_id.clone()));
                }
                if let Some(parent_ref) = parent_ref {
                    Self::require_configured_dep_namespace(
                        namespace_policies,
                        namespace,
                        parent_ref,
                    )?;
                    Self::require_live_dep_endpoint(state, store_state, namespace, parent_ref)?;
                    (requested_id.clone(), Some(parent_ref.clone()))
                } else {
                    (requested_id.clone(), None)
                }
            }
            (None, Some(parent_ref)) => {
                Self::require_configured_dep_namespace(namespace_policies, namespace, parent_ref)?;
                let child = next_child_id_for_ref(state, store_state, namespace, parent_ref)?;
                (child, Some(parent_ref.clone()))
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
        };

        for spec in dependencies.iter() {
            Self::require_configured_dep_namespace(namespace_policies, namespace, spec.to_ref())?;
            Self::require_live_dep_endpoint(state, store_state, namespace, spec.to_ref())?;
            DepKey::new(
                BeadRef::new(namespace.clone(), id.clone()),
                spec.to_ref().clone(),
                spec.kind(),
            )
            .map_err(|e| OpError::ValidationFailed {
                field: "dependency".into(),
                reason: e.to_string(),
            })?;
        }

        let mut source_repo_value = None;
        if let Some(spec) = dependencies
            .iter()
            .find(|spec| matches!(spec.kind(), DepKind::DiscoveredFrom))
            && let Some(parent_bead) =
                live_bead_for_ref(state, store_state, namespace, spec.to_ref())
            && let Some(sr) = &parent_bead.fields.source_repo.value
            && !sr.trim().is_empty()
        {
            source_repo_value = Some(sr.clone());
        }

        let mut patch = BeadPatchWireV1::new(id.clone());
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
            patch.status = Some(WorkflowStatus::InProgress);
            patch.closed_reason = WirePatch::Clear;
            patch.closed_on_branch = WirePatch::Clear;
        }

        let mut delta = TxnDeltaV1::new();
        insert_bead_upsert(&mut delta, patch)?;

        for label in labels.iter() {
            let dot = self.allocate_dot(dot_alloc, durability)?;
            delta
                .insert(TxnOpV1::LabelAdd(WireLabelAddV1 {
                    bead_id: id.clone(),
                    label: label.clone(),
                    dot: WireDotV1::from(dot),
                    lineage: Some(stamp.clone().into()),
                }))
                .map_err(delta_error_to_op)?;
        }

        if let Some(parent_ref) = parent_ref {
            let edge = ParentEdge::new(BeadRef::new(namespace.clone(), id.clone()), parent_ref)
                .map_err(|e| OpError::ValidationFailed {
                    field: "parent".into(),
                    reason: e.to_string(),
                })?;
            delta
                .insert(TxnOpV1::ParentAdd(WireParentAddV1 {
                    edge,
                    dot: WireDotV1::from(self.allocate_dot(dot_alloc, durability)?),
                }))
                .map_err(delta_error_to_op)?;
        }

        for spec in dependencies.iter() {
            let key = DepKey::new(
                BeadRef::new(namespace.clone(), id.clone()),
                spec.to_ref().clone(),
                spec.kind(),
            )
            .map_err(|e| OpError::ValidationFailed {
                field: "dependency".into(),
                reason: e.to_string(),
            })?;
            let key =
                Self::dep_add_key_into_inner(self.checked_dep_add_key(state, store_state, key)?);
            delta
                .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                    key,
                    dot: WireDotV1::from(self.allocate_dot(dot_alloc, durability)?),
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
        patch: ValidatedSurfaceBeadPatch,
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
        insert_bead_upsert(&mut delta, wire_patch)?;

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
        durability: &mut PendingPlanningDurabilityEffects,
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
            let dot = self.allocate_dot(dot_alloc, durability)?;
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
            let ctx = state.label_dvv(&id, label, &lineage);
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
        on_branch: Option<BranchName>,
    ) -> Result<PlannedDelta, OpError> {
        let bead = state.require_live(&id).map_live_err(&id)?;
        if bead.fields.workflow.value.is_closed() {
            return Err(OpError::InvalidTransition {
                from: "closed".into(),
                to: "closed".into(),
            });
        }

        let mut patch = BeadPatchWireV1::new(id.clone());
        patch.status = Some(WorkflowStatus::Closed);
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;
        if let Some(reason) = reason.clone() {
            patch.closed_reason = WirePatch::Set(reason);
        }
        if let Some(branch) = on_branch.clone() {
            patch.closed_on_branch = WirePatch::Set(branch);
        }

        let mut delta = TxnDeltaV1::new();
        insert_bead_upsert(&mut delta, patch)?;

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

        let mut patch = BeadPatchWireV1::new(id.clone());
        patch.status = Some(WorkflowStatus::Open);
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;

        let mut delta = TxnDeltaV1::new();
        insert_bead_upsert(&mut delta, patch)?;

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

    fn checked_dep_add_key(
        &self,
        state: &CanonicalState,
        store_state: Option<&StoreState>,
        key: DepKey,
    ) -> Result<DepAddKey, OpError> {
        if key.kind() == DepKind::Parent {
            return Err(OpError::ValidationFailed {
                field: "dependency".into(),
                reason: "parent edges must use set-parent".into(),
            });
        }

        match store_state {
            Some(store_state) => store_state.check_dep_add_key(key),
            None => state.check_dep_add_key(key),
        }
        .map_err(|err| OpError::ValidationFailed {
            field: "dependency".into(),
            reason: err.to_string(),
        })
    }

    fn dep_add_key_into_inner(key: DepAddKey) -> DepKey {
        match key {
            DepAddKey::Acyclic(key) => key.into_inner(),
            DepAddKey::Free(key) => key.into_inner(),
        }
    }

    fn dep_ref(
        active_namespace: &NamespaceId,
        endpoint_namespace: Option<NamespaceId>,
        id: BeadId,
    ) -> BeadRef {
        BeadRef::new(
            endpoint_namespace.unwrap_or_else(|| active_namespace.clone()),
            id,
        )
    }

    fn require_configured_dep_namespace(
        namespace_policies: Option<&BTreeMap<NamespaceId, NamespacePolicy>>,
        active_namespace: &NamespaceId,
        bead_ref: &BeadRef,
    ) -> Result<(), OpError> {
        match namespace_policies {
            Some(policies) if !policies.contains_key(bead_ref.namespace()) => {
                Err(OpError::NamespaceUnknown {
                    namespace: bead_ref.namespace().clone(),
                })
            }
            None if bead_ref.namespace() != active_namespace => Err(OpError::NamespaceUnknown {
                namespace: bead_ref.namespace().clone(),
            }),
            _ => Ok(()),
        }
    }

    fn require_dep_owner_namespace(
        active_namespace: &NamespaceId,
        from_ref: &BeadRef,
    ) -> Result<(), OpError> {
        if from_ref.namespace() == active_namespace {
            return Ok(());
        }
        Err(OpError::ValidationFailed {
            field: "dependency".into(),
            reason: format!(
                "dependency source namespace {} must match mutation namespace {}",
                from_ref.namespace(),
                active_namespace
            ),
        })
    }

    fn require_live_dep_endpoint(
        state: &CanonicalState,
        store_state: Option<&StoreState>,
        active_namespace: &NamespaceId,
        bead_ref: &BeadRef,
    ) -> Result<(), OpError> {
        if let Some(store_state) = store_state {
            let Some(endpoint_state) = store_state.get(bead_ref.namespace()) else {
                return Err(OpError::NotFound(bead_ref.id().clone()));
            };
            endpoint_state
                .require_live(bead_ref.id())
                .map_live_err(bead_ref.id())?;
            return Ok(());
        }

        if bead_ref.namespace() != active_namespace {
            return Err(OpError::NamespaceUnknown {
                namespace: bead_ref.namespace().clone(),
            });
        }
        state
            .require_live(bead_ref.id())
            .map_live_err(bead_ref.id())?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_add_dep(
        &self,
        state: &CanonicalState,
        store_state: Option<&StoreState>,
        namespace_policies: Option<&BTreeMap<NamespaceId, NamespacePolicy>>,
        namespace: &NamespaceId,
        _stamp: &Stamp,
        from_namespace: Option<NamespaceId>,
        from: BeadId,
        to_namespace: Option<NamespaceId>,
        to: BeadId,
        kind: DepKind,
        dot_alloc: &mut dyn DotAllocator,
        durability: &mut PendingPlanningDurabilityEffects,
    ) -> Result<PlannedDelta, OpError> {
        let from_ref = Self::dep_ref(namespace, from_namespace.clone(), from.clone());
        let to_ref = Self::dep_ref(namespace, to_namespace.clone(), to.clone());
        Self::require_configured_dep_namespace(namespace_policies, namespace, &from_ref)?;
        Self::require_configured_dep_namespace(namespace_policies, namespace, &to_ref)?;
        Self::require_dep_owner_namespace(namespace, &from_ref)?;
        Self::require_live_dep_endpoint(state, store_state, namespace, &from_ref)?;
        Self::require_live_dep_endpoint(state, store_state, namespace, &to_ref)?;

        let key = DepKey::new(from_ref, to_ref, kind).map_err(|e| OpError::ValidationFailed {
            field: "dependency".into(),
            reason: e.to_string(),
        })?;
        let key =
            Self::dep_add_key_into_inner(self.checked_dep_add_key(state, store_state, key)?);

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepAdd(WireDepAddV1 {
                key: key.clone(),
                dot: WireDotV1::from(self.allocate_dot(dot_alloc, durability)?),
            }))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::AddDep {
            from_namespace,
            from,
            to_namespace,
            to,
            kind,
        };

        Ok(PlannedDelta { delta, canonical })
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_remove_dep(
        &self,
        state: &CanonicalState,
        _store_state: Option<&StoreState>,
        namespace_policies: Option<&BTreeMap<NamespaceId, NamespacePolicy>>,
        namespace: &NamespaceId,
        _stamp: &Stamp,
        from_namespace: Option<NamespaceId>,
        from: BeadId,
        to_namespace: Option<NamespaceId>,
        to: BeadId,
        kind: DepKind,
    ) -> Result<PlannedDelta, OpError> {
        if kind == DepKind::Parent {
            return Err(OpError::ValidationFailed {
                field: "dependency".into(),
                reason: "parent edges must use set-parent".into(),
            });
        }
        let from_ref = Self::dep_ref(namespace, from_namespace.clone(), from.clone());
        let to_ref = Self::dep_ref(namespace, to_namespace.clone(), to.clone());
        Self::require_configured_dep_namespace(namespace_policies, namespace, &from_ref)?;
        Self::require_configured_dep_namespace(namespace_policies, namespace, &to_ref)?;
        Self::require_dep_owner_namespace(namespace, &from_ref)?;
        let key = DepKey::new(from_ref, to_ref, kind).map_err(|e| OpError::ValidationFailed {
            field: "dependency".into(),
            reason: e.to_string(),
        })?;

        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::DepRemove(WireDepRemoveV1 {
                key: key.clone(),
                ctx: WireDvvV1::from(&state.dep_dvv(&key)),
            }))
            .map_err(delta_error_to_op)?;

        let canonical = CanonicalMutationOp::RemoveDep {
            from_namespace,
            from,
            to_namespace,
            to,
            kind,
        };

        Ok(PlannedDelta { delta, canonical })
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_set_parent(
        &self,
        state: &CanonicalState,
        store_state: Option<&StoreState>,
        namespace_policies: Option<&BTreeMap<NamespaceId, NamespacePolicy>>,
        namespace: &NamespaceId,
        id: BeadId,
        parent: Option<String>,
        dot_alloc: &mut dyn DotAllocator,
        durability: &mut PendingPlanningDurabilityEffects,
    ) -> Result<PlannedDelta, OpError> {
        state.require_live(&id).map_live_err(&id)?;

        let child_ref = BeadRef::new(namespace.clone(), id.clone());
        let parent_edge = match parent.as_deref() {
            Some(parent_raw) => {
                let parent_ref = parse_bead_ref("parent", parent_raw, namespace)?;
                Self::require_configured_dep_namespace(namespace_policies, namespace, &parent_ref)?;
                Self::require_live_dep_endpoint(state, store_state, namespace, &parent_ref)?;
                let edge = ParentEdge::new(child_ref.clone(), parent_ref.clone()).map_err(|e| {
                    OpError::ValidationFailed {
                        field: "parent".into(),
                        reason: e.to_string(),
                    }
                })?;
                let raw_key = edge.to_dep_key();
                match store_state {
                    Some(store_state) => store_state.check_dep_add_key(raw_key),
                    None => state.check_dep_add_key(raw_key),
                }
                .map_err(|err| OpError::ValidationFailed {
                    field: "parent".into(),
                    reason: err.to_string(),
                })?;
                Some(edge)
            }
            None => None,
        };

        let existing_parents = state.parent_edges_from(&id);

        let mut delta = TxnDeltaV1::new();
        for existing_parent in existing_parents {
            let key = existing_parent.to_dep_key();
            delta
                .insert(TxnOpV1::ParentRemove(WireParentRemoveV1 {
                    edge: existing_parent,
                    ctx: WireDvvV1::from(&state.dep_dvv(&key)),
                }))
                .map_err(delta_error_to_op)?;
        }

        if let Some(parent_edge) = parent_edge.clone() {
            delta
                .insert(TxnOpV1::ParentAdd(WireParentAddV1 {
                    edge: parent_edge,
                    dot: WireDotV1::from(self.allocate_dot(dot_alloc, durability)?),
                }))
                .map_err(delta_error_to_op)?;
        }

        let canonical = CanonicalMutationOp::SetParent {
            id,
            parent: parent_edge.map(|edge| {
                if edge.parent_ref().namespace() == namespace {
                    edge.parent_ref().id().as_str().to_string()
                } else {
                    edge.parent_ref().to_string()
                }
            }),
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

        let mut patch = BeadPatchWireV1::new(id.clone());
        patch.assignee = WirePatch::Set(stamp.by.clone());
        patch.assignee_expires = WirePatch::Set(expires);
        patch.status = Some(WorkflowStatus::InProgress);
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;

        let mut delta = TxnDeltaV1::new();
        insert_bead_upsert(&mut delta, patch)?;

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

        let mut patch = BeadPatchWireV1::new(id.clone());
        patch.assignee = WirePatch::Clear;
        patch.assignee_expires = WirePatch::Clear;
        patch.status = Some(WorkflowStatus::Open);
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;

        let mut delta = TxnDeltaV1::new();
        insert_bead_upsert(&mut delta, patch)?;

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

        let mut patch = BeadPatchWireV1::new(id.clone());
        patch.assignee = WirePatch::Set(assignee);
        patch.assignee_expires = WirePatch::Set(expires);
        patch.status = Some(WorkflowStatus::InProgress);
        patch.closed_reason = WirePatch::Clear;
        patch.closed_on_branch = WirePatch::Clear;

        let mut delta = TxnDeltaV1::new();
        insert_bead_upsert(&mut delta, patch)?;

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
        parent: Option<String>,
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
        on_branch: Option<BranchName>,
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
        parent: Option<String>,
    },
    AddDep {
        #[serde(skip_serializing_if = "Option::is_none")]
        from_namespace: Option<NamespaceId>,
        from: BeadId,
        #[serde(skip_serializing_if = "Option::is_none")]
        to_namespace: Option<NamespaceId>,
        to: BeadId,
        kind: DepKind,
    },
    RemoveDep {
        #[serde(skip_serializing_if = "Option::is_none")]
        from_namespace: Option<NamespaceId>,
        from: BeadId,
        #[serde(skip_serializing_if = "Option::is_none")]
        to_namespace: Option<NamespaceId>,
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
    status: Patch<OpenInProgress>,
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

#[derive(Clone, Debug)]
struct CreateDepSpec {
    kind: DepKind,
    to: BeadRef,
}

impl CreateDepSpec {
    fn kind(&self) -> DepKind {
        self.kind
    }

    fn to_ref(&self) -> &BeadRef {
        &self.to
    }

    fn to_spec_string(&self, default_namespace: &NamespaceId) -> String {
        let id = if self.to.namespace() == default_namespace {
            self.to.id().as_str().to_string()
        } else {
            self.to.to_string()
        };
        if self.kind == DepKind::Blocks {
            id
        } else {
            format!("{}:{id}", self.kind.as_str())
        }
    }
}

fn parse_bead_ref(
    field: &'static str,
    raw: &str,
    default_namespace: &NamespaceId,
) -> Result<BeadRef, OpError> {
    BeadRef::parse(raw, default_namespace).map_err(|err| OpError::ValidationFailed {
        field: field.into(),
        reason: err.to_string(),
    })
}

fn parse_create_dep_specs(
    deps: &[String],
    default_namespace: &NamespaceId,
) -> Result<Vec<CreateDepSpec>, OpError> {
    let mut out = Vec::new();
    for raw in deps {
        let trimmed = raw.trim();
        let (kind, id_raw) = if let Some((kind_raw, id_raw)) = trimmed.split_once(':') {
            (
                crate::core::ValidatedDepKind::parse(kind_raw)
                    .map_err(|err| OpError::ValidationFailed {
                        field: "dependencies".into(),
                        reason: err.to_string(),
                    })?
                    .into_inner(),
                id_raw.trim(),
            )
        } else {
            (DepKind::Blocks, trimmed)
        };
        if kind == DepKind::Parent {
            return Err(OpError::ValidationFailed {
                field: "dependencies".into(),
                reason: "parent edges must use set-parent".into(),
            });
        }
        out.push(CreateDepSpec {
            kind,
            to: parse_bead_ref("dependencies", id_raw, default_namespace)?,
        });
    }
    out.sort_by(|a, b| a.kind.cmp(&b.kind).then_with(|| a.to.cmp(&b.to)));
    out.dedup_by(|a, b| a.kind == b.kind && a.to == b.to);
    Ok(out)
}

fn canonical_labels(labels: &Labels) -> Vec<String> {
    labels
        .iter()
        .map(|label| label.as_str().to_string())
        .collect()
}

fn canonical_create_deps(deps: &[CreateDepSpec], default_namespace: &NamespaceId) -> Vec<String> {
    deps.iter()
        .map(|spec| spec.to_spec_string(default_namespace))
        .collect()
}

fn live_bead_for_ref<'a>(
    state: &'a CanonicalState,
    store_state: Option<&'a StoreState>,
    active_namespace: &NamespaceId,
    bead_ref: &BeadRef,
) -> Option<&'a Bead> {
    if let Some(store_state) = store_state {
        return store_state.resolve_ref(bead_ref);
    }
    if bead_ref.namespace() != active_namespace {
        return None;
    }
    state.get_live(bead_ref.id())
}

fn next_child_id_for_ref(
    state: &CanonicalState,
    store_state: Option<&StoreState>,
    active_namespace: &NamespaceId,
    parent: &BeadRef,
) -> Result<BeadId, OpError> {
    if live_bead_for_ref(state, store_state, active_namespace, parent).is_none() {
        return Err(OpError::NotFound(parent.id().clone()));
    }
    next_child_id_from_active_state(state, parent.id())
}

fn next_child_id_from_active_state(
    state: &CanonicalState,
    parent: &BeadId,
) -> Result<BeadId, OpError> {
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
    patch: &ValidatedSurfaceBeadPatch,
) -> Result<(BeadPatchWireV1, CanonicalBeadPatch), OpError> {
    let mut wire = BeadPatchWireV1::new(id.clone());

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
        wire.status = Some((*status).into());
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
    limits
        .policy()
        .labels(labels)
        .map(|_| ())
        .map_err(|err| map_label_limit_violation(err, bead_id))
}

fn enforce_note_limit(content: &str, limits: &Limits) -> Result<(), OpError> {
    limits
        .policy()
        .note_content(content)
        .map(|_| ())
        .map_err(map_note_limit_violation)
}

fn map_txn_limit_violation(err: LimitViolation) -> OpError {
    match err {
        LimitViolation::OpsTooMany { max_ops, got_ops } => OpError::OpsTooMany { max_ops, got_ops },
        LimitViolation::NoteTooLarge {
            max_bytes,
            got_bytes,
        } => OpError::NoteTooLarge {
            max_bytes,
            got_bytes,
        },
        LimitViolation::NoteAppendsTooMany { max, got } => OpError::InvalidRequest {
            field: Some("note_appends".into()),
            reason: format!("note_appends {got} exceeds max {max}"),
        },
        other => OpError::ValidationFailed {
            field: "delta_limits".into(),
            reason: other.to_string(),
        },
    }
}

fn map_label_limit_violation(err: LimitViolation, bead_id: Option<BeadId>) -> OpError {
    match err {
        LimitViolation::LabelsTooMany { max, got } => OpError::LabelsTooMany {
            max_labels: max,
            got_labels: got,
            bead_id,
        },
        other => OpError::ValidationFailed {
            field: "labels".into(),
            reason: other.to_string(),
        },
    }
}

fn map_note_limit_violation(err: LimitViolation) -> OpError {
    match err {
        LimitViolation::NoteTooLarge {
            max_bytes,
            got_bytes,
        } => OpError::NoteTooLarge {
            max_bytes,
            got_bytes,
        },
        other => OpError::ValidationFailed {
            field: "note".into(),
            reason: other.to_string(),
        },
    }
}

fn map_wal_payload_violation(err: LimitViolation) -> OpError {
    match err {
        LimitViolation::WalRecordPayloadTooLarge {
            max_bytes,
            got_bytes,
        } => OpError::WalRecordTooLarge {
            max_wal_record_bytes: max_bytes,
            estimated_bytes: got_bytes,
        },
        other => OpError::ValidationFailed {
            field: "wal_record_payload".into(),
            reason: other.to_string(),
        },
    }
}

fn map_wal_record_violation(err: LimitViolation) -> OpError {
    match err {
        LimitViolation::WalRecordTooLarge {
            max_bytes,
            got_bytes,
        } => OpError::WalRecordTooLarge {
            max_wal_record_bytes: max_bytes,
            estimated_bytes: got_bytes,
        },
        other => OpError::ValidationFailed {
            field: "wal_record".into(),
            reason: other.to_string(),
        },
    }
}

fn validate_bead_patch(patch: BeadPatchWireV1) -> Result<BeadPatchWireV1, OpError> {
    ValidatedBeadPatch::try_from(patch)
        .map(|patch| patch.into_inner())
        .map_err(|err| OpError::ValidationFailed {
            field: "bead_patch".into(),
            reason: err.to_string(),
        })
}

fn insert_bead_upsert(delta: &mut TxnDeltaV1, patch: BeadPatchWireV1) -> Result<(), OpError> {
    let patch = validate_bead_patch(patch)?;
    delta
        .insert(TxnOpV1::BeadUpsert(Box::new(patch)))
        .map_err(delta_error_to_op)
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
        Bead, BeadCore, BeadFields, Claim, Dot, Lww, ReplicaId, Stamp, StoreId, Tombstone,
        Workflow, WriteStamp,
    };
    use beads_surface::ops::BeadPatch;

    fn actor_id(actor: &str) -> ActorId {
        ActorId::new(actor).unwrap()
    }

    fn make_state_with_bead(id: &str, actor: &ActorId) -> CanonicalState {
        let bead = make_bead(id, actor);
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();
        state
    }

    fn make_bead(id: &str, actor: &ActorId) -> Bead {
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
        Bead::new(core, fields)
    }

    fn make_stamp(now_ms: u64, actor: &ActorId) -> Stamp {
        Stamp::new(WriteStamp::new(now_ms, 0), actor.clone())
    }

    fn make_stamped_context(ctx: MutationContext, stamp: Stamp) -> StampedContext {
        StampedContext::new(ctx, stamp).unwrap()
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
        fn next_dot(&mut self) -> Result<DotAllocation, OpError> {
            self.counter += 1;
            Ok(DotAllocation {
                dot: Dot {
                    replica: self.replica_id,
                    counter: self.counter,
                },
                durability: DotDurabilityEffect::StoreMetaSyncBoundary,
            })
        }
    }

    fn store_identity(seed: u8) -> StoreIdentity {
        StoreIdentity::new(StoreId::new(Uuid::from_bytes([seed; 16])), 0.into())
    }

    fn namespace_policies(
        extra_namespaces: &[NamespaceId],
    ) -> BTreeMap<NamespaceId, NamespacePolicy> {
        let mut policies = BTreeMap::new();
        policies.insert(NamespaceId::core(), NamespacePolicy::core_default());
        for namespace in extra_namespaces {
            policies.insert(namespace.clone(), NamespacePolicy::core_default());
        }
        policies
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
        let stamped_a = make_stamped_context(ctx.clone(), stamp_a);
        let stamped_b = make_stamped_context(ctx, stamp_b);
        let replica_id = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
        let mut dots_a = TestDotAllocator::new(replica_id);
        let mut dots_b = TestDotAllocator::new(replica_id);

        let planned_a = engine
            .plan(&state, now_ms, stamped_a, store, None, req_a, &mut dots_a)
            .unwrap();
        let planned_b = engine
            .plan(&state, now_ms, stamped_b, store, None, req_b, &mut dots_b)
            .unwrap();
        let (draft_a, effects_a) = planned_a.acknowledge_durability();
        let (draft_b, effects_b) = planned_b.acknowledge_durability();

        assert_eq!(draft_a.request_sha256, draft_b.request_sha256);
        assert_eq!(draft_a.command.raw_delta(), draft_b.command.raw_delta());
        assert_eq!(effects_a.dot_meta_sync_boundaries, 2);
        assert_eq!(effects_b.dot_meta_sync_boundaries, 2);
    }

    #[test]
    fn plan_requires_durability_ack_for_dot_allocating_mutations() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([6u8; 16])),
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([1u8; 16])), 0.into());
        let state = make_state_with_bead("bd-123", &actor);
        let req = ParsedMutationRequest::parse_add_labels(LabelsPayload {
            id: BeadId::parse("bd-123").expect("bead id"),
            labels: vec!["a".into()],
        })
        .expect("request");
        let now_ms = 1_000;
        let stamped = make_stamped_context(ctx, make_stamp(now_ms, &actor));
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([4u8; 16])));

        let planned = engine
            .plan(&state, now_ms, stamped, store, None, req, &mut dots)
            .expect("plan");
        let (_draft, effects) = planned.acknowledge_durability();
        assert_eq!(effects.dot_meta_sync_boundaries, 1);
    }

    #[test]
    fn create_with_assignee_sets_in_progress() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([7u8; 16])),
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([1u8; 16])), 0.into());
        let state = CanonicalState::new();
        let stamp = make_stamp(1_000, &actor);
        let stamped = make_stamped_context(ctx, stamp);
        let id_ctx = IdContext {
            root_slug: None,
            remote_url: RemoteUrl::new("github.com/example/beads"),
        };
        let req = ParsedMutationRequest::parse_create(
            CreatePayload {
                id: Some(BeadId::parse("bd-claim").unwrap()),
                parent: None,
                title: "t".into(),
                bead_type: BeadType::Task,
                priority: Priority::default(),
                description: Some("d".into()),
                design: None,
                acceptance_criteria: None,
                assignee: Some(actor.as_str().to_string()),
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            },
            &actor,
        )
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan(&state, 1_000, stamped, store, Some(&id_ctx), req, &mut dots)
            .unwrap();
        let (draft, effects) = planned.acknowledge_durability();
        let patch = draft
            .command
            .raw_delta()
            .iter()
            .find_map(|op| match op {
                TxnOpV1::BeadUpsert(patch) => Some(patch.as_ref()),
                _ => None,
            })
            .expect("bead upsert");
        assert_eq!(effects.dot_meta_sync_boundaries, 0);

        assert_eq!(patch.status, Some(WorkflowStatus::InProgress));
        assert!(matches!(patch.assignee, WirePatch::Set(_)));
        assert!(matches!(patch.closed_reason, WirePatch::Clear));
        assert!(matches!(patch.closed_on_branch, WirePatch::Clear));
    }

    #[test]
    fn create_with_explicit_id_and_parent_plans_parent_edge() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let parent_id = BeadId::parse("bd-parent").unwrap();
        let child_id = BeadId::parse("bd-child").unwrap();
        let state = make_state_with_bead(parent_id.as_str(), &actor);
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([8u8; 16])),
        };
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([1u8; 16])), 0.into());
        let id_ctx = IdContext {
            root_slug: None,
            remote_url: RemoteUrl::new("github.com/example/beads"),
        };
        let req = ParsedMutationRequest::parse_create(
            CreatePayload {
                id: Some(child_id.clone()),
                parent: Some(parent_id.as_str().to_string()),
                title: "child".into(),
                bead_type: BeadType::Task,
                priority: Priority::default(),
                description: Some("d".into()),
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            },
            &actor,
        )
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan(
                &state,
                1_000,
                make_stamped_context(ctx, make_stamp(1_000, &actor)),
                store,
                Some(&id_ctx),
                req,
                &mut dots,
            )
            .unwrap();
        let (draft, effects) = planned.acknowledge_durability();
        let mut created_id = None;
        let mut parent_edge = None;
        for op in draft.command.raw_delta().iter() {
            match op {
                TxnOpV1::BeadUpsert(patch) => created_id = Some(patch.id.clone()),
                TxnOpV1::ParentAdd(add) => parent_edge = Some(add.edge.clone()),
                _ => {}
            }
        }

        assert_eq!(created_id.as_ref(), Some(&child_id));
        let parent_edge = parent_edge.expect("parent edge");
        assert_eq!(parent_edge.child(), &child_id);
        assert_eq!(parent_edge.parent(), &parent_id);
        assert_eq!(effects.dot_meta_sync_boundaries, 1);
    }

    #[test]
    fn create_accepts_namespaced_parent_and_dependency_refs() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let sessions = NamespaceId::parse("sessions").unwrap();
        let parent_id = BeadId::parse("bd-parent").unwrap();
        let dep_id = BeadId::parse("bd-dep").unwrap();
        let child_id = BeadId::parse("bd-child").unwrap();

        let mut store_state = StoreState::new();
        store_state
            .core_mut()
            .insert(make_bead(parent_id.as_str(), &actor))
            .unwrap();
        store_state
            .core_mut()
            .insert(make_bead(dep_id.as_str(), &actor))
            .unwrap();
        store_state.ensure_namespace(sessions.clone());
        let policies = namespace_policies(std::slice::from_ref(&sessions));

        let ctx = MutationContext {
            namespace: sessions.clone(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([8u8; 16])),
        };
        let id_ctx = IdContext {
            root_slug: None,
            remote_url: RemoteUrl::new("github.com/example/beads"),
        };
        let req = ParsedMutationRequest::parse_create(
            CreatePayload {
                id: Some(child_id.clone()),
                parent: Some(format!("core/{parent_id}")),
                title: "child".into(),
                bead_type: BeadType::Task,
                priority: Priority::default(),
                description: Some("d".into()),
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: vec![format!("core/{dep_id}")],
            },
            &actor,
        )
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan_for_store(
                &store_state,
                &policies,
                1_000,
                make_stamped_context(ctx, make_stamp(1_000, &actor)),
                store_identity(1),
                Some(&id_ctx),
                req,
                &mut dots,
            )
            .unwrap();
        let (draft, effects) = planned.acknowledge_durability();
        let mut parent_edge = None;
        let mut dep_key = None;
        for op in draft.command.raw_delta().iter() {
            match op {
                TxnOpV1::ParentAdd(add) => parent_edge = Some(add.edge.clone()),
                TxnOpV1::DepAdd(add) => dep_key = Some(add.key.clone()),
                _ => {}
            }
        }

        let child_ref = BeadRef::new(sessions.clone(), child_id);
        assert_eq!(
            parent_edge.expect("parent edge").parent_ref(),
            &BeadRef::new(NamespaceId::core(), parent_id)
        );
        assert_eq!(
            dep_key.expect("dep key").to_ref(),
            &BeadRef::new(NamespaceId::core(), dep_id)
        );
        assert_eq!(effects.dot_meta_sync_boundaries, 2);
        assert!(draft.command.raw_delta().iter().any(|op| {
            matches!(op, TxnOpV1::ParentAdd(add) if add.edge.child_ref() == &child_ref)
        }));
    }

    #[test]
    fn create_rejects_parent_namespace_missing_from_policy() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let stale = NamespaceId::parse("stale").unwrap();
        let parent_id = BeadId::parse("bd-parent").unwrap();
        let child_id = BeadId::parse("bd-child").unwrap();

        let mut store_state = StoreState::new();
        store_state
            .ensure_namespace(stale.clone())
            .insert(make_bead(parent_id.as_str(), &actor))
            .unwrap();
        let policies = namespace_policies(&[]);

        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([8u8; 16])),
        };
        let req = ParsedMutationRequest::parse_create(
            CreatePayload {
                id: Some(child_id),
                parent: Some(format!("{stale}/{parent_id}")),
                title: "child".into(),
                bead_type: BeadType::Task,
                priority: Priority::default(),
                description: Some("d".into()),
                design: None,
                acceptance_criteria: None,
                assignee: None,
                external_ref: None,
                estimated_minutes: None,
                labels: Vec::new(),
                dependencies: Vec::new(),
            },
            &actor,
        )
        .unwrap();
        let id_ctx = IdContext {
            root_slug: None,
            remote_url: RemoteUrl::new("github.com/example/beads"),
        };
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let err = engine
            .plan_for_store(
                &store_state,
                &policies,
                1_000,
                make_stamped_context(ctx, make_stamp(1_000, &actor)),
                store_identity(1),
                Some(&id_ctx),
                req,
                &mut dots,
            )
            .unwrap_err();

        assert!(matches!(err, OpError::NamespaceUnknown { namespace } if namespace == stale));
    }

    #[test]
    fn extend_claim_sets_in_progress() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let bead_id = BeadId::parse("bd-claim").unwrap();
        let stamp = make_stamp(10, &actor);
        let mut bead = make_bead(bead_id.as_str(), &actor);
        bead.fields.claim = Lww::new(
            Claim::claimed(actor.clone(), Some(WallClock(20))),
            stamp.clone(),
        );
        bead.fields.workflow = Lww::new(Workflow::Open, stamp.clone());
        let mut state = CanonicalState::new();
        state.insert(bead).unwrap();

        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let stamped = make_stamped_context(ctx, stamp);
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([2u8; 16])), 0.into());
        let req = ParsedMutationRequest::ExtendClaim {
            id: bead_id,
            lease_secs: 30,
        };
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([4u8; 16])));

        let planned = engine
            .plan(&state, 10, stamped, store, None, req, &mut dots)
            .unwrap();
        let (draft, effects) = planned.acknowledge_durability();
        let patch = draft
            .command
            .raw_delta()
            .iter()
            .find_map(|op| match op {
                TxnOpV1::BeadUpsert(patch) => Some(patch.as_ref()),
                _ => None,
            })
            .expect("bead upsert");
        assert_eq!(effects.dot_meta_sync_boundaries, 0);

        assert_eq!(patch.status, Some(WorkflowStatus::InProgress));
        assert!(matches!(patch.assignee, WirePatch::Set(_)));
        assert!(matches!(patch.closed_reason, WirePatch::Clear));
        assert!(matches!(patch.closed_on_branch, WirePatch::Clear));
    }

    #[test]
    fn invalid_workflow_patch_rejected_in_planning() {
        let mut patch = BeadPatchWireV1::new(BeadId::parse("bd-invalid").unwrap());
        patch.status = Some(WorkflowStatus::Closed);

        let err = validate_bead_patch(patch).unwrap_err();
        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "bead_patch"
        ));
    }

    #[test]
    fn update_rejects_clearing_required_fields() {
        let mut patch = BeadPatch::default();
        patch.title = Patch::Clear;

        let err = ParsedMutationRequest::parse_update(UpdatePayload {
            id: BeadId::parse("bd-required").unwrap(),
            patch,
            cas: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "title"
        ));

        let mut patch = BeadPatch::default();
        patch.description = Patch::Clear;

        let err = ParsedMutationRequest::parse_update(UpdatePayload {
            id: BeadId::parse("bd-required").unwrap(),
            patch,
            cas: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "description"
        ));
    }

    #[test]
    fn update_rejects_empty_required_fields() {
        let mut patch = BeadPatch::default();
        patch.title = Patch::Set("   ".into());

        let err = ParsedMutationRequest::parse_update(UpdatePayload {
            id: BeadId::parse("bd-required").unwrap(),
            patch,
            cas: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "title"
        ));

        let mut patch = BeadPatch::default();
        patch.description = Patch::Set("".into());

        let err = ParsedMutationRequest::parse_update(UpdatePayload {
            id: BeadId::parse("bd-required").unwrap(),
            patch,
            cas: None,
        })
        .unwrap_err();
        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "description"
        ));
    }

    #[test]
    fn update_trims_required_fields() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let bead_id = BeadId::parse("bd-trim").unwrap();
        let state = make_state_with_bead(bead_id.as_str(), &actor);
        let stamp = make_stamp(10, &actor);
        let stamped = make_stamped_context(
            MutationContext {
                namespace: NamespaceId::core(),
                actor_id: actor.clone(),
                client_request_id: None,
                trace_id: TraceId::new(Uuid::from_bytes([5u8; 16])),
            },
            stamp,
        );
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([2u8; 16])), 0.into());
        let mut patch = BeadPatch::default();
        patch.title = Patch::Set("  title  ".into());
        patch.description = Patch::Set("  desc  ".into());
        let req = ParsedMutationRequest::parse_update(UpdatePayload {
            id: bead_id.clone(),
            patch,
            cas: None,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([7u8; 16])));

        let planned = engine
            .plan(&state, 10, stamped, store, None, req, &mut dots)
            .unwrap();
        let (draft, effects) = planned.acknowledge_durability();
        let patch = draft
            .command
            .raw_delta()
            .iter()
            .find_map(|op| match op {
                TxnOpV1::BeadUpsert(patch) => Some(patch.as_ref()),
                _ => None,
            })
            .expect("bead upsert");

        assert_eq!(patch.title.as_deref(), Some("title"));
        assert_eq!(patch.description.as_deref(), Some("desc"));
        assert_eq!(effects.dot_meta_sync_boundaries, 0);
    }

    #[test]
    fn stamped_context_rejects_mismatched_actor() {
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor_id("alice"),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let stamp = Stamp::new(WriteStamp::new(10, 0), actor_id("bob"));
        let err = StampedContext::new(ctx, stamp).unwrap_err();

        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "stamp"
        ));
    }

    #[test]
    fn add_dep_rejects_parent_kind() {
        let err = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: None,
            from: BeadId::parse("bd-child").unwrap(),
            to_namespace: None,
            to: BeadId::parse("bd-parent").unwrap(),
            kind: DepKind::Parent,
        })
        .unwrap_err();

        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "dependency"
        ));
    }

    #[test]
    fn add_dep_rejects_blocks_cycle() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let bead_a = BeadId::parse("bd-a").unwrap();
        let bead_b = BeadId::parse("bd-b").unwrap();

        let mut state = CanonicalState::new();
        state.insert(make_bead(bead_a.as_str(), &actor)).unwrap();
        state.insert(make_bead(bead_b.as_str(), &actor)).unwrap();

        let stamp = make_stamp(10, &actor);
        let dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let key = DepKey::new_local(
            &NamespaceId::core(),
            bead_a.clone(),
            bead_b.clone(),
            DepKind::Blocks,
        )
        .unwrap();
        let key = state.check_dep_add_key(key).expect("acyclic key");
        state.apply_dep_add(key, dot, stamp.clone());

        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let stamped = make_stamped_context(ctx, stamp.clone());
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([2u8; 16])), 0.into());
        let req = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: None,
            from: bead_b.clone(),
            to_namespace: None,
            to: bead_a.clone(),
            kind: DepKind::Blocks,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let err = engine
            .plan(
                &state,
                stamp.at.wall_ms,
                stamped,
                store,
                None,
                req,
                &mut dots,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "dependency"
        ));
    }

    #[test]
    fn add_dep_allows_related_cycle() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let bead_a = BeadId::parse("bd-a").unwrap();
        let bead_b = BeadId::parse("bd-b").unwrap();

        let mut state = CanonicalState::new();
        state.insert(make_bead(bead_a.as_str(), &actor)).unwrap();
        state.insert(make_bead(bead_b.as_str(), &actor)).unwrap();

        let stamp = make_stamp(10, &actor);
        let dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let key = DepKey::new_local(
            &NamespaceId::core(),
            bead_a.clone(),
            bead_b.clone(),
            DepKind::Related,
        )
        .unwrap();
        let key = state.check_dep_add_key(key).expect("free key");
        state.apply_dep_add(key, dot, stamp.clone());

        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let stamped = make_stamped_context(ctx, stamp.clone());
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([2u8; 16])), 0.into());
        let req = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: None,
            from: bead_b.clone(),
            to_namespace: None,
            to: bead_a.clone(),
            kind: DepKind::Related,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan(
                &state,
                stamp.at.wall_ms,
                stamped,
                store,
                None,
                req,
                &mut dots,
            )
            .expect("plan related dep");
        let (draft, effects) = planned.acknowledge_durability();

        assert!(
            draft
                .command
                .raw_delta()
                .iter()
                .any(|op| matches!(op, TxnOpV1::DepAdd(_)))
        );
        assert_eq!(effects.dot_meta_sync_boundaries, 1);
    }

    #[test]
    fn add_dep_bare_endpoints_default_to_active_core_namespace() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let from = BeadId::parse("bd-from").unwrap();
        let to = BeadId::parse("bd-to").unwrap();
        let mut state = CanonicalState::new();
        state.insert(make_bead(from.as_str(), &actor)).unwrap();
        state.insert(make_bead(to.as_str(), &actor)).unwrap();
        let stamp = make_stamp(10, &actor);
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let req = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: None,
            from: from.clone(),
            to_namespace: None,
            to: to.clone(),
            kind: DepKind::Blocks,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan(
                &state,
                stamp.at.wall_ms,
                make_stamped_context(ctx, stamp),
                store_identity(2),
                None,
                req,
                &mut dots,
            )
            .expect("plan core dep");
        let (draft, _) = planned.acknowledge_durability();
        let add = draft
            .command
            .raw_delta()
            .iter()
            .find_map(|op| match op {
                TxnOpV1::DepAdd(add) => Some(add),
                _ => None,
            })
            .expect("dep add");

        assert_eq!(add.key.from_ref(), &BeadRef::new(NamespaceId::core(), from));
        assert_eq!(add.key.to_ref(), &BeadRef::new(NamespaceId::core(), to));
    }

    #[test]
    fn add_dep_accepts_namespaced_endpoint_in_configured_namespace() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let sessions = NamespaceId::parse("sessions").unwrap();
        let session_id = BeadId::parse("bd-session").unwrap();
        let core_id = BeadId::parse("bd-core").unwrap();

        let mut core_state = CanonicalState::new();
        core_state
            .insert(make_bead(core_id.as_str(), &actor))
            .unwrap();
        let mut sessions_state = CanonicalState::new();
        sessions_state
            .insert(make_bead(session_id.as_str(), &actor))
            .unwrap();
        let mut store_state = StoreState::new();
        store_state.set_core_state(core_state);
        store_state.set_namespace_state(sessions.clone(), sessions_state);

        let policies = namespace_policies(std::slice::from_ref(&sessions));
        let stamp = make_stamp(10, &actor);
        let ctx = MutationContext {
            namespace: sessions.clone(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let req = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: Some(sessions.clone()),
            from: session_id.clone(),
            to_namespace: Some(NamespaceId::core()),
            to: core_id.clone(),
            kind: DepKind::Blocks,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan_for_store(
                &store_state,
                &policies,
                stamp.at.wall_ms,
                make_stamped_context(ctx, stamp),
                store_identity(2),
                None,
                req,
                &mut dots,
            )
            .expect("plan cross-namespace dep");
        let (draft, effects) = planned.acknowledge_durability();
        let add = draft
            .command
            .raw_delta()
            .iter()
            .find_map(|op| match op {
                TxnOpV1::DepAdd(add) => Some(add),
                _ => None,
            })
            .expect("dep add");

        assert_eq!(add.key.from_ref(), &BeadRef::new(sessions, session_id));
        assert_eq!(
            add.key.to_ref(),
            &BeadRef::new(NamespaceId::core(), core_id)
        );
        assert_eq!(effects.dot_meta_sync_boundaries, 1);
    }

    #[test]
    fn add_dep_rejects_unknown_endpoint_namespace_before_planning_delta() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let missing = NamespaceId::parse("missing").unwrap();
        let mut store_state = StoreState::new();
        store_state
            .core_mut()
            .insert(make_bead("bd-core", &actor))
            .unwrap();
        let policies = namespace_policies(&[]);
        let stamp = make_stamp(10, &actor);
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let req = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: Some(missing.clone()),
            from: BeadId::parse("bd-missing").unwrap(),
            to_namespace: Some(NamespaceId::core()),
            to: BeadId::parse("bd-core").unwrap(),
            kind: DepKind::Blocks,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let err = engine
            .plan_for_store(
                &store_state,
                &policies,
                stamp.at.wall_ms,
                make_stamped_context(ctx, stamp),
                store_identity(2),
                None,
                req,
                &mut dots,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            OpError::NamespaceUnknown { namespace } if namespace == missing
        ));
        assert_eq!(dots.counter, 0);
    }

    #[test]
    fn add_dep_preserves_deleted_endpoint_error_across_namespaces() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let sessions = NamespaceId::parse("sessions").unwrap();
        let session_id = BeadId::parse("bd-session").unwrap();
        let deleted_id = BeadId::parse("bd-deleted").unwrap();
        let stamp = make_stamp(10, &actor);

        let mut store_state = StoreState::new();
        store_state
            .core_mut()
            .insert(make_bead(deleted_id.as_str(), &actor))
            .unwrap();
        store_state
            .core_mut()
            .delete(Tombstone::new(deleted_id.clone(), stamp.clone(), None));
        store_state
            .ensure_namespace(sessions.clone())
            .insert(make_bead(session_id.as_str(), &actor))
            .unwrap();

        let policies = namespace_policies(std::slice::from_ref(&sessions));
        let ctx = MutationContext {
            namespace: sessions.clone(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let req = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: Some(sessions),
            from: session_id,
            to_namespace: Some(NamespaceId::core()),
            to: deleted_id.clone(),
            kind: DepKind::Blocks,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let err = engine
            .plan_for_store(
                &store_state,
                &policies,
                stamp.at.wall_ms,
                make_stamped_context(ctx, stamp),
                store_identity(2),
                None,
                req,
                &mut dots,
            )
            .unwrap_err();

        assert!(matches!(err, OpError::BeadDeleted(id) if id == deleted_id));
        assert_eq!(dots.counter, 0);
    }

    #[test]
    fn add_dep_rejects_source_namespace_that_differs_from_mutation_namespace() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let sessions = NamespaceId::parse("sessions").unwrap();
        let session_id = BeadId::parse("bd-session").unwrap();
        let core_id = BeadId::parse("bd-core").unwrap();
        let mut store_state = StoreState::new();
        store_state
            .core_mut()
            .insert(make_bead(core_id.as_str(), &actor))
            .unwrap();
        store_state
            .ensure_namespace(sessions.clone())
            .insert(make_bead(session_id.as_str(), &actor))
            .unwrap();
        let policies = namespace_policies(std::slice::from_ref(&sessions));
        let stamp = make_stamp(10, &actor);
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let req = ParsedMutationRequest::parse_add_dep(DepPayload {
            from_namespace: Some(sessions),
            from: session_id,
            to_namespace: Some(NamespaceId::core()),
            to: core_id,
            kind: DepKind::Blocks,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let err = engine
            .plan_for_store(
                &store_state,
                &policies,
                stamp.at.wall_ms,
                make_stamped_context(ctx, stamp),
                store_identity(2),
                None,
                req,
                &mut dots,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "dependency"
        ));
        assert_eq!(dots.counter, 0);
    }

    #[test]
    fn remove_dep_uses_fully_qualified_key() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let sessions = NamespaceId::parse("sessions").unwrap();
        let from = BeadId::parse("bd-shared").unwrap();
        let to = BeadId::parse("bd-target").unwrap();
        let policies = namespace_policies(std::slice::from_ref(&sessions));
        let stamp = make_stamp(10, &actor);
        let key = DepKey::new(
            BeadRef::new(sessions.clone(), from.clone()),
            BeadRef::new(NamespaceId::core(), to.clone()),
            DepKind::Blocks,
        )
        .unwrap();
        let mut store_state = StoreState::new();
        store_state
            .core_mut()
            .insert(make_bead(to.as_str(), &actor))
            .unwrap();
        store_state
            .ensure_namespace(sessions.clone())
            .insert(make_bead(from.as_str(), &actor))
            .unwrap();
        let add_key = store_state.check_dep_add_key(key.clone()).unwrap();
        store_state.get_mut(&sessions).unwrap().apply_dep_add(
            add_key,
            Dot {
                replica: ReplicaId::new(Uuid::from_bytes([4u8; 16])),
                counter: 1,
            },
            stamp.clone(),
        );
        let ctx = MutationContext {
            namespace: sessions.clone(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let req = ParsedMutationRequest::parse_remove_dep(DepPayload {
            from_namespace: Some(sessions.clone()),
            from: from.clone(),
            to_namespace: Some(NamespaceId::core()),
            to: to.clone(),
            kind: DepKind::Blocks,
        })
        .unwrap();
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan_for_store(
                &store_state,
                &policies,
                stamp.at.wall_ms,
                make_stamped_context(ctx, stamp),
                store_identity(2),
                None,
                req,
                &mut dots,
            )
            .expect("plan remove");
        let (draft, effects) = planned.acknowledge_durability();
        let remove = draft
            .command
            .raw_delta()
            .iter()
            .find_map(|op| match op {
                TxnOpV1::DepRemove(remove) => Some(remove),
                _ => None,
            })
            .expect("dep remove");
        let wrong_core_key = DepKey::new_local(
            &NamespaceId::core(),
            from.clone(),
            to.clone(),
            DepKind::Blocks,
        )
        .unwrap();

        assert_eq!(remove.key.from_ref(), &BeadRef::new(sessions.clone(), from));
        assert_eq!(remove.key.to_ref(), &BeadRef::new(NamespaceId::core(), to));
        assert_ne!(&remove.key, &wrong_core_key);
        assert_eq!(
            remove.ctx,
            WireDvvV1::from(&store_state.get(&sessions).unwrap().dep_dvv(&key))
        );
        assert_eq!(effects.dot_meta_sync_boundaries, 0);
    }

    #[test]
    fn set_parent_rejects_cycle() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let bead_a = BeadId::parse("bd-a").unwrap();
        let bead_b = BeadId::parse("bd-b").unwrap();

        let mut state = CanonicalState::new();
        state.insert(make_bead(bead_a.as_str(), &actor)).unwrap();
        state.insert(make_bead(bead_b.as_str(), &actor)).unwrap();

        let stamp = make_stamp(10, &actor);
        let dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let edge =
            ParentEdge::new_local(&NamespaceId::core(), bead_a.clone(), bead_b.clone()).unwrap();
        let key = state
            .check_dep_add_key(edge.to_dep_key())
            .expect("parent key");
        state.apply_dep_add(key, dot, stamp.clone());

        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let stamped = make_stamped_context(ctx, stamp.clone());
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([2u8; 16])), 0.into());
        let req = ParsedMutationRequest::SetParent {
            id: bead_b.clone(),
            parent: Some(bead_a.as_str().to_string()),
        };
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let err = engine
            .plan(
                &state,
                stamp.at.wall_ms,
                stamped,
                store,
                None,
                req,
                &mut dots,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            OpError::ValidationFailed { field, .. } if field == "parent"
        ));
    }

    #[test]
    fn set_parent_replaces_existing_parent_with_parent_ops() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let child_id = BeadId::parse("bd-child").unwrap();
        let parent_a = BeadId::parse("bd-parent-a").unwrap();
        let parent_b = BeadId::parse("bd-parent-b").unwrap();

        let mut state = CanonicalState::new();
        for id in [&child_id, &parent_a, &parent_b] {
            state.insert(make_bead(id.as_str(), &actor)).unwrap();
        }

        let edge = ParentEdge::new_local(&NamespaceId::core(), child_id.clone(), parent_a.clone())
            .unwrap();
        let dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let stamp = make_stamp(10, &actor);
        let key = state
            .check_dep_add_key(edge.to_dep_key())
            .expect("parent dep key");
        state.apply_dep_add(key, dot, stamp.clone());

        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let stamped = make_stamped_context(ctx, stamp.clone());
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([2u8; 16])), 0.into());
        let req = ParsedMutationRequest::SetParent {
            id: child_id.clone(),
            parent: Some(parent_b.as_str().to_string()),
        };
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan(
                &state,
                stamp.at.wall_ms,
                stamped,
                store,
                None,
                req,
                &mut dots,
            )
            .expect("plan set parent");
        let (draft, effects) = planned.acknowledge_durability();

        let ops: Vec<_> = draft.command.raw_delta().iter().collect();
        assert!(ops.iter().any(|op| matches!(op, TxnOpV1::ParentRemove(_))));
        assert!(ops.iter().any(|op| matches!(op, TxnOpV1::ParentAdd(_))));
        assert!(
            !ops.iter()
                .any(|op| matches!(op, TxnOpV1::DepAdd(dep) if dep.kind() == DepKind::Parent))
        );
        assert_eq!(effects.dot_meta_sync_boundaries, 1);
    }

    #[test]
    fn set_parent_accepts_namespaced_parent_ref() {
        let engine = MutationEngine::new(Limits::default());
        let actor = actor_id("alice");
        let sessions = NamespaceId::parse("sessions").unwrap();
        let child_id = BeadId::parse("bd-child").unwrap();
        let parent_id = BeadId::parse("bd-parent").unwrap();

        let mut store_state = StoreState::new();
        store_state
            .ensure_namespace(sessions.clone())
            .insert(make_bead(child_id.as_str(), &actor))
            .unwrap();
        store_state
            .core_mut()
            .insert(make_bead(parent_id.as_str(), &actor))
            .unwrap();
        let policies = namespace_policies(std::slice::from_ref(&sessions));

        let req = ParsedMutationRequest::SetParent {
            id: child_id.clone(),
            parent: Some(format!("core/{parent_id}")),
        };
        let ctx = MutationContext {
            namespace: sessions.clone(),
            actor_id: actor.clone(),
            client_request_id: None,
            trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
        };
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));

        let planned = engine
            .plan_for_store(
                &store_state,
                &policies,
                1_000,
                make_stamped_context(ctx, make_stamp(1_000, &actor)),
                store_identity(1),
                None,
                req,
                &mut dots,
            )
            .expect("plan set parent");
        let (draft, effects) = planned.acknowledge_durability();
        let parent_edge = draft
            .command
            .raw_delta()
            .iter()
            .find_map(|op| match op {
                TxnOpV1::ParentAdd(add) => Some(add.edge.clone()),
                _ => None,
            })
            .expect("parent add");

        assert_eq!(parent_edge.child_ref(), &BeadRef::new(sessions, child_id));
        assert_eq!(
            parent_edge.parent_ref(),
            &BeadRef::new(NamespaceId::core(), parent_id)
        );
        assert_eq!(effects.dot_meta_sync_boundaries, 1);
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
        let stamped = make_stamped_context(ctx, stamp);
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([2u8; 16])));
        let err = engine
            .plan(&state, now_ms, stamped, store, None, req, &mut dots)
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
        let stamped = make_stamped_context(ctx, stamp);
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([3u8; 16])));
        let err = engine
            .plan(&state, now_ms, stamped, store, None, req, &mut dots)
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
        let stamped = make_stamped_context(ctx, stamp);
        let mut dots = TestDotAllocator::new(ReplicaId::new(Uuid::from_bytes([4u8; 16])));
        let planned = engine
            .plan(&state, now_ms, stamped, store, None, req, &mut dots)
            .unwrap();
        let (_draft, effects) = planned.acknowledge_durability();
        assert_eq!(effects.dot_meta_sync_boundaries, 1);

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
