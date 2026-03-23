use super::*;
use crate::runtime::mutation_engine::{DotAllocation, DotDurabilityEffect};

pub(super) fn wal_index_to_op(err: WalIndexError) -> OpError {
    OpError::from(StoreRuntimeError::WalIndex(err))
}

pub(super) fn segment_rel_path(store_dir: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(store_dir).unwrap_or(path).to_path_buf()
}

pub(super) fn event_wal_error_with_path(err: EventWalError, path: &Path) -> OpError {
    let err = match err {
        EventWalError::Io { source, .. } => EventWalError::Io {
            path: Some(path.to_path_buf()),
            source,
        },
        other => other,
    };
    OpError::from(err)
}

pub(super) fn enforce_exact_record_size(
    record: &VerifiedRecord,
    limits: &Limits,
) -> Result<(), OpError> {
    let exact_bytes = record.encoded_body_len()?;
    let max_wal_record_bytes = limits.policy().max_wal_record_bytes();
    if exact_bytes > max_wal_record_bytes {
        return Err(OpError::WalRecordTooLarge {
            max_wal_record_bytes,
            estimated_bytes: exact_bytes,
        });
    }
    Ok(())
}

pub(super) struct LocalAppendPlan {
    pub(super) origin_seq: Seq1,
    pub(super) prev_sha: Option<[u8; 32]>,
    pub(super) sequenced: SequencedEvent,
}

pub(super) struct RuntimeDotAllocator<'a> {
    replica_id: ReplicaId,
    wal_index_txn: &'a mut dyn WalIndexTxn,
}

impl<'a> RuntimeDotAllocator<'a> {
    pub(super) fn new(replica_id: ReplicaId, wal_index_txn: &'a mut dyn WalIndexTxn) -> Self {
        Self {
            replica_id,
            wal_index_txn,
        }
    }
}

impl DotAllocator for RuntimeDotAllocator<'_> {
    fn next_dot(&mut self) -> Result<DotAllocation, OpError> {
        let counter = self
            .wal_index_txn
            .next_orset_counter()
            .map_err(wal_index_to_op)?;
        Ok(DotAllocation {
            dot: Dot {
                replica: self.replica_id,
                counter,
            },
            durability: DotDurabilityEffect::WalIndexTxnMetadata,
        })
    }
}

pub(super) fn plan_local_append(
    engine: &MutationEngine,
    draft: EventDraft,
    store: StoreIdentity,
    namespace: NamespaceId,
    origin_replica_id: ReplicaId,
    durable_watermark: Watermark<Durable>,
    txn: &mut dyn WalIndexTxn,
) -> Result<LocalAppendPlan, OpError> {
    let origin_seq = txn
        .next_origin_seq(&namespace, &origin_replica_id)
        .map_err(wal_index_to_op)?;
    let expected_next = durable_watermark.seq().next();
    if origin_seq != expected_next {
        return Err(OpError::from(StoreRuntimeError::WatermarkInvalid {
            kind: "durable",
            namespace: namespace.clone(),
            origin: origin_replica_id,
            source: Box::new(WatermarkError::NonContiguous {
                expected: expected_next,
                got: origin_seq,
            }),
        }));
    }
    let prev_sha = match durable_watermark.head() {
        HeadStatus::Genesis => None,
        HeadStatus::Known(sha) => Some(sha),
    };
    let sequenced = engine.build_event(draft, store, namespace, origin_replica_id, origin_seq)?;

    Ok(LocalAppendPlan {
        origin_seq,
        prev_sha,
        sequenced,
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) fn try_reuse_idempotent_response(
    engine: &MutationEngine,
    ctx: &MutationContext,
    request: &ParsedMutationRequest,
    wal_index: &dyn WalIndex,
    store_dir: &Path,
    store: StoreIdentity,
    origin_replica_id: ReplicaId,
    durable_watermarks: Watermarks<Durable>,
    applied_watermarks: Watermarks<Applied>,
    limits: &Limits,
    current_requested: DurabilityClass,
    coordinator: &DurabilityCoordinator,
    wait_timeout: Duration,
) -> Result<Option<MutationOutcome>, OpError> {
    let Some(client_request_id) = ctx.client_request_id else {
        return Ok(None);
    };

    let request_sha256 = engine.request_sha256_for(ctx, request)?;
    let existing = wal_index
        .reader()
        .lookup_client_request(&ctx.namespace, &origin_replica_id, client_request_id)
        .map_err(wal_index_to_op)?;
    let Some(row) = existing else {
        return Ok(None);
    };

    if row.request_sha256 != request_sha256 {
        return Err(OpError::ClientRequestIdReuseMismatch {
            namespace: ctx.namespace.clone(),
            client_request_id,
            expected_request_sha256: Box::new(row.request_sha256),
            got_request_sha256: Box::new(request_sha256),
        });
    }

    let event_id = row.event_ids.first_event_id();
    let event_body = load_event_body(store_dir, wal_index, &event_id, limits)?;
    let EventKindV1::TxnV1(txn) = &event_body.kind;
    let result = op_result_from_delta(request, &txn.delta)?;
    let receipt = DurabilityReceipt::local_fsync(
        store,
        row.txn_id,
        row.event_ids.event_ids(),
        row.created_at_ms,
        durable_watermarks,
        applied_watermarks,
    );
    let max_seq = row.event_ids.max_seq();

    let mut response = OpResponse::new(result, receipt);
    let outcome = match row.durability_claim.clone() {
        Some(DurabilityRequestClaim::Replicated(claim)) => {
            let requested = DurabilityClass::ReplicatedFsync { k: claim.k };
            match coordinator.poll_claim(&ctx.namespace, origin_replica_id, max_seq, &claim) {
                Ok(ReplicatedPoll::Satisfied { acked_by }) => {
                    response.receipt = DurabilityCoordinator::achieved_receipt(
                        response.receipt,
                        requested,
                        claim.k,
                        acked_by,
                    );
                    MutationOutcome::Immediate(response)
                }
                Ok(ReplicatedPoll::Pending { acked_by, eligible }) => {
                    if wait_timeout.is_zero() {
                        let pending =
                            DurabilityCoordinator::pending_replica_ids(&eligible, &acked_by);
                        let pending_receipt = DurabilityCoordinator::pending_receipt(
                            response.receipt,
                            requested,
                            acked_by,
                        );
                        return Err(OpError::DurabilityTimeout {
                            requested,
                            waited_ms: 0,
                            pending_replica_ids: Some(pending),
                            receipt: Box::new(pending_receipt),
                        });
                    }
                    MutationOutcome::Pending(DurabilityWait {
                        coordinator: coordinator.clone(),
                        namespace: ctx.namespace.clone(),
                        origin: origin_replica_id,
                        seq: max_seq,
                        claim: DurabilityRequestClaim::Replicated(claim),
                        wait_timeout,
                        response,
                    })
                }
                Err(err) => return Err(err),
            }
        }
        Some(DurabilityRequestClaim::LocalFsync) => MutationOutcome::Immediate(response),
        None => match current_requested {
            // Legacy rows predate persisted claims. We can only honor the current retry
            // request, but we still freeze one cohort snapshot so this retry does not
            // observe moving quorum semantics mid-call.
            DurabilityClass::ReplicatedFsync { .. } => {
                let claim = match coordinator.request_claim(&ctx.namespace, current_requested)? {
                    DurabilityRequestClaim::Replicated(claim) => claim,
                    DurabilityRequestClaim::LocalFsync => {
                        return Err(OpError::Internal(
                            "replicated request claim downgraded unexpectedly",
                        ));
                    }
                };
                let requested = DurabilityClass::ReplicatedFsync { k: claim.k };
                match coordinator.poll_claim(&ctx.namespace, origin_replica_id, max_seq, &claim) {
                    Ok(ReplicatedPoll::Satisfied { acked_by }) => {
                        response.receipt = DurabilityCoordinator::achieved_receipt(
                            response.receipt,
                            requested,
                            claim.k,
                            acked_by,
                        );
                        MutationOutcome::Immediate(response)
                    }
                    Ok(ReplicatedPoll::Pending { acked_by, eligible }) => {
                        if wait_timeout.is_zero() {
                            let pending =
                                DurabilityCoordinator::pending_replica_ids(&eligible, &acked_by);
                            let pending_receipt = DurabilityCoordinator::pending_receipt(
                                response.receipt,
                                requested,
                                acked_by,
                            );
                            return Err(OpError::DurabilityTimeout {
                                requested,
                                waited_ms: 0,
                                pending_replica_ids: Some(pending),
                                receipt: Box::new(pending_receipt),
                            });
                        }
                        MutationOutcome::Pending(DurabilityWait {
                            coordinator: coordinator.clone(),
                            namespace: ctx.namespace.clone(),
                            origin: origin_replica_id,
                            seq: max_seq,
                            claim: DurabilityRequestClaim::Replicated(claim),
                            wait_timeout,
                            response,
                        })
                    }
                    Err(err) => return Err(err),
                }
            }
            DurabilityClass::LocalFsync => MutationOutcome::Immediate(response),
        },
    };

    let span = tracing::info_span!(
        "mutation",
        store_id = %store.store_id,
        store_epoch = store.store_epoch.get(),
        replica_id = %origin_replica_id,
        actor_id = %ctx.actor_id,
        durability = ?row
            .durability_claim
            .as_ref()
            .map(DurabilityRequestClaim::requested)
            .unwrap_or(current_requested),
        txn_id = %row.txn_id,
        client_request_id = ?ctx.client_request_id,
        trace_id = %ctx.trace_id,
        namespace = %ctx.namespace,
        origin_replica_id = %origin_replica_id,
        origin_seq = max_seq.get()
    );
    let _guard = span.enter();
    tracing::info!(target: "mutation", "mutation idempotent reuse");

    Ok(Some(outcome))
}

pub(super) fn load_event_body(
    store_dir: &Path,
    wal_index: &dyn WalIndex,
    event_id: &EventId,
    limits: &Limits,
) -> Result<ValidatedEventBody, OpError> {
    let reader = wal_index.reader();
    let from_seq_excl = event_id.origin_seq.prev_seq0();
    let max_bytes = limits
        .policy()
        .max_wal_record_bytes()
        .saturating_add(FRAME_HEADER_LEN);
    let items = reader
        .iter_from(
            &event_id.namespace,
            &event_id.origin_replica_id,
            from_seq_excl,
            max_bytes,
        )
        .map_err(wal_index_to_op)?;
    let item = items
        .into_iter()
        .find(|item| item.event_id == *event_id)
        .ok_or(OpError::Internal(
            "wal index missing event for idempotent request",
        ))?;
    let segments = reader
        .list_segments(&event_id.namespace)
        .map_err(wal_index_to_op)?;
    let segment = segments
        .into_iter()
        .find(|segment| segment.segment_id() == item.segment_id)
        .ok_or(OpError::Internal("wal index missing segment for event"))?;
    let path = if segment.segment_path().is_absolute() {
        segment.segment_path().to_path_buf()
    } else {
        store_dir.join(segment.segment_path())
    };

    let mut reader =
        open_segment_reader(&path).map_err(|err| event_wal_error_with_path(err, &path))?;
    reader
        .seek(SeekFrom::Start(item.offset))
        .map_err(|source| {
            OpError::from(EventWalError::Io {
                path: Some(path.clone()),
                source,
            })
        })?;

    let mut reader = FrameReader::new(reader, limits.policy().max_wal_record_bytes());
    let record = reader
        .read_next()
        .map_err(|err| event_wal_error_with_path(err, &path))?
        .ok_or_else(|| {
            OpError::from(EventWalError::FrameLengthInvalid {
                reason: "unexpected eof while reading record".to_string(),
            })
        })?;

    let (_, event_body) = decode_event_body(record.payload_bytes(), limits).map_err(|source| {
        OpError::from(StoreRuntimeError::WalReplay(Box::new(
            WalReplayError::EventBodyDecode {
                path: path.clone(),
                offset: item.offset,
                source,
            },
        )))
    })?;
    Ok(event_body)
}

pub(super) fn op_result_from_delta(
    request: &ParsedMutationRequest,
    delta: &TxnDeltaV1,
) -> Result<OpResult, OpError> {
    match request {
        ParsedMutationRequest::Create { .. } => {
            let id = find_created_id(delta)?;
            Ok(OpResult::Created { id })
        }
        ParsedMutationRequest::Update { id, .. }
        | ParsedMutationRequest::AddLabels { id, .. }
        | ParsedMutationRequest::RemoveLabels { id, .. }
        | ParsedMutationRequest::SetParent { id, .. } => Ok(OpResult::Updated { id: id.clone() }),
        ParsedMutationRequest::Close { id, .. } => Ok(OpResult::Closed { id: id.clone() }),
        ParsedMutationRequest::Reopen { id } => Ok(OpResult::Reopened { id: id.clone() }),
        ParsedMutationRequest::Delete { id, .. } => Ok(OpResult::Deleted { id: id.clone() }),
        ParsedMutationRequest::AddDep { from, to, .. } => Ok(OpResult::DepAdded {
            from: from.clone(),
            to: to.clone(),
        }),
        ParsedMutationRequest::RemoveDep { from, to, .. } => Ok(OpResult::DepRemoved {
            from: from.clone(),
            to: to.clone(),
        }),
        ParsedMutationRequest::AddNote { id, .. } => {
            let bead_id = id.clone();
            let note_id = find_note_id(delta, &bead_id)?;
            Ok(OpResult::NoteAdded {
                bead_id,
                note_id: note_id.as_str().to_string(),
            })
        }
        ParsedMutationRequest::Claim { id, .. } => {
            let bead_id = id.clone();
            let expires = find_claim_expiry(delta, &bead_id)?;
            Ok(OpResult::Claimed {
                id: bead_id,
                expires,
            })
        }
        ParsedMutationRequest::Unclaim { id } => Ok(OpResult::Unclaimed { id: id.clone() }),
        ParsedMutationRequest::ExtendClaim { id, .. } => {
            let bead_id = id.clone();
            let expires = find_claim_expiry(delta, &bead_id)?;
            Ok(OpResult::ClaimExtended {
                id: bead_id,
                expires,
            })
        }
    }
}

pub(super) fn attach_issue_if_created(
    namespace: &NamespaceId,
    state: &CanonicalState,
    response: &mut OpResponse,
) {
    if response.issue.is_some() {
        return;
    }
    let OpResult::Created { id } = &response.result else {
        return;
    };
    if let Some(view) = state.bead_view(id) {
        response.issue = Some(crate::api::Issue::from_view(namespace, &view));
    }
}

fn find_created_id(delta: &TxnDeltaV1) -> Result<BeadId, OpError> {
    let mut found: Option<BeadId> = None;
    for op in delta.iter() {
        if let TxnOpV1::BeadUpsert(patch) = op {
            match &found {
                None => found = Some(patch.id.clone()),
                Some(existing) if *existing == patch.id => {}
                Some(_) => {
                    return Err(OpError::Internal("create delta contains multiple bead ids"));
                }
            }
        }
    }
    found.ok_or(OpError::Internal("create delta missing bead upsert"))
}

fn find_note_id(delta: &TxnDeltaV1, expected: &BeadId) -> Result<NoteId, OpError> {
    let mut found: Option<NoteId> = None;
    for op in delta.iter() {
        if let TxnOpV1::NoteAppend(append) = op {
            if &append.bead_id != expected {
                return Err(OpError::Internal("note append bead id mismatch"));
            }
            if found.replace(append.note.id.clone()).is_some() {
                return Err(OpError::Internal("note append repeated in delta"));
            }
        }
    }
    found.ok_or(OpError::Internal("note append missing from delta"))
}

fn find_claim_expiry(delta: &TxnDeltaV1, expected: &BeadId) -> Result<WallClock, OpError> {
    let mut found: Option<WallClock> = None;
    for op in delta.iter() {
        if let TxnOpV1::BeadUpsert(patch) = op {
            if &patch.id != expected {
                continue;
            }
            if let WirePatch::Set(expires) = patch.assignee_expires
                && found.replace(expires).is_some()
            {
                return Err(OpError::Internal("claim expiry repeated in delta"));
            }
        }
    }
    found.ok_or(OpError::Internal("claim delta missing assignee_expires"))
}
