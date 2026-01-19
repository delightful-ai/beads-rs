//! Operation executors - apply mutations to state.
//!
//! Each mutation:
//! 1. Ensures repo is loaded
//! 2. Advances the clock
//! 3. Mutates state
//! 4. Marks dirty and schedules sync
//! 5. Returns response

use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crossbeam::channel::Sender;

use super::broadcast::BroadcastEvent;
use super::core::{Daemon, HandleOutcome, ParsedMutationMeta, detect_clock_skew};
use super::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
use super::git_worker::GitOp;
use super::ipc::{MutationMeta, OpResponse, Response, ResponsePayload};
use super::mutation_engine::{
    EventDraft, IdContext, MutationContext, MutationEngine, MutationRequest, ParsedMutationRequest,
    SequencedEvent,
};
use super::ops::{BeadPatch, OpError, OpResult};
use super::store_runtime::{StoreRuntimeError, load_replica_roster};
use super::wal::{
    EventWalError, FrameReader, HlcRow, RecordHeader, SegmentRow, VerifiedRecord, WalIndex,
    WalIndexError, WalIndexTxn, WalReplayError, open_segment_reader,
};
use crate::core::error::details::OverloadedSubsystem;
use crate::core::{
    Applied, BeadId, BeadType, CanonicalState, DepKind, DurabilityClass, DurabilityReceipt,
    Durable, EventBody, EventBytes, EventId, EventKindV1, HeadStatus, Limits, NamespaceId, NoteId,
    Priority, ReplicaId, Seq1, Sha256, StoreIdentity, TxnDeltaV1, TxnOpV1, WallClock, Watermark,
    WatermarkError, Watermarks, WirePatch, WriteStamp, apply_event, decode_event_body,
    hash_event_body,
};
use crate::daemon::metrics;
use crate::daemon::wal::frame::FRAME_HEADER_LEN;
use crate::paths;

#[derive(Debug)]
pub(crate) struct DurabilityWait {
    pub(crate) coordinator: DurabilityCoordinator,
    pub(crate) namespace: NamespaceId,
    pub(crate) origin: ReplicaId,
    pub(crate) seq: Seq1,
    pub(crate) requested: DurabilityClass,
    pub(crate) wait_timeout: Duration,
    pub(crate) response: OpResponse,
}

enum MutationOutcome {
    Immediate(OpResponse),
    Pending(DurabilityWait),
}

impl MutationOutcome {
    fn response_mut(&mut self) -> &mut OpResponse {
        match self {
            MutationOutcome::Immediate(response) => response,
            MutationOutcome::Pending(wait) => &mut wait.response,
        }
    }

    fn into_handle(self) -> HandleOutcome {
        match self {
            MutationOutcome::Immediate(response) => {
                HandleOutcome::Response(Response::ok(ResponsePayload::Op(response)))
            }
            MutationOutcome::Pending(wait) => HandleOutcome::DurabilityWait(wait),
        }
    }
}

impl Daemon {
    fn apply_mutation_request(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        request: MutationRequest,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        match self.apply_mutation_request_inner(repo, meta, request, git_tx) {
            Ok(outcome) => outcome.into_handle(),
            Err(err) => HandleOutcome::Response(Response::err(err)),
        }
    }

    fn apply_mutation_request_inner(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        request: MutationRequest,
        git_tx: &Sender<GitOp>,
    ) -> Result<MutationOutcome, OpError> {
        if self.is_shutting_down() {
            return Err(OpError::Overloaded {
                subsystem: OverloadedSubsystem::Ipc,
                retry_after_ms: Some(100),
                queue_bytes: None,
                queue_events: None,
            });
        }

        let proof = self.ensure_repo_loaded_strict(repo, git_tx)?;
        let meta = self.parse_mutation_meta(&proof, meta)?;
        if self.store_runtime(&proof)?.maintenance_mode {
            return Err(OpError::MaintenanceMode {
                reason: Some("maintenance mode enabled".into()),
            });
        }
        let admission = self.store_runtime(&proof)?.admission.clone();
        let _permit = admission.try_admit_ipc_mutation().map_err(OpError::from)?;
        let store = self.store_identity(&proof)?;
        let limits = self.limits().clone();
        let engine = MutationEngine::new(limits.clone());

        let ParsedMutationMeta {
            namespace,
            durability,
            client_request_id,
            trace_id,
            actor_id,
        } = meta;
        let (
            origin_replica_id,
            wal_index,
            peer_acks,
            policies,
            durable_watermarks_snapshot,
            applied_watermarks_snapshot,
        ) = {
            let store_runtime = self.store_runtime(&proof)?;
            (
                store_runtime.meta.replica_id,
                Arc::clone(&store_runtime.wal_index),
                store_runtime.peer_acks.clone(),
                store_runtime.policies.clone(),
                store_runtime.watermarks_durable.clone(),
                store_runtime.watermarks_applied.clone(),
            )
        };
        let roster = load_replica_roster(proof.store_id())
            .map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
        let coordinator =
            DurabilityCoordinator::new(origin_replica_id, policies, roster, peer_acks);
        let wait_timeout = Duration::from_millis(limits.dead_ms);
        let store_dir = paths::store_dir(proof.store_id());

        let ctx = MutationContext {
            namespace: namespace.clone(),
            actor_id,
            client_request_id,
            trace_id,
        };

        let parsed_request = ParsedMutationRequest::parse(request, &ctx.actor_id)?;

        if ctx.client_request_id.is_some()
            && let Some(mut outcome) = try_reuse_idempotent_response(
                &engine,
                &ctx,
                &parsed_request,
                wal_index.as_ref(),
                &store_dir,
                store,
                origin_replica_id,
                durable_watermarks_snapshot,
                applied_watermarks_snapshot,
                &limits,
                durability,
                &coordinator,
                wait_timeout,
            )?
        {
            let empty_state = CanonicalState::new();
            let state = self
                .store_runtime(&proof)?
                .state
                .get(&namespace)
                .unwrap_or(&empty_state);
            attach_issue_if_created(&namespace, state, outcome.response_mut());
            return Ok(outcome);
        }

        coordinator.ensure_available(&namespace, durability)?;

        let id_ctx = if matches!(parsed_request, ParsedMutationRequest::Create { .. }) {
            let repo_state = self.git_lane_state(&proof)?;
            Some(IdContext {
                root_slug: repo_state.root_slug.clone(),
                remote_url: proof.remote().clone(),
            })
        } else {
            None
        };

        let durable_watermark = self
            .store_runtime(&proof)?
            .watermarks_durable
            .get(&namespace, &origin_replica_id)
            .copied()
            .unwrap_or_else(Watermark::genesis);

        let state_snapshot = {
            let store = self.store_runtime(&proof)?;
            store.state.get_or_default(&namespace)
        };
        let draft = {
            let clock = self.clock_for_actor_mut(&ctx.actor_id);
            engine.plan(
                &state_snapshot,
                clock,
                store,
                id_ctx.as_ref(),
                ctx.clone(),
                parsed_request.clone(),
            )
        }?;

        let mut txn = wal_index.writer().begin_txn().map_err(wal_index_to_op)?;
        let LocalAppendPlan {
            origin_seq,
            prev_sha,
            sequenced,
        } = plan_local_append(
            &engine,
            draft,
            store,
            namespace.clone(),
            origin_replica_id,
            durable_watermark,
            &mut *txn,
        )?;

        let span = tracing::info_span!(
            "mutation",
            store_id = %store.store_id,
            store_epoch = store.store_epoch.get(),
            replica_id = %origin_replica_id,
            durability = ?durability,
            txn_id = %sequenced.event_body.txn_id,
            client_request_id = ?ctx.client_request_id,
            trace_id = %ctx.trace_id,
            namespace = %namespace,
            origin_replica_id = %origin_replica_id,
            origin_seq = origin_seq.get()
        );
        let _guard = span.enter();
        tracing::info!("mutation planned");

        let sha = hash_event_body(&sequenced.event_bytes);
        let sha_bytes = sha.0;
        let request_sha256 = sequenced
            .client_request_id
            .map(|_| sequenced.request_sha256);

        let record = VerifiedRecord::new(
            RecordHeader {
                origin_replica_id,
                origin_seq,
                event_time_ms: sequenced.event_body.event_time_ms,
                txn_id: sequenced.event_body.txn_id,
                client_request_id: sequenced.event_body.client_request_id,
                request_sha256,
                sha256: sha_bytes,
                prev_sha256: prev_sha,
            },
            Bytes::copy_from_slice(sequenced.event_bytes.as_ref()),
            &sequenced.event_body,
        )
        .map_err(|err| {
            tracing::error!(error = ?err, "record verification failed");
            OpError::Internal("record verification failed")
        })?;

        let now_ms = sequenced.event_body.event_time_ms;
        let (append, segment_snapshot) = {
            let store_runtime = self.store_runtime_mut(&proof)?;
            let append_start = Instant::now();
            let append = match store_runtime.event_wal.append(&namespace, &record, now_ms) {
                Ok(append) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_ok(elapsed);
                    metrics::wal_fsync_ok(elapsed);
                    append
                }
                Err(err) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_err(elapsed);
                    metrics::wal_fsync_err(elapsed);
                    return Err(err.into());
                }
            };
            let snapshot = store_runtime
                .event_wal
                .segment_snapshot(&namespace)
                .ok_or(OpError::Internal("missing active wal segment"))?;
            (append, snapshot)
        };
        let last_indexed_offset = append.offset + append.len as u64;
        let segment_row = SegmentRow {
            namespace: namespace.clone(),
            segment_id: append.segment_id,
            segment_path: segment_rel_path(&store_dir, &segment_snapshot.path),
            created_at_ms: segment_snapshot.created_at_ms,
            last_indexed_offset,
            sealed: false,
            final_len: None,
        };

        let broadcast_prev = prev_sha.map(Sha256);
        let event_id = EventId::new(origin_replica_id, namespace.clone(), origin_seq);
        let broadcast_event = BroadcastEvent::new(
            event_id.clone(),
            sha,
            broadcast_prev,
            EventBytes::from(sequenced.event_bytes.clone()),
        );
        let event_ids = vec![event_id];
        txn.upsert_segment(&segment_row).map_err(wal_index_to_op)?;
        if let Some(sealed) = append.sealed.as_ref() {
            let sealed_row = SegmentRow {
                namespace: namespace.clone(),
                segment_id: sealed.segment_id,
                segment_path: segment_rel_path(&store_dir, &sealed.path),
                created_at_ms: sealed.created_at_ms,
                last_indexed_offset: sealed.final_len,
                sealed: true,
                final_len: Some(sealed.final_len),
            };
            txn.upsert_segment(&sealed_row).map_err(wal_index_to_op)?;
        }
        txn.record_event(
            &namespace,
            &event_ids[0],
            sha_bytes,
            prev_sha,
            append.segment_id,
            append.offset,
            append.len,
            now_ms,
            sequenced.event_body.txn_id,
            ctx.client_request_id,
        )
        .map_err(wal_index_to_op)?;
        if let Some(client_request_id) = ctx.client_request_id {
            txn.upsert_client_request(
                &namespace,
                &origin_replica_id,
                client_request_id,
                sequenced.request_sha256,
                sequenced.event_body.txn_id,
                &event_ids,
                now_ms,
            )
            .map_err(wal_index_to_op)?;
        }
        let EventKindV1::TxnV1(txn_body) = &sequenced.event_body.kind;
        let hlc_max = &txn_body.hlc_max;
        txn.update_hlc(&HlcRow {
            actor_id: hlc_max.actor_id.clone(),
            last_physical_ms: hlc_max.physical_ms,
            last_logical: hlc_max.logical,
        })
        .map_err(wal_index_to_op)?;
        crate::daemon::test_hooks::maybe_pause("wal_before_index_commit");
        txn.commit().map_err(wal_index_to_op)?;

        let (
            durable_watermarks,
            applied_watermarks,
            applied_head,
            durable_head,
            applied_seq,
            durable_seq,
        ) = {
            let (store_runtime, repo_state) = self.store_and_lane_mut(&proof)?;
            let apply_start = Instant::now();
            let apply_result = {
                let state = store_runtime.state.ensure_namespace(namespace.clone());
                apply_event(state, &sequenced.event_body)
            };
            let outcome = match apply_result {
                Ok(outcome) => {
                    metrics::apply_ok(apply_start.elapsed());
                    outcome
                }
                Err(err) => {
                    metrics::apply_err(apply_start.elapsed());
                    tracing::error!(error = ?err, "apply_event failed");
                    return Err(OpError::Internal("apply_event failed"));
                }
            };
            store_runtime.record_checkpoint_dirty_shards(&namespace, &outcome);
            let write_stamp = WriteStamp::new(hlc_max.physical_ms, hlc_max.logical);
            let now_wall_ms = WallClock::now().0;
            repo_state.last_seen_stamp = Some(write_stamp.clone());
            repo_state.last_clock_skew = detect_clock_skew(now_wall_ms, write_stamp.wall_ms);
            repo_state.mark_dirty();
            if let Err(err) = store_runtime.broadcaster.publish(broadcast_event.clone()) {
                tracing::warn!("event broadcast failed: {err}");
            }

            store_runtime
                .watermarks_applied
                .advance_contiguous(&namespace, &origin_replica_id, origin_seq, sha_bytes)
                .map_err(|err| {
                    OpError::from(StoreRuntimeError::WatermarkInvalid {
                        kind: "applied",
                        namespace: namespace.clone(),
                        origin: origin_replica_id,
                        source: err,
                    })
                })?;
            store_runtime
                .watermarks_durable
                .advance_contiguous(&namespace, &origin_replica_id, origin_seq, sha_bytes)
                .map_err(|err| {
                    OpError::from(StoreRuntimeError::WatermarkInvalid {
                        kind: "durable",
                        namespace: namespace.clone(),
                        origin: origin_replica_id,
                        source: err,
                    })
                })?;

            let applied_head = store_runtime.applied_head_sha(&namespace, &origin_replica_id);
            let durable_head = store_runtime.durable_head_sha(&namespace, &origin_replica_id);
            let applied_seq = store_runtime
                .watermarks_applied
                .get(&namespace, &origin_replica_id)
                .copied()
                .unwrap_or_else(Watermark::genesis)
                .seq()
                .get();
            let durable_seq = store_runtime
                .watermarks_durable
                .get(&namespace, &origin_replica_id)
                .copied()
                .unwrap_or_else(Watermark::genesis)
                .seq()
                .get();

            (
                store_runtime.watermarks_durable.clone(),
                store_runtime.watermarks_applied.clone(),
                applied_head,
                durable_head,
                applied_seq,
                durable_seq,
            )
        };

        self.mark_checkpoint_dirty(proof.store_id(), &namespace, 1);
        self.schedule_sync(proof.remote().clone());

        let mut watermark_txn = wal_index.writer().begin_txn().map_err(wal_index_to_op)?;
        watermark_txn
            .update_watermark(
                &namespace,
                &origin_replica_id,
                applied_seq,
                durable_seq,
                applied_head,
                durable_head,
            )
            .map_err(wal_index_to_op)?;
        watermark_txn.commit().map_err(wal_index_to_op)?;

        let result = op_result_from_delta(&parsed_request, &txn_body.delta)?;
        let receipt = DurabilityReceipt::local_fsync(
            store,
            sequenced.event_body.txn_id,
            event_ids,
            now_ms,
            durable_watermarks,
            applied_watermarks,
        );

        let mut response = OpResponse::new(result, receipt);
        let mut outcome = match durability {
            DurabilityClass::ReplicatedFsync { k } => {
                match coordinator.poll_replicated(&namespace, origin_replica_id, origin_seq, k) {
                    Ok(ReplicatedPoll::Satisfied { acked_by }) => {
                        response.receipt = DurabilityCoordinator::achieved_receipt(
                            response.receipt,
                            durability,
                            k,
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
                                durability,
                            );
                            return Err(OpError::DurabilityTimeout {
                                requested: durability,
                                waited_ms: 0,
                                pending_replica_ids: Some(pending),
                                receipt: Box::new(pending_receipt),
                            });
                        }
                        MutationOutcome::Pending(DurabilityWait {
                            coordinator: coordinator.clone(),
                            namespace: namespace.clone(),
                            origin: origin_replica_id,
                            seq: origin_seq,
                            requested: durability,
                            wait_timeout,
                            response,
                        })
                    }
                    Err(err) => return Err(err),
                }
            }
            _ => MutationOutcome::Immediate(response),
        };

        tracing::info!("mutation committed");
        let empty_state = CanonicalState::new();
        let state = self
            .store_runtime(&proof)?
            .state
            .get(&namespace)
            .unwrap_or(&empty_state);
        attach_issue_if_created(&namespace, state, outcome.response_mut());
        Ok(outcome)
    }

    /// Create a new bead.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_create(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
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
    ) -> HandleOutcome {
        let request = MutationRequest::Create {
            id: requested_id,
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
        };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Update an existing bead.
    pub(crate) fn apply_update(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        patch: BeadPatch,
        cas: Option<String>,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::Update { id, patch, cas };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Add labels to a bead.
    pub(crate) fn apply_add_labels(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        labels: Vec<String>,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::AddLabels { id, labels };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Remove labels from a bead.
    pub(crate) fn apply_remove_labels(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        labels: Vec<String>,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::RemoveLabels { id, labels };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Replace the parent relationship for a bead.
    pub(crate) fn apply_set_parent(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        parent: Option<String>,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::SetParent { id, parent };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Close a bead.
    pub(crate) fn apply_close(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        reason: Option<String>,
        on_branch: Option<String>,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::Close {
            id,
            reason,
            on_branch,
        };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Reopen a closed bead.
    pub(crate) fn apply_reopen(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::Reopen { id };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Delete a bead (soft delete via tombstone).
    pub(crate) fn apply_delete(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        reason: Option<String>,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::Delete { id, reason };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Add a dependency.
    pub(crate) fn apply_add_dep(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        from: String,
        to: String,
        kind: DepKind,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::AddDep { from, to, kind };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Remove a dependency (soft delete).
    pub(crate) fn apply_remove_dep(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        from: String,
        to: String,
        kind: DepKind,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::RemoveDep { from, to, kind };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Add a note to a bead.
    pub(crate) fn apply_add_note(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        content: String,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::AddNote { id, content };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Claim a bead.
    pub(crate) fn apply_claim(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        lease_secs: u64,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::Claim { id, lease_secs };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Release a claim.
    pub(crate) fn apply_unclaim(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::Unclaim { id };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }

    /// Extend an existing claim.
    pub(crate) fn apply_extend_claim(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        id: String,
        lease_secs: u64,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        let request = MutationRequest::ExtendClaim { id, lease_secs };
        self.apply_mutation_request(repo, meta, request, git_tx)
    }
}

fn wal_index_to_op(err: WalIndexError) -> OpError {
    OpError::from(StoreRuntimeError::WalIndex(err))
}

fn segment_rel_path(store_dir: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(store_dir).unwrap_or(path).to_path_buf()
}

fn event_wal_error_with_path(err: EventWalError, path: &Path) -> OpError {
    let err = match err {
        EventWalError::Io { source, .. } => EventWalError::Io {
            path: Some(path.to_path_buf()),
            source,
        },
        other => other,
    };
    OpError::from(err)
}

struct LocalAppendPlan {
    origin_seq: Seq1,
    prev_sha: Option<[u8; 32]>,
    sequenced: SequencedEvent,
}

fn plan_local_append(
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
            source: WatermarkError::NonContiguous {
                expected: expected_next,
                got: origin_seq,
            },
        }));
    }
    let prev_sha = match durable_watermark.head() {
        HeadStatus::Genesis => None,
        HeadStatus::Known(sha) => Some(sha),
        HeadStatus::Unknown => unreachable!("durable watermark head should be known"),
    };
    let sequenced = engine.build_event(draft, store, namespace, origin_replica_id, origin_seq)?;

    Ok(LocalAppendPlan {
        origin_seq,
        prev_sha,
        sequenced,
    })
}

#[allow(clippy::too_many_arguments)]
fn try_reuse_idempotent_response(
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
    durability: DurabilityClass,
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

    let Some(event_id) = row.event_ids.first() else {
        return Err(OpError::Internal("idempotency row missing event id"));
    };
    let event_body = load_event_body(store_dir, wal_index, event_id, limits)?;
    let EventKindV1::TxnV1(txn) = &event_body.kind;
    let result = op_result_from_delta(request, &txn.delta)?;
    let receipt = DurabilityReceipt::local_fsync(
        store,
        row.txn_id,
        row.event_ids.clone(),
        row.created_at_ms,
        durable_watermarks,
        applied_watermarks,
    );
    let mut max_seq = event_id.origin_seq;
    for eid in &row.event_ids {
        if eid.origin_replica_id != origin_replica_id || eid.namespace != ctx.namespace {
            return Err(OpError::Internal("idempotent request event id mismatch"));
        }
        if eid.origin_seq > max_seq {
            max_seq = eid.origin_seq;
        }
    }

    let mut response = OpResponse::new(result, receipt);
    let outcome = match durability {
        DurabilityClass::ReplicatedFsync { k } => {
            match coordinator.poll_replicated(&ctx.namespace, origin_replica_id, max_seq, k) {
                Ok(ReplicatedPoll::Satisfied { acked_by }) => {
                    response.receipt = DurabilityCoordinator::achieved_receipt(
                        response.receipt,
                        durability,
                        k,
                        acked_by,
                    );
                    MutationOutcome::Immediate(response)
                }
                Ok(ReplicatedPoll::Pending { acked_by, eligible }) => {
                    if wait_timeout.is_zero() {
                        let pending =
                            DurabilityCoordinator::pending_replica_ids(&eligible, &acked_by);
                        let pending_receipt =
                            DurabilityCoordinator::pending_receipt(response.receipt, durability);
                        return Err(OpError::DurabilityTimeout {
                            requested: durability,
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
                        requested: durability,
                        wait_timeout,
                        response,
                    })
                }
                Err(err) => return Err(err),
            }
        }
        _ => MutationOutcome::Immediate(response),
    };

    tracing::info!(
        target: "mutation",
        txn_id = %row.txn_id,
        client_request_id = %client_request_id,
        namespace = %ctx.namespace,
        origin_replica_id = %origin_replica_id,
        origin_seq = max_seq.get(),
        "mutation idempotent reuse"
    );

    Ok(Some(outcome))
}

fn load_event_body(
    store_dir: &Path,
    wal_index: &dyn WalIndex,
    event_id: &EventId,
    limits: &Limits,
) -> Result<EventBody, OpError> {
    let reader = wal_index.reader();
    let from_seq_excl = event_id.origin_seq.prev_seq0();
    let max_bytes = limits.max_wal_record_bytes.saturating_add(FRAME_HEADER_LEN);
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
        .find(|segment| segment.segment_id == item.segment_id)
        .ok_or(OpError::Internal("wal index missing segment for event"))?;
    let path = if segment.segment_path.is_absolute() {
        segment.segment_path
    } else {
        store_dir.join(&segment.segment_path)
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

    let mut reader = FrameReader::new(reader, limits.max_wal_record_bytes);
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

fn op_result_from_delta(
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

fn attach_issue_if_created(
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
    if let Some(bead) = state.get(id) {
        response.issue = Some(crate::api::Issue::from_bead(namespace, bead));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use bytes::Bytes;
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::{
        ActorId, Bead, BeadCore, BeadFields, CanonicalState, Claim, ClientRequestId,
        DurabilityReceipt, Labels, Lww, NamespaceId, NoteAppendV1, NoteId, Stamp, StoreEpoch,
        StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, TraceId, TxnId, TxnOpV1,
        WireBeadPatch, WireNoteV1, WireStamp, Workflow, WriteStamp,
    };
    use crate::daemon::Clock;
    use crate::daemon::Daemon;
    use crate::daemon::core::insert_store_for_tests;
    use crate::daemon::ipc::{MutationMeta, Request};
    use crate::daemon::remote::RemoteUrl;
    use crate::daemon::wal::{
        IndexDurabilityMode, SegmentConfig, SegmentWriter, SqliteWalIndex, rebuild_index,
    };
    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;
    use tracing_subscriber::{Layer, Registry};

    fn bead_id(id: &str) -> BeadId {
        BeadId::parse(id).unwrap()
    }

    fn actor_id(id: &str) -> ActorId {
        ActorId::new(id).unwrap()
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

    #[derive(Clone, Debug, Default)]
    struct SpanFields {
        fields: BTreeMap<String, String>,
    }

    #[derive(Default)]
    struct FieldVisitor {
        fields: BTreeMap<String, String>,
    }

    impl FieldVisitor {
        fn record(&mut self, field: &Field, value: String) {
            self.fields.insert(field.name().to_string(), value);
        }
    }

    impl Visit for FieldVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.record(field, format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.record(field, value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.record(field, value.to_string());
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.record(field, value.to_string());
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.record(field, value.to_string());
        }

        fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
            self.record(field, value.to_string());
        }
    }

    struct CaptureLayer {
        spans: Arc<Mutex<Vec<BTreeMap<String, String>>>>,
    }

    impl CaptureLayer {
        fn new(spans: Arc<Mutex<Vec<BTreeMap<String, String>>>>) -> Self {
            Self { spans }
        }
    }

    impl<S> Layer<S> for CaptureLayer
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &tracing::Id,
            ctx: Context<'_, S>,
        ) {
            let mut visitor = FieldVisitor::default();
            attrs.record(&mut visitor);
            if let Some(span) = ctx.span(id) {
                span.extensions_mut().insert(SpanFields {
                    fields: visitor.fields,
                });
            }
        }

        fn on_record(
            &self,
            id: &tracing::Id,
            values: &tracing::span::Record<'_>,
            ctx: Context<'_, S>,
        ) {
            if let Some(span) = ctx.span(id) {
                let mut visitor = FieldVisitor::default();
                values.record(&mut visitor);
                let mut extensions = span.extensions_mut();
                if extensions.get_mut::<SpanFields>().is_none() {
                    extensions.insert(SpanFields::default());
                }
                let fields = extensions.get_mut::<SpanFields>().expect("span fields");
                fields.fields.extend(visitor.fields);
            }
        }

        fn on_close(&self, id: tracing::Id, ctx: Context<'_, S>) {
            let Some(span) = ctx.span(&id) else {
                return;
            };
            if span.metadata().name() != "mutation" {
                return;
            }
            let fields = span
                .extensions()
                .get::<SpanFields>()
                .map(|fields| fields.fields.clone())
                .unwrap_or_default();
            self.spans.lock().expect("span capture").push(fields);
        }
    }

    #[test]
    fn mutation_span_includes_realtime_context() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let _override = crate::paths::override_data_dir_for_tests(Some(data_dir));

        let repo_path = tmp.path().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();

        let mut daemon = Daemon::new(actor_id("trace@test"));
        let store_id = StoreId::new(Uuid::from_bytes([5u8; 16]));
        let remote = RemoteUrl("example.com/test/repo".into());
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let spans = Arc::new(Mutex::new(Vec::new()));
        let layer = CaptureLayer::new(spans.clone());
        let subscriber = Registry::default()
            .with(layer)
            .with(tracing_subscriber::filter::LevelFilter::TRACE);

        let (git_tx, _git_rx) = crossbeam::channel::unbounded();
        let client_request_id = ClientRequestId::new(Uuid::from_bytes([6u8; 16]));
        let request = Request::Create {
            repo: repo_path.clone(),
            id: None,
            parent: None,
            title: "trace".to_string(),
            bead_type: BeadType::Task,
            priority: Priority::MEDIUM,
            description: None,
            design: None,
            acceptance_criteria: None,
            assignee: None,
            external_ref: None,
            estimated_minutes: None,
            labels: Vec::new(),
            dependencies: Vec::new(),
            meta: MutationMeta {
                client_request_id: Some(client_request_id.to_string()),
                ..MutationMeta::default()
            },
        };

        tracing::dispatcher::with_default(&tracing::Dispatch::new(subscriber), || {
            let _ = daemon.handle_request(request, &git_tx);
        });

        let captured = spans.lock().expect("span capture");
        let fields = captured
            .iter()
            .find(|fields| fields.contains_key("store_id"))
            .cloned()
            .unwrap_or_default();

        for key in [
            "store_id",
            "store_epoch",
            "replica_id",
            "txn_id",
            "client_request_id",
            "trace_id",
            "namespace",
            "origin_replica_id",
            "origin_seq",
        ] {
            assert!(
                fields.contains_key(key),
                "mutation span missing {key}: {fields:?}"
            );
        }
    }

    #[test]
    fn op_result_create_uses_delta_id() {
        let id = bead_id("bd-123");
        let patch = WireBeadPatch::new(id.clone());
        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();

        let request = ParsedMutationRequest::Create {
            id: None,
            parent: None,
            title: "title".to_string(),
            bead_type: BeadType::Task,
            priority: Priority::default(),
            description: None,
            design: None,
            acceptance_criteria: None,
            assignee: None,
            external_ref: None,
            estimated_minutes: None,
            labels: Labels::new(),
            dependencies: Vec::new(),
        };

        let result = op_result_from_delta(&request, &delta).unwrap();
        assert!(matches!(result, OpResult::Created { id: got } if got == id));
    }

    #[test]
    fn attach_issue_includes_created_issue() {
        let actor = actor_id("alice");
        let state = make_state_with_bead("bd-123", &actor);
        let store = StoreIdentity::new(StoreId::new(Uuid::from_bytes([9u8; 16])), StoreEpoch::ZERO);
        let receipt = DurabilityReceipt::local_fsync_defaults(
            store,
            TxnId::new(Uuid::from_bytes([8u8; 16])),
            Vec::new(),
            1_700_000_000_000,
        );
        let mut response = OpResponse::new(
            OpResult::Created {
                id: bead_id("bd-123"),
            },
            receipt,
        );
        attach_issue_if_created(&NamespaceId::core(), &state, &mut response);
        let issue = response.issue.expect("issue attached");
        assert_eq!(issue.id, "bd-123");
    }

    #[test]
    fn op_result_add_note_uses_note_id() {
        let expected_bead_id = bead_id("bd-123");
        let note_id = NoteId::new("note-1").unwrap();
        let note = WireNoteV1 {
            id: note_id.clone(),
            content: "hi".to_string(),
            author: actor_id("alice"),
            at: WireStamp(1, 0),
        };
        let mut delta = TxnDeltaV1::new();
        delta
            .insert(TxnOpV1::NoteAppend(NoteAppendV1 {
                bead_id: expected_bead_id.clone(),
                note,
            }))
            .unwrap();

        let request = ParsedMutationRequest::AddNote {
            id: expected_bead_id.clone(),
            content: "hi".to_string(),
        };

        let result = op_result_from_delta(&request, &delta).unwrap();
        assert!(
            matches!(result, OpResult::NoteAdded { bead_id, note_id: got }
            if bead_id == expected_bead_id && got == note_id.as_str())
        );
    }

    #[test]
    fn op_result_claim_requires_expiry() {
        let bead_id = bead_id("bd-123");
        let patch = WireBeadPatch::new(bead_id.clone());
        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();

        let request = ParsedMutationRequest::Claim {
            id: bead_id.clone(),
            lease_secs: 60,
        };

        let err = op_result_from_delta(&request, &delta).unwrap_err();
        assert!(matches!(err, OpError::Internal(_)));
    }

    #[test]
    fn op_result_claim_reads_expiry() {
        let bead_id = bead_id("bd-123");
        let mut patch = WireBeadPatch::new(bead_id.clone());
        let expires = WallClock(1234);
        patch.assignee_expires = WirePatch::Set(expires);
        let mut delta = TxnDeltaV1::new();
        delta.insert(TxnOpV1::BeadUpsert(Box::new(patch))).unwrap();

        let request = ParsedMutationRequest::Claim {
            id: bead_id.clone(),
            lease_secs: 60,
        };

        let result = op_result_from_delta(&request, &delta).unwrap();
        assert!(matches!(result, OpResult::Claimed { id, expires: got }
            if id == bead_id && got == expires));
    }

    #[test]
    fn idempotent_retry_reuses_wal_mapping() {
        let temp = TempDir::new().unwrap();
        let store_dir = temp.path().join("store");
        std::fs::create_dir_all(&store_dir).unwrap();

        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let store = StoreIdentity::new(store_id, StoreEpoch::new(0));
        let replica_id = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
        let versions = StoreMetaVersions::new(1, 2, 1, 1, 1);
        let meta = StoreMeta::new(store, replica_id, versions, 1_700_000_000_000);

        let index = SqliteWalIndex::open(&store_dir, &meta, IndexDurabilityMode::Cache).unwrap();
        let limits = Limits::default();
        let engine = MutationEngine::new(limits.clone());
        let actor = actor_id("alice");
        let state = make_state_with_bead("bd-123", &actor);

        let client_request_id = ClientRequestId::new(Uuid::from_bytes([3u8; 16]));
        let ctx = MutationContext {
            namespace: NamespaceId::core(),
            actor_id: actor.clone(),
            client_request_id: Some(client_request_id),
            trace_id: TraceId::from(client_request_id),
        };
        let request = ParsedMutationRequest::parse(
            MutationRequest::AddLabels {
                id: "bd-123".into(),
                labels: vec!["alpha".into()],
            },
            &actor,
        )
        .unwrap();

        let origin_seq = Seq1::new(std::num::NonZeroU64::new(1).unwrap());
        let mut clock = fixed_clock(1_700_000_000_000);
        let draft = engine
            .plan(
                &state,
                &mut clock,
                store,
                None,
                ctx.clone(),
                request.clone(),
            )
            .unwrap();
        let sequenced = engine
            .build_event(draft, store, ctx.namespace.clone(), replica_id, origin_seq)
            .unwrap();

        let sha = hash_event_body(&sequenced.event_bytes).0;
        let record = VerifiedRecord::new(
            RecordHeader {
                origin_replica_id: replica_id,
                origin_seq,
                event_time_ms: sequenced.event_body.event_time_ms,
                txn_id: sequenced.event_body.txn_id,
                client_request_id: sequenced.event_body.client_request_id,
                request_sha256: Some(sequenced.request_sha256),
                sha256: sha,
                prev_sha256: None,
            },
            Bytes::copy_from_slice(sequenced.event_bytes.as_ref()),
            &sequenced.event_body,
        )
        .unwrap();

        let mut writer = SegmentWriter::open(
            &store_dir,
            &meta,
            &ctx.namespace,
            sequenced.event_body.event_time_ms,
            SegmentConfig::from_limits(&limits),
        )
        .unwrap();
        writer
            .append(&record, sequenced.event_body.event_time_ms)
            .unwrap();

        rebuild_index(&store_dir, &meta, &index, &limits).unwrap();

        let coordinator = DurabilityCoordinator::new(
            replica_id,
            std::collections::BTreeMap::new(),
            None,
            Arc::new(std::sync::Mutex::new(
                crate::daemon::repl::PeerAckTable::new(),
            )),
        );

        let outcome = try_reuse_idempotent_response(
            &engine,
            &ctx,
            &request,
            &index,
            &store_dir,
            store,
            replica_id,
            Watermarks::new(),
            Watermarks::new(),
            &limits,
            DurabilityClass::LocalFsync,
            &coordinator,
            Duration::from_millis(0),
        )
        .unwrap()
        .expect("expected idempotent response");

        let response = match outcome {
            MutationOutcome::Immediate(response) => response,
            MutationOutcome::Pending(_) => panic!("expected immediate response"),
        };

        assert_eq!(response.receipt.txn_id, sequenced.event_body.txn_id);
        assert_eq!(
            response.receipt.event_ids,
            vec![EventId::new(replica_id, ctx.namespace.clone(), origin_seq)]
        );
    }
}
