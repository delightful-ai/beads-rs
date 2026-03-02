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

use crossbeam::channel::Sender;

use super::core::{Daemon, HandleOutcome, ParsedMutationMeta, detect_clock_skew};
use super::durability_coordinator::{DurabilityCoordinator, ReplicatedPoll};
use super::git_worker::GitOp;
use super::ipc::{
    AddNotePayload, ClaimPayload, ClosePayload, CreatePayload, DeletePayload, DepPayload,
    IdPayload, LabelsPayload, LeasePayload, MutationMeta, OpResponse, ParentPayload, Response,
    ResponseExt, ResponsePayload, UpdatePayload,
};
use super::mutation_engine::{
    DotAllocator, EventDraft, IdContext, MutationContext, MutationEngine, ParsedMutationRequest,
    SequencedEvent, StampedContext,
};
use super::ops::OpError;
use super::store::runtime::{StoreRuntime, StoreRuntimeError, load_replica_roster};
use super::wal::{
    ClientRequestEventIds, EventWalError, FrameReader, HlcRow, RecordHeader, RequestProof,
    SegmentRow, VerifiedRecord, WalAppend, WalAppendDurabilityEffect, WalCursorOffset, WalIndex,
    WalIndexError, WalIndexTxn, WalReplayError, open_segment_reader,
};
use super::wal_atomic_commit::{AtomicWalCommitPath, AtomicWalDurabilityTxn, tip_watermark_pair};
use crate::broadcast::BroadcastEvent;
use crate::core::error::details::OverloadedSubsystem;
use crate::core::{
    ActorId, Applied, BeadId, CanonicalState, Dot, DurabilityClass, DurabilityReceipt, Durable,
    EventBytes, EventId, EventKindV1, HeadStatus, Limits, NamespaceId, NoteId, ReplicaId, Seq1,
    Sha256, Stamp, StoreIdentity, TxnDeltaV1, TxnOpV1, ValidatedEventBody, WallClock, Watermark,
    WatermarkError, Watermarks, WirePatch, WriteStamp, apply_event, decode_event_body,
    hash_event_body,
};
use crate::runtime::metrics;
use crate::runtime::wal::frame::FRAME_HEADER_LEN;
use beads_surface::ops::OpResult;

mod helpers;
use helpers::*;

#[derive(Debug)]
pub struct DurabilityWait {
    pub coordinator: DurabilityCoordinator,
    pub namespace: NamespaceId,
    pub origin: ReplicaId,
    pub seq: Seq1,
    pub requested: DurabilityClass,
    pub wait_timeout: Duration,
    pub response: OpResponse,
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
    fn apply_mutation_request_with<F>(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        parse: F,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome
    where
        F: FnOnce(&ActorId) -> Result<ParsedMutationRequest, OpError>,
    {
        match self.apply_mutation_request_with_inner(repo, meta, parse, git_tx) {
            Ok(outcome) => outcome.into_handle(),
            Err(err) => HandleOutcome::Response(Response::err_from(err)),
        }
    }

    fn apply_mutation_request_with_inner<F>(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        parse: F,
        git_tx: &Sender<GitOp>,
    ) -> Result<MutationOutcome, OpError>
    where
        F: FnOnce(&ActorId) -> Result<ParsedMutationRequest, OpError>,
    {
        if self.is_shutting_down() {
            return Err(OpError::Overloaded {
                subsystem: OverloadedSubsystem::Ipc,
                retry_after_ms: Some(100),
                queue_bytes: None,
                queue_events: None,
            });
        }

        let actor = self.actor().clone();
        let limits = self.limits().clone();
        let (store_id, remote, meta) = {
            let proof = self.ensure_repo_loaded_strict(repo, git_tx)?;
            let meta = proof.parse_mutation_meta(meta, &actor)?;
            (proof.store_id(), proof.remote().clone(), meta)
        };

        let ParsedMutationMeta {
            namespace,
            durability,
            client_request_id,
            trace_id,
            actor_id,
        } = meta;
        let store_dir = self.layout().store_dir(&store_id);
        let engine = MutationEngine::new(limits.clone());
        let ctx = MutationContext {
            namespace: namespace.clone(),
            actor_id,
            client_request_id,
            trace_id,
        };
        let parsed_request = parse(&ctx.actor_id)?;

        let proof = self.loaded_store(store_id, remote.clone());
        if proof.runtime().maintenance_mode {
            return Err(OpError::MaintenanceMode {
                reason: Some("maintenance mode enabled".into()),
            });
        }
        let admission = proof.runtime().admission.clone();
        let _permit = admission.try_admit_ipc_mutation().map_err(OpError::from)?;
        let store = proof.store_identity();

        let (
            origin_replica_id,
            wal_index,
            peer_acks,
            policies,
            durable_watermarks_snapshot,
            applied_watermarks_snapshot,
            layout,
        ) = {
            let store_runtime = proof.runtime();
            (
                store_runtime.meta.replica_id,
                Arc::clone(&store_runtime.wal_index),
                store_runtime.peer_acks.clone(),
                store_runtime.policies.clone(),
                store_runtime.watermarks_durable.clone(),
                store_runtime.watermarks_applied.clone(),
                store_runtime.layout().clone(),
            )
        };
        let roster = load_replica_roster(&layout, store_id)
            .map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
        let coordinator =
            DurabilityCoordinator::new(origin_replica_id, policies, roster, peer_acks);
        let wait_timeout = Duration::from_millis(limits.dead_ms);

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
            let state = Self::namespace_state(&proof, &namespace);
            attach_issue_if_created(&namespace, state, outcome.response_mut());
            return Ok(outcome);
        }

        coordinator.ensure_available(&namespace, durability)?;

        let id_ctx = if matches!(parsed_request, ParsedMutationRequest::Create { .. }) {
            let repo_state = proof.lane();
            Some(IdContext {
                root_slug: repo_state.root_slug.clone(),
                remote_url: proof.remote().clone(),
            })
        } else {
            None
        };

        let durable_watermark = proof
            .runtime()
            .watermarks_durable
            .get(&namespace, &origin_replica_id)
            .copied()
            .unwrap_or_else(Watermark::genesis);

        drop(proof);
        let (now_ms, stamp) = {
            let clock = self.clock_for_actor_mut(&ctx.actor_id);
            let now_ms = clock.wall_ms();
            let write_stamp = clock.tick();
            (now_ms, Stamp::new(write_stamp, ctx.actor_id.clone()))
        };
        let stamped_ctx = StampedContext::new(ctx.clone(), stamp.clone())?;
        let mut proof = self.loaded_store(store_id, remote.clone());
        let planned = {
            let store_runtime = proof.runtime_mut();
            let state_snapshot = store_runtime.state.get_or_default(&namespace);
            let mut dot_alloc = RuntimeDotAllocator::new(origin_replica_id, store_runtime);
            engine.plan(
                &state_snapshot,
                now_ms,
                stamped_ctx,
                store,
                id_ctx.as_ref(),
                parsed_request.clone(),
                &mut dot_alloc,
            )
        }?;
        let (draft, planning_effects) = planned.acknowledge_durability();

        let mut atomic_txn = AtomicWalDurabilityTxn::begin(
            wal_index.as_ref(),
            namespace.clone(),
            origin_replica_id,
            AtomicWalCommitPath::Mutation,
        )
        .map_err(wal_index_to_op)?;
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
            atomic_txn.index_mut(),
        )?;

        let span = tracing::info_span!(
            "mutation",
            store_id = %store.store_id,
            store_epoch = store.store_epoch.get(),
            replica_id = %origin_replica_id,
            actor_id = %ctx.actor_id,
            durability = ?durability,
            txn_id = %sequenced.event_body.txn_id,
            client_request_id = ?ctx.client_request_id,
            trace_id = %ctx.trace_id,
            namespace = %namespace,
            origin_replica_id = %origin_replica_id,
            origin_seq = origin_seq.get(),
            planning_dot_meta_sync_boundaries = planning_effects.dot_meta_sync_boundaries,
            wal_durability_effect = ?WalAppendDurabilityEffect::SyncBoundaryCrossed
        );
        let _guard = span.enter();
        tracing::info!("mutation planned");

        let sha = hash_event_body(&sequenced.event_bytes);
        let sha_bytes = sha.0;
        let commit_watermarks =
            tip_watermark_pair(origin_seq, sha_bytes).map_err(wal_index_to_op)?;
        let request_proof = sequenced
            .client_request_id
            .map(|client_request_id| RequestProof::Client {
                client_request_id,
                request_sha256: sequenced.request_sha256,
            })
            .unwrap_or(RequestProof::None);

        let record = VerifiedRecord::new(
            RecordHeader {
                origin_replica_id,
                origin_seq,
                event_time_ms: sequenced.event_body.event_time_ms,
                txn_id: sequenced.event_body.txn_id,
                request_proof,
                sha256: sha_bytes,
                prev_sha256: prev_sha,
            },
            sequenced.event_bytes.clone(),
            sequenced.event_body.clone(),
        )
        .map_err(|err| {
            tracing::error!(error = ?err, "record verification failed");
            OpError::Internal("record verification failed")
        })?;

        let now_ms = sequenced.event_body.event_time_ms;
        let (append, wal_effect, segment_snapshot) = {
            let store_runtime = proof.runtime_mut();
            let append_start = Instant::now();
            let append = match store_runtime
                .event_wal
                .wal_append(&namespace, &record, now_ms)
            {
                Ok(pending_append) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_ok(elapsed);
                    metrics::wal_fsync_ok(elapsed);
                    pending_append.acknowledge_durability()
                }
                Err(err) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_err(elapsed);
                    metrics::wal_fsync_err(elapsed);
                    return Err(err.into());
                }
            };
            let wal_effect = append.durability;
            let append = append.append;
            let snapshot = store_runtime
                .event_wal
                .segment_snapshot(&namespace)
                .ok_or(OpError::Internal("missing active wal segment"))?;
            (append, wal_effect, snapshot)
        };
        span.record("wal_durability_effect", tracing::field::debug(&wal_effect));
        tracing::debug!(
            ?planning_effects,
            ?wal_effect,
            "mutation durability effects acknowledged"
        );
        let last_indexed_offset = WalCursorOffset::new(append.offset + append.len as u64);
        let segment_row = SegmentRow::open(
            namespace.clone(),
            append.segment_id,
            segment_rel_path(&store_dir, &segment_snapshot.path),
            segment_snapshot.created_at_ms,
            last_indexed_offset,
        );

        let broadcast_prev = prev_sha.map(Sha256);
        let event_id = EventId::new(origin_replica_id, namespace.clone(), origin_seq);
        let broadcast_event = BroadcastEvent::new(
            event_id.clone(),
            sha,
            broadcast_prev,
            EventBytes::from(sequenced.event_bytes.clone()),
        );
        let event_ids = ClientRequestEventIds::single(event_id.clone());
        {
            let txn = atomic_txn.index_mut();
            txn.upsert_segment(&segment_row).map_err(wal_index_to_op)?;
            if let Some(sealed) = append.sealed.as_ref() {
                let sealed_row = SegmentRow::sealed(
                    namespace.clone(),
                    sealed.segment_id,
                    segment_rel_path(&store_dir, &sealed.path),
                    sealed.created_at_ms,
                    WalCursorOffset::new(sealed.final_len),
                    sealed.final_len,
                );
                txn.upsert_segment(&sealed_row).map_err(wal_index_to_op)?;
            }
            txn.record_event(
                &namespace,
                &event_id,
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
        }
        let EventKindV1::TxnV1(txn_body) = &sequenced.event_body.kind;
        let hlc_max = &txn_body.hlc_max;
        atomic_txn
            .index_mut()
            .update_hlc(&HlcRow {
                actor_id: hlc_max.actor_id.clone(),
                last_physical_ms: hlc_max.physical_ms,
                last_logical: hlc_max.logical,
            })
            .map_err(wal_index_to_op)?;
        atomic_txn
            .commit_with_watermarks(commit_watermarks)
            .map_err(wal_index_to_op)?;

        let (durable_watermarks, applied_watermarks) = {
            let apply_start = Instant::now();
            let outcome = {
                let state = Self::namespace_state_mut(&mut proof, namespace.clone());
                match apply_event(state, &sequenced.event_body) {
                    Ok(outcome) => {
                        metrics::apply_ok(apply_start.elapsed());
                        outcome
                    }
                    Err(err) => {
                        metrics::apply_err(apply_start.elapsed());
                        tracing::error!(error = ?err, "apply_event failed");
                        return Err(OpError::Internal("apply_event failed"));
                    }
                }
            };

            let (store_runtime, repo_state) = proof.split_mut();
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
                        source: Box::new(err),
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
                        source: Box::new(err),
                    })
                })?;

            (
                store_runtime.watermarks_durable.clone(),
                store_runtime.watermarks_applied.clone(),
            )
        };

        let result = op_result_from_delta(&parsed_request, &txn_body.delta)?;
        let receipt = DurabilityReceipt::local_fsync(
            store,
            sequenced.event_body.txn_id,
            event_ids.event_ids(),
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
                                acked_by,
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
            DurabilityClass::LocalFsync => MutationOutcome::Immediate(response),
        };

        tracing::info!("mutation committed");
        let state = Self::namespace_state(&proof, &namespace);
        attach_issue_if_created(&namespace, state, outcome.response_mut());
        drop(proof);
        self.mark_checkpoint_dirty(store_id, &namespace, 1);
        self.schedule_sync(remote.clone());
        Ok(outcome)
    }

    /// Create a new bead.
    pub(crate) fn apply_create(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: CreatePayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |actor| ParsedMutationRequest::parse_create(payload, actor),
            git_tx,
        )
    }

    /// Update an existing bead.
    pub(crate) fn apply_update(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: UpdatePayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_update(payload),
            git_tx,
        )
    }

    /// Add labels to a bead.
    pub(crate) fn apply_add_labels(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: LabelsPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_add_labels(payload),
            git_tx,
        )
    }

    /// Remove labels from a bead.
    pub(crate) fn apply_remove_labels(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: LabelsPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_remove_labels(payload),
            git_tx,
        )
    }

    /// Replace the parent relationship for a bead.
    pub(crate) fn apply_set_parent(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: ParentPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_set_parent(payload),
            git_tx,
        )
    }

    /// Close a bead.
    pub(crate) fn apply_close(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: ClosePayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_close(payload),
            git_tx,
        )
    }

    /// Reopen a closed bead.
    pub(crate) fn apply_reopen(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: IdPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_reopen(payload),
            git_tx,
        )
    }

    /// Delete a bead (soft delete via tombstone).
    pub(crate) fn apply_delete(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: DeletePayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_delete(payload),
            git_tx,
        )
    }

    /// Add a dependency.
    pub(crate) fn apply_add_dep(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: DepPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_add_dep(payload),
            git_tx,
        )
    }

    /// Remove a dependency (soft delete).
    pub(crate) fn apply_remove_dep(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: DepPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_remove_dep(payload),
            git_tx,
        )
    }

    /// Add a note to a bead.
    pub(crate) fn apply_add_note(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: AddNotePayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_add_note(payload),
            git_tx,
        )
    }

    /// Claim a bead.
    pub(crate) fn apply_claim(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: ClaimPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_claim(payload),
            git_tx,
        )
    }

    /// Release a claim.
    pub(crate) fn apply_unclaim(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: IdPayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_unclaim(payload),
            git_tx,
        )
    }

    /// Extend an existing claim.
    pub(crate) fn apply_extend_claim(
        &mut self,
        repo: &Path,
        meta: MutationMeta,
        payload: LeasePayload,
        git_tx: &Sender<GitOp>,
    ) -> HandleOutcome {
        self.apply_mutation_request_with(
            repo,
            meta,
            |_actor| ParsedMutationRequest::parse_extend_claim(payload),
            git_tx,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::core::{
        ActorId, Bead, BeadCore, BeadFields, BeadType, CanonicalState, Claim, ClientRequestId,
        DurabilityReceipt, EventId, Labels, Lww, NamespaceId, NoteAppendV1, NoteId, Priority, Seq1,
        Stamp, StoreEpoch, StoreId, StoreIdentity, StoreMeta, StoreMetaVersions, TraceId, TxnId,
        TxnOpV1, WireBeadPatch, WireNoteV1, WireStamp, Workflow, WriteStamp,
    };
    use crate::remote::RemoteUrl;
    use crate::runtime::Clock;
    use crate::runtime::Daemon;
    use crate::runtime::core::{HandleOutcome, insert_store_for_tests};
    use crate::runtime::ipc::{CreatePayload, MutationCtx, MutationMeta, Request, ResponsePayload};
    use crate::runtime::wal::{
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

    fn create_request(repo: &Path, id: &str, title: &str) -> Request {
        Request::Create {
            ctx: MutationCtx::new(repo.to_path_buf(), MutationMeta::default()),
            payload: CreatePayload {
                id: Some(BeadId::parse(id).expect("valid bead id")),
                parent: None,
                title: title.to_string(),
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
            },
        }
    }

    fn open_index_for_store(store_id: StoreId) -> (SqliteWalIndex, StoreMeta) {
        let store_dir = crate::paths::store_dir(store_id);
        let meta_path = crate::paths::store_meta_path(store_id);
        let meta: StoreMeta =
            serde_json::from_str(&std::fs::read_to_string(meta_path).expect("read store meta"))
                .expect("parse store meta");
        let index = SqliteWalIndex::open(&store_dir, &meta, IndexDurabilityMode::Cache)
            .expect("open wal index");
        (index, meta)
    }

    #[derive(Debug, PartialEq, Eq)]
    struct WalIndexSnapshot {
        event_sha: Option<[u8; 32]>,
        durable_seq: Option<u64>,
        durable_head: Option<[u8; 32]>,
        max_origin_seq: u64,
    }

    fn wal_index_snapshot(
        index: &SqliteWalIndex,
        namespace: &NamespaceId,
        origin: ReplicaId,
        seq: Seq1,
    ) -> WalIndexSnapshot {
        let reader = index.reader();
        let event_id = EventId::new(origin, namespace.clone(), seq);
        let event_sha = reader
            .lookup_event_sha(namespace, &event_id)
            .expect("lookup event sha");
        let watermark = reader
            .load_watermarks()
            .expect("load watermarks")
            .into_iter()
            .find(|row| row.namespace == *namespace && row.origin == origin);
        let (durable_seq, durable_head) = watermark
            .map(|row| (Some(row.durable_seq()), row.durable_head_sha()))
            .unwrap_or((None, None));
        let max_origin_seq = reader
            .max_origin_seq(namespace, &origin)
            .expect("max origin seq")
            .get();
        WalIndexSnapshot {
            event_sha,
            durable_seq,
            durable_head,
            max_origin_seq,
        }
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

    struct FixedTimeSource {
        now: Arc<AtomicU64>,
    }

    impl crate::clock::TimeSource for FixedTimeSource {
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
        fn next_dot(&mut self) -> Result<crate::runtime::mutation_engine::DotAllocation, OpError> {
            self.counter += 1;
            Ok(crate::runtime::mutation_engine::DotAllocation {
                dot: Dot {
                    replica: self.replica_id,
                    counter: self.counter,
                },
                durability:
                    crate::runtime::mutation_engine::DotDurabilityEffect::StoreMetaSyncBoundary,
            })
        }
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
            if attrs.metadata().name() == "mutation" {
                self.spans
                    .lock()
                    .expect("span capture")
                    .push(visitor.fields.clone());
            }
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
    }

    #[test]
    fn mutation_span_includes_realtime_context() {
        use crate::telemetry::schema;

        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let _override = crate::paths::override_data_dir_for_tests(Some(data_dir));

        let repo_path = tmp.path().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();

        let mut daemon = Daemon::new(actor_id("trace@test"));
        let store_id = StoreId::new(Uuid::from_bytes([5u8; 16]));
        let remote = RemoteUrl::new("example.com/test/repo");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();

        let spans = Arc::new(Mutex::new(Vec::new()));
        let layer = CaptureLayer::new(spans.clone());
        let subscriber = Registry::default()
            .with(layer)
            .with(tracing_subscriber::filter::LevelFilter::TRACE);

        let (git_tx, _git_rx) = crossbeam::channel::unbounded();
        let request = Request::Create {
            ctx: crate::runtime::ipc::MutationCtx::new(
                repo_path.clone(),
                MutationMeta {
                    // Keep this unset so the request cannot short-circuit through idempotency
                    // reuse and skip creating the `mutation` span under test.
                    client_request_id: None,
                    ..MutationMeta::default()
                },
            ),
            payload: CreatePayload {
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
            },
        };

        let outcome =
            tracing::dispatcher::with_default(&tracing::Dispatch::new(subscriber), || {
                daemon.handle_request(request, &git_tx)
            });
        if let HandleOutcome::Response(crate::runtime::ipc::Response::Err { err }) = &outcome {
            panic!("mutation failed: {err:?}");
        }

        let captured = spans.lock().expect("span capture");
        assert!(!captured.is_empty(), "expected mutation span fields");
        let fields = captured
            .iter()
            .find(|fields| fields.contains_key(schema::STORE_ID))
            .cloned()
            .unwrap_or_default();

        for key in [
            schema::STORE_ID,
            schema::STORE_EPOCH,
            schema::REPLICA_ID,
            schema::ACTOR_ID,
            schema::TXN_ID,
            schema::CLIENT_REQUEST_ID,
            schema::TRACE_ID,
            schema::NAMESPACE,
            schema::ORIGIN_REPLICA_ID,
            schema::ORIGIN_SEQ,
            "planning_dot_meta_sync_boundaries",
            "wal_durability_effect",
        ] {
            assert!(
                fields.contains_key(key),
                "mutation span missing {key}: {fields:?}"
            );
        }
    }

    #[test]
    fn mutation_atomic_commit_failpoint_rolls_back_event_and_watermark() {
        let tmp = TempDir::new().expect("temp dir");
        let data_dir = tmp.path().join("data");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        let _override = crate::paths::override_data_dir_for_tests(Some(data_dir));

        let repo_path = tmp.path().join("repo");
        std::fs::create_dir_all(&repo_path).expect("create repo path");

        let mut daemon = Daemon::new(actor_id("atomic@test"));
        let store_id = StoreId::new(Uuid::from_bytes([31u8; 16]));
        let remote = RemoteUrl::new("example.com/test/repo");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).expect("insert store");
        let (index, meta) = open_index_for_store(store_id);
        let namespace = NamespaceId::core();
        let origin = meta.replica_id;
        let seq = Seq1::from_u64(1).expect("nonzero seq");
        let (git_tx, _git_rx) = crossbeam::channel::unbounded();

        let failpoint = crate::runtime::test_hooks::set_atomic_commit_fail_stage_for_tests(
            "wal_mutation_before_atomic_commit",
        );
        let failed = daemon.handle_request(
            create_request(
                &repo_path,
                "bd-atomic-rollback",
                "atomic rollback should fail first",
            ),
            &git_tx,
        );
        drop(failpoint);
        match failed {
            HandleOutcome::Response(crate::runtime::ipc::Response::Err { .. }) => {}
            other => panic!("expected failed mutation response, got {other:?}"),
        }

        let before = wal_index_snapshot(&index, &namespace, origin, seq);
        assert_eq!(
            before,
            WalIndexSnapshot {
                event_sha: None,
                durable_seq: None,
                durable_head: None,
                max_origin_seq: 0,
            }
        );

        let retried = daemon.handle_request(
            create_request(
                &repo_path,
                "bd-atomic-rollback",
                "atomic rollback should succeed on retry",
            ),
            &git_tx,
        );
        let HandleOutcome::Response(crate::runtime::ipc::Response::Ok { ok }) = retried else {
            panic!("expected successful mutation response");
        };
        assert!(
            matches!(ok, ResponsePayload::Op(_)),
            "expected op response payload"
        );

        let after = wal_index_snapshot(&index, &namespace, origin, seq);
        assert_eq!(after.max_origin_seq, 1);
        assert_eq!(after.durable_seq, Some(1));
        let event_sha = after.event_sha.expect("event row present");
        assert_eq!(after.durable_head, Some(event_sha));
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
            dependencies: Vec::new().into(),
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
                lineage: None,
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
        let versions = StoreMetaVersions::new(1, 2, 1, 1, StoreMetaVersions::INDEX_SCHEMA_VERSION);
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
        let request = ParsedMutationRequest::parse_add_labels(LabelsPayload {
            id: BeadId::parse("bd-123").expect("bead id"),
            labels: vec!["alpha".into()],
        })
        .unwrap();

        let origin_seq = Seq1::new(std::num::NonZeroU64::new(1).unwrap());
        let mut clock = fixed_clock(1_700_000_000_000);
        let now_ms = clock.wall_ms();
        let stamp = Stamp::new(clock.tick(), actor.clone());
        let stamped_ctx = StampedContext::new(ctx.clone(), stamp.clone()).unwrap();
        let mut dots = TestDotAllocator::new(replica_id);
        let planned = engine
            .plan(
                &state,
                now_ms,
                stamped_ctx,
                store,
                None,
                request.clone(),
                &mut dots,
            )
            .unwrap();
        let (draft, _planning_effects) = planned.acknowledge_durability();
        let sequenced = engine
            .build_event(draft, store, ctx.namespace.clone(), replica_id, origin_seq)
            .unwrap();

        let sha = hash_event_body(&sequenced.event_bytes).0;
        let request_proof = sequenced
            .event_body
            .client_request_id
            .map(|client_request_id| RequestProof::Client {
                client_request_id,
                request_sha256: sequenced.request_sha256,
            })
            .unwrap_or(RequestProof::None);
        let record = VerifiedRecord::new(
            RecordHeader {
                origin_replica_id: replica_id,
                origin_seq,
                event_time_ms: sequenced.event_body.event_time_ms,
                txn_id: sequenced.event_body.txn_id,
                request_proof,
                sha256: sha,
                prev_sha256: None,
            },
            sequenced.event_bytes.clone(),
            sequenced.event_body.clone(),
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
                crate::runtime::repl::PeerAckTable::new(),
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

        assert_eq!(response.receipt.txn_id(), sequenced.event_body.txn_id);
        assert_eq!(
            response.receipt.event_ids(),
            vec![EventId::new(replica_id, ctx.namespace.clone(), origin_seq)]
        );
    }
}
