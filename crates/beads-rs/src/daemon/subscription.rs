//! Subscription and backfill planning helpers.

use std::collections::HashMap;
use std::path::Path;

use crossbeam::channel::Sender;

use crate::core::error::details as error_details;
use crate::core::{
    Applied, ErrorPayload, EventFrameV1, Limits, NamespaceId, ProtocolErrorCode, ReplicaId, Seq0,
    Watermark, Watermarks,
};
use crate::daemon::broadcast::{
    BroadcastError, BroadcastEvent, EventSubscription, SubscriberLimits,
};
use crate::daemon::core::Daemon;
use crate::daemon::git_worker::GitOp;
use crate::daemon::ipc::{ReadConsistency, Response, ResponsePayload};
use crate::daemon::ops::OpError;
use crate::daemon::repl::{WalRangeError, WalRangeReader};

pub struct SubscribeReply {
    pub ack: Response,
    pub namespace: NamespaceId,
    pub subscription: EventSubscription,
    pub hot_cache: Vec<BroadcastEvent>,
    pub backfill: Vec<EventFrameV1>,
    pub backfill_end: HashMap<ReplicaId, u64>,
}

pub fn prepare_subscription(
    daemon: &mut Daemon,
    repo: &Path,
    read: ReadConsistency,
    git_tx: &Sender<GitOp>,
) -> Result<SubscribeReply, Box<ErrorPayload>> {
    let loaded = daemon.ensure_repo_fresh(repo, git_tx).map_err(box_error)?;
    let read = daemon
        .normalize_read_consistency(&loaded, read)
        .map_err(box_error)?;
    daemon.check_read_gate(&loaded, &read).map_err(box_error)?;

    let store_runtime = daemon.store_runtime(&loaded).map_err(box_error)?;
    let subscription = store_runtime
        .broadcaster
        .subscribe(subscriber_limits(daemon.limits()))
        .map_err(|err| box_error(broadcast_error_to_op(err)))?;
    let hot_cache = store_runtime
        .broadcaster
        .hot_cache()
        .map_err(|err| box_error(broadcast_error_to_op(err)))?;

    let namespace = read.namespace().clone();
    let watermarks_applied = store_runtime.watermarks_applied.clone();
    let wal_reader = WalRangeReader::new(
        store_runtime.meta.store_id(),
        store_runtime.wal_index.clone(),
        daemon.limits().clone(),
    );
    let backfill = build_backfill_plan(
        read.require_min_seen(),
        &namespace,
        &watermarks_applied,
        &wal_reader,
        daemon.limits(),
    )?;

    let info = crate::api::SubscribeInfo {
        namespace: namespace.clone(),
        watermarks_applied,
    };
    let ack = Response::ok(ResponsePayload::subscribed(info));

    Ok(SubscribeReply {
        ack,
        namespace,
        subscription,
        hot_cache,
        backfill: backfill.frames,
        backfill_end: backfill.last_seq,
    })
}

pub fn subscriber_limits(limits: &Limits) -> SubscriberLimits {
    let max_events = limits.max_event_batch_events.max(1);
    let max_bytes = limits.max_event_batch_bytes.max(1);
    SubscriberLimits::new(max_events, max_bytes).expect("subscriber limits")
}

fn box_error(err: impl Into<ErrorPayload>) -> Box<ErrorPayload> {
    Box::new(err.into())
}

fn broadcast_error_to_op(err: BroadcastError) -> OpError {
    match err {
        BroadcastError::SubscriberLimitReached { max_subscribers } => OpError::Overloaded {
            subsystem: error_details::OverloadedSubsystem::Ipc,
            retry_after_ms: None,
            queue_bytes: None,
            queue_events: Some(max_subscribers as u64),
        },
        BroadcastError::InvalidSubscriberLimits { .. } => {
            OpError::Internal("invalid IPC subscriber limits")
        }
        BroadcastError::LockPoisoned => OpError::Internal("broadcaster lock poisoned"),
    }
}

#[derive(Debug, Default)]
struct BackfillPlan {
    frames: Vec<EventFrameV1>,
    last_seq: HashMap<ReplicaId, u64>,
}

trait WalRangeRead {
    fn read_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    ) -> Result<Vec<EventFrameV1>, WalRangeError>;
}

impl WalRangeRead for WalRangeReader {
    fn read_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    ) -> Result<Vec<EventFrameV1>, WalRangeError> {
        WalRangeReader::read_range(self, namespace, origin, from_seq_excl, max_bytes)
    }
}

fn build_backfill_plan<R: WalRangeRead>(
    required: Option<&Watermarks<Applied>>,
    namespace: &NamespaceId,
    applied: &Watermarks<Applied>,
    wal_reader: &R,
    limits: &Limits,
) -> Result<BackfillPlan, Box<ErrorPayload>> {
    let Some(required) = required else {
        return Ok(BackfillPlan::default());
    };

    let mut plan = BackfillPlan::default();
    for (origin, required_mark) in required.origins(namespace) {
        let required_seq = required_mark.seq().get();
        let current_seq = applied
            .get(namespace, origin)
            .copied()
            .unwrap_or_else(Watermark::genesis)
            .seq()
            .get();
        debug_assert!(
            current_seq >= required_seq,
            "read gate should ensure applied >= required"
        );

        plan.last_seq.insert(*origin, current_seq);
        if current_seq <= required_seq {
            continue;
        }

        let mut from_seq_excl = Seq0::new(required_seq);
        while from_seq_excl.get() < current_seq {
            let frames = wal_reader
                .read_range(
                    namespace,
                    origin,
                    from_seq_excl,
                    limits.max_event_batch_bytes,
                )
                .map_err(wal_range_error_payload)?;
            let Some(last) = frames.last() else {
                return Err(wal_range_error_payload(WalRangeError::MissingRange {
                    namespace: namespace.clone(),
                    origin: *origin,
                    from_seq_excl,
                }));
            };
            from_seq_excl = Seq0::new(last.eid.origin_seq.get());
            plan.frames.extend(frames);
        }
    }

    Ok(plan)
}

fn wal_range_error_payload(err: WalRangeError) -> Box<ErrorPayload> {
    Box::new(match err {
        WalRangeError::MissingRange { namespace, .. } => ErrorPayload::new(
            ProtocolErrorCode::BootstrapRequired.into(),
            "bootstrap required",
            false,
        )
        .with_details(error_details::BootstrapRequiredDetails {
            namespaces: vec![namespace],
            reason: error_details::SnapshotRangeReason::RangeMissing,
        }),
        other => other.as_error_payload(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::cell::RefCell;
    use uuid::Uuid;

    use crate::core::{EventBytes, EventId, HeadStatus, NamespaceId, Opaque, Seq1, Sha256};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct ReadCall {
        namespace: NamespaceId,
        origin: ReplicaId,
        from_seq_excl: Seq0,
        max_bytes: usize,
    }

    #[derive(Default)]
    struct FakeWalReader {
        responses: RefCell<WalReadResponses>,
        calls: RefCell<Vec<ReadCall>>,
    }

    type WalReadResult = Result<Vec<EventFrameV1>, WalRangeError>;
    type WalReadResponses = HashMap<(ReplicaId, Seq0), WalReadResult>;

    impl FakeWalReader {
        fn with_response(
            self,
            origin: ReplicaId,
            from_seq_excl: Seq0,
            response: Result<Vec<EventFrameV1>, WalRangeError>,
        ) -> Self {
            self.responses
                .borrow_mut()
                .insert((origin, from_seq_excl), response);
            self
        }

        fn calls(&self) -> Vec<ReadCall> {
            self.calls.borrow().clone()
        }
    }

    impl WalRangeRead for FakeWalReader {
        fn read_range(
            &self,
            namespace: &NamespaceId,
            origin: &ReplicaId,
            from_seq_excl: Seq0,
            max_bytes: usize,
        ) -> Result<Vec<EventFrameV1>, WalRangeError> {
            self.calls.borrow_mut().push(ReadCall {
                namespace: namespace.clone(),
                origin: *origin,
                from_seq_excl,
                max_bytes,
            });
            self.responses
                .borrow_mut()
                .remove(&(*origin, from_seq_excl))
                .expect("missing wal reader response")
        }
    }

    fn frame(origin: ReplicaId, namespace: NamespaceId, seq: u64) -> EventFrameV1 {
        EventFrameV1 {
            eid: EventId::new(origin, namespace, Seq1::from_u64(seq).unwrap()),
            sha256: Sha256([seq as u8; 32]),
            prev_sha256: None,
            bytes: EventBytes::<Opaque>::new(Bytes::from(vec![seq as u8])),
        }
    }

    fn watermark(seq: u64) -> Watermark<Applied> {
        let head = if seq == 0 {
            HeadStatus::Genesis
        } else {
            HeadStatus::Known([seq as u8; 32])
        };
        Watermark::new(Seq0::new(seq), head).expect("watermark")
    }

    #[test]
    fn backfill_plan_reads_ranges_and_tracks_last_seq() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([5u8; 16]));

        let mut required = Watermarks::<Applied>::new();
        let required_wm = watermark(2);
        required
            .observe_at_least(&namespace, &origin, required_wm.seq(), required_wm.head())
            .unwrap();
        let mut applied = Watermarks::<Applied>::new();
        let applied_wm = watermark(4);
        applied
            .observe_at_least(&namespace, &origin, applied_wm.seq(), applied_wm.head())
            .unwrap();

        let frame3 = frame(origin, namespace.clone(), 3);
        let frame4 = frame(origin, namespace.clone(), 4);
        let reader = FakeWalReader::default()
            .with_response(origin, Seq0::new(2), Ok(vec![frame3.clone()]))
            .with_response(origin, Seq0::new(3), Ok(vec![frame4.clone()]));
        let limits = Limits::default();

        let plan =
            build_backfill_plan(Some(&required), &namespace, &applied, &reader, &limits).unwrap();

        assert_eq!(plan.frames, vec![frame3, frame4]);
        assert_eq!(plan.last_seq.get(&origin), Some(&4));

        let calls = reader.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(
            calls,
            vec![
                ReadCall {
                    namespace: namespace.clone(),
                    origin,
                    from_seq_excl: Seq0::new(2),
                    max_bytes: limits.max_event_batch_bytes,
                },
                ReadCall {
                    namespace: namespace.clone(),
                    origin,
                    from_seq_excl: Seq0::new(3),
                    max_bytes: limits.max_event_batch_bytes,
                },
            ]
        );
    }

    #[test]
    fn backfill_missing_range_is_bootstrap_required() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([9u8; 16]));

        let mut required = Watermarks::<Applied>::new();
        let required_wm = watermark(1);
        required
            .observe_at_least(&namespace, &origin, required_wm.seq(), required_wm.head())
            .unwrap();
        let mut applied = Watermarks::<Applied>::new();
        let applied_wm = watermark(2);
        applied
            .observe_at_least(&namespace, &origin, applied_wm.seq(), applied_wm.head())
            .unwrap();

        let reader = FakeWalReader::default().with_response(
            origin,
            Seq0::new(1),
            Err(WalRangeError::MissingRange {
                namespace: namespace.clone(),
                origin,
                from_seq_excl: Seq0::new(1),
            }),
        );

        let err = *build_backfill_plan(
            Some(&required),
            &namespace,
            &applied,
            &reader,
            &Limits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, ProtocolErrorCode::BootstrapRequired.into());
        let details = err
            .details_as::<error_details::BootstrapRequiredDetails>()
            .unwrap()
            .expect("details");
        assert_eq!(details.namespaces, vec![namespace]);
        assert_eq!(
            details.reason,
            error_details::SnapshotRangeReason::RangeMissing
        );
    }

    #[test]
    fn backfill_corrupt_range_is_corruption() {
        let namespace = NamespaceId::core();
        let origin = ReplicaId::new(Uuid::from_bytes([11u8; 16]));

        let mut required = Watermarks::<Applied>::new();
        let required_wm = watermark(1);
        required
            .observe_at_least(&namespace, &origin, required_wm.seq(), required_wm.head())
            .unwrap();
        let mut applied = Watermarks::<Applied>::new();
        let applied_wm = watermark(2);
        applied
            .observe_at_least(&namespace, &origin, applied_wm.seq(), applied_wm.head())
            .unwrap();

        let reader = FakeWalReader::default().with_response(
            origin,
            Seq0::new(1),
            Err(WalRangeError::Corrupt {
                namespace: namespace.clone(),
                segment_id: None,
                offset: None,
                reason: "boom".to_string(),
            }),
        );

        let err = *build_backfill_plan(
            Some(&required),
            &namespace,
            &applied,
            &reader,
            &Limits::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, ProtocolErrorCode::WalCorrupt.into());
    }
}
