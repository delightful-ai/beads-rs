//! Model: Realtime replication core (events, gaps, WANT, ACK) using production logic.
//!
//! Plan alignment:
//! - EVENTS ordering + contiguity rules: REALTIME_PLAN.md §9.4
//! - ACK monotonicity + applied vs durable: REALTIME_PLAN.md §9.5, §2.3
//! - WANT semantics + gap buffering: REALTIME_PLAN.md §9.6, §9.8
//! - Equivocation detection: REALTIME_PLAN.md §9.4

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::time::Duration;

use beads_rs::model::{
    BufferedEventSnapshot, BufferedPrevSnapshot, GapBufferByNsOrigin, GapBufferByNsOriginSnapshot,
    GapBufferSnapshot, HeadSnapshot, OriginStreamSnapshot, WatermarkSnapshot, event_factory,
    repl_ingest,
};
use beads_rs::{
    ActorId, Applied, Durable, EventFrameError, EventFrameV1, EventId, EventShaLookup, HeadStatus,
    Limits, NamespaceId, ReplicaId, Seq0, Seq1, Sha256, StoreEpoch, StoreId, StoreIdentity,
    TxnDeltaV1, TxnId, Watermark,
};
use stateright::actor::{
    Actor, ActorModel, Envelope, Id, LossyNetwork, Network, Out, model_peers, model_timeout,
    ordered_reliable_link::ActorWrapper, ordered_reliable_link::MsgWrapper,
};
use stateright::{
    Checker, Expectation, Model, Representative, Rewrite, RewritePlan, report::WriteReporter,
};
use uuid::Uuid;

const DEFAULT_REPLICAS: usize = 2;
const MAX_SEQ: u64 = 3;
const GAP_TIMEOUT_TICKS: u64 = 2;
const MAX_GAP_EVENTS: usize = 3;
const MAX_GAP_BYTES: usize = 1024;
const EQUIVOCATE_SEQ: u64 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct StreamKey {
    namespace: NamespaceId,
    origin: ReplicaId,
}

impl StreamKey {
    fn new(namespace: NamespaceId, origin: ReplicaId) -> Self {
        Self { namespace, origin }
    }
}

impl Rewrite<Id> for StreamKey {
    fn rewrite<S>(&self, plan: &RewritePlan<Id, S>) -> Self {
        Self {
            namespace: self.namespace.clone(),
            origin: rewrite_replica_id(self.origin, plan),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct FrameDigest {
    eid: EventId,
    sha256: Sha256,
    prev_sha256: Option<Sha256>,
}

impl PartialOrd for FrameDigest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FrameDigest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let lhs_prev = self.prev_sha256.map(|s| s.0);
        let rhs_prev = other.prev_sha256.map(|s| s.0);
        (self.eid.clone(), self.sha256.0, lhs_prev).cmp(&(
            other.eid.clone(),
            other.sha256.0,
            rhs_prev,
        ))
    }
}

impl Rewrite<Id> for FrameDigest {
    fn rewrite<S>(&self, plan: &RewritePlan<Id, S>) -> Self {
        Self {
            eid: rewrite_event_id(&self.eid, plan),
            sha256: self.sha256,
            prev_sha256: self.prev_sha256,
        }
    }
}

#[derive(Clone, Debug)]
struct FrameMsg {
    digest: FrameDigest,
    frame: EventFrameV1,
}

impl FrameMsg {
    fn new(frame: EventFrameV1) -> Self {
        let digest = FrameDigest {
            eid: frame.eid.clone(),
            sha256: frame.sha256,
            prev_sha256: frame.prev_sha256,
        };
        Self { digest, frame }
    }
}

impl Rewrite<Id> for FrameMsg {
    fn rewrite<S>(&self, plan: &RewritePlan<Id, S>) -> Self {
        Self {
            digest: self.digest.rewrite(plan),
            frame: self.frame.clone(),
        }
    }
}

impl PartialEq for FrameMsg {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl Eq for FrameMsg {}

impl Hash for FrameMsg {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.digest.hash(state);
    }
}

impl PartialOrd for FrameMsg {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FrameMsg {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.digest.cmp(&other.digest)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(clippy::large_enum_variant)]
enum ReplMsg {
    Event(FrameMsg),
    Want { key: StreamKey, from: Seq0 },
    Ack { key: StreamKey, durable: Seq0 },
}

impl Rewrite<Id> for ReplMsg {
    fn rewrite<S>(&self, plan: &RewritePlan<Id, S>) -> Self {
        match self {
            ReplMsg::Event(frame) => ReplMsg::Event(frame.rewrite(plan)),
            ReplMsg::Want { key, from } => ReplMsg::Want {
                key: key.rewrite(plan),
                from: *from,
            },
            ReplMsg::Ack { key, durable } => ReplMsg::Ack {
                key: key.rewrite(plan),
                durable: *durable,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum TimerTag {
    Tick,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct History {
    sent: BTreeSet<FrameDigest>,
    delivered: BTreeSet<FrameDigest>,
    last_delivered: BTreeMap<(Id, StreamKey), Seq0>,
    last_want_out: BTreeMap<(Id, StreamKey), Seq0>,
    last_ack_out: BTreeMap<(Id, StreamKey), Seq0>,
    ack_monotonic: bool,
    want_not_ahead: bool,
}

impl Default for History {
    fn default() -> Self {
        Self {
            sent: BTreeSet::new(),
            delivered: BTreeSet::new(),
            last_delivered: BTreeMap::new(),
            last_want_out: BTreeMap::new(),
            last_ack_out: BTreeMap::new(),
            ack_monotonic: true,
            want_not_ahead: true,
        }
    }
}

impl Rewrite<Id> for History {
    fn rewrite<S>(&self, plan: &RewritePlan<Id, S>) -> Self {
        Self {
            sent: rewrite_frame_set(&self.sent, plan),
            delivered: rewrite_frame_set(&self.delivered, plan),
            last_delivered: rewrite_id_streamkey_map(&self.last_delivered, plan),
            last_want_out: rewrite_id_streamkey_map(&self.last_want_out, plan),
            last_ack_out: rewrite_id_streamkey_map(&self.last_ack_out, plan),
            ack_monotonic: self.ack_monotonic,
            want_not_ahead: self.want_not_ahead,
        }
    }
}

#[derive(Clone, Debug)]
struct ReplActor {
    store: StoreIdentity,
    namespaces: Vec<NamespaceId>,
    limits: Limits,
    max_seq: u64,
    equivocate: bool,
    peer_count: usize,
}

impl ReplActor {
    fn build_frames(&self, id: Id) -> Vec<FrameMsg> {
        let replica_id = replica_id_for(id);
        let actor_id = actor_id_for(id);
        let mut frames = Vec::new();
        for namespace in &self.namespaces {
            let factory = event_factory::EventFactory::new(
                self.store,
                namespace.clone(),
                replica_id,
                actor_id.clone(),
            );
            let mut prev_sha = None;
            for seq in 1..=self.max_seq {
                let seq1 = Seq1::from_u64(seq).expect("seq1");
                let prev_for_seq = prev_sha;
                let txn_id = TxnId::new(make_uuid(replica_id, seq, 0));
                let body = factory.txn_body(seq1, txn_id, seq, 0, TxnDeltaV1::new(), None);
                let frame = event_factory::encode_frame(&body, prev_for_seq).expect("encode frame");
                prev_sha = Some(frame.sha256);
                frames.push(FrameMsg::new(frame));

                if self.equivocate && seq == EQUIVOCATE_SEQ {
                    let txn_id = TxnId::new(make_uuid(replica_id, seq, 1));
                    let body =
                        factory.txn_body(seq1, txn_id, seq + 100, 0, TxnDeltaV1::new(), None);
                    let frame =
                        event_factory::encode_frame(&body, prev_for_seq).expect("encode frame");
                    frames.push(FrameMsg::new(frame));
                }
            }
        }
        frames
    }
}

impl Actor for ReplActor {
    type Msg = ReplMsg;
    type State = NodeState;
    type Timer = TimerTag;
    type Random = ();
    type Storage = ();

    fn on_start(&self, id: Id, _storage: &Option<Self::Storage>, o: &mut Out<Self>) -> Self::State {
        o.set_timer(TimerTag::Tick, model_timeout());

        let frames = self.build_frames(id);
        let peers = model_peers(usize::from(id), self.peer_count);
        for peer in peers {
            for frame in &frames {
                o.send(peer, ReplMsg::Event(frame.clone()));
            }
        }

        NodeState::new(self.limits.clone())
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        let state = state.to_mut();
        match msg {
            ReplMsg::Event(frame_msg) => {
                if state.errored {
                    return;
                }

                state.last_effect = None;
                state.last_want = None;
                state.now_ms = state.now_ms.saturating_add(1);

                let before = state.core_digest();
                let effect = state.ingest_frame(&frame_msg.frame, &self.limits, self.store);
                let after = state.core_digest();
                let changed = before != after;
                state.last_effect = Some(LastEffect {
                    kind: effect,
                    changed,
                });

                if let Some(last_want) = state.last_want.clone() {
                    o.send(
                        src,
                        ReplMsg::Want {
                            key: last_want.key.clone(),
                            from: last_want.from,
                        },
                    );
                }

                if let Some(last_effect) = state.last_effect.as_ref() {
                    let key = match &last_effect.kind {
                        LastEffectKind::Applied { key, .. }
                        | LastEffectKind::Buffered { key, .. }
                        | LastEffectKind::Duplicate { key, .. } => Some(key),
                        LastEffectKind::Rejected { .. } => None,
                    };
                    if let Some(key) = key {
                        if let Some(seq) = state.ack_durable.get(key) {
                            o.send(
                                src,
                                ReplMsg::Ack {
                                    key: key.clone(),
                                    durable: *seq,
                                },
                            );
                        }
                    }
                }
            }
            ReplMsg::Want { .. } | ReplMsg::Ack { .. } => {}
        }
    }

    fn on_timeout(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        _timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        let state = state.to_mut();
        if state.errored {
            return;
        }
        state.now_ms = state.now_ms.saturating_add(1);
        o.set_timer(TimerTag::Tick, model_timeout());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct WantInfo {
    key: StreamKey,
    from: Seq0,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum LastEffectKind {
    Applied {
        key: StreamKey,
        seq: Seq1,
        prev_seen: Seq0,
    },
    Buffered {
        key: StreamKey,
        seq: Seq1,
    },
    Duplicate {
        key: StreamKey,
        seq: Seq1,
    },
    Rejected {
        code: &'static str,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct LastEffect {
    kind: LastEffectKind,
    changed: bool,
}

#[derive(Clone, Debug)]
struct NodeState {
    now_ms: u64,
    gap: GapBufferByNsOrigin,
    durable: BTreeMap<StreamKey, Watermark<Durable>>,
    applied: BTreeMap<StreamKey, Watermark<Applied>>,
    event_store: BTreeMap<EventId, Sha256>,
    ack_durable: BTreeMap<StreamKey, Seq0>,
    ack_max: BTreeMap<StreamKey, Seq0>,
    durable_max: BTreeMap<StreamKey, Seq0>,
    errored: bool,
    equivocation_seen: bool,
    last_effect: Option<LastEffect>,
    last_want: Option<WantInfo>,
    digest_override: Option<StateDigest>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum HeadDigest {
    Genesis,
    Known(Sha256),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WatermarkDigest {
    seq: Seq0,
    head: HeadDigest,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CoreDigest {
    now_ms: u64,
    gap: GapBufferByNsOriginSnapshot,
    durable: BTreeMap<StreamKey, WatermarkDigest>,
    applied: BTreeMap<StreamKey, WatermarkDigest>,
    event_store: BTreeMap<EventId, Sha256>,
    ack_durable: BTreeMap<StreamKey, Seq0>,
    ack_max: BTreeMap<StreamKey, Seq0>,
    durable_max: BTreeMap<StreamKey, Seq0>,
    errored: bool,
    equivocation_seen: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct StateDigest {
    core: CoreDigest,
    last_effect: Option<LastEffect>,
    last_want: Option<WantInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum HeadOrderKey {
    Genesis,
    Known([u8; 32]),
}

impl From<&HeadSnapshot> for HeadOrderKey {
    fn from(head: &HeadSnapshot) -> Self {
        match head {
            HeadSnapshot::Genesis => HeadOrderKey::Genesis,
            HeadSnapshot::Known(bytes) => HeadOrderKey::Known(bytes.0),
        }
    }
}

impl From<&HeadDigest> for HeadOrderKey {
    fn from(head: &HeadDigest) -> Self {
        match head {
            HeadDigest::Genesis => HeadOrderKey::Genesis,
            HeadDigest::Known(bytes) => HeadOrderKey::Known(bytes.0),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct WatermarkOrderKey {
    seq: Seq0,
    head: HeadOrderKey,
}

impl From<&WatermarkSnapshot> for WatermarkOrderKey {
    fn from(wm: &WatermarkSnapshot) -> Self {
        Self {
            seq: wm.seq,
            head: HeadOrderKey::from(&wm.head),
        }
    }
}

impl From<&WatermarkDigest> for WatermarkOrderKey {
    fn from(wm: &WatermarkDigest) -> Self {
        Self {
            seq: wm.seq,
            head: HeadOrderKey::from(&wm.head),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum BufferedPrevOrderKey {
    Contiguous {
        prev: Option<[u8; 32]>,
    },
    Deferred {
        prev: [u8; 32],
        expected_prev_seq: Seq1,
    },
}

impl From<&BufferedPrevSnapshot> for BufferedPrevOrderKey {
    fn from(prev: &BufferedPrevSnapshot) -> Self {
        match prev {
            BufferedPrevSnapshot::Contiguous { prev } => BufferedPrevOrderKey::Contiguous {
                prev: prev.map(|sha| sha.0),
            },
            BufferedPrevSnapshot::Deferred {
                prev,
                expected_prev_seq,
            } => BufferedPrevOrderKey::Deferred {
                prev: prev.0,
                expected_prev_seq: *expected_prev_seq,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BufferedEventOrderKey {
    seq: Seq1,
    sha256: [u8; 32],
    prev: BufferedPrevOrderKey,
    bytes_len: usize,
}

impl From<&BufferedEventSnapshot> for BufferedEventOrderKey {
    fn from(ev: &BufferedEventSnapshot) -> Self {
        Self {
            seq: ev.seq,
            sha256: ev.sha256.0,
            prev: BufferedPrevOrderKey::from(&ev.prev),
            bytes_len: ev.bytes_len,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct GapBufferOrderKey {
    buffered: Vec<BufferedEventOrderKey>,
    buffered_bytes: usize,
    started_at_ms: Option<u64>,
    max_events: usize,
    max_bytes: usize,
    timeout_ms: u64,
}

impl From<&GapBufferSnapshot> for GapBufferOrderKey {
    fn from(gap: &GapBufferSnapshot) -> Self {
        Self {
            buffered: gap
                .buffered
                .iter()
                .map(BufferedEventOrderKey::from)
                .collect(),
            buffered_bytes: gap.buffered_bytes,
            started_at_ms: gap.started_at_ms,
            max_events: gap.max_events,
            max_bytes: gap.max_bytes,
            timeout_ms: gap.timeout_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OriginStreamOrderKey {
    namespace: NamespaceId,
    origin: ReplicaId,
    durable: WatermarkOrderKey,
    gap: GapBufferOrderKey,
}

impl From<&OriginStreamSnapshot> for OriginStreamOrderKey {
    fn from(origin: &OriginStreamSnapshot) -> Self {
        Self {
            namespace: origin.namespace.clone(),
            origin: origin.origin,
            durable: WatermarkOrderKey::from(&origin.durable),
            gap: GapBufferOrderKey::from(&origin.gap),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct GapBufferByNsOriginOrderKey {
    origins: Vec<OriginStreamOrderKey>,
}

impl From<&GapBufferByNsOriginSnapshot> for GapBufferByNsOriginOrderKey {
    fn from(gap: &GapBufferByNsOriginSnapshot) -> Self {
        Self {
            origins: gap.origins.iter().map(OriginStreamOrderKey::from).collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CoreOrderKey {
    now_ms: u64,
    gap: GapBufferByNsOriginOrderKey,
    durable: Vec<(StreamKey, WatermarkOrderKey)>,
    applied: Vec<(StreamKey, WatermarkOrderKey)>,
    event_store: Vec<(EventId, [u8; 32])>,
    ack_durable: Vec<(StreamKey, Seq0)>,
    ack_max: Vec<(StreamKey, Seq0)>,
    durable_max: Vec<(StreamKey, Seq0)>,
    errored: bool,
    equivocation_seen: bool,
}

impl From<&CoreDigest> for CoreOrderKey {
    fn from(digest: &CoreDigest) -> Self {
        Self {
            now_ms: digest.now_ms,
            gap: GapBufferByNsOriginOrderKey::from(&digest.gap),
            durable: digest
                .durable
                .iter()
                .map(|(key, wm)| (key.clone(), WatermarkOrderKey::from(wm)))
                .collect(),
            applied: digest
                .applied
                .iter()
                .map(|(key, wm)| (key.clone(), WatermarkOrderKey::from(wm)))
                .collect(),
            event_store: digest
                .event_store
                .iter()
                .map(|(key, sha)| (key.clone(), sha.0))
                .collect(),
            ack_durable: digest
                .ack_durable
                .iter()
                .map(|(key, seq)| (key.clone(), *seq))
                .collect(),
            ack_max: digest
                .ack_max
                .iter()
                .map(|(key, seq)| (key.clone(), *seq))
                .collect(),
            durable_max: digest
                .durable_max
                .iter()
                .map(|(key, seq)| (key.clone(), *seq))
                .collect(),
            errored: digest.errored,
            equivocation_seen: digest.equivocation_seen,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StateOrderKey {
    core: CoreOrderKey,
    last_effect: Option<LastEffect>,
    last_want: Option<WantInfo>,
}

impl From<&StateDigest> for StateOrderKey {
    fn from(digest: &StateDigest) -> Self {
        Self {
            core: CoreOrderKey::from(&digest.core),
            last_effect: digest.last_effect.clone(),
            last_want: digest.last_want.clone(),
        }
    }
}

impl NodeState {
    fn new(limits: Limits) -> Self {
        Self {
            now_ms: 0,
            gap: GapBufferByNsOrigin::new(limits),
            durable: BTreeMap::new(),
            applied: BTreeMap::new(),
            event_store: BTreeMap::new(),
            ack_durable: BTreeMap::new(),
            ack_max: BTreeMap::new(),
            durable_max: BTreeMap::new(),
            errored: false,
            equivocation_seen: false,
            last_effect: None,
            last_want: None,
            digest_override: None,
        }
    }

    fn core_digest(&self) -> CoreDigest {
        CoreDigest {
            now_ms: self.now_ms,
            gap: self.gap.model_snapshot(),
            durable: digest_watermarks(&self.durable),
            applied: digest_watermarks(&self.applied),
            event_store: self.event_store.clone(),
            ack_durable: self.ack_durable.clone(),
            ack_max: self.ack_max.clone(),
            durable_max: self.durable_max.clone(),
            errored: self.errored,
            equivocation_seen: self.equivocation_seen,
        }
    }

    fn digest(&self) -> StateDigest {
        if let Some(digest) = &self.digest_override {
            return digest.clone();
        }
        StateDigest {
            core: self.core_digest(),
            last_effect: self.last_effect.clone(),
            last_want: self.last_want.clone(),
        }
    }

    fn ingest_frame(
        &mut self,
        frame: &EventFrameV1,
        limits: &Limits,
        store: StoreIdentity,
    ) -> LastEffectKind {
        let namespace = frame.eid.namespace.clone();
        let origin = frame.eid.origin_replica_id;
        let key = StreamKey::new(namespace.clone(), origin);
        let durable = self
            .durable
            .get(&key)
            .copied()
            .unwrap_or_else(Watermark::genesis);
        let expected_prev = expected_prev_head(durable, frame.eid.origin_seq);
        let lookup = EventLookup {
            events: &self.event_store,
        };

        let verified = match repl_ingest::verify_frame(frame, limits, store, expected_prev, &lookup)
        {
            Ok(event) => event,
            Err(err) => {
                if matches!(err, EventFrameError::Equivocation) {
                    self.equivocation_seen = true;
                }
                self.errored = true;
                return LastEffectKind::Rejected {
                    code: frame_error_code(&err),
                };
            }
        };

        let prev_seen = durable.seq();
        match self
            .gap
            .ingest(namespace.clone(), origin, durable, verified, self.now_ms)
        {
            beads_rs::model::IngestDecision::ForwardContiguousBatch(batch) => {
                if let Err(code) = self.apply_batch(&namespace, origin, &batch) {
                    self.errored = true;
                    return LastEffectKind::Rejected { code };
                }
                if let Err(code) = self.drain_gap_ready(&namespace, origin) {
                    self.errored = true;
                    return LastEffectKind::Rejected { code };
                }
                self.refresh_acks();
                self.refresh_maxes();
                LastEffectKind::Applied {
                    key,
                    seq: batch
                        .first()
                        .map(|ev| ev.seq())
                        .unwrap_or_else(|| prev_seen.next()),
                    prev_seen,
                }
            }
            beads_rs::model::IngestDecision::BufferedNeedWant { want_from } => {
                self.last_want = Some(WantInfo {
                    key: key.clone(),
                    from: want_from,
                });
                self.refresh_acks();
                self.refresh_maxes();
                LastEffectKind::Buffered {
                    key,
                    seq: frame.eid.origin_seq,
                }
            }
            beads_rs::model::IngestDecision::DuplicateNoop => {
                self.refresh_acks();
                self.refresh_maxes();
                LastEffectKind::Duplicate {
                    key,
                    seq: frame.eid.origin_seq,
                }
            }
            beads_rs::model::IngestDecision::Reject { reason } => {
                self.errored = true;
                LastEffectKind::Rejected {
                    code: reject_reason_code(&reason),
                }
            }
        }
    }

    fn apply_batch(
        &mut self,
        namespace: &NamespaceId,
        origin: ReplicaId,
        batch: &[beads_rs::VerifiedEvent<beads_rs::PrevVerified>],
    ) -> Result<(), &'static str> {
        if batch.is_empty() {
            return Ok(());
        }

        for ev in batch {
            let eid = EventId::new(
                ev.body.origin_replica_id,
                namespace.clone(),
                ev.body.origin_seq,
            );
            self.event_store.insert(eid, ev.sha256);
        }

        self.gap
            .advance_durable_batch(namespace, &origin, batch)
            .map_err(|_| "gap_advance")?;

        let last = batch.last().expect("batch non-empty");
        let seq0 = Seq0::new(last.seq().get());
        let head = HeadStatus::Known(last.sha256.0);
        let durable = Watermark::new(seq0, head).map_err(|_| "watermark")?;
        let applied = Watermark::new(seq0, head).map_err(|_| "watermark")?;
        let key = StreamKey::new(namespace.clone(), origin);
        self.durable.insert(key.clone(), durable);
        self.applied.insert(key, applied);
        Ok(())
    }

    fn drain_gap_ready(
        &mut self,
        namespace: &NamespaceId,
        origin: ReplicaId,
    ) -> Result<(), &'static str> {
        loop {
            let batch = self
                .gap
                .drain_ready(namespace, &origin)
                .map_err(|_| "drain_prev_mismatch")?;
            let Some(batch) = batch else {
                return Ok(());
            };
            self.apply_batch(namespace, origin, &batch)?;
        }
    }

    fn refresh_acks(&mut self) {
        for (key, wm) in &self.durable {
            self.ack_durable.insert(key.clone(), wm.seq());
        }
    }

    fn refresh_maxes(&mut self) {
        for (key, wm) in &self.durable {
            let seq = wm.seq();
            let entry = self.durable_max.entry(key.clone()).or_insert(Seq0::ZERO);
            if seq > *entry {
                *entry = seq;
            }

            let entry = self.ack_max.entry(key.clone()).or_insert(Seq0::ZERO);
            if seq > *entry {
                *entry = seq;
            }
        }
    }
}

impl PartialEq for NodeState {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl Eq for NodeState {}

impl Hash for NodeState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.digest().hash(state);
    }
}

impl PartialOrd for NodeState {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeState {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        StateOrderKey::from(&self.digest()).cmp(&StateOrderKey::from(&other.digest()))
    }
}

impl Rewrite<Id> for NodeState {
    fn rewrite<S>(&self, plan: &RewritePlan<Id, S>) -> Self {
        let mut next = self.clone();
        next.digest_override = Some(rewrite_state_digest(self, plan));
        next
    }
}

fn digest_watermarks<K>(
    map: &BTreeMap<StreamKey, Watermark<K>>,
) -> BTreeMap<StreamKey, WatermarkDigest> {
    map.iter()
        .map(|(key, wm)| (key.clone(), WatermarkDigest::from_watermark(*wm)))
        .collect()
}

impl WatermarkDigest {
    fn from_watermark<K>(wm: Watermark<K>) -> Self {
        let head = match wm.head() {
            HeadStatus::Genesis => HeadDigest::Genesis,
            HeadStatus::Known(bytes) => HeadDigest::Known(Sha256(bytes)),
        };
        Self {
            seq: wm.seq(),
            head,
        }
    }
}

struct EventLookup<'a> {
    events: &'a BTreeMap<EventId, Sha256>,
}

impl EventShaLookup for EventLookup<'_> {
    fn lookup_event_sha(
        &self,
        eid: &EventId,
    ) -> Result<Option<Sha256>, beads_rs::EventShaLookupError> {
        Ok(self.events.get(eid).copied())
    }
}

fn rewrite_streamkey_map<S, V: Clone>(
    map: &BTreeMap<StreamKey, V>,
    plan: &RewritePlan<Id, S>,
) -> BTreeMap<StreamKey, V> {
    map.iter()
        .map(|(key, value)| (key.rewrite(plan), value.clone()))
        .collect()
}

fn rewrite_event_store<S>(
    map: &BTreeMap<EventId, Sha256>,
    plan: &RewritePlan<Id, S>,
) -> BTreeMap<EventId, Sha256> {
    map.iter()
        .map(|(eid, sha)| (rewrite_event_id(eid, plan), *sha))
        .collect()
}

fn rewrite_gap_snapshot<S>(
    snapshot: &GapBufferByNsOriginSnapshot,
    plan: &RewritePlan<Id, S>,
) -> GapBufferByNsOriginSnapshot {
    let mut origins = snapshot
        .origins
        .iter()
        .map(|origin| OriginStreamSnapshot {
            namespace: origin.namespace.clone(),
            origin: rewrite_replica_id(origin.origin, plan),
            durable: origin.durable.clone(),
            gap: origin.gap.clone(),
        })
        .collect::<Vec<_>>();
    origins.sort_by(|lhs, rhs| {
        (lhs.namespace.clone(), lhs.origin).cmp(&(rhs.namespace.clone(), rhs.origin))
    });
    GapBufferByNsOriginSnapshot { origins }
}

fn rewrite_last_effect<S>(effect: &LastEffect, plan: &RewritePlan<Id, S>) -> LastEffect {
    let kind = match &effect.kind {
        LastEffectKind::Applied {
            key,
            seq,
            prev_seen,
        } => LastEffectKind::Applied {
            key: key.rewrite(plan),
            seq: *seq,
            prev_seen: *prev_seen,
        },
        LastEffectKind::Buffered { key, seq } => LastEffectKind::Buffered {
            key: key.rewrite(plan),
            seq: *seq,
        },
        LastEffectKind::Duplicate { key, seq } => LastEffectKind::Duplicate {
            key: key.rewrite(plan),
            seq: *seq,
        },
        LastEffectKind::Rejected { code } => LastEffectKind::Rejected { code },
    };
    LastEffect {
        kind,
        changed: effect.changed,
    }
}

fn rewrite_want_info<S>(want: &WantInfo, plan: &RewritePlan<Id, S>) -> WantInfo {
    WantInfo {
        key: want.key.rewrite(plan),
        from: want.from,
    }
}

fn rewrite_core_digest<S>(digest: &CoreDigest, plan: &RewritePlan<Id, S>) -> CoreDigest {
    CoreDigest {
        now_ms: digest.now_ms,
        gap: rewrite_gap_snapshot(&digest.gap, plan),
        durable: rewrite_streamkey_map(&digest.durable, plan),
        applied: rewrite_streamkey_map(&digest.applied, plan),
        event_store: rewrite_event_store(&digest.event_store, plan),
        ack_durable: rewrite_streamkey_map(&digest.ack_durable, plan),
        ack_max: rewrite_streamkey_map(&digest.ack_max, plan),
        durable_max: rewrite_streamkey_map(&digest.durable_max, plan),
        errored: digest.errored,
        equivocation_seen: digest.equivocation_seen,
    }
}

fn rewrite_state_digest<S>(state: &NodeState, plan: &RewritePlan<Id, S>) -> StateDigest {
    let core = rewrite_core_digest(&state.core_digest(), plan);
    StateDigest {
        core,
        last_effect: state
            .last_effect
            .as_ref()
            .map(|effect| rewrite_last_effect(effect, plan)),
        last_want: state
            .last_want
            .as_ref()
            .map(|want| rewrite_want_info(want, plan)),
    }
}

fn expected_prev_head(durable: Watermark<Durable>, seq: Seq1) -> Option<Sha256> {
    if seq.get() != durable.seq().get().saturating_add(1) {
        return None;
    }
    match durable.head() {
        HeadStatus::Genesis => None,
        HeadStatus::Known(bytes) => Some(Sha256(bytes)),
    }
}

fn frame_error_code(err: &EventFrameError) -> &'static str {
    match err {
        EventFrameError::WrongStore { .. } => "wrong_store",
        EventFrameError::FrameMismatch => "frame_mismatch",
        EventFrameError::HashMismatch => "hash_mismatch",
        EventFrameError::PrevMismatch => "prev_mismatch",
        EventFrameError::Validation(_) => "validation",
        EventFrameError::Decode(_) => "decode",
        EventFrameError::Lookup(_) => "lookup",
        EventFrameError::Equivocation => "equivocation",
    }
}

fn reject_reason_code(reason: &beads_rs::core::error::details::ReplRejectReason) -> &'static str {
    use beads_rs::core::error::details::ReplRejectReason as R;
    match reason {
        R::PrevUnknown => "prev_unknown",
        R::GapTimeout => "gap_timeout",
        R::GapBufferOverflow => "gap_buffer_overflow",
        R::GapBufferBytesOverflow => "gap_buffer_bytes_overflow",
    }
}

fn replica_id_for(id: Id) -> ReplicaId {
    let idx = usize::from(id);
    ReplicaId::new(Uuid::from_bytes([idx as u8 + 1; 16]))
}

fn replica_id_to_id(replica_id: ReplicaId) -> Option<Id> {
    let uuid = replica_id.as_uuid();
    let bytes = uuid.as_bytes();
    let first = bytes[0];
    if first == 0 || bytes.iter().any(|b| *b != first) {
        return None;
    }
    let idx = first.saturating_sub(1) as usize;
    Some(Id::from(idx))
}

fn rewrite_replica_id<S>(replica_id: ReplicaId, plan: &RewritePlan<Id, S>) -> ReplicaId {
    match replica_id_to_id(replica_id) {
        Some(id) => replica_id_for(id.rewrite(plan)),
        None => replica_id,
    }
}

fn rewrite_event_id<S>(eid: &EventId, plan: &RewritePlan<Id, S>) -> EventId {
    let origin = rewrite_replica_id(eid.origin_replica_id, plan);
    EventId::new(origin, eid.namespace.clone(), eid.origin_seq)
}

fn actor_id_for(id: Id) -> ActorId {
    let idx = usize::from(id);
    ActorId::new(format!("replica-{idx}")).expect("actor id")
}

fn make_uuid(replica: ReplicaId, seq: u64, variant: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    let replica_uuid = replica.as_uuid();
    let seed = replica_uuid.as_bytes();
    bytes[0] = seed[0];
    bytes[1] = seed[1];
    bytes[2] = seq as u8;
    bytes[3] = variant;
    Uuid::from_bytes(bytes)
}

fn build_limits() -> Limits {
    Limits {
        repl_gap_timeout_ms: GAP_TIMEOUT_TICKS,
        max_repl_gap_events: MAX_GAP_EVENTS,
        max_repl_gap_bytes: MAX_GAP_BYTES,
        ..Limits::default()
    }
}

fn build_actors(
    store: StoreIdentity,
    namespaces: Vec<NamespaceId>,
    replica_count: usize,
    equivocate: bool,
) -> Vec<ReplActor> {
    let limits = build_limits();
    let template = ReplActor {
        store,
        namespaces,
        limits,
        max_seq: MAX_SEQ,
        equivocate,
        peer_count: replica_count,
    };
    (0..replica_count).map(|_| template.clone()).collect()
}

fn add_history_properties<A>(model: ActorModel<A, (), History>) -> ActorModel<A, (), History>
where
    A: Actor,
    A::Msg: Ord,
    A::Timer: Ord,
{
    model
        .property(Expectation::Always, "history ack monotonic", |_, s| {
            s.history.ack_monotonic
        })
        .property(
            Expectation::Always,
            "history wants not ahead of delivered",
            |_, s| s.history.want_not_ahead,
        )
}

fn add_base_properties(
    model: ActorModel<ReplActor, (), History>,
) -> ActorModel<ReplActor, (), History> {
    let model = add_history_properties(model);
    model
        .property(
            Expectation::Always,
            "ack never exceeds contiguous seen",
            |_, s| {
                s.actor_states.iter().all(|state| {
                    state.ack_durable.iter().all(|(key, ack)| {
                        let seen = state
                            .durable
                            .get(key)
                            .map(|wm| wm.seq())
                            .unwrap_or(Seq0::ZERO);
                        *ack <= seen
                    })
                })
            },
        )
        .property(Expectation::Always, "seen map is monotonic", |_, s| {
            s.actor_states.iter().all(|state| {
                state.durable.iter().all(|(key, wm)| {
                    state.durable_max.get(key).copied().unwrap_or(Seq0::ZERO) == wm.seq()
                })
            })
        })
        .property(Expectation::Always, "ack map is monotonic", |_, s| {
            s.actor_states.iter().all(|state| {
                state
                    .ack_durable
                    .iter()
                    .all(|(key, ack)| state.ack_max.get(key).copied().unwrap_or(Seq0::ZERO) == *ack)
            })
        })
        .property(
            Expectation::Always,
            "contiguous seen implies applied prefix",
            |_, s| {
                s.actor_states.iter().all(|state| {
                    state.durable.iter().all(|(key, wm)| {
                        let seen = wm.seq().get();
                        (1..=seen).all(|seq| {
                            let seq1 = Seq1::from_u64(seq).expect("seq1");
                            let eid = EventId::new(key.origin, key.namespace.clone(), seq1);
                            state.event_store.contains_key(&eid)
                        })
                    })
                })
            },
        )
        .property(
            Expectation::Always,
            "applies only the next contiguous seq",
            |_, s| {
                s.actor_states
                    .iter()
                    .all(|state| match state.last_effect.as_ref() {
                        Some(LastEffect {
                            kind: LastEffectKind::Applied { seq, prev_seen, .. },
                            ..
                        }) => seq.get() == prev_seen.get().saturating_add(1),
                        _ => true,
                    })
            },
        )
        .property(
            Expectation::Always,
            "buffered items are beyond next expected",
            |_, s| {
                s.actor_states.iter().all(|state| {
                    let snapshot = state.gap.model_snapshot();
                    snapshot.origins.iter().all(|origin| {
                        let durable_seq = origin.durable.seq.get();
                        origin
                            .gap
                            .buffered
                            .iter()
                            .all(|ev| ev.seq.get() > durable_seq.saturating_add(1))
                    })
                })
            },
        )
        .property(
            Expectation::Always,
            "buffer stays within bounds unless closed",
            |_, s| {
                s.actor_states.iter().all(|state| {
                    if state.errored {
                        return true;
                    }
                    let snapshot = state.gap.model_snapshot();
                    snapshot.origins.iter().all(|origin| {
                        origin.gap.buffered.len() <= origin.gap.max_events
                            && origin.gap.buffered_bytes <= origin.gap.max_bytes
                    })
                })
            },
        )
        .property(
            Expectation::Always,
            "equivocation implies hard close",
            |_, s| {
                s.actor_states
                    .iter()
                    .all(|state| !state.equivocation_seen || state.errored)
            },
        )
        .property(
            Expectation::Always,
            "duplicate deliveries are idempotent",
            |_, s| {
                s.actor_states
                    .iter()
                    .all(|state| match state.last_effect.as_ref() {
                        Some(LastEffect {
                            kind: LastEffectKind::Duplicate { .. },
                            changed,
                        }) => !*changed,
                        _ => true,
                    })
            },
        )
        .property(
            Expectation::Always,
            "want from equals seen when emitted",
            |_, s| {
                s.actor_states
                    .iter()
                    .all(|state| match state.last_want.as_ref() {
                        None => true,
                        Some(WantInfo { key, from }) => {
                            let seen = state
                                .durable
                                .get(key)
                                .map(|wm| wm.seq())
                                .unwrap_or(Seq0::ZERO);
                            let snapshot = state.gap.model_snapshot();
                            let has_gap = snapshot.origins.iter().any(|origin| {
                                origin.origin == key.origin
                                    && origin.namespace == key.namespace
                                    && !origin.gap.buffered.is_empty()
                            });
                            *from == seen && has_gap
                        }
                    })
            },
        )
        .property(
            Expectation::Always,
            "history applied not ahead of delivered",
            |_, s| {
                s.actor_states.iter().enumerate().all(|(ix, state)| {
                    let id = Id::from(ix);
                    state.applied.iter().all(|(key, wm)| {
                        let delivered = s
                            .history
                            .last_delivered
                            .get(&(id, key.clone()))
                            .copied()
                            .unwrap_or(Seq0::ZERO);
                        wm.seq() <= delivered
                    })
                })
            },
        )
        .property(Expectation::Sometimes, "can fully catch up", |_, s| {
            s.actor_states.iter().all(|state| {
                state.durable.len() == s.actor_states.len().saturating_sub(1)
                    && state.durable.values().all(|wm| wm.seq().get() >= MAX_SEQ)
            })
        })
}

fn add_orl_properties(
    model: ActorModel<ActorWrapper<ReplActor>, (), History>,
) -> ActorModel<ActorWrapper<ReplActor>, (), History> {
    add_history_properties(model)
}

fn build_model(
    network: Network<ReplMsg>,
    lossy: LossyNetwork,
    replica_count: usize,
    equivocate: bool,
) -> ActorModel<ReplActor, (), History> {
    let store = StoreIdentity::new(
        StoreId::new(Uuid::from_bytes([9u8; 16])),
        StoreEpoch::new(1),
    );
    let namespaces = vec![NamespaceId::core()];
    let actors = build_actors(store, namespaces, replica_count, equivocate);

    let model = ActorModel::new((), History::default())
        .actors(actors)
        .init_network(network)
        .lossy_network(lossy)
        .record_msg_in(record_msg_in)
        .record_msg_out(record_msg_out);

    add_base_properties(model)
}

fn build_model_wrapped(
    network: Network<MsgWrapper<ReplMsg>>,
    lossy: LossyNetwork,
    replica_count: usize,
    equivocate: bool,
) -> ActorModel<ActorWrapper<ReplActor>, (), History> {
    let store = StoreIdentity::new(
        StoreId::new(Uuid::from_bytes([9u8; 16])),
        StoreEpoch::new(1),
    );
    let namespaces = vec![NamespaceId::core()];
    let actors = build_actors(store, namespaces, replica_count, equivocate)
        .into_iter()
        .map(ActorWrapper::with_default_timeout)
        .collect::<Vec<_>>();

    let model = ActorModel::new((), History::default())
        .actors(actors)
        .init_network(network)
        .lossy_network(lossy)
        .record_msg_in(record_msg_in_orl)
        .record_msg_out(record_msg_out_orl);

    add_orl_properties(model)
}

fn rewrite_id_streamkey_map<S>(
    map: &BTreeMap<(Id, StreamKey), Seq0>,
    plan: &RewritePlan<Id, S>,
) -> BTreeMap<(Id, StreamKey), Seq0> {
    map.iter()
        .map(|((id, key), seq)| ((id.rewrite(plan), key.rewrite(plan)), *seq))
        .collect()
}

fn rewrite_frame_set<S>(
    set: &BTreeSet<FrameDigest>,
    plan: &RewritePlan<Id, S>,
) -> BTreeSet<FrameDigest> {
    set.iter().map(|digest| digest.rewrite(plan)).collect()
}

fn update_last_delivered(history: &mut History, dst: Id, key: StreamKey, seq: Seq0) {
    let entry = history
        .last_delivered
        .entry((dst, key))
        .or_insert(Seq0::ZERO);
    if seq > *entry {
        *entry = seq;
    }
}

fn update_monotonic(
    map: &mut BTreeMap<(Id, StreamKey), Seq0>,
    key: (Id, StreamKey),
    seq: Seq0,
    ok: &mut bool,
) {
    if let Some(prev) = map.get(&key) {
        if seq < *prev {
            *ok = false;
        }
    }
    map.insert(key, seq);
}

fn apply_msg_out(history: &History, env: Envelope<&ReplMsg>) -> History {
    let mut next = history.clone();
    match env.msg {
        ReplMsg::Event(frame) => {
            next.sent.insert(frame.digest.clone());
        }
        ReplMsg::Want { key, from } => {
            update_monotonic(
                &mut next.last_want_out,
                (env.src, key.clone()),
                *from,
                &mut next.want_not_ahead,
            );
            let delivered = next
                .last_delivered
                .get(&(env.src, key.clone()))
                .copied()
                .unwrap_or(Seq0::ZERO);
            if *from > delivered {
                next.want_not_ahead = false;
            }
        }
        ReplMsg::Ack { key, durable } => {
            update_monotonic(
                &mut next.last_ack_out,
                (env.src, key.clone()),
                *durable,
                &mut next.ack_monotonic,
            );
        }
    }
    next
}

fn apply_msg_in(history: &History, env: Envelope<&ReplMsg>) -> History {
    let mut next = history.clone();
    match env.msg {
        ReplMsg::Event(frame) => {
            next.delivered.insert(frame.digest.clone());
            let key = StreamKey::new(
                frame.digest.eid.namespace.clone(),
                frame.digest.eid.origin_replica_id,
            );
            let seq0 = Seq0::new(frame.digest.eid.origin_seq.get());
            update_last_delivered(&mut next, env.dst, key, seq0);
        }
        ReplMsg::Want { .. } | ReplMsg::Ack { .. } => {}
    }
    next
}

fn record_msg_out(_cfg: &(), history: &History, env: Envelope<&ReplMsg>) -> Option<History> {
    Some(apply_msg_out(history, env))
}

fn record_msg_in(_cfg: &(), history: &History, env: Envelope<&ReplMsg>) -> Option<History> {
    Some(apply_msg_in(history, env))
}

fn record_msg_out_orl(
    _cfg: &(),
    history: &History,
    env: Envelope<&MsgWrapper<ReplMsg>>,
) -> Option<History> {
    match env.msg {
        MsgWrapper::Deliver(_, msg) => Some(apply_msg_out(
            history,
            Envelope {
                src: env.src,
                dst: env.dst,
                msg,
            },
        )),
        MsgWrapper::Ack(_) => None,
    }
}

fn record_msg_in_orl(
    _cfg: &(),
    history: &History,
    env: Envelope<&MsgWrapper<ReplMsg>>,
) -> Option<History> {
    match env.msg {
        MsgWrapper::Deliver(_, msg) => Some(apply_msg_in(
            history,
            Envelope {
                src: env.src,
                dst: env.dst,
                msg,
            },
        )),
        MsgWrapper::Ack(_) => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NetworkKind {
    Ordered,
    UnorderedDuplicating,
    UnorderedNonDuplicating,
}

struct NetConfig {
    kind: NetworkKind,
    lossy: LossyNetwork,
    ordered_link: bool,
}

struct ModelConfig {
    kind: NetworkKind,
    lossy: LossyNetwork,
    ordered_link: bool,
    symmetry: bool,
    replica_count: usize,
    equivocate: bool,
    timeout_secs: u64,
}

fn parse_network(args: &mut pico_args::Arguments) -> Result<NetConfig, pico_args::Error> {
    let network = args
        .opt_value_from_str::<_, String>("--network")?
        .unwrap_or_else(|| "unordered_duplicating".to_string());
    let ordered_link = args.contains("--ordered-link");
    let kind = match network.as_str() {
        "ordered" => NetworkKind::Ordered,
        "unordered_duplicating" => NetworkKind::UnorderedDuplicating,
        "unordered_nonduplicating" => NetworkKind::UnorderedNonDuplicating,
        other => {
            return Err(pico_args::Error::ArgumentParsingFailed {
                cause: format!("unsupported --network {other}"),
            });
        }
    };

    let lossy_default = match kind {
        NetworkKind::UnorderedDuplicating => LossyNetwork::Yes,
        NetworkKind::Ordered | NetworkKind::UnorderedNonDuplicating => LossyNetwork::No,
    };
    let lossy = match args.opt_value_from_str::<_, String>("--lossy")? {
        None => lossy_default,
        Some(value) => match value.as_str() {
            "yes" => LossyNetwork::Yes,
            "no" => LossyNetwork::No,
            other => {
                return Err(pico_args::Error::ArgumentParsingFailed {
                    cause: format!("unsupported --lossy {other}"),
                });
            }
        },
    };

    if ordered_link && kind != NetworkKind::Ordered {
        return Err(pico_args::Error::ArgumentParsingFailed {
            cause: "--ordered-link requires --network ordered".into(),
        });
    }

    Ok(NetConfig {
        kind,
        lossy,
        ordered_link,
    })
}

fn parse_model_config(args: &mut pico_args::Arguments) -> Result<ModelConfig, pico_args::Error> {
    let net = parse_network(args)?;
    let replica_count = args
        .opt_value_from_str("--replicas")?
        .unwrap_or(DEFAULT_REPLICAS);
    if replica_count < 2 {
        return Err(pico_args::Error::ArgumentParsingFailed {
            cause: "--replicas must be >= 2".into(),
        });
    }
    if replica_count > u8::MAX as usize {
        return Err(pico_args::Error::ArgumentParsingFailed {
            cause: "--replicas too large for replica id mapping".into(),
        });
    }

    let symmetry_flag = args.contains("--symmetry");
    let no_symmetry_flag = args.contains("--no-symmetry");
    if symmetry_flag && no_symmetry_flag {
        return Err(pico_args::Error::ArgumentParsingFailed {
            cause: "cannot combine --symmetry and --no-symmetry".into(),
        });
    }
    let symmetry = if symmetry_flag {
        true
    } else if no_symmetry_flag {
        false
    } else {
        !net.ordered_link
    };
    if net.ordered_link && symmetry {
        return Err(pico_args::Error::ArgumentParsingFailed {
            cause: "--symmetry is not supported with --ordered-link".into(),
        });
    }

    let equivocate = args.contains("--equivocate");
    let timeout_secs = args.opt_value_from_str("--timeout-secs")?.unwrap_or(300);

    Ok(ModelConfig {
        kind: net.kind,
        lossy: net.lossy,
        ordered_link: net.ordered_link,
        symmetry,
        replica_count,
        equivocate,
        timeout_secs,
    })
}

fn network_for_base(kind: NetworkKind) -> Network<ReplMsg> {
    match kind {
        NetworkKind::Ordered => Network::new_ordered([]),
        NetworkKind::UnorderedDuplicating => Network::new_unordered_duplicating([]),
        NetworkKind::UnorderedNonDuplicating => Network::new_unordered_nonduplicating([]),
    }
}

fn network_for_wrapped(kind: NetworkKind) -> Network<MsgWrapper<ReplMsg>> {
    match kind {
        NetworkKind::Ordered => Network::new_ordered([]),
        NetworkKind::UnorderedDuplicating => Network::new_unordered_duplicating([]),
        NetworkKind::UnorderedNonDuplicating => Network::new_unordered_nonduplicating([]),
    }
}

fn network_label(kind: NetworkKind) -> &'static str {
    match kind {
        NetworkKind::Ordered => "ordered",
        NetworkKind::UnorderedDuplicating => "unordered_duplicating",
        NetworkKind::UnorderedNonDuplicating => "unordered_nonduplicating",
    }
}

fn lossy_label(lossy: LossyNetwork) -> &'static str {
    match lossy {
        LossyNetwork::Yes => "yes",
        LossyNetwork::No => "no",
    }
}

enum RunMode {
    Check,
    Explore(String),
}

fn run_model_plain<M>(model: M, mode: RunMode, timeout_secs: u64)
where
    M: Model + Send + Sync + 'static,
    M::State: std::fmt::Debug + Hash + Send + Sync + Clone + PartialEq,
    M::Action: std::fmt::Debug + Send + Sync + Clone + PartialEq,
{
    let timeout = Duration::from_secs(timeout_secs);
    match mode {
        RunMode::Explore(address) => {
            println!("Exploring replication core state space on {address}.");
            model
                .checker()
                .threads(num_cpus::get())
                .timeout(timeout)
                .serve(address);
        }
        RunMode::Check => {
            println!("Model checking replication core.");
            model
                .checker()
                .threads(num_cpus::get())
                .timeout(timeout)
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
    }
}

fn run_model_symmetric<M>(model: M, mode: RunMode, timeout_secs: u64)
where
    M: Model + Send + Sync + 'static,
    M::State: Representative + std::fmt::Debug + Hash + Send + Sync + Clone + PartialEq,
    M::Action: std::fmt::Debug + Send + Sync + Clone + PartialEq,
{
    let timeout = Duration::from_secs(timeout_secs);
    match mode {
        RunMode::Explore(address) => {
            println!("Exploring replication core state space on {address} (symmetry on).");
            model
                .checker()
                .symmetry()
                .threads(num_cpus::get())
                .timeout(timeout)
                .serve(address);
        }
        RunMode::Check => {
            println!("Model checking replication core (symmetry on).");
            model
                .checker()
                .symmetry()
                .threads(num_cpus::get())
                .timeout(timeout)
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();
    let mut args = pico_args::Arguments::from_env();
    let config = parse_model_config(&mut args)?;
    let mode = match args.subcommand()?.as_deref() {
        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            RunMode::Explore(address)
        }
        Some("check") | None => RunMode::Check,
        _ => {
            println!("USAGE:");
            println!(
                "  repl_core_machine check [--replicas N] [--network ordered|unordered_duplicating|unordered_nonduplicating] [--lossy yes|no] [--ordered-link] [--symmetry|--no-symmetry] [--equivocate] [--timeout-secs N]"
            );
            println!(
                "  repl_core_machine explore [ADDRESS] [--replicas N] [--network ordered|unordered_duplicating|unordered_nonduplicating] [--lossy yes|no] [--ordered-link] [--symmetry|--no-symmetry] [--equivocate] [--timeout-secs N]"
            );
            println!("  repl_core_machine check --timeout-secs 300");
            return Ok(());
        }
    };

    println!(
        "Config: replicas={}, network={}, lossy={}, ordered_link={}, symmetry={}, equivocate={}, timeout_secs={}",
        config.replica_count,
        network_label(config.kind),
        lossy_label(config.lossy),
        config.ordered_link,
        config.symmetry,
        config.equivocate,
        config.timeout_secs
    );

    if config.ordered_link {
        let network = network_for_wrapped(config.kind);
        let model = build_model_wrapped(
            network,
            config.lossy,
            config.replica_count,
            config.equivocate,
        );
        run_model_plain(model, mode, config.timeout_secs);
    } else {
        let network = network_for_base(config.kind);
        let model = build_model(
            network,
            config.lossy,
            config.replica_count,
            config.equivocate,
        );
        if config.symmetry {
            run_model_symmetric(model, mode, config.timeout_secs);
        } else {
            run_model_plain(model, mode, config.timeout_secs);
        }
    }

    Ok(())
}
