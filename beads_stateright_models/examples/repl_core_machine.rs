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

use beads_rs::model::{event_factory, repl_ingest, GapBufferByNsOrigin, GapBufferByNsOriginSnapshot};
use beads_rs::{
    ActorId, Applied, Durable, EventFrameError, EventFrameV1, EventId, EventShaLookup, HeadStatus,
    Limits, NamespaceId, ReplicaId, Seq0, Seq1, Sha256, StoreEpoch, StoreId, StoreIdentity,
    TxnDeltaV1, TxnId, Watermark,
};
use stateright::actor::{
    model_peers, model_timeout, Actor, ActorModel, Envelope, Id, LossyNetwork, Network, Out,
};
use stateright::{report::WriteReporter, Expectation, Property};
use uuid::Uuid;

const ACTOR_COUNT: usize = 2;
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct FrameDigest {
    eid: EventId,
    sha256: Sha256,
    prev_sha256: Option<Sha256>,
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
        Some(self.digest.cmp(&other.digest))
    }
}

impl Ord for FrameMsg {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.digest.cmp(&other.digest)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ReplMsg {
    Event(FrameMsg),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum TimerTag {
    Tick,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
struct History {
    sent: BTreeSet<FrameDigest>,
    delivered: BTreeSet<FrameDigest>,
}

#[derive(Clone, Debug)]
struct ReplActor {
    replica_id: ReplicaId,
    actor_id: ActorId,
    store: StoreIdentity,
    namespaces: Vec<NamespaceId>,
    limits: Limits,
    max_seq: u64,
    equivocate: bool,
    peer_count: usize,
}

impl ReplActor {
    fn build_frames(&self) -> Vec<FrameMsg> {
        let mut frames = Vec::new();
        for namespace in &self.namespaces {
            let factory = event_factory::EventFactory::new(
                self.store,
                namespace.clone(),
                self.replica_id,
                self.actor_id.clone(),
            );
            let mut prev_sha = None;
            for seq in 1..=self.max_seq {
                let seq1 = Seq1::from_u64(seq).expect("seq1");
                let prev_for_seq = prev_sha;
                let txn_id = TxnId::new(make_uuid(self.replica_id, seq, 0));
                let body = factory.txn_body(seq1, txn_id, seq, 0, TxnDeltaV1::new(), None);
                let frame = event_factory::encode_frame(&body, prev_for_seq)
                    .expect("encode frame");
                prev_sha = Some(frame.sha256);
                frames.push(FrameMsg::new(frame));

                if self.equivocate && seq == EQUIVOCATE_SEQ {
                    let txn_id = TxnId::new(make_uuid(self.replica_id, seq, 1));
                    let body = factory.txn_body(seq1, txn_id, seq + 100, 0, TxnDeltaV1::new(), None);
                    let frame = event_factory::encode_frame(&body, prev_for_seq)
                        .expect("encode frame");
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

        let frames = self.build_frames();
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
        _src: Id,
        msg: Self::Msg,
        _o: &mut Out<Self>,
    ) {
        let ReplMsg::Event(frame_msg) = msg;
        let state = state.to_mut();
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
        state.last_effect = Some(LastEffect { kind: effect, changed });
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WantInfo {
    key: StreamKey,
    from: Seq0,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum LastEffectKind {
    Applied { key: StreamKey, seq: Seq1, prev_seen: Seq0 },
    Buffered { key: StreamKey, seq: Seq1 },
    Duplicate { key: StreamKey, seq: Seq1 },
    Rejected { code: &'static str },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
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
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum HeadDigest {
    Genesis,
    Known(Sha256),
    Unknown,
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
                self.last_want = Some(WantInfo { key: key.clone(), from: want_from });
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
            let eid = EventId::new(ev.body.origin_replica_id, namespace.clone(), ev.body.origin_seq);
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
            HeadStatus::Unknown => HeadDigest::Unknown,
        };
        Self { seq: wm.seq(), head }
    }
}

struct EventLookup<'a> {
    events: &'a BTreeMap<EventId, Sha256>,
}

impl EventShaLookup for EventLookup<'_> {
    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, beads_rs::EventShaLookupError> {
        Ok(self.events.get(eid).copied())
    }
}

fn expected_prev_head(durable: Watermark<Durable>, seq: Seq1) -> Option<Sha256> {
    if seq.get() != durable.seq().get().saturating_add(1) {
        return None;
    }
    match durable.head() {
        HeadStatus::Genesis => None,
        HeadStatus::Known(bytes) => Some(Sha256(bytes)),
        HeadStatus::Unknown => None,
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

fn make_uuid(replica: ReplicaId, seq: u64, variant: u8) -> Uuid {
    let mut bytes = [0u8; 16];
    let seed = replica.as_uuid().as_bytes();
    bytes[0] = seed[0];
    bytes[1] = seed[1];
    bytes[2] = seq as u8;
    bytes[3] = variant;
    Uuid::from_bytes(bytes)
}

fn build_limits() -> Limits {
    let mut limits = Limits::default();
    limits.repl_gap_timeout_ms = GAP_TIMEOUT_TICKS;
    limits.max_repl_gap_events = MAX_GAP_EVENTS;
    limits.max_repl_gap_bytes = MAX_GAP_BYTES;
    limits
}

fn build_actors(store: StoreIdentity, namespaces: Vec<NamespaceId>) -> Vec<ReplActor> {
    let limits = build_limits();
    let replica_ids = (0..ACTOR_COUNT)
        .map(|ix| ReplicaId::new(Uuid::from_bytes([ix as u8 + 1; 16])))
        .collect::<Vec<_>>();
    replica_ids
        .iter()
        .enumerate()
        .map(|(ix, replica_id)| ReplActor {
            replica_id: *replica_id,
            actor_id: ActorId::new(format!("replica-{ix}")).expect("actor id"),
            store,
            namespaces: namespaces.clone(),
            limits: limits.clone(),
            max_seq: MAX_SEQ,
            equivocate: ix == 0,
            peer_count: ACTOR_COUNT,
        })
        .collect()
}

fn build_model(network: Network<ReplMsg>, lossy: LossyNetwork) -> ActorModel<ReplActor, (), History> {
    let store = StoreIdentity::new(
        StoreId::new(Uuid::from_bytes([9u8; 16])),
        StoreEpoch::new(1),
    );
    let namespaces = vec![NamespaceId::core()];
    let actors = build_actors(store, namespaces);

    ActorModel::new((), History::default())
        .actors(actors)
        .init_network(network)
        .lossy_network(lossy)
        .record_msg_in(record_msg_in)
        .record_msg_out(record_msg_out)
        .property(Expectation::Always, "ack never exceeds contiguous seen", |_, s| {
            s.actor_states.iter().all(|node| {
                node.ack_durable.iter().all(|(key, ack)| {
                    let seen = node
                        .durable
                        .get(key)
                        .map(|wm| wm.seq())
                        .unwrap_or(Seq0::ZERO);
                    *ack <= seen
                })
            })
        })
        .property(Expectation::Always, "seen map is monotonic", |_, s| {
            s.actor_states.iter().all(|node| {
                node.durable.iter().all(|(key, wm)| {
                    node.durable_max
                        .get(key)
                        .copied()
                        .unwrap_or(Seq0::ZERO)
                        == wm.seq()
                })
            })
        })
        .property(Expectation::Always, "ack map is monotonic", |_, s| {
            s.actor_states.iter().all(|node| {
                node.ack_durable.iter().all(|(key, ack)| {
                    node.ack_max
                        .get(key)
                        .copied()
                        .unwrap_or(Seq0::ZERO)
                        == *ack
                })
            })
        })
        .property(Expectation::Always, "contiguous seen implies applied prefix", |_, s| {
            s.actor_states.iter().all(|node| {
                node.durable.iter().all(|(key, wm)| {
                    let seen = wm.seq().get();
                    (1..=seen).all(|seq| {
                        let seq1 = Seq1::from_u64(seq).expect("seq1");
                        let eid = EventId::new(key.origin, key.namespace.clone(), seq1);
                        node.event_store.contains_key(&eid)
                    })
                })
            })
        })
        .property(Expectation::Always, "applies only the next contiguous seq", |_, s| {
            s.actor_states.iter().all(|node| match node.last_effect.as_ref() {
                Some(LastEffect {
                    kind: LastEffectKind::Applied { seq, prev_seen, .. },
                    ..
                }) => seq.get() == prev_seen.get().saturating_add(1),
                _ => true,
            })
        })
        .property(Expectation::Always, "buffered items are beyond next expected", |_, s| {
            s.actor_states.iter().all(|node| {
                let snapshot = node.gap.model_snapshot();
                snapshot.origins.iter().all(|origin| {
                    let durable_seq = origin.durable.seq.get();
                    origin
                        .gap
                        .buffered
                        .iter()
                        .all(|ev| ev.seq.get() > durable_seq.saturating_add(1))
                })
            })
        })
        .property(Expectation::Always, "buffer stays within bounds unless closed", |_, s| {
            s.actor_states.iter().all(|node| {
                if node.errored {
                    return true;
                }
                let snapshot = node.gap.model_snapshot();
                snapshot.origins.iter().all(|origin| {
                    origin.gap.buffered.len() <= origin.gap.max_events
                        && origin.gap.buffered_bytes <= origin.gap.max_bytes
                })
            })
        })
        .property(Expectation::Always, "equivocation implies hard close", |_, s| {
            s.actor_states
                .iter()
                .all(|node| !node.equivocation_seen || node.errored)
        })
        .property(Expectation::Always, "duplicate deliveries are idempotent", |_, s| {
            s.actor_states.iter().all(|node| match node.last_effect.as_ref() {
                Some(LastEffect {
                    kind: LastEffectKind::Duplicate { .. },
                    changed,
                }) => !*changed,
                _ => true,
            })
        })
        .property(Expectation::Always, "want from equals seen when emitted", |_, s| {
            s.actor_states.iter().all(|node| match node.last_want.as_ref() {
                None => true,
                Some(WantInfo { key, from }) => {
                    let seen = node
                        .durable
                        .get(key)
                        .map(|wm| wm.seq())
                        .unwrap_or(Seq0::ZERO);
                    let snapshot = node.gap.model_snapshot();
                    let has_gap = snapshot.origins.iter().any(|origin| {
                        origin.origin == key.origin
                            && origin.namespace == key.namespace
                            && !origin.gap.buffered.is_empty()
                    });
                    *from == seen && has_gap
                }
            })
        })
        .property(Expectation::Sometimes, "can fully catch up", |_, s| {
            s.actor_states.iter().all(|node| {
                node.durable.len() == ACTOR_COUNT.saturating_sub(1)
                    && node.durable.values().all(|wm| wm.seq().get() >= MAX_SEQ)
            })
        })
}

fn record_msg_out(_cfg: &(), history: &History, env: Envelope<&ReplMsg>) -> Option<History> {
    let ReplMsg::Event(frame) = env.msg;
    let mut next = history.clone();
    next.sent.insert(frame.digest.clone());
    Some(next)
}

fn record_msg_in(_cfg: &(), history: &History, env: Envelope<&ReplMsg>) -> Option<History> {
    let ReplMsg::Event(frame) = env.msg;
    let mut next = history.clone();
    next.delivered.insert(frame.digest.clone());
    Some(next)
}

fn parse_network(args: &mut pico_args::Arguments) -> Result<(Network<ReplMsg>, LossyNetwork), pico_args::Error> {
    let network = args
        .opt_value_from_str::<_, String>("--network")?
        .unwrap_or_else(|| "unordered_duplicating".to_string());
    match network.as_str() {
        "ordered" => Ok((Network::new_ordered([]), LossyNetwork::No)),
        "unordered_duplicating" => Ok((Network::new_unordered_duplicating([]), LossyNetwork::Yes)),
        other => Err(pico_args::Error::ArgumentParsingFailed {
            cause: format!("unsupported --network {other}").into(),
        }),
    }
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();
    let mut args = pico_args::Arguments::from_env();
    let (network, lossy) = parse_network(&mut args)?;
    match args.subcommand()?.as_deref() {
        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!("Exploring replication core state space on {address}.");
            build_model(network, lossy)
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking replication core.");
            build_model(network, lossy)
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  repl_core_machine check [--network ordered|unordered_duplicating]");
            println!("  repl_core_machine explore [ADDRESS] [--network ordered|unordered_duplicating]");
        }
    }

    Ok(())
}
