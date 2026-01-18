//! Model: Idempotent mutation receipts with durability waiting, crash/recover, and storage.
//!
//! Uses production-backed WAL index + durability coordinator logic with Stateright timers
//! and crash/recover to exercise retry stability.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beads_rs::daemon::repl::proto::{WatermarkHeads, WatermarkMap};
use beads_rs::daemon::wal::WalIndex;
use beads_rs::model::{durability, MemoryWalIndex, MemoryWalIndexSnapshot, PeerAckTable};
use beads_rs::{
    Applied, ClientRequestId, DurabilityClass, DurabilityOutcome, DurabilityReceipt, EventId,
    NamespaceId, ReplicaId, Seq0, Seq1, Sha256, StoreEpoch, StoreId, StoreIdentity, TxnId,
    Watermarks,
};
use stateright::actor::{
    model_timeout, Actor, ActorModel, Envelope, Id, LossyNetwork, Network, Out,
};
use stateright::{report::WriteReporter, Checker, Expectation, Model};
use uuid::Uuid;

const SERVER_ID: usize = 0;
const PEER_ID: usize = 1;
const CLIENT_ID: usize = 2;
const MAX_RETRIES: u8 = 2;
const MAX_ACK_SEQ: u64 = 2;
const CHECK_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ModelDurability {
    ReplicatedK1,
}

impl ModelDurability {
    fn to_class(self) -> DurabilityClass {
        match self {
            ModelDurability::ReplicatedK1 => DurabilityClass::ReplicatedFsync {
                k: NonZeroU32::new(1).expect("k"),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Msg {
    Submit {
        request_id: ClientRequestId,
        request_sha: [u8; 32],
        durability: ModelDurability,
    },
    Response {
        request_id: ClientRequestId,
        digest: ReceiptDigest,
        kind: ResponseKind,
    },
    PeerAck {
        peer: ReplicaId,
        seq: Seq0,
        head: Sha256,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ResponseKind {
    Ok,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum OutcomeKind {
    Achieved,
    Pending,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ReceiptDigest {
    txn_id: TxnId,
    event_ids: Vec<EventId>,
    outcome: OutcomeKind,
}

impl ReceiptDigest {
    fn from_receipt(receipt: &DurabilityReceipt) -> Self {
        let outcome = match receipt.outcome {
            DurabilityOutcome::Achieved { .. } => OutcomeKind::Achieved,
            DurabilityOutcome::Pending { .. } => OutcomeKind::Pending,
        };
        Self {
            txn_id: receipt.txn_id,
            event_ids: receipt.event_ids.clone(),
            outcome,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Timer {
    ClientRetry,
    PeerAckTick,
    DurabilityWait,
}

#[derive(Clone)]
struct ModelActor {
    role: Role,
    server_cfg: ServerCfg,
}

#[derive(Clone)]
struct ServerCfg {
    store: StoreIdentity,
    namespace: NamespaceId,
    local_replica: ReplicaId,
    roster: beads_rs::ReplicaRoster,
    policies: BTreeMap<NamespaceId, beads_rs::NamespacePolicy>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Server,
    Peer,
    Client,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Storage {
    wal: MemoryWalIndexSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ActorState {
    Server(ServerState),
    Peer(PeerState),
    Client(ClientState),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ServerState {
    now_ms: u64,
    wal: MemoryWalIndexSnapshot,
    peer_acks: PeerAckTable,
    pending: Option<PendingRequest>,
    errored: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PendingRequest {
    request_id: ClientRequestId,
    durability: ModelDurability,
    max_seq: Seq1,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PeerState {
    ack_seq: Seq0,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClientState {
    retries_left: u8,
    last_response: Option<ResponseKind>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct History {
    ids_stable: bool,
    outcome_monotonic: bool,
    receipts: BTreeMap<ClientRequestId, ReceiptDigest>,
    completed: BTreeSet<ClientRequestId>,
}

impl Default for History {
    fn default() -> Self {
        Self {
            ids_stable: true,
            outcome_monotonic: true,
            receipts: BTreeMap::new(),
            completed: BTreeSet::new(),
        }
    }
}

impl ModelActor {
    fn server() -> Self {
        let local_replica = replica_for(SERVER_ID);
        let roster = beads_rs::ReplicaRoster {
            replicas: vec![
                beads_rs::ReplicaEntry {
                    replica_id: local_replica,
                    name: "local".to_string(),
                    role: beads_rs::ReplicaRole::Anchor,
                    durability_eligible: true,
                    allowed_namespaces: None,
                    expire_after_ms: None,
                },
                beads_rs::ReplicaEntry {
                    replica_id: replica_for(PEER_ID),
                    name: "peer".to_string(),
                    role: beads_rs::ReplicaRole::Peer,
                    durability_eligible: true,
                    allowed_namespaces: None,
                    expire_after_ms: None,
                },
            ],
        };
        let namespace = NamespaceId::core();
        let mut policies = BTreeMap::new();
        policies.insert(namespace.clone(), beads_rs::NamespacePolicy::core_default());
        let store = StoreIdentity::new(
            StoreId::new(Uuid::from_bytes([7u8; 16])),
            StoreEpoch::new(1),
        );
        Self {
            role: Role::Server,
            server_cfg: ServerCfg {
                store,
                namespace,
                local_replica,
                roster,
                policies,
            },
        }
    }

    fn peer() -> Self {
        Self {
            role: Role::Peer,
            server_cfg: Self::server().server_cfg,
        }
    }

    fn client() -> Self {
        Self {
            role: Role::Client,
            server_cfg: Self::server().server_cfg,
        }
    }
}

impl Actor for ModelActor {
    type Msg = Msg;
    type State = ActorState;
    type Timer = Timer;
    type Random = ();
    type Storage = Storage;

    fn on_start(&self, _id: Id, storage: &Option<Self::Storage>, o: &mut Out<Self>) -> Self::State {
        match self.role {
            Role::Server => {
                let wal = storage
                    .as_ref()
                    .map(|s| s.wal.clone())
                    .unwrap_or_else(|| MemoryWalIndex::new().model_snapshot());
                o.save(Storage { wal: wal.clone() });
                ActorState::Server(ServerState {
                    now_ms: 0,
                    wal,
                    peer_acks: PeerAckTable::new(),
                    pending: None,
                    errored: false,
                })
            }
            Role::Peer => {
                o.set_timer(Timer::PeerAckTick, model_timeout());
                ActorState::Peer(PeerState {
                    ack_seq: Seq0::ZERO,
                })
            }
            Role::Client => {
                o.set_timer(Timer::ClientRetry, model_timeout());
                let (request_id, request_sha) = client_request();
                o.send(
                    server_id(),
                    Msg::Submit {
                        request_id,
                        request_sha,
                        durability: ModelDurability::ReplicatedK1,
                    },
                );
                ActorState::Client(ClientState {
                    retries_left: MAX_RETRIES,
                    last_response: None,
                })
            }
        }
    }

    fn on_msg(
        &self,
        id: Id,
        state: &mut Cow<Self::State>,
        _src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        match (self.role, state.to_mut()) {
            (Role::Server, ActorState::Server(state)) => match msg {
                Msg::Submit {
                    request_id,
                    request_sha,
                    durability,
                } => {
                    state.now_ms = state.now_ms.saturating_add(1);
                    let result = handle_submit(
                        &self.server_cfg,
                        state,
                        request_id,
                        request_sha,
                        durability,
                        o,
                    );
                    if result.is_err() {
                        state.errored = true;
                    }
                }
                Msg::PeerAck { peer, seq, head } => {
                    state.now_ms = state.now_ms.saturating_add(1);
                    if let Err(_) = handle_peer_ack(&self.server_cfg, state, peer, seq, head, o) {
                        state.errored = true;
                    }
                }
                Msg::Response { .. } => {}
            },
            (Role::Peer, ActorState::Peer(state)) => {
                if let Msg::Response { .. } | Msg::Submit { .. } = msg {
                    return;
                }
                if let Msg::PeerAck { .. } = msg {
                    return;
                }
                drop((id, state));
            }
            (Role::Client, ActorState::Client(state)) => match msg {
                Msg::Response { kind, .. } => {
                    state.last_response = Some(kind);
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn on_timeout(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        match (self.role, state.to_mut(), timer) {
            (Role::Peer, ActorState::Peer(state), Timer::PeerAckTick) => {
                if state.ack_seq.get() < MAX_ACK_SEQ {
                    let next = Seq0::new(state.ack_seq.get().saturating_add(1));
                    state.ack_seq = next;
                    o.send(
                        server_id(),
                        Msg::PeerAck {
                            peer: replica_for(PEER_ID),
                            seq: next,
                            head: ack_head(next),
                        },
                    );
                }
                if state.ack_seq.get() < MAX_ACK_SEQ {
                    o.set_timer(Timer::PeerAckTick, model_timeout());
                }
            }
            (Role::Server, ActorState::Server(state), Timer::DurabilityWait) => {
                if let Some(pending) = state.pending.take() {
                    let _ =
                        resolve_pending(&self.server_cfg, state, pending, o, Resolution::Timeout);
                }
            }
            (Role::Client, ActorState::Client(state), Timer::ClientRetry) => {
                if state.retries_left == 0 {
                    return;
                }
                if matches!(state.last_response, Some(ResponseKind::Ok)) {
                    return;
                }
                state.retries_left = state.retries_left.saturating_sub(1);
                let (request_id, request_sha) = client_request();
                o.send(
                    server_id(),
                    Msg::Submit {
                        request_id,
                        request_sha,
                        durability: ModelDurability::ReplicatedK1,
                    },
                );
                o.set_timer(Timer::ClientRetry, model_timeout());
            }
            _ => {}
        }
    }
}

enum Resolution {
    Acked,
    Timeout,
}

fn handle_submit(
    cfg: &ServerCfg,
    state: &mut ServerState,
    request_id: ClientRequestId,
    request_sha: [u8; 32],
    durability: ModelDurability,
    o: &mut Out<ModelActor>,
) -> Result<(), ()> {
    let (receipt, max_seq, updated_snapshot) =
        build_receipt(cfg, &state.wal, request_id, request_sha, state.now_ms)?;
    state.wal = updated_snapshot.clone();
    o.save(Storage {
        wal: updated_snapshot,
    });

    match durability.to_class() {
        DurabilityClass::LocalFsync => {
            send_receipt(o, request_id, receipt, ResponseKind::Ok);
        }
        DurabilityClass::ReplicatedFsync { k } => {
            let poll = poll_replicated(
                cfg,
                &mut state.peer_acks,
                cfg.namespace.clone(),
                cfg.local_replica,
                max_seq,
                k,
            )?;
            match poll {
                beads_rs::daemon::durability_coordinator::ReplicatedPoll::Satisfied {
                    acked_by,
                } => {
                    let achieved =
                        durability::achieved_receipt(receipt, durability.to_class(), k, acked_by);
                    send_receipt(o, request_id, achieved, ResponseKind::Ok);
                }
                beads_rs::daemon::durability_coordinator::ReplicatedPoll::Pending { .. } => {
                    state.pending = Some(PendingRequest {
                        request_id,
                        durability,
                        max_seq,
                    });
                    o.set_timer(Timer::DurabilityWait, model_timeout());
                }
            }
        }
    }

    Ok(())
}

fn handle_peer_ack(
    cfg: &ServerCfg,
    state: &mut ServerState,
    peer: ReplicaId,
    seq: Seq0,
    head: Sha256,
    o: &mut Out<ModelActor>,
) -> Result<(), ()> {
    let mut map: WatermarkMap = BTreeMap::new();
    map.entry(cfg.namespace.clone())
        .or_default()
        .insert(cfg.local_replica, seq);
    let mut heads: WatermarkHeads = BTreeMap::new();
    heads
        .entry(cfg.namespace.clone())
        .or_default()
        .insert(cfg.local_replica, head);
    state
        .peer_acks
        .update_peer(peer, &map, Some(&heads), None, None, state.now_ms)
        .map_err(|_| ())?;

    if let Some(pending) = state.pending.take() {
        let _ = resolve_pending(cfg, state, pending, o, Resolution::Acked);
    }

    Ok(())
}

fn resolve_pending(
    cfg: &ServerCfg,
    state: &mut ServerState,
    pending: PendingRequest,
    o: &mut Out<ModelActor>,
    resolution: Resolution,
) -> Result<(), ()> {
    let receipt = lookup_receipt(cfg, &state.wal, pending.request_id)?;
    match pending.durability.to_class() {
        DurabilityClass::LocalFsync => {
            send_receipt(o, pending.request_id, receipt, ResponseKind::Ok);
        }
        DurabilityClass::ReplicatedFsync { k } => match resolution {
            Resolution::Acked => {
                let poll = poll_replicated(
                    cfg,
                    &mut state.peer_acks,
                    cfg.namespace.clone(),
                    cfg.local_replica,
                    pending.max_seq,
                    k,
                )?;
                match poll {
                    beads_rs::daemon::durability_coordinator::ReplicatedPoll::Satisfied {
                        acked_by,
                    } => {
                        let achieved = durability::achieved_receipt(
                            receipt,
                            pending.durability.to_class(),
                            k,
                            acked_by,
                        );
                        send_receipt(o, pending.request_id, achieved, ResponseKind::Ok);
                    }
                    beads_rs::daemon::durability_coordinator::ReplicatedPoll::Pending {
                        ..
                    } => {
                        state.pending = Some(pending);
                        o.set_timer(Timer::DurabilityWait, model_timeout());
                    }
                }
            }
            Resolution::Timeout => {
                let pending_receipt =
                    durability::pending_receipt(receipt, pending.durability.to_class());
                send_receipt(
                    o,
                    pending.request_id,
                    pending_receipt,
                    ResponseKind::Timeout,
                );
            }
        },
    }
    Ok(())
}

fn send_receipt(
    o: &mut Out<ModelActor>,
    request_id: ClientRequestId,
    receipt: DurabilityReceipt,
    kind: ResponseKind,
) {
    let digest = ReceiptDigest::from_receipt(&receipt);
    o.send(
        client_id(),
        Msg::Response {
            request_id,
            digest,
            kind,
        },
    );
}

fn build_receipt(
    cfg: &ServerCfg,
    snapshot: &MemoryWalIndexSnapshot,
    request_id: ClientRequestId,
    request_sha: [u8; 32],
    now_ms: u64,
) -> Result<(DurabilityReceipt, Seq1, MemoryWalIndexSnapshot), ()> {
    let index = MemoryWalIndex::from_snapshot(snapshot.clone());
    let existing = index
        .reader()
        .lookup_client_request(&cfg.namespace, &cfg.local_replica, request_id)
        .map_err(|_| ())?;
    let (txn_id, event_ids, created_at_ms, max_seq) = if let Some(row) = existing {
        if row.request_sha256 != request_sha {
            return Err(());
        }
        let max_seq = row
            .event_ids
            .iter()
            .map(|eid| eid.origin_seq)
            .max()
            .unwrap_or_else(|| Seq1::from_u64(1).expect("seq1"));
        (row.txn_id, row.event_ids, row.created_at_ms, max_seq)
    } else {
        let mut txn = index.writer().begin_txn().map_err(|_| ())?;
        let seq = txn
            .next_origin_seq(&cfg.namespace, &cfg.local_replica)
            .map_err(|_| ())?;
        let event_id = EventId::new(cfg.local_replica, cfg.namespace.clone(), seq);
        let txn_id = TxnId::new(make_uuid(cfg.local_replica, seq.get(), 0));
        txn.upsert_client_request(
            &cfg.namespace,
            &cfg.local_replica,
            request_id,
            request_sha,
            txn_id,
            &[event_id.clone()],
            now_ms,
        )
        .map_err(|_| ())?;
        txn.commit().map_err(|_| ())?;
        (txn_id, vec![event_id], now_ms, seq)
    };

    let receipt = DurabilityReceipt::local_fsync(
        cfg.store,
        txn_id,
        event_ids.clone(),
        created_at_ms,
        Watermarks::<beads_rs::Durable>::new(),
        Watermarks::<Applied>::new(),
    );
    let snapshot = index.model_snapshot();
    Ok((receipt, max_seq, snapshot))
}

fn lookup_receipt(
    cfg: &ServerCfg,
    snapshot: &MemoryWalIndexSnapshot,
    request_id: ClientRequestId,
) -> Result<DurabilityReceipt, ()> {
    let index = MemoryWalIndex::from_snapshot(snapshot.clone());
    let Some(row) = index
        .reader()
        .lookup_client_request(&cfg.namespace, &cfg.local_replica, request_id)
        .map_err(|_| ())?
    else {
        return Err(());
    };
    Ok(DurabilityReceipt::local_fsync(
        cfg.store,
        row.txn_id,
        row.event_ids,
        row.created_at_ms,
        Watermarks::<beads_rs::Durable>::new(),
        Watermarks::<Applied>::new(),
    ))
}

fn poll_replicated(
    cfg: &ServerCfg,
    peer_acks: &mut PeerAckTable,
    namespace: NamespaceId,
    origin: ReplicaId,
    seq: Seq1,
    k: NonZeroU32,
) -> Result<beads_rs::daemon::durability_coordinator::ReplicatedPoll, ()> {
    let arc = Arc::new(Mutex::new(peer_acks.clone()));
    let coordinator = beads_rs::daemon::durability_coordinator::DurabilityCoordinator::new(
        cfg.local_replica,
        cfg.policies.clone(),
        Some(cfg.roster.clone()),
        Arc::clone(&arc),
    );
    let result =
        durability::poll_replicated(&coordinator, &namespace, origin, seq, k).map_err(|_| ());
    *peer_acks = arc.lock().expect("peer ack lock poisoned").clone();
    result
}

fn client_request() -> (ClientRequestId, [u8; 32]) {
    let request_id = ClientRequestId::new(Uuid::from_bytes([5u8; 16]));
    (request_id, [9u8; 32])
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

fn replica_for(ix: usize) -> ReplicaId {
    ReplicaId::new(Uuid::from_bytes([ix as u8 + 1; 16]))
}

fn server_id() -> Id {
    Id::from(SERVER_ID)
}

fn client_id() -> Id {
    Id::from(CLIENT_ID)
}

fn ack_head(seq: Seq0) -> Sha256 {
    let mut bytes = [0u8; 32];
    bytes[0] = seq.get() as u8;
    Sha256(bytes)
}

fn build_model() -> ActorModel<ModelActor, (), History> {
    let actors = vec![
        ModelActor::server(),
        ModelActor::peer(),
        ModelActor::client(),
    ];
    ActorModel::new((), History::default())
        .actors(actors)
        .init_network(Network::new_ordered([]))
        .lossy_network(LossyNetwork::Yes)
        .max_crashes(1)
        .within_boundary(|_, state| {
            state
                .crashed
                .iter()
                .enumerate()
                .all(|(ix, crashed)| ix == SERVER_ID || !*crashed)
        })
        .record_msg_in(record_msg_in)
        .property(Expectation::Always, "idempotent receipt ids", |_, s| {
            s.history.ids_stable
        })
        .property(
            Expectation::Always,
            "receipt outcome monotonic (client observed)",
            |_, s| s.history.outcome_monotonic,
        )
        .property(Expectation::Always, "server never errors", |_, s| {
            s.actor_states.iter().all(|state| {
                if let ActorState::Server(state) = &**state {
                    !state.errored
                } else {
                    true
                }
            })
        })
}

fn record_msg_in(_cfg: &(), history: &History, env: Envelope<&Msg>) -> Option<History> {
    let Msg::Response {
        request_id,
        digest,
        kind,
    } = env.msg
    else {
        return None;
    };
    let mut next = history.clone();
    if next.completed.contains(request_id) {
        return None;
    }
    if let Some(existing) = next.receipts.get(request_id) {
        if existing.txn_id != digest.txn_id || existing.event_ids != digest.event_ids {
            next.ids_stable = false;
        }
        if matches!(existing.outcome, OutcomeKind::Achieved)
            && digest.outcome != OutcomeKind::Achieved
        {
            next.outcome_monotonic = false;
        }
        if matches!(existing.outcome, OutcomeKind::Pending)
            && digest.outcome == OutcomeKind::Pending
        {
            // still pending is fine
        }
    } else {
        next.receipts.insert(*request_id, digest.clone());
    }
    if let Some(existing) = next.receipts.get_mut(request_id) {
        if digest.outcome == OutcomeKind::Achieved {
            existing.outcome = OutcomeKind::Achieved;
        }
    }
    if *kind == ResponseKind::Ok {
        next.completed.insert(*request_id);
    }
    Some(next)
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();
    let mut args = pico_args::Arguments::from_env();
    match args.subcommand()?.as_deref() {
        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!("Exploring idempotent durability model on {address}.");
            build_model()
                .checker()
                .threads(num_cpus::get())
                .timeout(CHECK_TIMEOUT)
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking idempotent durability.");
            build_model()
                .checker()
                .threads(num_cpus::get())
                .timeout(CHECK_TIMEOUT)
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  durability_idempotency_machine check");
            println!("  durability_idempotency_machine explore [ADDRESS]");
        }
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn run_regression_check() {
    let checker = build_model().checker().threads(1).spawn_dfs().join();
    assert!(checker.is_done(), "model checking did not complete");
    checker.assert_properties();
}
