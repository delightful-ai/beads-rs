//! Model: Read/receipt consistency using Stateright linearizability testing.
//!
//! This model uses `RegisterMsg` helpers with a linearizability tester to validate
//! that reads observe the latest receipt key produced by mutation requests.

use std::borrow::Cow;
use std::time::Duration;

use beads_rs::daemon::wal::WalIndex;
use beads_rs::model::{MemoryWalIndex, MemoryWalIndexSnapshot};
use beads_rs::{
    Applied, ClientRequestId, DurabilityReceipt, EventId, NamespaceId, ReplicaId, StoreEpoch,
    StoreId, StoreIdentity, TxnId, Watermarks,
};
use stateright::actor::register::RegisterMsg;
use stateright::actor::{Actor, ActorModel, Id, LossyNetwork, Network, Out};
use stateright::report::WriteReporter;
use stateright::semantics::ConsistencyTester;
use stateright::semantics::LinearizabilityTester;
use stateright::semantics::register::Register;
use stateright::{Checker, Expectation, Model};
use uuid::Uuid;

const SERVER_ID: usize = 0;
const MAX_REQUESTS: u8 = 3;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ReceiptKey {
    txn_id: TxnId,
    event_ids: Vec<EventId>,
}

impl ReceiptKey {
    fn from_receipt(receipt: &DurabilityReceipt) -> Self {
        Self {
            txn_id: receipt.txn_id,
            event_ids: receipt.event_ids.clone(),
        }
    }
}

impl Default for ReceiptKey {
    fn default() -> Self {
        Self {
            txn_id: TxnId::new(Uuid::from_bytes([0u8; 16])),
            event_ids: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct ServerCfg {
    store: StoreIdentity,
    namespace: NamespaceId,
    local_replica: ReplicaId,
}

impl ServerCfg {
    fn new() -> Self {
        let store = StoreIdentity::new(
            StoreId::new(Uuid::from_bytes([9u8; 16])),
            StoreEpoch::new(1),
        );
        Self {
            store,
            namespace: NamespaceId::core(),
            local_replica: ReplicaId::new(Uuid::from_bytes([7u8; 16])),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Server,
    Client,
}

#[derive(Clone)]
struct ModelActor {
    role: Role,
    cfg: ServerCfg,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ServerState {
    wal: MemoryWalIndexSnapshot,
    mismatch: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ClientPhase {
    Idle,
    WaitingPut {
        request_id: ClientRequestId,
        key: ReceiptKey,
    },
    WaitingGet {
        request_id: ClientRequestId,
        key: ReceiptKey,
    },
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ClientState {
    wal: MemoryWalIndexSnapshot,
    next_seq: u8,
    phase: ClientPhase,
    mismatch: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ActorState {
    Server(ServerState),
    Client(ClientState),
}

impl ModelActor {
    fn server(cfg: ServerCfg) -> Self {
        Self {
            role: Role::Server,
            cfg,
        }
    }

    fn client(cfg: ServerCfg) -> Self {
        Self {
            role: Role::Client,
            cfg,
        }
    }
}

impl Actor for ModelActor {
    type Msg = RegisterMsg<ClientRequestId, ReceiptKey, ()>;
    type State = ActorState;
    type Timer = ();
    type Random = ();
    type Storage = ();

    fn on_start(
        &self,
        _id: Id,
        _storage: &Option<Self::Storage>,
        o: &mut Out<Self>,
    ) -> Self::State {
        match self.role {
            Role::Server => ActorState::Server(ServerState {
                wal: MemoryWalIndex::new().model_snapshot(),
                mismatch: false,
            }),
            Role::Client => {
                let mut state = ClientState {
                    wal: MemoryWalIndex::new().model_snapshot(),
                    next_seq: 1,
                    phase: ClientPhase::Idle,
                    mismatch: false,
                };
                issue_put(&self.cfg, &mut state, o);
                ActorState::Client(state)
            }
        }
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
        match (self.role, state) {
            (Role::Server, ActorState::Server(state)) => {
                handle_server_msg(&self.cfg, state, src, msg, o);
            }
            (Role::Client, ActorState::Client(state)) => {
                handle_client_msg(&self.cfg, state, msg, o);
            }
            _ => {}
        }
    }
}

fn server_id() -> Id {
    Id::from(SERVER_ID)
}

fn request_id_for(seq: u8) -> ClientRequestId {
    let mut bytes = [0u8; 16];
    bytes[0] = seq;
    ClientRequestId::new(Uuid::from_bytes(bytes))
}

fn request_sha_for(id: ClientRequestId) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    let uuid = id.as_uuid();
    let seed = uuid.as_bytes();
    bytes[..16].copy_from_slice(seed);
    bytes[16..].copy_from_slice(seed);
    bytes
}

fn make_uuid(replica: ReplicaId, seq: u64) -> Uuid {
    let mut bytes = [0u8; 16];
    let seed = replica.as_uuid();
    let seed = seed.as_bytes();
    bytes[0] = seed[0];
    bytes[1] = seed[1];
    bytes[2] = seq as u8;
    Uuid::from_bytes(bytes)
}

fn build_receipt(
    cfg: &ServerCfg,
    snapshot: &MemoryWalIndexSnapshot,
    request_id: ClientRequestId,
    request_sha: [u8; 32],
    now_ms: u64,
) -> Result<(DurabilityReceipt, MemoryWalIndexSnapshot), ()> {
    let index = MemoryWalIndex::from_snapshot(snapshot.clone());
    let existing = index
        .reader()
        .lookup_client_request(&cfg.namespace, &cfg.local_replica, request_id)
        .map_err(|_| ())?;
    let (txn_id, event_ids, created_at_ms) = if let Some(row) = existing {
        if row.request_sha256 != request_sha {
            return Err(());
        }
        (row.txn_id, row.event_ids, row.created_at_ms)
    } else {
        let mut txn = index.writer().begin_txn().map_err(|_| ())?;
        let seq = txn
            .next_origin_seq(&cfg.namespace, &cfg.local_replica)
            .map_err(|_| ())?;
        let event_id = EventId::new(cfg.local_replica, cfg.namespace.clone(), seq);
        let txn_id = TxnId::new(make_uuid(cfg.local_replica, seq.get()));
        txn.upsert_client_request(
            &cfg.namespace,
            &cfg.local_replica,
            request_id,
            request_sha,
            txn_id,
            std::slice::from_ref(&event_id),
            now_ms,
        )
        .map_err(|_| ())?;
        txn.commit().map_err(|_| ())?;
        (txn_id, vec![event_id], now_ms)
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
    Ok((receipt, snapshot))
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

fn handle_server_msg(
    cfg: &ServerCfg,
    state: &mut ServerState,
    src: Id,
    msg: RegisterMsg<ClientRequestId, ReceiptKey, ()>,
    o: &mut Out<ModelActor>,
) {
    match msg {
        RegisterMsg::Put(request_id, expected_key) => {
            let request_sha = request_sha_for(request_id);
            match build_receipt(cfg, &state.wal, request_id, request_sha, 0) {
                Ok((receipt, snapshot)) => {
                    let key = ReceiptKey::from_receipt(&receipt);
                    if key != expected_key {
                        state.mismatch = true;
                    }
                    state.wal = snapshot;
                }
                Err(()) => {
                    state.mismatch = true;
                }
            }
            o.send(src, RegisterMsg::PutOk(request_id));
        }
        RegisterMsg::Get(request_id) => {
            if let Ok(receipt) = lookup_receipt(cfg, &state.wal, request_id) {
                let key = ReceiptKey::from_receipt(&receipt);
                o.send(src, RegisterMsg::GetOk(request_id, key));
            } else {
                state.mismatch = true;
            }
        }
        _ => {}
    }
}

fn issue_put(cfg: &ServerCfg, state: &mut ClientState, o: &mut Out<ModelActor>) {
    if state.next_seq > MAX_REQUESTS {
        state.phase = ClientPhase::Done;
        return;
    }
    let request_id = request_id_for(state.next_seq);
    let request_sha = request_sha_for(request_id);
    match build_receipt(cfg, &state.wal, request_id, request_sha, 0) {
        Ok((receipt, snapshot)) => {
            let key = ReceiptKey::from_receipt(&receipt);
            state.wal = snapshot;
            o.send(server_id(), RegisterMsg::Put(request_id, key.clone()));
            state.phase = ClientPhase::WaitingPut { request_id, key };
        }
        Err(()) => {
            state.mismatch = true;
            state.phase = ClientPhase::Done;
        }
    }
}

fn handle_client_msg(
    cfg: &ServerCfg,
    state: &mut ClientState,
    msg: RegisterMsg<ClientRequestId, ReceiptKey, ()>,
    o: &mut Out<ModelActor>,
) {
    match msg {
        RegisterMsg::PutOk(ok_id) => {
            let (request_id, key) = match &state.phase {
                ClientPhase::WaitingPut { request_id, key } if *request_id == ok_id => {
                    (*request_id, key.clone())
                }
                _ => return,
            };
            o.send(server_id(), RegisterMsg::Get(request_id));
            state.phase = ClientPhase::WaitingGet { request_id, key };
        }
        RegisterMsg::GetOk(ok_id, got_key) => {
            let (_request_id, key) = match &state.phase {
                ClientPhase::WaitingGet { request_id, key } if *request_id == ok_id => {
                    (*request_id, key.clone())
                }
                _ => return,
            };
            if key != got_key {
                state.mismatch = true;
            }
            state.next_seq = state.next_seq.saturating_add(1);
            state.phase = ClientPhase::Idle;
            issue_put(cfg, state, o);
        }
        _ => {}
    }
}

fn build_model() -> ActorModel<ModelActor, (), LinearizabilityTester<Id, Register<ReceiptKey>>> {
    let cfg = ServerCfg::new();
    let actors = vec![ModelActor::server(cfg.clone()), ModelActor::client(cfg)];
    ActorModel::new(
        (),
        LinearizabilityTester::new(Register(ReceiptKey::default())),
    )
    .actors(actors)
    .init_network(Network::new_ordered([]))
    .lossy_network(LossyNetwork::No)
    .record_msg_out(RegisterMsg::record_invocations)
    .record_msg_in(RegisterMsg::record_returns)
    .property(
        Expectation::Always,
        "receipt history is linearizable",
        |_, s| s.history.is_consistent(),
    )
    .property(
        Expectation::Always,
        "client/server receipt keys stay in sync",
        |_, s| {
            s.actor_states.iter().all(|state| match &**state {
                ActorState::Server(state) => !state.mismatch,
                ActorState::Client(state) => !state.mismatch,
            })
        },
    )
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init();

    let mut args = pico_args::Arguments::from_env();
    match args.subcommand()?.as_deref() {
        Some("explore") => {
            let address = args
                .opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!("Exploring receipt consistency state space on {address}.");
            build_model()
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .serve(address);
        }
        Some("check") | None => {
            println!("Model checking receipt consistency.");
            build_model()
                .checker()
                .threads(num_cpus::get())
                .timeout(Duration::from_secs(60))
                .spawn_dfs()
                .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        _ => {
            println!("USAGE:");
            println!("  receipt_consistency_machine check");
            println!("  receipt_consistency_machine explore [ADDRESS]");
        }
    }

    Ok(())
}
