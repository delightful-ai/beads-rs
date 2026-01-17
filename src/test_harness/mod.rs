use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crossbeam::channel::{Receiver, Sender, unbounded};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use uuid::Uuid;

use crate::core::time::{WallClockGuard, WallClockSource, set_wall_clock_source_for_tests};
use crate::core::{
    ActorId, BeadId, EventId, EventShaLookupError, Limits, NamespaceId, ReplicaId, Seq0, Sha256,
    StoreEpoch, StoreId, StoreIdentity,
};
use crate::daemon::Clock;
use crate::daemon::admission::AdmissionController;
use crate::daemon::core::{Daemon, HandleOutcome, insert_store_for_tests};
use crate::daemon::ipc::{MutationMeta, Request, Response, ResponsePayload};
use crate::daemon::remote::RemoteUrl;
use crate::daemon::repl::frame::{FrameReader, encode_frame};
use crate::daemon::repl::proto::{
    PROTOCOL_VERSION_V1, ReplEnvelope, decode_envelope, encode_envelope,
};
use crate::daemon::repl::{
    Ack, Events, IngestOutcome, ReplError, Session, SessionAction, SessionConfig, SessionRole,
    SessionStore, Want, WatermarkHeads, WatermarkMap, WatermarkSnapshot,
};
use crate::daemon::wal::{
    EventWal, MemoryWalIndex, SegmentConfig, SegmentSyncMode, WalIndexError, rebuild_index,
};
use crate::paths;
use std::io::Cursor;

#[derive(Clone)]
pub struct TestClock {
    now: Arc<AtomicU64>,
}

impl TestClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            now: Arc::new(AtomicU64::new(start_ms)),
        }
    }

    pub fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    pub fn advance_ms(&self, delta_ms: u64) {
        self.now.fetch_add(delta_ms, Ordering::SeqCst);
    }

    pub fn set_ms(&self, now_ms: u64) {
        self.now.store(now_ms, Ordering::SeqCst);
    }
}

impl WallClockSource for TestClock {
    fn now_ms(&self) -> u64 {
        self.now_ms()
    }
}

impl crate::daemon::clock::TimeSource for TestClock {
    fn now_ms(&self) -> u64 {
        self.now_ms()
    }
}

pub struct TestWorld {
    clock: TestClock,
    _guard: WallClockGuard,
    tmp_root: PathBuf,
    keep_tmp: bool,
}

impl TestWorld {
    pub fn new(start_ms: u64) -> Self {
        let clock = TestClock::new(start_ms);
        let guard = set_wall_clock_source_for_tests(Arc::new(clock.clone()));
        let tmp_root = ensure_tmp_root();
        let keep_tmp = std::env::var("BD_TEST_KEEP_TMP").is_ok();
        Self {
            clock,
            _guard: guard,
            tmp_root,
            keep_tmp,
        }
    }

    pub fn clock(&self) -> TestClock {
        self.clock.clone()
    }

    pub fn node(&self, label: &str, store_id: StoreId, options: NodeOptions) -> TestNode {
        TestNode::new(
            label,
            store_id,
            self.clock.clone(),
            &self.tmp_root,
            self.keep_tmp,
            options,
        )
    }

    pub fn in_memory_node(&self, label: &str, store_id: StoreId) -> TestNode {
        self.node(label, store_id, NodeOptions::in_memory())
    }
}

#[derive(Clone)]
pub struct TestNode {
    inner: Arc<RefCell<TestNodeInner>>,
}

struct TestNodeInner {
    daemon: Daemon,
    store_id: StoreId,
    replica_id: ReplicaId,
    repo_path: PathBuf,
    data_dir: PathBuf,
    _dir: TestDir,
    git_tx: Sender<crate::daemon::GitOp>,
    _git_rx: Receiver<crate::daemon::GitOp>,
}

#[derive(Clone)]
pub struct NodeOptions {
    pub limits: Limits,
    pub sync_mode: SegmentSyncMode,
    pub wal_index: WalIndexBackend,
    pub wal_backend: WalBackend,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            sync_mode: SegmentSyncMode::None,
            wal_index: WalIndexBackend::Sqlite,
            wal_backend: WalBackend::Disk,
        }
    }
}

impl NodeOptions {
    pub fn in_memory() -> Self {
        Self {
            wal_index: WalIndexBackend::Memory,
            wal_backend: WalBackend::Memory,
            ..Self::default()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalIndexBackend {
    Sqlite,
    Memory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalBackend {
    Disk,
    Memory,
}

pub fn new_in_memory_store(start_ms: u64, label: &str) -> (TestWorld, TestNode, StoreId) {
    let world = TestWorld::new(start_ms);
    let store_id = StoreId::new(Uuid::new_v4());
    let node = world.in_memory_node(label, store_id);
    (world, node, store_id)
}

pub struct InMemoryStore {
    pub world: TestWorld,
    pub node: TestNode,
    pub store_id: StoreId,
}

impl InMemoryStore {
    pub fn new(start_ms: u64, label: &str) -> Self {
        let store_id = deterministic_store_id(label, start_ms);
        let world = TestWorld::new(start_ms);
        let node = world.in_memory_node(label, store_id);
        Self {
            world,
            node,
            store_id,
        }
    }
}

pub fn deterministic_store_id(label: &str, start_ms: u64) -> StoreId {
    let seed = format!("{label}:{start_ms}");
    StoreId::new(Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes()))
}

impl TestNode {
    pub fn new(
        label: &str,
        store_id: StoreId,
        clock: TestClock,
        tmp_root: &Path,
        keep_tmp: bool,
        options: NodeOptions,
    ) -> Self {
        let dir = TestDir::new(tmp_root, label, keep_tmp);
        let repo_path = dir.path.join("repo");
        let data_dir = dir.path.join("data");
        std::fs::create_dir_all(&repo_path).expect("create repo dir");
        std::fs::create_dir_all(&data_dir).expect("create data dir");

        let actor = ActorId::new(format!("test-{label}")).expect("actor id");
        let mut daemon = Daemon::new_with_limits(actor, options.limits.clone());
        *daemon.clock_mut() = Clock::with_time_source_and_max_forward_drift(
            Box::new(clock.clone()),
            options.limits.hlc_max_forward_drift_ms,
        );
        let remote = RemoteUrl(format!("test://{store_id}"));

        let (git_tx, git_rx) = unbounded();
        {
            let _guard = paths::override_data_dir_for_tests(Some(data_dir.clone()));
            insert_store_for_tests(&mut daemon, store_id, remote, &repo_path)
                .expect("insert store");
            if let Some(runtime) = daemon.store_runtime_by_id_mut(store_id) {
                let config =
                    SegmentConfig::from_limits(&options.limits).with_sync_mode(options.sync_mode);
                runtime.event_wal = match options.wal_backend {
                    WalBackend::Disk => EventWal::new_with_config(
                        paths::store_dir(store_id),
                        runtime.meta.clone(),
                        config,
                    ),
                    WalBackend::Memory => EventWal::new_memory_with_config(
                        paths::store_dir(store_id),
                        runtime.meta.clone(),
                        config,
                    ),
                };
                if options.wal_index == WalIndexBackend::Memory {
                    runtime.wal_index = Arc::new(MemoryWalIndex::new());
                    let store_dir = paths::store_dir(store_id);
                    rebuild_index(
                        &store_dir,
                        &runtime.meta,
                        runtime.wal_index.as_ref(),
                        &options.limits,
                    )
                    .expect("rebuild memory wal index");
                }
            }
        }

        let replica_id = {
            let _guard = paths::override_data_dir_for_tests(Some(data_dir.clone()));
            daemon
                .store_runtime_by_id(store_id)
                .expect("store runtime")
                .meta
                .replica_id
        };

        let inner = TestNodeInner {
            daemon,
            store_id,
            replica_id,
            repo_path,
            data_dir,
            _dir: dir,
            git_tx,
            _git_rx: git_rx,
        };

        let _ = clock; // keep clock alive for the node's lifetime
        Self {
            inner: Arc::new(RefCell::new(inner)),
        }
    }

    pub fn store_id(&self) -> StoreId {
        self.inner.borrow().store_id
    }

    pub fn replica_id(&self) -> ReplicaId {
        self.inner.borrow().replica_id
    }

    pub fn store_identity(&self) -> StoreIdentity {
        let inner = self.inner.borrow();
        StoreIdentity::new(inner.store_id, StoreEpoch::ZERO)
    }

    pub fn repo_path(&self) -> PathBuf {
        self.inner.borrow().repo_path.clone()
    }

    pub fn session_store(&self) -> TestSessionStore {
        TestSessionStore {
            node: self.clone(),
            store_id: self.store_id(),
        }
    }

    pub fn apply_request(&self, req: Request) -> Response {
        self.with_daemon_mut(|daemon, git_tx| match daemon.handle_request(req, git_tx) {
            HandleOutcome::Response(response) => response,
            HandleOutcome::DurabilityWait(_) => {
                panic!("durability wait not supported in test harness")
            }
        })
    }

    pub fn create_issue(&self, title: &str) -> String {
        let request = Request::Create {
            repo: self.repo_path(),
            id: None,
            parent: None,
            title: title.to_string(),
            bead_type: crate::core::BeadType::Task,
            priority: crate::core::Priority::MEDIUM,
            description: None,
            design: None,
            acceptance_criteria: None,
            assignee: None,
            external_ref: None,
            estimated_minutes: None,
            labels: Vec::new(),
            dependencies: Vec::new(),
            meta: MutationMeta::default(),
        };

        let response = self.apply_request(request);
        match response {
            Response::Ok {
                ok: ResponsePayload::Op(payload),
            } => payload.issue.expect("created issue").id,
            other => panic!("unexpected response: {other:?}"),
        }
    }

    pub fn has_bead(&self, bead_id: &BeadId) -> bool {
        self.with_daemon(|daemon| {
            let store_id = self.store_id();
            let Some(runtime) = daemon.store_runtime_by_id(store_id) else {
                return false;
            };
            runtime
                .state
                .get(&NamespaceId::core())
                .and_then(|state| state.get_live(bead_id))
                .is_some()
        })
    }

    pub fn applied_seq(&self, namespace: &NamespaceId, origin: ReplicaId) -> u64 {
        self.with_daemon(|daemon| {
            let store_id = self.store_id();
            let runtime = daemon.store_runtime_by_id(store_id).expect("store runtime");
            runtime
                .watermarks_applied
                .get(namespace, &origin)
                .map(|mark| mark.seq().get())
                .unwrap_or(0)
        })
    }

    pub fn durable_seq(&self, namespace: &NamespaceId, origin: ReplicaId) -> u64 {
        self.with_daemon(|daemon| {
            let store_id = self.store_id();
            let runtime = daemon.store_runtime_by_id(store_id).expect("store runtime");
            runtime
                .watermarks_durable
                .get(namespace, &origin)
                .map(|mark| mark.seq().get())
                .unwrap_or(0)
        })
    }

    pub fn watermark_snapshot(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
        let store = self.session_store();
        store.watermark_snapshot(namespaces)
    }

    pub fn read_wal_range(
        &self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        from_seq_excl: Seq0,
    ) -> Vec<crate::core::EventFrameV1> {
        self.with_daemon(|daemon| {
            let store_id = self.store_id();
            let runtime = daemon.store_runtime_by_id(store_id).expect("store runtime");
            let reader = crate::daemon::repl::WalRangeReader::new(
                store_id,
                runtime.wal_index.clone(),
                daemon.limits().clone(),
            );
            match reader.read_range(
                namespace,
                origin,
                from_seq_excl,
                daemon.limits().max_event_batch_bytes,
            ) {
                Ok(frames) => frames,
                Err(crate::daemon::repl::WalRangeError::MissingRange { .. }) => Vec::new(),
                Err(err) => panic!("wal range: {err:?}"),
            }
        })
    }

    pub fn record_peer_ack(&self, peer: ReplicaId, ack: &Ack) {
        self.with_daemon(|daemon| {
            let store_id = self.store_id();
            let runtime = daemon.store_runtime_by_id(store_id).expect("store runtime");
            let mut table = runtime.peer_acks.lock().expect("peer ack lock poisoned");
            let _ = table.update_peer(
                peer,
                &ack.durable,
                ack.durable_heads.as_ref(),
                ack.applied.as_ref(),
                ack.applied_heads.as_ref(),
                crate::core::WallClock::now().0,
            );
        });
    }

    fn with_daemon<R>(&self, f: impl FnOnce(&Daemon) -> R) -> R {
        let data_dir = self.inner.borrow().data_dir.clone();
        let _guard = paths::override_data_dir_for_tests(Some(data_dir));
        let inner = self.inner.borrow();
        f(&inner.daemon)
    }

    fn with_daemon_mut<R>(
        &self,
        f: impl FnOnce(&mut Daemon, &Sender<crate::daemon::GitOp>) -> R,
    ) -> R {
        let data_dir = self.inner.borrow().data_dir.clone();
        let _guard = paths::override_data_dir_for_tests(Some(data_dir));
        let mut inner = self.inner.borrow_mut();
        let git_tx = inner.git_tx.clone();
        f(&mut inner.daemon, &git_tx)
    }
}

pub struct TestSessionStore {
    node: TestNode,
    store_id: StoreId,
}

impl SessionStore for TestSessionStore {
    fn watermark_snapshot(&self, namespaces: &[NamespaceId]) -> WatermarkSnapshot {
        let namespace_filter = if namespaces.is_empty() {
            None
        } else {
            Some(namespaces.to_vec())
        };
        self.node.with_daemon(|daemon| {
            let runtime = daemon
                .store_runtime_by_id(self.store_id)
                .expect("store runtime");
            let rows = match runtime.wal_index.reader().load_watermarks() {
                Ok(rows) => rows,
                Err(_) => {
                    return WatermarkSnapshot {
                        durable: WatermarkMap::new(),
                        durable_heads: WatermarkHeads::new(),
                        applied: WatermarkMap::new(),
                        applied_heads: WatermarkHeads::new(),
                    };
                }
            };

            let mut durable = WatermarkMap::new();
            let mut durable_heads = WatermarkHeads::new();
            let mut applied = WatermarkMap::new();
            let mut applied_heads = WatermarkHeads::new();

            for row in rows {
                if let Some(filter) = &namespace_filter
                    && !filter.contains(&row.namespace)
                {
                    continue;
                }

                let ns = row.namespace.clone();
                let origin = row.origin;
                durable
                    .entry(ns.clone())
                    .or_default()
                    .insert(origin, Seq0::new(row.durable_seq));
                applied
                    .entry(ns.clone())
                    .or_default()
                    .insert(origin, Seq0::new(row.applied_seq));

                if let Some(head) = row.durable_head_sha {
                    durable_heads
                        .entry(ns.clone())
                        .or_default()
                        .insert(origin, Sha256(head));
                }
                if let Some(head) = row.applied_head_sha {
                    applied_heads
                        .entry(ns)
                        .or_default()
                        .insert(origin, Sha256(head));
                }
            }

            WatermarkSnapshot {
                durable,
                durable_heads,
                applied,
                applied_heads,
            }
        })
    }

    fn lookup_event_sha(&self, eid: &EventId) -> Result<Option<Sha256>, EventShaLookupError> {
        self.node.with_daemon(|daemon| {
            let runtime = daemon
                .store_runtime_by_id(self.store_id)
                .expect("store runtime");
            runtime
                .wal_index
                .reader()
                .lookup_event_sha(&eid.namespace, eid)
                .map(|opt: Option<[u8; 32]>| opt.map(Sha256))
                .map_err(EventShaLookupError::new)
        })
    }

    fn ingest_remote_batch(
        &mut self,
        namespace: &NamespaceId,
        origin: &ReplicaId,
        batch: &[crate::core::VerifiedEvent<crate::core::PrevVerified>],
        now_ms: u64,
    ) -> Result<IngestOutcome, ReplError> {
        let events = batch.to_vec();
        self.node.with_daemon_mut(|daemon, _git_tx| {
            daemon.ingest_remote_batch_for_tests(
                self.store_id,
                namespace.clone(),
                *origin,
                events,
                now_ms,
            )
        })
    }

    fn update_replica_liveness(
        &mut self,
        replica_id: ReplicaId,
        last_seen_ms: u64,
        last_handshake_ms: u64,
        role: crate::core::ReplicaRole,
        durability_eligible: bool,
    ) -> Result<(), WalIndexError> {
        self.node.with_daemon(|daemon| {
            let runtime = daemon
                .store_runtime_by_id(self.store_id)
                .expect("store runtime");
            let mut txn = runtime.wal_index.writer().begin_txn()?;
            txn.upsert_replica_liveness(&crate::daemon::wal::ReplicaLivenessRow {
                replica_id,
                last_seen_ms,
                last_handshake_ms,
                role,
                durability_eligible,
            })?;
            txn.commit()
        })
    }
}

pub struct ReplicationRig {
    _world: TestWorld,
    clock: TestClock,
    nodes: Vec<TestNode>,
    links: Vec<RigLink>,
}

impl ReplicationRig {
    pub fn new(node_count: usize, start_ms: u64) -> Self {
        Self::new_with_options(node_count, start_ms, NodeOptions::default())
    }

    pub fn new_with_options(node_count: usize, start_ms: u64, options: NodeOptions) -> Self {
        assert!(node_count >= 2, "replication rig needs at least two nodes");
        let world = TestWorld::new(start_ms);
        let clock = world.clock();
        let store_id = StoreId::new(Uuid::new_v4());

        let mut nodes = Vec::with_capacity(node_count);
        for idx in 0..node_count {
            nodes.push(world.node(&format!("node-{idx}"), store_id, options.clone()));
        }

        let mut rig = Self {
            _world: world,
            clock,
            nodes,
            links: Vec::new(),
        };
        rig.connect_all();
        rig
    }

    pub fn node(&self, index: usize) -> TestNode {
        self.nodes[index].clone()
    }

    pub fn nodes(&self) -> &[TestNode] {
        &self.nodes
    }

    pub fn set_network_profile_all(&mut self, profile: NetworkProfile, seed: u64) {
        for (idx, link) in self.links.iter_mut().enumerate() {
            let net = &mut link.transport.network;
            net.set_profile(profile);
            net.set_seed(seed ^ (idx as u64).wrapping_mul(0x9E3779B97F4A7C15));
        }
    }

    pub fn clock(&self) -> TestClock {
        self.clock.clone()
    }

    pub fn advance_ms(&mut self, delta_ms: u64) {
        self.clock.advance_ms(delta_ms);
        for link in &mut self.links {
            link.transport.network.advance(delta_ms);
        }
    }

    pub fn pump(&mut self, max_steps: usize) -> usize {
        let mut steps = 0;
        while steps < max_steps {
            let mut progressed = false;
            for link in &mut self.links {
                progressed |= link.step(self.clock.now_ms());
            }
            if !progressed {
                break;
            }
            steps += 1;
        }
        steps
    }

    pub fn pump_until<F>(&mut self, max_steps: usize, mut predicate: F)
    where
        F: FnMut(&Self) -> bool,
    {
        for _ in 0..max_steps {
            if predicate(self) {
                return;
            }
            if self.pump(1) == 0 {
                self.advance_ms(1);
            }
        }
        assert!(predicate(self), "replication rig did not converge");
    }

    pub fn link_network(&mut self, from: usize, to: usize) -> Option<&mut NetworkController> {
        self.links
            .iter_mut()
            .find(|link| link.from == from && link.to == to)
            .map(|link| &mut link.transport.network)
    }

    pub fn seen_vectors(&self, namespaces: &[NamespaceId]) -> Vec<WatermarkSnapshot> {
        self.nodes
            .iter()
            .map(|node| node.watermark_snapshot(namespaces))
            .collect()
    }

    pub fn converged(&self, namespaces: &[NamespaceId]) -> bool {
        let mut snapshots = self.seen_vectors(namespaces).into_iter();
        let Some(first) = snapshots.next() else {
            return true;
        };
        snapshots.all(|snapshot| {
            snapshot.durable == first.durable
                && snapshot.durable_heads == first.durable_heads
                && snapshot.applied == first.applied
                && snapshot.applied_heads == first.applied_heads
        })
    }

    pub fn assert_converged(&self, namespaces: &[NamespaceId]) {
        assert!(self.converged(namespaces), "replication rig not converged");
    }

    pub fn pump_until_converged(&mut self, max_steps: usize, namespaces: &[NamespaceId]) {
        self.pump_until(max_steps, |rig| rig.converged(namespaces));
    }

    fn connect_all(&mut self) {
        let limits = self.nodes[0].with_daemon(|daemon| daemon.limits().clone());
        for from in 0..self.nodes.len() {
            for to in 0..self.nodes.len() {
                if from == to {
                    continue;
                }
                let transport = ChannelTransport::with_limits(&limits);
                let from_node = self.nodes[from].clone();
                let to_node = self.nodes[to].clone();
                let identity = from_node.store_identity();
                let from_replica = from_node.replica_id();
                let to_replica = to_node.replica_id();
                let outbound_store = from_node.session_store();
                let inbound_store = to_node.session_store();
                let mut outbound =
                    new_session(SessionRole::Outbound, identity, from_replica, &limits);
                let inbound = new_session(SessionRole::Inbound, identity, to_replica, &limits);
                if let Some(action) = outbound.begin_handshake(&outbound_store, self.clock.now_ms())
                {
                    let mut link = RigLink::new(
                        from,
                        to,
                        transport,
                        outbound,
                        inbound,
                        outbound_store,
                        inbound_store,
                        from_node,
                        to_node,
                    );
                    link.apply_action(action, Endpoint::Outbound, self.clock.now_ms());
                    self.links.push(link);
                } else {
                    self.links.push(RigLink::new(
                        from,
                        to,
                        transport,
                        outbound,
                        inbound,
                        outbound_store,
                        inbound_store,
                        from_node,
                        to_node,
                    ));
                }
            }
        }
    }
}

fn new_session(
    role: SessionRole,
    identity: StoreIdentity,
    replica: ReplicaId,
    limits: &Limits,
) -> Session {
    let mut config = SessionConfig::new(identity, replica, limits);
    config.requested_namespaces = vec![NamespaceId::core()];
    config.offered_namespaces = vec![NamespaceId::core()];
    let admission = AdmissionController::new(limits);
    Session::new(role, config, limits.clone(), admission)
}

enum Endpoint {
    Outbound,
    Inbound,
}

struct RigLink {
    from: usize,
    to: usize,
    transport: ChannelTransport,
    outbound: Session,
    inbound: Session,
    outbound_store: TestSessionStore,
    inbound_store: TestSessionStore,
    from_node: TestNode,
    to_node: TestNode,
    last_want_sent: Option<WatermarkMap>,
    last_want_sent_at_ms: Option<u64>,
}

impl RigLink {
    fn new(
        from: usize,
        to: usize,
        transport: ChannelTransport,
        outbound: Session,
        inbound: Session,
        outbound_store: TestSessionStore,
        inbound_store: TestSessionStore,
        from_node: TestNode,
        to_node: TestNode,
    ) -> Self {
        Self {
            from,
            to,
            transport,
            outbound,
            inbound,
            outbound_store,
            inbound_store,
            from_node,
            to_node,
            last_want_sent: None,
            last_want_sent_at_ms: None,
        }
    }

    fn step(&mut self, now_ms: u64) -> bool {
        let mut progressed = false;
        self.transport.network.flush();

        let outbound_msgs = self.transport.a.drain_messages();
        if !outbound_msgs.is_empty() {
            progressed = true;
            for envelope in outbound_msgs {
                let actions = self.outbound.handle_message(
                    envelope.message,
                    &mut self.outbound_store,
                    now_ms,
                );
                for action in actions {
                    self.apply_action(action, Endpoint::Outbound, now_ms);
                }
            }
        }

        let inbound_msgs = self.transport.b.drain_messages();
        if !inbound_msgs.is_empty() {
            progressed = true;
            for envelope in inbound_msgs {
                let actions =
                    self.inbound
                        .handle_message(envelope.message, &mut self.inbound_store, now_ms);
                for action in actions {
                    self.apply_action(action, Endpoint::Inbound, now_ms);
                }
            }
        }

        if matches!(
            self.inbound.phase(),
            crate::daemon::repl::SessionPhase::Streaming
        ) {
            let inbound_snapshot = self
                .inbound_store
                .watermark_snapshot(&[NamespaceId::core()]);
            let outbound_snapshot = self
                .outbound_store
                .watermark_snapshot(&[NamespaceId::core()]);
            let mut want_map = WatermarkMap::new();
            if let Some(outbound_ns) = outbound_snapshot.durable.get(&NamespaceId::core()) {
                let inbound_ns = inbound_snapshot.durable.get(&NamespaceId::core());
                for (origin, outbound_seq) in outbound_ns {
                    let inbound_seq = inbound_ns
                        .and_then(|m| m.get(origin))
                        .copied()
                        .unwrap_or(Seq0::ZERO);
                    if outbound_seq.get() > inbound_seq.get() {
                        want_map
                            .entry(NamespaceId::core())
                            .or_default()
                            .insert(*origin, inbound_seq);
                    }
                }
            }

            if want_map.is_empty() {
                self.last_want_sent = None;
                self.last_want_sent_at_ms = None;
            } else {
                let resend_due = self
                    .last_want_sent_at_ms
                    .map(|last| now_ms.saturating_sub(last) >= 25)
                    .unwrap_or(true);
                let changed = self
                    .last_want_sent
                    .as_ref()
                    .map(|last| last != &want_map)
                    .unwrap_or(true);
                if resend_due || changed {
                    let want = Want {
                        want: want_map.clone(),
                    };
                    self.transport
                        .b
                        .send_message(&crate::daemon::repl::ReplMessage::Want(want));
                    self.last_want_sent = Some(want_map);
                    self.last_want_sent_at_ms = Some(now_ms);
                    progressed = true;
                }
            }
        }

        progressed
    }

    fn apply_action(&mut self, action: SessionAction, endpoint: Endpoint, now_ms: u64) {
        match action {
            SessionAction::Send(msg) => match endpoint {
                Endpoint::Outbound => self.transport.a.send_message(&msg),
                Endpoint::Inbound => self.transport.b.send_message(&msg),
            },
            SessionAction::PeerWant(want) => {
                let (source, transport) = match endpoint {
                    Endpoint::Outbound => (&self.from_node, &self.transport.a),
                    Endpoint::Inbound => (&self.to_node, &self.transport.b),
                };
                let namespace = NamespaceId::core();
                let origin = source.replica_id();
                let from_seq = want
                    .want
                    .get(&namespace)
                    .and_then(|m| m.get(&origin))
                    .copied()
                    .unwrap_or(Seq0::ZERO);
                let events = source.read_wal_range(&namespace, &origin, from_seq);
                if !events.is_empty() {
                    let message = Events { events };
                    transport.send_message(&crate::daemon::repl::ReplMessage::Events(message));
                }
            }
            SessionAction::PeerAck(ack) => match endpoint {
                Endpoint::Outbound => {
                    let peer = self.to_node.replica_id();
                    self.from_node.record_peer_ack(peer, &ack);
                }
                Endpoint::Inbound => {
                    let peer = self.from_node.replica_id();
                    self.to_node.record_peer_ack(peer, &ack);
                }
            },
            SessionAction::PeerError(_err) => {}
            SessionAction::Close { .. } => {
                self.outbound.mark_closed();
                self.inbound.mark_closed();
            }
        }
        if matches!(endpoint, Endpoint::Outbound) {
            if let Some(action) = self.outbound.begin_handshake(&self.outbound_store, now_ms) {
                self.apply_action(action, Endpoint::Outbound, now_ms);
            }
        }
    }
}

struct TestDir {
    path: PathBuf,
    keep: bool,
}

impl TestDir {
    fn new(root: &Path, label: &str, keep: bool) -> Self {
        let id = Uuid::new_v4();
        let path = root.join(format!("{label}-{id}"));
        std::fs::create_dir_all(&path).expect("create tmp root");
        Self { path, keep }
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn in_memory_node_skips_disk_wal_dir() {
        let world = TestWorld::new(1_700_000_000_000);
        let store_id = StoreId::new(Uuid::new_v4());
        let node = world.node("mem-wal", store_id, NodeOptions::in_memory());

        let _ = node.create_issue("no wal files");
        node.with_daemon(|_| {
            let wal_dir = paths::store_dir(store_id).join("wal");
            assert!(
                !wal_dir.exists(),
                "expected no wal directory on disk for in-memory WAL"
            );
        });
    }
}

fn ensure_tmp_root() -> PathBuf {
    let root = PathBuf::from("./tmp");
    std::fs::create_dir_all(&root).expect("create ./tmp");
    root
}

#[derive(Clone)]
pub struct NetworkController {
    inner: Arc<RefCell<NetworkSimulator>>,
}

impl NetworkController {
    pub fn set_profile(&self, profile: NetworkProfile) {
        self.inner.borrow_mut().profile = profile;
    }

    pub fn set_seed(&self, seed: u64) {
        self.inner.borrow_mut().rng = StdRng::seed_from_u64(seed);
    }

    pub fn tailnet(&self, seed: u64) {
        self.set_profile(NetworkProfile::tailnet());
        self.set_seed(seed);
    }

    pub fn flush(&self) {
        self.inner.borrow_mut().flush();
    }

    pub fn advance(&self, delta_ms: u64) {
        self.inner.borrow_mut().advance(delta_ms);
    }

    pub fn drop_next(&self, direction: Direction) {
        self.inner.borrow_mut().drop_next(direction);
    }

    pub fn delay_next(&self, direction: Direction, delay_ms: u64) {
        self.inner.borrow_mut().delay_next(direction, delay_ms);
    }

    pub fn reorder_next(&self, direction: Direction) {
        self.inner.borrow_mut().reorder_next(direction);
    }

    pub fn duplicate_next(&self, direction: Direction) {
        self.inner.borrow_mut().duplicate_next(direction);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    AtoB,
    BtoA,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NetworkProfile {
    pub base_latency_ms: u64,
    pub jitter_ms: u64,
    pub loss_rate: f64,
    pub duplicate_rate: f64,
    pub reorder_rate: f64,
}

impl Default for NetworkProfile {
    fn default() -> Self {
        Self {
            base_latency_ms: 0,
            jitter_ms: 0,
            loss_rate: 0.0,
            duplicate_rate: 0.0,
            reorder_rate: 0.0,
        }
    }
}

impl NetworkProfile {
    pub fn tailnet() -> Self {
        Self {
            base_latency_ms: 20,
            jitter_ms: 30,
            loss_rate: 0.01,
            duplicate_rate: 0.002,
            reorder_rate: 0.01,
        }
    }

    fn sample_delay_ms(&self, rng: &mut StdRng) -> u64 {
        if self.base_latency_ms == 0 && self.jitter_ms == 0 {
            return 0;
        }
        let jitter = if self.jitter_ms > 0 {
            rng.random_range(0..=self.jitter_ms)
        } else {
            0
        };
        self.base_latency_ms.saturating_add(jitter)
    }
}

#[derive(Clone, Debug)]
struct ScheduledFrame {
    deliver_at_ms: u64,
    frame: Vec<u8>,
}

#[derive(Clone, Debug)]
struct SimulatedLink {
    queue: Vec<Vec<ScheduledFrame>>,
    drop_next: bool,
    delay_next_ms: u64,
    reorder_next: bool,
    duplicate_next: bool,
}

impl SimulatedLink {
    fn new() -> Self {
        Self {
            queue: Vec::new(),
            drop_next: false,
            delay_next_ms: 0,
            reorder_next: false,
            duplicate_next: false,
        }
    }

    fn enqueue(&mut self, now_ms: u64, frame: Vec<u8>) {
        if self.drop_next {
            self.drop_next = false;
            self.delay_next_ms = 0;
            self.reorder_next = false;
            self.duplicate_next = false;
            return;
        }

        let deliver_at_ms = now_ms.saturating_add(self.delay_next_ms);
        self.delay_next_ms = 0;
        let entry = ScheduledFrame {
            deliver_at_ms,
            frame,
        };

        let mut frames = vec![entry];
        if self.duplicate_next {
            self.duplicate_next = false;
            if let Some(clone) = frames.first().cloned() {
                frames.push(clone);
            }
        }

        if self.reorder_next && !self.queue.is_empty() {
            self.queue.insert(0, frames);
        } else {
            self.queue.push(frames);
        }
        self.reorder_next = false;
    }

    fn drain_ready(&mut self, now_ms: u64, deliver: &Sender<Vec<u8>>) {
        while let Some(front) = self.queue.first() {
            let mut ready = true;
            for frame in front {
                if frame.deliver_at_ms > now_ms {
                    ready = false;
                    break;
                }
            }
            if !ready {
                break;
            }
            let frames = self.queue.remove(0);
            for entry in frames {
                let _ = deliver.send(entry.frame);
            }
        }
    }
}

#[derive(Debug)]
struct NetworkSimulator {
    now_ms: u64,
    a_to_b: SimulatedLink,
    b_to_a: SimulatedLink,
    a_tx: Sender<Vec<u8>>,
    b_tx: Sender<Vec<u8>>,
    profile: NetworkProfile,
    rng: StdRng,
}

impl NetworkSimulator {
    fn new(a_tx: Sender<Vec<u8>>, b_tx: Sender<Vec<u8>>) -> Self {
        Self {
            now_ms: 0,
            a_to_b: SimulatedLink::new(),
            b_to_a: SimulatedLink::new(),
            a_tx,
            b_tx,
            profile: NetworkProfile::default(),
            rng: StdRng::seed_from_u64(0),
        }
    }

    fn send(&mut self, direction: Direction, frame: Vec<u8>) {
        let delay_ms = self.profile.sample_delay_ms(&mut self.rng);
        let loss_decision = self.profile.loss_rate > 0.0 && self.roll(self.profile.loss_rate);
        let reorder_decision =
            self.profile.reorder_rate > 0.0 && self.roll(self.profile.reorder_rate);
        let duplicate_decision =
            self.profile.duplicate_rate > 0.0 && self.roll(self.profile.duplicate_rate);

        let link = match direction {
            Direction::AtoB => &mut self.a_to_b,
            Direction::BtoA => &mut self.b_to_a,
        };

        if !link.drop_next && loss_decision {
            link.drop_next = true;
        }
        if link.delay_next_ms == 0 {
            link.delay_next_ms = delay_ms;
        }
        if !link.reorder_next && reorder_decision {
            link.reorder_next = true;
        }
        if !link.duplicate_next && duplicate_decision {
            link.duplicate_next = true;
        }

        link.enqueue(self.now_ms, frame);
    }

    fn flush(&mut self) {
        self.a_to_b.drain_ready(self.now_ms, &self.b_tx);
        self.b_to_a.drain_ready(self.now_ms, &self.a_tx);
    }

    fn advance(&mut self, delta_ms: u64) {
        self.now_ms = self.now_ms.saturating_add(delta_ms);
        self.flush();
    }

    fn drop_next(&mut self, direction: Direction) {
        match direction {
            Direction::AtoB => self.a_to_b.drop_next = true,
            Direction::BtoA => self.b_to_a.drop_next = true,
        }
    }

    fn delay_next(&mut self, direction: Direction, delay_ms: u64) {
        match direction {
            Direction::AtoB => self.a_to_b.delay_next_ms = delay_ms,
            Direction::BtoA => self.b_to_a.delay_next_ms = delay_ms,
        }
    }

    fn reorder_next(&mut self, direction: Direction) {
        match direction {
            Direction::AtoB => self.a_to_b.reorder_next = true,
            Direction::BtoA => self.b_to_a.reorder_next = true,
        }
    }

    fn duplicate_next(&mut self, direction: Direction) {
        match direction {
            Direction::AtoB => self.a_to_b.duplicate_next = true,
            Direction::BtoA => self.b_to_a.duplicate_next = true,
        }
    }

    fn roll(&mut self, rate: f64) -> bool {
        if rate <= 0.0 {
            return false;
        }
        let value: f64 = self.rng.random();
        value < rate
    }
}

#[derive(Clone)]
struct NetworkHandle {
    inner: Arc<RefCell<NetworkSimulator>>,
    direction: Direction,
}

impl NetworkHandle {
    fn send(&self, frame: Vec<u8>) {
        self.inner.borrow_mut().send(self.direction, frame);
    }
}

pub struct ChannelEndpoint {
    sender: NetworkHandle,
    receiver: Receiver<Vec<u8>>,
    max_frame_bytes: usize,
}

impl ChannelEndpoint {
    pub fn send_message(&self, message: &crate::daemon::repl::ReplMessage) {
        let frame = encode_message_frame(message.clone(), self.max_frame_bytes);
        self.sender.send(frame);
    }

    pub fn try_recv_message(&self) -> Option<ReplEnvelope> {
        let frame = self.receiver.try_recv().ok()?;
        Some(decode_message_frame(&frame, self.max_frame_bytes))
    }

    pub fn drain_messages(&self) -> Vec<ReplEnvelope> {
        let mut out = Vec::new();
        while let Ok(frame) = self.receiver.try_recv() {
            out.push(decode_message_frame(&frame, self.max_frame_bytes));
        }
        out
    }
}

pub struct ChannelTransport {
    pub a: ChannelEndpoint,
    pub b: ChannelEndpoint,
    pub network: NetworkController,
}

impl ChannelTransport {
    pub fn with_limits(limits: &Limits) -> Self {
        Self::pair(limits.max_frame_bytes)
    }

    pub fn pair(max_frame_bytes: usize) -> Self {
        let (a_tx, a_rx) = unbounded();
        let (b_tx, b_rx) = unbounded();

        let simulator = Arc::new(RefCell::new(NetworkSimulator::new(a_tx, b_tx)));
        let network = NetworkController {
            inner: simulator.clone(),
        };

        let a = ChannelEndpoint {
            sender: NetworkHandle {
                inner: simulator.clone(),
                direction: Direction::AtoB,
            },
            receiver: a_rx,
            max_frame_bytes,
        };
        let b = ChannelEndpoint {
            sender: NetworkHandle {
                inner: simulator.clone(),
                direction: Direction::BtoA,
            },
            receiver: b_rx,
            max_frame_bytes,
        };

        Self { a, b, network }
    }
}

pub fn latency_budget_ms() -> u128 {
    std::env::var("BD_E2E_LATENCY_BUDGET_MS")
        .ok()
        .and_then(|raw| raw.parse::<u128>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100)
}

pub fn replication_latency_budget_ms() -> u64 {
    std::env::var("BD_E2E_REPL_LATENCY_BUDGET_MS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100)
}

pub fn measure_latency<F: FnOnce()>(f: F) -> u128 {
    let start = Instant::now();
    f();
    start.elapsed().as_millis()
}

fn encode_message_frame(
    message: crate::daemon::repl::ReplMessage,
    max_frame_bytes: usize,
) -> Vec<u8> {
    let envelope = ReplEnvelope {
        version: PROTOCOL_VERSION_V1,
        message,
    };
    let payload = encode_envelope(&envelope).expect("encode repl envelope");
    encode_frame(&payload, max_frame_bytes).expect("encode repl frame")
}

fn decode_message_frame(frame: &[u8], max_frame_bytes: usize) -> ReplEnvelope {
    let mut reader = FrameReader::new(Cursor::new(frame), max_frame_bytes);
    let payload = reader
        .read_next()
        .expect("decode repl frame")
        .expect("repl payload");
    decode_envelope(&payload, &Limits::default()).expect("decode repl envelope")
}
