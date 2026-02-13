use super::ServerReply;
use super::socket::stream_event_response;
use super::spans::{ReadConsistencyTag, read_consistency_tag, request_span};
use super::waiters::{
    DurabilityWaiter, ReadGateWaiter, flush_durability_waiters, flush_read_gate_waiters,
};
use bytes::Bytes;
use crossbeam::channel::{Receiver, Sender};
use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uuid::Uuid;

use beads_daemon::broadcast::BroadcastEvent;
use beads_daemon::remote::RemoteUrl;
use beads_daemon_core::repl::proto::WatermarkState;

use crate::core::error::details as error_details;
use crate::core::replica_roster::ReplicaEntry;
use crate::core::{
    ActorId, Applied, BeadId, BeadType, ClientRequestId, DurabilityClass, DurabilityReceipt,
    Durable, EventBytes, EventId, HeadStatus, Limits, NamespaceId, NamespacePolicy, Opaque,
    Priority, ProtocolErrorCode, ReplicaDurabilityRole, ReplicaId, ReplicaRoster, Seq0, Seq1,
    Sha256, StoreEpoch, StoreId, StoreIdentity, TxnId, Watermark, Watermarks,
};
use crate::daemon::OpResult;
use crate::daemon::core::{Daemon, insert_store_for_tests};
use crate::daemon::durability_coordinator::DurabilityCoordinator;
use crate::daemon::executor::DurabilityWait;
use crate::daemon::git_worker::GitOp;
use crate::daemon::ipc::{
    MutationMeta, OpResponse, ReadConsistency, Request, Response, ResponsePayload,
};
use crate::daemon::repl::PeerAckTable;

struct TestEnv {
    _temp: TempDir,
    _override: crate::paths::DataDirOverride,
    repo_path: PathBuf,
    git_tx: Sender<GitOp>,
    _git_rx: Receiver<GitOp>,
    daemon: Daemon,
}

impl TestEnv {
    fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();
        let override_guard = crate::paths::override_data_dir_for_tests(Some(data_dir.clone()));
        let actor = ActorId::new("test@host".to_string()).unwrap();
        let mut daemon = Daemon::new(actor);
        let repo_path = temp.path().join("repo");
        std::fs::create_dir_all(&repo_path).unwrap();
        let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
        let remote = RemoteUrl::new("example.com/test/repo");
        insert_store_for_tests(&mut daemon, store_id, remote, &repo_path).unwrap();
        let (git_tx, git_rx) = crossbeam::channel::unbounded();

        Self {
            _temp: temp,
            _override: override_guard,
            repo_path,
            git_tx,
            _git_rx: git_rx,
            daemon,
        }
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
fn request_context_extracts_create_fields() {
    let repo = PathBuf::from("/tmp/repo");
    let namespace = NamespaceId::core();
    let actor = ActorId::new("actor@example.com").unwrap();
    let client_request_id = ClientRequestId::new(Uuid::from_bytes([7u8; 16]));
    let meta = MutationMeta {
        namespace: Some(namespace.clone()),
        client_request_id: Some(client_request_id),
        actor_id: Some(actor.clone()),
        durability: None,
    };
    let request = Request::Create {
        ctx: crate::daemon::ipc::MutationCtx::new(repo.clone(), meta),
        payload: crate::daemon::ipc::CreatePayload {
            id: None,
            parent: None,
            title: "title".to_string(),
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

    let info = request.info();
    assert_eq!(info.op, "create");
    assert_eq!(info.repo, Some(repo.as_path()));
    assert_eq!(info.namespace, Some(&namespace));
    assert_eq!(info.actor_id, Some(&actor));
    assert_eq!(info.client_request_id, Some(&client_request_id));
    assert!(info.read.is_none());
}

#[test]
fn request_context_extracts_show_fields() {
    let repo = PathBuf::from("/tmp/repo");
    let namespace = NamespaceId::core();
    let read = ReadConsistency {
        namespace: Some(namespace.clone()),
        require_min_seen: None,
        wait_timeout_ms: None,
    };
    let request = Request::Show {
        ctx: crate::daemon::ipc::ReadCtx::new(repo.clone(), read),
        payload: crate::daemon::ipc::IdPayload {
            id: BeadId::parse("bd-123").expect("bead id"),
        },
    };

    let info = request.info();
    assert_eq!(info.op, "show");
    assert_eq!(info.repo, Some(repo.as_path()));
    assert_eq!(info.namespace, Some(&namespace));
    assert!(info.read.is_some());
    assert_eq!(
        info.read.map(read_consistency_tag),
        Some(ReadConsistencyTag::Default)
    );
}

#[test]
fn request_span_includes_schema_fields() {
    use crate::telemetry::schema;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use tracing::Subscriber;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Registry;
    use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

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
    }

    #[derive(Default)]
    struct SpanFields {
        fields: BTreeMap<String, String>,
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
            if span.metadata().name() != "ipc_request" {
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

    let spans = Arc::new(Mutex::new(Vec::new()));
    let layer = CaptureLayer::new(spans.clone());
    let subscriber = Registry::default().with(layer);

    tracing::dispatcher::with_default(&tracing::Dispatch::new(subscriber), || {
        let repo = PathBuf::from("/tmp/repo");
        let meta = MutationMeta {
            namespace: Some(NamespaceId::core()),
            client_request_id: Some(ClientRequestId::new(Uuid::from_bytes([7u8; 16]))),
            actor_id: Some(ActorId::new("actor@example.com").unwrap()),
            durability: None,
        };
        let request = Request::Create {
            ctx: crate::daemon::ipc::MutationCtx::new(repo.clone(), meta),
            payload: crate::daemon::ipc::CreatePayload {
                id: None,
                parent: None,
                title: "title".to_string(),
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
        let info = request.info();
        let span = request_span(&info);
        let _guard = span.enter();
    });

    let captured = spans.lock().expect("span capture");
    let fields = captured.last().cloned().unwrap_or_default();
    for key in [
        schema::REQUEST_TYPE,
        schema::REPO,
        schema::NAMESPACE,
        schema::ACTOR_ID,
        schema::CLIENT_REQUEST_ID,
    ] {
        assert!(
            fields.contains_key(key),
            "ipc_request span missing {key}: {fields:?}"
        );
    }
}

#[test]
fn stream_event_decode_failure_is_wal_corrupt() {
    let namespace = NamespaceId::core();
    let origin = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
    let event_id = EventId::new(origin, namespace.clone(), Seq1::from_u64(1).unwrap());
    let bytes = EventBytes::<Opaque>::new(Bytes::from(vec![0x01]));
    let event = BroadcastEvent::new(event_id, Sha256([0u8; 32]), None, bytes);

    let response = stream_event_response(event, &Limits::default());
    let Response::Err { err } = response else {
        panic!("expected corruption error");
    };
    assert_eq!(err.code, ProtocolErrorCode::WalCorrupt.into());
    let details = err
        .details_as::<error_details::WalCorruptDetails>()
        .unwrap()
        .expect("details");
    assert_eq!(details.namespace, namespace);
    assert!(!details.reason.is_empty());
}

#[test]
fn read_gate_waiter_releases_on_apply() {
    let mut env = TestEnv::new();
    let loaded = env
        .daemon
        .ensure_repo_fresh(&env.repo_path, &env.git_tx)
        .unwrap();
    let origin = loaded.runtime().meta.replica_id;
    let namespace = NamespaceId::core();

    let mut required = Watermarks::<Applied>::new();
    let required_wm = watermark(1);
    required
        .observe_at_least(&namespace, &origin, required_wm.seq(), required_wm.head())
        .unwrap();

    let read = ReadConsistency {
        namespace: Some(namespace.clone()),
        require_min_seen: Some(required),
        wait_timeout_ms: Some(200),
    };
    let request = Request::Status {
        ctx: crate::daemon::ipc::ReadCtx::new(env.repo_path.clone(), read.clone()),
        payload: crate::daemon::ipc::EmptyPayload {},
    };
    let normalized = loaded.read_scope(read).unwrap();
    drop(loaded);
    let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
    let started_at = Instant::now();
    let deadline = started_at + Duration::from_millis(normalized.wait_timeout_ms());
    let waiter = ReadGateWaiter {
        request,
        respond: respond_tx,
        repo: env.repo_path.clone(),
        read: normalized,
        span: tracing::Span::none(),
        started_at,
        deadline,
    };

    let mut waiters = vec![waiter];
    let mut sync_waiters = HashMap::new();
    let mut checkpoint_waiters = Vec::new();
    let mut durability_waiters = Vec::new();
    flush_read_gate_waiters(
        &mut env.daemon,
        &mut waiters,
        &env.git_tx,
        &mut sync_waiters,
        &mut checkpoint_waiters,
        &mut durability_waiters,
    );
    assert_eq!(waiters.len(), 1);
    assert!(respond_rx.try_recv().is_err());

    let applied_wm = watermark(1);
    let mut loaded = env
        .daemon
        .ensure_repo_fresh(&env.repo_path, &env.git_tx)
        .unwrap();
    loaded
        .runtime_mut()
        .watermarks_applied
        .observe_at_least(&namespace, &origin, applied_wm.seq(), applied_wm.head())
        .unwrap();
    drop(loaded);

    flush_read_gate_waiters(
        &mut env.daemon,
        &mut waiters,
        &env.git_tx,
        &mut sync_waiters,
        &mut checkpoint_waiters,
        &mut durability_waiters,
    );
    assert!(waiters.is_empty());

    let reply = respond_rx.recv().unwrap();
    let ServerReply::Response(response) = reply else {
        panic!("expected response");
    };
    assert!(matches!(response, Response::Ok { .. }));
}

#[test]
fn read_gate_waiter_times_out() {
    let mut env = TestEnv::new();
    let loaded = env
        .daemon
        .ensure_repo_fresh(&env.repo_path, &env.git_tx)
        .unwrap();
    let origin = loaded.runtime().meta.replica_id;
    let namespace = NamespaceId::core();

    let mut required = Watermarks::<Applied>::new();
    let required_wm = watermark(1);
    required
        .observe_at_least(&namespace, &origin, required_wm.seq(), required_wm.head())
        .unwrap();

    let read = ReadConsistency {
        namespace: Some(namespace.clone()),
        require_min_seen: Some(required),
        wait_timeout_ms: Some(10),
    };
    let request = Request::Status {
        ctx: crate::daemon::ipc::ReadCtx::new(env.repo_path.clone(), read.clone()),
        payload: crate::daemon::ipc::EmptyPayload {},
    };
    let normalized = loaded.read_scope(read).unwrap();
    drop(loaded);
    let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
    let started_at = Instant::now() - Duration::from_millis(20);
    let deadline = started_at;
    let waiter = ReadGateWaiter {
        request,
        respond: respond_tx,
        repo: env.repo_path.clone(),
        read: normalized,
        span: tracing::Span::none(),
        started_at,
        deadline,
    };

    let mut waiters = vec![waiter];
    let mut sync_waiters = HashMap::new();
    let mut checkpoint_waiters = Vec::new();
    let mut durability_waiters = Vec::new();
    flush_read_gate_waiters(
        &mut env.daemon,
        &mut waiters,
        &env.git_tx,
        &mut sync_waiters,
        &mut checkpoint_waiters,
        &mut durability_waiters,
    );
    assert!(waiters.is_empty());

    let reply = respond_rx.recv().unwrap();
    let ServerReply::Response(response) = reply else {
        panic!("expected response");
    };
    let Response::Err { err } = response else {
        panic!("expected timeout error");
    };
    assert_eq!(err.code, ProtocolErrorCode::RequireMinSeenTimeout.into());
}

fn replica(seed: u128) -> ReplicaId {
    ReplicaId::new(Uuid::from_u128(seed))
}

fn roster(entries: Vec<ReplicaEntry>) -> ReplicaRoster {
    ReplicaRoster { replicas: entries }
}

#[test]
fn durability_waiter_releases_on_quorum() {
    let namespace = NamespaceId::core();
    let local = replica(1);
    let peer_a = replica(2);
    let peer_b = replica(3);
    let roster = roster(vec![
        ReplicaEntry {
            replica_id: local,
            name: "local".to_string(),
            role: ReplicaDurabilityRole::anchor(true),
            allowed_namespaces: None,
            expire_after_ms: None,
        },
        ReplicaEntry {
            replica_id: peer_a,
            name: "peer-a".to_string(),
            role: ReplicaDurabilityRole::peer(true),
            allowed_namespaces: None,
            expire_after_ms: None,
        },
        ReplicaEntry {
            replica_id: peer_b,
            name: "peer-b".to_string(),
            role: ReplicaDurabilityRole::peer(true),
            allowed_namespaces: None,
            expire_after_ms: None,
        },
    ]);

    let mut policies = BTreeMap::new();
    policies.insert(namespace.clone(), NamespacePolicy::core_default());

    let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
    let coordinator =
        DurabilityCoordinator::new(local, policies, Some(roster), Arc::clone(&peer_acks));

    let mut durable: WatermarkState<Durable> = BTreeMap::new();
    durable.entry(namespace.clone()).or_default().insert(
        local,
        Watermark::new(Seq0::new(2), HeadStatus::Known([2u8; 32])).unwrap(),
    );
    peer_acks
        .lock()
        .unwrap()
        .update_peer(peer_a, &durable, None, 10)
        .unwrap();
    peer_acks
        .lock()
        .unwrap()
        .update_peer(peer_b, &durable, None, 12)
        .unwrap();

    let store = StoreIdentity::new(StoreId::new(Uuid::from_u128(10)), StoreEpoch::ZERO);
    let receipt = DurabilityReceipt::local_fsync_defaults(
        store,
        TxnId::new(Uuid::from_u128(11)),
        Vec::new(),
        123,
    );
    let bead_id = BeadId::parse("bd-abc").unwrap();
    let response = OpResponse::new(OpResult::Updated { id: bead_id }, receipt);
    let wait = DurabilityWait {
        coordinator,
        namespace: namespace.clone(),
        origin: local,
        seq: Seq1::from_u64(2).unwrap(),
        requested: DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(2).unwrap(),
        },
        wait_timeout: Duration::from_millis(50),
        response,
    };

    let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
    let started_at = Instant::now();
    let deadline = started_at + Duration::from_millis(50);
    let mut waiters = vec![DurabilityWaiter {
        respond: respond_tx,
        wait,
        span: tracing::Span::none(),
        started_at,
        deadline,
    }];

    flush_durability_waiters(&mut waiters);
    assert!(waiters.is_empty());

    let reply = respond_rx.recv().unwrap();
    let ServerReply::Response(response) = reply else {
        panic!("expected response");
    };
    let Response::Ok { ok } = response else {
        panic!("expected ok response");
    };
    let ResponsePayload::Op(op) = ok else {
        panic!("expected op response");
    };

    assert!(op.receipt.outcome().is_achieved());
    assert_eq!(
        op.receipt.outcome().requested(),
        DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(2).unwrap()
        }
    );
    assert_eq!(
        op.receipt.outcome().achieved(),
        Some(DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(2).unwrap()
        })
    );

    let proof = op
        .receipt
        .durability_proof()
        .replicated
        .as_ref()
        .expect("replicated proof");
    assert_eq!(proof.k.get(), 2);
    assert_eq!(proof.acked_by.len(), 2);
    assert!(proof.acked_by.contains(&peer_a));
    assert!(proof.acked_by.contains(&peer_b));
}

#[test]
fn durability_waiter_times_out() {
    let namespace = NamespaceId::core();
    let local = replica(5);
    let peer = replica(6);
    let roster = roster(vec![
        ReplicaEntry {
            replica_id: local,
            name: "local".to_string(),
            role: ReplicaDurabilityRole::anchor(true),
            allowed_namespaces: None,
            expire_after_ms: None,
        },
        ReplicaEntry {
            replica_id: peer,
            name: "peer".to_string(),
            role: ReplicaDurabilityRole::peer(true),
            allowed_namespaces: None,
            expire_after_ms: None,
        },
    ]);

    let mut policies = BTreeMap::new();
    policies.insert(namespace.clone(), NamespacePolicy::core_default());

    let peer_acks = Arc::new(Mutex::new(PeerAckTable::new()));
    let coordinator =
        DurabilityCoordinator::new(local, policies, Some(roster), Arc::clone(&peer_acks));

    let store = StoreIdentity::new(StoreId::new(Uuid::from_u128(12)), StoreEpoch::ZERO);
    let receipt = DurabilityReceipt::local_fsync_defaults(
        store,
        TxnId::new(Uuid::from_u128(13)),
        Vec::new(),
        123,
    );
    let bead_id = BeadId::parse("bd-def").unwrap();
    let response = OpResponse::new(OpResult::Updated { id: bead_id }, receipt);
    let wait = DurabilityWait {
        coordinator,
        namespace: namespace.clone(),
        origin: local,
        seq: Seq1::from_u64(1).unwrap(),
        requested: DurabilityClass::ReplicatedFsync {
            k: NonZeroU32::new(1).unwrap(),
        },
        wait_timeout: Duration::from_millis(10),
        response,
    };

    let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
    let started_at = Instant::now() - Duration::from_millis(20);
    let deadline = started_at;
    let mut waiters = vec![DurabilityWaiter {
        respond: respond_tx,
        wait,
        span: tracing::Span::none(),
        started_at,
        deadline,
    }];

    flush_durability_waiters(&mut waiters);
    assert!(waiters.is_empty());

    let reply = respond_rx.recv().unwrap();
    let ServerReply::Response(response) = reply else {
        panic!("expected response");
    };
    let Response::Err { err } = response else {
        panic!("expected error");
    };
    assert_eq!(err.code, ProtocolErrorCode::DurabilityTimeout.into());
    let receipt = err
        .receipt_as::<DurabilityReceipt>()
        .unwrap()
        .expect("receipt");
    assert!(receipt.outcome().is_pending());
}
