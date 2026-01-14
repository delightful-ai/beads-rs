//! Phase 7 tests: streaming subscriptions.

mod fixtures;

use std::path::PathBuf;

use beads_rs::daemon::ipc::{ReadConsistency, Request, Response, subscribe_stream};
use beads_rs::{Applied, ErrorCode, HeadStatus, NamespaceId, Seq0, Watermarks};

use fixtures::admin_status::StatusCollector;
use fixtures::ipc_stream::StreamingClient;
use fixtures::load_gen::{LoadGenerator, LoadReport};
use fixtures::realtime::RealtimeFixture;

#[test]
fn phase7_subscribe_streams_events_in_order() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();

    let (origin, start_seq) = current_origin_seq(&repo, &namespace);

    let report = run_load(repo.clone(), 5, &namespace);
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let required = require_min_seen(&namespace, origin, start_seq);
    let read = ReadConsistency {
        namespace: Some(namespace.as_str().to_string()),
        require_min_seen: Some(required),
        wait_timeout_ms: None,
    };
    let mut client = StreamingClient::subscribe_with_read(repo, read).expect("subscribe");
    let seqs = collect_origin_seqs(&mut client, origin, start_seq, report.successes);

    let expected: Vec<u64> = ((start_seq + 1)..=(start_seq + report.successes as u64)).collect();
    assert_eq!(seqs, expected);
}

#[test]
fn phase7_subscribe_gates_on_require_min_seen() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let (origin, start_seq) = current_origin_seq(&repo, &namespace);

    let required_seq = start_seq + 1;
    let required = require_min_seen(&namespace, origin, required_seq);
    let read = ReadConsistency {
        namespace: Some(namespace.as_str().to_string()),
        require_min_seen: Some(required.clone()),
        wait_timeout_ms: None,
    };
    let request = Request::Subscribe {
        repo: repo.clone(),
        read,
    };

    let mut stream = subscribe_stream(&request).expect("subscribe stream");
    let response = stream
        .read_response()
        .expect("read response")
        .expect("response");
    match response {
        Response::Err { err } => {
            assert_eq!(err.code, ErrorCode::RequireMinSeenUnsatisfied);
            assert!(err.retryable, "require_min_seen should be retryable");
        }
        other => panic!("expected require_min_seen error, got {other:?}"),
    }

    let report = run_load(repo.clone(), 1, &namespace);
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let read = ReadConsistency {
        namespace: Some(namespace.as_str().to_string()),
        require_min_seen: Some(required),
        wait_timeout_ms: None,
    };
    let mut client = StreamingClient::subscribe_with_read(repo, read).expect("subscribe");
    let event = client.next_event().expect("next event").expect("event");
    assert_eq!(event.event_id.origin_replica_id, origin);
    assert!(event.event_id.origin_seq.get() >= required_seq);
}

#[test]
fn phase7_subscribe_multiple_clients_receive_same_events() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let (origin, start_seq) = current_origin_seq(&repo, &namespace);

    let report = run_load(repo.clone(), 3, &namespace);
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let required = require_min_seen(&namespace, origin, start_seq);
    let read = ReadConsistency {
        namespace: Some(namespace.as_str().to_string()),
        require_min_seen: Some(required),
        wait_timeout_ms: None,
    };

    let mut client_a = StreamingClient::subscribe_with_read(repo.clone(), read.clone())
        .expect("subscribe client A");
    let mut client_b =
        StreamingClient::subscribe_with_read(repo, read).expect("subscribe client B");

    let seqs_a = collect_origin_seqs(&mut client_a, origin, start_seq, report.successes);
    let seqs_b = collect_origin_seqs(&mut client_b, origin, start_seq, report.successes);

    assert_eq!(seqs_a, seqs_b);
}

fn run_load(repo: PathBuf, total: usize, namespace: &NamespaceId) -> LoadReport {
    let mut generator = LoadGenerator::new(repo);
    let config = generator.config_mut();
    config.workers = 1;
    config.total_requests = total;
    config.namespace = Some(namespace.as_str().to_string());
    generator.run()
}

fn current_origin_seq(repo: &PathBuf, namespace: &NamespaceId) -> (beads_rs::ReplicaId, u64) {
    let mut collector = StatusCollector::new(repo.clone());
    let status = collector.sample().expect("admin status");
    let origin = status.replica_id;
    let start_seq = status
        .watermarks_applied
        .get(namespace, &origin)
        .map(|mark| mark.seq().get())
        .unwrap_or(0);
    (origin, start_seq)
}

fn require_min_seen(
    namespace: &NamespaceId,
    origin: beads_rs::ReplicaId,
    seq: u64,
) -> Watermarks<Applied> {
    let mut required = Watermarks::<Applied>::new();
    let head = if seq == 0 {
        HeadStatus::Genesis
    } else {
        HeadStatus::Unknown
    };
    required
        .observe_at_least(namespace, &origin, Seq0::new(seq), head)
        .expect("watermark");
    required
}

fn collect_origin_seqs(
    client: &mut StreamingClient,
    origin: beads_rs::ReplicaId,
    start_seq: u64,
    total: usize,
) -> Vec<u64> {
    let mut seqs = Vec::with_capacity(total);
    while seqs.len() < total {
        let event = client.next_event().expect("stream event").expect("event");
        if event.event_id.origin_replica_id != origin {
            continue;
        }
        let seq = event.event_id.origin_seq.get();
        if seq <= start_seq {
            continue;
        }
        seqs.push(seq);
    }
    seqs
}
