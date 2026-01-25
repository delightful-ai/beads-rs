#![cfg(feature = "slow-tests")]
//! IPC streaming subscriptions.
//!
//! These tests exercise realtime streaming and include time-based waits.
use std::io::ErrorKind;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use beads_rs::daemon::ipc::{IpcError, ReadConsistency, Request, Response};
use beads_rs::{
    Applied, CliErrorCode, HeadStatus, NamespaceId, ProtocolErrorCode, Seq0, Watermarks,
};

use crate::fixtures::admin_status::StatusCollector;
use crate::fixtures::ipc_stream::{StreamClientError, StreamingClient};
use crate::fixtures::load_gen::{Autostart, LoadGenerator, LoadReport};
use crate::fixtures::realtime::RealtimeFixture;

#[test]
fn subscribe_streams_events_in_order() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let ipc_client = fixture.ipc_client().with_autostart(false);

    let (origin, start_seq) = current_origin_seq(&repo, &namespace, ipc_client.clone());

    let required = require_min_seen(&namespace, origin, start_seq);
    let read = ReadConsistency {
        namespace: Some(namespace.as_str().to_string()),
        require_min_seen: Some(required),
        wait_timeout_ms: None,
    };
    let mut client =
        StreamingClient::subscribe_with_client(repo.clone(), read.clone(), ipc_client.clone())
            .expect("subscribe");

    let report = run_load(repo.clone(), 5, &namespace, ipc_client.clone());
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);
    let seqs = collect_origin_seqs(
        &mut client,
        origin,
        start_seq,
        report.successes,
        &repo,
        &read,
        &ipc_client,
    );

    let expected: Vec<u64> = ((start_seq + 1)..=(start_seq + report.successes as u64)).collect();
    assert_eq!(seqs, expected);
}

#[test]
fn subscribe_gates_on_require_min_seen() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let ipc_client = fixture.ipc_client().with_autostart(false);
    let (origin, start_seq) = current_origin_seq(&repo, &namespace, ipc_client.clone());

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

    let mut stream = ipc_client
        .subscribe_stream(&request)
        .expect("subscribe stream");
    let response = stream
        .read_response()
        .expect("read response")
        .expect("response");
    match response {
        Response::Err { err } => {
            assert_eq!(
                err.code,
                ProtocolErrorCode::RequireMinSeenUnsatisfied.into()
            );
            assert!(err.retryable, "require_min_seen should be retryable");
        }
        other => panic!("expected require_min_seen error, got {other:?}"),
    }

    let report = run_load(repo.clone(), 1, &namespace, ipc_client.clone());
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let read = ReadConsistency {
        namespace: Some(namespace.as_str().to_string()),
        require_min_seen: Some(required),
        wait_timeout_ms: None,
    };
    let mut client =
        StreamingClient::subscribe_with_client(repo.clone(), read.clone(), ipc_client.clone())
            .expect("subscribe");

    let report = run_load(repo.clone(), 1, &namespace, ipc_client.clone());
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let seqs = collect_origin_seqs(
        &mut client,
        origin,
        required_seq,
        1,
        &repo,
        &read,
        &ipc_client,
    );
    assert_eq!(seqs, vec![required_seq + 1]);
}

#[test]
fn subscribe_multiple_clients_receive_same_events() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let ipc_client = fixture.ipc_client().with_autostart(false);
    let (origin, start_seq) = current_origin_seq(&repo, &namespace, ipc_client.clone());

    let required = require_min_seen(&namespace, origin, start_seq);
    let read = ReadConsistency {
        namespace: Some(namespace.as_str().to_string()),
        require_min_seen: Some(required),
        wait_timeout_ms: None,
    };

    let mut client_a =
        StreamingClient::subscribe_with_client(repo.clone(), read.clone(), ipc_client.clone())
            .expect("subscribe client A");
    let mut client_b =
        StreamingClient::subscribe_with_client(repo.clone(), read.clone(), ipc_client.clone())
            .expect("subscribe client B");

    let report = run_load(repo.clone(), 3, &namespace, ipc_client.clone());
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let seqs_a = collect_origin_seqs(
        &mut client_a,
        origin,
        start_seq,
        report.successes,
        &repo,
        &read,
        &ipc_client,
    );
    let seqs_b = collect_origin_seqs(
        &mut client_b,
        origin,
        start_seq,
        report.successes,
        &repo,
        &read,
        &ipc_client,
    );

    assert_eq!(seqs_a, seqs_b);
}

fn run_load(
    repo: PathBuf,
    total: usize,
    namespace: &NamespaceId,
    client: beads_rs::daemon::ipc::IpcClient,
) -> LoadReport {
    let mut generator = LoadGenerator::with_client(repo, client);
    let config = generator.config_mut();
    config.workers = 1;
    config.total_requests = total;
    config.namespace = Some(namespace.as_str().to_string());
    config.autostart = Autostart::Disabled;
    generator.run()
}

fn current_origin_seq(
    repo: &PathBuf,
    namespace: &NamespaceId,
    client: beads_rs::daemon::ipc::IpcClient,
) -> (beads_rs::ReplicaId, u64) {
    let mut collector = StatusCollector::with_client(repo.clone(), client);
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
        HeadStatus::Known([seq as u8; 32])
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
    repo: &PathBuf,
    read: &ReadConsistency,
    ipc_client: &beads_rs::daemon::ipc::IpcClient,
) -> Vec<u64> {
    const MAX_RECONNECTS: usize = 3;
    const MAX_WAIT: Duration = Duration::from_secs(30);
    let read_timeout = Duration::from_millis(200);

    let mut seqs = Vec::with_capacity(total);
    let mut last_seq = start_seq;
    let mut reconnects = 0;
    let deadline = Instant::now() + MAX_WAIT;

    client
        .set_read_timeout(Some(read_timeout))
        .expect("set subscribe timeout");

    while seqs.len() < total {
        if Instant::now() > deadline {
            let _ = client.set_read_timeout(None);
            panic!(
                "subscribe stream timed out after {:?} (got {} of {})",
                MAX_WAIT,
                seqs.len(),
                total
            );
        }

        match client.next_event() {
            Ok(Some(event)) => {
                if event.event_id.origin_replica_id != origin {
                    continue;
                }
                let seq = event.event_id.origin_seq.get();
                if seq <= last_seq {
                    continue;
                }
                last_seq = seq;
                seqs.push(seq);
            }
            Ok(None) => continue,
            Err(StreamClientError::Ipc(IpcError::Io(err)))
                if matches!(err.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) =>
            {
                continue;
            }
            Err(StreamClientError::Ipc(IpcError::Disconnected)) => {
                reconnects += 1;
                if reconnects > MAX_RECONNECTS {
                    let _ = client.set_read_timeout(None);
                    panic!("subscribe stream disconnected {} times", reconnects);
                }
                std::thread::sleep(Duration::from_millis(50 * reconnects as u64));
                *client = StreamingClient::subscribe_with_client(
                    repo.clone(),
                    read.clone(),
                    ipc_client.clone(),
                )
                .expect("resubscribe");
                client
                    .set_read_timeout(Some(read_timeout))
                    .expect("reset subscribe timeout");
            }
            Err(StreamClientError::Remote(err)) => {
                if err.retryable && err.code == CliErrorCode::Disconnected.into() {
                    reconnects += 1;
                    if reconnects > MAX_RECONNECTS {
                        let _ = client.set_read_timeout(None);
                        panic!(
                            "subscribe stream disconnected {} times (remote)",
                            reconnects
                        );
                    }
                    std::thread::sleep(Duration::from_millis(50 * reconnects as u64));
                    *client = StreamingClient::subscribe_with_client(
                        repo.clone(),
                        read.clone(),
                        ipc_client.clone(),
                    )
                    .expect("resubscribe");
                    client
                        .set_read_timeout(Some(read_timeout))
                        .expect("reset subscribe timeout");
                } else {
                    let _ = client.set_read_timeout(None);
                    panic!("stream event: {err:?}");
                }
            }
            Err(err) => {
                let _ = client.set_read_timeout(None);
                panic!("stream event: {err:?}");
            }
        }
    }
    let _ = client.set_read_timeout(None);
    seqs
}
