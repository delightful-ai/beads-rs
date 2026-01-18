//! Realtime smoke test for basic WAL + apply plumbing.

use beads_rs::NamespaceId;

use crate::fixtures::admin_status::StatusCollector;
use crate::fixtures::load_gen::LoadGenerator;
use crate::fixtures::realtime::RealtimeFixture;

#[test]
fn realtime_smoke_applies_and_persists() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let client = fixture.ipc_client().with_autostart(false);

    let mut generator = LoadGenerator::with_client(repo.clone(), client.clone());
    let config = generator.config_mut();
    config.workers = 1;
    config.total_requests = 2;
    config.namespace = Some(namespace.as_str().to_string());
    config.autostart = false;

    let report = generator.run();
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let mut collector = StatusCollector::with_client(repo, client);
    let status = collector.sample().expect("admin status");

    let applied = status
        .watermarks_applied
        .get(&namespace, &status.replica_id)
        .map(|mark| mark.seq().get())
        .unwrap_or(0);
    assert!(
        applied >= report.successes as u64,
        "applied seq {applied} should cover {successes} events",
        successes = report.successes
    );

    let durable = status
        .watermarks_durable
        .get(&namespace, &status.replica_id)
        .map(|mark| mark.seq().get())
        .unwrap_or(0);
    assert!(
        durable >= report.successes as u64,
        "durable seq {durable} should cover {successes} events",
        successes = report.successes
    );

    let wal = status
        .wal
        .iter()
        .find(|wal| wal.namespace == namespace)
        .expect("wal namespace entry");
    assert!(wal.segment_count > 0, "expected wal segments for core namespace");
}
