//! Phase 7 tests: admin.status correctness under load.

mod fixtures;

use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use beads_rs::NamespaceId;
use beads_rs::api::AdminStatusOutput;

use fixtures::admin_status::{StatusCollector, assert_monotonic_watermarks};
use fixtures::load_gen::{LoadGenerator, LoadReport};
use fixtures::realtime::RealtimeFixture;

#[test]
fn phase7_admin_status_monotonic_under_load() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let client = fixture.ipc_client().with_autostart(false);

    let mut generator = LoadGenerator::with_client(repo.clone(), client.clone());
    let config = generator.config_mut();
    config.workers = 2;
    config.total_requests = 40;
    config.namespace = Some(namespace.as_str().to_string());
    config.autostart = false;

    let handle = thread::spawn(move || generator.run());

    let mut collector = StatusCollector::with_client(repo, client);
    collector
        .collect_for(Duration::from_millis(250), Duration::from_millis(25))
        .expect("collect status");

    let report = handle.join().expect("load join");
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);
    assert_monotonic_watermarks(collector.samples());
}

#[test]
fn phase7_admin_status_segment_stats_match_files() {
    let fixture = RealtimeFixture::new();
    fixture.start_daemon();

    let namespace = NamespaceId::core();
    let repo = fixture.repo_path().to_path_buf();
    let client = fixture.ipc_client().with_autostart(false);

    let report = run_load(repo.clone(), 6, &namespace, client.clone());
    assert_eq!(report.failures, 0, "load failures: {:?}", report.errors);

    let mut collector = StatusCollector::with_client(repo, client);
    let status = collector.sample().expect("admin status").clone();

    assert_segments_match_files(&status);

    let applied_seq = status
        .watermarks_applied
        .get(&namespace, &status.replica_id)
        .map(|mark| mark.seq().get())
        .unwrap_or(0);
    assert!(
        applied_seq >= report.successes as u64,
        "applied seq {applied_seq} should cover {successes} events",
        successes = report.successes
    );
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
    config.autostart = false;
    generator.run()
}

fn assert_segments_match_files(status: &AdminStatusOutput) {
    for wal in &status.wal {
        assert_eq!(wal.segment_count, wal.segments.len());
        let mut total_bytes = 0u64;
        for segment in &wal.segments {
            let bytes = segment.bytes.expect("segment bytes");
            let meta = fs::metadata(&segment.path).expect("segment file");
            assert_eq!(
                bytes,
                meta.len(),
                "segment {id:?} length mismatch",
                id = segment.segment_id
            );
            total_bytes = total_bytes.saturating_add(bytes);
        }
        assert_eq!(wal.total_bytes, total_bytes);
    }
}
