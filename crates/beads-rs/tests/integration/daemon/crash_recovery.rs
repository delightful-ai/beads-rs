#![cfg(feature = "slow-tests")]

use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crate::fixtures::bd_runtime::{
    store_dir_from_data_dir, store_meta_from_data_dir, wait_for_daemon_pid, wait_for_store_id,
};
use crate::fixtures::ipc_client::runtime_bound_client_no_autostart;
use crate::fixtures::realtime::RealtimeFixture;
use crate::fixtures::store_lock::unlock_store;
use crate::fixtures::wait;
use beads_api::QueryResult;
use beads_core::{BeadId, BeadType, EventId, NamespaceId, Priority, Seq1};
use beads_daemon::testkit::wal::{IndexDurabilityMode, SqliteWalIndex, WalIndex};
use beads_surface::ipc::{
    CreatePayload, MutationCtx, MutationMeta, Request, Response, ResponsePayload,
};

fn marker_path(dir: &Path, stage: &str) -> PathBuf {
    dir.join(format!("beads-wal-hang-{stage}"))
}

fn kill_daemon(pid: u32) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
    let _ = wait::wait_for_process_exit(pid, Duration::from_secs(2));
}

fn latest_wal_segment(store_dir: &Path, namespace: &NamespaceId) -> PathBuf {
    let wal_dir = store_dir.join("wal").join(namespace.as_str());
    let mut segments: Vec<PathBuf> = fs::read_dir(&wal_dir)
        .expect("read wal dir")
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .collect();
    segments.sort();
    segments.pop().expect("expected wal segment")
}

fn segment_header_len(path: &Path) -> u64 {
    let mut file = fs::File::open(path).expect("open wal segment");
    let mut prefix = [0u8; 13];
    file.read_exact(&mut prefix)
        .expect("read wal header prefix");
    u32::from_le_bytes([prefix[9], prefix[10], prefix[11], prefix[12]]) as u64
}

fn ensure_partial_tail(path: &Path) {
    let header_len = segment_header_len(path);
    let len = fs::metadata(path).expect("segment metadata").len();
    if len <= header_len {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .expect("open wal segment for append");
        let _ = file.write_all(&[0u8; 5]);
        return;
    }
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .expect("open wal segment for truncate");
    file.set_len(len - 1).expect("truncate wal segment");
}

fn create_request(repo: &Path, id: &str, title: &str) -> Request {
    Request::Create {
        ctx: MutationCtx::new(repo.to_path_buf(), MutationMeta::default()),
        payload: CreatePayload {
            id: Some(BeadId::parse(id).expect("valid bead id")),
            parent: None,
            title: title.to_string(),
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
    }
}

fn start_daemon_with_hang(fixture: &RealtimeFixture, stage: &str, hang_dir: &Path) {
    fixture
        .bd()
        .env("BD_TEST_WAL_HANG_STAGE", stage)
        .env("BD_TEST_WAL_HANG_DIR", hang_dir)
        .env("BD_TEST_WAL_HANG_TIMEOUT_MS", "30000")
        .arg("init")
        .assert()
        .success();
}

#[test]
fn crash_recovery_truncates_tail_and_resets_origin_seq() {
    let fixture = RealtimeFixture::new();
    let hang_dir = fixture.runtime_dir().join("hang");
    fs::create_dir_all(&hang_dir).expect("create hang dir");
    start_daemon_with_hang(&fixture, "wal_after_write", &hang_dir);

    let request = create_request(fixture.repo_path(), "bd-crash-tail", "crash tail");
    let client = runtime_bound_client_no_autostart(fixture.runtime_dir());
    let handle = thread::spawn(move || client.send_request(&request));

    let marker = marker_path(&hang_dir, "wal_after_write");
    assert!(
        wait::wait_for_path(&marker, Duration::from_secs(5)),
        "wal hang marker missing"
    );
    let pid = wait_for_daemon_pid(fixture.runtime_dir(), Duration::from_secs(2))
        .expect("daemon meta missing");
    kill_daemon(pid);
    let _ = handle.join();

    let store_id = wait_for_store_id(fixture.data_dir(), Duration::from_secs(2))
        .expect("store id should be discovered");
    let store_dir = store_dir_from_data_dir(fixture.data_dir()).expect("expected store dir");
    let wal_segment = latest_wal_segment(&store_dir, &NamespaceId::core());
    ensure_partial_tail(&wal_segment);

    unlock_store(fixture.data_dir(), store_id).expect("unlock store");

    let len_before = fs::metadata(&wal_segment).expect("segment metadata").len();
    let output = fixture
        .bd()
        .args(["status", "--json"])
        .output()
        .expect("status output");
    assert!(output.status.success(), "status failed: {:?}", output);
    let _payload: ResponsePayload =
        serde_json::from_slice(&output.stdout).expect("parse status payload");
    let len_after = fs::metadata(&wal_segment)
        .expect("segment metadata after restart")
        .len();
    assert!(
        len_after < len_before,
        "expected tail truncation to shrink segment"
    );

    let client = runtime_bound_client_no_autostart(fixture.runtime_dir());
    let create_resp = client
        .send_request(&create_request(
            fixture.repo_path(),
            "bd-after-crash",
            "post crash",
        ))
        .expect("create response");
    let origin_seq = match create_resp {
        Response::Ok {
            ok: ResponsePayload::Op(op),
        } => op
            .receipt
            .event_ids()
            .first()
            .expect("event id")
            .origin_seq
            .get(),
        other => panic!("unexpected create response: {other:?}"),
    };
    assert_eq!(origin_seq, 1, "origin_seq should reset after truncation");
}

#[test]
fn crash_recovery_rebuilds_index_after_fsync_before_commit() {
    let fixture = RealtimeFixture::new();
    let hang_dir = fixture.runtime_dir().join("hang");
    fs::create_dir_all(&hang_dir).expect("create hang dir");
    start_daemon_with_hang(&fixture, "wal_mutation_before_atomic_commit", &hang_dir);

    let request = create_request(fixture.repo_path(), "bd-crash-index", "crash index");
    let client = runtime_bound_client_no_autostart(fixture.runtime_dir());
    let handle = thread::spawn(move || client.send_request(&request));

    let marker = marker_path(&hang_dir, "wal_mutation_before_atomic_commit");
    assert!(
        wait::wait_for_path(&marker, Duration::from_secs(5)),
        "wal hang marker missing"
    );
    let pid = wait_for_daemon_pid(fixture.runtime_dir(), Duration::from_secs(2))
        .expect("daemon meta missing");
    kill_daemon(pid);
    let _ = handle.join();

    let store_id = wait_for_store_id(fixture.data_dir(), Duration::from_secs(2))
        .expect("store id should be discovered");
    let store_dir = store_dir_from_data_dir(fixture.data_dir()).expect("expected store dir");
    let store_meta = store_meta_from_data_dir(fixture.data_dir()).expect("store meta");
    let namespace = NamespaceId::core();
    let origin = store_meta.replica_id;
    let event_id = EventId::new(
        origin,
        namespace.clone(),
        Seq1::from_u64(1).expect("nonzero seq"),
    );

    let index = SqliteWalIndex::open(&store_dir, &store_meta, IndexDurabilityMode::Cache)
        .expect("open wal index before restart");
    let reader = index.reader();
    assert_eq!(
        reader
            .lookup_event_sha(&namespace, &event_id)
            .expect("lookup event sha before restart"),
        None,
        "event row must be absent before atomic commit",
    );
    let watermark_before = reader
        .load_watermarks()
        .expect("load watermarks before restart")
        .into_iter()
        .find(|row| row.namespace == namespace && row.origin == origin);
    if let Some(row) = watermark_before {
        assert_eq!(row.applied_seq(), 0, "baseline applied seq");
        assert_eq!(row.durable_seq(), 0, "baseline durable seq");
        assert_eq!(row.applied_head_sha(), None, "baseline applied head");
        assert_eq!(row.durable_head_sha(), None, "baseline durable head");
    }

    unlock_store(fixture.data_dir(), store_id).expect("unlock store");

    let output = fixture
        .bd()
        .args(["show", "bd-crash-index", "--json"])
        .output()
        .expect("show output");
    assert!(output.status.success(), "show failed: {:?}", output);
    let payload: ResponsePayload =
        serde_json::from_slice(&output.stdout).expect("parse show payload");
    match payload {
        ResponsePayload::Query(QueryResult::Issue(issue)) => {
            assert_eq!(issue.id, "bd-crash-index");
        }
        other => panic!("unexpected show payload: {other:?}"),
    }

    let index = SqliteWalIndex::open(&store_dir, &store_meta, IndexDurabilityMode::Cache)
        .expect("open wal index after recovery");
    let reader = index.reader();
    let event_sha = reader
        .lookup_event_sha(&namespace, &event_id)
        .expect("lookup event sha after restart")
        .expect("event row should exist after recovery");
    let watermark = reader
        .load_watermarks()
        .expect("load watermarks after restart")
        .into_iter()
        .find(|row| row.namespace == namespace && row.origin == origin)
        .expect("watermark row should exist after recovery");
    assert_eq!(watermark.applied_seq(), 1);
    assert_eq!(watermark.durable_seq(), 1);
    assert_eq!(watermark.applied_head_sha(), Some(event_sha));
    assert_eq!(watermark.durable_head_sha(), Some(event_sha));
}
