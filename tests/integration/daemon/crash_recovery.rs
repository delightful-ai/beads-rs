#![cfg(feature = "slow-tests")]

use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use crate::fixtures::realtime::RealtimeFixture;
use beads_rs::api::QueryResult;
use beads_rs::core::{BeadType, NamespaceId, Priority, StoreId, StoreMeta};
use beads_rs::daemon::ipc::{IpcClient, MutationMeta, Request, Response, ResponsePayload};

fn marker_path(dir: &Path, stage: &str) -> PathBuf {
    dir.join(format!("beads-wal-hang-{stage}"))
}

fn wait_for_marker(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut backoff = Duration::from_millis(5);
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(backoff);
        backoff = std::cmp::min(backoff.saturating_mul(2), Duration::from_millis(50));
    }
    path.exists()
}

fn daemon_pid(runtime_dir: &Path) -> u32 {
    let meta_path = runtime_dir.join("beads").join("daemon.meta.json");
    assert!(
        wait_for_marker(&meta_path, Duration::from_secs(2)),
        "daemon meta missing"
    );
    let contents = fs::read_to_string(&meta_path).expect("read daemon meta");
    let meta: serde_json::Value = serde_json::from_str(&contents).expect("parse daemon meta");
    meta["pid"].as_u64().expect("pid missing") as u32
}

fn kill_daemon(pid: u32) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
    wait_for_exit(pid, Duration::from_secs(2));
}

fn wait_for_exit(pid: u32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !process_alive(pid) {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn process_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

fn store_dir_from_data_dir(data_dir: &Path) -> PathBuf {
    let stores_dir = data_dir.join("stores");
    let mut entries: Vec<PathBuf> = fs::read_dir(&stores_dir)
        .expect("read stores dir")
        .flatten()
        .map(|entry| entry.path())
        .collect();
    entries.sort();
    assert_eq!(entries.len(), 1, "expected exactly one store dir");
    entries.remove(0)
}

fn store_meta_from_data_dir(data_dir: &Path) -> StoreMeta {
    let store_dir = store_dir_from_data_dir(data_dir);
    let meta_path = store_dir.join("meta.json");
    let contents = fs::read_to_string(&meta_path).expect("read store meta");
    serde_json::from_str(&contents).expect("parse store meta")
}

fn store_id_from_data_dir(data_dir: &Path) -> StoreId {
    store_meta_from_data_dir(data_dir).store_id()
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
        repo: repo.to_path_buf(),
        id: Some(id.to_string()),
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
        meta: MutationMeta::default(),
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
    let client = IpcClient::for_runtime_dir(fixture.runtime_dir()).with_autostart(false);
    let handle = thread::spawn(move || client.send_request(&request));

    let marker = marker_path(&hang_dir, "wal_after_write");
    assert!(
        wait_for_marker(&marker, Duration::from_secs(5)),
        "wal hang marker missing"
    );
    let pid = daemon_pid(fixture.runtime_dir());
    kill_daemon(pid);
    let _ = handle.join();

    let store_id = store_id_from_data_dir(fixture.data_dir());
    let store_dir = store_dir_from_data_dir(fixture.data_dir());
    let wal_segment = latest_wal_segment(&store_dir, &NamespaceId::core());
    ensure_partial_tail(&wal_segment);

    fixture
        .bd()
        .args([
            "store",
            "unlock",
            "--store-id",
            store_id.to_string().as_str(),
        ])
        .assert()
        .success();

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

    let client = IpcClient::for_runtime_dir(fixture.runtime_dir()).with_autostart(false);
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
            .event_ids
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
    start_daemon_with_hang(&fixture, "wal_before_index_commit", &hang_dir);

    let request = create_request(fixture.repo_path(), "bd-crash-index", "crash index");
    let client = IpcClient::for_runtime_dir(fixture.runtime_dir()).with_autostart(false);
    let handle = thread::spawn(move || client.send_request(&request));

    let marker = marker_path(&hang_dir, "wal_before_index_commit");
    assert!(
        wait_for_marker(&marker, Duration::from_secs(5)),
        "wal hang marker missing"
    );
    let pid = daemon_pid(fixture.runtime_dir());
    kill_daemon(pid);
    let _ = handle.join();

    let store_id = store_id_from_data_dir(fixture.data_dir());
    fixture
        .bd()
        .args([
            "store",
            "unlock",
            "--store-id",
            store_id.to_string().as_str(),
        ])
        .assert()
        .success();

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
}
