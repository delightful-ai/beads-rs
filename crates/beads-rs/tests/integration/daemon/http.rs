//! HTTP transport assembly coverage for tracker-facing board flows.
//!
//! These tests keep the real daemon process boundary and exercise the shipped
//! HTTP router over TCP so tracker reads/writes stay honest at the product seam.

use std::net::TcpListener;
use std::thread;
use std::time::Duration;

use crate::fixtures::bd_runtime::{BdCommandProfile, BdRuntimeRepo};
use crate::fixtures::wait;
use serde_json::{Value, json};

struct HttpTrackerFixture {
    runtime: BdRuntimeRepo,
    base_url: String,
}

impl HttpTrackerFixture {
    fn new() -> Self {
        let runtime = BdRuntimeRepo::new_with_origin();
        runtime.start_daemon(BdCommandProfile::fast_daemon());
        let base_url = spawn_http_server(runtime.ipc_client_no_autostart());
        Self { runtime, base_url }
    }

    fn repo_path(&self) -> &std::path::Path {
        self.runtime.path()
    }

    fn rpc(&self, request: Value) -> Value {
        let response = ureq::post(&format!("{}/rpc", self.base_url))
            .set("content-type", "application/json")
            .send_string(&request.to_string())
            .expect("http rpc response");
        serde_json::from_reader(response.into_reader()).expect("decode http rpc response")
    }

    fn create_issue(&self, title: &str, description: &str, priority: u8) -> String {
        let response = self.rpc(json!({
            "op": "create",
            "repo": self.repo_path(),
            "title": title,
            "description": description,
            "type": "task",
            "priority": priority,
        }));
        assert_eq!(response["ok"]["result"], "created");
        response["ok"]["id"]
            .as_str()
            .expect("created issue id")
            .to_string()
    }

    fn add_blocks_dep(&self, blocked: &str, blocker: &str) {
        let response = self.rpc(json!({
            "op": "add_dep",
            "repo": self.repo_path(),
            "from": blocked,
            "to": blocker,
            "kind": "blocks",
        }));
        assert_eq!(response["ok"]["result"], "dep_added");
    }

    fn tracker_transition(&self, id: &str, status: &str) {
        let response = self.rpc(json!({
            "op": "tracker_transition",
            "repo": self.repo_path(),
            "id": id,
            "status": status,
        }));
        assert_eq!(response["ok"]["result"], "updated");
    }

    fn tracker_comment(&self, id: &str, content: &str) {
        let response = self.rpc(json!({
            "op": "tracker_comment",
            "repo": self.repo_path(),
            "id": id,
            "content": content,
        }));
        assert_eq!(response["ok"]["result"], "note_added");
    }

    fn tracker_list(&self) -> Value {
        self.rpc(json!({
            "op": "tracker_list",
            "repo": self.repo_path(),
        }))
    }
}

fn spawn_http_server(client: beads_surface::IpcClient) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind http listener");
    let addr = listener.local_addr().expect("listener addr");
    listener
        .set_nonblocking(true)
        .expect("set nonblocking listener");

    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("http runtime");
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            beads_http::serve(listener, client)
                .await
                .expect("serve http transport");
        });
    });

    let base_url = format!("http://{addr}");
    let healthy = wait::poll_until(Duration::from_secs(5), || {
        let Ok(response) = ureq::get(&format!("{base_url}/healthz")).call() else {
            return false;
        };
        response.status() == 200
    });
    assert!(
        healthy,
        "http transport failed to become healthy at {base_url}"
    );
    base_url
}

#[test]
fn tracker_http_flow_projects_blockers_and_supports_tracker_mutations() {
    let fixture = HttpTrackerFixture::new();

    let todo_id = fixture.create_issue("Board demo issue", "seed tracker issue", 1);
    let blocker_id = fixture.create_issue("Backend sync lane", "Long-running backend work", 1);
    let review_id = fixture.create_issue("API review pass", "Waiting for review", 2);
    let blocked_id = fixture.create_issue("Blocked UI polish", "Depends on backend sync", 2);

    fixture.add_blocks_dep(&blocked_id, &blocker_id);
    fixture.tracker_transition(&blocker_id, "In Progress");
    fixture.tracker_transition(&review_id, "Human Review");
    fixture.tracker_comment(&review_id, "## Codex Workpad\n- review pending");
    fixture.tracker_transition(&todo_id, "In Progress");

    let response = fixture.tracker_list();
    let issues = response["ok"]["data"]
        .as_array()
        .expect("tracker issue array");

    let blocked_issue = issues
        .iter()
        .find(|issue| issue["id"] == blocked_id)
        .expect("blocked issue");
    assert_eq!(blocked_issue["status"], "Todo");
    assert_eq!(blocked_issue["blocked_by"][0]["id"], blocker_id);
    assert_eq!(blocked_issue["blocked_by"][0]["status"], "In Progress");

    let review_issue = issues
        .iter()
        .find(|issue| issue["id"] == review_id)
        .expect("review issue");
    assert_eq!(review_issue["status"], "Human Review");
    assert_eq!(review_issue["labels"], json!([]));

    let transitioned_issue = issues
        .iter()
        .find(|issue| issue["id"] == todo_id)
        .expect("transitioned issue");
    assert_eq!(transitioned_issue["status"], "In Progress");
}
