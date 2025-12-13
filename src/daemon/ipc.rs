//! IPC protocol types and codec.
//!
//! Protocol: newline-delimited JSON (ndjson) over Unix socket.
//!
//! Request format: `{"op": "create", ...}\n`
//! Response format: `{"ok": ...}\n` or `{"err": {"code": "...", "message": "..."}}\n`

use std::io::{BufRead, BufReader, Write};
use std::fs;
use std::os::unix::net::UnixStream;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use std::fs::OpenOptions;
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::ops::{BeadOp, BeadPatch, OpError, OpResult};
use super::query::{Filters, Query, QueryResult};
use crate::core::{BeadId, BeadType, DepKind, Priority, CoreError, InvalidId};
use crate::error::{Effect, Transience};

// =============================================================================
// Request - All IPC requests
// =============================================================================

/// IPC request (mutation or query).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    // === Mutations ===
    /// Create a new bead.
    Create {
        repo: PathBuf,
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        parent: Option<String>,
        title: String,
        #[serde(rename = "type")]
        bead_type: BeadType,
        priority: Priority,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        design: Option<String>,
        #[serde(default)]
        acceptance_criteria: Option<String>,
        #[serde(default)]
        assignee: Option<String>,
        #[serde(default)]
        external_ref: Option<String>,
        #[serde(default)]
        estimated_minutes: Option<u32>,
        #[serde(default)]
        labels: Vec<String>,
        #[serde(default)]
        dependencies: Vec<String>,
    },

    /// Update an existing bead.
    Update {
        repo: PathBuf,
        id: String,
        patch: BeadPatch,
        #[serde(default)]
        cas: Option<String>,
    },

    /// Close a bead.
    Close {
        repo: PathBuf,
        id: String,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        on_branch: Option<String>,
    },

    /// Reopen a closed bead.
    Reopen { repo: PathBuf, id: String },

    /// Delete a bead (soft delete).
    Delete {
        repo: PathBuf,
        id: String,
        #[serde(default)]
        reason: Option<String>,
    },

    /// Add a dependency.
    AddDep {
        repo: PathBuf,
        from: String,
        to: String,
        kind: DepKind,
    },

    /// Remove a dependency.
    RemoveDep {
        repo: PathBuf,
        from: String,
        to: String,
        kind: DepKind,
    },

    /// Add a note.
    AddNote {
        repo: PathBuf,
        id: String,
        content: String,
    },

    /// Claim a bead.
    Claim {
        repo: PathBuf,
        id: String,
        #[serde(default = "default_lease_secs")]
        lease_secs: u64,
    },

    /// Release a claim.
    Unclaim { repo: PathBuf, id: String },

    /// Extend a claim.
    ExtendClaim {
        repo: PathBuf,
        id: String,
        lease_secs: u64,
    },

    // === Queries ===
    /// Get a single bead.
    Show { repo: PathBuf, id: String },

    /// List beads.
    List {
        repo: PathBuf,
        #[serde(default)]
        filters: Filters,
    },

    /// Get ready beads.
    Ready {
        repo: PathBuf,
        #[serde(default)]
        limit: Option<usize>,
    },

    /// Get dependency tree.
    DepTree { repo: PathBuf, id: String },

    /// Get dependencies.
    Deps { repo: PathBuf, id: String },

    /// Get notes.
    Notes { repo: PathBuf, id: String },

    /// Get blocked issues.
    Blocked { repo: PathBuf },

    /// Get stale issues.
    Stale {
        repo: PathBuf,
        #[serde(default)]
        days: u32,
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        limit: Option<usize>,
    },

    /// Count issues matching filters.
    Count {
        repo: PathBuf,
        #[serde(default)]
        filters: Filters,
        #[serde(default)]
        group_by: Option<String>,
    },

    /// Show deleted (tombstoned) issues.
    Deleted {
        repo: PathBuf,
        #[serde(default)]
        since_ms: Option<u64>,
        #[serde(default)]
        id: Option<String>,
    },

    /// Epic completion status.
    EpicStatus {
        repo: PathBuf,
        #[serde(default)]
        eligible_only: bool,
    },

    // === Control ===
    /// Force sync now.
    Sync { repo: PathBuf },

    /// Wait until repo is clean (debounced sync flushed).
    SyncWait { repo: PathBuf },

    /// Initialize beads ref.
    Init { repo: PathBuf },

    /// Get sync status.
    Status { repo: PathBuf },

    /// Validate state.
    Validate { repo: PathBuf },

    /// Ping (health check).
    Ping,

    /// Shutdown daemon.
    Shutdown,
}

fn default_lease_secs() -> u64 {
    3600 // 1 hour default
}

impl Request {
    /// Convert to BeadOp if this is a mutation request.
    pub fn to_op(&self) -> Result<Option<BeadOp>, IpcError> {
        match self {
            Request::Create {
                repo,
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
            } => Ok(Some(BeadOp::Create {
                repo: repo.clone(),
                id: id.clone(),
                parent: parent.clone(),
                title: title.clone(),
                bead_type: *bead_type,
                priority: *priority,
                description: description.clone(),
                design: design.clone(),
                acceptance_criteria: acceptance_criteria.clone(),
                assignee: assignee.clone(),
                external_ref: external_ref.clone(),
                estimated_minutes: *estimated_minutes,
                labels: labels.clone(),
                dependencies: dependencies.clone(),
            })),

            Request::Update {
                repo,
                id,
                patch,
                cas,
            } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::Update {
                    repo: repo.clone(),
                    id,
                    patch: patch.clone(),
                    cas: cas.clone(),
                }))
            }

            Request::Close {
                repo,
                id,
                reason,
                on_branch,
            } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::Close {
                    repo: repo.clone(),
                    id,
                    reason: reason.clone(),
                    on_branch: on_branch.clone(),
                }))
            }

            Request::Reopen { repo, id } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::Reopen {
                    repo: repo.clone(),
                    id,
                }))
            }

            Request::Delete { repo, id, reason } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::Delete {
                    repo: repo.clone(),
                    id,
                    reason: reason.clone(),
                }))
            }

            Request::AddDep {
                repo,
                from,
                to,
                kind,
            } => {
                let from = BeadId::parse(from).map_err(map_core_invalid_id)?;
                let to = BeadId::parse(to).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::AddDep {
                    repo: repo.clone(),
                    from,
                    to,
                    kind: *kind,
                }))
            }

            Request::RemoveDep {
                repo,
                from,
                to,
                kind,
            } => {
                let from = BeadId::parse(from).map_err(map_core_invalid_id)?;
                let to = BeadId::parse(to).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::RemoveDep {
                    repo: repo.clone(),
                    from,
                    to,
                    kind: *kind,
                }))
            }

            Request::AddNote { repo, id, content } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::AddNote {
                    repo: repo.clone(),
                    id,
                    content: content.clone(),
                }))
            }

            Request::Claim {
                repo,
                id,
                lease_secs,
            } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::Claim {
                    repo: repo.clone(),
                    id,
                    lease_secs: *lease_secs,
                }))
            }

            Request::Unclaim { repo, id } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::Unclaim {
                    repo: repo.clone(),
                    id,
                }))
            }

            Request::ExtendClaim {
                repo,
                id,
                lease_secs,
            } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(BeadOp::ExtendClaim {
                    repo: repo.clone(),
                    id,
                    lease_secs: *lease_secs,
                }))
            }

            // Not mutations
            _ => Ok(None),
        }
    }

    /// Convert to Query if this is a query request.
    pub fn to_query(&self) -> Result<Option<Query>, IpcError> {
        match self {
            Request::Show { repo, id } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(Query::Show {
                    repo: repo.clone(),
                    id,
                }))
            }

            Request::List { repo, filters } => Ok(Some(Query::List {
                repo: repo.clone(),
                filters: filters.clone(),
            })),

            Request::Ready { repo, limit } => Ok(Some(Query::Ready {
                repo: repo.clone(),
                limit: *limit,
            })),

            Request::DepTree { repo, id } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(Query::DepTree {
                    repo: repo.clone(),
                    id,
                }))
            }

            Request::Deps { repo, id } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(Query::Deps {
                    repo: repo.clone(),
                    id,
                }))
            }

            Request::Notes { repo, id } => {
                let id = BeadId::parse(id).map_err(map_core_invalid_id)?;
                Ok(Some(Query::Notes {
                    repo: repo.clone(),
                    id,
                }))
            }

            Request::Status { repo } => Ok(Some(Query::Status { repo: repo.clone() })),

            Request::Blocked { repo } => Ok(Some(Query::Blocked { repo: repo.clone() })),

            Request::Stale {
                repo,
                days,
                status,
                limit,
            } => Ok(Some(Query::Stale {
                repo: repo.clone(),
                days: *days,
                status: status.clone(),
                limit: *limit,
            })),

            Request::Count {
                repo,
                filters,
                group_by,
            } => Ok(Some(Query::Count {
                repo: repo.clone(),
                filter: filters.clone(),
                group_by: group_by.clone(),
            })),

            Request::Deleted {
                repo,
                since_ms,
                id,
            } => Ok(Some(Query::Deleted {
                repo: repo.clone(),
                since_ms: *since_ms,
                id: id
                    .as_ref()
                    .map(|s| BeadId::parse(s).map_err(map_core_invalid_id))
                    .transpose()?,
            })),

            Request::EpicStatus {
                repo,
                eligible_only,
            } => Ok(Some(Query::EpicStatus {
                repo: repo.clone(),
                eligible_only: *eligible_only,
            })),

            Request::Validate { repo } => Ok(Some(Query::Validate { repo: repo.clone() })),

            // Not queries
            _ => Ok(None),
        }
    }
}

// =============================================================================
// Response - IPC responses
// =============================================================================

/// IPC response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Response {
    Ok { ok: ResponsePayload },
    Err { err: ErrorPayload },
}

impl Response {
    /// Create a success response.
    pub fn ok(payload: ResponsePayload) -> Self {
        Response::Ok { ok: payload }
    }

    /// Create an error response.
    pub fn err(error: impl Into<ErrorPayload>) -> Self {
        Response::Err { err: error.into() }
    }
}

/// Successful response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResponsePayload {
    /// Mutation result.
    Op(OpResult),

    /// Query result.
    Query(QueryResult),

    /// Sync completed.
    Synced,

    /// Init completed.
    Initialized,

    /// Pong.
    Pong,

    /// Shutdown ack.
    ShuttingDown,
}

/// Error response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorPayload {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl From<OpError> for ErrorPayload {
    fn from(e: OpError) -> Self {
        let details = serde_json::json!({
            "retryable": e.transience().is_retryable(),
            "effect": e.effect().as_str(),
        });
        ErrorPayload {
            code: e.code().to_string(),
            message: e.to_string(),
            details: Some(details),
        }
    }
}

impl From<IpcError> for ErrorPayload {
    fn from(e: IpcError) -> Self {
        let details = serde_json::json!({
            "retryable": e.transience().is_retryable(),
            "effect": e.effect().as_str(),
        });
        ErrorPayload {
            code: e.code().to_string(),
            message: e.to_string(),
            details: Some(details),
        }
    }
}

// =============================================================================
// IpcError
// =============================================================================

/// IPC-specific errors.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum IpcError {
    #[error("parse error: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    InvalidId(#[from] InvalidId),

    #[error("client disconnected")]
    Disconnected,

    #[error("daemon unavailable: {0}")]
    DaemonUnavailable(String),
}

impl IpcError {
    pub fn code(&self) -> &'static str {
        match self {
            IpcError::Parse(_) => "parse_error",
            IpcError::Io(_) => "io_error",
            IpcError::InvalidId(_) => "invalid_id",
            IpcError::Disconnected => "disconnected",
            IpcError::DaemonUnavailable(_) => "daemon_unavailable",
        }
    }

    /// Whether retrying the IPC operation may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            IpcError::DaemonUnavailable(_) | IpcError::Io(_) | IpcError::Disconnected => {
                Transience::Retryable
            }
            IpcError::Parse(_) | IpcError::InvalidId(_) => Transience::Permanent,
        }
    }

    /// What we know about side effects when this IPC error is returned.
    pub fn effect(&self) -> Effect {
        match self {
            IpcError::Io(_) | IpcError::Disconnected => Effect::Unknown,
            IpcError::DaemonUnavailable(_) | IpcError::Parse(_) | IpcError::InvalidId(_) => {
                Effect::None
            }
        }
    }
}

fn map_core_invalid_id(e: CoreError) -> IpcError {
    match e {
        CoreError::InvalidId(invalid) => IpcError::InvalidId(invalid),
        other => IpcError::DaemonUnavailable(other.to_string()),
    }
}

// =============================================================================
// Codec - Encoding/decoding
// =============================================================================

/// Encode a response to bytes.
pub fn encode_response(resp: &Response) -> Result<Vec<u8>, IpcError> {
    let mut bytes = serde_json::to_vec(resp)?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Decode a request from a line.
pub fn decode_request(line: &str) -> Result<Request, IpcError> {
    Ok(serde_json::from_str(line)?)
}

/// Send a response over a stream.
pub fn send_response(stream: &mut UnixStream, resp: &Response) -> Result<(), IpcError> {
    let bytes = encode_response(resp)?;
    stream.write_all(&bytes)?;
    Ok(())
}

/// Read requests from a stream.
pub fn read_requests(stream: UnixStream) -> impl Iterator<Item = Result<Request, IpcError>> {
    let reader = BufReader::new(stream);
    reader.lines().map(|line| {
        let line = line?;
        decode_request(&line)
    })
}

// =============================================================================
// Socket path
// =============================================================================

/// Get the directory that will contain the daemon socket.
pub fn socket_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        PathBuf::from(dir).join("beads")
    } else if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join(".beads")
    } else {
        per_user_tmp_dir()
    }
}

/// Ensure the socket directory exists and is user-private.
pub fn ensure_socket_dir() -> Result<PathBuf, IpcError> {
    let dir = socket_dir();
    fs::create_dir_all(&dir)?;

    #[cfg(unix)]
    {
        let mode = fs::metadata(&dir)?.permissions().mode() & 0o777;
        if mode != 0o700 {
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        }
    }

    Ok(dir)
}

/// Get the daemon socket path.
pub fn socket_path() -> PathBuf {
    socket_dir().join("daemon.sock")
}

fn per_user_tmp_dir() -> PathBuf {
    let uid = unsafe { libc::geteuid() };
    PathBuf::from("/tmp").join(format!("beads-{}", uid))
}

// =============================================================================
// Client - Send requests to daemon
// =============================================================================

fn should_autostart(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
    )
}

fn maybe_remove_stale_lock(lock_path: &PathBuf) {
    if let Ok(meta) = fs::metadata(lock_path) {
        if let Ok(modified) = meta.modified() {
            if let Ok(age) = modified.elapsed() {
                if age > Duration::from_secs(10) {
                    let _ = fs::remove_file(lock_path);
                }
            }
        }
    }
}

fn daemon_command() -> Command {
    if let Ok(exe) = std::env::current_exe() {
        let mut cmd = Command::new(exe);
        cmd.arg("daemon").arg("run");
        return cmd;
    }

    let mut cmd = Command::new("bd");
    cmd.arg("daemon").arg("run");
    cmd
}

fn connect_with_autostart(socket: &PathBuf) -> Result<UnixStream, IpcError> {
    match UnixStream::connect(socket) {
        Ok(stream) => return Ok(stream),
        Err(e) if should_autostart(&e) => {
            // Try to autostart daemon with a simple lock to avoid herds.
            let dir = ensure_socket_dir()?;
            let lock_path = dir.join("daemon.lock");
            maybe_remove_stale_lock(&lock_path);

            let mut we_spawned = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .is_ok();

            if we_spawned {
                let mut cmd = daemon_command();
                cmd.stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                cmd.spawn().map_err(|e| {
                    IpcError::DaemonUnavailable(format!("failed to spawn daemon: {}", e))
                })?;
            }

            let deadline = SystemTime::now() + Duration::from_secs(30);
            let mut backoff = Duration::from_millis(50);

            loop {
                match UnixStream::connect(socket) {
                    Ok(stream) => {
                        if we_spawned {
                            let _ = fs::remove_file(&lock_path);
                        }
                        return Ok(stream);
                    }
                    Err(e) if should_autostart(&e) => {
                        if !we_spawned {
                            // If the lock disappeared (spawner died), try to take over.
                            maybe_remove_stale_lock(&lock_path);
                            if OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(&lock_path)
                                .is_ok()
                            {
                                we_spawned = true;
                                let mut cmd = daemon_command();
                                cmd.stdin(Stdio::null())
                                    .stdout(Stdio::null())
                                    .stderr(Stdio::null());
                                if let Err(e) = cmd.spawn() {
                                    let _ = fs::remove_file(&lock_path);
                                    return Err(IpcError::DaemonUnavailable(format!(
                                        "failed to spawn daemon: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        if SystemTime::now() >= deadline {
                            if we_spawned {
                                let _ = fs::remove_file(&lock_path);
                            }
                            return Err(IpcError::DaemonUnavailable(
                                "timed out waiting for daemon socket".into(),
                            ));
                        }
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, Duration::from_millis(200));
                    }
                    Err(e) => {
                        if we_spawned {
                            let _ = fs::remove_file(&lock_path);
                        }
                        return Err(IpcError::Io(e));
                    }
                }
            }
        }
        Err(e) => Err(IpcError::Io(e)),
    }
}

/// Send a request to the daemon and receive a response.
pub fn send_request(req: &Request) -> Result<Response, IpcError> {
    let socket = socket_path();
    let mut stream = connect_with_autostart(&socket)?;
    let mut json = serde_json::to_string(req)?;
    json.push('\n');
    stream.write_all(json.as_bytes())?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    Ok(serde_json::from_str(&line)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let req = Request::Create {
            repo: PathBuf::from("/test"),
            id: None,
            parent: None,
            title: "test".to_string(),
            bead_type: BeadType::Task,
            priority: Priority::default(),
            description: None,
            design: None,
            acceptance_criteria: None,
            assignee: None,
            external_ref: None,
            estimated_minutes: None,
            labels: Vec::new(),
            dependencies: Vec::new(),
        };

        let json = serde_json::to_string(&req).unwrap();
        let parsed: Request = serde_json::from_str(&json).unwrap();

        match parsed {
            Request::Create { title, .. } => assert_eq!(title, "test"),
            _ => panic!("wrong request type"),
        }
    }

    #[test]
    fn response_ok() {
        let resp = Response::ok(ResponsePayload::Synced);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\""));
    }

    #[test]
    fn response_err() {
        let resp = Response::err(ErrorPayload {
            code: "not_found".to_string(),
            message: "bead not found".to_string(),
            details: None,
        });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"err\""));
        assert!(json.contains("not_found"));
    }
}
