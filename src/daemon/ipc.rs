//! IPC protocol types and codec.
//!
//! Protocol: newline-delimited JSON (ndjson) over Unix socket.
//!
//! Request format: `{"op": "create", ...}\n`
//! Response format: `{"ok": ...}\n` or `{"err": {"code": "...", "message": "..."}}\n`

use std::fs;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::ops::{BeadOp, BeadPatch, OpError, OpResult};
use super::query::{Filters, Query, QueryResult};
use crate::core::{BeadId, BeadType, CoreError, DepKind, InvalidId, Priority};
use crate::error::{Effect, Transience};

pub const IPC_PROTOCOL_VERSION: u32 = 1;

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
    /// Force reload state from git (invalidates cache).
    /// Use after external changes to refs/heads/beads/store (e.g., migration).
    Refresh { repo: PathBuf },

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

            Request::Deleted { repo, since_ms, id } => Ok(Some(Query::Deleted {
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
#[allow(clippy::large_enum_variant)]
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
///
/// Uses untagged serialization for backward compatibility. Unit-like variants
/// use wrapper structs with a `result` field to avoid serializing as `null`,
/// which would be ambiguous during deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum ResponsePayload {
    /// Mutation result.
    Op(OpResult),

    /// Query result.
    Query(QueryResult),

    /// Sync completed.
    Synced(SyncedPayload),

    /// Refresh completed (state reloaded from git).
    Refreshed(RefreshedPayload),

    /// Init completed.
    Initialized(InitializedPayload),

    /// Shutdown ack.
    ShuttingDown(ShuttingDownPayload),
}

impl ResponsePayload {
    /// Create a synced payload.
    pub fn synced() -> Self {
        ResponsePayload::Synced(SyncedPayload::default())
    }

    /// Create an initialized payload.
    pub fn initialized() -> Self {
        ResponsePayload::Initialized(InitializedPayload::default())
    }

    /// Create a refreshed payload.
    pub fn refreshed() -> Self {
        ResponsePayload::Refreshed(RefreshedPayload::default())
    }

    /// Create a shutting down payload.
    pub fn shutting_down() -> Self {
        ResponsePayload::ShuttingDown(ShuttingDownPayload::default())
    }
}

/// Payload for sync completion. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SyncedPayload {
    result: SyncedTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum SyncedTag {
    #[default]
    #[serde(rename = "synced")]
    Synced,
}

/// Payload for refresh completion. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RefreshedPayload {
    result: RefreshedTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum RefreshedTag {
    #[default]
    #[serde(rename = "refreshed")]
    Refreshed,
}

/// Payload for init completion. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InitializedPayload {
    result: InitializedTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum InitializedTag {
    #[default]
    #[serde(rename = "initialized")]
    Initialized,
}

/// Payload for shutdown acknowledgment. Uses typed discriminant for unambiguous deserialization.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShuttingDownPayload {
    result: ShuttingDownTag,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
enum ShuttingDownTag {
    #[default]
    #[serde(rename = "shutting_down")]
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

    #[error("daemon version mismatch; restart the daemon and retry")]
    DaemonVersionMismatch {
        daemon: Option<crate::api::DaemonInfo>,
        client_version: String,
        protocol_version: u32,
        /// If set, the mismatch was detected via a parse failure.
        parse_error: Option<String>,
    },
}

impl IpcError {
    pub fn code(&self) -> &'static str {
        match self {
            IpcError::Parse(_) => "parse_error",
            IpcError::Io(_) => "io_error",
            IpcError::InvalidId(_) => "invalid_id",
            IpcError::Disconnected => "disconnected",
            IpcError::DaemonUnavailable(_) => "daemon_unavailable",
            IpcError::DaemonVersionMismatch { .. } => "daemon_version_mismatch",
        }
    }

    /// Whether retrying the IPC operation may succeed.
    pub fn transience(&self) -> Transience {
        match self {
            IpcError::DaemonUnavailable(_) | IpcError::Io(_) | IpcError::Disconnected => {
                Transience::Retryable
            }
            IpcError::DaemonVersionMismatch { .. } => Transience::Retryable,
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
            IpcError::DaemonVersionMismatch { .. } => Effect::None,
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
    socket_dir_candidates()
        .into_iter()
        .next()
        .unwrap_or_else(per_user_tmp_dir)
}

/// Ensure the socket directory exists and is user-private.
pub fn ensure_socket_dir() -> Result<PathBuf, IpcError> {
    let mut last_err: Option<std::io::Error> = None;
    for dir in socket_dir_candidates() {
        match fs::create_dir_all(&dir) {
            Ok(()) => {
                #[cfg(unix)]
                {
                    let mode = fs::metadata(&dir)?.permissions().mode() & 0o777;
                    if mode != 0o700 {
                        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
                    }
                }
                return Ok(dir);
            }
            Err(e) => last_err = Some(e),
        }
    }

    Err(IpcError::Io(last_err.unwrap_or_else(|| {
        std::io::Error::other("unable to create a writable socket directory")
    })))
}

/// Get the daemon socket path.
pub fn socket_path() -> PathBuf {
    ensure_socket_dir()
        .map(|dir| dir.join("daemon.sock"))
        .unwrap_or_else(|_| per_user_tmp_dir().join("daemon.sock"))
}

/// Read daemon metadata from the meta file.
/// Returns None if file doesn't exist or is corrupt.
fn read_daemon_meta() -> Option<crate::api::DaemonInfo> {
    let dir = ensure_socket_dir().ok()?;
    let meta_path = dir.join("daemon.meta.json");
    let contents = fs::read_to_string(&meta_path).ok()?;
    serde_json::from_str(&contents).ok()
}

fn per_user_tmp_dir() -> PathBuf {
    let uid = nix::unistd::geteuid();
    PathBuf::from("/tmp").join(format!("beads-{}", uid))
}

fn socket_dir_candidates() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR")
        && !dir.trim().is_empty()
    {
        dirs.push(PathBuf::from(dir).join("beads"));
    }
    if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        dirs.push(PathBuf::from(home).join(".beads"));
    }
    dirs.push(per_user_tmp_dir());
    dirs
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
    if let Ok(meta) = fs::metadata(lock_path)
        && let Ok(modified) = meta.modified()
        && let Ok(age) = modified.elapsed()
        && age > Duration::from_secs(10)
    {
        let _ = fs::remove_file(lock_path);
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
        Ok(stream) => Ok(stream),
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

fn write_req_line(stream: &mut UnixStream, req: &Request) -> Result<(), IpcError> {
    let mut json = serde_json::to_string(req)?;
    json.push('\n');
    stream.write_all(json.as_bytes())?;
    Ok(())
}

fn read_resp_line(reader: &mut BufReader<UnixStream>) -> Result<Response, IpcError> {
    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line)?;
    // EOF means daemon closed connection (likely just shut down)
    if bytes_read == 0 || line.trim().is_empty() {
        return Err(IpcError::DaemonUnavailable(
            "daemon not running (stale socket)".into(),
        ));
    }
    Ok(serde_json::from_str(&line)?)
}

/// Read response line, converting parse errors to version mismatch.
///
/// Used during version verification where a parse failure likely indicates
/// an incompatible daemon version.
fn read_resp_line_version_check(reader: &mut BufReader<UnixStream>) -> Result<Response, IpcError> {
    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line)?;
    if bytes_read == 0 || line.trim().is_empty() {
        return Err(IpcError::DaemonUnavailable(
            "daemon not running (stale socket)".into(),
        ));
    }
    serde_json::from_str(&line).map_err(|e| IpcError::DaemonVersionMismatch {
        daemon: None,
        client_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: IPC_PROTOCOL_VERSION,
        parse_error: Some(e.to_string()),
    })
}

fn verify_daemon_version(
    writer: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
) -> Result<(), IpcError> {
    write_req_line(writer, &Request::Ping)?;
    // Use version-check variant that converts parse errors to version mismatch
    let resp = read_resp_line_version_check(reader)?;
    let Response::Ok { ok } = resp else {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    };

    let ResponsePayload::Query(QueryResult::DaemonInfo(info)) = ok else {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: Some("unexpected response payload type".into()),
        });
    };

    if info.protocol_version != IPC_PROTOCOL_VERSION || info.version != env!("CARGO_PKG_VERSION") {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: Some(info),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    }

    Ok(())
}

fn send_request_over_stream(stream: UnixStream, req: &Request) -> Result<Response, IpcError> {
    let mut writer = stream;
    let reader_stream = writer.try_clone()?;
    let mut reader = BufReader::new(reader_stream);

    // Verify daemon version/protocol once per connection.
    if !matches!(req, Request::Ping) {
        verify_daemon_version(&mut writer, &mut reader)?;
    }

    write_req_line(&mut writer, req)?;
    read_resp_line(&mut reader)
}

/// Send a request to the daemon and receive a response.
///
/// Retries up to 3 times on version mismatch or stale socket errors,
/// with exponential backoff between attempts.
pub fn send_request(req: &Request) -> Result<Response, IpcError> {
    const MAX_ATTEMPTS: u32 = 3;
    let socket = socket_path();

    for attempt in 1..=MAX_ATTEMPTS {
        let stream = match connect_with_autostart(&socket) {
            Ok(s) => s,
            Err(e) => {
                if attempt >= MAX_ATTEMPTS {
                    return Err(e);
                }
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
                continue;
            }
        };

        match send_request_over_stream(stream, req) {
            Ok(resp) => return Ok(resp),
            Err(IpcError::DaemonVersionMismatch { daemon, .. }) if attempt < MAX_ATTEMPTS => {
                tracing::info!(
                    "daemon version mismatch, restarting (attempt {}/{})",
                    attempt,
                    MAX_ATTEMPTS
                );

                // Try to restart the daemon
                if let Some(info) = daemon {
                    let _ = kill_daemon_forcefully(info.pid, &socket);
                } else {
                    let _ = try_restart_daemon_by_socket(&socket);
                }

                // Exponential backoff: 100ms, 200ms, 400ms
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
            }
            Err(IpcError::DaemonUnavailable(ref msg)) if attempt < MAX_ATTEMPTS => {
                tracing::debug!("daemon unavailable ({}), retrying", msg);
                // Socket might have gone stale mid-request
                let _ = try_restart_daemon_by_socket(&socket);
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
            }
            Err(e) => return Err(e),
        }
    }

    Err(IpcError::DaemonUnavailable(
        "max retry attempts exceeded".into(),
    ))
}

/// Send a request without auto-starting the daemon.
///
/// Returns `DaemonUnavailable` if daemon is not running.
pub fn send_request_no_autostart(req: &Request) -> Result<Response, IpcError> {
    let socket = socket_path();
    let stream = UnixStream::connect(&socket)
        .map_err(|e| IpcError::DaemonUnavailable(format!("daemon not running: {}", e)))?;
    send_request_over_stream(stream, req)
}

/// Wait for daemon to be ready and responding with expected version.
///
/// Returns Ok if daemon is responsive with matching version, Err on timeout (30s).
pub fn wait_for_daemon_ready(expected_version: &str) -> Result<(), IpcError> {
    let socket = socket_path();
    let deadline = SystemTime::now() + Duration::from_secs(30);
    let mut backoff = Duration::from_millis(50);

    while SystemTime::now() < deadline {
        match UnixStream::connect(&socket) {
            Ok(stream) => {
                let mut writer = stream;
                let reader_stream = match writer.try_clone() {
                    Ok(s) => s,
                    Err(_) => {
                        std::thread::sleep(backoff);
                        backoff = std::cmp::min(backoff * 2, Duration::from_millis(500));
                        continue;
                    }
                };
                let mut reader = BufReader::new(reader_stream);

                if write_req_line(&mut writer, &Request::Ping).is_err() {
                    std::thread::sleep(backoff);
                    backoff = std::cmp::min(backoff * 2, Duration::from_millis(500));
                    continue;
                }

                if let Ok(Response::Ok {
                    ok: ResponsePayload::Query(QueryResult::DaemonInfo(info)),
                }) = read_resp_line(&mut reader)
                {
                    if info.version == expected_version {
                        tracing::info!("daemon ready with version {}", expected_version);
                        return Ok(());
                    }
                    // Wrong version - old daemon hasn't fully died yet
                    tracing::debug!(
                        "daemon has version {}, waiting for {}",
                        info.version,
                        expected_version
                    );
                }
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, Duration::from_millis(500));
            }
            Err(_) => {
                std::thread::sleep(backoff);
                backoff = std::cmp::min(backoff * 2, Duration::from_millis(200));
            }
        }
    }

    Err(IpcError::DaemonUnavailable(format!(
        "timed out waiting for daemon version {}",
        expected_version
    )))
}

/// Kill daemon with SIGTERM, escalating to SIGKILL if needed.
fn kill_daemon_forcefully(pid: u32, socket: &PathBuf) -> Result<(), IpcError> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let nix_pid = Pid::from_raw(pid as i32);

    // First try SIGTERM (graceful)
    if let Err(e) = kill(nix_pid, Signal::SIGTERM) {
        // ESRCH = no such process - already dead, that's fine
        if e == nix::errno::Errno::ESRCH {
            let _ = fs::remove_file(socket);
            let _ = fs::remove_file(socket.with_file_name("daemon.meta.json"));
            return Ok(());
        }
        return Err(IpcError::DaemonUnavailable(format!(
            "failed to stop daemon pid {pid}: {e}"
        )));
    }

    // Wait for graceful shutdown (3 seconds)
    let deadline = SystemTime::now() + Duration::from_secs(3);
    while SystemTime::now() < deadline {
        if UnixStream::connect(socket).is_err() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Escalate to SIGKILL
    tracing::warn!(
        "daemon pid {} did not stop gracefully, sending SIGKILL",
        pid
    );
    if let Err(e) = kill(nix_pid, Signal::SIGKILL)
        && e != nix::errno::Errno::ESRCH
    {
        return Err(IpcError::DaemonUnavailable(format!(
            "failed to kill daemon pid {pid}: {e}"
        )));
    }

    // Wait for socket to become stale (2 more seconds)
    let deadline = SystemTime::now() + Duration::from_secs(2);
    while SystemTime::now() < deadline {
        if UnixStream::connect(socket).is_err() {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    // Force remove socket and meta as last resort
    let _ = fs::remove_file(socket);
    let _ = fs::remove_file(socket.with_file_name("daemon.meta.json"));
    Ok(())
}

/// Try to restart daemon when we don't have the PID from response.
///
/// Uses daemon.meta.json to find PID if available.
fn try_restart_daemon_by_socket(socket: &PathBuf) -> Result<(), IpcError> {
    // Try to get PID from meta file first
    if let Some(meta) = read_daemon_meta() {
        tracing::debug!("found daemon pid {} from meta file", meta.pid);
        return kill_daemon_forcefully(meta.pid, socket);
    }

    // No meta file (very old daemon or corrupt state)
    tracing::warn!("no daemon meta file found, removing stale socket");

    // Best effort: remove socket file. The orphaned daemon will eventually
    // exit when it has no clients and no work.
    if let Err(e) = fs::remove_file(socket)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        return Err(IpcError::DaemonUnavailable(format!(
            "failed to remove stale socket: {e}"
        )));
    }

    // Also remove meta file if present
    let _ = fs::remove_file(socket.with_file_name("daemon.meta.json"));

    Ok(())
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
        let resp = Response::ok(ResponsePayload::synced());
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\""));
        // Synced now serializes to {"result":"synced"}, not null
        assert!(json.contains("\"result\":\"synced\""));
    }

    #[test]
    fn unit_variants_are_distinguishable() {
        // Each variant must serialize to a distinct, non-null value with result field
        let synced = serde_json::to_string(&ResponsePayload::synced()).unwrap();
        let initialized = serde_json::to_string(&ResponsePayload::initialized()).unwrap();
        let shutting_down = serde_json::to_string(&ResponsePayload::shutting_down()).unwrap();

        assert!(synced.contains("\"result\":\"synced\""));
        assert!(initialized.contains("\"result\":\"initialized\""));
        assert!(shutting_down.contains("\"result\":\"shutting_down\""));

        // None serialize as null
        assert!(!synced.contains("null"));
        assert!(!initialized.contains("null"));
        assert!(!shutting_down.contains("null"));

        // All are distinct
        assert_ne!(synced, initialized);
        assert_ne!(synced, shutting_down);
        assert_ne!(initialized, shutting_down);
    }

    #[test]
    fn unit_variants_roundtrip() {
        // Verify each variant can be deserialized back correctly
        let synced_json = serde_json::to_string(&ResponsePayload::synced()).unwrap();
        let parsed: ResponsePayload = serde_json::from_str(&synced_json).unwrap();
        assert!(matches!(parsed, ResponsePayload::Synced(_)));

        let init_json = serde_json::to_string(&ResponsePayload::initialized()).unwrap();
        let parsed: ResponsePayload = serde_json::from_str(&init_json).unwrap();
        assert!(matches!(parsed, ResponsePayload::Initialized(_)));
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

    #[test]
    fn ping_info_serializes_as_query() {
        let info = crate::api::DaemonInfo {
            version: "0.0.0-test".to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            pid: 123,
        };
        let resp = Response::ok(ResponsePayload::Query(QueryResult::DaemonInfo(info)));
        let json = serde_json::to_string(&resp).unwrap();
        // Query variant serializes directly to its content (untagged)
        assert!(json.contains("\"result\":\"daemon_info\""));
        assert!(json.contains("\"version\""));
        assert!(json.contains("\"protocol_version\""));
        assert!(json.contains("\"pid\""));
    }

    #[test]
    fn version_mismatch_error_includes_parse_error() {
        let err = IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: "0.1.0".into(),
            protocol_version: 1,
            parse_error: Some("data did not match any variant".into()),
        };
        // Error should indicate version mismatch
        assert_eq!(err.code(), "daemon_version_mismatch");
        // The error message should be actionable
        assert!(err.to_string().contains("restart the daemon"));
    }

    #[test]
    fn version_mismatch_is_retryable() {
        let err = IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: "0.1.0".into(),
            protocol_version: 1,
            parse_error: None,
        };
        assert!(err.transience().is_retryable());
    }

    #[test]
    fn invalid_json_would_cause_parse_error() {
        // Simulate what an old/incompatible daemon might send
        let bad_json = r#"{"unexpected": "format"}"#;
        let result: Result<Response, _> = serde_json::from_str(bad_json);
        assert!(result.is_err());
        // This is the error type that would be converted to DaemonVersionMismatch
        let err = result.unwrap_err();
        assert!(err.to_string().contains("did not match"));
    }

    // Regression tests: verify all ResponsePayload variants roundtrip through Response
    mod response_roundtrip {
        use super::*;

        fn roundtrip_response(resp: Response) {
            let json = serde_json::to_string(&resp).unwrap();
            let parsed: Response = serde_json::from_str(&json)
                .unwrap_or_else(|e| panic!("Failed to parse: {e}\nJSON: {json}"));
            // Re-serialize to verify structural equality
            let json2 = serde_json::to_string(&parsed).unwrap();
            assert_eq!(json, json2, "Roundtrip mismatch");
        }

        #[test]
        fn op_created() {
            let resp = Response::ok(ResponsePayload::Op(OpResult::Created {
                id: BeadId::parse("bd-abc").unwrap(),
            }));
            roundtrip_response(resp);
        }

        #[test]
        fn op_updated() {
            let resp = Response::ok(ResponsePayload::Op(OpResult::Updated {
                id: BeadId::parse("bd-abc").unwrap(),
            }));
            roundtrip_response(resp);
        }

        #[test]
        fn query_issues_empty() {
            let resp = Response::ok(ResponsePayload::Query(QueryResult::Issues(vec![])));
            roundtrip_response(resp);
        }

        #[test]
        fn query_daemon_info() {
            let info = crate::api::DaemonInfo {
                version: "0.1.0".into(),
                protocol_version: IPC_PROTOCOL_VERSION,
                pid: 12345,
            };
            let resp = Response::ok(ResponsePayload::Query(QueryResult::DaemonInfo(info)));
            roundtrip_response(resp);
        }

        #[test]
        fn synced() {
            roundtrip_response(Response::ok(ResponsePayload::synced()));
        }

        #[test]
        fn initialized() {
            roundtrip_response(Response::ok(ResponsePayload::initialized()));
        }

        #[test]
        fn shutting_down() {
            roundtrip_response(Response::ok(ResponsePayload::shutting_down()));
        }

        #[test]
        fn error_response() {
            let resp = Response::err(ErrorPayload {
                code: "test_error".into(),
                message: "Something went wrong".into(),
                details: Some(serde_json::json!({"key": "value"})),
            });
            roundtrip_response(resp);
        }
    }
}
