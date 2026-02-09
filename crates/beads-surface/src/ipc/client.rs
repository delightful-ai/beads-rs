use std::ffi::OsString;
use std::fs;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use beads_api::QueryResult;

use super::spawn_sanitizer::prepare_daemon_spawn_command;
use crate::ipc::IpcError;
use crate::ipc::{IPC_PROTOCOL_VERSION, Request, Response, ResponsePayload};

// =============================================================================
// Socket path
// =============================================================================

static RUNTIME_DIR_OVERRIDE: OnceLock<Mutex<Option<PathBuf>>> = OnceLock::new();

/// Set or clear the runtime directory override. Can be called multiple times; latest value wins.
pub fn set_runtime_dir_override(dir: Option<PathBuf>) {
    let lock = RUNTIME_DIR_OVERRIDE.get_or_init(|| Mutex::new(None));
    *lock.lock().expect("runtime dir lock poisoned") = dir;
}

fn runtime_dir_override() -> Option<PathBuf> {
    let lock = RUNTIME_DIR_OVERRIDE.get()?;
    lock.lock().expect("runtime dir lock poisoned").clone()
}

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

/// Build a daemon socket path for a specific runtime directory.
pub fn socket_path_for_runtime_dir(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.sock")
}

/// Read daemon metadata from the meta file in the socket directory.
/// Returns None if file doesn't exist or is corrupt.
fn read_daemon_meta_at(socket: &Path) -> Option<beads_api::DaemonInfo> {
    let meta_path = socket.with_file_name("daemon.meta.json");
    let contents = fs::read_to_string(&meta_path).ok()?;
    serde_json::from_str(&contents).ok()
}

/// Read daemon metadata from the default socket directory.
fn read_daemon_meta() -> Option<beads_api::DaemonInfo> {
    let dir = ensure_socket_dir().ok()?;
    read_daemon_meta_at(&dir.join("daemon.sock"))
}

fn daemon_pid_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    kill(Pid::from_raw(pid as i32), None).is_ok()
}

fn per_user_tmp_dir() -> PathBuf {
    let uid = nix::unistd::geteuid();
    PathBuf::from("/tmp").join(format!("beads-{}", uid))
}

fn socket_dir_candidates() -> Vec<PathBuf> {
    socket_dir_candidates_with(|key| std::env::var(key).ok())
}

fn socket_dir_candidates_with<F>(mut lookup: F) -> Vec<PathBuf>
where
    F: FnMut(&str) -> Option<String>,
{
    let mut dirs = Vec::new();

    if let Some(runtime_dir) = runtime_dir_override() {
        dirs.push(runtime_dir.join("beads"));
    }

    // Env override (works even without config init)
    if let Some(dir) = lookup("BD_RUNTIME_DIR") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            dirs.push(PathBuf::from(trimmed).join("beads"));
        }
    }

    if let Some(dir) = lookup("XDG_RUNTIME_DIR") {
        let trimmed = dir.trim();
        if !trimmed.is_empty() {
            dirs.push(PathBuf::from(trimmed).join("beads"));
        }
    }
    if let Some(home) = lookup("HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            dirs.push(PathBuf::from(trimmed).join(".beads"));
        }
    }
    dirs.push(per_user_tmp_dir());
    dirs
}

fn expected_daemon_version(expected_version: Option<&str>) -> &str {
    expected_version.unwrap_or(env!("CARGO_PKG_VERSION"))
}

// =============================================================================
// Client - Send requests to daemon
// =============================================================================

#[derive(Clone, Debug)]
pub struct IpcClient {
    socket: PathBuf,
    autostart: bool,
    autostart_program: Option<PathBuf>,
    autostart_args: Vec<OsString>,
    expected_version: Option<String>,
}

impl IpcClient {
    pub fn new() -> Self {
        Self {
            socket: socket_path(),
            autostart: true,
            autostart_program: None,
            autostart_args: Vec::new(),
            expected_version: None,
        }
    }

    pub fn for_socket_path(socket: PathBuf) -> Self {
        Self {
            socket,
            autostart: true,
            autostart_program: None,
            autostart_args: Vec::new(),
            expected_version: None,
        }
    }

    pub fn for_runtime_dir(runtime_dir: &Path) -> Self {
        Self::for_socket_path(socket_path_for_runtime_dir(runtime_dir))
    }

    pub fn with_autostart(mut self, autostart: bool) -> Self {
        self.autostart = autostart;
        self
    }

    /// Override the program/args used for autostart.
    /// Default spawns `bd daemon run`.
    pub fn with_autostart_program(mut self, program: PathBuf, args: Vec<OsString>) -> Self {
        self.autostart_program = Some(program);
        self.autostart_args = args;
        self
    }

    /// Allow callers to relax or pin the version check.
    /// Default uses env!("CARGO_PKG_VERSION").
    pub fn with_expected_daemon_version(mut self, version: Option<String>) -> Self {
        self.expected_version = version;
        self
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket
    }

    pub fn connect(&self) -> Result<IpcConnection, IpcError> {
        IpcConnection::connect_with_options(
            self.socket.clone(),
            self.autostart,
            self.autostart_program.as_deref(),
            &self.autostart_args,
            self.expected_version.as_deref(),
        )
    }

    pub fn send_request(&self, req: &Request) -> Result<Response, IpcError> {
        if self.autostart {
            send_request_at_with_options(
                &self.socket,
                req,
                self.autostart_program.as_deref(),
                &self.autostart_args,
                self.expected_version.as_deref(),
            )
        } else {
            send_request_no_autostart_at_with_expected_version(
                &self.socket,
                req,
                self.expected_version.as_deref(),
            )
        }
    }

    pub fn send_request_no_autostart(&self, req: &Request) -> Result<Response, IpcError> {
        send_request_no_autostart_at_with_expected_version(
            &self.socket,
            req,
            self.expected_version.as_deref(),
        )
    }

    pub fn subscribe_stream(&self, req: &Request) -> Result<SubscriptionStream, IpcError> {
        if self.autostart {
            subscribe_stream_at_with_options(
                &self.socket,
                req,
                self.autostart_program.as_deref(),
                &self.autostart_args,
                self.expected_version.as_deref(),
            )
        } else {
            subscribe_stream_no_autostart_at_with_expected_version(
                &self.socket,
                req,
                self.expected_version.as_deref(),
            )
        }
    }

    pub fn wait_for_daemon_ready(&self, expected_version: &str) -> Result<(), IpcError> {
        wait_for_daemon_ready_at(&self.socket, expected_version)
    }
}

impl Default for IpcClient {
    fn default() -> Self {
        Self::new()
    }
}

pub struct IpcConnection {
    writer: UnixStream,
    reader: BufReader<UnixStream>,
}

impl IpcConnection {
    pub fn connect(socket: PathBuf, autostart: bool) -> Result<Self, IpcError> {
        Self::connect_with_options(socket, autostart, None, &[], None)
    }

    pub fn connect_with_options(
        socket: PathBuf,
        autostart: bool,
        autostart_program: Option<&Path>,
        autostart_args: &[OsString],
        expected_version: Option<&str>,
    ) -> Result<Self, IpcError> {
        const MAX_ATTEMPTS: u32 = 3;

        for attempt in 1..=MAX_ATTEMPTS {
            let stream = if autostart {
                connect_with_autostart(&socket, autostart_program, autostart_args)
            } else {
                UnixStream::connect(&socket).map_err(IpcError::from)
            };
            let stream = match stream {
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

            let mut conn = IpcConnection::new(stream)?;
            match verify_daemon_version(
                &socket,
                &mut conn.writer,
                &mut conn.reader,
                expected_version,
            ) {
                Ok(()) => return Ok(conn),
                Err(IpcError::DaemonVersionMismatch { daemon, .. }) if attempt < MAX_ATTEMPTS => {
                    tracing::info!(
                        "daemon version mismatch, restarting (attempt {}/{})",
                        attempt,
                        MAX_ATTEMPTS
                    );
                    if let Some(info) = daemon {
                        let _ = kill_daemon_forcefully(info.pid, &socket);
                    } else {
                        let _ = try_restart_daemon_by_socket(&socket);
                    }
                    let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                    std::thread::sleep(backoff);
                }
                Err(IpcError::DaemonUnavailable(ref msg)) if attempt < MAX_ATTEMPTS => {
                    tracing::debug!("daemon unavailable ({}), retrying", msg);
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

    fn new(stream: UnixStream) -> Result<Self, IpcError> {
        let reader_stream = stream.try_clone()?;
        Ok(Self {
            writer: stream,
            reader: BufReader::new(reader_stream),
        })
    }

    pub fn send_request(&mut self, req: &Request) -> Result<Response, IpcError> {
        write_req_line(&mut self.writer, req)?;
        read_resp_line(&mut self.reader)
    }
}

fn should_autostart(err: &std::io::Error) -> bool {
    matches!(
        err.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
    )
}

const AUTOSTART_CONNECT_TIMEOUT_SECS: u64 = 30;
const AUTOSTART_CONNECT_TIMEOUT_TEST_FAST_SECS: u64 = 5;
const AUTOSTART_LOCK_STALE_GRACE_SECS: u64 = 5;

fn autostart_connect_timeout() -> Duration {
    if std::env::var_os("BD_TEST_FAST").is_some() {
        Duration::from_secs(AUTOSTART_CONNECT_TIMEOUT_TEST_FAST_SECS)
    } else {
        Duration::from_secs(AUTOSTART_CONNECT_TIMEOUT_SECS)
    }
}

fn autostart_lock_stale_age(connect_timeout: Duration) -> Duration {
    connect_timeout.saturating_add(Duration::from_secs(AUTOSTART_LOCK_STALE_GRACE_SECS))
}

fn maybe_remove_stale_lock(lock_path: &PathBuf, max_age: Duration) {
    if let Ok(meta) = fs::metadata(lock_path)
        && let Ok(modified) = meta.modified()
        && let Ok(age) = modified.elapsed()
        && age > max_age
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

fn daemon_command_override(program: Option<&Path>, args: &[OsString]) -> Command {
    if let Some(program) = program {
        let mut cmd = Command::new(program);
        cmd.args(args);
        return cmd;
    }

    daemon_command()
}

fn connect_with_autostart(
    socket: &PathBuf,
    autostart_program: Option<&Path>,
    autostart_args: &[OsString],
) -> Result<UnixStream, IpcError> {
    match UnixStream::connect(socket) {
        Ok(stream) => Ok(stream),
        Err(e) if should_autostart(&e) => {
            // Try to autostart daemon with a simple lock to avoid herds.
            let connect_timeout = autostart_connect_timeout();
            let stale_lock_age = autostart_lock_stale_age(connect_timeout);
            let dir = ensure_socket_dir()?;
            let lock_path = dir.join("daemon.lock");
            maybe_remove_stale_lock(&lock_path, stale_lock_age);

            let mut we_spawned = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .is_ok();

            if we_spawned {
                let mut cmd = daemon_command_override(autostart_program, autostart_args);
                prepare_daemon_spawn_command(&mut cmd);
                cmd.spawn().map_err(|e| {
                    IpcError::DaemonUnavailable(format!("failed to spawn daemon: {}", e))
                })?;
            }

            let deadline = SystemTime::now() + connect_timeout;
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
                            maybe_remove_stale_lock(&lock_path, stale_lock_age);
                            if OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(&lock_path)
                                .is_ok()
                            {
                                we_spawned = true;
                                let mut cmd =
                                    daemon_command_override(autostart_program, autostart_args);
                                prepare_daemon_spawn_command(&mut cmd);
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
                            return Err(IpcError::DaemonUnavailable(format!(
                                "timed out waiting for daemon socket after {}s",
                                connect_timeout.as_secs()
                            )));
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
fn read_resp_line_version_check(
    reader: &mut BufReader<UnixStream>,
    expected_version: Option<&str>,
) -> Result<Response, IpcError> {
    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line)?;
    if bytes_read == 0 || line.trim().is_empty() {
        return Err(IpcError::DaemonUnavailable(
            "daemon not running (stale socket)".into(),
        ));
    }
    let expected_version = expected_daemon_version(expected_version);
    serde_json::from_str(&line).map_err(|e| IpcError::DaemonVersionMismatch {
        daemon: None,
        client_version: expected_version.to_string(),
        protocol_version: IPC_PROTOCOL_VERSION,
        parse_error: Some(e.to_string()),
    })
}

fn verify_daemon_version(
    socket: &Path,
    writer: &mut UnixStream,
    reader: &mut BufReader<UnixStream>,
    expected_version: Option<&str>,
) -> Result<(), IpcError> {
    let expected_version = expected_daemon_version(expected_version);
    if let Some(meta) = read_daemon_meta_at(socket)
        && daemon_pid_alive(meta.pid)
    {
        if meta.protocol_version == IPC_PROTOCOL_VERSION && meta.version == expected_version {
            return Ok(());
        }
        return Err(IpcError::DaemonVersionMismatch {
            daemon: Some(meta),
            client_version: expected_version.to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    }

    write_req_line(writer, &Request::Ping)?;
    // Use version-check variant that converts parse errors to version mismatch
    let resp = read_resp_line_version_check(reader, Some(expected_version))?;
    let Response::Ok { ok } = resp else {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: expected_version.to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    };

    let ResponsePayload::Query(QueryResult::DaemonInfo(info)) = ok else {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: None,
            client_version: expected_version.to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: Some("unexpected response payload type".into()),
        });
    };

    if info.protocol_version != IPC_PROTOCOL_VERSION || info.version != expected_version {
        return Err(IpcError::DaemonVersionMismatch {
            daemon: Some(info),
            client_version: expected_version.to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            parse_error: None,
        });
    }

    Ok(())
}

fn send_request_over_stream(
    stream: UnixStream,
    socket: &Path,
    req: &Request,
    expected_version: Option<&str>,
) -> Result<Response, IpcError> {
    let mut writer = stream;
    let reader_stream = writer.try_clone()?;
    let mut reader = BufReader::new(reader_stream);

    // Verify daemon version/protocol once per connection.
    if !matches!(req, Request::Ping) {
        verify_daemon_version(socket, &mut writer, &mut reader, expected_version)?;
    }

    write_req_line(&mut writer, req)?;
    read_resp_line(&mut reader)
}

/// Send a request to the daemon and receive a response.
///
/// Retries up to 3 times on version mismatch or stale socket errors,
/// with exponential backoff between attempts.
pub fn send_request_at(socket: &PathBuf, req: &Request) -> Result<Response, IpcError> {
    send_request_at_with_options(socket, req, None, &[], None)
}

fn send_request_at_with_options(
    socket: &PathBuf,
    req: &Request,
    autostart_program: Option<&Path>,
    autostart_args: &[OsString],
    expected_version: Option<&str>,
) -> Result<Response, IpcError> {
    const MAX_ATTEMPTS: u32 = 3;

    for attempt in 1..=MAX_ATTEMPTS {
        let stream = match connect_with_autostart(socket, autostart_program, autostart_args) {
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

        match send_request_over_stream(stream, socket, req, expected_version) {
            Ok(resp) => return Ok(resp),
            Err(IpcError::DaemonVersionMismatch { daemon, .. }) if attempt < MAX_ATTEMPTS => {
                tracing::info!(
                    "daemon version mismatch, restarting (attempt {}/{})",
                    attempt,
                    MAX_ATTEMPTS
                );

                // Try to restart the daemon
                if let Some(info) = daemon {
                    let _ = kill_daemon_forcefully(info.pid, socket);
                } else {
                    let _ = try_restart_daemon_by_socket(socket);
                }

                // Exponential backoff: 100ms, 200ms, 400ms
                let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                std::thread::sleep(backoff);
            }
            Err(IpcError::DaemonUnavailable(ref msg)) if attempt < MAX_ATTEMPTS => {
                tracing::debug!("daemon unavailable ({}), retrying", msg);
                // Socket might have gone stale mid-request
                let _ = try_restart_daemon_by_socket(socket);
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

pub fn send_request(req: &Request) -> Result<Response, IpcError> {
    let socket = socket_path();
    send_request_at(&socket, req)
}

/// Send a request without auto-starting the daemon.
///
/// Returns `DaemonUnavailable` if daemon is not running.
pub fn send_request_no_autostart_at(socket: &PathBuf, req: &Request) -> Result<Response, IpcError> {
    send_request_no_autostart_at_with_expected_version(socket, req, None)
}

fn send_request_no_autostart_at_with_expected_version(
    socket: &PathBuf,
    req: &Request,
    expected_version: Option<&str>,
) -> Result<Response, IpcError> {
    let stream = UnixStream::connect(socket)
        .map_err(|e| IpcError::DaemonUnavailable(format!("daemon not running: {}", e)))?;
    send_request_over_stream(stream, socket, req, expected_version)
}

pub fn send_request_no_autostart(req: &Request) -> Result<Response, IpcError> {
    let socket = socket_path();
    send_request_no_autostart_at(&socket, req)
}

/// Stream responses for a subscribe request.
pub struct SubscriptionStream {
    _writer: UnixStream,
    reader: BufReader<UnixStream>,
}

impl SubscriptionStream {
    pub fn read_response(&mut self) -> Result<Option<Response>, IpcError> {
        let mut line = String::new();
        let bytes_read = self.reader.read_line(&mut line)?;
        if bytes_read == 0 || line.trim().is_empty() {
            return Ok(None);
        }
        let response = serde_json::from_str(&line)?;
        Ok(Some(response))
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<(), IpcError> {
        self.reader.get_ref().set_read_timeout(timeout)?;
        Ok(())
    }
}

/// Send a subscribe request and return a stream of responses.
pub fn subscribe_stream_at(
    socket: &PathBuf,
    req: &Request,
) -> Result<SubscriptionStream, IpcError> {
    subscribe_stream_at_with_options(socket, req, None, &[], None)
}

fn subscribe_stream_at_with_options(
    socket: &PathBuf,
    req: &Request,
    autostart_program: Option<&Path>,
    autostart_args: &[OsString],
    expected_version: Option<&str>,
) -> Result<SubscriptionStream, IpcError> {
    if !matches!(req, Request::Subscribe { .. }) {
        return Err(IpcError::InvalidRequest {
            field: Some("op".into()),
            reason: "subscribe_stream expects subscribe request".into(),
        });
    }

    const MAX_ATTEMPTS: u32 = 3;
    for attempt in 1..=MAX_ATTEMPTS {
        let stream = match connect_with_autostart(socket, autostart_program, autostart_args) {
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

        let mut writer = stream;
        let reader_stream = writer.try_clone()?;
        let mut reader = BufReader::new(reader_stream);

        if let Err(err) = verify_daemon_version(socket, &mut writer, &mut reader, expected_version)
        {
            match err {
                IpcError::DaemonVersionMismatch { daemon, .. } if attempt < MAX_ATTEMPTS => {
                    tracing::info!(
                        "daemon version mismatch, restarting (attempt {}/{})",
                        attempt,
                        MAX_ATTEMPTS
                    );
                    if let Some(info) = daemon {
                        let _ = kill_daemon_forcefully(info.pid, socket);
                    } else {
                        let _ = try_restart_daemon_by_socket(socket);
                    }
                    let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                    std::thread::sleep(backoff);
                    continue;
                }
                IpcError::Parse(_)
                | IpcError::Io(_)
                | IpcError::InvalidId(_)
                | IpcError::InvalidRequest { .. }
                | IpcError::Disconnected
                | IpcError::DaemonUnavailable(_)
                | IpcError::DaemonVersionMismatch { .. }
                | IpcError::FrameTooLarge { .. } => return Err(err),
            }
        }

        write_req_line(&mut writer, req)?;
        return Ok(SubscriptionStream {
            _writer: writer,
            reader,
        });
    }

    Err(IpcError::DaemonUnavailable(
        "max retry attempts exceeded".into(),
    ))
}

/// Send a subscribe request without auto-starting the daemon.
pub fn subscribe_stream_no_autostart_at(
    socket: &PathBuf,
    req: &Request,
) -> Result<SubscriptionStream, IpcError> {
    subscribe_stream_no_autostart_at_with_expected_version(socket, req, None)
}

fn subscribe_stream_no_autostart_at_with_expected_version(
    socket: &PathBuf,
    req: &Request,
    expected_version: Option<&str>,
) -> Result<SubscriptionStream, IpcError> {
    if !matches!(req, Request::Subscribe { .. }) {
        return Err(IpcError::InvalidRequest {
            field: Some("op".into()),
            reason: "subscribe_stream expects subscribe request".into(),
        });
    }

    let stream = UnixStream::connect(socket)
        .map_err(|e| IpcError::DaemonUnavailable(format!("daemon not running: {}", e)))?;
    let mut writer = stream;
    let reader_stream = writer.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    verify_daemon_version(socket, &mut writer, &mut reader, expected_version)?;
    write_req_line(&mut writer, req)?;
    Ok(SubscriptionStream {
        _writer: writer,
        reader,
    })
}

/// Send a subscribe request and return a stream of responses.
pub fn subscribe_stream(req: &Request) -> Result<SubscriptionStream, IpcError> {
    let socket = socket_path();
    subscribe_stream_at(&socket, req)
}

/// Wait for daemon to be ready and responding with expected version.
///
/// Returns Ok if daemon is responsive with matching version, Err on timeout (30s).
pub fn wait_for_daemon_ready(expected_version: &str) -> Result<(), IpcError> {
    let socket = socket_path();
    wait_for_daemon_ready_at(&socket, expected_version)
}

/// Wait for daemon to be ready and responding with expected version.
pub fn wait_for_daemon_ready_at(socket: &PathBuf, expected_version: &str) -> Result<(), IpcError> {
    let deadline = SystemTime::now() + Duration::from_secs(30);
    let mut backoff = Duration::from_millis(50);

    while SystemTime::now() < deadline {
        match UnixStream::connect(socket) {
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
    use tempfile::TempDir;

    #[test]
    fn version_check_uses_meta_when_matching() {
        let temp = TempDir::new().expect("temp dir");
        let socket = temp.path().join("daemon.sock");
        let meta = beads_api::DaemonInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            pid: std::process::id(),
        };
        let meta_path = socket.with_file_name("daemon.meta.json");
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        let (stream, _peer) = UnixStream::pair().expect("socket pair");
        let mut writer = stream;
        let reader_stream = writer.try_clone().expect("clone stream");
        let mut reader = BufReader::new(reader_stream);
        verify_daemon_version(&socket, &mut writer, &mut reader, None).expect("meta match");
    }

    #[test]
    fn version_check_rejects_meta_mismatch() {
        let temp = TempDir::new().expect("temp dir");
        let socket = temp.path().join("daemon.sock");
        let meta = beads_api::DaemonInfo {
            version: "0.0.0-fake".to_string(),
            protocol_version: IPC_PROTOCOL_VERSION,
            pid: std::process::id(),
        };
        let meta_path = socket.with_file_name("daemon.meta.json");
        std::fs::write(&meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();

        let (stream, _peer) = UnixStream::pair().expect("socket pair");
        let mut writer = stream;
        let reader_stream = writer.try_clone().expect("clone stream");
        let mut reader = BufReader::new(reader_stream);
        let err = verify_daemon_version(&socket, &mut writer, &mut reader, None).unwrap_err();
        assert!(matches!(
            err,
            IpcError::DaemonVersionMismatch {
                daemon: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn socket_dir_candidates_prefers_runtime_override() {
        let temp = TempDir::new().expect("temp dir");
        if runtime_dir_override().is_none() {
            set_runtime_dir_override(Some(temp.path().to_path_buf()));
        }

        let runtime_dir = runtime_dir_override().expect("runtime override");
        let expected = runtime_dir.join("beads");
        let dirs = socket_dir_candidates();
        assert_eq!(dirs.first(), Some(&expected));
    }

    #[test]
    fn socket_dir_candidates_prefers_runtime_env() {
        let temp = TempDir::new().expect("temp dir");
        let dirs = socket_dir_candidates_with(|key| match key {
            "BD_RUNTIME_DIR" => Some(
                temp.path()
                    .to_str()
                    .expect("runtime dir is valid utf-8")
                    .to_string(),
            ),
            _ => None,
        });
        let expected = temp.path().join("beads");
        if runtime_dir_override().is_some() {
            assert!(dirs.contains(&expected));
        } else {
            assert_eq!(dirs.first(), Some(&expected));
        }
    }
}
