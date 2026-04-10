use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, Stdio};
use std::sync::Mutex;
use std::time::Duration;

use assert_cmd::Command;
use beads_api::{AdminStoreUnlockOutput, QueryResult};
use beads_core::{StoreId, StoreMeta};
use beads_surface::ipc::{EmptyPayload, IpcClient, RepoCtx, Request, Response, ResponsePayload};
use tempfile::TempDir;

use super::daemon_runtime::shutdown_daemon;
use super::git::{init_bare_repo, init_repo, init_repo_with_origin};
use super::ipc_client::{runtime_bound_client, runtime_bound_client_no_autostart};
use super::temp;
use super::timing;
use super::wait;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointMode {
    Enabled,
    Disabled,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BdCommandProfile {
    fast: bool,
    testing: bool,
    git_sync_enabled: bool,
    checkpoints: CheckpointMode,
    wal_sync_mode: Option<&'static str>,
}

impl BdCommandProfile {
    pub(crate) fn cli() -> Self {
        Self {
            fast: false,
            testing: true,
            git_sync_enabled: false,
            checkpoints: CheckpointMode::Enabled,
            wal_sync_mode: None,
        }
    }

    pub(crate) fn daemon() -> Self {
        Self {
            fast: false,
            testing: false,
            git_sync_enabled: false,
            checkpoints: CheckpointMode::Enabled,
            wal_sync_mode: None,
        }
    }

    pub(crate) fn fast_daemon() -> Self {
        Self {
            fast: true,
            testing: true,
            git_sync_enabled: false,
            checkpoints: CheckpointMode::Disabled,
            wal_sync_mode: Some("none"),
        }
    }

    #[cfg(feature = "slow-tests")]
    pub(crate) fn with_git_sync(mut self, enabled: bool) -> Self {
        self.git_sync_enabled = enabled;
        self
    }

    pub(crate) fn with_checkpoints(mut self, checkpoints: CheckpointMode) -> Self {
        self.checkpoints = checkpoints;
        self
    }
}

trait CommandEnv {
    fn current_dir_path(&mut self, dir: &Path);
    fn env_os(&mut self, key: &str, value: &OsStr);
    fn env_remove_var(&mut self, key: &str);
}

impl CommandEnv for Command {
    fn current_dir_path(&mut self, dir: &Path) {
        self.current_dir(dir);
    }

    fn env_os(&mut self, key: &str, value: &OsStr) {
        self.env(key, value);
    }

    fn env_remove_var(&mut self, key: &str) {
        self.env_remove(key);
    }
}

impl CommandEnv for StdCommand {
    fn current_dir_path(&mut self, dir: &Path) {
        self.current_dir(dir);
    }

    fn env_os(&mut self, key: &str, value: &OsStr) {
        self.env(key, value);
    }

    fn env_remove_var(&mut self, key: &str) {
        self.env_remove(key);
    }
}

fn scrub_inherited_test_env<C: CommandEnv>(cmd: &mut C) {
    for key in beads_bootstrap::config::CONFIG_ENV_KEYS {
        cmd.env_remove_var(key);
    }
    for key in [
        "XDG_CONFIG_HOME",
        "XDG_RUNTIME_DIR",
        "GIT_DIR",
        "GIT_WORK_TREE",
    ] {
        cmd.env_remove_var(key);
    }
}

pub(crate) fn scrub_assert_test_env(cmd: &mut Command) {
    scrub_inherited_test_env(cmd);
}

pub(crate) fn scrub_std_test_env(cmd: &mut StdCommand) {
    scrub_inherited_test_env(cmd);
}

pub fn data_dir_for_runtime(runtime_dir: &Path) -> PathBuf {
    let _phase =
        timing::scoped_phase_with_context("fixture.bd_runtime.data_dir", runtime_dir.display());
    let dir = runtime_dir.join("data");
    fs::create_dir_all(&dir).expect("failed to create test data dir");
    dir
}

pub fn config_dir_for_runtime(runtime_dir: &Path) -> PathBuf {
    let dir = runtime_dir.join("config");
    fs::create_dir_all(&dir).expect("failed to create test config dir");
    dir
}

pub fn daemon_socket_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.sock")
}

pub fn daemon_meta_path(runtime_dir: &Path) -> PathBuf {
    runtime_dir.join("beads").join("daemon.meta.json")
}

pub fn daemon_pid(runtime_dir: &Path) -> Option<u32> {
    let contents = fs::read_to_string(daemon_meta_path(runtime_dir)).ok()?;
    let meta: serde_json::Value = serde_json::from_str(&contents).ok()?;
    meta["pid"].as_u64().map(|pid| pid as u32)
}

pub fn daemon_version(runtime_dir: &Path) -> Option<String> {
    let contents = fs::read_to_string(daemon_meta_path(runtime_dir)).ok()?;
    let meta: serde_json::Value = serde_json::from_str(&contents).ok()?;
    meta["version"].as_str().map(ToOwned::to_owned)
}

pub fn wait_for_daemon_pid(runtime_dir: &Path, timeout: Duration) -> Option<u32> {
    let mut pid = None;
    let ready = wait::poll_until_with_phase(
        "fixture.bd_runtime.wait_for_daemon_pid",
        runtime_dir.display(),
        timeout,
        || {
            pid = daemon_pid(runtime_dir);
            pid.is_some()
        },
    );
    ready.then_some(pid).flatten()
}

pub fn store_dir_from_data_dir(data_dir: &Path) -> Option<PathBuf> {
    let stores_dir = data_dir.join("stores");
    let mut entries: Vec<PathBuf> = fs::read_dir(&stores_dir)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .collect();
    entries.sort();
    (entries.len() == 1).then(|| entries.remove(0))
}

pub fn store_meta_from_data_dir(data_dir: &Path) -> Option<StoreMeta> {
    let store_dir = store_dir_from_data_dir(data_dir)?;
    let contents = fs::read_to_string(store_dir.join("meta.json")).ok()?;
    serde_json::from_str(&contents).ok()
}

pub fn wait_for_store_id(data_dir: &Path, timeout: Duration) -> Option<StoreId> {
    let mut store_id = None;
    let ready = wait::poll_until_with_phase(
        "fixture.bd_runtime.wait_for_store_id",
        data_dir.display(),
        timeout,
        || {
            store_id = store_meta_from_data_dir(data_dir).map(|meta| meta.store_id());
            store_id.is_some()
        },
    );
    ready.then_some(store_id).flatten()
}

fn configure_bd_command_with_paths<C: CommandEnv>(
    cmd: &mut C,
    cwd: &Path,
    runtime_dir: &Path,
    data_dir: &Path,
    config_dir: &Path,
    store_id: Option<StoreId>,
    profile: BdCommandProfile,
) {
    scrub_inherited_test_env(cmd);
    cmd.current_dir_path(cwd);
    cmd.env_os("XDG_RUNTIME_DIR", runtime_dir.as_os_str());
    cmd.env_os("BD_RUNTIME_DIR", runtime_dir.as_os_str());
    cmd.env_os("BD_WAL_DIR", runtime_dir.as_os_str());
    cmd.env_os("BD_DATA_DIR", data_dir.as_os_str());
    cmd.env_os("BD_CONFIG_DIR", config_dir.as_os_str());
    if let Some(store_id) = store_id {
        let store_id = store_id.to_string();
        cmd.env_os("BD_STORE_ID", OsStr::new(store_id.as_str()));
    } else {
        cmd.env_remove_var("BD_STORE_ID");
    }
    cmd.env_os("BD_NO_AUTO_UPGRADE", OsStr::new("1"));
    if profile.testing {
        cmd.env_os("BD_TESTING", OsStr::new("1"));
        cmd.env_os(
            "BD_TEST_DISABLE_GIT_SYNC",
            if profile.git_sync_enabled {
                OsStr::new("0")
            } else {
                OsStr::new("1")
            },
        );

        if profile.fast {
            cmd.env_os("BD_TEST_FAST", OsStr::new("1"));
        } else {
            cmd.env_remove_var("BD_TEST_FAST");
        }

        match profile.checkpoints {
            CheckpointMode::Enabled => cmd.env_remove_var("BD_TEST_DISABLE_CHECKPOINTS"),
            CheckpointMode::Disabled => cmd.env_os("BD_TEST_DISABLE_CHECKPOINTS", OsStr::new("1")),
        }

        if let Some(mode) = profile.wal_sync_mode {
            cmd.env_os("BD_WAL_SYNC_MODE", OsStr::new(mode));
        } else {
            cmd.env_remove_var("BD_WAL_SYNC_MODE");
        }
    } else {
        cmd.env_remove_var("BD_TESTING");
        cmd.env_remove_var("BD_TEST_DISABLE_GIT_SYNC");
        cmd.env_remove_var("BD_TEST_FAST");
        cmd.env_remove_var("BD_TEST_DISABLE_CHECKPOINTS");
        cmd.env_remove_var("BD_WAL_SYNC_MODE");
    }
}

fn configure_bd_command<C: CommandEnv>(
    cmd: &mut C,
    cwd: &Path,
    runtime_dir: &Path,
    data_dir: &Path,
    profile: BdCommandProfile,
) {
    let config_dir = config_dir_for_runtime(runtime_dir);
    configure_bd_command_with_paths(cmd, cwd, runtime_dir, data_dir, &config_dir, None, profile);
}

pub(crate) fn configure_std_bd_command(
    cmd: &mut StdCommand,
    cwd: &Path,
    runtime_dir: &Path,
    data_dir: &Path,
    profile: BdCommandProfile,
) {
    configure_bd_command(cmd, cwd, runtime_dir, data_dir, profile);
}

pub(crate) fn configure_std_bd_command_with_paths(
    cmd: &mut StdCommand,
    cwd: &Path,
    runtime_dir: &Path,
    data_dir: &Path,
    config_dir: &Path,
    store_id: Option<StoreId>,
    profile: BdCommandProfile,
) {
    configure_bd_command_with_paths(
        cmd,
        cwd,
        runtime_dir,
        data_dir,
        config_dir,
        store_id,
        profile,
    );
}

pub(crate) fn spawn_daemon_with_paths(
    cwd: &Path,
    runtime_dir: &Path,
    data_dir: &Path,
    config_dir: &Path,
    store_id: Option<StoreId>,
    profile: BdCommandProfile,
) -> Child {
    let mut cmd = StdCommand::new(assert_cmd::cargo::cargo_bin!("bd"));
    configure_std_bd_command_with_paths(
        &mut cmd,
        cwd,
        runtime_dir,
        data_dir,
        config_dir,
        store_id,
        profile,
    );
    cmd.args(["daemon", "run"]);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd.spawn().expect("spawn daemon")
}

pub(crate) fn wait_for_daemon_ready(client: &IpcClient, timeout: Duration) -> bool {
    wait::poll_until(timeout, || ping_daemon(client))
}

pub(crate) fn initialize_repo_with_client(client: &IpcClient, repo_path: &Path) {
    let request = Request::Init {
        ctx: RepoCtx::new(repo_path.to_path_buf()),
        payload: EmptyPayload {},
    };
    let response = {
        let _phase = timing::scoped_phase("fixture.bd_runtime.init_request");
        client
            .send_request_no_autostart(&request)
            .expect("init response")
    };
    match response {
        Response::Ok {
            ok: ResponsePayload::Initialized(_),
        } => {}
        other => panic!("unexpected init response: {other:?}"),
    }
}

pub struct BdRuntimeRepo {
    pub work_dir: TempDir,
    #[allow(dead_code)]
    pub remote_dir: TempDir,
    pub runtime_dir: TempDir,
    pub data_dir: PathBuf,
    pub store_id: Option<beads_core::StoreId>,
    daemon_child: Mutex<Option<Child>>,
}

impl BdRuntimeRepo {
    pub fn new() -> Self {
        Self::new_with_origin()
    }

    pub fn new_with_origin() -> Self {
        let _phase = timing::scoped_phase("fixture.bd_runtime.new_with_origin");
        let remote_dir = temp::fixture_tempdir("bd-remote");
        init_bare_repo(remote_dir.path()).expect("failed to init bare repo");

        let work_dir = temp::fixture_tempdir("bd-work");
        init_repo_with_origin(work_dir.path(), remote_dir.path())
            .expect("failed to init repo with origin");

        let runtime_dir = temp::fixture_tempdir("bd-runtime");
        let data_dir = data_dir_for_runtime(runtime_dir.path());

        Self {
            work_dir,
            remote_dir,
            runtime_dir,
            data_dir,
            store_id: None,
            daemon_child: Mutex::new(None),
        }
    }

    pub fn new_local_only() -> Self {
        let _phase = timing::scoped_phase("fixture.bd_runtime.new_local_only");
        let remote_dir = temp::fixture_tempdir("bd-remote-placeholder");
        let work_dir = temp::fixture_tempdir("bd-work");
        init_repo(work_dir.path()).expect("failed to init local-only repo");

        let runtime_dir = temp::fixture_tempdir("bd-runtime");
        let data_dir = data_dir_for_runtime(runtime_dir.path());

        Self {
            work_dir,
            remote_dir,
            runtime_dir,
            data_dir,
            store_id: None,
            daemon_child: Mutex::new(None),
        }
    }

    pub fn with_runtime_derived_store_id(mut self) -> Self {
        let runtime_str = self.runtime_dir.path().to_string_lossy();
        let store_uuid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, runtime_str.as_bytes());
        self.store_id = Some(beads_core::StoreId::new(store_uuid));
        self
    }

    pub fn path(&self) -> &Path {
        self.work_dir.path()
    }

    pub fn runtime_dir(&self) -> &Path {
        self.runtime_dir.path()
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn daemon_socket_path(&self) -> PathBuf {
        daemon_socket_path(self.runtime_dir())
    }

    pub fn daemon_meta_path(&self) -> PathBuf {
        daemon_meta_path(self.runtime_dir())
    }

    pub fn daemon_pid(&self) -> Option<u32> {
        daemon_pid(self.runtime_dir())
    }

    pub fn daemon_version(&self) -> Option<String> {
        daemon_version(self.runtime_dir())
    }

    pub fn ipc_client_no_autostart(&self) -> IpcClient {
        runtime_bound_client_no_autostart(self.runtime_dir())
    }

    #[allow(dead_code)]
    pub fn ipc_client_autostart(&self) -> IpcClient {
        runtime_bound_client(self.runtime_dir())
    }

    pub fn bd_with_profile(&self, profile: BdCommandProfile) -> Command {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("bd");
        self.configure_command(&mut cmd, profile);
        cmd
    }

    pub fn bd(&self) -> Command {
        self.bd_with_profile(BdCommandProfile::cli())
    }

    pub fn store_unlock_output(&self, store_id: StoreId, force: bool) -> AdminStoreUnlockOutput {
        let mut cmd = self.bd();
        cmd.args(["store", "unlock", "--store-id"]);
        let store_id = store_id.to_string();
        cmd.arg(store_id.as_str());
        if force {
            cmd.arg("--force");
        }
        cmd.arg("--json");
        let output = cmd.output().expect("run bd store unlock");
        assert!(
            output.status.success(),
            "bd store unlock failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        serde_json::from_slice(&output.stdout).expect("parse store unlock output")
    }

    #[cfg(feature = "slow-tests")]
    pub fn bd_sync_enabled(&self) -> Command {
        self.bd_with_profile(BdCommandProfile::cli().with_git_sync(true))
    }

    pub fn spawn_daemon(&self, profile: BdCommandProfile) -> Child {
        let _phase = timing::scoped_phase("fixture.bd_runtime.daemon_spawn");
        let config_dir = config_dir_for_runtime(self.runtime_dir());
        spawn_daemon_with_paths(
            self.path(),
            self.runtime_dir(),
            self.data_dir(),
            &config_dir,
            self.store_id,
            profile,
        )
    }

    pub fn ensure_daemon_running(&self, profile: BdCommandProfile) -> IpcClient {
        let _phase = timing::scoped_phase("fixture.bd_runtime.ensure_daemon_running");
        let client = self.ipc_client_no_autostart();
        if !ping_daemon(&client) {
            self.ensure_daemon_child_running(profile);
            let ok = {
                let _phase = timing::scoped_phase("fixture.bd_runtime.daemon_ready_wait");
                wait_for_daemon_ready(&client, Duration::from_secs(5))
            };
            assert!(ok, "daemon failed to start for {}", self.path().display());
        }
        client
    }

    pub fn initialize_repo(&self, client: &IpcClient) {
        initialize_repo_with_client(client, self.path());
    }

    pub fn start_daemon(&self, profile: BdCommandProfile) -> IpcClient {
        let client = self.ensure_daemon_running(profile);
        self.initialize_repo(&client);
        client
    }

    fn configure_command<C: CommandEnv>(&self, cmd: &mut C, profile: BdCommandProfile) {
        let config_dir = config_dir_for_runtime(self.runtime_dir());
        configure_bd_command_with_paths(
            cmd,
            self.path(),
            self.runtime_dir(),
            self.data_dir(),
            &config_dir,
            self.store_id,
            profile,
        );
    }

    fn ensure_daemon_child_running(&self, profile: BdCommandProfile) {
        let mut guard = self.daemon_child.lock().expect("daemon child lock");
        let needs_spawn = match guard.as_mut() {
            Some(child) => child.try_wait().expect("poll daemon child").is_some(),
            None => true,
        };
        if !needs_spawn {
            return;
        }

        *guard = Some(self.spawn_daemon(profile));
    }

    fn sync_daemon_child_exit(&self, timeout: Duration) {
        let mut guard = self.daemon_child.lock().expect("daemon child lock");
        let Some(child) = guard.as_mut() else {
            return;
        };
        if wait::wait_for_child_exit(child, timeout) {
            let _ = guard.take();
        }
    }
}

impl Drop for BdRuntimeRepo {
    fn drop(&mut self) {
        shutdown_daemon(self.runtime_dir(), self.data_dir());
        self.sync_daemon_child_exit(Duration::from_secs(2));
    }
}

pub(crate) fn ping_daemon(client: &IpcClient) -> bool {
    matches!(
        client.send_request_no_autostart(&Request::Ping),
        Ok(Response::Ok {
            ok: ResponsePayload::Query(QueryResult::DaemonInfo(_)),
        })
    )
}

#[cfg(test)]
mod tests {
    use std::ffi::{OsStr, OsString};
    use std::path::Path;
    use std::process::Command as StdCommand;
    use std::time::Duration;

    use super::{
        BdCommandProfile, BdRuntimeRepo, CheckpointMode, configure_std_bd_command,
        scrub_std_test_env,
    };

    #[test]
    fn runtime_repo_allocates_isolated_runtime_and_data_dirs() {
        let left = BdRuntimeRepo::new_with_origin();
        let right = BdRuntimeRepo::new_with_origin();

        assert_ne!(left.runtime_dir(), right.runtime_dir());
        assert_ne!(left.data_dir(), right.data_dir());
        assert!(left.data_dir().starts_with(left.runtime_dir()));
        assert!(right.data_dir().starts_with(right.runtime_dir()));
    }

    #[test]
    fn fast_daemon_profile_applies_expected_env() {
        let mut cmd = StdCommand::new("bd");
        configure_std_bd_command(
            &mut cmd,
            Path::new("/tmp/repo"),
            Path::new("/tmp/runtime"),
            Path::new("/tmp/runtime/data"),
            BdCommandProfile::fast_daemon().with_checkpoints(CheckpointMode::Enabled),
        );
        let envs: Vec<(OsString, Option<OsString>)> = cmd
            .get_envs()
            .map(|(key, value)| (key.to_owned(), value.map(OsString::from)))
            .collect();

        assert!(envs.iter().any(|(key, value)| {
            key == OsStr::new("BD_TEST_FAST")
                && value.as_ref().is_some_and(|value| value == OsStr::new("1"))
        }));
        assert!(envs.iter().any(|(key, value)| {
            key == OsStr::new("BD_WAL_SYNC_MODE")
                && value
                    .as_ref()
                    .is_some_and(|value| value == OsStr::new("none"))
        }));
        assert!(envs.iter().any(|(key, value)| {
            key == OsStr::new("BD_TEST_DISABLE_GIT_SYNC")
                && value.as_ref().is_some_and(|value| value == OsStr::new("1"))
        }));
        assert!(envs.iter().any(|(key, value)| {
            key == OsStr::new("BD_TEST_DISABLE_CHECKPOINTS") && value.is_none()
        }));
        assert!(envs.iter().any(|(key, value)| {
            key == OsStr::new("BD_RUNTIME_DIR")
                && value
                    .as_ref()
                    .is_some_and(|value| value == OsStr::new("/tmp/runtime"))
        }));
        assert!(envs.iter().any(|(key, value)| {
            key == OsStr::new("BD_CONFIG_DIR")
                && value
                    .as_ref()
                    .is_some_and(|value| value == OsStr::new("/tmp/runtime/config"))
        }));
    }

    #[test]
    fn daemon_profile_clears_test_only_env() {
        let mut cmd = StdCommand::new("bd");
        cmd.env("BD_TESTING", "1");
        cmd.env("BD_TEST_FAST", "1");
        cmd.env("BD_TEST_DISABLE_GIT_SYNC", "1");
        cmd.env("BD_TEST_DISABLE_CHECKPOINTS", "1");
        cmd.env("BD_WAL_SYNC_MODE", "none");
        configure_std_bd_command(
            &mut cmd,
            Path::new("/tmp/repo"),
            Path::new("/tmp/runtime"),
            Path::new("/tmp/runtime/data"),
            BdCommandProfile::daemon(),
        );
        let envs: Vec<(OsString, Option<OsString>)> = cmd
            .get_envs()
            .map(|(key, value)| (key.to_owned(), value.map(OsString::from)))
            .collect();

        for key in [
            "BD_TESTING",
            "BD_TEST_FAST",
            "BD_TEST_DISABLE_GIT_SYNC",
            "BD_TEST_DISABLE_CHECKPOINTS",
            "BD_WAL_SYNC_MODE",
        ] {
            assert!(
                envs.iter()
                    .any(|(env_key, value)| env_key == OsStr::new(key) && value.is_none()),
                "expected {key} to be removed"
            );
        }
    }

    #[test]
    fn scrub_test_env_removes_ambient_config_and_git_overrides() {
        let mut cmd = StdCommand::new("bd");
        scrub_std_test_env(&mut cmd);
        let envs: Vec<(OsString, Option<OsString>)> = cmd
            .get_envs()
            .map(|(key, value)| (key.to_owned(), value.map(OsString::from)))
            .collect();

        for key in [
            "BD_CONFIG_DIR",
            "BD_RUNTIME_DIR",
            "BD_ACTOR",
            "BD_REPL_LISTEN_ADDR",
            "BD_LOG_DIR",
            "XDG_CONFIG_HOME",
            "XDG_RUNTIME_DIR",
            "GIT_DIR",
            "GIT_WORK_TREE",
        ] {
            assert!(
                envs.iter()
                    .any(|(env_key, value)| { env_key == OsStr::new(key) && value.is_none() })
            );
        }
    }

    #[test]
    fn sync_daemon_child_exit_reaps_owned_child() {
        let repo = BdRuntimeRepo::new_local_only();
        let child = StdCommand::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .expect("spawn test child");
        *repo.daemon_child.lock().expect("daemon child lock") = Some(child);

        repo.sync_daemon_child_exit(Duration::from_secs(1));

        assert!(
            repo.daemon_child
                .lock()
                .expect("daemon child lock")
                .is_none()
        );
    }
}
