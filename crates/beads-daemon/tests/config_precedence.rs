use std::env;
use std::fs;
use std::sync::{LazyLock, Mutex};

use beads_daemon::config::load_for_repo;

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[test]
fn load_for_repo_ignores_user_config_when_repo_config_exists() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let _env_guard = EnvGuard::capture(&[
        "BD_CONFIG_DIR",
        "BD_DATA_DIR",
        "BD_RUNTIME_DIR",
        "BD_REPL_LISTEN_ADDR",
    ]);

    let user_dir = tempfile::tempdir().expect("user config dir");
    let repo_dir = tempfile::tempdir().expect("repo dir");

    let config_dir = user_dir.path().join("beads-rs");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.toml"),
        r#"
[logging]
stdout = false
"#,
    )
    .expect("write user config");
    fs::write(
        repo_dir.path().join("beads.toml"),
        r#"
[replication]
listen_addr = "127.0.0.1:7777"
"#,
    )
    .expect("write repo config");

    unsafe {
        env::set_var("BD_CONFIG_DIR", &config_dir);
    }

    let config = load_for_repo(Some(repo_dir.path())).expect("load config");

    assert_eq!(config.replication.listen_addr, "127.0.0.1:7777");
    assert!(config.logging.stdout);
}

struct EnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvGuard {
    fn capture(keys: &[&'static str]) -> Self {
        let saved = keys
            .iter()
            .map(|key| (*key, env::var(key).ok()))
            .collect::<Vec<_>>();
        for key in keys {
            unsafe {
                env::remove_var(key);
            }
        }
        Self { saved }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (key, value) in &self.saved {
            match value {
                Some(value) => unsafe {
                    env::set_var(key, value);
                },
                None => unsafe {
                    env::remove_var(key);
                },
            }
        }
    }
}
