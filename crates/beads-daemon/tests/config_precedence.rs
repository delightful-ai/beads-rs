use std::fs;

use beads_daemon::config::load_for_repo;
#[path = "support/env.rs"]
mod env_support;

use env_support::ConfigEnvGuard;

#[test]
fn load_for_repo_ignores_user_config_when_repo_config_exists() {
    let _guard = env_support::lock();
    let _env_guard = ConfigEnvGuard::capture();

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
        std::env::set_var("BD_CONFIG_DIR", &config_dir);
    }

    let config = load_for_repo(Some(repo_dir.path())).expect("load config");

    assert_eq!(config.replication.listen_addr, "127.0.0.1:7777");
    assert!(config.logging.stdout);
}

#[test]
fn config_env_guard_clears_all_bootstrap_config_env() {
    let _guard = env_support::lock();

    unsafe {
        std::env::set_var("BD_LOG_STDOUT", "false");
    }
    let _env_guard = ConfigEnvGuard::capture();

    let user_dir = tempfile::tempdir().expect("user config dir");
    unsafe {
        std::env::set_var("BD_CONFIG_DIR", user_dir.path());
    }

    let config = load_for_repo(None).expect("load default config");

    assert!(config.logging.stdout);
}

#[test]
fn load_for_repo_treats_repo_file_as_full_config() {
    let _guard = env_support::lock();
    let _env_guard = ConfigEnvGuard::capture();

    let user_dir = tempfile::tempdir().expect("user config dir");
    let repo_dir = tempfile::tempdir().expect("repo dir");

    unsafe {
        std::env::set_var("BD_CONFIG_DIR", user_dir.path());
    }

    fs::write(
        repo_dir.path().join("beads.toml"),
        r#"
[namespace_defaults.namespaces.core]
persist_to_git = false
replicate_mode = "None"
retention = "Forever"
ready_eligible = true
visibility = "Normal"
gc_authority = "None"
ttl_basis = "LastMutationStamp"

[checkpoint_groups.repo_only]
namespaces = ["core"]
"#,
    )
    .expect("write repo config");

    let config = load_for_repo(Some(repo_dir.path())).expect("load repo config");

    assert_eq!(
        config
            .namespace_defaults
            .namespaces
            .keys()
            .map(beads_core::NamespaceId::as_str)
            .collect::<Vec<_>>(),
        vec!["core"]
    );
    assert_eq!(
        config
            .checkpoint_groups
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["repo_only"]
    );
}
