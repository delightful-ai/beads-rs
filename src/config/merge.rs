use std::path::PathBuf;

use crate::core::ActorId;

use super::{Config, ConfigLayer, LogFormat, LogRotation};

const TEST_FAST_REPL_BACKOFF_BASE_MS: u64 = 25;
const TEST_FAST_REPL_BACKOFF_MAX_MS: u64 = 250;
const TEST_FAST_KEEPALIVE_MS: u64 = 200;
const TEST_FAST_DEAD_MS: u64 = 1_000;
const TEST_FAST_REPL_GAP_TIMEOUT_MS: u64 = 2_000;
const TEST_FAST_CHECKPOINT_DEBOUNCE_MS: u64 = 10;
const TEST_FAST_CHECKPOINT_MAX_INTERVAL_MS: u64 = 50;

pub fn merge_layers(user: Option<ConfigLayer>, repo: Option<ConfigLayer>) -> Config {
    let mut config = Config::default();
    if let Some(layer) = user {
        layer.apply_to(&mut config);
    }
    if let Some(layer) = repo {
        layer.apply_to(&mut config);
    }
    config
}

pub fn apply_env_overrides(config: &mut Config) {
    apply_env_overrides_from(config, |key| std::env::var(key).ok());
}

fn apply_env_overrides_from<F>(config: &mut Config, mut lookup: F)
where
    F: FnMut(&str) -> Option<String>,
{
    if lookup("BD_NO_AUTO_UPGRADE").is_some() {
        config.auto_upgrade = false;
    }

    if let Some(raw) = lookup("BD_ACTOR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            match ActorId::new(trimmed) {
                Ok(actor) => {
                    config.defaults.actor = Some(actor);
                }
                Err(err) => {
                    tracing::warn!("invalid BD_ACTOR, ignoring: {err}");
                }
            }
        }
    }

    if let Some(raw) = lookup("BD_REPL_LISTEN_ADDR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            config.replication.listen_addr = trimmed.to_string();
        }
    }

    if let Some(raw) = lookup("BD_REPL_MAX_CONNECTIONS") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            match trimmed.parse::<usize>() {
                Ok(value) => {
                    config.replication.max_connections = Some(value);
                }
                Err(err) => {
                    tracing::warn!("invalid BD_REPL_MAX_CONNECTIONS, ignoring: {err}");
                }
            }
        }
    }

    if let Some(raw) = lookup("BD_LOG_STDOUT") {
        if let Some(value) = parse_boolish(&raw) {
            config.logging.stdout = value;
        } else {
            tracing::warn!("invalid BD_LOG_STDOUT, ignoring: {raw}");
        }
    }

    if let Some(raw) = lookup("BD_LOG_STDOUT_FORMAT") {
        if let Some(format) = parse_log_format(&raw) {
            config.logging.stdout_format = format;
        } else {
            tracing::warn!("invalid BD_LOG_STDOUT_FORMAT, ignoring: {raw}");
        }
    }

    if let Some(raw) = lookup("BD_LOG_FILTER") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            config.logging.filter = Some(trimmed.to_string());
        }
    }

    if let Some(raw) = lookup("BD_LOG_FILE") {
        if let Some(value) = parse_boolish(&raw) {
            config.logging.file.enabled = value;
        } else {
            tracing::warn!("invalid BD_LOG_FILE, ignoring: {raw}");
        }
    }

    if let Some(raw) = lookup("BD_LOG_DIR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            config.logging.file.dir = Some(PathBuf::from(trimmed));
        }
    }

    if let Some(raw) = lookup("BD_LOG_FILE_FORMAT") {
        if let Some(format) = parse_log_format(&raw) {
            config.logging.file.format = format;
        } else {
            tracing::warn!("invalid BD_LOG_FILE_FORMAT, ignoring: {raw}");
        }
    }

    if let Some(raw) = lookup("BD_LOG_ROTATION") {
        if let Some(rotation) = parse_log_rotation(&raw) {
            config.logging.file.rotation = rotation;
        } else {
            tracing::warn!("invalid BD_LOG_ROTATION, ignoring: {raw}");
        }
    }

    if let Some(raw) = lookup("BD_LOG_RETENTION_DAYS") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            match trimmed.parse::<u64>() {
                Ok(value) => {
                    config.logging.file.retention_max_age_days = Some(value);
                }
                Err(err) => {
                    tracing::warn!("invalid BD_LOG_RETENTION_DAYS, ignoring: {err}");
                }
            }
        }
    }

    if let Some(raw) = lookup("BD_LOG_RETENTION_FILES") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            match trimmed.parse::<usize>() {
                Ok(value) => {
                    config.logging.file.retention_max_files = Some(value);
                }
                Err(err) => {
                    tracing::warn!("invalid BD_LOG_RETENTION_FILES, ignoring: {err}");
                }
            }
        }
    }

    if let Some(raw) = lookup("BD_DATA_DIR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            config.paths.data_dir = Some(PathBuf::from(trimmed));
        }
    }

    if let Some(raw) = lookup("BD_RUNTIME_DIR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            config.paths.runtime_dir = Some(PathBuf::from(trimmed));
        }
    }

    if test_fast_enabled(&mut lookup) {
        apply_test_fast_overrides(config);
    }
}

fn parse_boolish(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

fn test_fast_enabled<F>(lookup: &mut F) -> bool
where
    F: FnMut(&str) -> Option<String>,
{
    let Some(raw) = lookup("BD_TEST_FAST") else {
        return false;
    };
    matches!(parse_boolish(&raw), Some(true))
}

fn clamp_option_u64(value: Option<u64>, max: u64) -> u64 {
    match value {
        Some(value) => value.min(max),
        None => max,
    }
}

fn apply_test_fast_overrides(config: &mut Config) {
    config.replication.backoff_base_ms = config
        .replication
        .backoff_base_ms
        .min(TEST_FAST_REPL_BACKOFF_BASE_MS);
    config.replication.backoff_max_ms = config
        .replication
        .backoff_max_ms
        .min(TEST_FAST_REPL_BACKOFF_MAX_MS);
    if config.replication.backoff_max_ms < config.replication.backoff_base_ms {
        config.replication.backoff_max_ms = config.replication.backoff_base_ms;
    }

    config.limits.keepalive_ms = config.limits.keepalive_ms.min(TEST_FAST_KEEPALIVE_MS);
    config.limits.dead_ms = config.limits.dead_ms.min(TEST_FAST_DEAD_MS);
    if config.limits.dead_ms < config.limits.keepalive_ms {
        config.limits.dead_ms = config.limits.keepalive_ms;
    }
    config.limits.repl_gap_timeout_ms = config
        .limits
        .repl_gap_timeout_ms
        .min(TEST_FAST_REPL_GAP_TIMEOUT_MS);

    for group in config.checkpoint_groups.values_mut() {
        let debounce = clamp_option_u64(group.debounce_ms, TEST_FAST_CHECKPOINT_DEBOUNCE_MS);
        let max_interval =
            clamp_option_u64(group.max_interval_ms, TEST_FAST_CHECKPOINT_MAX_INTERVAL_MS);
        group.debounce_ms = Some(debounce);
        group.max_interval_ms = Some(max_interval.max(debounce));
    }
}

fn parse_log_format(raw: &str) -> Option<LogFormat> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "tree" => Some(LogFormat::Tree),
        "pretty" => Some(LogFormat::Pretty),
        "compact" => Some(LogFormat::Compact),
        "json" => Some(LogFormat::Json),
        _ => None,
    }
}

fn parse_log_rotation(raw: &str) -> Option<LogRotation> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "daily" => Some(LogRotation::Daily),
        "hourly" => Some(LogRotation::Hourly),
        "minutely" => Some(LogRotation::Minutely),
        "never" => Some(LogRotation::Never),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::config::DefaultsConfig;
    use crate::core::NamespaceId;

    #[test]
    fn merge_layers_respects_precedence() {
        let user = ConfigLayer {
            auto_upgrade: Some(false),
            defaults: DefaultsConfig {
                namespace: Some(NamespaceId::parse("wf").unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        let repo = ConfigLayer {
            auto_upgrade: Some(true),
            defaults: DefaultsConfig {
                namespace: Some(NamespaceId::parse("sys").unwrap()),
                ..Default::default()
            },
            ..Default::default()
        };

        let config = merge_layers(Some(user), Some(repo));
        assert!(config.auto_upgrade);
        assert_eq!(
            config.defaults.namespace,
            Some(NamespaceId::parse("sys").unwrap())
        );
    }

    #[test]
    fn env_overrides_apply() {
        let lookup = |key: &str| -> Option<String> {
            match key {
                "BD_NO_AUTO_UPGRADE" => Some("1".to_string()),
                "BD_ACTOR" => Some("alice@example.com".to_string()),
                "BD_REPL_LISTEN_ADDR" => Some("127.0.0.1:9999".to_string()),
                "BD_REPL_MAX_CONNECTIONS" => Some("12".to_string()),
                "BD_LOG_STDOUT" => Some("false".to_string()),
                "BD_LOG_STDOUT_FORMAT" => Some("compact".to_string()),
                "BD_LOG_FILTER" => Some("beads_rs=debug".to_string()),
                "BD_LOG_FILE" => Some("true".to_string()),
                "BD_LOG_DIR" => Some("/tmp/beads-logs".to_string()),
                "BD_LOG_FILE_FORMAT" => Some("json".to_string()),
                "BD_LOG_ROTATION" => Some("hourly".to_string()),
                "BD_LOG_RETENTION_DAYS" => Some("5".to_string()),
                "BD_LOG_RETENTION_FILES" => Some("25".to_string()),
                _ => None,
            }
        };

        let mut config = Config::default();
        apply_env_overrides_from(&mut config, lookup);

        assert!(!config.auto_upgrade);
        assert_eq!(
            config.defaults.actor,
            Some(ActorId::new("alice@example.com").unwrap())
        );
        assert_eq!(config.replication.listen_addr, "127.0.0.1:9999");
        assert_eq!(config.replication.max_connections, Some(12));
        assert!(!config.logging.stdout);
        assert!(matches!(config.logging.stdout_format, LogFormat::Compact));
        assert_eq!(config.logging.filter.as_deref(), Some("beads_rs=debug"));
        assert!(config.logging.file.enabled);
        assert_eq!(
            config.logging.file.dir.as_ref().unwrap().to_string_lossy(),
            "/tmp/beads-logs"
        );
        assert!(matches!(config.logging.file.format, LogFormat::Json));
        assert!(matches!(config.logging.file.rotation, LogRotation::Hourly));
        assert_eq!(config.logging.file.retention_max_age_days, Some(5));
        assert_eq!(config.logging.file.retention_max_files, Some(25));
    }

    #[test]
    fn env_test_fast_overrides_clamp_timers() {
        let lookup = |key: &str| -> Option<String> {
            match key {
                "BD_TEST_FAST" => Some("1".to_string()),
                _ => None,
            }
        };

        let mut config = Config::default();
        config.replication.backoff_base_ms = 10;
        config.replication.backoff_max_ms = 10_000;
        config.limits.keepalive_ms = 100;
        config.limits.dead_ms = 5_000;
        config.limits.repl_gap_timeout_ms = 30_000;
        if let Some(core) = config.checkpoint_groups.get_mut("core") {
            core.debounce_ms = Some(5);
            core.max_interval_ms = Some(20);
        }

        apply_env_overrides_from(&mut config, lookup);

        assert_eq!(config.replication.backoff_base_ms, 10);
        assert_eq!(
            config.replication.backoff_max_ms,
            TEST_FAST_REPL_BACKOFF_MAX_MS
        );
        assert_eq!(config.limits.keepalive_ms, 100);
        assert_eq!(config.limits.dead_ms, TEST_FAST_DEAD_MS);
        assert_eq!(
            config.limits.repl_gap_timeout_ms,
            TEST_FAST_REPL_GAP_TIMEOUT_MS
        );
        let core = config.checkpoint_groups.get("core").expect("core group");
        assert_eq!(core.debounce_ms, Some(5));
        assert_eq!(core.max_interval_ms, Some(20));
    }

    #[test]
    fn env_log_filter_overrides_config() {
        let lookup = |key: &str| -> Option<String> {
            match key {
                "BD_LOG_FILTER" => Some("metrics=info".to_string()),
                _ => None,
            }
        };
        let mut config = Config::default();
        config.logging.filter = Some("beads_rs=debug".to_string());

        apply_env_overrides_from(&mut config, lookup);

        assert_eq!(config.logging.filter.as_deref(), Some("metrics=info"));
    }

    #[test]
    fn env_path_overrides_apply() {
        let lookup = |key: &str| -> Option<String> {
            match key {
                "BD_DATA_DIR" => Some("/tmp/bd-test-data".to_string()),
                "BD_RUNTIME_DIR" => Some("/tmp/bd-test-runtime".to_string()),
                _ => None,
            }
        };

        let mut config = Config::default();
        apply_env_overrides_from(&mut config, lookup);

        assert_eq!(
            config.paths.data_dir,
            Some(PathBuf::from("/tmp/bd-test-data"))
        );
        assert_eq!(
            config.paths.runtime_dir,
            Some(PathBuf::from("/tmp/bd-test-runtime"))
        );
    }
}
