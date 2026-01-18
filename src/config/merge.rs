use std::path::PathBuf;

use crate::core::ActorId;

use super::{Config, ConfigLayer, LogFormat, LogRotation};

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
}

fn parse_boolish(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Some(true),
        "0" | "false" | "no" | "n" | "off" => Some(false),
        _ => None,
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
}
