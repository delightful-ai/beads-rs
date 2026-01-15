use crate::core::ActorId;

use super::{Config, ConfigLayer};

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
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::NamespaceId;

    #[test]
    fn merge_layers_respects_precedence() {
        let mut user = ConfigLayer::default();
        user.auto_upgrade = Some(false);
        user.defaults.namespace = Some(NamespaceId::parse("wf").unwrap());

        let mut repo = ConfigLayer::default();
        repo.auto_upgrade = Some(true);
        repo.defaults.namespace = Some(NamespaceId::parse("sys").unwrap());

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
    }
}
