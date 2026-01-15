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
    if std::env::var("BD_NO_AUTO_UPGRADE").is_ok() {
        config.auto_upgrade = false;
    }

    if let Ok(raw) = std::env::var("BD_ACTOR") {
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

    if let Ok(raw) = std::env::var("BD_REPL_LISTEN_ADDR") {
        let trimmed = raw.trim();
        if !trimmed.is_empty() {
            config.replication.listen_addr = trimmed.to_string();
        }
    }

    if let Ok(raw) = std::env::var("BD_REPL_MAX_CONNECTIONS") {
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

    use std::sync::{Mutex, MutexGuard, OnceLock};

    use crate::core::NamespaceId;

    fn env_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock")
    }

    struct EnvGuard {
        _lock: MutexGuard<'static, ()>,
        prev: Vec<(String, Option<String>)>,
    }

    impl EnvGuard {
        fn new(vars: &[(&str, &str)]) -> Self {
            let lock = env_lock();
            let mut prev = Vec::with_capacity(vars.len());
            for (key, value) in vars {
                let key_string = (*key).to_string();
                let prior = std::env::var(key).ok();
                prev.push((key_string.clone(), prior));
                std::env::set_var(key, value);
            }
            Self { _lock: lock, prev }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.prev.drain(..) {
                match value {
                    Some(val) => std::env::set_var(&key, val),
                    None => std::env::remove_var(&key),
                }
            }
        }
    }

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
        let _guard = EnvGuard::new(&[
            ("BD_NO_AUTO_UPGRADE", "1"),
            ("BD_ACTOR", "alice@example.com"),
            ("BD_REPL_LISTEN_ADDR", "127.0.0.1:9999"),
            ("BD_REPL_MAX_CONNECTIONS", "12"),
        ]);

        let mut config = Config::default();
        apply_env_overrides(&mut config);

        assert!(!config.auto_upgrade);
        assert_eq!(
            config.defaults.actor,
            Some(ActorId::new("alice@example.com").unwrap())
        );
        assert_eq!(config.replication.listen_addr, "127.0.0.1:9999");
        assert_eq!(config.replication.max_connections, Some(12));
    }
}
