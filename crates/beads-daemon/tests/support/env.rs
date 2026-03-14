use std::env;
use std::sync::{LazyLock, Mutex, MutexGuard};

static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

pub fn lock() -> MutexGuard<'static, ()> {
    ENV_LOCK.lock().expect("env lock")
}

pub struct ConfigEnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl ConfigEnvGuard {
    pub fn capture() -> Self {
        let saved = beads_bootstrap::config::CONFIG_ENV_KEYS
            .iter()
            .map(|key| (*key, env::var(key).ok()))
            .collect::<Vec<_>>();
        for key in beads_bootstrap::config::CONFIG_ENV_KEYS {
            unsafe {
                env::remove_var(key);
            }
        }
        Self { saved }
    }
}

impl Drop for ConfigEnvGuard {
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
