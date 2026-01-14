#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use thiserror::Error;

use beads_rs::daemon::ipc::{IpcError, MutationMeta, Request, Response, send_request, send_request_no_autostart};
use beads_rs::{BeadType, Priority};

#[derive(Debug, Error)]
pub enum LoadError {
    #[error(transparent)]
    Ipc(#[from] IpcError),
    #[error("remote error: {0:?}")]
    Remote(beads_rs::ErrorPayload),
}

#[derive(Debug, Default, Clone)]
pub struct LoadConfig {
    pub workers: usize,
    pub total_requests: usize,
    pub rate_per_sec: Option<u64>,
    pub namespace: Option<String>,
    pub actor_id: Option<String>,
    pub autostart: bool,
    pub max_errors: usize,
}

#[derive(Debug, Default, Clone)]
pub struct LoadReport {
    pub attempts: usize,
    pub successes: usize,
    pub failures: usize,
    pub errors: Vec<LoadError>,
    pub elapsed: Duration,
}

pub struct LoadGenerator {
    repo: PathBuf,
    config: LoadConfig,
    counter: Arc<AtomicUsize>,
}

impl LoadGenerator {
    pub fn new(repo: PathBuf) -> Self {
        Self {
            repo,
            config: LoadConfig {
                workers: 1,
                total_requests: 1,
                rate_per_sec: None,
                namespace: None,
                actor_id: None,
                autostart: true,
                max_errors: 16,
            },
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn config_mut(&mut self) -> &mut LoadConfig {
        &mut self.config
    }

    pub fn run(&self) -> LoadReport {
        let workers = self.config.workers.max(1);
        let total = self.config.total_requests.max(1);
        let per_worker = (total + workers - 1) / workers;
        let errors = Arc::new(Mutex::new(Vec::new()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let successes = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));

        let started = Instant::now();
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            let repo = self.repo.clone();
            let config = self.config.clone();
            let errors = Arc::clone(&errors);
            let attempts = Arc::clone(&attempts);
            let successes = Arc::clone(&successes);
            let failures = Arc::clone(&failures);
            let counter = Arc::clone(&self.counter);
            handles.push(thread::spawn(move || {
                let interval = config
                    .rate_per_sec
                    .filter(|rate| *rate > 0)
                    .map(|rate| Duration::from_secs_f64(1.0 / rate as f64));
                for i in 0..per_worker {
                    let idx = worker * per_worker + i;
                    if idx >= total {
                        break;
                    }
                    attempts.fetch_add(1, Ordering::Relaxed);
                    let seq = counter.fetch_add(1, Ordering::Relaxed);
                    let title = format!("load-{seq:06}");
                    let request = Request::Create {
                        repo: repo.clone(),
                        id: None,
                        parent: None,
                        title,
                        bead_type: BeadType::Task,
                        priority: Priority::MEDIUM,
                        description: None,
                        design: None,
                        acceptance_criteria: None,
                        assignee: None,
                        external_ref: None,
                        estimated_minutes: None,
                        labels: Vec::new(),
                        dependencies: Vec::new(),
                        meta: MutationMeta {
                            namespace: config.namespace.clone(),
                            durability: None,
                            client_request_id: None,
                            actor_id: config.actor_id.clone(),
                        },
                    };
                    let result = if config.autostart {
                        send_request(&request)
                    } else {
                        send_request_no_autostart(&request)
                    };
                    match result {
                        Ok(Response::Ok { .. }) => {
                            successes.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Response::Err { err }) => {
                            failures.fetch_add(1, Ordering::Relaxed);
                            record_error(&errors, LoadError::Remote(err), config.max_errors);
                        }
                        Err(err) => {
                            failures.fetch_add(1, Ordering::Relaxed);
                            record_error(&errors, LoadError::Ipc(err), config.max_errors);
                        }
                    }
                    if let Some(interval) = interval {
                        thread::sleep(interval);
                    }
                }
            }));
        }

        for handle in handles {
            let _ = handle.join();
        }

        let errors = errors.lock().expect("errors lock").clone();
        LoadReport {
            attempts: attempts.load(Ordering::Relaxed),
            successes: successes.load(Ordering::Relaxed),
            failures: failures.load(Ordering::Relaxed),
            errors,
            elapsed: started.elapsed(),
        }
    }
}

fn record_error(errors: &Mutex<Vec<LoadError>>, error: LoadError, max: usize) {
    let mut guard = errors.lock().expect("errors lock");
    if guard.len() < max {
        guard.push(error);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixtures_load_gen_reports_failures_when_daemon_missing() {
        let temp = tempfile::TempDir::new().expect("temp repo");
        let mut generator = LoadGenerator::new(temp.path().to_path_buf());
        generator.config_mut().autostart = false;
        generator.config_mut().total_requests = 1;
        let report = generator.run();
        assert_eq!(report.attempts, report.successes + report.failures);
        assert!(report.failures > 0);
    }
}
