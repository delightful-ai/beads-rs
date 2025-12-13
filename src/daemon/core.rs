//! Daemon core - the central coordinator.
//!
//! Owns all per-repo state, the HLC clock, actor identity, and sync scheduler.
//! The serialization point for all mutations - runs on a single thread.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crossbeam::channel::Sender;
use git2::Repository;

use super::git_worker::GitOp;
use super::ipc::{ErrorPayload, Request, Response, ResponsePayload};
use super::ops::OpError;
use super::repo::RepoState;
use super::remote::{normalize_url, RemoteUrl};
use super::scheduler::SyncScheduler;
use super::Clock;
use crate::core::{ActorId, BeadId, CanonicalState};
use crate::git::SyncError;

const REFRESH_TTL: Duration = Duration::from_millis(1000);

/// The daemon coordinator.
///
/// Owns all state and coordinates between IPC, state mutations, and git sync.
pub struct Daemon {
    /// Per-remote state, keyed by normalized remote URL.
    repos: BTreeMap<RemoteUrl, RepoState>,

    /// Cache of repo path → remote URL.
    path_to_remote: HashMap<PathBuf, RemoteUrl>,

    /// HLC clock for generating timestamps.
    clock: Clock,

    /// Actor identity (username@hostname).
    actor: ActorId,

    /// Sync scheduler for debouncing.
    scheduler: SyncScheduler,
}

impl Daemon {
    /// Create a new daemon.
    pub fn new(actor: ActorId, timer_tx: Sender<RemoteUrl>) -> Self {
        Daemon {
            repos: BTreeMap::new(),
            path_to_remote: HashMap::new(),
            clock: Clock::new(),
            actor,
            scheduler: SyncScheduler::new(timer_tx),
        }
    }

    /// Get the actor identity.
    pub fn actor(&self) -> &ActorId {
        &self.actor
    }

    /// Get the clock (for creating stamps).
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Get mutable clock.
    pub fn clock_mut(&mut self) -> &mut Clock {
        &mut self.clock
    }

    /// Get repo state by remote if loaded.
    pub(crate) fn repo_state(&self, remote: &RemoteUrl) -> Option<&RepoState> {
        self.repos.get(remote)
    }

    /// Get mutable repo state by remote if loaded.
    pub(crate) fn repo_state_mut(&mut self, remote: &RemoteUrl) -> Option<&mut RepoState> {
        self.repos.get_mut(remote)
    }

    /// Resolve a repo path to a normalized remote URL.
    fn resolve_remote(&mut self, repo_path: &Path) -> Result<RemoteUrl, OpError> {
        // 1. Env override (highest priority).
        if let Ok(url) = std::env::var("BD_REMOTE_URL") {
            if !url.trim().is_empty() {
                return Ok(RemoteUrl(normalize_url(&url)));
            }
        }

        // 2. Cache.
        if let Some(remote) = self.path_to_remote.get(repo_path) {
            return Ok(remote.clone());
        }

        // 3. Git config.
        let repo = Repository::open(repo_path)
            .map_err(|_| OpError::NotAGitRepo(repo_path.to_owned()))?;
        let remote = repo
            .find_remote("origin")
            .map_err(|_| OpError::NoRemote(repo_path.to_owned()))?;
        let url = remote
            .url()
            .ok_or_else(|| OpError::NoRemote(repo_path.to_owned()))?;

        let remote = RemoteUrl(normalize_url(url));
        self.path_to_remote
            .insert(repo_path.to_owned(), remote.clone());
        Ok(remote)
    }

    /// Ensure repo is loaded, fetching from git if needed.
    ///
    /// This is a blocking operation - sends Load to git thread and waits.
    pub fn ensure_repo_loaded(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<RemoteUrl, OpError> {
        let remote = self.resolve_remote(repo)?;
        self.path_to_remote.insert(repo.to_owned(), remote.clone());

        if !self.repos.contains_key(&remote) {
            // Blocking load from git (fetches remote first in GitWorker).
            let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
            git_tx
                .send(GitOp::Load {
                    repo: repo.to_owned(),
                    respond: respond_tx,
                })
                .map_err(|_| OpError::Internal("git thread not responding"))?;

            match respond_rx.recv() {
                Ok(Ok(state)) => {
                    if let Some(max_stamp) = state.max_write_stamp() {
                        self.clock.receive(&max_stamp);
                    }
                    self.repos.insert(
                        remote.clone(),
                        RepoState::with_state_and_path(state, repo.to_owned()),
                    );
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    return Err(OpError::RepoNotInitialized(repo.to_owned()));
                }
                Ok(Err(e)) => {
                    return Err(OpError::Sync(e));
                }
                Err(_) => {
                    return Err(OpError::Internal("git thread died"));
                }
            }
        } else if let Some(repo_state) = self.repos.get_mut(&remote) {
            repo_state.register_path(repo.to_owned());
        }

        Ok(remote)
    }

    /// Ensure repo is loaded and reasonably fresh from remote.
    ///
    /// For clean (non-dirty) repos, we periodically re-load from git so read-only
    /// commands observe updates pushed by other machines or daemons.
    pub fn ensure_repo_fresh(
        &mut self,
        repo: &Path,
        git_tx: &Sender<GitOp>,
    ) -> Result<RemoteUrl, OpError> {
        let remote = self.ensure_repo_loaded(repo, git_tx)?;

        let needs_refresh = self
            .repo_state(&remote)
            .map(|s| {
                !s.dirty
                    && !s.sync_in_progress
                    && s.last_refresh.map(|t| t.elapsed() >= REFRESH_TTL).unwrap_or(true)
            })
            .unwrap_or(false);

        if needs_refresh {
            let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
            git_tx
                .send(GitOp::Load {
                    repo: repo.to_owned(),
                    respond: respond_tx,
                })
                .map_err(|_| OpError::Internal("git thread not responding"))?;

            match respond_rx.recv() {
                Ok(Ok(state)) => {
                    if let Some(max_stamp) = state.max_write_stamp() {
                        self.clock.receive(&max_stamp);
                    }
                    if let Some(repo_state) = self.repo_state_mut(&remote) {
                        repo_state.state = state;
                        repo_state.last_refresh = Some(Instant::now());
                    }
                }
                Ok(Err(SyncError::NoLocalRef(_))) => {
                    return Err(OpError::RepoNotInitialized(repo.to_owned()));
                }
                Ok(Err(e)) => {
                    return Err(OpError::Sync(e));
                }
                Err(_) => {
                    return Err(OpError::Internal("git thread died"));
                }
            }
        }

        Ok(remote)
    }

    /// Maybe start a background sync for a repo.
    ///
    /// Only starts if:
    /// - Repo is dirty
    /// - Not already syncing
    pub fn maybe_start_sync(&mut self, remote: &RemoteUrl, git_tx: &Sender<GitOp>) {
        let repo_state = match self.repos.get_mut(remote) {
            Some(s) => s,
            None => return,
        };

        if !repo_state.dirty || repo_state.sync_in_progress {
            return;
        }

        let path = match repo_state.any_valid_path() {
            Some(p) => p.clone(),
            None => return,
        };

        repo_state.start_sync();

        let _ = git_tx.send(GitOp::Sync {
            repo: path,
            remote: remote.clone(),
            state: repo_state.state.clone(),
            actor: self.actor.clone(),
        });
    }

    /// Complete a sync operation.
    ///
    /// Called when git thread reports sync result.
    pub fn complete_sync(&mut self, remote: &RemoteUrl, result: Result<CanonicalState, SyncError>) {
        let repo_state = match self.repos.get_mut(remote) {
            Some(s) => s,
            None => return,
        };

        match result {
            Ok(synced_state) => {
                // Advance clock to account for remote stamps
                if let Some(max_stamp) = synced_state.max_write_stamp() {
                    self.clock.receive(&max_stamp);
                }

                // If mutations happened during sync, merge them
                if repo_state.dirty {
                    // Merge local mutations with synced state
                    match CanonicalState::join(&synced_state, &repo_state.state) {
                        Ok(merged) => {
                            repo_state.complete_sync(
                                merged,
                                self.clock.wall_ms(),
                            );
                            // Still dirty from mutations during sync - reschedule
                            repo_state.dirty = true;
                            self.scheduler.schedule(remote.clone());
                        }
                        Err(_) => {
                            // Merge failed - just take synced state, we'll catch up next sync
                            repo_state.complete_sync(
                                synced_state,
                                self.clock.wall_ms(),
                            );
                        }
                    }
                } else {
                    // No mutations during sync - just take synced state
                    repo_state.complete_sync(synced_state, self.clock.wall_ms());
                }
            }
            Err(e) => {
                eprintln!("sync failed for {:?}: {:?}", remote, e);
                repo_state.fail_sync();

                // Exponential backoff
                let backoff = Duration::from_millis(repo_state.backoff_ms());
                self.scheduler.schedule_after(remote.clone(), backoff);
            }
        }
    }

    /// Mark repo as dirty and schedule sync.
    pub fn mark_dirty_and_schedule(&mut self, remote: &RemoteUrl) {
        if let Some(repo_state) = self.repos.get_mut(remote) {
            repo_state.mark_dirty();
            self.scheduler.schedule(remote.clone());
        }
    }

    /// Handle a debounce timer firing.
    ///
    /// Returns true if we should start a sync.
    pub fn handle_timer(&mut self, remote: &RemoteUrl) -> bool {
        self.scheduler.should_fire(remote)
    }

    /// Get iterator over all repos.
    pub fn repos(&self) -> impl Iterator<Item = (&RemoteUrl, &RepoState)> {
        self.repos.iter()
    }

    /// Get mutable iterator over all repos.
    pub fn repos_mut(&mut self) -> impl Iterator<Item = (&RemoteUrl, &mut RepoState)> {
        self.repos.iter_mut()
    }

    /// Handle a request from IPC.
    ///
    /// Dispatches to appropriate handler based on request type.
    pub fn handle_request(&mut self, req: Request, git_tx: &Sender<GitOp>) -> Response {
        match req {
            // Mutations - delegate to executor module
            Request::Create {
                repo,
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
            } => self.apply_create(
                &repo,
                id,
                parent,
                title,
                bead_type,
                priority,
                description,
                design,
                acceptance_criteria,
                assignee,
                external_ref,
                estimated_minutes,
                labels,
                dependencies,
                git_tx,
            ),

            Request::Update {
                repo,
                id,
                patch,
                cas,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_update(&repo, &id, patch, cas, git_tx)
            }

            Request::Close {
                repo,
                id,
                reason,
                on_branch,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_close(&repo, &id, reason, on_branch, git_tx)
            }

            Request::Reopen { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_reopen(&repo, &id, git_tx)
            }

            Request::Delete { repo, id, reason } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_delete(&repo, &id, reason, git_tx)
            }

            Request::AddDep {
                repo,
                from,
                to,
                kind,
            } => {
                let from = match BeadId::parse(&from) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                let to = match BeadId::parse(&to) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_add_dep(&repo, &from, &to, kind, git_tx)
            }

            Request::RemoveDep {
                repo,
                from,
                to,
                kind,
            } => {
                let from = match BeadId::parse(&from) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                let to = match BeadId::parse(&to) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_remove_dep(&repo, &from, &to, kind, git_tx)
            }

            Request::AddNote { repo, id, content } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_add_note(&repo, &id, content, git_tx)
            }

            Request::Claim {
                repo,
                id,
                lease_secs,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_claim(&repo, &id, lease_secs, git_tx)
            }

            Request::Unclaim { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_unclaim(&repo, &id, git_tx)
            }

            Request::ExtendClaim {
                repo,
                id,
                lease_secs,
            } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.apply_extend_claim(&repo, &id, lease_secs, git_tx)
            }

            // Queries - delegate to query_executor module
            Request::Show { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.query_show(&repo, &id, git_tx)
            }

            Request::List { repo, filters } => self.query_list(&repo, &filters, git_tx),

            Request::Ready { repo, limit } => self.query_ready(&repo, limit, git_tx),

            Request::DepTree { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.query_dep_tree(&repo, &id, git_tx)
            }

            Request::Deps { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.query_deps(&repo, &id, git_tx)
            }

            Request::Notes { repo, id } => {
                let id = match BeadId::parse(&id) {
                    Ok(id) => id,
                    Err(e) => return Response::err(error_payload("invalid_id", &e.to_string())),
                };
                self.query_notes(&repo, &id, git_tx)
            }

            Request::Blocked { repo } => self.query_blocked(&repo, git_tx),

            Request::Stale {
                repo,
                days,
                status,
                limit,
            } => self.query_stale(&repo, days, status.as_deref(), limit, git_tx),

            Request::Count {
                repo,
                filters,
                group_by,
            } => self.query_count(&repo, &filters, group_by.as_deref(), git_tx),

            Request::Deleted {
                repo,
                since_ms,
                id,
            } => {
                let id = match id {
                    Some(s) => Some(match BeadId::parse(&s) {
                        Ok(id) => id,
                        Err(e) => {
                            return Response::err(error_payload("invalid_id", &e.to_string()))
                        }
                    }),
                    None => None,
                };
                self.query_deleted(&repo, since_ms, id.as_ref(), git_tx)
            }

            Request::EpicStatus { repo, eligible_only } => {
                self.query_epic_status(&repo, eligible_only, git_tx)
            }

            Request::Status { repo } => self.query_status(&repo, git_tx),

            Request::Validate { repo } => self.query_validate(&repo, git_tx),

            // Control
            Request::Sync { repo } => {
                // Force immediate sync (used for graceful shutdown)
                match self.ensure_repo_loaded(&repo, git_tx) {
                    Ok(remote) => {
                        self.maybe_start_sync(&remote, git_tx);
                        Response::ok(ResponsePayload::Synced)
                    }
                    Err(e) => Response::err(e),
                }
            }

            Request::SyncWait { repo } => {
                // Best-effort: trigger an immediate sync if dirty. The state loop can
                // optionally implement a true barrier by delaying the response.
                match self.ensure_repo_loaded(&repo, git_tx) {
                    Ok(remote) => {
                        self.maybe_start_sync(&remote, git_tx);
                        Response::ok(ResponsePayload::Synced)
                    }
                    Err(e) => Response::err(e),
                }
            }

            Request::Init { repo } => {
                let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
                if git_tx
                    .send(GitOp::Init {
                        repo: repo.clone(),
                        respond: respond_tx,
                    })
                    .is_err()
                {
                    return Response::err(error_payload("internal", "git thread not responding"));
                }

                match respond_rx.recv() {
                    Ok(Ok(())) => {
                        // Load the newly initialized state
                        if self.ensure_repo_loaded(&repo, git_tx).is_ok() {
                            Response::ok(ResponsePayload::Initialized)
                        } else {
                            Response::ok(ResponsePayload::Initialized)
                        }
                    }
                    Ok(Err(e)) => Response::err(error_payload("init_failed", &e.to_string())),
                    Err(_) => Response::err(error_payload("internal", "git thread died")),
                }
            }

            Request::Ping => Response::ok(ResponsePayload::Pong),

            Request::Shutdown => Response::ok(ResponsePayload::ShuttingDown),
        }
    }
}

fn error_payload(code: &str, message: &str) -> ErrorPayload {
    ErrorPayload {
        code: code.to_string(),
        message: message.to_string(),
        details: None,
    }
}
