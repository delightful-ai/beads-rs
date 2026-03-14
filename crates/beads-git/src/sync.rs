//! Sync typestate machine.
//!
//! Implements the git sync protocol with typestate guarantees:
//! - Idle → Fetched → Merged → Committed
//! - Each transition consumes `self`, returns next phase
//! - Can't skip steps - enforced at compile time
//!
//! Key design:
//! - Linear history: always parent remote HEAD, no merge commits
//! - Memory-only local state: no local commits, sync pushes directly
//! - Retry on non-fast-forward: fetch again, re-merge
//! - Meaningful commit messages: "beads(store): +2 created, ~1 updated"

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use git2::{ErrorCode, ObjectType, Oid, Repository, Signature};
use tempfile::TempDir;

use super::error::SyncError;
use super::observe::{NoopSyncObserver, SyncObserver};
use super::wire;
use crate::core::crdt::Crdt;
use crate::core::{BeadId, BeadSlug, CanonicalState, WallClock, WriteStamp};

const BACKUP_REF_PREFIX: &str = "refs/beads/backup/";
const BACKUP_REFS_GLOB: &str = "refs/beads/backup/*";
const MAX_BACKUP_REFS: usize = 64;
const BACKUP_REF_LOCK_STALE_AFTER: Duration = Duration::from_secs(30);
const ENV_TESTING: &str = "BD_TESTING";
const ENV_TEST_MIGRATE_BEFORE_PUSH_WINNER: &str = "BD_TEST_MIGRATE_BEFORE_PUSH_WINNER";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BackupLockPidState {
    Missing,
    Alive,
    Unknown,
}

// =============================================================================
// Phase markers (zero-sized types for typestate)
// =============================================================================

#[derive(Clone, Copy)]
enum FetchPolicy {
    Strict,
    BestEffort,
}

/// Initial phase - ready to start sync.
pub struct Idle;

/// Fetched phase - have remote state.
pub struct Fetched {
    /// Local ref oid (`refs/heads/beads/store`).
    pub local_oid: Oid,
    /// Remote oid (for detecting divergence, may differ from local_oid).
    pub remote_oid: Oid,
    /// Parsed remote state (for CRDT merge).
    pub remote_state: CanonicalState,
    /// Root slug from meta.json (if any).
    pub root_slug: Option<BeadSlug>,
    /// Max write stamp stored in meta.json (if any).
    pub parent_meta_stamp: Option<WriteStamp>,
    /// Fetch error from best-effort mode (if any).
    pub fetch_error: Option<String>,
    /// Divergence detected between local and remote refs (if any).
    pub divergence: Option<DivergenceInfo>,
    /// Force-push detected on remote tracking ref (if any).
    pub force_push: Option<ForcePushInfo>,
}

/// Merged phase - have merged state ready to commit.
pub struct Merged {
    /// Merged state (local + remote).
    pub state: CanonicalState,
    /// Parent oid for commit (local ref, not remote).
    pub parent_oid: Oid,
    /// Diff summary for commit message.
    pub diff: SyncDiff,
    /// Root slug from meta.json (if any).
    pub root_slug: Option<BeadSlug>,
    /// Max write stamp from parent meta.json (if any).
    pub parent_meta_stamp: Option<WriteStamp>,
}

/// Committed phase - have local commit, ready to push.
pub struct Committed {
    /// The commit oid we created.
    pub commit_oid: Oid,
    /// The merged state.
    pub state: CanonicalState,
}

/// Local/remote divergence info (non-linear history).
#[derive(Clone, Debug)]
pub struct DivergenceInfo {
    pub local_oid: Oid,
    pub remote_oid: Oid,
}

/// Remote history rewrite info (force-push).
#[derive(Clone, Debug)]
pub struct ForcePushInfo {
    pub previous_remote_oid: Oid,
    pub remote_oid: Oid,
}

/// Result of a successful sync (state + diagnostics).
#[derive(Clone, Debug)]
pub struct SyncOutcome {
    pub state: CanonicalState,
    pub divergence: Option<DivergenceInfo>,
    pub force_push: Option<ForcePushInfo>,
    pub last_seen_stamp: Option<WriteStamp>,
}

/// A single change tracked for commit message.
#[derive(Clone, Debug)]
pub struct ChangeDetail {
    pub id: String,
    pub title: String,
    pub change_type: ChangeType,
    /// Which fields changed (for updates).
    pub changed_fields: Vec<&'static str>,
}

/// Type of change for a bead.
#[derive(Clone, Debug, PartialEq)]
pub enum ChangeType {
    Created,
    Updated,
    Deleted,
}

/// Summary of changes for commit message.
#[derive(Default, Clone)]
pub struct SyncDiff {
    pub created: usize,
    pub updated: usize,
    pub deleted: usize,
    /// Detailed changes for smart commit messages.
    pub details: Vec<ChangeDetail>,
}

impl SyncDiff {
    const MAX_DETAILED_CHANGES: usize = 5;
    const COMMIT_PREFIX: &'static str = "beads(store):";

    /// Generate a smart commit message with bead details.
    ///
    /// Format:
    /// - Short: "beads(store): +1 created, ~2 updated" (more than 5 changes, or no details)
    /// - Detailed:
    ///   - subject: "beads(store): +1 created, ~1 updated"
    ///   - body: "created bd-xxx \"Title\""
    pub fn to_commit_message(&self) -> String {
        // Use detailed format if we have details and <=MAX_DETAILED_CHANGES.
        let total = self.created + self.updated + self.deleted;
        if !self.details.is_empty() && total <= Self::MAX_DETAILED_CHANGES {
            return self.to_detailed_message();
        }

        // Fallback to count-based format
        self.to_count_message()
    }

    fn summary_parts(&self) -> Vec<String> {
        let mut parts = Vec::new();
        if self.created > 0 {
            parts.push(format!("+{} created", self.created));
        }
        if self.updated > 0 {
            parts.push(format!("~{} updated", self.updated));
        }
        if self.deleted > 0 {
            parts.push(format!("-{} deleted", self.deleted));
        }
        parts
    }

    fn to_count_message(&self) -> String {
        let parts = self.summary_parts();
        if parts.is_empty() {
            format!("{} no changes", Self::COMMIT_PREFIX)
        } else {
            format!("{} {}", Self::COMMIT_PREFIX, parts.join(", "))
        }
    }

    fn to_detailed_message(&self) -> String {
        let subject = self.to_count_message();
        let parts: Vec<String> = self
            .details
            .iter()
            .map(|d| {
                // Truncate title to keep message reasonable
                let title = if d.title.len() > 40 {
                    format!("{}...", &d.title[..37])
                } else {
                    d.title.clone()
                };

                match d.change_type {
                    ChangeType::Created => format!("created {}: \"{}\"", d.id, title),
                    ChangeType::Deleted => format!("deleted {}: \"{}\"", d.id, title),
                    ChangeType::Updated => {
                        if d.changed_fields.is_empty() {
                            format!("updated {}: \"{}\"", d.id, title)
                        } else {
                            format!(
                                "updated {} [{}]: \"{}\"",
                                d.id,
                                d.changed_fields.join(", "),
                                title
                            )
                        }
                    }
                }
            })
            .collect();

        if parts.is_empty() {
            subject
        } else {
            format!("{subject}\n\n{}", parts.join("\n"))
        }
    }
}

// =============================================================================
// SyncProcess - the typestate machine
// =============================================================================

/// Sync configuration (mostly set from environment).
#[derive(Clone, Debug, Default)]
pub struct SyncConfig {
    pub tombstone_ttl_ms: Option<u64>,
}

impl SyncConfig {
    const MIN_TOMBSTONE_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

    pub fn from_env() -> Self {
        let Ok(raw) = std::env::var("BD_TOMBSTONE_TTL_MS") else {
            return Self::default();
        };
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Self::default();
        }
        let tombstone_ttl_ms = match trimmed.parse::<u64>() {
            Ok(ms) if ms < Self::MIN_TOMBSTONE_TTL_MS => {
                tracing::warn!(
                    raw = trimmed,
                    min_ms = Self::MIN_TOMBSTONE_TTL_MS,
                    "BD_TOMBSTONE_TTL_MS below minimum; clamping to min"
                );
                Some(Self::MIN_TOMBSTONE_TTL_MS)
            }
            Ok(ms) => Some(ms),
            Err(err) => {
                tracing::warn!(
                    raw = trimmed,
                    error = %err,
                    "invalid BD_TOMBSTONE_TTL_MS; ignoring"
                );
                None
            }
        };
        Self { tombstone_ttl_ms }
    }
}

/// Sync process with typestate-enforced phases.
///
/// Use `SyncProcess::new()` to start, then chain transitions:
/// ```ignore
/// let synced = SyncProcess::new(repo_path)
///     .fetch(&repo)?
///     .merge(&local_state)?
///     .commit(&repo)?
///     .push(&repo)?;
/// ```
pub struct SyncProcess<Phase> {
    pub repo_path: PathBuf,
    pub config: SyncConfig,
    pub observer: Arc<dyn SyncObserver>,
    pub phase: Phase,
}

impl SyncProcess<Idle> {
    /// Create a new sync process in Idle phase.
    pub fn new(repo_path: PathBuf) -> Self {
        Self::new_with_config_and_observer(
            repo_path,
            SyncConfig::from_env(),
            Arc::new(NoopSyncObserver),
        )
    }

    pub fn new_with_observer(repo_path: PathBuf, observer: Arc<dyn SyncObserver>) -> Self {
        Self::new_with_config_and_observer(repo_path, SyncConfig::from_env(), observer)
    }

    pub fn new_with_config(repo_path: PathBuf, config: SyncConfig) -> Self {
        Self::new_with_config_and_observer(repo_path, config, Arc::new(NoopSyncObserver))
    }

    pub fn new_with_config_and_observer(
        repo_path: PathBuf,
        config: SyncConfig,
        observer: Arc<dyn SyncObserver>,
    ) -> Self {
        SyncProcess {
            repo_path,
            config,
            observer,
            phase: Idle,
        }
    }

    /// Fetch from remote, transition to Fetched phase.
    ///
    /// If no remote ref exists, creates an empty initial state.
    pub fn fetch(self, repo: &Repository) -> Result<SyncProcess<Fetched>, SyncError> {
        self.fetch_inner(repo, FetchPolicy::Strict)
    }

    /// Best-effort fetch (allows offline/local-only operation).
    pub fn fetch_best_effort(self, repo: &Repository) -> Result<SyncProcess<Fetched>, SyncError> {
        self.fetch_inner(repo, FetchPolicy::BestEffort)
    }

    fn fetch_inner(
        self,
        repo: &Repository,
        policy: FetchPolicy,
    ) -> Result<SyncProcess<Fetched>, SyncError> {
        let SyncProcess {
            repo_path,
            config,
            observer,
            phase: _,
        } = self;
        let fetch_started = Instant::now();
        observer.on_fetch_start();
        // Keep backup refs bounded even when this fetch path does not create a new backup.
        prune_backup_refs_with_observer(repo, MAX_BACKUP_REFS, Oid::zero(), observer.as_ref())?;
        let mut fetch_error = None;
        let prev_remote_oid_opt = refname_to_id_optional(repo, "refs/remotes/origin/beads/store")?;
        // Fetch from origin
        if let Ok(mut remote) = repo.find_remote("origin") {
            let cfg = repo.config().ok();
            let mut callbacks = git2::RemoteCallbacks::new();
            callbacks.credentials(move |url, username_from_url, allowed| {
                if allowed.is_ssh_key()
                    && let Some(user) = username_from_url
                {
                    return git2::Cred::ssh_key_from_agent(user);
                }
                if allowed.is_user_pass_plaintext()
                    && let Some(ref cfg) = cfg
                    && let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
                {
                    return Ok(cred);
                }
                git2::Cred::default()
            });
            let mut fo = git2::FetchOptions::new();
            fo.remote_callbacks(callbacks);
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            if let Err(e) = remote.fetch(&[refspec], Some(&mut fo), None) {
                match policy {
                    FetchPolicy::Strict => return Err(SyncError::Fetch(e)),
                    FetchPolicy::BestEffort => {
                        fetch_error = Some(e.to_string());
                        tracing::warn!("beads fetch failed (best-effort): {}", e);
                    }
                }
            }
        }

        let local_oid_opt = refname_to_id_optional(repo, "refs/heads/beads/store")?;
        let remote_oid_opt = refname_to_id_optional(repo, "refs/remotes/origin/beads/store")?;
        let mut force_push = None;
        if let (Some(prev_remote_oid), Some(remote_oid)) = (prev_remote_oid_opt, remote_oid_opt)
            && prev_remote_oid != remote_oid
        {
            let remote_is_descendant = repo
                .graph_descendant_of(remote_oid, prev_remote_oid)
                .map_err(SyncError::MergeBase)?;
            if !remote_is_descendant {
                force_push = Some(ForcePushInfo {
                    previous_remote_oid: prev_remote_oid,
                    remote_oid,
                });
            }
        }

        // Get remote state for CRDT merge
        let (remote_oid, remote_state, root_slug, parent_meta_stamp) = match remote_oid_opt {
            Some(oid) => {
                let loaded = read_state_at_oid(repo, oid)?;
                (
                    oid,
                    loaded.state,
                    loaded.meta.root_slug().cloned(),
                    loaded.meta.last_write_stamp().cloned(),
                )
            }
            None => match local_oid_opt {
                Some(local_oid) => {
                    let loaded = read_state_at_oid(repo, local_oid)?;
                    (
                        local_oid,
                        loaded.state,
                        loaded.meta.root_slug().cloned(),
                        loaded.meta.last_write_stamp().cloned(),
                    )
                }
                None => (Oid::zero(), CanonicalState::new(), None, None),
            },
        };

        // Fast-forward local ref to match remote only when it's safe.
        let mut divergence = None;
        if let Some(remote_oid) = remote_oid_opt {
            match local_oid_opt {
                Some(local_oid) => {
                    if local_oid != remote_oid {
                        let remote_is_descendant = repo
                            .graph_descendant_of(remote_oid, local_oid)
                            .map_err(SyncError::MergeBase)?;
                        if remote_is_descendant {
                            update_ref(
                                repo,
                                "refs/heads/beads/store",
                                remote_oid,
                                "beads sync: fast-forward to remote",
                            )?;
                        } else {
                            let local_is_descendant = repo
                                .graph_descendant_of(local_oid, remote_oid)
                                .map_err(SyncError::MergeBase)?;
                            if !local_is_descendant {
                                divergence = Some(DivergenceInfo {
                                    local_oid,
                                    remote_oid,
                                });
                            }
                            ensure_backup_ref_with_observer(repo, local_oid, observer.as_ref())?;
                        }
                    }
                }
                None => {
                    repo.reference(
                        "refs/heads/beads/store",
                        remote_oid,
                        true,
                        "beads sync: init local from remote",
                    )?;
                }
            }
        }

        let local_oid =
            refname_to_id_optional(repo, "refs/heads/beads/store")?.unwrap_or(Oid::zero());
        let fetch_elapsed_ms = fetch_started.elapsed().as_millis();
        observer.on_fetch_end(u64::try_from(fetch_elapsed_ms).unwrap_or(u64::MAX));

        Ok(SyncProcess {
            repo_path,
            config,
            observer,
            phase: Fetched {
                local_oid,
                remote_oid,
                remote_state,
                root_slug,
                parent_meta_stamp,
                fetch_error,
                divergence,
                force_push,
            },
        })
    }
}

impl SyncProcess<Fetched> {
    /// Merge local state with remote, transition to Merged phase.
    ///
    /// Handles:
    /// - CRDT pairwise merge (no base needed)
    /// - Resurrection rule (bead beats tombstone if newer)
    pub fn merge(self, local_state: &CanonicalState) -> Result<SyncProcess<Merged>, SyncError> {
        let SyncProcess {
            repo_path,
            config,
            observer,
            phase:
                Fetched {
                    local_oid,
                    remote_oid,
                    remote_state,
                    root_slug,
                    parent_meta_stamp,
                    ..
                },
        } = self;
        observer.on_merge_start();

        let parent_oid = if remote_oid.is_zero() {
            local_oid
        } else {
            remote_oid
        };

        // Fast path: if remote is empty/uninitialized, just use local
        if remote_oid.is_zero() {
            let diff = compute_diff(&CanonicalState::new(), local_state);
            let merged_count = diff.created + diff.updated + diff.deleted;
            observer.on_merge_end(merged_count);
            return Ok(SyncProcess {
                repo_path,
                config,
                observer,
                phase: Merged {
                    state: local_state.clone(),
                    parent_oid,
                    diff,
                    root_slug,
                    parent_meta_stamp,
                },
            });
        }

        // Pairwise CRDT merge
        let mut merged = local_state.join(&remote_state);

        // Garbage collect tombstones if configured.
        if let Some(ttl_ms) = config.tombstone_ttl_ms {
            let removed = merged.gc_tombstones(ttl_ms, WallClock::now());
            if removed > 0 {
                tracing::info!(removed, ttl_ms, "tombstone GC pruned records");
            }
        }

        // Compute diff for commit message
        let diff = compute_diff(&remote_state, &merged);
        let merged_count = diff.created + diff.updated + diff.deleted;
        observer.on_merge_end(merged_count);

        Ok(SyncProcess {
            repo_path,
            config,
            observer,
            phase: Merged {
                state: merged,
                parent_oid,
                diff,
                root_slug,
                parent_meta_stamp,
            },
        })
    }
}

impl SyncProcess<Merged> {
    /// Commit the merged state, transition to Committed phase.
    ///
    /// Creates a commit with:
    /// - Single parent (remote HEAD) - keeps history linear
    /// - Meaningful commit message based on diff
    pub fn commit(self, repo: &Repository) -> Result<SyncProcess<Committed>, SyncError> {
        let SyncProcess {
            repo_path,
            config,
            observer,
            phase:
                Merged {
                    state,
                    parent_oid,
                    diff,
                    root_slug,
                    parent_meta_stamp,
                    ..
                },
        } = self;
        let message = diff.to_commit_message();
        let persisted_meta_stamp =
            max_write_stamp(state.max_write_stamp(), parent_meta_stamp.clone());
        let commit_oid = commit_store_snapshot(
            repo,
            parent_oid,
            &state,
            root_slug.as_ref(),
            persisted_meta_stamp.as_ref(),
            &message,
        )?;

        Ok(SyncProcess {
            repo_path,
            config,
            observer,
            phase: Committed { commit_oid, state },
        })
    }
}

impl SyncProcess<Committed> {
    /// Push to remote, completing the sync.
    ///
    /// Returns the synced state on success.
    /// Returns NonFastForward error if remote moved - caller should retry.
    /// If no remote is configured, returns success (local-only repo).
    pub fn push(self, repo: &Repository) -> Result<CanonicalState, SyncError> {
        let SyncProcess {
            observer, phase, ..
        } = self;
        let Committed {
            commit_oid: _,
            state,
        } = phase;
        let push_started = Instant::now();
        observer.on_push_start();

        // Try to find remote - if no remote, just return success (local-only)
        let mut remote = match repo.find_remote("origin") {
            Ok(r) => r,
            Err(_) => {
                // No remote configured - this is a local-only repo
                // Commit was already made, so just return success
                let push_elapsed_ms = push_started.elapsed().as_millis();
                observer.on_push_end(u64::try_from(push_elapsed_ms).unwrap_or(u64::MAX));
                return Ok(state);
            }
        };

        // Push with refspec
        let refspec = "refs/heads/beads/store:refs/heads/beads/store";

        // Try push - use RefCell for interior mutability in callback
        use std::cell::RefCell;
        let push_error: RefCell<Option<String>> = RefCell::new(None);

        {
            let cfg = repo.config().ok();
            let mut callbacks = git2::RemoteCallbacks::new();
            callbacks.credentials(move |url, username_from_url, allowed| {
                if allowed.is_ssh_key()
                    && let Some(user) = username_from_url
                {
                    return git2::Cred::ssh_key_from_agent(user);
                }
                if allowed.is_user_pass_plaintext()
                    && let Some(ref cfg) = cfg
                    && let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
                {
                    return Ok(cred);
                }
                git2::Cred::default()
            });
            callbacks.push_update_reference(|_ref_name, status| {
                if let Some(msg) = status {
                    *push_error.borrow_mut() = Some(msg.to_string());
                }
                Ok(())
            });

            let mut push_options = git2::PushOptions::new();
            push_options.remote_callbacks(callbacks);

            if let Err(e) = remote.push(&[refspec], Some(&mut push_options)) {
                let msg = e.to_string();
                if is_retryable_push_error_message(&msg) {
                    return Err(SyncError::NonFastForward);
                }
                return Err(SyncError::from(e));
            }
        }

        if let Some(err) = push_error.into_inner() {
            if is_retryable_push_error_message(&err) {
                return Err(SyncError::NonFastForward);
            }
            return Err(crate::error::PushRejected { message: err }.into());
        }

        let push_elapsed_ms = push_started.elapsed().as_millis();
        observer.on_push_end(u64::try_from(push_elapsed_ms).unwrap_or(u64::MAX));
        Ok(state)
    }

    /// Get the commit oid.
    pub fn commit_oid(&self) -> Oid {
        self.phase.commit_oid
    }

    /// Get the synced state without pushing.
    ///
    /// Used when push isn't needed (e.g., local-only testing).
    pub fn into_state(self) -> CanonicalState {
        self.phase.state
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Data loaded from a beads store commit.
pub struct LoadedStore {
    pub state: CanonicalState,
    pub meta: wire::SupportedStoreMeta,
}

/// Data loaded from a beads store commit via migration-aware parsing.
pub struct LoadedStoreMigration {
    pub state: CanonicalState,
    pub meta: wire::SupportedStoreMeta,
    pub deps_format: wire::DepsFormat,
    pub warnings: Vec<String>,
    pub notes_present: bool,
}

pub struct RemoteStorePreview {
    _tempdir: TempDir,
    repo: Repository,
    oid: Oid,
}

impl RemoteStorePreview {
    pub fn repo(&self) -> &Repository {
        &self.repo
    }

    pub fn oid(&self) -> Oid {
        self.oid
    }
}

/// Load state and metadata from the current `refs/heads/beads/store` ref.
pub fn load_store(repo: &Repository) -> Result<LoadedStore, SyncError> {
    let oid = repo
        .refname_to_id("refs/heads/beads/store")
        .map_err(|_| SyncError::NoLocalRef("refs/heads/beads/store".into()))?;
    read_state_at_oid(repo, oid)
}

/// Load only the canonical state from the current `refs/heads/beads/store` ref.
pub fn load_state(repo: &Repository) -> Result<CanonicalState, SyncError> {
    Ok(load_store(repo)?.state)
}
/// Read state from a commit oid.
pub fn read_state_at_oid(repo: &Repository, oid: Oid) -> Result<LoadedStore, SyncError> {
    let commit = repo.find_commit(oid)?;
    let tree = commit.tree()?;

    // Read state.jsonl
    let state_entry = tree
        .get_name("state.jsonl")
        .ok_or(SyncError::MissingFile("state.jsonl".into()))?;
    let state_blob = repo
        .find_object(state_entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;

    // Read tombstones.jsonl
    let tombs_entry = tree
        .get_name("tombstones.jsonl")
        .ok_or(SyncError::MissingFile("tombstones.jsonl".into()))?;
    let tombs_blob = repo
        .find_object(tombs_entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;

    // Read deps.jsonl
    let deps_entry = tree
        .get_name("deps.jsonl")
        .ok_or(SyncError::MissingFile("deps.jsonl".into()))?;
    let deps_blob = repo
        .find_object(deps_entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;

    // Read notes.jsonl (optional in legacy format)
    let notes_bytes = if let Some(notes_entry) = tree.get_name("notes.jsonl") {
        let notes_blob = repo
            .find_object(notes_entry.id(), Some(ObjectType::Blob))?
            .peel_to_blob()?;
        notes_blob.content().to_vec()
    } else {
        Vec::new()
    };

    // Read meta.json for root_slug + last_write_stamp + checksums
    let meta = if let Some(meta_entry) = tree.get_name("meta.json") {
        let meta_blob = repo
            .find_object(meta_entry.id(), Some(ObjectType::Blob))?
            .peel_to_blob()?;
        wire::parse_supported_meta(meta_blob.content())?
    } else {
        wire::SupportedStoreMeta::legacy()
    };
    let checksums = meta.checksums();

    if let Some(expected) = checksums {
        wire::verify_store_checksums(
            expected,
            state_blob.content(),
            tombs_blob.content(),
            deps_blob.content(),
            &notes_bytes,
        )?;
    }

    let state = wire::parse_legacy_state(
        state_blob.content(),
        tombs_blob.content(),
        deps_blob.content(),
        &notes_bytes,
    )?;

    Ok(LoadedStore { state, meta })
}

/// Read state from a commit oid using migration-aware deps parsing.
///
/// Runtime strict loads should continue to use `read_state_at_oid`.
pub fn read_state_at_oid_for_migration(
    repo: &Repository,
    oid: Oid,
) -> Result<LoadedStoreMigration, SyncError> {
    let commit = repo.find_commit(oid)?;
    let tree = commit.tree()?;

    let state_bytes = read_required_tree_blob(repo, &tree, "state.jsonl")?;
    let tombs_bytes = read_required_tree_blob(repo, &tree, "tombstones.jsonl")?;
    let deps_bytes = read_required_tree_blob(repo, &tree, "deps.jsonl")?;
    let notes_bytes = read_optional_tree_blob(repo, &tree, "notes.jsonl")?.unwrap_or_default();
    let notes_present = tree.get_name("notes.jsonl").is_some();

    let meta = if let Some(meta_bytes) = read_optional_tree_blob(repo, &tree, "meta.json")? {
        wire::parse_supported_meta(&meta_bytes)?
    } else {
        wire::SupportedStoreMeta::legacy()
    };

    if let Some(expected) = meta.checksums() {
        wire::verify_store_checksums(
            expected,
            &state_bytes,
            &tombs_bytes,
            &deps_bytes,
            &notes_bytes,
        )?;
    }

    let (state, deps_format, warnings) =
        wire::parse_state_allow_legacy_deps(&state_bytes, &tombs_bytes, &deps_bytes, &notes_bytes)?;

    Ok(LoadedStoreMigration {
        state,
        meta,
        deps_format,
        warnings,
        notes_present,
    })
}

fn read_required_tree_blob(
    repo: &Repository,
    tree: &git2::Tree<'_>,
    name: &'static str,
) -> Result<Vec<u8>, SyncError> {
    let entry = tree
        .get_name(name)
        .ok_or_else(|| SyncError::MissingFile(name.into()))?;
    let blob = repo
        .find_object(entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;
    Ok(blob.content().to_vec())
}

fn read_optional_tree_blob(
    repo: &Repository,
    tree: &git2::Tree<'_>,
    name: &'static str,
) -> Result<Option<Vec<u8>>, SyncError> {
    let Some(entry) = tree.get_name(name) else {
        return Ok(None);
    };
    let blob = repo
        .find_object(entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;
    Ok(Some(blob.content().to_vec()))
}

/// Compute diff between two states for commit message.
fn compute_diff(before: &CanonicalState, after: &CanonicalState) -> SyncDiff {
    let mut diff = SyncDiff::default();
    let deps_changed = dep_change_ids(before, after);

    // Count created and updated
    for (id, bead) in after.iter_live() {
        let dep_changed = deps_changed.contains(id);
        match before.get_live(id) {
            None => {
                diff.created += 1;
                diff.details.push(ChangeDetail {
                    id: id.to_string(),
                    title: bead.title().to_string(),
                    change_type: ChangeType::Created,
                    changed_fields: Vec::new(),
                });
            }
            Some(old_bead) => {
                let field_changed = before.updated_stamp_for(id) != after.updated_stamp_for(id);
                if field_changed || dep_changed {
                    diff.updated += 1;
                    let mut changed_fields = if field_changed {
                        detect_changed_fields(before, after, id, old_bead, bead)
                    } else {
                        Vec::new()
                    };
                    if dep_changed && !changed_fields.contains(&"deps") {
                        changed_fields.push("deps");
                    }
                    diff.details.push(ChangeDetail {
                        id: id.to_string(),
                        title: bead.title().to_string(),
                        change_type: ChangeType::Updated,
                        changed_fields,
                    });
                }
            }
        }
    }

    // Count deleted (in before.live but not in after.live)
    for (id, bead) in before.iter_live() {
        if after.get_live(id).is_none() {
            diff.deleted += 1;
            diff.details.push(ChangeDetail {
                id: id.to_string(),
                title: bead.title().to_string(),
                change_type: ChangeType::Deleted,
                changed_fields: Vec::new(),
            });
        }
    }

    diff
}

fn dep_change_ids(before: &CanonicalState, after: &CanonicalState) -> BTreeSet<BeadId> {
    let before_active: BTreeSet<_> = before.dep_store().values().cloned().collect();
    let after_active: BTreeSet<_> = after.dep_store().values().cloned().collect();

    before_active
        .symmetric_difference(&after_active)
        .map(|key| key.from().clone())
        .collect()
}

fn max_write_stamp(a: Option<WriteStamp>, b: Option<WriteStamp>) -> Option<WriteStamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

/// Detect which fields changed between two beads by comparing LWW stamps.
fn detect_changed_fields(
    before: &CanonicalState,
    after: &CanonicalState,
    id: &BeadId,
    old: &crate::core::Bead,
    new: &crate::core::Bead,
) -> Vec<&'static str> {
    let mut changed = Vec::new();

    if old.fields.title.stamp != new.fields.title.stamp {
        changed.push("title");
    }
    if old.fields.description.stamp != new.fields.description.stamp {
        changed.push("desc");
    }
    if old.fields.design.stamp != new.fields.design.stamp {
        changed.push("design");
    }
    if old.fields.acceptance_criteria.stamp != new.fields.acceptance_criteria.stamp {
        changed.push("acceptance");
    }
    if old.fields.priority.stamp != new.fields.priority.stamp {
        changed.push("priority");
    }
    if old.fields.bead_type.stamp != new.fields.bead_type.stamp {
        changed.push("type");
    }
    if old.fields.external_ref.stamp != new.fields.external_ref.stamp {
        changed.push("external_ref");
    }
    if old.fields.source_repo.stamp != new.fields.source_repo.stamp {
        changed.push("source_repo");
    }
    if old.fields.estimated_minutes.stamp != new.fields.estimated_minutes.stamp {
        changed.push("estimate");
    }
    if old.fields.workflow.stamp != new.fields.workflow.stamp {
        changed.push("status");
    }
    if old.fields.claim.stamp != new.fields.claim.stamp {
        changed.push("claim");
    }
    let before_labels = before.labels_for(id);
    let after_labels = after.labels_for(id);
    if before_labels != after_labels || before.label_stamp(id) != after.label_stamp(id) {
        changed.push("labels");
    }

    // Check notes (compare note counts as a simple heuristic)
    if before.notes_for(id).len() != after.notes_for(id).len() {
        changed.push("notes");
    }

    changed
}

/// Run a full sync cycle with retry on non-fast-forward.
///
/// This is the main entry point for syncing - handles the retry loop.
pub fn sync_with_retry(
    repo: &Repository,
    repo_path: &Path,
    local_state: &CanonicalState,
    max_retries: usize,
) -> Result<SyncOutcome, SyncError> {
    sync_with_retry_with_observer(
        repo,
        repo_path,
        local_state,
        max_retries,
        Arc::new(NoopSyncObserver),
    )
}

pub fn sync_with_retry_with_observer(
    repo: &Repository,
    repo_path: &Path,
    local_state: &CanonicalState,
    max_retries: usize,
    observer: Arc<dyn SyncObserver>,
) -> Result<SyncOutcome, SyncError> {
    let mut retries = 0;
    let mut force_push = None;
    let started = Instant::now();

    loop {
        let attempt = retries + 1;
        tracing::debug!(attempt, "sync attempt");
        let fetched =
            SyncProcess::new_with_observer(repo_path.to_owned(), observer.clone()).fetch(repo)?;
        let divergence = fetched.phase.divergence.clone();
        if force_push.is_none() {
            force_push = fetched.phase.force_push.clone();
        }
        let parent_meta_stamp = fetched.phase.parent_meta_stamp.clone();
        let result = fetched.merge(local_state)?.commit(repo)?.push(repo);

        match result {
            Ok(state) => {
                let elapsed_ms = started.elapsed().as_millis();
                tracing::info!(elapsed_ms, retries, "sync succeeded");
                let last_seen_stamp = max_write_stamp(state.max_write_stamp(), parent_meta_stamp);
                return Ok(SyncOutcome {
                    state,
                    divergence,
                    force_push,
                    last_seen_stamp,
                });
            }
            Err(SyncError::NonFastForward) => {
                retries += 1;
                if retries > max_retries {
                    let elapsed_ms = started.elapsed().as_millis();
                    tracing::warn!(elapsed_ms, retries, "sync retry limit exceeded");
                    return Err(SyncError::TooManyRetries(retries));
                }
                // Loop will fetch again and re-merge
                continue;
            }
            Err(e) => {
                let elapsed_ms = started.elapsed().as_millis();
                tracing::warn!(elapsed_ms, retries, error = ?e, "sync failed");
                return Err(e);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigratePushDisposition {
    Pushed,
    SkippedNoPush,
    SkippedNoRemote,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateStoreToV1Outcome {
    pub from_effective_version: u32,
    pub deps_format_before: wire::DepsFormat,
    pub converted_deps: bool,
    pub added_notes_file: bool,
    pub wrote_checksums: bool,
    pub commit_oid: Option<Oid>,
    pub push: MigratePushDisposition,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MigrationSourceSummary {
    from_effective_version: u32,
    deps_format_before: wire::DepsFormat,
    converted_deps: bool,
    added_notes_file: bool,
    wrote_checksums: bool,
    warnings: Vec<String>,
}

struct ResolvedMigrationInputs {
    merged_state: CanonicalState,
    root_slug: BeadSlug,
    parent_meta_stamp: Option<WriteStamp>,
    summary: MigrationSourceSummary,
}

/// Rewrite `refs/heads/beads/store` to canonical v1 shape.
///
/// This is migration-only and may parse legacy deps. Runtime strict load paths
/// remain unchanged.
pub fn migrate_store_ref_to_v1(
    repo: &Repository,
    repo_path: &Path,
    dry_run: bool,
    force: bool,
    no_push: bool,
    max_retries: usize,
) -> Result<MigrateStoreToV1Outcome, SyncError> {
    if dry_run {
        return preview_migrate_store_ref_to_v1(repo, force);
    }
    migrate_store_ref_to_v1_with_before_push(
        repo,
        repo_path,
        dry_run,
        force,
        no_push,
        max_retries,
        |retries, _| apply_test_migrate_before_push_hook(repo, retries),
    )
}

#[doc(hidden)]
pub fn migrate_store_ref_to_v1_with_before_push_for_testing<F>(
    repo: &Repository,
    repo_path: &Path,
    dry_run: bool,
    force: bool,
    no_push: bool,
    max_retries: usize,
    before_push: F,
) -> Result<MigrateStoreToV1Outcome, SyncError>
where
    F: FnMut(usize, Oid) -> Result<(), SyncError>,
{
    migrate_store_ref_to_v1_with_before_push(
        repo,
        repo_path,
        dry_run,
        force,
        no_push,
        max_retries,
        before_push,
    )
}

fn migrate_store_ref_to_v1_with_before_push<F>(
    repo: &Repository,
    _repo_path: &Path,
    dry_run: bool,
    force: bool,
    no_push: bool,
    max_retries: usize,
    mut before_push: F,
) -> Result<MigrateStoreToV1Outcome, SyncError>
where
    F: FnMut(usize, Oid) -> Result<(), SyncError>,
{
    let initial_local_oid_opt = refname_to_id_optional(repo, "refs/heads/beads/store")?;
    let mut source_summary: Option<MigrationSourceSummary> = None;
    let mut retries = 0usize;

    loop {
        let local_oid_before_fetch = refname_to_id_optional(repo, "refs/heads/beads/store")?;
        if !no_push || local_oid_before_fetch.is_none() {
            fetch_store_ref(repo)?;
        }

        let local_oid_opt = refname_to_id_optional(repo, "refs/heads/beads/store")?;
        let remote_oid_opt = refname_to_id_optional(repo, "refs/remotes/origin/beads/store")?;

        if local_oid_opt.is_none() && remote_oid_opt.is_none() {
            return Err(SyncError::NoLocalRef("refs/heads/beads/store".into()));
        }

        if let (Some(local_oid), Some(remote_oid)) = (local_oid_opt, remote_oid_opt)
            && local_oid != remote_oid
            && should_refuse_migration_divergence(
                force,
                has_common_ancestor(repo, local_oid, remote_oid)?,
            )
        {
            restore_local_store_ref(repo, initial_local_oid_opt, local_oid_opt)?;
            return Err(SyncError::NoCommonAncestor);
        }

        let local_loaded = match local_oid_opt {
            Some(oid) => Some(read_state_at_oid_for_migration(repo, oid)?),
            None => None,
        };
        let remote_loaded = match remote_oid_opt {
            Some(oid) => Some(read_state_at_oid_for_migration(repo, oid)?),
            None => None,
        };

        let parent_oid = remote_oid_opt
            .or(local_oid_opt)
            .expect("store ref oid should exist");
        let resolution = resolve_migration_inputs(local_loaded.as_ref(), remote_loaded.as_ref());
        if !force && !resolution.summary.warnings.is_empty() {
            return Err(SyncError::MigrationWarnings(
                resolution.summary.warnings.clone(),
            ));
        }

        let snapshot = serialize_store_snapshot(
            &resolution.merged_state,
            Some(&resolution.root_slug),
            resolution.parent_meta_stamp.as_ref(),
        )?;
        let parent_matches_snapshot = store_tree_matches_snapshot(repo, parent_oid, &snapshot)?;
        let needs_rewrite = !parent_matches_snapshot;
        if needs_rewrite && source_summary.is_none() {
            source_summary = Some(resolution.summary.clone());
        }

        if !needs_rewrite {
            let has_origin_remote = repo.find_remote("origin").is_ok();
            let should_push_existing_local = !no_push
                && remote_oid_opt.is_none()
                && has_origin_remote
                && local_oid_opt.is_some();

            if should_push_existing_local {
                let local_oid = local_oid_opt.expect("local store ref should exist for no-op push");
                before_push(retries, local_oid)?;
                match push_store_ref(repo) {
                    Ok(pushed) => {
                        let noop_summary = canonical_noop_summary(&resolution.summary);
                        let summary = source_summary.as_ref().unwrap_or(&noop_summary);
                        return Ok(migration_outcome(
                            summary,
                            None,
                            if pushed {
                                MigratePushDisposition::Pushed
                            } else {
                                MigratePushDisposition::SkippedNoRemote
                            },
                        ));
                    }
                    Err(SyncError::NonFastForward) => {
                        retries += 1;
                        if retries > max_retries {
                            return Err(SyncError::TooManyRetries(retries));
                        }
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }

            if !dry_run {
                maybe_align_local_store_ref_to_remote(repo, local_oid_opt, remote_oid_opt)?;
            }
            let noop_summary = canonical_noop_summary(&resolution.summary);
            let summary = source_summary.as_ref().unwrap_or(&noop_summary);
            let push = if no_push || remote_oid_opt.is_some() {
                MigratePushDisposition::SkippedNoPush
            } else {
                MigratePushDisposition::SkippedNoRemote
            };
            return Ok(migration_outcome(summary, None, push));
        }

        if let Some(local_oid) = local_oid_opt {
            ensure_backup_ref_with_observer(repo, local_oid, &NoopSyncObserver)?;
        }

        let message = "migrate: deps to OR-Set v1";
        let commit_oid = commit_store_snapshot(
            repo,
            parent_oid,
            &resolution.merged_state,
            Some(&resolution.root_slug),
            resolution.parent_meta_stamp.as_ref(),
            message,
        )?;
        let summary = source_summary.as_ref().unwrap_or(&resolution.summary);

        if no_push {
            return Ok(migration_outcome(
                summary,
                Some(commit_oid),
                MigratePushDisposition::SkippedNoPush,
            ));
        }

        before_push(retries, commit_oid)?;
        match push_store_ref(repo) {
            Ok(pushed) => {
                return Ok(migration_outcome(
                    summary,
                    Some(commit_oid),
                    if pushed {
                        MigratePushDisposition::Pushed
                    } else {
                        MigratePushDisposition::SkippedNoRemote
                    },
                ));
            }
            Err(SyncError::NonFastForward) => {
                retries += 1;
                if retries > max_retries {
                    let current_local_oid_opt =
                        refname_to_id_optional(repo, "refs/heads/beads/store")?;
                    restore_local_store_ref(repo, initial_local_oid_opt, current_local_oid_opt)?;
                    return Err(SyncError::TooManyRetries(retries));
                }
            }
            Err(err) => return Err(err),
        }
    }
}

fn apply_test_migrate_before_push_hook(repo: &Repository, retries: usize) -> Result<(), SyncError> {
    if std::env::var_os(ENV_TESTING).as_deref() != Some(OsStr::new("1")) {
        return Ok(());
    }
    let Some(spec) = std::env::var_os(ENV_TEST_MIGRATE_BEFORE_PUSH_WINNER) else {
        return Ok(());
    };
    let (only_once, oid_text) = parse_test_before_push_winner_spec(spec)?;
    if only_once && retries > 0 {
        return Ok(());
    }

    let winner_oid = Oid::from_str(&oid_text).map_err(|err| {
        SyncError::Io(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("{ENV_TEST_MIGRATE_BEFORE_PUSH_WINNER} winner oid is invalid: {err}"),
        ))
    })?;
    let remote = repo.find_remote("origin").map_err(SyncError::Git)?;
    let remote_url = remote.url().ok_or_else(|| {
        SyncError::Io(std::io::Error::new(
            ErrorKind::InvalidInput,
            "origin remote has no configured URL for migration test hook",
        ))
    })?;
    let remote_repo =
        Repository::open(remote_url).map_err(|err| SyncError::OpenRepo(remote_url.into(), err))?;
    remote_repo
        .reference(
            "refs/heads/beads/store",
            winner_oid,
            true,
            "beads migrate test hook: simulate push race",
        )
        .map_err(SyncError::from)?;
    Ok(())
}

fn parse_test_before_push_winner_spec(spec: OsString) -> Result<(bool, String), SyncError> {
    let spec = spec.into_string().map_err(|_| {
        SyncError::Io(std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("{ENV_TEST_MIGRATE_BEFORE_PUSH_WINNER} must be valid UTF-8"),
        ))
    })?;
    if let Some((mode, oid)) = spec.split_once(':') {
        match mode {
            "once" => return Ok((true, oid.to_string())),
            "always" => return Ok((false, oid.to_string())),
            _ => {
                return Err(SyncError::Io(std::io::Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "{ENV_TEST_MIGRATE_BEFORE_PUSH_WINNER} mode must be `once` or `always`"
                    ),
                )));
            }
        }
    }
    Ok((true, spec))
}

fn preview_migrate_store_ref_to_v1(
    repo: &Repository,
    force: bool,
) -> Result<MigrateStoreToV1Outcome, SyncError> {
    let local_oid_opt = refname_to_id_optional(repo, "refs/heads/beads/store")?;
    let live_remote_preview = open_remote_store_preview(repo)?;
    let remote_oid_opt = live_remote_preview.as_ref().map(RemoteStorePreview::oid);

    if local_oid_opt.is_none() && remote_oid_opt.is_none() {
        return Err(SyncError::NoLocalRef("refs/heads/beads/store".into()));
    }

    if let (Some(local_oid), Some(remote_oid)) = (local_oid_opt, remote_oid_opt)
        && local_oid != remote_oid
    {
        let has_common_ancestor = match live_remote_preview.as_ref() {
            Some(preview) if remote_oid_opt == Some(preview.oid()) => {
                preview_has_common_ancestor(repo, local_oid, preview)?
            }
            _ => has_common_ancestor(repo, local_oid, remote_oid)?,
        };
        if should_refuse_migration_divergence(force, has_common_ancestor) {
            return Err(SyncError::NoCommonAncestor);
        }
    }

    let local_loaded = match local_oid_opt {
        Some(oid) => Some(read_state_at_oid_for_migration(repo, oid)?),
        None => None,
    };
    let remote_loaded = if let Some(preview) = live_remote_preview.as_ref() {
        Some(read_state_at_oid_for_migration(
            preview.repo(),
            preview.oid(),
        )?)
    } else {
        None
    };
    let resolution = resolve_migration_inputs(local_loaded.as_ref(), remote_loaded.as_ref());
    let parent_oid = remote_oid_opt
        .or(local_oid_opt)
        .expect("store ref oid should exist");
    let snapshot = serialize_store_snapshot(
        &resolution.merged_state,
        Some(&resolution.root_slug),
        resolution.parent_meta_stamp.as_ref(),
    )?;
    let parent_matches_snapshot = match live_remote_preview.as_ref() {
        Some(preview) if remote_oid_opt == Some(preview.oid()) => {
            store_tree_matches_snapshot(preview.repo(), preview.oid(), &snapshot)?
        }
        _ => store_tree_matches_snapshot(repo, parent_oid, &snapshot)?,
    };
    let needs_rewrite = !parent_matches_snapshot;
    let summary = if needs_rewrite {
        resolution.summary.clone()
    } else {
        canonical_noop_summary(&resolution.summary)
    };
    Ok(migration_outcome(
        &summary,
        None,
        MigratePushDisposition::SkippedNoPush,
    ))
}

fn should_refuse_migration_divergence(force: bool, has_common_ancestor: bool) -> bool {
    !force && !has_common_ancestor
}

fn resolve_migration_inputs(
    local_loaded: Option<&LoadedStoreMigration>,
    remote_loaded: Option<&LoadedStoreMigration>,
) -> ResolvedMigrationInputs {
    let mut merged_state = CanonicalState::new();
    if let Some(local) = local_loaded {
        merged_state = merged_state.join(&local.state);
    }
    if let Some(remote) = remote_loaded {
        merged_state = merged_state.join(&remote.state);
    }

    let local_has_legacy_deps = matches!(
        local_loaded.map(|loaded| loaded.deps_format),
        Some(wire::DepsFormat::LegacyEdges)
    );
    let remote_has_legacy_deps = matches!(
        remote_loaded.map(|loaded| loaded.deps_format),
        Some(wire::DepsFormat::LegacyEdges)
    );
    let deps_format_before = if local_has_legacy_deps || remote_has_legacy_deps {
        wire::DepsFormat::LegacyEdges
    } else {
        wire::DepsFormat::OrSetV1
    };
    let converted_deps = local_has_legacy_deps || remote_has_legacy_deps;
    let notes_present_before = local_loaded
        .map(|loaded| loaded.notes_present)
        .unwrap_or(true)
        && remote_loaded
            .map(|loaded| loaded.notes_present)
            .unwrap_or(true);
    let checksums_present_before = local_loaded.map(has_full_store_checksums).unwrap_or(true)
        && remote_loaded.map(has_full_store_checksums).unwrap_or(true);
    let from_effective_version = effective_format_version_before(
        local_loaded,
        remote_loaded,
        deps_format_before,
        notes_present_before,
        checksums_present_before,
    );
    let root_slug = remote_loaded
        .and_then(|loaded| loaded.meta.root_slug())
        .or_else(|| local_loaded.and_then(|loaded| loaded.meta.root_slug()))
        .cloned()
        .or_else(|| infer_root_slug_from_state(&merged_state))
        .unwrap_or_else(default_root_slug);
    let parent_meta_stamp = max_write_stamp(
        remote_loaded
            .and_then(|loaded| loaded.meta.last_write_stamp())
            .cloned(),
        local_loaded
            .and_then(|loaded| loaded.meta.last_write_stamp())
            .cloned(),
    );
    let mut warnings = Vec::new();
    if let Some(local) = local_loaded {
        warnings.extend(local.warnings.clone());
    }
    if let Some(remote) = remote_loaded {
        warnings.extend(remote.warnings.clone());
    }
    warnings.sort();
    warnings.dedup();

    ResolvedMigrationInputs {
        merged_state,
        root_slug,
        parent_meta_stamp,
        summary: MigrationSourceSummary {
            from_effective_version,
            deps_format_before,
            converted_deps,
            added_notes_file: !notes_present_before,
            wrote_checksums: true,
            warnings,
        },
    }
}

fn migration_outcome(
    summary: &MigrationSourceSummary,
    commit_oid: Option<Oid>,
    push: MigratePushDisposition,
) -> MigrateStoreToV1Outcome {
    MigrateStoreToV1Outcome {
        from_effective_version: summary.from_effective_version,
        deps_format_before: summary.deps_format_before,
        converted_deps: summary.converted_deps,
        added_notes_file: summary.added_notes_file,
        wrote_checksums: summary.wrote_checksums,
        commit_oid,
        push,
        warnings: summary.warnings.clone(),
    }
}

fn canonical_noop_summary(summary: &MigrationSourceSummary) -> MigrationSourceSummary {
    MigrationSourceSummary {
        converted_deps: false,
        added_notes_file: false,
        wrote_checksums: false,
        ..summary.clone()
    }
}

fn maybe_align_local_store_ref_to_remote(
    repo: &Repository,
    local_oid_opt: Option<Oid>,
    remote_oid_opt: Option<Oid>,
) -> Result<(), SyncError> {
    let Some(remote_oid) = remote_oid_opt else {
        return Ok(());
    };
    if local_oid_opt == Some(remote_oid) {
        return Ok(());
    }
    if let Some(local_oid) = local_oid_opt {
        ensure_backup_ref_with_observer(repo, local_oid, &NoopSyncObserver)?;
    }
    update_ref(
        repo,
        "refs/heads/beads/store",
        remote_oid,
        "beads migrate: align local store ref to remote winner",
    )
}

fn effective_format_version_before(
    local_loaded: Option<&LoadedStoreMigration>,
    remote_loaded: Option<&LoadedStoreMigration>,
    deps_format_before: wire::DepsFormat,
    notes_present_before: bool,
    checksums_present_before: bool,
) -> u32 {
    let latest = crate::core::FormatVersion::CURRENT.get();
    let meta_is_v1 = local_loaded
        .into_iter()
        .chain(remote_loaded)
        .all(|loaded| matches!(loaded.meta.meta(), wire::StoreMeta::V1 { .. }));
    if meta_is_v1
        && matches!(deps_format_before, wire::DepsFormat::OrSetV1)
        && notes_present_before
        && checksums_present_before
    {
        latest
    } else {
        0
    }
}

fn infer_root_slug_from_state(state: &CanonicalState) -> Option<BeadSlug> {
    let mut counts: BTreeMap<BeadSlug, usize> = BTreeMap::new();
    for (id, _) in state.iter_live() {
        *counts.entry(id.slug_value()).or_default() += 1;
    }
    for (key, _) in state.iter_tombstones() {
        *counts.entry(key.id.slug_value()).or_default() += 1;
    }
    counts
        .into_iter()
        .max_by(|(slug_a, count_a), (slug_b, count_b)| {
            count_a.cmp(count_b).then_with(|| slug_b.cmp(slug_a))
        })
        .map(|(slug, _)| slug)
}

fn default_root_slug() -> BeadSlug {
    BeadSlug::parse("bd").expect("default slug must be valid")
}

fn has_common_ancestor(
    repo: &Repository,
    local_oid: Oid,
    remote_oid: Oid,
) -> Result<bool, SyncError> {
    match repo.merge_base(local_oid, remote_oid) {
        Ok(_) => Ok(true),
        Err(err) if err.code() == ErrorCode::NotFound => Ok(false),
        Err(err) => Err(SyncError::MergeBase(err)),
    }
}

fn preview_has_common_ancestor(
    local_repo: &Repository,
    local_oid: Oid,
    remote_preview: &RemoteStorePreview,
) -> Result<bool, SyncError> {
    let local_repo_url = local_repo.path().to_string_lossy().into_owned();
    let mut local_remote = remote_preview.repo().remote_anonymous(&local_repo_url)?;
    fetch_store_ref_with_refspec(
        local_repo,
        &mut local_remote,
        "refs/heads/beads/store:refs/heads/local-preview",
    )?;
    drop(local_remote);
    let preview_local_oid = remote_preview
        .repo()
        .refname_to_id("refs/heads/local-preview")?;
    debug_assert_eq!(preview_local_oid, local_oid);
    has_common_ancestor(
        remote_preview.repo(),
        preview_local_oid,
        remote_preview.oid(),
    )
}

fn restore_local_store_ref(
    repo: &Repository,
    initial_local_oid_opt: Option<Oid>,
    current_local_oid_opt: Option<Oid>,
) -> Result<(), SyncError> {
    if initial_local_oid_opt == current_local_oid_opt {
        return Ok(());
    }
    match initial_local_oid_opt {
        Some(initial_oid) => update_ref(
            repo,
            "refs/heads/beads/store",
            initial_oid,
            "beads migrate: restore local store ref after failed retry",
        ),
        None => delete_ref(repo, "refs/heads/beads/store"),
    }
}

fn has_full_store_checksums(loaded: &LoadedStoreMigration) -> bool {
    loaded
        .meta
        .checksums()
        .is_some_and(|checksums| checksums.notes.is_some())
}

struct StoreSnapshotBytes {
    state: Vec<u8>,
    tombstones: Vec<u8>,
    deps: Vec<u8>,
    notes: Vec<u8>,
    meta: Vec<u8>,
}

fn serialize_store_snapshot(
    state: &CanonicalState,
    root_slug: Option<&BeadSlug>,
    parent_meta_stamp: Option<&WriteStamp>,
) -> Result<StoreSnapshotBytes, SyncError> {
    let state_bytes = wire::serialize_state(state)?;
    let tombstones_bytes = wire::serialize_tombstones(state)?;
    let deps_bytes = wire::serialize_deps(state)?;
    let notes_bytes = wire::serialize_notes(state)?;
    let checksums = wire::StoreChecksums::from_bytes(
        &state_bytes,
        &tombstones_bytes,
        &deps_bytes,
        Some(&notes_bytes),
    );
    let meta_bytes = wire::serialize_meta(
        root_slug.map(BeadSlug::as_str),
        parent_meta_stamp,
        &checksums,
    )?;
    Ok(StoreSnapshotBytes {
        state: state_bytes,
        tombstones: tombstones_bytes,
        deps: deps_bytes,
        notes: notes_bytes,
        meta: meta_bytes,
    })
}

fn store_tree_matches_snapshot(
    repo: &Repository,
    parent_oid: Oid,
    snapshot: &StoreSnapshotBytes,
) -> Result<bool, SyncError> {
    let commit = repo.find_commit(parent_oid)?;
    let tree = commit.tree()?;
    Ok(
        tree_blob_matches(repo, &tree, "state.jsonl", &snapshot.state)?
            && tree_blob_matches(repo, &tree, "tombstones.jsonl", &snapshot.tombstones)?
            && tree_blob_matches(repo, &tree, "deps.jsonl", &snapshot.deps)?
            && tree_blob_matches(repo, &tree, "notes.jsonl", &snapshot.notes)?
            && tree_blob_matches(repo, &tree, "meta.json", &snapshot.meta)?,
    )
}

fn tree_blob_matches(
    repo: &Repository,
    tree: &git2::Tree<'_>,
    name: &'static str,
    expected: &[u8],
) -> Result<bool, SyncError> {
    let Some(entry) = tree.get_name(name) else {
        return Ok(false);
    };
    let blob = repo
        .find_object(entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()
        .map_err(|_| SyncError::NotABlob(name))?;
    Ok(blob.content() == expected)
}

fn commit_store_snapshot(
    repo: &Repository,
    parent_oid: Oid,
    state: &CanonicalState,
    root_slug: Option<&BeadSlug>,
    parent_meta_stamp: Option<&WriteStamp>,
    message: &str,
) -> Result<Oid, SyncError> {
    let snapshot = serialize_store_snapshot(state, root_slug, parent_meta_stamp)?;

    let state_oid = repo.blob(&snapshot.state)?;
    let tombs_oid = repo.blob(&snapshot.tombstones)?;
    let deps_oid = repo.blob(&snapshot.deps)?;
    let notes_oid = repo.blob(&snapshot.notes)?;
    let meta_oid = repo.blob(&snapshot.meta)?;

    let mut builder = repo.treebuilder(None)?;
    builder.insert("state.jsonl", state_oid, 0o100644)?;
    builder.insert("tombstones.jsonl", tombs_oid, 0o100644)?;
    builder.insert("deps.jsonl", deps_oid, 0o100644)?;
    builder.insert("notes.jsonl", notes_oid, 0o100644)?;
    builder.insert("meta.json", meta_oid, 0o100644)?;
    let tree_oid = builder.write()?;
    let tree = repo.find_tree(tree_oid)?;

    let sig = Signature::now("beads", "beads@localhost")?;
    let parents: Vec<_> = if parent_oid.is_zero() {
        Vec::new()
    } else {
        vec![repo.find_commit(parent_oid)?]
    };
    let parent_refs: Vec<_> = parents.iter().collect();
    let commit_oid = repo.commit(None, &sig, &sig, message, &tree, &parent_refs)?;
    update_ref(
        repo,
        "refs/heads/beads/store",
        commit_oid,
        "beads sync: update local ref",
    )?;
    Ok(commit_oid)
}

pub fn refresh_store_tracking_ref(repo: &Repository) -> Result<(), SyncError> {
    fetch_store_ref(repo)
}

pub fn open_remote_store_preview(
    repo: &Repository,
) -> Result<Option<RemoteStorePreview>, SyncError> {
    let Some(url) = origin_remote_url(repo) else {
        return Ok(None);
    };

    let tempdir = TempDir::new().map_err(SyncError::Io)?;
    let preview_repo = Repository::init_bare(tempdir.path())?;
    let mut remote = preview_repo.remote_anonymous(&url)?;
    fetch_store_ref_with_refspec(
        repo,
        &mut remote,
        "refs/heads/beads/store:refs/heads/beads/store",
    )?;
    drop(remote);
    let oid = match preview_repo.refname_to_id("refs/heads/beads/store") {
        Ok(oid) => oid,
        Err(err) if err.code() == ErrorCode::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    Ok(Some(RemoteStorePreview {
        _tempdir: tempdir,
        repo: preview_repo,
        oid,
    }))
}

fn fetch_store_ref(repo: &Repository) -> Result<(), SyncError> {
    let mut remote = match repo.find_remote("origin") {
        Ok(remote) => remote,
        Err(_) => return Ok(()),
    };
    fetch_store_ref_with_refspec(
        repo,
        &mut remote,
        "refs/heads/beads/store:refs/remotes/origin/beads/store",
    )
}

fn fetch_store_ref_with_refspec(
    repo: &Repository,
    remote: &mut git2::Remote<'_>,
    refspec: &str,
) -> Result<(), SyncError> {
    let mut options = git2::FetchOptions::new();
    options.remote_callbacks(remote_callbacks(repo));
    remote
        .fetch(&[refspec], Some(&mut options), None)
        .map_err(SyncError::Fetch)
}

fn origin_remote_url(repo: &Repository) -> Option<String> {
    repo.find_remote("origin")
        .ok()
        .and_then(|remote| remote.url().map(ToOwned::to_owned))
}

fn remote_callbacks(repo: &Repository) -> git2::RemoteCallbacks<'static> {
    let cfg = repo.config().ok();
    let mut callbacks = git2::RemoteCallbacks::new();
    callbacks.credentials(move |url, username_from_url, allowed| {
        if allowed.is_ssh_key()
            && let Some(user) = username_from_url
        {
            return git2::Cred::ssh_key_from_agent(user);
        }
        if allowed.is_user_pass_plaintext()
            && let Some(ref cfg) = cfg
            && let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
        {
            return Ok(cred);
        }
        git2::Cred::default()
    });
    callbacks
}

fn push_store_ref(repo: &Repository) -> Result<bool, SyncError> {
    use std::cell::RefCell;

    let mut remote = match repo.find_remote("origin") {
        Ok(remote) => remote,
        Err(_) => return Ok(false),
    };
    let refspec = "refs/heads/beads/store:refs/heads/beads/store";
    let push_error: RefCell<Option<String>> = RefCell::new(None);

    {
        let mut callbacks = remote_callbacks(repo);
        callbacks.push_update_reference(|_ref_name, status| {
            if let Some(msg) = status {
                *push_error.borrow_mut() = Some(msg.to_string());
            }
            Ok(())
        });
        let mut push_options = git2::PushOptions::new();
        push_options.remote_callbacks(callbacks);

        if let Err(err) = remote.push(&[refspec], Some(&mut push_options)) {
            if is_retryable_push_error_message(&err.to_string()) {
                return Err(SyncError::NonFastForward);
            }
            return Err(SyncError::from(err));
        }
    }

    if let Some(message) = push_error.into_inner() {
        if is_retryable_push_error_message(&message) {
            return Err(SyncError::NonFastForward);
        }
        return Err(crate::error::PushRejected { message }.into());
    }

    Ok(true)
}

fn is_retryable_push_error_message(message: &str) -> bool {
    let msg = message.to_lowercase();
    msg.contains("non-fast-forward")
        || msg.contains("fetch first")
        || msg.contains("contains commits that are not present locally")
        || msg.contains("remote contains work that you do not have locally")
        || msg.contains("cannot lock ref")
        || msg.contains("failed to update ref")
        || msg.contains("failed to lock file")
}

/// Derive a root slug from repository info.
///
/// Priority:
/// 1. Remote URL basename (e.g., "github.com/foo/bar.git" -> "bar")
/// 2. Working directory name (e.g., "/home/user/bar" -> "bar")
/// 3. Fallback to "bd"
///
/// The slug is sanitized to be valid for bead IDs (lowercase alphanumeric + hyphens).
fn derive_root_slug(repo: &Repository) -> BeadSlug {
    // Try remote URL first
    if let Ok(remote) = repo.find_remote("origin")
        && let Some(url) = remote.url()
        && let Some(slug) = extract_slug_from_url(url)
    {
        return slug;
    }

    // Try workdir name
    if let Some(workdir) = repo.workdir()
        && let Some(name) = workdir.file_name().and_then(|n| n.to_str())
    {
        let sanitized = sanitize_slug(name);
        if !sanitized.is_empty()
            && let Ok(slug) = BeadSlug::parse(&sanitized)
        {
            return slug;
        }
    }

    // Fallback
    BeadSlug::parse("bd").expect("default slug must be valid")
}

/// Extract a slug from a git remote URL.
fn extract_slug_from_url(url: &str) -> Option<BeadSlug> {
    // Handle various URL formats:
    // - git@github.com:user/repo.git
    // - https://github.com/user/repo.git
    // - https://github.com/user/repo
    // - file:///path/to/repo

    let path = if url.contains(':') && !url.contains("://") {
        // SSH format: git@github.com:user/repo.git
        url.split(':').next_back()?
    } else {
        // URL format or file path
        url.trim_end_matches('/')
    };

    // Get the last path component
    let basename = path.rsplit('/').next()?;

    // Remove .git suffix
    let name = basename.strip_suffix(".git").unwrap_or(basename);

    let sanitized = sanitize_slug(name);
    if sanitized.is_empty() {
        return None;
    }
    BeadSlug::parse(&sanitized).ok()
}

/// Sanitize a string into a valid bead ID slug.
fn sanitize_slug(name: &str) -> String {
    let mut slug = String::new();
    let mut last_was_hyphen = true; // Prevent leading hyphens

    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
            last_was_hyphen = false;
        } else if !last_was_hyphen && (c == '-' || c == '_' || c == '.') {
            slug.push('-');
            last_was_hyphen = true;
        }
    }

    // Remove trailing hyphens
    while slug.ends_with('-') {
        slug.pop();
    }

    slug
}

fn refname_to_id_optional(repo: &Repository, name: &str) -> Result<Option<Oid>, SyncError> {
    match repo.refname_to_id(name) {
        Ok(oid) => Ok(Some(oid)),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(SyncError::Git(e)),
    }
}

fn update_ref(repo: &Repository, name: &str, oid: Oid, message: &str) -> Result<(), SyncError> {
    match repo.find_reference(name) {
        Ok(mut reference) => {
            reference.set_target(oid, message)?;
            Ok(())
        }
        Err(e) if e.code() == ErrorCode::NotFound => {
            repo.reference(name, oid, true, message)?;
            Ok(())
        }
        Err(e) => Err(SyncError::Git(e)),
    }
}

fn delete_ref(repo: &Repository, name: &str) -> Result<(), SyncError> {
    match repo.find_reference(name) {
        Ok(mut reference) => {
            reference.delete()?;
            Ok(())
        }
        Err(e) if e.code() == ErrorCode::NotFound => Ok(()),
        Err(e) => Err(SyncError::Git(e)),
    }
}

fn ensure_backup_ref_with_observer(
    repo: &Repository,
    oid: Oid,
    observer: &dyn SyncObserver,
) -> Result<(), SyncError> {
    let name = format!("{BACKUP_REF_PREFIX}{oid}");
    let _created = match repo.refname_to_id(&name) {
        Ok(_) => false,
        Err(err) if err.code() == ErrorCode::NotFound => {
            create_backup_reference_with_observer(repo, &name, oid, observer)?
        }
        Err(err) => return Err(SyncError::Git(err)),
    };
    prune_backup_refs_with_observer(repo, MAX_BACKUP_REFS, oid, observer)?;
    Ok(())
}

fn prune_backup_refs_with_observer(
    repo: &Repository,
    keep: usize,
    protected_oid: Oid,
    observer: &dyn SyncObserver,
) -> Result<(), SyncError> {
    let refs = match repo.references_glob(BACKUP_REFS_GLOB) {
        Ok(refs) => refs,
        Err(err) if err.code() == ErrorCode::Locked => {
            observer.on_backup_ref_lock_contention("scan");
            tracing::warn!(
                error = %err,
                "backup ref scan skipped due lock contention"
            );
            observer.on_backup_ref_scan(0);
            return Ok(());
        }
        Err(err) => return Err(SyncError::Git(err)),
    };

    let mut protected = false;
    let mut candidates: Vec<(String, i64)> = Vec::new();
    let mut scan_count = 0usize;
    for reference in refs {
        let reference = match reference {
            Ok(reference) => reference,
            Err(err) if err.code() == ErrorCode::Locked => {
                observer.on_backup_ref_lock_contention("scan");
                tracing::warn!(
                    error = %err,
                    "backup ref scan entry skipped due lock contention"
                );
                continue;
            }
            Err(err) => return Err(SyncError::Git(err)),
        };
        let Some(name) = reference.name().map(str::to_owned) else {
            continue;
        };
        let Some(target) = reference.target() else {
            continue;
        };
        scan_count = scan_count.saturating_add(1);
        if target == protected_oid {
            protected = true;
            continue;
        }
        let commit_time = repo
            .find_commit(target)
            .map(|commit| commit.time().seconds())
            .unwrap_or(i64::MIN);
        candidates.push((name, commit_time));
    }
    observer.on_backup_ref_scan(scan_count);

    let total_refs = candidates.len() + usize::from(protected);
    if total_refs <= keep {
        return Ok(());
    }

    let keep_candidates = if protected {
        keep.saturating_sub(1)
    } else {
        keep
    };
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));

    let mut pruned = 0usize;
    for (name, _) in candidates.into_iter().skip(keep_candidates) {
        if delete_backup_reference_with_observer(repo, &name, observer)? {
            pruned = pruned.saturating_add(1);
        }
    }
    observer.on_backup_ref_pruned(pruned);
    Ok(())
}

fn create_backup_reference_with_observer(
    repo: &Repository,
    name: &str,
    oid: Oid,
    observer: &dyn SyncObserver,
) -> Result<bool, SyncError> {
    match repo.reference(name, oid, false, "beads sync: backup local ref") {
        Ok(_) => Ok(true),
        Err(err) if err.code() == ErrorCode::Locked => {
            observer.on_backup_ref_lock_contention("create");
            if recover_backup_ref_lock(repo, name, &err, "create", observer) {
                match repo.reference(name, oid, false, "beads sync: backup local ref") {
                    Ok(_) => Ok(true),
                    Err(retry_err) if retry_err.code() == ErrorCode::Locked => {
                        observer.on_backup_ref_lock_contention("create");
                        tracing::warn!(
                            reference = %name,
                            error = %retry_err,
                            "backup ref create skipped due lock contention"
                        );
                        Ok(false)
                    }
                    Err(retry_err) => Err(SyncError::Git(retry_err)),
                }
            } else {
                tracing::warn!(
                    reference = %name,
                    error = %err,
                    "backup ref create skipped due lock contention"
                );
                Ok(false)
            }
        }
        Err(err) => Err(SyncError::Git(err)),
    }
}

fn delete_backup_reference_with_observer(
    repo: &Repository,
    name: &str,
    observer: &dyn SyncObserver,
) -> Result<bool, SyncError> {
    match repo.find_reference(name) {
        Ok(mut reference) => match reference.delete() {
            Ok(()) => Ok(true),
            Err(err) if err.code() == ErrorCode::Locked => {
                observer.on_backup_ref_lock_contention("delete");
                if recover_backup_ref_lock(repo, name, &err, "delete", observer) {
                    match repo.find_reference(name) {
                        Ok(mut retry_reference) => match retry_reference.delete() {
                            Ok(()) => Ok(true),
                            Err(retry_err) if retry_err.code() == ErrorCode::Locked => {
                                observer.on_backup_ref_lock_contention("delete");
                                tracing::warn!(
                                    reference = %name,
                                    error = %retry_err,
                                    "backup ref delete skipped due lock contention"
                                );
                                Ok(false)
                            }
                            Err(retry_err) => Err(SyncError::Git(retry_err)),
                        },
                        Err(retry_err) if retry_err.code() == ErrorCode::NotFound => Ok(false),
                        Err(retry_err) => Err(SyncError::Git(retry_err)),
                    }
                } else {
                    tracing::warn!(
                        reference = %name,
                        error = %err,
                        "backup ref delete skipped due lock contention"
                    );
                    Ok(false)
                }
            }
            Err(err) => Err(SyncError::Git(err)),
        },
        Err(err) if err.code() == ErrorCode::NotFound => Ok(false),
        Err(err) => Err(SyncError::Git(err)),
    }
}

fn recover_backup_ref_lock(
    repo: &Repository,
    reference_name: &str,
    err: &git2::Error,
    operation: &'static str,
    observer: &dyn SyncObserver,
) -> bool {
    if err.code() != ErrorCode::Locked {
        return false;
    }
    let expected_lock = backup_ref_lock_path(repo, reference_name);
    let lock_path = match lock_path_from_error(err) {
        Some(path) if path == expected_lock => path,
        Some(path) => {
            tracing::warn!(
                reference = %reference_name,
                lock_path = %path.display(),
                expected_lock = %expected_lock.display(),
                "skipping backup lock cleanup: lock path mismatch"
            );
            observer.on_backup_ref_lock_cleanup(operation, "path_mismatch");
            return false;
        }
        None => expected_lock,
    };

    let lock_age = lock_file_age(&lock_path, SystemTime::now());
    let lock_pid = lock_file_pid(&lock_path);
    let pid_state = lock_pid.map(backup_lock_pid_state);
    let (should_remove, skip_reason) =
        should_cleanup_backup_ref_lock(lock_age, pid_state, BACKUP_REF_LOCK_STALE_AFTER);
    if !should_remove {
        observer.on_backup_ref_lock_cleanup(operation, skip_reason);
        tracing::debug!(
            reference = %reference_name,
            lock_path = %lock_path.display(),
            lock_age_ms = lock_age.map(|age| age.as_millis() as u64),
            lock_pid,
            skip_reason,
            "backup ref lock cleanup skipped by policy"
        );
        return false;
    }

    match fs::remove_file(&lock_path) {
        Ok(()) => {
            tracing::warn!(
                reference = %reference_name,
                lock_path = %lock_path.display(),
                "removed stale backup ref lock"
            );
            observer.on_backup_ref_lock_cleanup(operation, "removed");
            true
        }
        Err(io_err) if io_err.kind() == std::io::ErrorKind::NotFound => {
            observer.on_backup_ref_lock_cleanup(operation, "missing");
            true
        }
        Err(io_err) => {
            tracing::warn!(
                reference = %reference_name,
                lock_path = %lock_path.display(),
                error = %io_err,
                "failed to remove stale backup ref lock"
            );
            observer.on_backup_ref_lock_cleanup(operation, "remove_failed");
            false
        }
    }
}

fn backup_ref_lock_path(repo: &Repository, reference_name: &str) -> PathBuf {
    let lock_ref = format!("{reference_name}.lock");
    repo.path().join(lock_ref)
}

fn lock_path_from_error(err: &git2::Error) -> Option<PathBuf> {
    if err.code() != ErrorCode::Locked {
        return None;
    }
    err.message()
        .split('\'')
        .find(|segment| segment.ends_with(".lock"))
        .map(PathBuf::from)
}

fn lock_file_age(lock_path: &Path, now: SystemTime) -> Option<Duration> {
    let modified = fs::metadata(lock_path).ok()?.modified().ok()?;
    now.duration_since(modified).ok()
}

fn lock_file_pid(lock_path: &Path) -> Option<u32> {
    let contents = fs::read_to_string(lock_path).ok()?;
    parse_lock_pid(&contents)
}

fn parse_lock_pid(contents: &str) -> Option<u32> {
    for line in contents.lines().take(4) {
        let token = line.trim();
        if token.is_empty() {
            continue;
        }
        if let Some(raw_pid) = token.strip_prefix("pid=") {
            return raw_pid.trim().parse().ok();
        }
        if token.as_bytes().iter().all(|byte| byte.is_ascii_digit()) {
            return token.parse().ok();
        }
        if let Some((key, value)) = token.split_once('=')
            && key.trim() == "pid"
        {
            return value.trim().parse().ok();
        }
        break;
    }
    None
}

fn backup_lock_pid_state(pid: u32) -> BackupLockPidState {
    #[cfg(unix)]
    {
        use nix::errno::Errno;
        use nix::sys::signal::kill;
        use nix::unistd::Pid;

        let Ok(raw_pid) = i32::try_from(pid) else {
            return BackupLockPidState::Missing;
        };
        match kill(Pid::from_raw(raw_pid), None) {
            Ok(()) | Err(Errno::EPERM) => BackupLockPidState::Alive,
            Err(Errno::ESRCH) => BackupLockPidState::Missing,
            Err(err) => {
                tracing::debug!(%err, pid, "backup ref lock pid check returned unexpected error");
                BackupLockPidState::Unknown
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        BackupLockPidState::Unknown
    }
}

fn should_cleanup_backup_ref_lock(
    age: Option<Duration>,
    pid_state: Option<BackupLockPidState>,
    stale_after: Duration,
) -> (bool, &'static str) {
    match pid_state {
        Some(BackupLockPidState::Alive) => return (false, "pid_alive"),
        Some(BackupLockPidState::Missing) => return (true, "pid_missing"),
        Some(BackupLockPidState::Unknown) | None => {}
    }

    match age {
        Some(lock_age) if lock_age >= stale_after => {
            if pid_state == Some(BackupLockPidState::Unknown) {
                (true, "age_stale_pid_unknown")
            } else {
                (true, "age_stale")
            }
        }
        Some(_) => {
            if pid_state == Some(BackupLockPidState::Unknown) {
                (false, "age_recent_pid_unknown")
            } else {
                (false, "age_recent")
            }
        }
        None => {
            if pid_state == Some(BackupLockPidState::Unknown) {
                (false, "age_unknown_pid_unknown")
            } else {
                (false, "age_unknown")
            }
        }
    }
}

/// Initialize the beads ref if it doesn't exist.
///
/// Handles the race condition where multiple daemons try to init:
/// - Fetch first (in case remote already has the ref)
/// - If remote exists, point local to it
/// - If not, create orphan commit and try push
/// - On non-fast-forward, retry (lost the race)
pub fn init_beads_ref(repo: &Repository, max_retries: usize) -> Result<(), SyncError> {
    let mut retries = 0;

    loop {
        // If local ref already exists, treat init as a no-op.
        if refname_to_id_optional(repo, "refs/heads/beads/store")?.is_some() {
            return Ok(());
        }

        // Always fetch first
        if let Ok(mut remote) = repo.find_remote("origin") {
            let cfg = repo.config().ok();
            let mut callbacks = git2::RemoteCallbacks::new();
            callbacks.credentials(move |url, username_from_url, allowed| {
                if allowed.is_ssh_key()
                    && let Some(user) = username_from_url
                {
                    return git2::Cred::ssh_key_from_agent(user);
                }
                if allowed.is_user_pass_plaintext()
                    && let Some(ref cfg) = cfg
                    && let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
                {
                    return Ok(cred);
                }
                git2::Cred::default()
            });
            let mut fo = git2::FetchOptions::new();
            fo.remote_callbacks(callbacks);
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote
                .fetch(&[refspec], Some(&mut fo), None)
                .map_err(SyncError::Fetch)?;

            // If remote has the ref, point local to it
            if let Some(remote_oid) =
                refname_to_id_optional(repo, "refs/remotes/origin/beads/store")?
            {
                repo.reference(
                    "refs/heads/beads/store",
                    remote_oid,
                    false,
                    "beads init from remote",
                )?;
                return Ok(());
            }
        }

        // Derive root slug from repo name
        let root_slug = derive_root_slug(repo);

        // Create empty initial state
        let state = CanonicalState::new();
        let state_bytes = wire::serialize_state(&state)?;
        let tombs_bytes = wire::serialize_tombstones(&state)?;
        let deps_bytes = wire::serialize_deps(&state)?;
        let notes_bytes = wire::serialize_notes(&state)?;
        let checksums = wire::StoreChecksums::from_bytes(
            &state_bytes,
            &tombs_bytes,
            &deps_bytes,
            Some(&notes_bytes),
        );
        let meta_bytes = wire::serialize_meta(Some(root_slug.as_str()), None, &checksums)?;

        // Write blobs
        let state_oid = repo.blob(&state_bytes)?;
        let tombs_oid = repo.blob(&tombs_bytes)?;
        let deps_oid = repo.blob(&deps_bytes)?;
        let notes_oid = repo.blob(&notes_bytes)?;
        let meta_oid = repo.blob(&meta_bytes)?;

        // Build tree
        let mut builder = repo.treebuilder(None)?;
        builder.insert("state.jsonl", state_oid, 0o100644)?;
        builder.insert("tombstones.jsonl", tombs_oid, 0o100644)?;
        builder.insert("deps.jsonl", deps_oid, 0o100644)?;
        builder.insert("notes.jsonl", notes_oid, 0o100644)?;
        builder.insert("meta.json", meta_oid, 0o100644)?;
        let tree_oid = builder.write()?;
        let tree = repo.find_tree(tree_oid)?;

        // Create orphan commit (no parents)
        let sig = Signature::now("beads", "beads@localhost")?;
        let _commit_oid = repo.commit(
            Some("refs/heads/beads/store"),
            &sig,
            &sig,
            "init",
            &tree,
            &[], // No parents - orphan commit
        )?;

        // Try to push
        if let Ok(mut remote) = repo.find_remote("origin") {
            use std::cell::RefCell;
            let refspec = "refs/heads/beads/store:refs/heads/beads/store";
            let push_error: RefCell<Option<String>> = RefCell::new(None);

            let push_result = {
                let cfg = repo.config().ok();
                let mut callbacks = git2::RemoteCallbacks::new();
                callbacks.credentials(move |url, username_from_url, allowed| {
                    if allowed.is_ssh_key()
                        && let Some(user) = username_from_url
                    {
                        return git2::Cred::ssh_key_from_agent(user);
                    }
                    if allowed.is_user_pass_plaintext()
                        && let Some(ref cfg) = cfg
                        && let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
                    {
                        return Ok(cred);
                    }
                    git2::Cred::default()
                });
                callbacks.push_update_reference(|_ref_name, status| {
                    if let Some(msg) = status {
                        *push_error.borrow_mut() = Some(msg.to_string());
                    }
                    Ok(())
                });

                let mut push_options = git2::PushOptions::new();
                push_options.remote_callbacks(callbacks);

                remote.push(&[refspec], Some(&mut push_options))
            };

            if let Err(e) = push_result {
                // Push failed - might be a race
                retries += 1;
                if retries > max_retries {
                    return Err(SyncError::InitFailed(e));
                }
                continue;
            }

            if let Some(err) = push_error.into_inner() {
                if err.contains("non-fast-forward") || err.contains("fetch first") {
                    // Lost race - retry
                    retries += 1;
                    if retries > max_retries {
                        return Err(SyncError::TooManyRetries(retries));
                    }
                    // Delete our orphan commit's ref so we can try again
                    let _ = repo
                        .find_reference("refs/heads/beads/store")
                        .map(|mut r| r.delete());
                    continue;
                }
                return Err(crate::error::PushRejected { message: err }.into());
            }
        }

        // Either no remote (local only) or push succeeded
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WireError;
    use crate::core::{
        ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, Claim, DepKey, DepKind, Dot, Lww,
        Priority, ReplicaId, Stamp, StateJsonlSha256, Tombstone, Workflow,
    };
    use git2::Time;
    #[cfg(feature = "slow-tests")]
    use proptest::test_runner::TestCaseError;
    #[cfg(feature = "slow-tests")]
    use proptest::{prop_assert, prop_assert_eq};
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};
    use tempfile::TempDir;
    use uuid::Uuid;

    fn make_stamp(wall_ms: u64, actor: &str) -> Stamp {
        Stamp::new(WriteStamp::new(wall_ms, 0), ActorId::new(actor).unwrap())
    }

    fn make_bead(id: &str, stamp: &Stamp) -> Bead {
        let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("test".to_string(), stamp.clone()),
            description: Lww::new(String::new(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::new(2).unwrap(), stamp.clone()),
            bead_type: Lww::new(BeadType::Task, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::default(), stamp.clone()),
        };
        Bead::new(core, fields)
    }

    fn store_bytes() -> (Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>) {
        let state = CanonicalState::new();
        let state_bytes = wire::serialize_state(&state).unwrap();
        let tombs_bytes = wire::serialize_tombstones(&state).unwrap();
        let deps_bytes = wire::serialize_deps(&state).unwrap();
        let notes_bytes = wire::serialize_notes(&state).unwrap();
        (state_bytes, tombs_bytes, deps_bytes, notes_bytes)
    }

    #[derive(Default)]
    struct RecordingSyncObserver {
        lock_contention: Mutex<BTreeMap<&'static str, u64>>,
        lock_cleanup: Mutex<BTreeMap<(&'static str, &'static str), u64>>,
        backup_ref_scan_samples: Mutex<Vec<usize>>,
        backup_ref_pruned_total: Mutex<u64>,
    }

    impl RecordingSyncObserver {
        fn lock_contention_count(&self, operation: &'static str) -> u64 {
            *self
                .lock_contention
                .lock()
                .expect("lock_contention poisoned")
                .get(operation)
                .unwrap_or(&0)
        }

        fn lock_cleanup_count(&self, operation: &'static str, outcome: &'static str) -> u64 {
            *self
                .lock_cleanup
                .lock()
                .expect("lock_cleanup poisoned")
                .get(&(operation, outcome))
                .unwrap_or(&0)
        }

        fn backup_ref_scan_samples(&self) -> usize {
            self.backup_ref_scan_samples
                .lock()
                .expect("backup_ref_scan_samples poisoned")
                .len()
        }

        fn backup_ref_pruned_total(&self) -> u64 {
            *self
                .backup_ref_pruned_total
                .lock()
                .expect("backup_ref_pruned_total poisoned")
        }
    }

    impl SyncObserver for RecordingSyncObserver {
        fn on_backup_ref_scan(&self, count: usize) {
            self.backup_ref_scan_samples
                .lock()
                .expect("backup_ref_scan_samples poisoned")
                .push(count);
        }

        fn on_backup_ref_pruned(&self, count: usize) {
            let mut total = self
                .backup_ref_pruned_total
                .lock()
                .expect("backup_ref_pruned_total poisoned");
            *total = total.saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        }

        fn on_backup_ref_lock_contention(&self, operation: &'static str) {
            let mut counts = self
                .lock_contention
                .lock()
                .expect("lock_contention poisoned");
            *counts.entry(operation).or_insert(0) += 1;
        }

        fn on_backup_ref_lock_cleanup(&self, operation: &'static str, outcome: &'static str) {
            let mut counts = self.lock_cleanup.lock().expect("lock_cleanup poisoned");
            *counts.entry((operation, outcome)).or_insert(0) += 1;
        }
    }

    fn write_store_commit(repo: &Repository, parent: Option<Oid>, message: &str) -> Oid {
        write_store_commit_with_meta(repo, parent, message, None)
    }

    fn write_store_commit_with_meta(
        repo: &Repository,
        parent: Option<Oid>,
        message: &str,
        last_stamp: Option<WriteStamp>,
    ) -> Oid {
        let (state_bytes, tombs_bytes, deps_bytes, notes_bytes) = store_bytes();
        let checksums = wire::StoreChecksums::from_bytes(
            &state_bytes,
            &tombs_bytes,
            &deps_bytes,
            Some(&notes_bytes),
        );
        let meta_bytes =
            wire::serialize_meta(Some("test"), last_stamp.as_ref(), &checksums).unwrap();
        write_store_commit_with_meta_bytes(repo, parent, message, Some(meta_bytes))
    }

    fn write_store_commit_with_meta_bytes(
        repo: &Repository,
        parent: Option<Oid>,
        message: &str,
        meta_bytes: Option<Vec<u8>>,
    ) -> Oid {
        let (state_bytes, tombs_bytes, deps_bytes, notes_bytes) = store_bytes();

        let state_oid = repo.blob(&state_bytes).unwrap();
        let tombs_oid = repo.blob(&tombs_bytes).unwrap();
        let deps_oid = repo.blob(&deps_bytes).unwrap();
        let notes_oid = repo.blob(&notes_bytes).unwrap();

        let mut builder = repo.treebuilder(None).unwrap();
        builder.insert("state.jsonl", state_oid, 0o100644).unwrap();
        builder
            .insert("tombstones.jsonl", tombs_oid, 0o100644)
            .unwrap();
        builder.insert("deps.jsonl", deps_oid, 0o100644).unwrap();
        builder.insert("notes.jsonl", notes_oid, 0o100644).unwrap();
        if let Some(meta_bytes) = meta_bytes {
            let meta_oid = repo.blob(&meta_bytes).unwrap();
            builder.insert("meta.json", meta_oid, 0o100644).unwrap();
        }
        let tree_oid = builder.write().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();

        let sig = Signature::now("test", "test@example.com").unwrap();
        let parents = parent
            .and_then(|oid| repo.find_commit(oid).ok())
            .map(|p| vec![p])
            .unwrap_or_default();
        let parent_refs: Vec<_> = parents.iter().collect();
        let commit_oid = repo
            .commit(None, &sig, &sig, message, &tree, &parent_refs)
            .unwrap_or_else(|e| panic!("commit failed: {e}"));
        update_ref(
            repo,
            "refs/heads/beads/store",
            commit_oid,
            "beads test: update ref",
        )
        .unwrap_or_else(|e| panic!("update ref failed: {e}"));
        commit_oid
    }

    fn write_store_commit_with_custom_deps(
        repo: &Repository,
        parent: Option<Oid>,
        message: &str,
        deps_bytes: &[u8],
        include_notes: bool,
        include_meta: bool,
    ) -> Oid {
        let state_bytes = Vec::<u8>::new();
        let tombs_bytes = Vec::<u8>::new();
        let notes_bytes = if include_notes {
            Vec::<u8>::new()
        } else {
            Vec::new()
        };

        let state_oid = repo.blob(&state_bytes).unwrap();
        let tombs_oid = repo.blob(&tombs_bytes).unwrap();
        let deps_oid = repo.blob(deps_bytes).unwrap();
        let notes_oid = if include_notes {
            Some(repo.blob(&notes_bytes).unwrap())
        } else {
            None
        };
        let meta_oid = if include_meta {
            let checksums = wire::StoreChecksums::from_bytes(
                &state_bytes,
                &tombs_bytes,
                deps_bytes,
                Some(&notes_bytes),
            );
            let meta_bytes = wire::serialize_meta(Some("test"), None, &checksums).unwrap();
            Some(repo.blob(&meta_bytes).unwrap())
        } else {
            None
        };

        let mut builder = repo.treebuilder(None).unwrap();
        builder.insert("state.jsonl", state_oid, 0o100644).unwrap();
        builder
            .insert("tombstones.jsonl", tombs_oid, 0o100644)
            .unwrap();
        builder.insert("deps.jsonl", deps_oid, 0o100644).unwrap();
        if let Some(notes_oid) = notes_oid {
            builder.insert("notes.jsonl", notes_oid, 0o100644).unwrap();
        }
        if let Some(meta_oid) = meta_oid {
            builder.insert("meta.json", meta_oid, 0o100644).unwrap();
        }
        let tree_oid = builder.write().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();

        let sig = Signature::now("test", "test@example.com").unwrap();
        let parents = parent
            .and_then(|oid| repo.find_commit(oid).ok())
            .map(|p| vec![p])
            .unwrap_or_default();
        let parent_refs: Vec<_> = parents.iter().collect();
        let commit_oid = repo
            .commit(None, &sig, &sig, message, &tree, &parent_refs)
            .unwrap_or_else(|e| panic!("commit failed: {e}"));
        update_ref(
            repo,
            "refs/heads/beads/store",
            commit_oid,
            "beads test: update ref",
        )
        .unwrap_or_else(|e| panic!("update ref failed: {e}"));
        commit_oid
    }

    fn write_store_commit_without_meta(
        repo: &Repository,
        parent: Option<Oid>,
        message: &str,
    ) -> Oid {
        write_store_commit_with_meta_bytes(repo, parent, message, None)
    }

    #[test]
    fn sync_diff_count_message() {
        // When >5 changes, falls back to count format
        let diff = SyncDiff {
            created: 6,
            updated: 1,
            deleted: 0,
            details: vec![],
        };
        assert_eq!(
            diff.to_commit_message(),
            "beads(store): +6 created, ~1 updated"
        );

        let diff = SyncDiff {
            created: 0,
            updated: 0,
            deleted: 3,
            details: vec![],
        };
        assert_eq!(diff.to_commit_message(), "beads(store): -3 deleted");

        let diff = SyncDiff::default();
        assert_eq!(diff.to_commit_message(), "beads(store): no changes");
    }

    #[test]
    fn sync_diff_marks_dep_changes_as_updates() {
        let stamp = make_stamp(1000, "alice");
        let bead_a = make_bead("bd-aaa", &stamp);
        let bead_b = make_bead("bd-bbb", &stamp);

        let mut before = CanonicalState::new();
        before.insert(bead_a).unwrap();
        before.insert(bead_b).unwrap();

        let mut after = before.clone();
        let dep_key = DepKey::new(
            BeadId::parse("bd-aaa").unwrap(),
            BeadId::parse("bd-bbb").unwrap(),
            DepKind::Blocks,
        )
        .unwrap();
        let dep_dot = Dot {
            replica: ReplicaId::new(Uuid::from_bytes([1u8; 16])),
            counter: 1,
        };
        let dep_key = after.check_dep_add_key(dep_key).unwrap();
        after.apply_dep_add(dep_key, dep_dot, stamp);

        let diff = compute_diff(&before, &after);
        assert_eq!(diff.created, 0);
        assert_eq!(diff.deleted, 0);
        assert_eq!(diff.updated, 1);
        assert_eq!(diff.details.len(), 1);
        assert_eq!(diff.details[0].id, "bd-aaa");
        assert!(diff.details[0].changed_fields.contains(&"deps"));
    }

    #[test]
    fn merge_keeps_tombstones_without_gc() {
        let id = BeadId::parse("bd-dead").unwrap();
        let deleted = make_stamp(1, "alice");
        let mut local_state = CanonicalState::new();
        local_state.insert_tombstone(Tombstone::new(id.clone(), deleted, Some("gone".into())));

        let fetched = SyncProcess {
            repo_path: PathBuf::new(),
            config: SyncConfig::default(),
            observer: Arc::new(NoopSyncObserver),
            phase: Fetched {
                local_oid: Oid::from_bytes(&[1; 20]).unwrap(),
                remote_oid: Oid::from_bytes(&[2; 20]).unwrap(),
                remote_state: CanonicalState::new(),
                root_slug: None,
                parent_meta_stamp: None,
                fetch_error: None,
                divergence: None,
                force_push: None,
            },
        };

        let merged = fetched.merge(&local_state).expect("merge");
        assert!(merged.phase.state.get_tombstone(&id).is_some());
    }

    #[test]
    fn sync_diff_detailed_message() {
        // When <=5 changes and has details, uses detailed format
        let diff = SyncDiff {
            created: 1,
            updated: 1,
            deleted: 0,
            details: vec![
                ChangeDetail {
                    id: "bd-abc".to_string(),
                    title: "Fix bug".to_string(),
                    change_type: ChangeType::Created,
                    changed_fields: vec![],
                },
                ChangeDetail {
                    id: "bd-xyz".to_string(),
                    title: "Update feature".to_string(),
                    change_type: ChangeType::Updated,
                    changed_fields: vec!["status", "desc"],
                },
            ],
        };
        let msg = diff.to_commit_message();
        assert!(msg.contains("created bd-abc: \"Fix bug\""));
        assert!(msg.contains("updated bd-xyz [status, desc]: \"Update feature\""));
    }

    #[test]
    fn sync_diff_truncates_long_titles() {
        let diff = SyncDiff {
            created: 1,
            updated: 0,
            deleted: 0,
            details: vec![ChangeDetail {
                id: "bd-abc".to_string(),
                title: "This is a very long title that should be truncated to fit".to_string(),
                change_type: ChangeType::Created,
                changed_fields: vec![],
            }],
        };
        let msg = diff.to_commit_message();
        assert!(msg.contains("..."));
        assert!(!msg.contains("truncated to fit"));
    }

    #[test]
    fn read_state_at_oid_reads_meta_last_write_stamp() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let stamp = WriteStamp::new(1234, 7);
        let oid = write_store_commit_with_meta(&repo, None, "meta", Some(stamp.clone()));

        let loaded = read_state_at_oid(&repo, oid).unwrap();
        assert_eq!(
            loaded.meta.last_write_stamp(),
            Some(&stamp),
            "expected meta last_write_stamp"
        );
    }

    #[test]
    fn sync_commit_persists_latest_write_stamp_in_meta() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let parent_meta_stamp = WriteStamp::new(1234, 7);
        let parent_oid =
            write_store_commit_with_meta(&repo, None, "parent", Some(parent_meta_stamp.clone()));

        let stamp = make_stamp(2345, "alice");
        let mut local_state = CanonicalState::new();
        local_state.insert_live(make_bead("bd-meta-max-stamp", &stamp));

        let fetched = SyncProcess {
            repo_path: PathBuf::new(),
            config: SyncConfig::default(),
            observer: Arc::new(NoopSyncObserver),
            phase: Fetched {
                local_oid: parent_oid,
                remote_oid: parent_oid,
                remote_state: CanonicalState::new(),
                root_slug: Some(BeadSlug::parse("test").unwrap()),
                parent_meta_stamp: Some(parent_meta_stamp),
                fetch_error: None,
                divergence: None,
                force_push: None,
            },
        };

        let commit_oid = fetched
            .merge(&local_state)
            .expect("merge")
            .commit(&repo)
            .expect("commit")
            .phase
            .commit_oid;
        let loaded = read_state_at_oid(&repo, commit_oid).expect("read committed state");

        assert_eq!(loaded.meta.last_write_stamp(), Some(&stamp.at));
    }

    #[test]
    fn read_state_at_oid_errors_on_invalid_meta() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let oid = write_store_commit_with_meta_bytes(
            &repo,
            None,
            "bad-meta",
            Some(b"{not json".to_vec()),
        );

        let err = read_state_at_oid(&repo, oid)
            .err()
            .expect("expected invalid meta error");
        assert!(matches!(err, SyncError::Wire(_)));
    }

    #[test]
    fn read_state_at_oid_errors_on_checksum_mismatch() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();

        let (state_bytes, tombs_bytes, deps_bytes, notes_bytes) = store_bytes();
        let mut checksums = wire::StoreChecksums::from_bytes(
            &state_bytes,
            &tombs_bytes,
            &deps_bytes,
            Some(&notes_bytes),
        );
        checksums.state = StateJsonlSha256::from_jsonl_bytes(&[0xAB; 32]);
        let meta_bytes = wire::serialize_meta(Some("test"), None, &checksums).unwrap();
        let oid =
            write_store_commit_with_meta_bytes(&repo, None, "bad-checksums", Some(meta_bytes));

        let err = read_state_at_oid(&repo, oid)
            .err()
            .expect("expected checksum mismatch error");
        assert!(matches!(
            err,
            SyncError::Wire(WireError::ChecksumMismatch {
                blob: "state.jsonl",
                ..
            })
        ));
    }

    #[test]
    fn read_state_at_oid_allows_missing_meta() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let oid = write_store_commit_without_meta(&repo, None, "no-meta");

        let loaded = read_state_at_oid(&repo, oid).unwrap();
        assert!(loaded.meta.is_legacy());
    }

    #[test]
    fn read_state_at_oid_for_migration_accepts_legacy_deps() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let oid = write_store_commit_with_custom_deps(
            &repo,
            None,
            "legacy-deps",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            true,
            true,
        );

        let strict_err = read_state_at_oid(&repo, oid)
            .err()
            .expect("strict load should reject legacy deps");
        assert!(matches!(strict_err, SyncError::Wire(_)));

        let loaded = read_state_at_oid_for_migration(&repo, oid).expect("migration load");
        assert_eq!(loaded.deps_format, wire::DepsFormat::LegacyEdges);
        assert!(loaded.warnings.is_empty());
        let dep_key = DepKey::new(
            BeadId::parse("bd-a").unwrap(),
            BeadId::parse("bd-b").unwrap(),
            DepKind::Blocks,
        )
        .unwrap();
        assert!(loaded.state.dep_store().contains(&dep_key));
    }

    #[test]
    fn migrate_store_ref_to_v1_rewrites_legacy_deps_to_strict_snapshot() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        write_store_commit_with_custom_deps(
            &repo,
            None,
            "legacy-deps",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            false,
            false,
        );

        let outcome = migrate_store_ref_to_v1(&repo, tmp.path(), false, false, true, 0).unwrap();
        assert!(outcome.converted_deps);
        assert!(outcome.added_notes_file);
        assert!(outcome.wrote_checksums);
        let commit_oid = outcome.commit_oid.expect("migration commit oid");

        let loaded = read_state_at_oid(&repo, commit_oid).expect("strict load after migration");
        assert!(matches!(
            loaded.meta.meta(),
            wire::StoreMeta::V1 {
                checksums: Some(_),
                ..
            }
        ));

        let commit = repo.find_commit(commit_oid).unwrap();
        let tree = commit.tree().unwrap();
        assert!(tree.get_name("notes.jsonl").is_some());
        let deps_entry = tree.get_name("deps.jsonl").expect("deps");
        let deps_blob = repo
            .find_object(deps_entry.id(), Some(ObjectType::Blob))
            .unwrap()
            .peel_to_blob()
            .unwrap();
        let parsed = wire::parse_deps_wire(deps_blob.content()).expect("strict deps decode");
        assert_eq!(parsed.entries.len(), 1);
    }

    #[test]
    fn migrate_store_ref_to_v1_aborts_on_legacy_parse_warnings_without_force() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let original_oid = write_store_commit_with_custom_deps(
            &repo,
            None,
            "legacy-deps-with-warning",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
{"from":"","to":"bd-c","kind":"blocks"}
"#,
            false,
            false,
        );

        let err = migrate_store_ref_to_v1(&repo, tmp.path(), false, false, true, 0)
            .expect_err("migration should fail without --force when warnings are present");
        match err {
            SyncError::MigrationWarnings(warnings) => {
                assert!(!warnings.is_empty(), "expected warning details in error");
                assert!(
                    warnings
                        .iter()
                        .any(|warning| warning.starts_with("LEGACY_DEPS_SHAPE(")),
                    "expected malformed-line warning, got: {warnings:?}"
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let head_after = repo.refname_to_id("refs/heads/beads/store").unwrap();
        assert_eq!(
            head_after, original_oid,
            "migration must not create a commit when warning gate fails"
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_dry_run_peeks_remote_without_mutating_tracking_ref() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let remote_v1 = write_store_commit(&remote_repo, None, "remote-v1");
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut options = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut options), None).unwrap();
        }
        local_repo
            .reference(
                "refs/heads/beads/store",
                remote_v1,
                true,
                "init local store",
            )
            .unwrap();

        let remote_v2 = write_store_commit_with_custom_deps(
            &remote_repo,
            Some(remote_v1),
            "remote-v2-legacy",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            true,
            true,
        );
        assert_ne!(remote_v1, remote_v2);

        let outcome =
            migrate_store_ref_to_v1(&local_repo, &local_dir, true, false, true, 0).unwrap();
        assert!(outcome.commit_oid.is_none(), "dry-run must not commit");
        assert_eq!(outcome.deps_format_before, wire::DepsFormat::LegacyEdges);
        assert_eq!(outcome.from_effective_version, 0);

        let remote_tracking_after = local_repo
            .refname_to_id("refs/remotes/origin/beads/store")
            .unwrap();
        assert_eq!(
            remote_tracking_after, remote_v1,
            "dry-run must classify live remote state without mutating tracking refs"
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_noop_realigns_same_tree_local_and_creates_backup_ref() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let remote_oid = write_store_commit(&remote_repo, None, "remote");
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut options = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut options), None).unwrap();
        }
        local_repo
            .reference(
                "refs/heads/beads/store",
                remote_oid,
                true,
                "init local store",
            )
            .unwrap();
        let local_ahead_oid = write_store_commit(&local_repo, Some(remote_oid), "local-ahead");

        let outcome =
            migrate_store_ref_to_v1(&local_repo, &local_dir, false, false, true, 0).unwrap();
        assert!(
            outcome.commit_oid.is_none(),
            "canonical store should remain no-op"
        );

        let backup_ref = format!("refs/beads/backup/{local_ahead_oid}");
        assert_eq!(
            local_repo.refname_to_id(&backup_ref).unwrap(),
            local_ahead_oid,
            "realigning away from an existing local ref should preserve a backup even when the tree already matches"
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_rewrite_creates_backup_ref_for_existing_local_store() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();

        let original_oid = write_store_commit_with_custom_deps(
            &repo,
            None,
            "local-legacy",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            false,
            false,
        );

        let outcome = migrate_store_ref_to_v1(&repo, tmp.path(), false, false, true, 0).unwrap();
        assert!(
            outcome.commit_oid.is_some(),
            "legacy local store should be rewritten"
        );

        let backup_ref = format!("refs/beads/backup/{original_oid}");
        let backup_oid = repo
            .refname_to_id(&backup_ref)
            .expect("backup ref should be created before rewrite");
        assert_eq!(backup_oid, original_oid);
        let head_after = repo.refname_to_id("refs/heads/beads/store").unwrap();
        assert_ne!(
            head_after, original_oid,
            "local ref should move to migrated commit"
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_force_keeps_canonical_store_noop() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let original_oid = write_store_commit(&repo, None, "canonical");

        let outcome = migrate_store_ref_to_v1(&repo, tmp.path(), false, true, true, 0).unwrap();
        assert!(
            outcome.commit_oid.is_none(),
            "force should not create a marker commit when bytes already match"
        );
        let head_after = repo.refname_to_id("refs/heads/beads/store").unwrap();
        assert_eq!(head_after, original_oid);
    }

    #[test]
    fn migrate_store_ref_to_v1_no_origin_noop_reports_skipped_no_remote() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let original_oid = write_store_commit(&repo, None, "canonical");

        let outcome = migrate_store_ref_to_v1(&repo, tmp.path(), false, false, false, 0).unwrap();
        assert!(
            outcome.commit_oid.is_none(),
            "canonical store should remain no-op"
        );
        assert_eq!(outcome.push, MigratePushDisposition::SkippedNoRemote);
        assert_eq!(
            repo.refname_to_id("refs/heads/beads/store").unwrap(),
            original_oid
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_no_push_rewrites_stale_local_without_fetching_remote_winner() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let base_oid = write_store_commit_with_custom_deps(
            &remote_repo,
            None,
            "legacy-base",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            false,
            false,
        );
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut options = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut options), None).unwrap();
        }
        local_repo
            .reference(
                "refs/heads/beads/store",
                base_oid,
                true,
                "init local legacy store",
            )
            .unwrap();

        let winner_oid = migrate_store_ref_to_v1(&remote_repo, &remote_dir, false, false, true, 0)
            .unwrap()
            .commit_oid
            .expect("winner migration commit");

        let outcome =
            migrate_store_ref_to_v1(&local_repo, &local_dir, false, false, true, 0).unwrap();
        let local_commit_oid = outcome
            .commit_oid
            .expect("no-push stale clone should still rewrite the local store");
        assert_eq!(outcome.push, MigratePushDisposition::SkippedNoPush);
        assert_eq!(
            local_repo.refname_to_id("refs/heads/beads/store").unwrap(),
            local_commit_oid,
            "local store ref should advance to the local-only rewrite commit"
        );
        assert_eq!(
            local_commit_oid, winner_oid,
            "local-only rewrite should deterministically converge to the same canonical commit"
        );
        assert_eq!(
            local_repo
                .refname_to_id("refs/remotes/origin/beads/store")
                .unwrap(),
            base_oid,
            "no-push migration should leave the stale tracking ref untouched"
        );
        assert_eq!(
            local_repo
                .find_commit(local_commit_oid)
                .unwrap()
                .tree()
                .unwrap()
                .id(),
            remote_repo
                .find_commit(winner_oid)
                .unwrap()
                .tree()
                .unwrap()
                .id(),
            "local-only rewrite should still converge to the same canonical snapshot tree"
        );
        let backup_ref = format!("refs/beads/backup/{base_oid}");
        assert_eq!(
            local_repo.refname_to_id(&backup_ref).unwrap(),
            base_oid,
            "realigning away from a legacy local ref should preserve a backup"
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_refuses_unrelated_divergence_without_force() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let remote_oid = write_store_commit(&remote_repo, None, "remote-v1");
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut options = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut options), None).unwrap();
        }

        let local_legacy_oid = write_store_commit_with_custom_deps(
            &local_repo,
            None,
            "local-legacy",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            false,
            false,
        );
        assert_ne!(local_legacy_oid, remote_oid);

        let err =
            migrate_store_ref_to_v1(&local_repo, &local_dir, false, false, true, 0).unwrap_err();
        assert!(matches!(err, SyncError::NoCommonAncestor));
        let head_after = local_repo.refname_to_id("refs/heads/beads/store").unwrap();
        assert_eq!(
            head_after, local_legacy_oid,
            "migration must not rewrite without --force"
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_retry_still_refuses_unrelated_divergence_without_force() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let base_oid = write_store_commit_with_custom_deps(
            &remote_repo,
            None,
            "legacy-base",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            false,
            false,
        );
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut options = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut options), None).unwrap();
        }
        local_repo
            .reference(
                "refs/heads/beads/store",
                base_oid,
                true,
                "init local legacy store",
            )
            .unwrap();

        let unrelated_oid = write_store_commit(&remote_repo, None, "remote-unrelated");
        update_ref(
            &remote_repo,
            "refs/heads/beads/store",
            base_oid,
            "reset remote to shared base",
        )
        .unwrap();

        let err = migrate_store_ref_to_v1_with_before_push(
            &local_repo,
            &local_dir,
            false,
            false,
            false,
            1,
            |retries, _commit_oid| {
                if retries == 0 {
                    update_ref(
                        &remote_repo,
                        "refs/heads/beads/store",
                        unrelated_oid,
                        "simulate unrelated remote winner before first push",
                    )?;
                }
                Ok(())
            },
        )
        .expect_err("retry should still refuse unrelated remote divergence without --force");

        assert!(matches!(err, SyncError::NoCommonAncestor));
        assert_eq!(
            local_repo.refname_to_id("refs/heads/beads/store").unwrap(),
            base_oid,
            "local store ref must remain unchanged after unrelated retry refusal"
        );
    }

    #[test]
    fn migrate_store_ref_to_v1_force_divergence_rewrites_local_missing_invariants() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let remote_oid = write_store_commit(&remote_repo, None, "remote-v1");

        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut options = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut options), None).unwrap();
        }

        let local_legacy_oid = write_store_commit_with_custom_deps(
            &local_repo,
            None,
            "local-legacy",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            false,
            false,
        );
        assert_ne!(local_legacy_oid, remote_oid);

        let outcome =
            migrate_store_ref_to_v1(&local_repo, &local_dir, false, true, true, 0).unwrap();
        assert!(
            outcome.commit_oid.is_some(),
            "migration should rewrite even when remote side is already canonical"
        );
        assert!(outcome.added_notes_file);
        assert!(outcome.wrote_checksums);

        let local_head = local_repo.refname_to_id("refs/heads/beads/store").unwrap();
        let loaded = read_state_at_oid(&local_repo, local_head).expect("strict load");
        assert!(matches!(
            loaded.meta.meta(),
            wire::StoreMeta::V1 {
                checksums: Some(_),
                ..
            }
        ));
        let commit = local_repo.find_commit(local_head).unwrap();
        let tree = commit.tree().unwrap();
        assert!(tree.get_name("notes.jsonl").is_some());
        assert!(tree.get_name("meta.json").is_some());
    }

    #[test]
    fn migrate_store_ref_to_v1_retries_true_push_race_and_realigns_local_ref() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let base_oid = write_store_commit_with_custom_deps(
            &remote_repo,
            None,
            "legacy-base",
            br#"{"from":"bd-a","to":"bd-b","kind":"blocks"}
"#,
            false,
            false,
        );
        let winner_oid = migrate_store_ref_to_v1(&remote_repo, &remote_dir, false, false, true, 0)
            .unwrap()
            .commit_oid
            .expect("winner migration commit");
        let winner_oid = {
            let commit = remote_repo.find_commit(winner_oid).unwrap();
            let tree = commit.tree().unwrap();
            let mut parents = Vec::new();
            for idx in 0..commit.parent_count() {
                parents.push(commit.parent(idx).unwrap());
            }
            let parent_refs: Vec<_> = parents.iter().collect();
            let sig =
                Signature::new("test", "test@example.com", &Time::new(1_900_000_005, 0)).unwrap();
            let distinct_oid = remote_repo
                .commit(
                    None,
                    &sig,
                    &sig,
                    "simulate distinct remote winner",
                    &tree,
                    &parent_refs,
                )
                .unwrap();
            update_ref(
                &remote_repo,
                "refs/heads/beads/store",
                distinct_oid,
                "rewrite remote winner",
            )
            .unwrap();
            distinct_oid
        };
        update_ref(
            &remote_repo,
            "refs/heads/beads/store",
            base_oid,
            "reset remote to legacy base",
        )
        .unwrap();

        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut options = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut options), None).unwrap();
        }
        local_repo
            .reference(
                "refs/heads/beads/store",
                base_oid,
                true,
                "init local legacy store",
            )
            .unwrap();

        let outcome = migrate_store_ref_to_v1_with_before_push(
            &local_repo,
            &local_dir,
            false,
            false,
            false,
            1,
            |retries, _commit_oid| {
                if retries == 0 {
                    update_ref(
                        &remote_repo,
                        "refs/heads/beads/store",
                        winner_oid,
                        "simulate remote winner before first push",
                    )?;
                }
                Ok(())
            },
        )
        .unwrap();

        assert!(
            outcome.commit_oid.is_none(),
            "retry should converge to the remote winner without minting another commit: {outcome:?}"
        );
        assert_eq!(outcome.push, MigratePushDisposition::SkippedNoPush);
        assert_eq!(outcome.from_effective_version, 0);
        assert_eq!(outcome.deps_format_before, wire::DepsFormat::LegacyEdges);
        assert!(outcome.converted_deps);
        assert!(outcome.added_notes_file);
        assert!(outcome.wrote_checksums);
        assert!(
            outcome.warnings.is_empty(),
            "base legacy fixture should not invent warnings: {outcome:?}"
        );
        assert_eq!(
            local_repo.refname_to_id("refs/heads/beads/store").unwrap(),
            winner_oid,
            "local store ref should realign to the fetched remote winner"
        );
        assert_eq!(
            local_repo
                .refname_to_id("refs/remotes/origin/beads/store")
                .unwrap(),
            winner_oid,
            "retry fetch should refresh remote tracking to the winner"
        );
        let backup_ref = format!("refs/beads/backup/{base_oid}");
        assert_eq!(
            local_repo.refname_to_id(&backup_ref).unwrap(),
            base_oid,
            "original local ref should still be backed up after the lost race"
        );
    }

    #[test]
    fn load_store_reads_current_beads_store_ref() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let stamp = WriteStamp::new(4321, 9);
        let oid = write_store_commit_with_meta(&repo, None, "store-ref", Some(stamp.clone()));

        let loaded = load_store(&repo).unwrap();
        let state = load_state(&repo).unwrap();

        assert_eq!(repo.refname_to_id("refs/heads/beads/store").unwrap(), oid);
        assert_eq!(
            loaded.meta.last_write_stamp(),
            Some(&stamp),
            "expected load_store to read from refs/heads/beads/store"
        );
        assert_eq!(state.iter_live().count(), loaded.state.iter_live().count());
    }

    #[test]
    fn fetch_preserves_local_ahead_and_creates_backup() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let remote_oid = write_store_commit(&remote_repo, None, "remote");

        // Fetch remote into local tracking ref.
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut fo = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut fo), None).unwrap();
        }

        local_repo
            .reference("refs/heads/beads/store", remote_oid, true, "init local")
            .unwrap();

        let local_oid = write_store_commit(&local_repo, Some(remote_oid), "local");
        assert_ne!(local_oid, remote_oid);

        let _fetched = SyncProcess::new(local_dir).fetch(&local_repo).unwrap();

        let local_after = local_repo.refname_to_id("refs/heads/beads/store").unwrap();
        assert_eq!(local_after, local_oid);

        let backup_ref = format!("refs/beads/backup/{}", local_oid);
        let backup_oid = local_repo.refname_to_id(&backup_ref).unwrap();
        assert_eq!(backup_oid, local_oid);
    }

    #[test]
    fn ensure_backup_ref_prunes_to_max_and_keeps_latest() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();

        let mut parent = None;
        let mut latest = Oid::zero();
        for i in 0..(MAX_BACKUP_REFS + 10) {
            let oid = write_store_commit(&repo, parent, &format!("commit-{i}"));
            parent = Some(oid);
            ensure_backup_ref_with_observer(&repo, oid, &NoopSyncObserver).unwrap();
            latest = oid;
        }

        let backup_ref_count = repo
            .references_glob(BACKUP_REFS_GLOB)
            .unwrap()
            .filter_map(Result::ok)
            .count();
        assert!(backup_ref_count <= MAX_BACKUP_REFS);

        let latest_ref = format!("{BACKUP_REF_PREFIX}{latest}");
        let latest_oid = repo.refname_to_id(&latest_ref).unwrap();
        assert_eq!(latest_oid, latest);
    }

    #[test]
    fn ensure_backup_ref_recovers_from_stale_lockfile() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let oid = write_store_commit(&repo, None, "commit");
        let backup_ref = format!("{BACKUP_REF_PREFIX}{oid}");
        let lock_path = backup_ref_lock_path(&repo, &backup_ref);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&lock_path, format!("pid={}\n", i32::MAX)).unwrap();

        ensure_backup_ref_with_observer(&repo, oid, &NoopSyncObserver).unwrap();

        assert_eq!(repo.refname_to_id(&backup_ref).unwrap(), oid);
        assert!(!lock_path.exists());
    }

    #[test]
    fn prune_backup_refs_skips_live_lock_without_failing_refresh_path() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let oid = write_store_commit(&repo, None, "commit");
        let backup_ref = format!("{BACKUP_REF_PREFIX}{oid}");
        repo.reference(&backup_ref, oid, false, "test backup")
            .unwrap();
        let lock_path = backup_ref_lock_path(&repo, &backup_ref);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&lock_path, format!("pid={}\n", std::process::id())).unwrap();

        prune_backup_refs_with_observer(&repo, 0, Oid::zero(), &NoopSyncObserver).unwrap();

        assert_eq!(repo.refname_to_id(&backup_ref).unwrap(), oid);
        assert!(lock_path.exists());
    }

    #[test]
    fn backup_ref_lock_contention_metrics_for_create_and_delete() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let observer = RecordingSyncObserver::default();
        let create_oid = write_store_commit(&repo, None, "create-target");
        let create_ref = format!("{BACKUP_REF_PREFIX}{create_oid}");
        let create_lock_path = backup_ref_lock_path(&repo, &create_ref);
        if let Some(parent) = create_lock_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&create_lock_path, format!("pid={}\n", std::process::id())).unwrap();

        let before_create_lock = observer.lock_contention_count("create");
        let before_create_cleanup = observer.lock_cleanup_count("create", "pid_alive");

        assert!(
            !create_backup_reference_with_observer(&repo, &create_ref, create_oid, &observer)
                .unwrap()
        );

        let after_create_lock = observer.lock_contention_count("create");
        let after_create_cleanup = observer.lock_cleanup_count("create", "pid_alive");
        assert!(
            after_create_lock >= before_create_lock + 1,
            "expected create lock contention callback to increment"
        );
        assert!(
            after_create_cleanup >= before_create_cleanup + 1,
            "expected create lock cleanup callback to increment"
        );

        let delete_oid = write_store_commit(&repo, Some(create_oid), "delete-target");
        let delete_ref = format!("{BACKUP_REF_PREFIX}{delete_oid}");
        repo.reference(&delete_ref, delete_oid, false, "test backup")
            .unwrap();
        let delete_lock_path = backup_ref_lock_path(&repo, &delete_ref);
        if let Some(parent) = delete_lock_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&delete_lock_path, format!("pid={}\n", std::process::id())).unwrap();

        let before_delete_lock = observer.lock_contention_count("delete");
        let before_delete_cleanup = observer.lock_cleanup_count("delete", "pid_alive");

        assert!(!delete_backup_reference_with_observer(&repo, &delete_ref, &observer).unwrap());
        assert_eq!(repo.refname_to_id(&delete_ref).unwrap(), delete_oid);

        let after_delete_lock = observer.lock_contention_count("delete");
        let after_delete_cleanup = observer.lock_cleanup_count("delete", "pid_alive");
        assert!(
            after_delete_lock >= before_delete_lock + 1,
            "expected delete lock contention callback to increment"
        );
        assert!(
            after_delete_cleanup >= before_delete_cleanup + 1,
            "expected delete lock cleanup callback to increment"
        );
    }

    #[test]
    fn fetch_prunes_existing_backup_refs() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();

        let mut parent = None;
        let mut backup_oids = Vec::new();
        for i in 0..(MAX_BACKUP_REFS + 10) {
            let oid = write_store_commit(&repo, parent, &format!("commit-{i}"));
            parent = Some(oid);
            backup_oids.push(oid);
        }
        for oid in backup_oids {
            let name = format!("{BACKUP_REF_PREFIX}{oid}");
            repo.reference(&name, oid, false, "test backup").unwrap();
        }

        let backup_ref_count_before = repo
            .references_glob(BACKUP_REFS_GLOB)
            .unwrap()
            .filter_map(Result::ok)
            .count();
        assert!(backup_ref_count_before > MAX_BACKUP_REFS);

        let _fetched = SyncProcess::new(tmp.path().to_path_buf())
            .fetch(&repo)
            .unwrap();

        let backup_ref_count_after = repo
            .references_glob(BACKUP_REFS_GLOB)
            .unwrap()
            .filter_map(Result::ok)
            .count();
        assert!(backup_ref_count_after <= MAX_BACKUP_REFS);
    }

    #[test]
    fn prune_backup_refs_recovers_from_stale_lockfile() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let oid = write_store_commit(&repo, None, "commit");
        let backup_ref = format!("{BACKUP_REF_PREFIX}{oid}");
        repo.reference(&backup_ref, oid, false, "test backup")
            .unwrap();
        let lock_path = backup_ref_lock_path(&repo, &backup_ref);
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&lock_path, format!("pid={}\n", i32::MAX)).unwrap();

        prune_backup_refs_with_observer(&repo, 0, Oid::zero(), &NoopSyncObserver).unwrap();

        let err = repo
            .refname_to_id(&backup_ref)
            .err()
            .expect("expected ref to be pruned");
        assert_eq!(err.code(), ErrorCode::NotFound);
        assert!(!lock_path.exists());
    }

    #[test]
    fn backup_lock_cleanup_policy_is_age_and_pid_aware() {
        let stale_after = Duration::from_secs(10);
        assert_eq!(
            should_cleanup_backup_ref_lock(Some(Duration::from_secs(1)), None, stale_after),
            (false, "age_recent")
        );
        assert_eq!(
            should_cleanup_backup_ref_lock(Some(Duration::from_secs(30)), None, stale_after),
            (true, "age_stale")
        );
        assert_eq!(
            should_cleanup_backup_ref_lock(
                Some(Duration::from_secs(1)),
                Some(BackupLockPidState::Alive),
                stale_after,
            ),
            (false, "pid_alive")
        );
        assert_eq!(
            should_cleanup_backup_ref_lock(
                Some(Duration::from_secs(1)),
                Some(BackupLockPidState::Missing),
                stale_after,
            ),
            (true, "pid_missing")
        );
        assert_eq!(
            should_cleanup_backup_ref_lock(
                Some(Duration::from_secs(1)),
                Some(BackupLockPidState::Unknown),
                stale_after,
            ),
            (false, "age_recent_pid_unknown")
        );
    }

    #[test]
    fn prune_backup_refs_reports_scan_and_pruned_metrics() {
        let tmp = TempDir::new().unwrap();
        let repo = Repository::init(tmp.path()).unwrap();
        let observer = RecordingSyncObserver::default();
        let mut parent = None;
        for i in 0..6 {
            let oid = write_store_commit(&repo, parent, &format!("commit-{i}"));
            parent = Some(oid);
            let backup_ref = format!("{BACKUP_REF_PREFIX}{oid}");
            repo.reference(&backup_ref, oid, false, "test backup")
                .unwrap();
        }

        let before_scan_samples = observer.backup_ref_scan_samples();
        let before_pruned_total = observer.backup_ref_pruned_total();

        prune_backup_refs_with_observer(&repo, 2, Oid::zero(), &observer).unwrap();

        let after_scan_samples = observer.backup_ref_scan_samples();
        let after_pruned_total = observer.backup_ref_pruned_total();
        assert!(
            after_scan_samples >= before_scan_samples + 1,
            "expected at least one backup ref scan callback"
        );
        assert!(
            after_pruned_total >= before_pruned_total + 4,
            "expected at least four pruned refs in callbacks"
        );
    }

    #[test]
    fn fetch_best_effort_surfaces_error() {
        let tmp = TempDir::new().unwrap();
        let local_dir = tmp.path().join("local");
        let local_repo = Repository::init(&local_dir).unwrap();

        local_repo.remote("origin", "/does/not/exist").unwrap();

        let fetched = SyncProcess::new(local_dir)
            .fetch_best_effort(&local_repo)
            .unwrap();
        assert!(fetched.phase.fetch_error.is_some());
    }

    #[test]
    fn fetch_updates_remote_tracking_ref() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let remote_oid = write_store_commit(&remote_repo, None, "remote");

        let _fetched = SyncProcess::new(local_dir).fetch(&local_repo).unwrap();
        let tracking_oid = local_repo
            .refname_to_id("refs/remotes/origin/beads/store")
            .unwrap();
        assert_eq!(tracking_oid, remote_oid);
    }

    #[test]
    fn fetch_fast_forwards_when_remote_ahead() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let local_v1 = write_store_commit(&local_repo, None, "local-v1");
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut push_options = git2::PushOptions::new();
            remote
                .push(
                    &["refs/heads/beads/store:refs/heads/beads/store"],
                    Some(&mut push_options),
                )
                .unwrap();
        }

        let remote_v2 = write_store_commit(&remote_repo, Some(local_v1), "remote-v2");

        let _fetched = SyncProcess::new(local_dir).fetch(&local_repo).unwrap();

        let local_after = local_repo.refname_to_id("refs/heads/beads/store").unwrap();
        assert_eq!(local_after, remote_v2);
    }

    #[test]
    fn fetch_detects_divergence() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let remote_oid = write_store_commit(&remote_repo, None, "remote");

        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut fo = git2::FetchOptions::new();
            let refspec = "refs/heads/beads/store:refs/remotes/origin/beads/store";
            remote.fetch(&[refspec], Some(&mut fo), None).unwrap();
        }

        let local_oid = write_store_commit(&local_repo, None, "local");
        assert_ne!(local_oid, remote_oid);

        let fetched = SyncProcess::new(local_dir).fetch(&local_repo).unwrap();
        assert!(fetched.phase.divergence.is_some());
    }

    #[test]
    fn sync_with_retry_reports_divergence() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let _remote_oid = write_store_commit(&remote_repo, None, "remote");
        let _local_oid = write_store_commit(&local_repo, None, "local");

        let outcome = sync_with_retry(&local_repo, &local_dir, &CanonicalState::new(), 1).unwrap();

        assert!(outcome.divergence.is_some());
    }

    #[test]
    fn sync_with_retry_respects_max_retries_on_non_fast_forward() {
        use std::fs;

        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let _remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let _base_oid = write_store_commit(&local_repo, None, "base");
        {
            let mut remote = local_repo.find_remote("origin").unwrap();
            let mut push_options = git2::PushOptions::new();
            remote
                .push(
                    &["refs/heads/beads/store:refs/heads/beads/store"],
                    Some(&mut push_options),
                )
                .unwrap();
        }

        let lock_path = remote_dir.join("refs/heads/beads/store.lock");
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&lock_path, b"lock").unwrap();

        let result = sync_with_retry(&local_repo, &local_dir, &CanonicalState::new(), 0);

        match result {
            Err(SyncError::TooManyRetries(retries)) => assert_eq!(retries, 1),
            other => panic!("expected TooManyRetries, got {other:?}"),
        }
    }

    #[cfg(feature = "slow-tests")]
    #[derive(Debug, Clone, Copy)]
    enum Relation {
        BothMissing,
        LocalOnly,
        RemoteOnly,
        Same,
        LocalAhead,
        RemoteAhead,
        Diverged,
    }

    #[cfg(feature = "slow-tests")]
    fn relation_cases() -> [Relation; 7] {
        [
            Relation::BothMissing,
            Relation::LocalOnly,
            Relation::RemoteOnly,
            Relation::Same,
            Relation::LocalAhead,
            Relation::RemoteAhead,
            Relation::Diverged,
        ]
    }

    #[cfg(feature = "slow-tests")]
    fn nonzero_steps(raw: u8) -> u8 {
        (raw % 3) + 1
    }

    #[cfg(feature = "slow-tests")]
    fn push_store(local_repo: &Repository) -> Result<(), TestCaseError> {
        let mut remote = local_repo
            .find_remote("origin")
            .map_err(|e| TestCaseError::fail(format!("find remote: {e}")))?;
        let mut push_options = git2::PushOptions::new();
        remote
            .push(
                &["refs/heads/beads/store:refs/heads/beads/store"],
                Some(&mut push_options),
            )
            .map_err(|e| TestCaseError::fail(format!("push: {e}")))?;
        Ok(())
    }

    #[cfg(feature = "slow-tests")]
    fn write_chain(repo: &Repository, parent: Option<Oid>, steps: u8, label: &str) -> Option<Oid> {
        let mut current = parent;
        for idx in 0..steps {
            let msg = format!("{label}-{idx}");
            current = Some(write_store_commit(repo, current, &msg));
        }
        current
    }

    #[cfg(feature = "slow-tests")]
    #[derive(Debug, Clone)]
    enum Action {
        LocalCommit(u8),
        RemoteCommit(u8),
        PushLocal,
        ForcePushRemote,
        Fetch,
    }

    #[cfg(feature = "slow-tests")]
    fn fetch_sequence_cases() -> Vec<(&'static str, Vec<Action>)> {
        vec![
            (
                "local_only_fetch_noop",
                vec![Action::LocalCommit(1), Action::Fetch],
            ),
            (
                "remote_only_fetch_imports_remote",
                vec![Action::RemoteCommit(1), Action::Fetch],
            ),
            (
                "same_head_fetch_noop",
                vec![Action::LocalCommit(1), Action::PushLocal, Action::Fetch],
            ),
            (
                "local_ahead_fetch_preserves_local",
                vec![
                    Action::LocalCommit(1),
                    Action::PushLocal,
                    Action::LocalCommit(2),
                    Action::Fetch,
                ],
            ),
            (
                "remote_ahead_fetch_fast_forwards",
                vec![
                    Action::LocalCommit(1),
                    Action::PushLocal,
                    Action::RemoteCommit(2),
                    Action::Fetch,
                ],
            ),
            (
                "diverged_fetch_preserves_local_and_backup",
                vec![
                    Action::LocalCommit(1),
                    Action::PushLocal,
                    Action::LocalCommit(1),
                    Action::RemoteCommit(1),
                    Action::Fetch,
                ],
            ),
            (
                "repeat_fetch_after_divergence_is_stable",
                vec![
                    Action::LocalCommit(1),
                    Action::PushLocal,
                    Action::LocalCommit(1),
                    Action::RemoteCommit(1),
                    Action::Fetch,
                    Action::Fetch,
                ],
            ),
            (
                "non_fast_forward_push_is_ignored_then_fetch_recovers",
                vec![
                    Action::LocalCommit(1),
                    Action::PushLocal,
                    Action::RemoteCommit(1),
                    Action::PushLocal,
                    Action::Fetch,
                ],
            ),
            (
                "force_push_after_tracking_creates_backup",
                vec![
                    Action::LocalCommit(1),
                    Action::PushLocal,
                    Action::Fetch,
                    Action::LocalCommit(1),
                    Action::ForcePushRemote,
                    Action::Fetch,
                ],
            ),
            (
                "interleaved_remote_and_local_updates",
                vec![
                    Action::LocalCommit(1),
                    Action::PushLocal,
                    Action::RemoteCommit(1),
                    Action::Fetch,
                    Action::LocalCommit(1),
                    Action::RemoteCommit(1),
                    Action::Fetch,
                ],
            ),
        ]
    }

    #[cfg(feature = "slow-tests")]
    fn local_ref(repo: &Repository) -> Result<Option<Oid>, TestCaseError> {
        match refname_to_id_optional(repo, "refs/heads/beads/store") {
            Ok(oid) => Ok(oid),
            Err(e) => Err(TestCaseError::fail(format!("local ref: {e}"))),
        }
    }

    #[cfg(feature = "slow-tests")]
    fn remote_ref(repo: &Repository) -> Result<Option<Oid>, TestCaseError> {
        match refname_to_id_optional(repo, "refs/heads/beads/store") {
            Ok(oid) => Ok(oid),
            Err(e) => Err(TestCaseError::fail(format!("remote ref: {e}"))),
        }
    }

    #[cfg(feature = "slow-tests")]
    struct SyncTestRepos {
        _tmp: TempDir,
        local_dir: PathBuf,
        local_repo: Repository,
        remote_repo: Repository,
    }

    #[cfg(feature = "slow-tests")]
    struct RelationFixture {
        repos: SyncTestRepos,
        local_head: Option<Oid>,
        remote_head: Option<Oid>,
    }

    #[cfg(feature = "slow-tests")]
    fn sync_test_repos() -> Result<SyncTestRepos, TestCaseError> {
        let tmp = TempDir::new().map_err(|e| TestCaseError::fail(format!("tempdir: {e}")))?;
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir)
            .map_err(|e| TestCaseError::fail(format!("init remote: {e}")))?;
        let local_repo = Repository::init(&local_dir)
            .map_err(|e| TestCaseError::fail(format!("init local: {e}")))?;
        let remote_url = remote_dir
            .to_str()
            .ok_or_else(|| TestCaseError::fail("remote path not utf8"))?;
        local_repo
            .remote("origin", remote_url)
            .map_err(|e| TestCaseError::fail(format!("add remote: {e}")))?;

        Ok(SyncTestRepos {
            _tmp: tmp,
            local_dir,
            local_repo,
            remote_repo,
        })
    }

    #[cfg(feature = "slow-tests")]
    fn setup_relation_fixture(
        relation: Relation,
        local_steps: u8,
        remote_steps: u8,
    ) -> Result<RelationFixture, TestCaseError> {
        let repos = sync_test_repos()?;
        let local_repo = &repos.local_repo;
        let remote_repo = &repos.remote_repo;
        let mut local_head = None;
        let mut remote_head = None;

        match relation {
            Relation::BothMissing => {}
            Relation::LocalOnly => {
                local_head = write_chain(local_repo, None, nonzero_steps(local_steps), "local");
            }
            Relation::RemoteOnly => {
                remote_head = write_chain(remote_repo, None, nonzero_steps(remote_steps), "remote");
            }
            Relation::Same => {
                local_head = write_chain(local_repo, None, 1, "base");
                push_store(local_repo)?;
                remote_head = local_head;
            }
            Relation::LocalAhead => {
                local_head = write_chain(local_repo, None, 1, "base");
                push_store(local_repo)?;
                remote_head = local_head;
                local_head =
                    write_chain(local_repo, local_head, nonzero_steps(local_steps), "local");
            }
            Relation::RemoteAhead => {
                local_head = write_chain(local_repo, None, 1, "base");
                push_store(local_repo)?;
                remote_head = local_head;
                remote_head = write_chain(
                    remote_repo,
                    remote_head,
                    nonzero_steps(remote_steps),
                    "remote",
                );
            }
            Relation::Diverged => {
                local_head = write_chain(local_repo, None, 1, "base");
                push_store(local_repo)?;
                remote_head = local_head;
                local_head =
                    write_chain(local_repo, local_head, nonzero_steps(local_steps), "local");
                remote_head = write_chain(
                    remote_repo,
                    remote_head,
                    nonzero_steps(remote_steps),
                    "remote",
                );
            }
        }

        Ok(RelationFixture {
            repos,
            local_head,
            remote_head,
        })
    }

    #[cfg(feature = "slow-tests")]
    fn seed_remote_tracking(local_repo: &Repository, remote_oid: Oid) -> Result<(), TestCaseError> {
        local_repo
            .reference(
                "refs/remotes/origin/beads/store",
                remote_oid,
                true,
                "seed remote tracking ref for fetch test",
            )
            .map(|_| ())
            .map_err(|e| TestCaseError::fail(format!("seed remote tracking: {e}")))
    }

    #[cfg(feature = "slow-tests")]
    fn run_fetch_respects_history_invariants_case(
        relation: Relation,
        local_steps: u8,
        remote_steps: u8,
    ) -> Result<(), TestCaseError> {
        let fixture = setup_relation_fixture(relation, local_steps, remote_steps)?;
        let local_repo = &fixture.repos.local_repo;
        let local_before = refname_to_id_optional(local_repo, "refs/heads/beads/store")
            .map_err(|e| TestCaseError::fail(format!("local before: {e}")))?;
        prop_assert_eq!(local_before, fixture.local_head);

        let fetched = SyncProcess::new(fixture.repos.local_dir.clone())
            .fetch(local_repo)
            .map_err(|e| TestCaseError::fail(format!("fetch: {e}")))?;

        let local_after = refname_to_id_optional(local_repo, "refs/heads/beads/store")
            .map_err(|e| TestCaseError::fail(format!("local after: {e}")))?;
        let remote_after = refname_to_id_optional(local_repo, "refs/remotes/origin/beads/store")
            .map_err(|e| TestCaseError::fail(format!("remote tracking: {e}")))?;

        if let Some(remote_oid) = fixture.remote_head {
            prop_assert_eq!(remote_after, Some(remote_oid));
            match local_before {
                None => {
                    prop_assert_eq!(local_after, Some(remote_oid));
                }
                Some(local_oid) => {
                    if local_oid == remote_oid {
                        prop_assert_eq!(local_after, Some(local_oid));
                    } else if local_repo
                        .graph_descendant_of(remote_oid, local_oid)
                        .map_err(|e| TestCaseError::fail(format!("descendant: {e}")))?
                    {
                        prop_assert_eq!(local_after, Some(remote_oid));
                    } else {
                        prop_assert_eq!(local_after, Some(local_oid));
                        let backup_ref = format!("refs/beads/backup/{local_oid}");
                        let backup_oid = refname_to_id_optional(local_repo, &backup_ref)
                            .map_err(|e| TestCaseError::fail(format!("backup ref: {e}")))?;
                        prop_assert_eq!(backup_oid, Some(local_oid));
                    }
                }
            }
        } else {
            prop_assert_eq!(remote_after, None);
            prop_assert_eq!(local_after, local_before);
            if let Some(local_oid) = local_before {
                prop_assert_eq!(fetched.phase.remote_oid, local_oid);
            } else {
                prop_assert!(fetched.phase.remote_oid.is_zero());
            }
        }
        Ok(())
    }

    #[cfg(feature = "slow-tests")]
    #[test]
    fn fetch_respects_history_invariants() {
        for relation in relation_cases() {
            for local_steps in 0u8..=3 {
                for remote_steps in 0u8..=3 {
                    run_fetch_respects_history_invariants_case(
                        relation,
                        local_steps,
                        remote_steps,
                    )
                    .unwrap_or_else(|err| {
                        panic!(
                            "fetch_respects_history_invariants failed for relation={relation:?} local_steps={local_steps} remote_steps={remote_steps}: {err:?}"
                        )
                    });
                }
            }
        }
    }

    #[cfg(feature = "slow-tests")]
    fn run_fetch_sequence_preserves_local_history_case(
        actions: &[Action],
    ) -> Result<(), TestCaseError> {
        let repos = sync_test_repos()?;
        let remote_repo = &repos.remote_repo;
        let local_repo = &repos.local_repo;
        let local_dir = repos.local_dir.clone();

        for action in actions {
            match action {
                Action::LocalCommit(steps) => {
                    let parent = local_ref(local_repo)?;
                    write_chain(local_repo, parent, *steps, "local");
                }
                Action::RemoteCommit(steps) => {
                    let parent = remote_ref(remote_repo)?;
                    write_chain(remote_repo, parent, *steps, "remote");
                }
                Action::PushLocal => {
                    if let Err(err) = push_store(local_repo) {
                        let _ = err;
                    }
                }
                Action::ForcePushRemote => {
                    let _ = write_chain(remote_repo, None, 1, "force");
                }
                Action::Fetch => {
                    let local_before = local_ref(local_repo)?;
                    let remote_before = remote_ref(remote_repo)?;

                    let fetched = SyncProcess::new(local_dir.clone())
                        .fetch(local_repo)
                        .map_err(|e| TestCaseError::fail(format!("fetch: {e}")))?;

                    let local_after = local_ref(local_repo)?;
                    let remote_tracking =
                        refname_to_id_optional(local_repo, "refs/remotes/origin/beads/store")
                            .map_err(|e| TestCaseError::fail(format!("remote tracking: {e}")))?;

                    if let Some(remote_oid) = remote_before {
                        prop_assert_eq!(remote_tracking, Some(remote_oid));
                        match local_before {
                            None => {
                                prop_assert_eq!(local_after, Some(remote_oid));
                            }
                            Some(local_oid) => {
                                if local_oid == remote_oid {
                                    prop_assert_eq!(local_after, Some(local_oid));
                                } else {
                                    let remote_is_descendant = local_repo
                                        .graph_descendant_of(remote_oid, local_oid)
                                        .map_err(|e| {
                                            TestCaseError::fail(format!("descendant: {e}"))
                                        })?;
                                    if remote_is_descendant {
                                        prop_assert_eq!(local_after, Some(remote_oid));
                                    } else {
                                        prop_assert_eq!(local_after, Some(local_oid));
                                        let backup_ref = format!("refs/beads/backup/{local_oid}");
                                        let backup_oid =
                                            refname_to_id_optional(local_repo, &backup_ref)
                                                .map_err(|e| {
                                                    TestCaseError::fail(format!("backup ref: {e}"))
                                                })?;
                                        prop_assert_eq!(backup_oid, Some(local_oid));
                                        let local_is_descendant = local_repo
                                            .graph_descendant_of(local_oid, remote_oid)
                                            .map_err(|e| {
                                                TestCaseError::fail(format!("descendant: {e}"))
                                            })?;
                                        prop_assert!(
                                            fetched.phase.divergence.is_some()
                                                || local_is_descendant
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        prop_assert_eq!(remote_tracking, None);
                        prop_assert_eq!(local_after, local_before);
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "slow-tests")]
    #[test]
    fn fetch_sequence_preserves_local_history() {
        for (name, actions) in fetch_sequence_cases() {
            run_fetch_sequence_preserves_local_history_case(&actions).unwrap_or_else(|err| {
                panic!("fetch_sequence_preserves_local_history failed for {name}: {err:?}")
            });
        }
    }

    #[cfg(feature = "slow-tests")]
    fn run_sync_with_retry_surfaces_fetch_divergence_case(
        relation: Relation,
        local_steps: u8,
        remote_steps: u8,
    ) -> Result<(), TestCaseError> {
        let fixture = setup_relation_fixture(relation, local_steps, remote_steps)?;
        let local_repo = &fixture.repos.local_repo;
        let expected_diverged = matches!(relation, Relation::Diverged);

        let outcome = sync_with_retry(
            local_repo,
            &fixture.repos.local_dir,
            &CanonicalState::new(),
            1,
        )
        .map_err(|e| TestCaseError::fail(format!("sync_with_retry: {e}")))?;

        prop_assert_eq!(outcome.divergence.is_some(), expected_diverged);

        let local_after = local_ref(local_repo)?;
        let remote_after = refname_to_id_optional(local_repo, "refs/remotes/origin/beads/store")
            .map_err(|e| TestCaseError::fail(format!("remote tracking: {e}")))?;
        if let (Some(local_oid), Some(remote_oid)) = (local_after, remote_after) {
            let remote_is_descendant = remote_oid == local_oid
                || local_repo
                    .graph_descendant_of(remote_oid, local_oid)
                    .map_err(|e| TestCaseError::fail(format!("descendant: {e}")))?;
            if !remote_is_descendant {
                let backup_ref = format!("refs/beads/backup/{local_oid}");
                let backup_oid = refname_to_id_optional(local_repo, &backup_ref)
                    .map_err(|e| TestCaseError::fail(format!("backup ref: {e}")))?;
                prop_assert_eq!(backup_oid, Some(local_oid));
            }
        }
        Ok(())
    }

    #[cfg(feature = "slow-tests")]
    #[test]
    fn sync_with_retry_surfaces_fetch_divergence() {
        for relation in relation_cases() {
            for local_steps in 0u8..=3 {
                for remote_steps in 0u8..=3 {
                    run_sync_with_retry_surfaces_fetch_divergence_case(
                        relation,
                        local_steps,
                        remote_steps,
                    )
                    .unwrap_or_else(|err| {
                        panic!(
                            "sync_with_retry_surfaces_fetch_divergence failed for relation={relation:?} local_steps={local_steps} remote_steps={remote_steps}: {err:?}"
                        )
                    });
                }
            }
        }
    }

    #[cfg(feature = "slow-tests")]
    fn run_fetch_detects_force_push_case(rewrite_steps: u8) -> Result<(), TestCaseError> {
        let repos = sync_test_repos()?;
        let base = write_store_commit(&repos.local_repo, None, "base");
        push_store(&repos.local_repo)?;
        seed_remote_tracking(&repos.local_repo, base)?;
        let local_oid = write_store_commit(&repos.local_repo, Some(base), "local");
        let _ = write_chain(&repos.remote_repo, None, rewrite_steps, "force");

        let fetched = SyncProcess::new(repos.local_dir.clone())
            .fetch(&repos.local_repo)
            .map_err(|e| TestCaseError::fail(format!("fetch: {e}")))?;

        prop_assert!(fetched.phase.force_push.is_some());
        let local_after = local_ref(&repos.local_repo)?;
        prop_assert_eq!(local_after, Some(local_oid));

        let backup_ref = format!("refs/beads/backup/{local_oid}");
        let backup_oid = refname_to_id_optional(&repos.local_repo, &backup_ref)
            .map_err(|e| TestCaseError::fail(format!("backup ref: {e}")))?;
        prop_assert_eq!(backup_oid, Some(local_oid));
        Ok(())
    }

    #[cfg(feature = "slow-tests")]
    #[test]
    fn fetch_detects_force_push() {
        for rewrite_steps in 1u8..=3 {
            run_fetch_detects_force_push_case(rewrite_steps).unwrap_or_else(|err| {
                panic!("fetch_detects_force_push failed for rewrite_steps={rewrite_steps}: {err:?}")
            });
        }
    }

    #[test]
    fn init_does_not_override_existing_local_ref() {
        let tmp = TempDir::new().unwrap();
        let remote_dir = tmp.path().join("remote");
        let local_dir = tmp.path().join("local");

        let remote_repo = Repository::init_bare(&remote_dir).unwrap();
        let local_repo = Repository::init(&local_dir).unwrap();
        local_repo
            .remote("origin", remote_dir.to_str().unwrap())
            .unwrap();

        let _remote_oid = write_store_commit(&remote_repo, None, "remote");
        let local_oid = write_store_commit(&local_repo, None, "local");

        init_beads_ref(&local_repo, 1).unwrap();

        let local_after = local_repo.refname_to_id("refs/heads/beads/store").unwrap();
        assert_eq!(local_after, local_oid);
    }
}
