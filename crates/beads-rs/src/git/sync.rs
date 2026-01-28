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

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

use git2::{ErrorCode, ObjectType, Oid, Repository, Signature};

use super::error::SyncError;
use super::wire;
use crate::core::{BeadId, BeadSlug, CanonicalState, WallClock, WriteStamp};

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
    pub phase: Phase,
}

impl SyncProcess<Idle> {
    /// Create a new sync process in Idle phase.
    pub fn new(repo_path: PathBuf) -> Self {
        Self::new_with_config(repo_path, SyncConfig::from_env())
    }

    pub fn new_with_config(repo_path: PathBuf, config: SyncConfig) -> Self {
        SyncProcess {
            repo_path,
            config,
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
            phase: _,
        } = self;
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
                            ensure_backup_ref(repo, local_oid)?;
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

        Ok(SyncProcess {
            repo_path,
            config,
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

        let parent_oid = if remote_oid.is_zero() {
            local_oid
        } else {
            remote_oid
        };

        // Fast path: if remote is empty/uninitialized, just use local
        if remote_oid.is_zero() {
            let diff = compute_diff(&CanonicalState::new(), local_state);
            return Ok(SyncProcess {
                repo_path,
                config,
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
        let mut merged = CanonicalState::join(local_state, &remote_state)
            .map_err(|errs| SyncError::MergeConflict { errors: errs })?;

        // Garbage collect tombstones if configured.
        if let Some(ttl_ms) = config.tombstone_ttl_ms {
            let removed = merged.gc_tombstones(ttl_ms, WallClock::now());
            if removed > 0 {
                tracing::info!(removed, ttl_ms, "tombstone GC pruned records");
            }
        }

        // Compute diff for commit message
        let diff = compute_diff(&remote_state, &merged);

        Ok(SyncProcess {
            repo_path,
            config,
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

        // Serialize state to blobs
        let state_bytes = wire::serialize_state(&state)?;
        let tombs_bytes = wire::serialize_tombstones(&state)?;
        let deps_bytes = wire::serialize_deps(&state)?;
        let notes_bytes = wire::serialize_notes(&state)?;
        let mut meta_last_stamp = state.max_write_stamp();
        if let Some(parent_stamp) = parent_meta_stamp {
            meta_last_stamp = match meta_last_stamp {
                Some(state_stamp) => Some(std::cmp::max(state_stamp, parent_stamp)),
                None => Some(parent_stamp),
            };
        }
        let checksums = wire::StoreChecksums::from_bytes(
            &state_bytes,
            &tombs_bytes,
            &deps_bytes,
            Some(&notes_bytes),
        );
        let meta_bytes = wire::serialize_meta(
            root_slug.as_ref().map(BeadSlug::as_str),
            meta_last_stamp.as_ref(),
            &checksums,
        )?;

        // Write blobs to ODB
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

        // Create signature
        let sig = Signature::now("beads", "beads@localhost")?;

        // Commit message
        let message = diff.to_commit_message();

        // Create commit with single parent (linear history)
        let parents: Vec<_> = if parent_oid.is_zero() {
            // Initial commit - no parents
            vec![]
        } else {
            let parent_commit = repo.find_commit(parent_oid)?;
            vec![parent_commit]
        };

        let parent_refs: Vec<_> = parents.iter().collect();
        let commit_oid = repo.commit(None, &sig, &sig, &message, &tree, &parent_refs)?;
        update_ref(
            repo,
            "refs/heads/beads/store",
            commit_oid,
            "beads sync: update local ref",
        )?;

        Ok(SyncProcess {
            repo_path,
            config,
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
        let is_retryable = |message: &str| {
            let msg = message.to_lowercase();
            msg.contains("non-fast-forward")
                || msg.contains("fetch first")
                || msg.contains("cannot lock ref")
                || msg.contains("failed to update ref")
                || msg.contains("failed to lock file")
        };

        // Try to find remote - if no remote, just return success (local-only)
        let mut remote = match repo.find_remote("origin") {
            Ok(r) => r,
            Err(_) => {
                // No remote configured - this is a local-only repo
                // Commit was already made, so just return success
                return Ok(self.phase.state);
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
                if is_retryable(&msg) {
                    return Err(SyncError::NonFastForward);
                }
                return Err(SyncError::from(e));
            }
        }

        if let Some(err) = push_error.into_inner() {
            if is_retryable(&err) {
                return Err(SyncError::NonFastForward);
            }
            return Err(crate::git::error::PushRejected { message: err }.into());
        }

        Ok(self.phase.state)
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
    pub meta: wire::StoreMeta,
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
        wire::parse_meta(meta_blob.content())?
    } else {
        wire::StoreMeta::Legacy
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
    let mut retries = 0;
    let mut force_push = None;
    let started = Instant::now();

    loop {
        let attempt = retries + 1;
        tracing::debug!(attempt, "sync attempt");
        let fetched = SyncProcess::new(repo_path.to_owned()).fetch(repo)?;
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

fn ensure_backup_ref(repo: &Repository, oid: Oid) -> Result<(), SyncError> {
    let name = format!("refs/beads/backup/{}", oid);
    if repo.refname_to_id(&name).is_ok() {
        return Ok(());
    }
    repo.reference(&name, oid, false, "beads sync: backup local ref")?;
    Ok(())
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
                return Err(crate::git::error::PushRejected { message: err }.into());
            }
        }

        // Either no remote (local only) or push succeeded
        return Ok(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, Claim, ContentHash, DepKey, DepKind,
        Dot, Lww, Priority, ReplicaId, Stamp, Tombstone, Workflow,
    };
    use crate::git::WireError;
    #[cfg(feature = "slow-tests")]
    use proptest::prelude::*;
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
        checksums.state = ContentHash::from_bytes([0xAB; 32]);
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
        assert!(matches!(loaded.meta, wire::StoreMeta::Legacy));
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
    #[derive(Debug, Clone)]
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
    fn relation_strategy() -> impl Strategy<Value = Relation> {
        prop_oneof![
            Just(Relation::BothMissing),
            Just(Relation::LocalOnly),
            Just(Relation::RemoteOnly),
            Just(Relation::Same),
            Just(Relation::LocalAhead),
            Just(Relation::RemoteAhead),
            Just(Relation::Diverged),
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
    fn action_strategy() -> impl Strategy<Value = Action> {
        prop_oneof![
            (1u8..=3).prop_map(Action::LocalCommit),
            (1u8..=3).prop_map(Action::RemoteCommit),
            Just(Action::PushLocal),
            Just(Action::ForcePushRemote),
            Just(Action::Fetch),
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
    proptest! {
        #[test]
        fn fetch_respects_history_invariants(
            relation in relation_strategy(),
            local_steps in 0u8..=3,
            remote_steps in 0u8..=3,
        ) {
            let tmp = match TempDir::new() {
                Ok(tmp) => tmp,
                Err(e) => return Err(TestCaseError::fail(format!("tempdir: {e}"))),
            };
            let remote_dir = tmp.path().join("remote");
            let local_dir = tmp.path().join("local");

            let remote_repo = match Repository::init_bare(&remote_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init remote: {e}"))),
            };
            let local_repo = match Repository::init(&local_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init local: {e}"))),
            };
            let remote_url = match remote_dir.to_str() {
                Some(url) => url,
                None => return Err(TestCaseError::fail("remote path not utf8")),
            };
            if let Err(e) = local_repo.remote("origin", remote_url) {
                return Err(TestCaseError::fail(format!("add remote: {e}")));
            }

            let mut local_head: Option<Oid> = None;
            let mut remote_head: Option<Oid> = None;

            match relation {
                Relation::BothMissing => {}
                Relation::LocalOnly => {
                    let steps = nonzero_steps(local_steps);
                    local_head = write_chain(&local_repo, None, steps, "local");
                }
                Relation::RemoteOnly => {
                    let steps = nonzero_steps(remote_steps);
                    remote_head = write_chain(&remote_repo, None, steps, "remote");
                }
                Relation::Same => {
                    local_head = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                    remote_head = local_head;
                }
                Relation::LocalAhead => {
                    local_head = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                    remote_head = local_head;

                    let steps = nonzero_steps(local_steps);
                    local_head = write_chain(&local_repo, local_head, steps, "local");
                }
                Relation::RemoteAhead => {
                    local_head = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                    remote_head = local_head;

                    let steps = nonzero_steps(remote_steps);
                    remote_head = write_chain(&remote_repo, remote_head, steps, "remote");
                }
                Relation::Diverged => {
                    local_head = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                    remote_head = local_head;

                    let local_extra = nonzero_steps(local_steps);
                    let remote_extra = nonzero_steps(remote_steps);
                    local_head = write_chain(&local_repo, local_head, local_extra, "local");
                    remote_head = write_chain(&remote_repo, remote_head, remote_extra, "remote");
                }
            }

            let local_before = match refname_to_id_optional(&local_repo, "refs/heads/beads/store") {
                Ok(oid) => oid,
                Err(e) => return Err(TestCaseError::fail(format!("local before: {e}"))),
            };
            prop_assert_eq!(local_before, local_head);

            let fetched = match SyncProcess::new(local_dir.clone()).fetch(&local_repo) {
                Ok(result) => result,
                Err(e) => return Err(TestCaseError::fail(format!("fetch: {e}"))),
            };

            let local_after = match refname_to_id_optional(&local_repo, "refs/heads/beads/store") {
                Ok(oid) => oid,
                Err(e) => return Err(TestCaseError::fail(format!("local after: {e}"))),
            };
            let remote_after =
                match refname_to_id_optional(&local_repo, "refs/remotes/origin/beads/store") {
                    Ok(oid) => oid,
                    Err(e) => return Err(TestCaseError::fail(format!("remote tracking: {e}"))),
                };

            if let Some(remote_oid) = remote_head {
                prop_assert_eq!(remote_after, Some(remote_oid));
                match local_before {
                    None => {
                        prop_assert_eq!(local_after, Some(remote_oid));
                    }
                    Some(local_oid) => {
                        if local_oid == remote_oid {
                            prop_assert_eq!(local_after, Some(local_oid));
                        } else {
                            let remote_is_descendant =
                                match local_repo.graph_descendant_of(remote_oid, local_oid) {
                                    Ok(value) => value,
                                    Err(e) => {
                                        return Err(TestCaseError::fail(format!(
                                            "descendant: {e}"
                                        )));
                                    }
                                };
                            if remote_is_descendant {
                                prop_assert_eq!(local_after, Some(remote_oid));
                            } else {
                                prop_assert_eq!(local_after, Some(local_oid));
                                let backup_ref = format!("refs/beads/backup/{}", local_oid);
                                let backup_oid = match refname_to_id_optional(
                                    &local_repo,
                                    &backup_ref,
                                ) {
                                    Ok(oid) => oid,
                                    Err(e) => {
                                        return Err(TestCaseError::fail(format!(
                                            "backup ref: {e}"
                                        )));
                                    }
                                };
                                prop_assert_eq!(backup_oid, Some(local_oid));
                            }
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
        }
    }

    #[cfg(feature = "slow-tests")]
    proptest! {
        #[test]
        fn fetch_sequence_preserves_local_history(
            actions in prop::collection::vec(action_strategy(), 1..=8),
        ) {
            let tmp = match TempDir::new() {
                Ok(tmp) => tmp,
                Err(e) => return Err(TestCaseError::fail(format!("tempdir: {e}"))),
            };
            let remote_dir = tmp.path().join("remote");
            let local_dir = tmp.path().join("local");

            let remote_repo = match Repository::init_bare(&remote_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init remote: {e}"))),
            };
            let local_repo = match Repository::init(&local_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init local: {e}"))),
            };
            let remote_url = match remote_dir.to_str() {
                Some(url) => url,
                None => return Err(TestCaseError::fail("remote path not utf8")),
            };
            if let Err(e) = local_repo.remote("origin", remote_url) {
                return Err(TestCaseError::fail(format!("add remote: {e}")));
            }

            for action in actions {
                match action {
                    Action::LocalCommit(steps) => {
                        let parent = match local_ref(&local_repo) {
                            Ok(oid) => oid,
                            Err(e) => return Err(e),
                        };
                        write_chain(&local_repo, parent, steps, "local");
                    }
                    Action::RemoteCommit(steps) => {
                        let parent = match remote_ref(&remote_repo) {
                            Ok(oid) => oid,
                            Err(e) => return Err(e),
                        };
                        write_chain(&remote_repo, parent, steps, "remote");
                    }
                    Action::PushLocal => {
                        if let Err(e) = push_store(&local_repo) {
                            // non-fast-forward is expected sometimes; ignore and continue
                            let _ = e;
                        }
                    }
                    Action::ForcePushRemote => {
                        let _ = write_chain(&remote_repo, None, 1, "force");
                    }
                    Action::Fetch => {
                        let local_before = match local_ref(&local_repo) {
                            Ok(oid) => oid,
                            Err(e) => return Err(e),
                        };
                        let remote_before = match remote_ref(&remote_repo) {
                            Ok(oid) => oid,
                            Err(e) => return Err(e),
                        };

                        let fetched = match SyncProcess::new(local_dir.clone()).fetch(&local_repo) {
                            Ok(result) => result,
                            Err(e) => return Err(TestCaseError::fail(format!("fetch: {e}"))),
                        };

                        let local_after = match local_ref(&local_repo) {
                            Ok(oid) => oid,
                            Err(e) => return Err(e),
                        };
                        let remote_tracking = match refname_to_id_optional(
                            &local_repo,
                            "refs/remotes/origin/beads/store",
                        ) {
                            Ok(oid) => oid,
                            Err(e) => return Err(TestCaseError::fail(format!("remote tracking: {e}"))),
                        };

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
                                        let remote_is_descendant = match local_repo
                                            .graph_descendant_of(remote_oid, local_oid)
                                        {
                                            Ok(value) => value,
                                            Err(e) => {
                                                return Err(TestCaseError::fail(format!(
                                                    "descendant: {e}"
                                                )));
                                            }
                                        };
                                        if remote_is_descendant {
                                            prop_assert_eq!(local_after, Some(remote_oid));
                                        } else {
                                            prop_assert_eq!(local_after, Some(local_oid));
                                            let backup_ref = format!("refs/beads/backup/{}", local_oid);
                                            let backup_oid =
                                                match refname_to_id_optional(&local_repo, &backup_ref) {
                                                    Ok(oid) => oid,
                                                    Err(e) => {
                                                        return Err(TestCaseError::fail(format!(
                                                            "backup ref: {e}"
                                                        )));
                                                    }
                                                };
                                            prop_assert_eq!(backup_oid, Some(local_oid));
                                            let local_is_descendant =
                                                match local_repo.graph_descendant_of(local_oid, remote_oid) {
                                                    Ok(value) => value,
                                                    Err(e) => {
                                                        return Err(TestCaseError::fail(format!(
                                                            "descendant: {e}"
                                                        )));
                                                    }
                                                };
                                            prop_assert!(fetched.phase.divergence.is_some() || local_is_descendant);
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
        }
    }

    #[cfg(feature = "slow-tests")]
    proptest! {
        #[test]
        fn sync_with_retry_surfaces_fetch_divergence(
            relation in relation_strategy(),
            local_steps in 0u8..=3,
            remote_steps in 0u8..=3,
        ) {
            let tmp = match TempDir::new() {
                Ok(tmp) => tmp,
                Err(e) => return Err(TestCaseError::fail(format!("tempdir: {e}"))),
            };
            let remote_dir = tmp.path().join("remote");
            let local_dir = tmp.path().join("local");

            let remote_repo = match Repository::init_bare(&remote_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init remote: {e}"))),
            };
            let local_repo = match Repository::init(&local_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init local: {e}"))),
            };
            let remote_url = match remote_dir.to_str() {
                Some(url) => url,
                None => return Err(TestCaseError::fail("remote path not utf8")),
            };
            if let Err(e) = local_repo.remote("origin", remote_url) {
                return Err(TestCaseError::fail(format!("add remote: {e}")));
            }

            match relation {
                Relation::BothMissing => {}
                Relation::LocalOnly => {
                    let steps = nonzero_steps(local_steps);
                    let _ = write_chain(&local_repo, None, steps, "local");
                }
                Relation::RemoteOnly => {
                    let steps = nonzero_steps(remote_steps);
                    let _ = write_chain(&remote_repo, None, steps, "remote");
                }
                Relation::Same => {
                    let _ = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                }
                Relation::LocalAhead => {
                    let _ = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                    let parent = match local_ref(&local_repo) {
                        Ok(oid) => oid,
                        Err(e) => return Err(e),
                    };
                    let _ = write_chain(&local_repo, parent, nonzero_steps(local_steps), "local");
                }
                Relation::RemoteAhead => {
                    let _ = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                    let parent = match remote_ref(&remote_repo) {
                        Ok(oid) => oid,
                        Err(e) => return Err(e),
                    };
                    let _ = write_chain(&remote_repo, parent, nonzero_steps(remote_steps), "remote");
                }
                Relation::Diverged => {
                    let _ = write_chain(&local_repo, None, 1, "base");
                    push_store(&local_repo)?;
                    let local_parent = match local_ref(&local_repo) {
                        Ok(oid) => oid,
                        Err(e) => return Err(e),
                    };
                    let remote_parent = match remote_ref(&remote_repo) {
                        Ok(oid) => oid,
                        Err(e) => return Err(e),
                    };
                    let _ = write_chain(&local_repo, local_parent, nonzero_steps(local_steps), "local");
                    let _ = write_chain(&remote_repo, remote_parent, nonzero_steps(remote_steps), "remote");
                }
            }

            let fetched = match SyncProcess::new(local_dir.clone()).fetch(&local_repo) {
                Ok(result) => result,
                Err(e) => return Err(TestCaseError::fail(format!("fetch: {e}"))),
            };

            let expected_diverged = fetched.phase.divergence.is_some();
            let local_after_fetch = match local_ref(&local_repo) {
                Ok(oid) => oid,
                Err(e) => return Err(e),
            };
            let remote_after_fetch = match refname_to_id_optional(
                &local_repo,
                "refs/remotes/origin/beads/store",
            ) {
                Ok(oid) => oid,
                Err(e) => return Err(TestCaseError::fail(format!("remote tracking: {e}"))),
            };

            let outcome = match sync_with_retry(
                &local_repo,
                &local_dir,
                &CanonicalState::new(),
                1,
            ) {
                Ok(outcome) => outcome,
                Err(e) => return Err(TestCaseError::fail(format!("sync_with_retry: {e}"))),
            };

            prop_assert_eq!(outcome.divergence.is_some(), expected_diverged);

            if let (Some(local_oid), Some(remote_oid)) = (local_after_fetch, remote_after_fetch) {
                let remote_is_descendant = if remote_oid == local_oid {
                    true
                } else {
                    match local_repo.graph_descendant_of(remote_oid, local_oid) {
                        Ok(value) => value,
                        Err(e) => return Err(TestCaseError::fail(format!("descendant: {e}"))),
                    }
                };
                if !remote_is_descendant {
                    let backup_ref = format!("refs/beads/backup/{}", local_oid);
                    let backup_oid = match refname_to_id_optional(&local_repo, &backup_ref) {
                        Ok(oid) => oid,
                        Err(e) => return Err(TestCaseError::fail(format!("backup ref: {e}"))),
                    };
                    prop_assert_eq!(backup_oid, Some(local_oid));
                }
            }
        }
    }

    #[cfg(feature = "slow-tests")]
    proptest! {
        #[test]
        fn fetch_detects_force_push(rewrite_steps in 1u8..=3) {
            let tmp = match TempDir::new() {
                Ok(tmp) => tmp,
                Err(e) => return Err(TestCaseError::fail(format!("tempdir: {e}"))),
            };
            let remote_dir = tmp.path().join("remote");
            let local_dir = tmp.path().join("local");

            let remote_repo = match Repository::init_bare(&remote_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init remote: {e}"))),
            };
            let local_repo = match Repository::init(&local_dir) {
                Ok(repo) => repo,
                Err(e) => return Err(TestCaseError::fail(format!("init local: {e}"))),
            };
            let remote_url = match remote_dir.to_str() {
                Some(url) => url,
                None => return Err(TestCaseError::fail("remote path not utf8")),
            };
            if let Err(e) = local_repo.remote("origin", remote_url) {
                return Err(TestCaseError::fail(format!("add remote: {e}")));
            }

            let base = write_store_commit(&local_repo, None, "base");
            push_store(&local_repo)?;
            let _ = SyncProcess::new(local_dir.clone()).fetch(&local_repo);
            let local_oid = write_store_commit(&local_repo, Some(base), "local");
            let _ = write_chain(&remote_repo, None, rewrite_steps, "force");

            let fetched = match SyncProcess::new(local_dir.clone()).fetch(&local_repo) {
                Ok(result) => result,
                Err(e) => return Err(TestCaseError::fail(format!("fetch: {e}"))),
            };

            prop_assert!(fetched.phase.force_push.is_some());
            let local_after = match local_ref(&local_repo) {
                Ok(oid) => oid,
                Err(e) => return Err(e),
            };
            prop_assert_eq!(local_after, Some(local_oid));

            let backup_ref = format!("refs/beads/backup/{}", local_oid);
            let backup_oid = match refname_to_id_optional(&local_repo, &backup_ref) {
                Ok(oid) => oid,
                Err(e) => return Err(TestCaseError::fail(format!("backup ref: {e}"))),
            };
            prop_assert_eq!(backup_oid, Some(local_oid));
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
