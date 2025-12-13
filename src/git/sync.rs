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
//! - Meaningful commit messages: "sync: +2 created, ~1 updated"

use std::path::{Path, PathBuf};

use git2::{ObjectType, Oid, Repository, Signature};

use super::collision::{Collision, detect_collisions, resolve_collisions};
use super::error::SyncError;
use super::wire;
use crate::core::{CanonicalState, Stamp};

// =============================================================================
// Phase markers (zero-sized types for typestate)
// =============================================================================

/// Initial phase - ready to start sync.
pub struct Idle;

/// Fetched phase - have remote state.
pub struct Fetched {
    /// Remote HEAD oid (will be parent of our commit).
    pub remote_oid: Oid,
    /// Parsed remote state.
    pub remote_state: CanonicalState,
}

/// Merged phase - have merged state ready to commit.
pub struct Merged {
    /// Merged state (local + remote).
    pub state: CanonicalState,
    /// Collisions that were resolved.
    pub collisions: Vec<Collision>,
    /// Parent oid for commit (remote HEAD).
    pub parent_oid: Oid,
    /// Diff summary for commit message.
    pub diff: SyncDiff,
}

/// Committed phase - have local commit, ready to push.
pub struct Committed {
    /// The commit oid we created.
    pub commit_oid: Oid,
    /// The merged state.
    pub state: CanonicalState,
}

/// Summary of changes for commit message.
#[derive(Default, Clone)]
pub struct SyncDiff {
    pub created: usize,
    pub updated: usize,
    pub deleted: usize,
}

impl SyncDiff {
    pub fn to_commit_message(&self) -> String {
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
        if parts.is_empty() {
            "sync: no changes".to_string()
        } else {
            format!("sync: {}", parts.join(", "))
        }
    }
}

// =============================================================================
// SyncProcess - the typestate machine
// =============================================================================

/// Sync process with typestate-enforced phases.
///
/// Use `SyncProcess::new()` to start, then chain transitions:
/// ```ignore
/// let synced = SyncProcess::new(repo_path)
///     .fetch(&repo)?
///     .merge(&local_state, stamp)?
///     .commit(&repo)?
///     .push(&repo)?;
/// ```
pub struct SyncProcess<Phase> {
    pub repo_path: PathBuf,
    pub phase: Phase,
}

impl SyncProcess<Idle> {
    /// Create a new sync process in Idle phase.
    pub fn new(repo_path: PathBuf) -> Self {
        SyncProcess {
            repo_path,
            phase: Idle,
        }
    }

    /// Fetch from remote, transition to Fetched phase.
    ///
    /// If no remote ref exists, creates an empty initial state.
    pub fn fetch(self, repo: &Repository) -> Result<SyncProcess<Fetched>, SyncError> {
        // Fetch from origin
        if let Ok(mut remote) = repo.find_remote("origin") {
            // Ignore fetch errors - remote might not exist yet
            let cfg = repo.config().ok();
            let mut callbacks = git2::RemoteCallbacks::new();
            callbacks.credentials(move |url, username_from_url, allowed| {
                if allowed.is_ssh_key() {
                    if let Some(user) = username_from_url {
                        return git2::Cred::ssh_key_from_agent(user);
                    }
                }
                if allowed.is_user_pass_plaintext() {
                    if let Some(ref cfg) = cfg {
                        if let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
                        {
                            return Ok(cred);
                        }
                    }
                }
                git2::Cred::default()
            });
            let mut fo = git2::FetchOptions::new();
            fo.remote_callbacks(callbacks);
            let _ = remote.fetch(&["refs/heads/beads/store"], Some(&mut fo), None);
        }

        // Get base state - prefer remote, fall back to local
        let (base_oid, base_state) = match repo.refname_to_id("refs/remotes/origin/beads/store") {
            Ok(oid) => {
                let state = read_state_at_oid(repo, oid)?;
                (oid, state)
            }
            Err(_) => {
                // No remote - check local ref (handles local-only repos)
                match repo.refname_to_id("refs/heads/beads/store") {
                    Ok(local_oid) => {
                        let state = read_state_at_oid(repo, local_oid)?;
                        (local_oid, state)
                    }
                    Err(_) => {
                        // Neither remote nor local - truly first sync
                        (Oid::zero(), CanonicalState::new())
                    }
                }
            }
        };

        Ok(SyncProcess {
            repo_path: self.repo_path,
            phase: Fetched {
                remote_oid: base_oid,
                remote_state: base_state,
            },
        })
    }
}

impl SyncProcess<Fetched> {
    /// Merge local state with remote, transition to Merged phase.
    ///
    /// Handles:
    /// - ID collisions (detects and resolves)
    /// - CRDT pairwise merge (no base needed)
    /// - Resurrection rule (bead beats tombstone if newer)
    pub fn merge(
        self,
        local_state: &CanonicalState,
        resolution_stamp: Stamp,
    ) -> Result<SyncProcess<Merged>, SyncError> {
        let Fetched {
            remote_oid,
            remote_state,
        } = self.phase;

        // Fast path: if remote is empty/uninitialized, just use local
        if remote_oid.is_zero() {
            let diff = compute_diff(&CanonicalState::new(), local_state);
            return Ok(SyncProcess {
                repo_path: self.repo_path,
                phase: Merged {
                    state: local_state.clone(),
                    collisions: Vec::new(),
                    parent_oid: remote_oid,
                    diff,
                },
            });
        }

        // Detect ID collisions
        let collisions = detect_collisions(local_state, &remote_state);

        // Resolve collisions if any
        let (local_resolved, remote_resolved) = if collisions.is_empty() {
            (local_state.clone(), remote_state)
        } else {
            resolve_collisions(local_state, &remote_state, &collisions, resolution_stamp)
        };

        // Pairwise CRDT merge
        let mut merged = CanonicalState::join(&local_resolved, &remote_resolved)
            .map_err(|errs| SyncError::MergeConflict { errors: errs })?;

        // Garbage collect old tombstones (30 day TTL) and soft-deleted deps
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let now = crate::core::WallClock(now_ms);
        const TOMBSTONE_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1000; // 30 days
        merged.gc_tombstones(TOMBSTONE_TTL_MS, now);
        merged.gc_deleted_deps();

        // Compute diff for commit message
        let diff = compute_diff(&remote_resolved, &merged);

        Ok(SyncProcess {
            repo_path: self.repo_path,
            phase: Merged {
                state: merged,
                collisions,
                parent_oid: remote_oid,
                diff,
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
        let Merged {
            state,
            parent_oid,
            diff,
            ..
        } = self.phase;

        // Serialize state to blobs
        let state_bytes = wire::serialize_state(&state);
        let tombs_bytes = wire::serialize_tombstones(&state);
        let deps_bytes = wire::serialize_deps(&state);
        let meta_bytes = wire::serialize_meta();

        // Write blobs to ODB
        let state_oid = repo.blob(&state_bytes)?;
        let tombs_oid = repo.blob(&tombs_bytes)?;
        let deps_oid = repo.blob(&deps_bytes)?;
        let meta_oid = repo.blob(&meta_bytes)?;

        // Build tree
        let mut builder = repo.treebuilder(None)?;
        builder.insert("state.jsonl", state_oid, 0o100644)?;
        builder.insert("tombstones.jsonl", tombs_oid, 0o100644)?;
        builder.insert("deps.jsonl", deps_oid, 0o100644)?;
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
        let commit_oid = repo.commit(
            Some("refs/heads/beads/store"),
            &sig,
            &sig,
            &message,
            &tree,
            &parent_refs,
        )?;

        Ok(SyncProcess {
            repo_path: self.repo_path,
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
                if allowed.is_ssh_key() {
                    if let Some(user) = username_from_url {
                        return git2::Cred::ssh_key_from_agent(user);
                    }
                }
                if allowed.is_user_pass_plaintext() {
                    if let Some(ref cfg) = cfg {
                        if let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
                        {
                            return Ok(cred);
                        }
                    }
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
                if msg.contains("non-fast-forward")
                    || msg.contains("fetch first")
                    || msg.contains("cannot lock ref")
                    || msg.contains("failed to update ref")
                {
                    return Err(SyncError::NonFastForward);
                }
                return Err(SyncError::from(e));
            }
        }

        if let Some(err) = push_error.into_inner() {
            if err.contains("non-fast-forward") || err.contains("fetch first") {
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

/// Read state from a commit oid.
pub fn read_state_at_oid(repo: &Repository, oid: Oid) -> Result<CanonicalState, SyncError> {
    let commit = repo.find_commit(oid)?;
    let tree = commit.tree()?;

    // Read state.jsonl
    let state_entry = tree
        .get_name("state.jsonl")
        .ok_or(SyncError::MissingFile("state.jsonl".into()))?;
    let state_blob = repo
        .find_object(state_entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;
    let beads = wire::parse_state(state_blob.content())?;

    // Read tombstones.jsonl
    let tombs_entry = tree
        .get_name("tombstones.jsonl")
        .ok_or(SyncError::MissingFile("tombstones.jsonl".into()))?;
    let tombs_blob = repo
        .find_object(tombs_entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;
    let tombstones = wire::parse_tombstones(tombs_blob.content())?;

    // Read deps.jsonl
    let deps_entry = tree
        .get_name("deps.jsonl")
        .ok_or(SyncError::MissingFile("deps.jsonl".into()))?;
    let deps_blob = repo
        .find_object(deps_entry.id(), Some(ObjectType::Blob))?
        .peel_to_blob()?;
    let deps = wire::parse_deps(deps_blob.content())?;

    // Build state
    let mut state = CanonicalState::new();
    for bead in beads {
        state.insert_live(bead);
    }
    for tomb in tombstones {
        state.insert_tombstone(tomb);
    }
    for dep in deps {
        state.insert_dep(dep);
    }

    Ok(state)
}

/// Compute diff between two states for commit message.
fn compute_diff(before: &CanonicalState, after: &CanonicalState) -> SyncDiff {
    let mut diff = SyncDiff::default();

    // Count created and updated
    for (id, bead) in after.iter_live() {
        match before.get_live(id) {
            None => diff.created += 1,
            Some(old_bead) => {
                if bead.updated_stamp() != old_bead.updated_stamp() {
                    diff.updated += 1;
                }
            }
        }
    }

    // Count deleted (in before.live but not in after.live)
    for (id, _) in before.iter_live() {
        if after.get_live(id).is_none() {
            diff.deleted += 1;
        }
    }

    diff
}

/// Run a full sync cycle with retry on non-fast-forward.
///
/// This is the main entry point for syncing - handles the retry loop.
pub fn sync_with_retry(
    repo: &Repository,
    repo_path: &Path,
    local_state: &CanonicalState,
    resolution_stamp: Stamp,
    max_retries: usize,
) -> Result<CanonicalState, SyncError> {
    let mut retries = 0;

    loop {
        let result = SyncProcess::new(repo_path.to_owned())
            .fetch(repo)?
            .merge(local_state, resolution_stamp.clone())?
            .commit(repo)?
            .push(repo);

        match result {
            Ok(state) => return Ok(state),
            Err(SyncError::NonFastForward) => {
                retries += 1;
                if retries > max_retries {
                    return Err(SyncError::TooManyRetries(retries));
                }
                // Loop will fetch again and re-merge
                continue;
            }
            Err(e) => return Err(e),
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
        // Always fetch first
        if let Ok(mut remote) = repo.find_remote("origin") {
            let cfg = repo.config().ok();
            let mut callbacks = git2::RemoteCallbacks::new();
            callbacks.credentials(move |url, username_from_url, allowed| {
                if allowed.is_ssh_key() {
                    if let Some(user) = username_from_url {
                        return git2::Cred::ssh_key_from_agent(user);
                    }
                }
                if allowed.is_user_pass_plaintext() {
                    if let Some(ref cfg) = cfg {
                        if let Ok(cred) = git2::Cred::credential_helper(cfg, url, username_from_url)
                        {
                            return Ok(cred);
                        }
                    }
                }
                git2::Cred::default()
            });
            let mut fo = git2::FetchOptions::new();
            fo.remote_callbacks(callbacks);
            let _ = remote.fetch(&["refs/heads/beads/store"], Some(&mut fo), None);

            // If remote has the ref, point local to it
            if let Ok(remote_oid) = repo.refname_to_id("refs/remotes/origin/beads/store") {
                repo.reference(
                    "refs/heads/beads/store",
                    remote_oid,
                    true,
                    "beads init from remote",
                )?;
                return Ok(());
            }
        }

        // No remote - check if local already exists
        if repo.refname_to_id("refs/heads/beads/store").is_ok() {
            return Ok(());
        }

        // Create empty initial state
        let state = CanonicalState::new();
        let state_bytes = wire::serialize_state(&state);
        let tombs_bytes = wire::serialize_tombstones(&state);
        let deps_bytes = wire::serialize_deps(&state);
        let meta_bytes = wire::serialize_meta();

        // Write blobs
        let state_oid = repo.blob(&state_bytes)?;
        let tombs_oid = repo.blob(&tombs_bytes)?;
        let deps_oid = repo.blob(&deps_bytes)?;
        let meta_oid = repo.blob(&meta_bytes)?;

        // Build tree
        let mut builder = repo.treebuilder(None)?;
        builder.insert("state.jsonl", state_oid, 0o100644)?;
        builder.insert("tombstones.jsonl", tombs_oid, 0o100644)?;
        builder.insert("deps.jsonl", deps_oid, 0o100644)?;
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
                    if allowed.is_ssh_key() {
                        if let Some(user) = username_from_url {
                            return git2::Cred::ssh_key_from_agent(user);
                        }
                    }
                    if allowed.is_user_pass_plaintext() {
                        if let Some(ref cfg) = cfg {
                            if let Ok(cred) =
                                git2::Cred::credential_helper(cfg, url, username_from_url)
                            {
                                return Ok(cred);
                            }
                        }
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

    #[test]
    fn sync_diff_message() {
        let diff = SyncDiff {
            created: 2,
            updated: 1,
            deleted: 0,
        };
        assert_eq!(diff.to_commit_message(), "sync: +2 created, ~1 updated");

        let diff = SyncDiff {
            created: 0,
            updated: 0,
            deleted: 3,
        };
        assert_eq!(diff.to_commit_message(), "sync: -3 deleted");

        let diff = SyncDiff::default();
        assert_eq!(diff.to_commit_message(), "sync: no changes");
    }
}
