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
    /// Root slug from meta.json (if any).
    pub root_slug: Option<String>,
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
    /// Root slug from meta.json (if any).
    pub root_slug: Option<String>,
}

/// Committed phase - have local commit, ready to push.
pub struct Committed {
    /// The commit oid we created.
    pub commit_oid: Oid,
    /// The merged state.
    pub state: CanonicalState,
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
            let _ = remote.fetch(&["refs/heads/beads/store"], Some(&mut fo), None);
        }

        // Get base state - prefer remote, fall back to local
        let (base_oid, base_state, root_slug) =
            match repo.refname_to_id("refs/remotes/origin/beads/store") {
                Ok(oid) => {
                    let loaded = read_state_at_oid(repo, oid)?;
                    (oid, loaded.state, loaded.root_slug)
                }
                Err(_) => {
                    // No remote - check local ref (handles local-only repos)
                    match repo.refname_to_id("refs/heads/beads/store") {
                        Ok(local_oid) => {
                            let loaded = read_state_at_oid(repo, local_oid)?;
                            (local_oid, loaded.state, loaded.root_slug)
                        }
                        Err(_) => {
                            // Neither remote nor local - truly first sync
                            (Oid::zero(), CanonicalState::new(), None)
                        }
                    }
                }
            };

        Ok(SyncProcess {
            repo_path: self.repo_path,
            phase: Fetched {
                remote_oid: base_oid,
                remote_state: base_state,
                root_slug,
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
            root_slug,
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
                    root_slug,
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
                root_slug,
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
            root_slug,
            ..
        } = self.phase;

        // Serialize state to blobs
        let state_bytes = wire::serialize_state(&state);
        let tombs_bytes = wire::serialize_tombstones(&state);
        let deps_bytes = wire::serialize_deps(&state);
        let meta_bytes = wire::serialize_meta(root_slug.as_deref());

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

/// Data loaded from a beads store commit.
pub struct LoadedStore {
    pub state: CanonicalState,
    pub root_slug: Option<String>,
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

    // Read meta.json for root_slug
    let root_slug = if let Some(meta_entry) = tree.get_name("meta.json") {
        if let Ok(meta_obj) = repo.find_object(meta_entry.id(), Some(ObjectType::Blob)) {
            if let Ok(meta_blob) = meta_obj.peel_to_blob() {
                wire::parse_meta(meta_blob.content())
                    .ok()
                    .and_then(|m| m.root_slug)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

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

    Ok(LoadedStore { state, root_slug })
}

/// Compute diff between two states for commit message.
fn compute_diff(before: &CanonicalState, after: &CanonicalState) -> SyncDiff {
    let mut diff = SyncDiff::default();

    // Count created and updated
    for (id, bead) in after.iter_live() {
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
                if bead.updated_stamp() != old_bead.updated_stamp() {
                    diff.updated += 1;
                    let changed_fields = detect_changed_fields(old_bead, bead);
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

/// Detect which fields changed between two beads by comparing LWW stamps.
fn detect_changed_fields(old: &crate::core::Bead, new: &crate::core::Bead) -> Vec<&'static str> {
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
    if old.fields.labels.stamp != new.fields.labels.stamp {
        changed.push("labels");
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
    // Check notes (compare note counts as a simple heuristic)
    if old.notes.len() != new.notes.len() {
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

/// Derive a root slug from repository info.
///
/// Priority:
/// 1. Remote URL basename (e.g., "github.com/foo/bar.git" -> "bar")
/// 2. Working directory name (e.g., "/home/user/bar" -> "bar")
/// 3. Fallback to "bd"
///
/// The slug is sanitized to be valid for bead IDs (lowercase alphanumeric + hyphens).
fn derive_root_slug(repo: &Repository) -> String {
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
        if !sanitized.is_empty() {
            return sanitized;
        }
    }

    // Fallback
    "bd".to_string()
}

/// Extract a slug from a git remote URL.
fn extract_slug_from_url(url: &str) -> Option<String> {
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
        None
    } else {
        Some(sanitized)
    }
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

        // Derive root slug from repo name
        let root_slug = derive_root_slug(repo);

        // Create empty initial state
        let state = CanonicalState::new();
        let state_bytes = wire::serialize_state(&state);
        let tombs_bytes = wire::serialize_tombstones(&state);
        let deps_bytes = wire::serialize_deps(&state);
        let meta_bytes = wire::serialize_meta(Some(&root_slug));

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
}
