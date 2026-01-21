//! Git checkpoint lane state for a single store.
//!
//! Provides:
//! - `GitLaneState` - in-memory state for legacy git sync/checkpoint lane

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Instant;

use crate::core::WriteStamp;

const DEFAULT_BACKOFF_BASE_MS: u64 = 500;
const TEST_FAST_BACKOFF_BASE_MS: u64 = 50;

fn backoff_base_ms() -> u64 {
    if env_flag_truthy("BD_TEST_FAST") {
        TEST_FAST_BACKOFF_BASE_MS
    } else {
        DEFAULT_BACKOFF_BASE_MS
    }
}

fn env_flag_truthy(name: &str) -> bool {
    let Ok(raw) = std::env::var(name) else {
        return false;
    };
    !matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "0" | "false" | "no" | "n" | "off"
    )
}

#[derive(Clone, Debug)]
pub struct FetchErrorRecord {
    pub message: String,
    pub wall_ms: u64,
}

#[derive(Clone, Debug)]
pub struct DivergenceRecord {
    pub local_oid: String,
    pub remote_oid: String,
    pub wall_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ForcePushRecord {
    pub previous_remote_oid: String,
    pub remote_oid: String,
    pub wall_ms: u64,
}

#[derive(Clone, Debug)]
pub struct ClockSkewRecord {
    pub delta_ms: i64,
    pub wall_ms: u64,
}

/// In-memory state for the legacy git checkpoint lane.
pub struct GitLaneState {
    /// Root slug for bead IDs (from meta.json).
    /// When set, new bead IDs will use this slug (e.g., "myproject-xxx").
    pub root_slug: Option<String>,

    /// All known clone paths for this remote.
    pub known_paths: HashSet<PathBuf>,

    /// Whether the state has uncommitted changes.
    pub dirty: bool,

    /// Time of last mutation (for debounce scheduling).
    pub last_mutation: Option<Instant>,

    /// Whether a sync is currently in progress.
    pub sync_in_progress: bool,

    /// Whether a background refresh is currently in progress.
    pub refresh_in_progress: bool,

    /// Time of last successful sync.
    pub last_sync: Option<Instant>,

    /// Wall clock time (ms) of last successful sync - for IPC responses.
    pub last_sync_wall_ms: Option<u64>,

    /// Time of last successful refresh-from-remote.
    pub last_refresh: Option<Instant>,

    /// Number of consecutive sync failures (for exponential backoff).
    pub consecutive_failures: u32,

    /// Last observed write stamp (from state or meta).
    pub last_seen_stamp: Option<WriteStamp>,

    /// Last fetch error (best-effort).
    pub last_fetch_error: Option<FetchErrorRecord>,

    /// Last divergence detected between local and remote.
    pub last_divergence: Option<DivergenceRecord>,

    /// Last detected remote force-push.
    pub last_force_push: Option<ForcePushRecord>,

    /// Last detected clock skew.
    pub last_clock_skew: Option<ClockSkewRecord>,
}

impl GitLaneState {
    /// Create a new GitLaneState.
    pub fn new() -> Self {
        GitLaneState {
            root_slug: None,
            known_paths: HashSet::new(),
            dirty: false,
            last_mutation: None,
            sync_in_progress: false,
            refresh_in_progress: false,
            last_sync: None,
            last_sync_wall_ms: None,
            last_refresh: None,
            consecutive_failures: 0,
            last_seen_stamp: None,
            last_fetch_error: None,
            last_divergence: None,
            last_force_push: None,
            last_clock_skew: None,
        }
    }

    /// Create a new GitLaneState with root slug and initial clone path.
    pub fn with_path(root_slug: Option<String>, path: PathBuf) -> Self {
        let mut s = Self::new();
        s.root_slug = root_slug;
        s.known_paths.insert(path);
        s.last_refresh = Some(Instant::now());
        s
    }

    /// Register another clone path for this remote.
    pub fn register_path(&mut self, path: PathBuf) {
        self.known_paths.insert(path);
    }

    /// Pick any existing clone path to run git ops against.
    pub fn any_valid_path(&self) -> Option<&PathBuf> {
        self.known_paths.iter().find(|p| p.exists())
    }

    /// Mark the state as dirty (has uncommitted changes).
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
        self.last_mutation = Some(Instant::now());
    }

    /// Check if enough time has passed since last mutation for sync.
    pub fn ready_for_sync(&self, debounce_ms: u64) -> bool {
        if !self.dirty {
            return false;
        }
        if self.sync_in_progress {
            return false;
        }

        match self.last_mutation {
            Some(last) => last.elapsed().as_millis() >= debounce_ms as u128,
            None => true,
        }
    }

    /// Mark sync as started.
    pub fn start_sync(&mut self) {
        self.sync_in_progress = true;
        // Clear dirty to track whether mutations happen *during* this sync.
        // If a mutation occurs while syncing, mark_dirty() will set dirty=true again
        // and we will schedule a follow-up sync after completion.
        self.dirty = false;
    }

    /// Mark sync as completed successfully.
    pub fn complete_sync(&mut self, wall_ms: u64) {
        self.sync_in_progress = false;
        let now = Instant::now();
        self.last_sync = Some(now);
        self.last_sync_wall_ms = Some(wall_ms);
        self.last_refresh = Some(now);
        self.consecutive_failures = 0;

        // Only clear dirty if no mutations happened during sync
        // (The daemon will re-mark dirty if needed)
    }

    /// Mark sync as failed.
    pub fn fail_sync(&mut self) {
        self.sync_in_progress = false;
        self.consecutive_failures += 1;
        // Keep dirty=true so we retry the sync after backoff.
        self.dirty = true;
    }

    /// Calculate backoff delay for retries (exponential).
    pub fn backoff_ms(&self) -> u64 {
        let base = backoff_base_ms();
        let max_exponent = 6; // Max ~32 seconds
        let exponent = self.consecutive_failures.min(max_exponent);
        base * 2u64.pow(exponent)
    }
}

impl Default for GitLaneState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_state_not_dirty() {
        let state = GitLaneState::new();
        assert!(!state.dirty);
        assert!(!state.sync_in_progress);
        assert!(!state.refresh_in_progress);
        assert_eq!(state.consecutive_failures, 0);
    }

    #[test]
    fn mark_dirty() {
        let mut state = GitLaneState::new();
        state.mark_dirty();
        assert!(state.dirty);
        assert!(state.last_mutation.is_some());
    }

    #[test]
    fn backoff_exponential() {
        let mut state = GitLaneState::new();
        let base = backoff_base_ms();
        let max_backoff = base * 2u64.pow(6);
        assert_eq!(state.backoff_ms(), base);

        state.consecutive_failures = 1;
        assert_eq!(state.backoff_ms(), base * 2);

        state.consecutive_failures = 2;
        assert_eq!(state.backoff_ms(), base * 4);

        state.consecutive_failures = 6;
        assert_eq!(state.backoff_ms(), max_backoff);

        // Should cap at 6
        state.consecutive_failures = 10;
        assert_eq!(state.backoff_ms(), max_backoff);
    }
}
