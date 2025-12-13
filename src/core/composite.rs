//! Layer 5: Composite domain types
//!
//! Note: immutable comment/note on a bead
//! Claim: assignee + expiry (no redundant `at` - use Lww.stamp)
//! Closure: closure info when bead is closed
//! Workflow: sum type - status derived from variant

use serde::{Deserialize, Serialize};

use super::identity::{ActorId, NoteId};
use super::time::{WallClock, WriteStamp}; // WriteStamp still used in Note

/// Immutable note/comment on a bead.
///
/// Once created, never changes. ID is unique within the bead.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Note {
    pub id: NoteId,
    pub content: String,
    pub author: ActorId,
    pub at: WriteStamp,
}

impl Note {
    pub fn new(id: NoteId, content: String, author: ActorId, at: WriteStamp) -> Self {
        Self {
            id,
            content,
            author,
            at,
        }
    }
}

/// Claim state - explicit Unclaimed vs Claimed.
///
/// NOTE: No `at` field here - when wrapped in Lww<Claim>,
/// the Lww.stamp IS the claim timestamp. Derive assignee_at from that.
///
/// Using an enum instead of Option<ClaimData> makes states explicit
/// and prevents accidentally having empty/invalid claim states.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
#[derive(Default)]
pub enum Claim {
    #[default]
    Unclaimed,
    Claimed {
        assignee: ActorId,
        /// Lease expiry (wall clock, not causal). None = no expiry.
        expires: Option<WallClock>,
    },
}

impl Claim {
    pub fn unclaimed() -> Self {
        Self::Unclaimed
    }

    pub fn claimed(assignee: ActorId, expires: Option<WallClock>) -> Self {
        Self::Claimed { assignee, expires }
    }

    /// Get assignee if claimed.
    pub fn assignee(&self) -> Option<&ActorId> {
        match self {
            Self::Claimed { assignee, .. } => Some(assignee),
            Self::Unclaimed => None,
        }
    }

    /// Get expiry if claimed.
    pub fn expires(&self) -> Option<WallClock> {
        match self {
            Self::Claimed { expires, .. } => *expires,
            Self::Unclaimed => None,
        }
    }

    /// Check if claimed and expired.
    pub fn is_expired(&self, now: WallClock) -> bool {
        match self {
            Self::Claimed {
                expires: Some(exp), ..
            } => *exp < now,
            _ => false,
        }
    }

    /// Check if currently claimed (not expired).
    pub fn is_claimed(&self) -> bool {
        matches!(self, Self::Claimed { .. })
    }
}

/// Closure info - only exists when bead is closed.
///
/// NOTE: No `at`/`by` fields - when wrapped in `Lww<Workflow::Closed(Closure)>`,
/// the Lww.stamp IS the closure timestamp. Derive closed_at/closed_by from that
/// during wire serialization.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Closure {
    pub reason: Option<String>,
    pub on_branch: Option<String>,
}

impl Closure {
    pub fn new(reason: Option<String>, on_branch: Option<String>) -> Self {
        Self { reason, on_branch }
    }
}

/// Workflow as sum type.
///
/// Status is DERIVED from this, not stored separately.
/// Impossible to have status=Closed without Closure data.
/// This is the key invariant that prevents Go's status/closed_at desync.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
#[derive(Default)]
pub enum Workflow {
    #[default]
    Open,
    InProgress,
    Closed(Closure),
}

impl Workflow {
    /// Derive status string from workflow state.
    pub fn status(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::InProgress => "in_progress",
            Self::Closed(_) => "closed",
        }
    }

    /// Get closure info if closed.
    pub fn closure(&self) -> Option<&Closure> {
        match self {
            Self::Closed(c) => Some(c),
            _ => None,
        }
    }

    /// Check if workflow is in terminal state.
    pub fn is_closed(&self) -> bool {
        matches!(self, Self::Closed(_))
    }
}
