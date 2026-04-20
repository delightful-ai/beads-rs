//! Layer 5: Composite domain types
//!
//! Note: immutable comment/note on a bead
//! Claim: assignee + expiry (no redundant `at` - use Lww.stamp)
//! IssueStatus: canonical board/work execution state

use serde::{Deserialize, Serialize};
use sha2::Digest;

use super::identity::ContentHashable;
use super::identity::{ActorId, NoteId};
use super::time::{WallClock, WriteStamp}; // WriteStamp still used in Note

/// Immutable note/comment on a bead.
///
/// Once created, never changes. ID is unique within the bead.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
/// the Lww.stamp IS the claim timestamp. Wire snapshots use that stamp.
///
/// Using an enum instead of Option<ClaimData> makes states explicit
/// and prevents accidentally having empty/invalid claim states.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
            Self::Claimed { expires: None, .. } | Self::Unclaimed => false,
        }
    }

    /// Check if currently claimed (not expired).
    pub fn is_claimed(&self) -> bool {
        matches!(self, Self::Claimed { .. })
    }
}

/// Canonical board/work execution state.
///
/// This is the one writable truth for tracker/board lifecycle. Older coarse
/// workflow views such as `open`, `in_progress`, and `closed` should be derived
/// from this enum instead of stored alongside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub enum IssueStatus {
    #[default]
    #[serde(rename = "Todo")]
    Todo,
    #[serde(rename = "In Progress")]
    InProgress,
    #[serde(rename = "Human Review")]
    HumanReview,
    #[serde(rename = "Rework")]
    Rework,
    #[serde(rename = "Merging")]
    Merging,
    #[serde(rename = "Done")]
    Done,
    #[serde(rename = "Cancelled")]
    Cancelled,
    #[serde(rename = "Duplicate")]
    Duplicate,
}

crate::enum_str! {
    impl IssueStatus {
        pub fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            Todo => ["Todo", "todo"],
            InProgress => ["In Progress", "in_progress", "in progress"],
            HumanReview => ["Human Review", "human_review", "human review"],
            Rework => ["Rework", "rework"],
            Merging => ["Merging", "merging"],
            Done => ["Done", "done"],
            Cancelled => ["Cancelled", "Canceled", "cancelled", "canceled"],
            Duplicate => ["Duplicate", "duplicate"],
        }
    }
}

impl IssueStatus {
    pub const VALID_CLOSE_REASON_HELP: &str = "done, cancelled, duplicate";

    pub const fn bucket_str(self) -> &'static str {
        match self {
            Self::Todo => "open",
            Self::InProgress | Self::HumanReview | Self::Rework | Self::Merging => "in_progress",
            Self::Done | Self::Cancelled | Self::Duplicate => "closed",
        }
    }

    pub const fn is_active(self) -> bool {
        matches!(
            self,
            Self::Todo | Self::InProgress | Self::HumanReview | Self::Rework | Self::Merging
        )
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Cancelled | Self::Duplicate)
    }

    pub const fn closed_reason(self) -> Option<&'static str> {
        match self {
            Self::Done => Some("done"),
            Self::Cancelled => Some("cancelled"),
            Self::Duplicate => Some("duplicate"),
            Self::Todo | Self::InProgress | Self::HumanReview | Self::Rework | Self::Merging => {
                None
            }
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        Self::parse_str(raw)
    }

    pub fn from_close_reason(raw: Option<&str>) -> Option<Self> {
        match raw.map(str::trim).filter(|raw| !raw.is_empty()) {
            None => Some(Self::Done),
            Some(raw) => Self::parse(raw).filter(|state| state.is_terminal()),
        }
    }

    pub fn from_legacy_workflow(raw: &str, close_reason: Option<&str>) -> Option<Self> {
        match raw.trim() {
            "open" => Some(Self::Todo),
            "in_progress" => Some(Self::InProgress),
            "closed" => Self::from_close_reason(close_reason),
            _ => None,
        }
    }
}

impl ContentHashable for IssueStatus {
    fn hash_content(&self, hasher: &mut impl Digest) {
        hasher.update(self.as_str().as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::IssueStatus;

    #[test]
    fn status_from_close_reason_defaults_to_done() {
        assert_eq!(
            IssueStatus::from_close_reason(None),
            Some(IssueStatus::Done)
        );
    }

    #[test]
    fn status_from_close_reason_accepts_terminal_aliases() {
        assert_eq!(
            IssueStatus::from_close_reason(Some("Canceled")),
            Some(IssueStatus::Cancelled)
        );
        assert_eq!(
            IssueStatus::from_close_reason(Some("duplicate")),
            Some(IssueStatus::Duplicate)
        );
    }

    #[test]
    fn status_from_close_reason_rejects_non_terminal_names() {
        assert_eq!(IssueStatus::from_close_reason(Some("Closed")), None);
        assert_eq!(IssueStatus::from_close_reason(Some("In Progress")), None);
    }
}
