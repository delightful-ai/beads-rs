//! Layer 2: Domain enums
//!
//! BeadType: built-in issue/work item classifications
//! DepKind: typed workflow and graph relationship kinds
//! Priority: 0-4 (0 = critical)

use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::cmp::Ordering;

use super::error::{CoreError, InvalidDepKind, RangeError};
use super::identity::ContentHashable;

/// Issue type classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadType {
    Bug,
    Feature,
    Task,
    Epic,
    Chore,
    Decision,
    Message,
    Molecule,
    Spike,
    Story,
    Milestone,
    Event,
}

crate::enum_str! {
    impl BeadType {
        pub fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            Bug => ["bug"],
            Feature => ["feature"],
            Task => ["task"],
            Epic => ["epic"],
            Chore => ["chore"],
            Decision => ["decision"],
            Message => ["message"],
            Molecule => ["molecule"],
            Spike => ["spike"],
            Story => ["story"],
            Milestone => ["milestone"],
            Event => ["event"],
        }
    }
}

impl BeadType {
    pub fn parse(raw: &str) -> Option<Self> {
        Self::parse_str(raw)
    }
}

impl ContentHashable for BeadType {
    fn hash_content(&self, hasher: &mut impl Digest) {
        hasher.update(self.as_str().as_bytes());
    }
}

/// Dependency relationship kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DepKind {
    Blocks,
    Parent,
    ConditionalBlocks,
    WaitsFor,
    Related,
    DiscoveredFrom,
    RepliesTo,
    RelatesTo,
    Duplicates,
    Supersedes,
    AuthoredBy,
    AssignedTo,
    ApprovedBy,
    Attests,
    Tracks,
    Until,
    CausedBy,
    Validates,
    DelegatedFrom,
}

crate::enum_str! {
    impl DepKind {
        pub fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            Blocks => ["blocks", "block"],
            Parent => ["parent", "parent-child", "parent_child", "parentchild"],
            ConditionalBlocks => ["conditional-blocks", "conditional_blocks", "conditionalblocks"],
            WaitsFor => ["waits-for", "waits_for", "waitsfor"],
            Related => ["related", "relates"],
            DiscoveredFrom => ["discovered_from", "discovered-from", "discoveredfrom"],
            RepliesTo => ["replies-to", "replies_to", "repliesto"],
            RelatesTo => ["relates-to", "relates_to", "relatesto"],
            Duplicates => ["duplicates", "duplicate"],
            Supersedes => ["supersedes", "supersede"],
            AuthoredBy => ["authored-by", "authored_by", "authoredby"],
            AssignedTo => ["assigned-to", "assigned_to", "assignedto"],
            ApprovedBy => ["approved-by", "approved_by", "approvedby"],
            Attests => ["attests", "attest"],
            Tracks => ["tracks", "track"],
            Until => ["until"],
            CausedBy => ["caused-by", "caused_by", "causedby"],
            Validates => ["validates", "validate"],
            DelegatedFrom => ["delegated-from", "delegated_from", "delegatedfrom"],
        }
    }
}

impl DepKind {
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        let s = raw.trim().to_lowercase();
        Self::parse_str(&s).ok_or_else(|| {
            InvalidDepKind {
                raw: raw.to_string(),
            }
            .into()
        })
    }

    /// Returns true if this dependency kind requires DAG enforcement (no cycles).
    ///
    /// - Blocking/control-flow kinds have ordering semantics and must be acyclic.
    /// - Informational graph kinds can be cyclic.
    pub fn requires_dag(&self) -> bool {
        matches!(
            self,
            Self::Blocks | Self::Parent | Self::ConditionalBlocks | Self::WaitsFor
        )
    }
}

impl Ord for DepKind {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for DepKind {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::str::FromStr for DepKind {
    type Err = CoreError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        DepKind::parse(s)
    }
}

/// Priority level: 0-4 inclusive, 0 = critical.
///
/// Validated at construction - invalid values are unrepresentable.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct Priority(u8);

impl Priority {
    pub const CRITICAL: Priority = Priority(0);
    pub const HIGH: Priority = Priority(1);
    pub const MEDIUM: Priority = Priority(2);
    pub const LOW: Priority = Priority(3);
    pub const LOWEST: Priority = Priority(4);

    pub fn new(n: u8) -> Result<Self, CoreError> {
        if n > 4 {
            Err(RangeError {
                field: "priority",
                value: n,
                min: 0,
                max: 4,
            }
            .into())
        } else {
            Ok(Self(n))
        }
    }

    pub fn value(&self) -> u8 {
        self.0
    }
}

impl ContentHashable for Priority {
    fn hash_content(&self, hasher: &mut impl Digest) {
        hasher.update(self.value().to_string().as_bytes());
    }
}

impl Default for Priority {
    fn default() -> Self {
        Self::MEDIUM
    }
}

impl TryFrom<u8> for Priority {
    type Error = CoreError;
    fn try_from(n: u8) -> Result<Self, Self::Error> {
        Priority::new(n)
    }
}

impl From<Priority> for u8 {
    fn from(p: Priority) -> u8 {
        p.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dep_kind_parse_accepts_aliases() {
        assert_eq!(DepKind::parse("blocks").unwrap(), DepKind::Blocks);
        assert_eq!(DepKind::parse("block").unwrap(), DepKind::Blocks);
        assert_eq!(DepKind::parse("parent").unwrap(), DepKind::Parent);
        assert_eq!(DepKind::parse("parent-child").unwrap(), DepKind::Parent);
        assert_eq!(DepKind::parse("related").unwrap(), DepKind::Related);
        assert_eq!(DepKind::parse("relates").unwrap(), DepKind::Related);
        assert_eq!(
            DepKind::parse("discovered_from").unwrap(),
            DepKind::DiscoveredFrom
        );
        assert_eq!(
            DepKind::parse("discoveredfrom").unwrap(),
            DepKind::DiscoveredFrom
        );
    }

    #[test]
    fn dep_kind_parse_rejects_unknown() {
        let err = DepKind::parse("unknown").unwrap_err();
        assert!(err.to_string().contains("dependency kind"));
    }

    #[test]
    fn dep_kind_ordering_is_lexical() {
        let mut kinds = vec![
            DepKind::Related,
            DepKind::Blocks,
            DepKind::Parent,
            DepKind::DiscoveredFrom,
        ];
        kinds.sort();
        assert_eq!(
            kinds,
            vec![
                DepKind::Blocks,
                DepKind::DiscoveredFrom,
                DepKind::Parent,
                DepKind::Related
            ]
        );
    }

    #[test]
    fn priority_serde_validates_on_deserialize() {
        // Valid values
        for val in 0..=4u8 {
            let json = val.to_string();
            let p: Priority = serde_json::from_str(&json).unwrap();
            assert_eq!(p.value(), val);
        }

        // Invalid: out of range
        let json = "5";
        let err = serde_json::from_str::<Priority>(json).unwrap_err();
        assert!(err.to_string().contains("out of range"));

        let json = "255";
        let err = serde_json::from_str::<Priority>(json).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn priority_serde_roundtrip() {
        for val in 0..=4u8 {
            let p = Priority::new(val).unwrap();
            let json = serde_json::to_string(&p).unwrap();
            let back: Priority = serde_json::from_str(&json).unwrap();
            assert_eq!(p, back);
        }
    }
}
