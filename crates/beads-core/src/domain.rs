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
    Convoy,
    Gate,
    #[serde(rename = "merge-request", alias = "merge_request")]
    MergeRequest,
    Agent,
    Role,
    Rig,
    Session,
    Spec,
    Convergence,
}

crate::enum_str! {
    impl BeadType {
        pub fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            Bug => ["bug"],
            Feature => ["feature", "feat", "enhancement"],
            Task => ["task"],
            Epic => ["epic"],
            Chore => ["chore"],
            Decision => ["decision", "dec", "adr"],
            Message => ["message"],
            Molecule => ["molecule"],
            Spike => ["spike", "investigation", "timebox"],
            Story => ["story", "user-story", "user_story"],
            Milestone => ["milestone", "ms"],
            Event => ["event"],
            Convoy => ["convoy"],
            Gate => ["gate"],
            MergeRequest => ["merge-request", "merge_request", "mergerequest"],
            Agent => ["agent"],
            Role => ["role"],
            Rig => ["rig"],
            Session => ["session"],
            Spec => ["spec"],
            Convergence => ["convergence"],
        }
    }
}

impl BeadType {
    pub fn parse(raw: &str) -> Option<Self> {
        let s = raw.trim().to_lowercase();
        Self::parse_str(&s)
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

    /// Returns true if this dependency kind affects ready-work calculation.
    pub fn affects_readiness(&self) -> bool {
        matches!(
            self,
            Self::Blocks | Self::ConditionalBlocks | Self::WaitsFor
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
    fn bead_type_parse_accepts_gascity_known_types_and_aliases() {
        let cases = [
            ("enhancement", BeadType::Feature),
            ("feat", BeadType::Feature),
            ("dec", BeadType::Decision),
            ("adr", BeadType::Decision),
            ("investigation", BeadType::Spike),
            ("timebox", BeadType::Spike),
            ("user-story", BeadType::Story),
            ("user_story", BeadType::Story),
            ("ms", BeadType::Milestone),
            ("convoy", BeadType::Convoy),
            ("gate", BeadType::Gate),
            ("merge-request", BeadType::MergeRequest),
            ("merge_request", BeadType::MergeRequest),
            ("agent", BeadType::Agent),
            ("role", BeadType::Role),
            ("rig", BeadType::Rig),
            ("session", BeadType::Session),
            ("spec", BeadType::Spec),
            ("convergence", BeadType::Convergence),
        ];

        for (raw, expected) in cases {
            assert_eq!(BeadType::parse(raw), Some(expected), "{raw}");
        }
    }

    #[test]
    fn bead_type_json_preserves_gascity_strings_and_rejects_unknowns() {
        assert_eq!(
            serde_json::to_string(&BeadType::MergeRequest).unwrap(),
            r#""merge-request""#
        );
        assert_eq!(
            serde_json::from_str::<BeadType>(r#""merge_request""#).unwrap(),
            BeadType::MergeRequest
        );
        assert_eq!(BeadType::parse("wisp"), None);
        assert!(serde_json::from_str::<BeadType>(r#""wisp""#).is_err());
    }

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
    fn dep_kind_parse_accepts_well_known_go_vocabulary() {
        let cases = [
            ("blocks", DepKind::Blocks),
            ("parent-child", DepKind::Parent),
            ("conditional-blocks", DepKind::ConditionalBlocks),
            ("waits-for", DepKind::WaitsFor),
            ("related", DepKind::Related),
            ("discovered-from", DepKind::DiscoveredFrom),
            ("replies-to", DepKind::RepliesTo),
            ("relates-to", DepKind::RelatesTo),
            ("duplicates", DepKind::Duplicates),
            ("supersedes", DepKind::Supersedes),
            ("authored-by", DepKind::AuthoredBy),
            ("assigned-to", DepKind::AssignedTo),
            ("approved-by", DepKind::ApprovedBy),
            ("attests", DepKind::Attests),
            ("tracks", DepKind::Tracks),
            ("until", DepKind::Until),
            ("caused-by", DepKind::CausedBy),
            ("validates", DepKind::Validates),
            ("delegated-from", DepKind::DelegatedFrom),
        ];

        for (raw, expected) in cases {
            assert_eq!(DepKind::parse(raw).unwrap(), expected, "{raw}");
        }
    }

    #[test]
    fn dep_kind_readiness_predicate_matches_live_ready_queries() {
        let readiness_affecting = [
            DepKind::Blocks,
            DepKind::ConditionalBlocks,
            DepKind::WaitsFor,
        ];
        for kind in readiness_affecting {
            assert!(
                kind.affects_readiness(),
                "expected {kind:?} to affect ready work"
            );
        }

        let non_readiness = [
            DepKind::Parent,
            DepKind::Related,
            DepKind::DiscoveredFrom,
            DepKind::RepliesTo,
            DepKind::RelatesTo,
            DepKind::Duplicates,
            DepKind::Supersedes,
            DepKind::AuthoredBy,
            DepKind::AssignedTo,
            DepKind::ApprovedBy,
            DepKind::Attests,
            DepKind::Tracks,
            DepKind::Until,
            DepKind::CausedBy,
            DepKind::Validates,
            DepKind::DelegatedFrom,
        ];
        for kind in non_readiness {
            assert!(
                !kind.affects_readiness(),
                "expected {kind:?} to be informational"
            );
        }
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
