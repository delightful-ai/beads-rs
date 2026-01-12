//! Namespace identity and policies.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::{CoreError, InvalidId, ReplicaId};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct NamespaceId(String);

impl NamespaceId {
    pub const CORE: &'static str = "core";
    const MAX_LEN: usize = 32;

    pub fn parse(s: impl Into<String>) -> Result<Self, CoreError> {
        let raw = s.into();
        if raw.is_empty() {
            return Err(InvalidId::Namespace {
                raw,
                reason: "empty".into(),
            }
            .into());
        }
        if raw.len() > Self::MAX_LEN {
            return Err(InvalidId::Namespace {
                raw,
                reason: format!("length must be <= {}", Self::MAX_LEN),
            }
            .into());
        }
        let bytes = raw.as_bytes();
        let first = bytes[0];
        if !first.is_ascii_lowercase() {
            return Err(InvalidId::Namespace {
                raw,
                reason: "must start with [a-z]".into(),
            }
            .into());
        }
        for &b in &bytes[1..] {
            let ok = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_';
            if !ok {
                return Err(InvalidId::Namespace {
                    raw,
                    reason: "contains invalid character".into(),
                }
                .into());
            }
        }
        Ok(Self(raw))
    }

    pub fn core() -> Self {
        Self(Self::CORE.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NamespaceId({:?})", self.0)
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for NamespaceId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        NamespaceId::parse(s)
    }
}

impl From<NamespaceId> for String {
    fn from(id: NamespaceId) -> String {
        id.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicateMode {
    None,
    Anchors,
    Peers,
    P2p,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPolicy {
    Forever,
    Ttl { ttl_ms: u64 },
    Size { max_bytes: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceVisibility {
    Normal,
    Pinned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcAuthority {
    CheckpointWriter,
    ExplicitReplica { replica_id: ReplicaId },
    None,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TtlBasis {
    LastMutationStamp,
    EventTime,
    ExplicitField,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespacePolicy {
    pub persist_to_git: bool,
    pub replicate_mode: ReplicateMode,
    pub retention: RetentionPolicy,
    pub ready_eligible: bool,
    pub visibility: NamespaceVisibility,
    pub gc_authority: GcAuthority,
    pub ttl_basis: TtlBasis,
}

impl NamespacePolicy {
    const DAY_MS: u64 = 24 * 60 * 60 * 1_000;

    pub fn core_default() -> Self {
        Self {
            persist_to_git: true,
            replicate_mode: ReplicateMode::Peers,
            retention: RetentionPolicy::Forever,
            ready_eligible: true,
            visibility: NamespaceVisibility::Normal,
            gc_authority: GcAuthority::CheckpointWriter,
            ttl_basis: TtlBasis::LastMutationStamp,
        }
    }

    pub fn sys_default() -> Self {
        Self {
            persist_to_git: true,
            replicate_mode: ReplicateMode::Anchors,
            retention: RetentionPolicy::Forever,
            ready_eligible: false,
            visibility: NamespaceVisibility::Pinned,
            gc_authority: GcAuthority::CheckpointWriter,
            ttl_basis: TtlBasis::LastMutationStamp,
        }
    }

    pub fn wf_default() -> Self {
        Self {
            persist_to_git: false,
            replicate_mode: ReplicateMode::Anchors,
            retention: RetentionPolicy::Ttl {
                ttl_ms: 7 * Self::DAY_MS,
            },
            ready_eligible: false,
            visibility: NamespaceVisibility::Normal,
            gc_authority: GcAuthority::CheckpointWriter,
            ttl_basis: TtlBasis::LastMutationStamp,
        }
    }

    pub fn tmp_default() -> Self {
        Self {
            persist_to_git: false,
            replicate_mode: ReplicateMode::None,
            retention: RetentionPolicy::Ttl {
                ttl_ms: Self::DAY_MS,
            },
            ready_eligible: false,
            visibility: NamespaceVisibility::Normal,
            gc_authority: GcAuthority::None,
            ttl_basis: TtlBasis::LastMutationStamp,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointGroup {
    pub namespaces: Vec<NamespaceId>,
    pub git_remote: Option<String>,
    pub git_ref: String,
    pub checkpoint_writers: Vec<ReplicaId>,
    pub primary_writer: Option<ReplicaId>,
    pub debounce_ms: u64,
    pub max_interval_ms: u64,
    pub max_events: u64,
    pub durable_copy_via_git: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_id_validates() {
        let valid = ["core", "a", "abc123", "a_b", "a0_b1"];
        for name in valid {
            let id = NamespaceId::parse(name).unwrap();
            assert_eq!(id.as_str(), name);
        }

        let invalid = [
            "",
            "Core",
            "1core",
            "_core",
            "core-1",
            "core name",
            "core/name",
        ];
        for name in invalid {
            assert!(NamespaceId::parse(name).is_err(), "{name}");
        }

        let too_long = "a".repeat(33);
        assert!(NamespaceId::parse(too_long).is_err());
    }

    #[test]
    fn namespace_id_serde_roundtrip() {
        let id = NamespaceId::parse("core").unwrap();
        let json = serde_json::to_string(&id).unwrap();
        let parsed: NamespaceId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn namespace_policy_defaults_match_plan() {
        let core = NamespacePolicy::core_default();
        assert!(core.persist_to_git);
        assert_eq!(core.replicate_mode, ReplicateMode::Peers);
        assert_eq!(core.retention, RetentionPolicy::Forever);
        assert!(core.ready_eligible);
        assert_eq!(core.visibility, NamespaceVisibility::Normal);
        assert_eq!(core.gc_authority, GcAuthority::CheckpointWriter);
        assert_eq!(core.ttl_basis, TtlBasis::LastMutationStamp);

        let sys = NamespacePolicy::sys_default();
        assert!(sys.persist_to_git);
        assert_eq!(sys.replicate_mode, ReplicateMode::Anchors);
        assert_eq!(sys.retention, RetentionPolicy::Forever);
        assert!(!sys.ready_eligible);
        assert_eq!(sys.visibility, NamespaceVisibility::Pinned);
        assert_eq!(sys.gc_authority, GcAuthority::CheckpointWriter);
        assert_eq!(sys.ttl_basis, TtlBasis::LastMutationStamp);

        let wf = NamespacePolicy::wf_default();
        assert!(!wf.persist_to_git);
        assert_eq!(wf.replicate_mode, ReplicateMode::Anchors);
        assert_eq!(
            wf.retention,
            RetentionPolicy::Ttl {
                ttl_ms: 7 * NamespacePolicy::DAY_MS
            }
        );
        assert!(!wf.ready_eligible);
        assert_eq!(wf.visibility, NamespaceVisibility::Normal);
        assert_eq!(wf.gc_authority, GcAuthority::CheckpointWriter);
        assert_eq!(wf.ttl_basis, TtlBasis::LastMutationStamp);

        let tmp = NamespacePolicy::tmp_default();
        assert!(!tmp.persist_to_git);
        assert_eq!(tmp.replicate_mode, ReplicateMode::None);
        assert_eq!(
            tmp.retention,
            RetentionPolicy::Ttl {
                ttl_ms: NamespacePolicy::DAY_MS
            }
        );
        assert!(!tmp.ready_eligible);
        assert_eq!(tmp.visibility, NamespaceVisibility::Normal);
        assert_eq!(tmp.gc_authority, GcAuthority::None);
        assert_eq!(tmp.ttl_basis, TtlBasis::LastMutationStamp);
    }
}
