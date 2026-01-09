//! Layer 6: Dependency edges
//!
//! DepKey: identity tuple (from, to, kind)
//! DepEdge: key + created + life (active/deleted via LWW)
//!
//! The `DepLife` enum with `Lww<DepLife>` makes delete-vs-restore
//! pure LWW comparison - algebraically cleaner than `Option<Stamp>`.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::crdt::Lww;
use super::domain::DepKind;
use super::error::{CoreError, InvalidDependency};
use super::identity::BeadId;
use super::time::Stamp;

/// Parsed dependency spec from CLI/IPC input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepSpec {
    kind: DepKind,
    id: BeadId,
}

impl DepSpec {
    /// Create a new dependency spec.
    pub fn new(kind: DepKind, id: BeadId) -> Self {
        Self { kind, id }
    }

    /// Parse a single dependency spec (`kind:id` or `id`).
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        let trimmed = raw.trim();
        let (kind, id_raw) = if let Some((k, id)) = trimmed.split_once(':') {
            (DepKind::parse(k)?, id.trim())
        } else {
            (DepKind::Blocks, trimmed)
        };
        let id = BeadId::parse(id_raw)?;
        Ok(Self::new(kind, id))
    }

    /// Parse a list of dependency specs, allowing comma-separated lists.
    pub fn parse_list(raw: &[String]) -> Result<Vec<Self>, CoreError> {
        let mut out = Vec::new();
        for spec in raw {
            for part in spec.split(',') {
                let p = part.trim();
                if p.is_empty() {
                    continue;
                }
                out.push(Self::parse(p)?);
            }
        }
        Ok(out)
    }

    /// Get the dependency kind.
    pub fn kind(&self) -> DepKind {
        self.kind
    }

    /// Get the dependency target bead ID.
    pub fn id(&self) -> &BeadId {
        &self.id
    }

    /// Format the spec string, omitting the default kind.
    pub fn to_spec_string(&self) -> String {
        if self.kind == DepKind::Blocks {
            self.id.as_str().to_string()
        } else {
            format!("{}:{}", self.kind.as_str(), self.id.as_str())
        }
    }
}

/// Dependency identity tuple.
///
/// Deps are unique by (from, to, kind). Self-dependencies are structurally
/// impossible - the constructor validates that `from != to`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DepKey {
    from: BeadId,
    to: BeadId,
    kind: DepKind,
}

impl DepKey {
    /// Create a new dependency key.
    ///
    /// Returns an error if `from == to` (self-dependency).
    pub fn new(from: BeadId, to: BeadId, kind: DepKind) -> Result<Self, InvalidDependency> {
        if from == to {
            return Err(InvalidDependency {
                reason: format!("cannot create self-dependency on {}", from),
            });
        }
        Ok(Self { from, to, kind })
    }

    /// Get the source bead ID (the bead that depends on another).
    pub fn from(&self) -> &BeadId {
        &self.from
    }

    /// Get the target bead ID (the bead being depended on).
    pub fn to(&self) -> &BeadId {
        &self.to
    }

    /// Get the dependency kind.
    pub fn kind(&self) -> DepKind {
        self.kind
    }
}

/// Serde proxy for DepKey that validates on deserialization.
#[derive(Serialize, Deserialize)]
struct DepKeyProxy {
    from: BeadId,
    to: BeadId,
    kind: DepKind,
}

impl Serialize for DepKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        DepKeyProxy {
            from: self.from.clone(),
            to: self.to.clone(),
            kind: self.kind,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DepKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let proxy = DepKeyProxy::deserialize(deserializer)?;
        DepKey::new(proxy.from, proxy.to, proxy.kind).map_err(serde::de::Error::custom)
    }
}

/// Dependency lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum DepLife {
    #[default]
    Active,
    Deleted,
}

/// Dependency edge with LWW lifecycle.
///
/// Merge semantics:
/// - `life` uses pure LWW comparison (higher stamp wins)
/// - Keep earlier `created` for stable provenance
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepEdge {
    pub key: DepKey,
    pub created: Stamp,
    pub life: Lww<DepLife>,
}

impl DepEdge {
    /// Create a new active dependency.
    pub fn new(key: DepKey, created: Stamp) -> Self {
        Self {
            key,
            created: created.clone(),
            life: Lww::new(DepLife::Active, created),
        }
    }

    /// Create a dependency with explicit life state.
    pub fn with_life(key: DepKey, created: Stamp, life: Lww<DepLife>) -> Self {
        Self { key, created, life }
    }

    /// Check if edge is active (not deleted).
    pub fn is_active(&self) -> bool {
        matches!(self.life.value, DepLife::Active)
    }

    /// Check if edge is deleted.
    pub fn is_deleted(&self) -> bool {
        matches!(self.life.value, DepLife::Deleted)
    }

    /// Mark edge as deleted with the given stamp.
    pub fn delete(&mut self, stamp: Stamp) {
        // Only update if this stamp is newer
        let new_life = Lww::new(DepLife::Deleted, stamp);
        self.life = Lww::join(&self.life, &new_life);
    }

    /// Restore a deleted edge (undelete) with the given stamp.
    pub fn restore(&mut self, stamp: Stamp) {
        // Only update if this stamp is newer
        let new_life = Lww::new(DepLife::Active, stamp);
        self.life = Lww::join(&self.life, &new_life);
    }

    /// Get deleted stamp if the edge is deleted.
    pub fn deleted_stamp(&self) -> Option<&Stamp> {
        if self.is_deleted() {
            Some(&self.life.stamp)
        } else {
            None
        }
    }

    /// Merge two edges with same key.
    ///
    /// - Life uses pure LWW comparison (higher stamp wins)
    /// - Keep earlier created for stable provenance
    pub fn join(a: &Self, b: &Self) -> Self {
        debug_assert_eq!(a.key, b.key, "join requires same key");

        // Keep earlier created for stable provenance
        let created = if a.created <= b.created {
            a.created.clone()
        } else {
            b.created.clone()
        };

        // Life is pure LWW
        let life = Lww::join(&a.life, &b.life);

        Self {
            key: a.key.clone(),
            created,
            life,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{ActorId, WriteStamp};

    fn make_stamp(wall_ms: u64, actor: &str) -> Stamp {
        Stamp::new(WriteStamp::new(wall_ms, 0), ActorId::new(actor).unwrap())
    }

    fn make_key() -> DepKey {
        DepKey::new(
            BeadId::parse("bd-abc").unwrap(),
            BeadId::parse("bd-xyz").unwrap(),
            DepKind::Blocks,
        )
        .unwrap()
    }

    #[test]
    fn self_dependency_rejected() {
        let id = BeadId::parse("bd-abc").unwrap();
        let result = DepKey::new(id.clone(), id, DepKind::Blocks);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.reason.contains("self-dependency"));
    }

    #[test]
    fn accessors_work() {
        let from = BeadId::parse("bd-abc").unwrap();
        let to = BeadId::parse("bd-xyz").unwrap();
        let key = DepKey::new(from.clone(), to.clone(), DepKind::Parent).unwrap();
        assert_eq!(key.from(), &from);
        assert_eq!(key.to(), &to);
        assert_eq!(key.kind(), DepKind::Parent);
    }

    #[test]
    fn serde_roundtrip() {
        let key = make_key();
        let json = serde_json::to_string(&key).unwrap();
        let parsed: DepKey = serde_json::from_str(&json).unwrap();
        assert_eq!(key, parsed);
    }

    #[test]
    fn serde_rejects_self_dep() {
        // Manually craft JSON with from == to
        let json = r#"{"from":"bd-abc","to":"bd-abc","kind":"blocks"}"#;
        let result: Result<DepKey, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn new_edge_is_active() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");
        let edge = DepEdge::new(key, stamp);
        assert!(edge.is_active());
        assert!(!edge.is_deleted());
    }

    #[test]
    fn dep_spec_parse_list_handles_kinds_and_commas() {
        let specs = vec![
            "blocks:bd-abc,bd-xyz".to_string(),
            "related:bd-rel".to_string(),
        ];
        let parsed = DepSpec::parse_list(&specs).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].kind(), DepKind::Blocks);
        assert_eq!(parsed[0].id().as_str(), "bd-abc");
        assert_eq!(parsed[1].kind(), DepKind::Blocks);
        assert_eq!(parsed[1].id().as_str(), "bd-xyz");
        assert_eq!(parsed[2].kind(), DepKind::Related);
        assert_eq!(parsed[2].id().as_str(), "bd-rel");
    }

    #[test]
    fn dep_spec_to_spec_string_omits_default_kind() {
        let id = BeadId::parse("bd-abc").unwrap();
        let spec = DepSpec::new(DepKind::Blocks, id);
        assert_eq!(spec.to_spec_string(), "bd-abc");

        let id = BeadId::parse("bd-rel").unwrap();
        let spec = DepSpec::new(DepKind::Related, id);
        assert_eq!(spec.to_spec_string(), "related:bd-rel");
    }

    #[test]
    fn delete_marks_deleted() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");
        let mut edge = DepEdge::new(key, stamp);

        let delete_stamp = make_stamp(2000, "bob");
        edge.delete(delete_stamp.clone());

        assert!(edge.is_deleted());
        assert_eq!(edge.deleted_stamp(), Some(&delete_stamp));
    }

    #[test]
    fn restore_undeletes() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");
        let mut edge = DepEdge::new(key, stamp);

        edge.delete(make_stamp(2000, "bob"));
        assert!(edge.is_deleted());

        edge.restore(make_stamp(3000, "charlie"));
        assert!(edge.is_active());
    }

    #[test]
    fn join_lww_later_wins() {
        let key = make_key();
        let stamp = make_stamp(1000, "alice");

        // Edge A: deleted at t=2000
        let mut edge_a = DepEdge::new(key.clone(), stamp.clone());
        edge_a.delete(make_stamp(2000, "bob"));

        // Edge B: active at t=3000 (restored)
        let mut edge_b = DepEdge::new(key.clone(), stamp.clone());
        edge_b.delete(make_stamp(2000, "bob"));
        edge_b.restore(make_stamp(3000, "charlie"));

        // Join: B wins because t=3000 > t=2000
        let merged = DepEdge::join(&edge_a, &edge_b);
        assert!(merged.is_active());
    }

    #[test]
    fn join_keeps_earlier_created() {
        let key = make_key();

        let edge_a = DepEdge::new(key.clone(), make_stamp(1000, "alice"));
        let edge_b = DepEdge::new(key.clone(), make_stamp(2000, "bob"));

        let merged = DepEdge::join(&edge_a, &edge_b);
        assert_eq!(merged.created, make_stamp(1000, "alice"));
    }
}
