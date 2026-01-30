//! Layer 6: Dependency edges
//!
//! DepKey: identity tuple (from, to, kind)
//! Dependency edges are represented as OR-Set membership of `DepKey`.

use std::cmp::Ordering;
use std::ops::Deref;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::domain::DepKind;
use super::error::{CoreError, InvalidDependency};
use super::identity::BeadId;
use super::orset::{OrSetValue, sealed::Sealed};
use super::validated::{ValidatedBeadId, ValidatedDepKind};

/// Parsed dependency spec from CLI/IPC input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct DepSpec {
    kind: DepKind,
    id: BeadId,
}

impl DepSpec {
    /// Create a new dependency spec.
    pub fn new(kind: DepKind, id: BeadId) -> Result<Self, InvalidDependency> {
        if kind == DepKind::Parent {
            return Err(InvalidDependency {
                reason: "parent edges must use parent-specific operations".to_string(),
            });
        }
        Ok(Self { kind, id })
    }

    /// Parse a single dependency spec (`kind:id` or `id`).
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        let trimmed = raw.trim();
        let (kind, id_raw) = if let Some((k, id)) = trimmed.split_once(':') {
            (ValidatedDepKind::parse(k)?, id.trim())
        } else {
            (ValidatedDepKind::from(DepKind::Blocks), trimmed)
        };
        let id = ValidatedBeadId::parse(id_raw)?.into_inner();
        Ok(Self::new(kind.into_inner(), id)?)
    }

    /// Parse a list of dependency specs, allowing comma-separated lists.
    pub fn parse_list(raw: &[String]) -> Result<DepSpecSet, CoreError> {
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
        Ok(DepSpecSet::new(out))
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

impl Ord for DepSpec {
    fn cmp(&self, other: &Self) -> Ordering {
        self.kind
            .cmp(&other.kind)
            .then_with(|| self.id.cmp(&other.id))
    }
}

impl PartialOrd for DepSpec {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<String> for DepSpec {
    type Error = CoreError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        DepSpec::parse(&raw)
    }
}

impl From<DepSpec> for String {
    fn from(spec: DepSpec) -> String {
        spec.to_spec_string()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(from = "Vec<DepSpec>", into = "Vec<DepSpec>")]
pub struct DepSpecSet(Vec<DepSpec>);

impl DepSpecSet {
    pub fn new(mut specs: Vec<DepSpec>) -> Self {
        canonicalize_dep_specs(&mut specs);
        Self(specs)
    }

    pub fn as_slice(&self) -> &[DepSpec] {
        &self.0
    }

    pub fn iter(&self) -> impl Iterator<Item = &DepSpec> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_vec(self) -> Vec<DepSpec> {
        self.0
    }
}

impl From<Vec<DepSpec>> for DepSpecSet {
    fn from(specs: Vec<DepSpec>) -> Self {
        Self::new(specs)
    }
}

impl From<DepSpecSet> for Vec<DepSpec> {
    fn from(specs: DepSpecSet) -> Self {
        specs.0
    }
}

impl AsRef<[DepSpec]> for DepSpecSet {
    fn as_ref(&self) -> &[DepSpec] {
        &self.0
    }
}

impl Deref for DepSpecSet {
    type Target = [DepSpec];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromIterator<DepSpec> for DepSpecSet {
    fn from_iter<T: IntoIterator<Item = DepSpec>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

fn canonicalize_dep_specs(specs: &mut Vec<DepSpec>) {
    if specs.len() <= 1 {
        return;
    }
    specs.sort();
    specs.dedup();
}

/// Parent-child edge (child -> parent).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ParentEdge {
    child: BeadId,
    parent: BeadId,
}

impl ParentEdge {
    /// Create a new parent edge.
    ///
    /// Returns an error if `child == parent`.
    pub fn new(child: BeadId, parent: BeadId) -> Result<Self, InvalidDependency> {
        if child == parent {
            return Err(InvalidDependency {
                reason: format!("cannot create self-parent edge on {}", child),
            });
        }
        Ok(Self { child, parent })
    }

    /// Get the child bead ID.
    pub fn child(&self) -> &BeadId {
        &self.child
    }

    /// Get the parent bead ID.
    pub fn parent(&self) -> &BeadId {
        &self.parent
    }

    /// Convert to a dependency key with kind `Parent`.
    pub fn to_dep_key(&self) -> DepKey {
        DepKey::new(self.child.clone(), self.parent.clone(), DepKind::Parent)
            .expect("parent edge should be valid")
    }
}

impl TryFrom<DepKey> for ParentEdge {
    type Error = InvalidDependency;

    fn try_from(key: DepKey) -> Result<Self, Self::Error> {
        if key.kind() != DepKind::Parent {
            return Err(InvalidDependency {
                reason: format!("expected parent dependency, got {}", key.kind().as_str()),
            });
        }
        ParentEdge::new(key.from.clone(), key.to.clone())
    }
}

/// Serde proxy for ParentEdge that validates on deserialization.
#[derive(Serialize, Deserialize)]
struct ParentEdgeProxy {
    child: BeadId,
    parent: BeadId,
}

impl Serialize for ParentEdge {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ParentEdgeProxy {
            child: self.child.clone(),
            parent: self.parent.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ParentEdge {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let proxy = ParentEdgeProxy::deserialize(deserializer)?;
        ParentEdge::new(proxy.child, proxy.parent).map_err(serde::de::Error::custom)
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

impl Sealed for DepKey {}

impl OrSetValue for DepKey {
    fn collision_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend(self.from.as_str().as_bytes());
        bytes.push(0);
        bytes.extend(self.to.as_str().as_bytes());
        bytes.push(0);
        bytes.extend(self.kind.as_str().as_bytes());
        bytes
    }
}

/// Proof token that a dependency edge does not introduce a DAG cycle.
///
/// Only constructible by core cycle checks.
#[derive(Debug)]
pub struct NoCycleProof {
    _private: (),
}

impl NoCycleProof {
    pub(crate) fn new() -> Self {
        Self { _private: () }
    }
}

/// Dependency key for DAG-only kinds (`Blocks`, `Parent`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AcyclicDepKey {
    key: DepKey,
}

impl AcyclicDepKey {
    pub fn new(
        from: BeadId,
        to: BeadId,
        kind: DepKind,
        _proof: NoCycleProof,
    ) -> Result<Self, InvalidDependency> {
        if !kind.requires_dag() {
            return Err(InvalidDependency {
                reason: format!(
                    "dependency kind {} does not require DAG proof",
                    kind.as_str()
                ),
            });
        }
        Ok(Self {
            key: DepKey::new(from, to, kind)?,
        })
    }

    pub fn from_dep_key(key: DepKey, _proof: NoCycleProof) -> Result<Self, InvalidDependency> {
        if !key.kind().requires_dag() {
            return Err(InvalidDependency {
                reason: format!(
                    "dependency kind {} does not require DAG proof",
                    key.kind().as_str()
                ),
            });
        }
        Ok(Self { key })
    }

    pub fn into_inner(self) -> DepKey {
        self.key
    }
}

impl AsRef<DepKey> for AcyclicDepKey {
    fn as_ref(&self) -> &DepKey {
        &self.key
    }
}

impl Deref for AcyclicDepKey {
    type Target = DepKey;

    fn deref(&self) -> &Self::Target {
        &self.key
    }
}

/// Dependency key for non-DAG kinds (`Related`, `DiscoveredFrom`).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FreeDepKey {
    key: DepKey,
}

impl FreeDepKey {
    pub fn new(from: BeadId, to: BeadId, kind: DepKind) -> Result<Self, InvalidDependency> {
        if kind.requires_dag() {
            return Err(InvalidDependency {
                reason: format!("dependency kind {} requires DAG proof", kind.as_str()),
            });
        }
        Ok(Self {
            key: DepKey::new(from, to, kind)?,
        })
    }

    pub fn from_dep_key(key: DepKey) -> Result<Self, InvalidDependency> {
        if key.kind().requires_dag() {
            return Err(InvalidDependency {
                reason: format!("dependency kind {} requires DAG proof", key.kind().as_str()),
            });
        }
        Ok(Self { key })
    }

    pub fn into_inner(self) -> DepKey {
        self.key
    }
}

impl AsRef<DepKey> for FreeDepKey {
    fn as_ref(&self) -> &DepKey {
        &self.key
    }
}

impl Deref for FreeDepKey {
    type Target = DepKey;

    fn deref(&self) -> &Self::Target {
        &self.key
    }
}

/// Typed dependency key for dependency adds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DepAddKey {
    Acyclic(AcyclicDepKey),
    Free(FreeDepKey),
}

impl DepAddKey {
    pub fn as_dep_key(&self) -> &DepKey {
        match self {
            Self::Acyclic(key) => key.as_ref(),
            Self::Free(key) => key.as_ref(),
        }
    }

    pub fn into_dep_key(self) -> DepKey {
        match self {
            Self::Acyclic(key) => key.into_inner(),
            Self::Free(key) => key.into_inner(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let edge = ParentEdge::new(from.clone(), to.clone()).unwrap();
        assert_eq!(edge.child(), &from);
        assert_eq!(edge.parent(), &to);
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
    fn dep_spec_parse_list_handles_kinds_and_commas() {
        let specs = vec![
            "blocks:bd-abc,bd-xyz".to_string(),
            "related:bd-rel".to_string(),
        ];
        let parsed = DepSpec::parse_list(&specs).unwrap();
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed.as_slice()[0].kind(), DepKind::Blocks);
        assert_eq!(parsed.as_slice()[0].id().as_str(), "bd-abc");
        assert_eq!(parsed.as_slice()[1].kind(), DepKind::Blocks);
        assert_eq!(parsed.as_slice()[1].id().as_str(), "bd-xyz");
        assert_eq!(parsed.as_slice()[2].kind(), DepKind::Related);
        assert_eq!(parsed.as_slice()[2].id().as_str(), "bd-rel");
    }

    #[test]
    fn dep_spec_to_spec_string_omits_default_kind() {
        let id = BeadId::parse("bd-abc").unwrap();
        let spec = DepSpec::new(DepKind::Blocks, id).unwrap();
        assert_eq!(spec.to_spec_string(), "bd-abc");

        let id = BeadId::parse("bd-rel").unwrap();
        let spec = DepSpec::new(DepKind::Related, id).unwrap();
        assert_eq!(spec.to_spec_string(), "related:bd-rel");
    }

    #[test]
    fn dep_spec_rejects_parent_kind() {
        let id = BeadId::parse("bd-abc").unwrap();
        let err = DepSpec::new(DepKind::Parent, id).unwrap_err();
        assert!(
            err.reason
                .contains("parent edges must use parent-specific operations")
        );
    }

    #[test]
    fn dep_spec_set_sorts_and_dedups() {
        let a = DepSpec::parse("related:bd-aaa").unwrap();
        let b = DepSpec::parse("blocks:bd-bbb").unwrap();
        let set = DepSpecSet::from(vec![a.clone(), b.clone(), a.clone()]);
        assert_eq!(set.len(), 2);
        assert_eq!(set.as_slice()[0], b);
        assert_eq!(set.as_slice()[1], a);
    }

    #[test]
    fn dep_spec_set_json_roundtrip() {
        let set = DepSpecSet::from(vec![
            DepSpec::parse("bd-abc").unwrap(),
            DepSpec::parse("related:bd-xyz").unwrap(),
        ]);
        let json = serde_json::to_string(&set).unwrap();
        let parsed: DepSpecSet = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, set);
    }

    #[test]
    fn parent_edge_rejects_self_parent() {
        let id = BeadId::parse("bd-abc").unwrap();
        let err = ParentEdge::new(id.clone(), id).unwrap_err();
        assert!(err.reason.contains("self-parent"));
    }
}
