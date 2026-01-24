//! Layer 6: Dependency edges
//!
//! DepKey: identity tuple (from, to, kind)
//! Dependency edges are represented as OR-Set membership of `DepKey`.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::domain::DepKind;
use super::error::{CoreError, InvalidDependency};
use super::identity::BeadId;
use super::orset::OrSetValue;

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
}
