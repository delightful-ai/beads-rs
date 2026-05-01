//! Dependency schemas.

use serde::{Deserialize, Serialize};

use beads_core::DepKey as CoreDepKey;

// =============================================================================
// Dependencies
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepEdge {
    pub from_namespace: String,
    pub from: String,
    pub to_namespace: String,
    pub to: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepCycles {
    pub cycles: Vec<Vec<String>>,
}

impl From<&CoreDepKey> for DepEdge {
    fn from(key: &CoreDepKey) -> Self {
        Self {
            from_namespace: key.from_ref().namespace().as_str().to_string(),
            from: key.from().as_str().to_string(),
            to_namespace: key.to_ref().namespace().as_str().to_string(),
            to: key.to().as_str().to_string(),
            kind: key.kind().as_str().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beads_core::{BeadId, BeadRef, DepKind, NamespaceId};

    #[test]
    fn dep_edge_from_core_key_carries_endpoint_namespaces() {
        let key = CoreDepKey::new(
            BeadRef::new(
                NamespaceId::parse("sessions").unwrap(),
                BeadId::parse("bd-a").unwrap(),
            ),
            BeadRef::new(
                NamespaceId::parse("extmsg").unwrap(),
                BeadId::parse("bd-b").unwrap(),
            ),
            DepKind::Blocks,
        )
        .unwrap();

        let edge = DepEdge::from(&key);
        assert_eq!(edge.from_namespace, "sessions");
        assert_eq!(edge.from, "bd-a");
        assert_eq!(edge.to_namespace, "extmsg");
        assert_eq!(edge.to, "bd-b");

        let json = serde_json::to_value(&edge).unwrap();
        assert_eq!(json["from_namespace"], "sessions");
        assert_eq!(json["from"], "bd-a");
        assert_eq!(json["to_namespace"], "extmsg");
        assert_eq!(json["to"], "bd-b");
    }
}
