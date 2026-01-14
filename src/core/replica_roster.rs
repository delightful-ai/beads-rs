//! Replica roster types for realtime replication.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::core::{NamespaceId, ReplicaId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaRoster {
    pub replicas: Vec<ReplicaEntry>,
}

impl ReplicaRoster {
    pub fn from_toml_str(input: &str) -> Result<Self, ReplicaRosterError> {
        let mut roster: ReplicaRoster = toml::from_str(input)?;
        roster.normalize();
        roster.validate()?;
        Ok(roster)
    }

    pub fn replica(&self, replica_id: &ReplicaId) -> Option<&ReplicaEntry> {
        self.replicas
            .iter()
            .find(|entry| &entry.replica_id == replica_id)
    }

    fn normalize(&mut self) {
        for entry in &mut self.replicas {
            if let Some(namespaces) = &mut entry.allowed_namespaces {
                namespaces.sort();
                namespaces.dedup();
            }
        }
    }

    fn validate(&self) -> Result<(), ReplicaRosterError> {
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();

        for entry in &self.replicas {
            if entry.name.trim().is_empty() {
                return Err(ReplicaRosterError::InvalidName {
                    reason: "name cannot be empty".to_string(),
                });
            }
            if !ids.insert(entry.replica_id) {
                return Err(ReplicaRosterError::DuplicateReplicaId {
                    replica_id: entry.replica_id,
                });
            }
            if !names.insert(entry.name.clone()) {
                return Err(ReplicaRosterError::DuplicateName {
                    name: entry.name.clone(),
                });
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaEntry {
    pub replica_id: ReplicaId,
    pub name: String,
    pub role: ReplicaRole,
    pub durability_eligible: bool,
    #[serde(default)]
    pub allowed_namespaces: Option<Vec<NamespaceId>>,
    #[serde(default)]
    pub expire_after_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRole {
    Anchor,
    Peer,
    Observer,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReplicaRosterError {
    #[error("replica roster parse failed: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("duplicate replica_id {replica_id}")]
    DuplicateReplicaId { replica_id: ReplicaId },
    #[error("duplicate replica name {name}")]
    DuplicateName { name: String },
    #[error("invalid replica name: {reason}")]
    InvalidName { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn parses_roster_with_defaults() {
        let input = r#"
[[replicas]]
replica_id = "00000000-0000-0000-0000-000000000001"
name = "alpha"
role = "anchor"
durability_eligible = true
"#;

        let roster = ReplicaRoster::from_toml_str(input).unwrap();
        assert_eq!(roster.replicas.len(), 1);
        let entry = &roster.replicas[0];
        assert_eq!(entry.replica_id, ReplicaId::new(Uuid::from_bytes([0u8; 16])));
        assert_eq!(entry.role, ReplicaRole::Anchor);
        assert!(entry.durability_eligible);
        assert!(entry.allowed_namespaces.is_none());
        assert!(entry.expire_after_ms.is_none());
    }

    #[test]
    fn rejects_duplicate_replica_id() {
        let input = r#"
[[replicas]]
replica_id = "00000000-0000-0000-0000-000000000001"
name = "alpha"
role = "anchor"
durability_eligible = true

[[replicas]]
replica_id = "00000000-0000-0000-0000-000000000001"
name = "beta"
role = "peer"
durability_eligible = false
"#;

        let err = ReplicaRoster::from_toml_str(input).unwrap_err();
        assert!(matches!(err, ReplicaRosterError::DuplicateReplicaId { .. }));
    }

    #[test]
    fn rejects_duplicate_replica_name() {
        let input = r#"
[[replicas]]
replica_id = "00000000-0000-0000-0000-000000000001"
name = "alpha"
role = "anchor"
durability_eligible = true

[[replicas]]
replica_id = "00000000-0000-0000-0000-000000000002"
name = "alpha"
role = "peer"
durability_eligible = false
"#;

        let err = ReplicaRoster::from_toml_str(input).unwrap_err();
        assert!(matches!(err, ReplicaRosterError::DuplicateName { .. }));
    }
}
