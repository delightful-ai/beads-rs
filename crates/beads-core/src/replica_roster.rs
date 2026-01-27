//! Replica roster types for realtime replication.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{NamespaceId, ReplicaId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ReplicaRosterWire", into = "ReplicaRosterWire")]
pub struct ReplicaRoster {
    pub replicas: Vec<ReplicaEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplicaRosterWire {
    replicas: Vec<ReplicaEntryWire>,
}

impl TryFrom<ReplicaRosterWire> for ReplicaRoster {
    type Error = ReplicaRosterError;

    fn try_from(value: ReplicaRosterWire) -> Result<Self, Self::Error> {
        let replicas = value
            .replicas
            .into_iter()
            .map(ReplicaEntry::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { replicas })
    }
}

impl From<ReplicaRoster> for ReplicaRosterWire {
    fn from(value: ReplicaRoster) -> Self {
        Self {
            replicas: value
                .replicas
                .into_iter()
                .map(ReplicaEntryWire::from)
                .collect(),
        }
    }
}

impl ReplicaRoster {
    pub fn from_toml_str(input: &str) -> Result<Self, ReplicaRosterError> {
        let roster_wire: ReplicaRosterWire = toml::from_str(input)?;
        let mut roster: ReplicaRoster = roster_wire.try_into()?;
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
#[serde(try_from = "ReplicaEntryWire", into = "ReplicaEntryWire")]
pub struct ReplicaEntry {
    pub replica_id: ReplicaId,
    pub name: String,
    pub role: ReplicaDurabilityRole,
    #[serde(default)]
    pub allowed_namespaces: Option<Vec<NamespaceId>>,
    #[serde(default)]
    pub expire_after_ms: Option<u64>,
}

impl ReplicaEntry {
    pub fn role(&self) -> ReplicaRole {
        self.role.role()
    }

    pub fn durability_eligible(&self) -> bool {
        self.role.durability_eligible()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplicaEntryWire {
    pub replica_id: ReplicaId,
    pub name: String,
    pub role: ReplicaRole,
    pub durability_eligible: bool,
    #[serde(default)]
    pub allowed_namespaces: Option<Vec<NamespaceId>>,
    #[serde(default)]
    pub expire_after_ms: Option<u64>,
}

impl TryFrom<ReplicaEntryWire> for ReplicaEntry {
    type Error = ReplicaDurabilityRoleError;

    fn try_from(value: ReplicaEntryWire) -> Result<Self, Self::Error> {
        let role = ReplicaDurabilityRole::try_from((value.role, value.durability_eligible))?;
        Ok(Self {
            replica_id: value.replica_id,
            name: value.name,
            role,
            allowed_namespaces: value.allowed_namespaces,
            expire_after_ms: value.expire_after_ms,
        })
    }
}

impl From<ReplicaEntry> for ReplicaEntryWire {
    fn from(value: ReplicaEntry) -> Self {
        let role = value.role.role();
        let durability_eligible = value.role.durability_eligible();
        Self {
            replica_id: value.replica_id,
            name: value.name,
            role,
            durability_eligible,
            allowed_namespaces: value.allowed_namespaces,
            expire_after_ms: value.expire_after_ms,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRole {
    Anchor,
    Peer,
    Observer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReplicaDurabilityRole {
    Anchor { eligible: bool },
    Peer { eligible: bool },
    Observer,
}

impl ReplicaDurabilityRole {
    pub fn anchor(eligible: bool) -> Self {
        Self::Anchor { eligible }
    }

    pub fn peer(eligible: bool) -> Self {
        Self::Peer { eligible }
    }

    pub fn observer() -> Self {
        Self::Observer
    }

    pub fn role(&self) -> ReplicaRole {
        match self {
            Self::Anchor { .. } => ReplicaRole::Anchor,
            Self::Peer { .. } => ReplicaRole::Peer,
            Self::Observer => ReplicaRole::Observer,
        }
    }

    pub fn durability_eligible(&self) -> bool {
        match self {
            Self::Anchor { eligible } | Self::Peer { eligible } => *eligible,
            Self::Observer => false,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("durability eligibility {eligible} invalid for role {role:?}")]
pub struct ReplicaDurabilityRoleError {
    pub role: ReplicaRole,
    pub eligible: bool,
}

impl TryFrom<(ReplicaRole, bool)> for ReplicaDurabilityRole {
    type Error = ReplicaDurabilityRoleError;

    fn try_from(value: (ReplicaRole, bool)) -> Result<Self, Self::Error> {
        let (role, eligible) = value;
        match role {
            ReplicaRole::Anchor => Ok(Self::Anchor { eligible }),
            ReplicaRole::Peer => Ok(Self::Peer { eligible }),
            ReplicaRole::Observer => {
                if eligible {
                    Err(ReplicaDurabilityRoleError { role, eligible })
                } else {
                    Ok(Self::Observer)
                }
            }
        }
    }
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
    #[error("invalid durability eligibility {eligible} for role {role:?}")]
    InvalidRoleEligibility { role: ReplicaRole, eligible: bool },
}

impl From<ReplicaDurabilityRoleError> for ReplicaRosterError {
    fn from(value: ReplicaDurabilityRoleError) -> Self {
        Self::InvalidRoleEligibility {
            role: value.role,
            eligible: value.eligible,
        }
    }
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
        let expected = Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("uuid");
        assert_eq!(entry.replica_id, ReplicaId::new(expected));
        assert_eq!(entry.role, ReplicaDurabilityRole::anchor(true));
        assert_eq!(entry.role(), ReplicaRole::Anchor);
        assert!(entry.durability_eligible());
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

    #[test]
    fn rejects_observer_with_durability_eligibility() {
        let input = r#"
[[replicas]]
replica_id = "00000000-0000-0000-0000-000000000001"
name = "observer"
role = "observer"
durability_eligible = true
"#;

        let err = ReplicaRoster::from_toml_str(input).unwrap_err();
        assert!(matches!(
            err,
            ReplicaRosterError::InvalidRoleEligibility {
                role: ReplicaRole::Observer,
                eligible: true
            }
        ));
    }
}
