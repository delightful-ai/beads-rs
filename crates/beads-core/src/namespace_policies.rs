//! Namespace policy file parsing.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{NamespaceId, NamespacePolicy};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespacePolicies {
    pub namespaces: BTreeMap<NamespaceId, NamespacePolicy>,
}

impl NamespacePolicies {
    pub fn from_toml_str(input: &str) -> Result<Self, NamespacePoliciesError> {
        let mut policies: NamespacePolicies = toml::from_str(input)?;
        policies.normalize();
        policies.validate()?;
        Ok(policies)
    }

    fn normalize(&mut self) {
        // BTreeMap already keeps namespaces sorted; nothing else to normalize yet.
    }

    fn validate(&self) -> Result<(), NamespacePoliciesError> {
        if self.namespaces.is_empty() {
            return Err(NamespacePoliciesError::Empty);
        }
        let core = NamespaceId::core();
        if !self.namespaces.contains_key(&core) {
            return Err(NamespacePoliciesError::MissingNamespace { namespace: core });
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum NamespacePoliciesError {
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    #[error("no namespaces configured")]
    Empty,
    #[error("missing namespace policy for {namespace}")]
    MissingNamespace { namespace: NamespaceId },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use crate::{ReplicateMode, RetentionPolicy};

    #[test]
    fn test_roundtrip_default() {
        let mut namespaces = BTreeMap::new();
        namespaces.insert(NamespaceId::core(), NamespacePolicy::core_default());
        let original = NamespacePolicies { namespaces };

        let toml_str = toml::to_string(&original).expect("serialize");
        let parsed = NamespacePolicies::from_toml_str(&toml_str).expect("deserialize");

        assert_eq!(original, parsed);
    }

    #[test]
    fn test_from_toml_str_complex() {
        let toml_input = r#"
            [namespaces.core]
            persist_to_git = true
            replicate_mode = "Peers"
            retention = "Forever"
            ready_eligible = true
            visibility = "Normal"
            gc_authority = "CheckpointWriter"
            ttl_basis = "LastMutationStamp"

            [namespaces.temp]
            persist_to_git = false
            replicate_mode = "None"
            retention = { Ttl = { ttl_ms = 3600000 } }
            ready_eligible = false
            visibility = "Normal"
            gc_authority = "None"
            ttl_basis = "EventTime"
        "#;

        let policies = NamespacePolicies::from_toml_str(toml_input).expect("parse complex toml");
        assert_eq!(policies.namespaces.len(), 2);

        let core = policies.namespaces.get(&NamespaceId::core()).unwrap();
        assert_eq!(core.replicate_mode, ReplicateMode::Peers);

        let temp_id = NamespaceId::parse("temp").unwrap();
        let temp = policies.namespaces.get(&temp_id).unwrap();
        assert_eq!(temp.replicate_mode, ReplicateMode::None);
        match temp.retention {
            RetentionPolicy::Ttl { ttl_ms } => assert_eq!(ttl_ms, 3600000),
            _ => panic!("expected Ttl retention"),
        }
    }

    #[test]
    fn test_validate_missing_core() {
        let toml_input = r#"
            [namespaces.other]
            persist_to_git = true
            replicate_mode = "Peers"
            retention = "Forever"
            ready_eligible = true
            visibility = "Normal"
            gc_authority = "CheckpointWriter"
            ttl_basis = "LastMutationStamp"
        "#;

        let res = NamespacePolicies::from_toml_str(toml_input);
        match res {
            Err(NamespacePoliciesError::MissingNamespace { namespace }) => {
                assert!(namespace.is_core());
            }
            _ => panic!("expected MissingNamespace error, got {:?}", res),
        }
    }

    #[test]
    fn test_validate_empty() {
        // "namespaces = {}" parses to empty BTreeMap, which fails validate()
        let toml_input = "namespaces = {}";
        let res = NamespacePolicies::from_toml_str(toml_input);
        match res {
            Err(NamespacePoliciesError::Empty) => {} // OK
            _ => panic!("expected Empty error, got {:?}", res),
        }
    }

    #[test]
    fn test_empty_string_is_toml_error() {
        // Empty string is missing `namespaces` field
        let toml_input = "";
        let res = NamespacePolicies::from_toml_str(toml_input);
        match res {
            Err(NamespacePoliciesError::Toml(_)) => {} // OK
            _ => panic!("expected Toml error, got {:?}", res),
        }
    }

    #[test]
    fn test_invalid_toml() {
        let toml_input = "invalid syntax =";
        let res = NamespacePolicies::from_toml_str(toml_input);
        match res {
            Err(NamespacePoliciesError::Toml(_)) => {} // OK
            _ => panic!("expected Toml error, got {:?}", res),
        }
    }
}
