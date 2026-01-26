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
