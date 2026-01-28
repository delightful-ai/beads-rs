//! Checkpoint manifest definitions.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::json_canon::{CanonJsonError, to_canon_json_bytes};
use crate::core::{ContentHash, NamespaceId, StoreEpoch, StoreId, sha256_bytes};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedCheckpointManifest {
    manifest: CheckpointManifest,
}

impl ParsedCheckpointManifest {
    pub(crate) fn new(manifest: CheckpointManifest) -> Self {
        Self { manifest }
    }

    pub fn manifest(&self) -> &CheckpointManifest {
        &self.manifest
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestFile {
    pub sha256: ContentHash,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointManifest {
    pub checkpoint_group: String,
    pub store_id: StoreId,
    pub store_epoch: StoreEpoch,
    pub namespaces: Vec<NamespaceId>,
    pub files: BTreeMap<String, ManifestFile>,
}

impl CheckpointManifest {
    pub fn canon_bytes(&self) -> Result<Vec<u8>, CanonJsonError> {
        to_canon_json_bytes(&self.normalized())
    }

    pub fn manifest_hash(&self) -> Result<ContentHash, CanonJsonError> {
        let bytes = self.canon_bytes()?;
        Ok(ContentHash::from_bytes(sha256_bytes(&bytes).0))
    }

    fn normalized(&self) -> Self {
        let mut cloned = self.clone();
        cloned.namespaces.sort();
        cloned.namespaces.dedup();
        cloned
    }

    pub(crate) fn namespaces_normalized(&self) -> Vec<NamespaceId> {
        let mut namespaces = self.namespaces.clone();
        namespaces.sort();
        namespaces.dedup();
        namespaces
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn manifest_hash_matches_canonical_bytes() {
        let store_id = StoreId::new(Uuid::from_u128(1));
        let mut files = BTreeMap::new();
        files.insert(
            "namespaces/core/state/00.jsonl".to_string(),
            ManifestFile {
                sha256: ContentHash::from_bytes([2u8; 32]),
                bytes: 12,
            },
        );

        let manifest = CheckpointManifest {
            checkpoint_group: "core".to_string(),
            store_id,
            store_epoch: StoreEpoch::new(0),
            namespaces: vec![NamespaceId::core()],
            files,
        };

        let bytes = manifest.canon_bytes().expect("manifest canon bytes");
        let expected = br#"{"checkpoint_group":"core","files":{"namespaces/core/state/00.jsonl":{"bytes":12,"sha256":"0202020202020202020202020202020202020202020202020202020202020202"}},"namespaces":["core"],"store_epoch":0,"store_id":"00000000-0000-0000-0000-000000000001"}"#;
        assert_eq!(bytes, expected);

        let expected_hash = ContentHash::from_bytes(sha256_bytes(expected).0);
        let computed = manifest.manifest_hash().expect("manifest hash");
        assert_eq!(computed, expected_hash);
    }
}
