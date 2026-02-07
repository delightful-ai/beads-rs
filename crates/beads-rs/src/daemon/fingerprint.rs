//! Admin fingerprint calculation for divergence detection.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::api::{AdminFingerprintKind, AdminFingerprintShard, AdminNamespaceFingerprint};
use crate::core::{ContentHash, NamespaceId, sha256_bytes};
use crate::daemon::store::runtime::StoreRuntime;
use crate::git::checkpoint::CheckpointSnapshotError;
use crate::git::checkpoint::layout::{CheckpointFileKind, SHARD_COUNT, ShardName};

const FINGERPRINT_GROUP: &str = "admin-fingerprint";

#[derive(Clone, Debug)]
pub enum FingerprintMode {
    Full,
    Sample { shard_count: usize, nonce: String },
}

#[derive(Debug, Error)]
pub enum FingerprintError {
    #[error(transparent)]
    Snapshot(#[from] CheckpointSnapshotError),
    #[error("invalid checkpoint shard index for {path}")]
    InvalidShardIndex { path: String },
}

pub fn fingerprint_namespaces(
    store: &StoreRuntime,
    namespaces: &[NamespaceId],
    mode: FingerprintMode,
    now_ms: u64,
) -> Result<Vec<AdminNamespaceFingerprint>, FingerprintError> {
    let snapshot = store.checkpoint_snapshot_readonly(FINGERPRINT_GROUP, namespaces, now_ms)?;
    let empty_hash = ContentHash::from_bytes(sha256_bytes(&[]).0);

    let mut by_namespace = BTreeMap::new();
    for namespace in &snapshot.namespaces {
        by_namespace.insert(namespace.clone(), NamespaceHashes::new(empty_hash));
    }

    for (path, payload) in &snapshot.shards {
        let shard_index =
            shard_index(&path.shard).ok_or_else(|| FingerprintError::InvalidShardIndex {
                path: path.to_path(),
            })?;
        let hash = ContentHash::from_bytes(sha256_bytes(payload.bytes.as_ref()).0);
        let entry = by_namespace
            .entry(path.namespace.clone())
            .or_insert_with(|| NamespaceHashes::new(empty_hash));
        entry.assign(path.kind, shard_index, hash);
    }

    let mut out = Vec::new();
    for namespace in &snapshot.namespaces {
        let hashes = by_namespace
            .get(namespace)
            .expect("namespace fingerprint missing");
        let state_root = type_root(&hashes.state);
        let tombstones_root = type_root(&hashes.tombstones);
        let deps_root = type_root(&hashes.deps);
        let namespace_root = namespace_root(&state_root, &tombstones_root, &deps_root);
        let shards = match &mode {
            FingerprintMode::Full => collect_all_shards(hashes),
            FingerprintMode::Sample { shard_count, nonce } => {
                let indices = sample_indices(namespace, nonce, *shard_count);
                collect_sample_shards(hashes, &indices)
            }
        };
        out.push(AdminNamespaceFingerprint {
            namespace: namespace.clone(),
            state_sha256: state_root,
            tombstones_sha256: tombstones_root,
            deps_sha256: deps_root,
            namespace_root,
            shards,
        });
    }

    Ok(out)
}

#[derive(Clone, Debug)]
struct NamespaceHashes {
    state: Vec<ContentHash>,
    tombstones: Vec<ContentHash>,
    deps: Vec<ContentHash>,
}

impl NamespaceHashes {
    fn new(empty_hash: ContentHash) -> Self {
        Self {
            state: vec![empty_hash; SHARD_COUNT],
            tombstones: vec![empty_hash; SHARD_COUNT],
            deps: vec![empty_hash; SHARD_COUNT],
        }
    }

    fn assign(&mut self, kind: CheckpointFileKind, index: u8, hash: ContentHash) {
        let slot = match kind {
            CheckpointFileKind::State => &mut self.state,
            CheckpointFileKind::Tombstones => &mut self.tombstones,
            CheckpointFileKind::Deps => &mut self.deps,
        };
        slot[index as usize] = hash;
    }
}

fn shard_index(shard: &ShardName) -> Option<u8> {
    let stem = shard.as_str().trim_end_matches(".jsonl");
    if stem.len() != 2 {
        return None;
    }
    u8::from_str_radix(stem, 16).ok()
}

fn type_root(shards: &[ContentHash]) -> ContentHash {
    let mut preimage = Vec::with_capacity(SHARD_COUNT * (1 + 32));
    for (index, hash) in shards.iter().enumerate() {
        preimage.push(index as u8);
        preimage.extend_from_slice(hash.as_bytes());
    }
    ContentHash::from_bytes(sha256_bytes(&preimage).0)
}

fn namespace_root(
    state_root: &ContentHash,
    tombstones_root: &ContentHash,
    deps_root: &ContentHash,
) -> ContentHash {
    let mut preimage = Vec::with_capacity(32 * 3);
    preimage.extend_from_slice(state_root.as_bytes());
    preimage.extend_from_slice(tombstones_root.as_bytes());
    preimage.extend_from_slice(deps_root.as_bytes());
    ContentHash::from_bytes(sha256_bytes(&preimage).0)
}

fn sample_indices(namespace: &NamespaceId, nonce: &str, shard_count: usize) -> Vec<u8> {
    let mut seed = Vec::with_capacity(namespace.as_str().len() + nonce.len() + 1);
    seed.extend_from_slice(namespace.as_str().as_bytes());
    seed.push(0);
    seed.extend_from_slice(nonce.as_bytes());

    let mut scores = Vec::with_capacity(SHARD_COUNT);
    for index in 0..SHARD_COUNT {
        let mut buf = Vec::with_capacity(seed.len() + 1);
        buf.extend_from_slice(&seed);
        buf.push(index as u8);
        let hash = sha256_bytes(&buf);
        scores.push((hash, index as u8));
    }

    scores.sort_by(|(a_hash, a_index), (b_hash, b_index)| {
        a_hash
            .as_bytes()
            .cmp(b_hash.as_bytes())
            .then_with(|| a_index.cmp(b_index))
    });

    let mut indices = scores
        .into_iter()
        .take(shard_count)
        .map(|(_, index)| index)
        .collect::<Vec<_>>();
    indices.sort();
    indices
}

fn collect_all_shards(hashes: &NamespaceHashes) -> Vec<AdminFingerprintShard> {
    let mut out = Vec::with_capacity(SHARD_COUNT * 3);
    push_all(&mut out, AdminFingerprintKind::State, &hashes.state);
    push_all(
        &mut out,
        AdminFingerprintKind::Tombstones,
        &hashes.tombstones,
    );
    push_all(&mut out, AdminFingerprintKind::Deps, &hashes.deps);
    out
}

fn collect_sample_shards(hashes: &NamespaceHashes, indices: &[u8]) -> Vec<AdminFingerprintShard> {
    let mut out = Vec::with_capacity(indices.len() * 3);
    push_sample(
        &mut out,
        AdminFingerprintKind::State,
        &hashes.state,
        indices,
    );
    push_sample(
        &mut out,
        AdminFingerprintKind::Tombstones,
        &hashes.tombstones,
        indices,
    );
    push_sample(&mut out, AdminFingerprintKind::Deps, &hashes.deps, indices);
    out
}

fn push_all(
    out: &mut Vec<AdminFingerprintShard>,
    kind: AdminFingerprintKind,
    hashes: &[ContentHash],
) {
    for (index, hash) in hashes.iter().enumerate() {
        out.push(AdminFingerprintShard {
            kind,
            index: index as u8,
            sha256: *hash,
        });
    }
}

fn push_sample(
    out: &mut Vec<AdminFingerprintShard>,
    kind: AdminFingerprintKind,
    hashes: &[ContentHash],
    indices: &[u8],
) {
    for index in indices {
        out.push(AdminFingerprintShard {
            kind,
            index: *index,
            sha256: hashes[*index as usize],
        });
    }
}
