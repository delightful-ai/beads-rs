//! Checkpoint path layout helpers.

use crate::core::{BeadId, DepKind, NamespaceId, sha256_bytes};

pub const META_FILE: &str = "meta.json";
pub const MANIFEST_FILE: &str = "manifest.json";
pub const NAMESPACES_DIR: &str = "namespaces";
pub const STATE_DIR: &str = "state";
pub const TOMBSTONES_DIR: &str = "tombstones";
pub const DEPS_DIR: &str = "deps";
pub const SHARD_COUNT: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CheckpointFileKind {
    State,
    Tombstones,
    Deps,
}

impl CheckpointFileKind {
    pub fn dir_name(&self) -> &'static str {
        match self {
            CheckpointFileKind::State => STATE_DIR,
            CheckpointFileKind::Tombstones => TOMBSTONES_DIR,
            CheckpointFileKind::Deps => DEPS_DIR,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct CheckpointShardPath {
    pub namespace: NamespaceId,
    pub kind: CheckpointFileKind,
    pub shard: String,
}

impl CheckpointShardPath {
    pub fn new(namespace: NamespaceId, kind: CheckpointFileKind, shard: String) -> Self {
        Self {
            namespace,
            kind,
            shard,
        }
    }

    pub fn to_path(&self) -> String {
        shard_path(&self.namespace, self.kind, &self.shard)
    }
}

pub fn shard_name(byte: u8) -> String {
    format!("{:02x}.jsonl", byte)
}

pub fn shard_for_bead(id: &BeadId) -> String {
    shard_for_key(id.as_str().as_bytes())
}

pub fn shard_for_tombstone(id: &BeadId) -> String {
    shard_for_key(id.as_str().as_bytes())
}

pub fn shard_for_dep(from: &BeadId, to: &BeadId, kind: DepKind) -> String {
    let mut buf =
        Vec::with_capacity(from.as_str().len() + to.as_str().len() + kind.as_str().len() + 2);
    buf.extend_from_slice(from.as_str().as_bytes());
    buf.push(0);
    buf.extend_from_slice(to.as_str().as_bytes());
    buf.push(0);
    buf.extend_from_slice(kind.as_str().as_bytes());
    shard_for_key(&buf)
}

fn shard_for_key(key: &[u8]) -> String {
    let hash = sha256_bytes(key);
    shard_name(hash.as_bytes()[0])
}

pub fn shard_path(namespace: &NamespaceId, kind: CheckpointFileKind, shard: &str) -> String {
    format!(
        "{}/{}/{}/{}",
        NAMESPACES_DIR,
        namespace.as_str(),
        kind.dir_name(),
        shard
    )
}

pub fn parse_shard_path(path: &str) -> Option<CheckpointShardPath> {
    let mut parts = path.split('/');
    if parts.next()? != NAMESPACES_DIR {
        return None;
    }
    let namespace_raw = parts.next()?;
    let namespace = NamespaceId::parse(namespace_raw.to_string()).ok()?;
    let kind_raw = parts.next()?;
    let file = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let kind = match kind_raw {
        STATE_DIR => CheckpointFileKind::State,
        TOMBSTONES_DIR => CheckpointFileKind::Tombstones,
        DEPS_DIR => CheckpointFileKind::Deps,
        _ => return None,
    };
    if !file.ends_with(".jsonl") {
        return None;
    }
    let stem = file.trim_end_matches(".jsonl");
    if stem.len() != 2 || !stem.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(CheckpointShardPath {
        namespace,
        kind,
        shard: file.to_string(),
    })
}
