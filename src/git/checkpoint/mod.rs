//! Checkpoint manifest and metadata types.

pub mod export;
pub mod import;
pub mod json_canon;
pub mod layout;
pub mod manifest;
pub mod meta;
pub mod types;

pub const CHECKPOINT_FORMAT_VERSION: u32 = 1;

pub use export::{
    CheckpointExport, CheckpointExportError, CheckpointExportInput, CheckpointSnapshotError,
    CheckpointSnapshotInput, build_snapshot, export_checkpoint, policy_hash,
};
pub use import::{CheckpointImport, CheckpointImportError, import_checkpoint, merge_store_states};
pub use layout::{
    CheckpointFileKind, CheckpointShardPath, DEPS_DIR, MANIFEST_FILE, META_FILE, NAMESPACES_DIR,
    SHARD_COUNT, STATE_DIR, TOMBSTONES_DIR, parse_shard_path, shard_for_bead, shard_for_dep,
    shard_for_tombstone, shard_name, shard_path,
};
pub use manifest::{CheckpointManifest, ManifestFile};
pub use meta::{CheckpointMeta, CheckpointMetaPreimage, IncludedHeads, IncludedWatermarks};
pub use types::{CheckpointShardPayload, CheckpointSnapshot};
