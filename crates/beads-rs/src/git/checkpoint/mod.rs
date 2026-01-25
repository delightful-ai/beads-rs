//! Checkpoint manifest and metadata types.

pub mod cache;
pub mod export;
pub mod import;
pub mod json_canon;
pub mod layout;
pub mod manifest;
pub mod meta;
pub mod publish;
pub mod types;

pub const CHECKPOINT_FORMAT_VERSION: u32 =
    crate::core::StoreMetaVersions::CHECKPOINT_FORMAT_VERSION;

pub use cache::{
    CheckpointCache, CheckpointCacheEntry, CheckpointCacheError, DEFAULT_CHECKPOINT_CACHE_KEEP,
};
pub use export::{
    CheckpointExport, CheckpointExportError, CheckpointExportInput, CheckpointSnapshotError,
    CheckpointSnapshotFromStateInput, CheckpointSnapshotInput, build_snapshot,
    build_snapshot_from_state, export_checkpoint, policy_hash, roster_hash,
};
pub use import::{
    CheckpointImport, CheckpointImportError, import_checkpoint, import_checkpoint_export,
    merge_store_states, store_state_from_legacy,
};
pub use layout::{
    CheckpointFileKind, CheckpointShardPath, DEPS_DIR, MANIFEST_FILE, META_FILE, NAMESPACES_DIR,
    SHARD_COUNT, STATE_DIR, TOMBSTONES_DIR, parse_shard_path, shard_for_bead, shard_for_dep,
    shard_for_tombstone, shard_name, shard_path,
};
pub use manifest::{CheckpointManifest, ManifestFile};
pub use meta::{CheckpointMeta, CheckpointMetaPreimage, IncludedHeads, IncludedWatermarks};
pub use publish::{
    CheckpointPublishError, CheckpointPublishOutcome, CheckpointStoreMeta, STORE_META_REF,
    publish_checkpoint, publish_checkpoint_with_retry,
};
pub use types::{CheckpointShardPayload, CheckpointSnapshot};
