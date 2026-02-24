use std::path::PathBuf;

use beads_core::StoreId;
use beads_daemon::config::{CheckpointPolicy, DaemonRuntimeConfig, GitSyncPolicy};
use beads_daemon::layout::DaemonLayout;
use uuid::Uuid;

#[test]
fn layout_derives_store_and_wal_paths() {
    let data_dir = PathBuf::from("/tmp/beads-data");
    let socket_path = PathBuf::from("/tmp/beads-runtime/beads/daemon.sock");
    let log_dir = PathBuf::from("/tmp/beads-log");
    let layout = DaemonLayout::new(data_dir.clone(), socket_path.clone(), log_dir.clone());

    let store_id = StoreId::new(Uuid::from_bytes([3u8; 16]));

    assert_eq!(layout.data_dir, data_dir);
    assert_eq!(layout.socket_path, socket_path);
    assert_eq!(
        layout.store_dir(&store_id),
        PathBuf::from("/tmp/beads-data/stores/03030303-0303-0303-0303-030303030303")
    );
    assert_eq!(
        layout.wal_dir(&store_id),
        PathBuf::from("/tmp/beads-data/stores/03030303-0303-0303-0303-030303030303/wal")
    );
    assert_eq!(layout.log_dir(), log_dir);
}

#[test]
fn runtime_config_defaults_enable_policies() {
    let config = DaemonRuntimeConfig::default();
    assert!(matches!(config.git_sync_policy, GitSyncPolicy::Enabled));
    assert!(matches!(
        config.checkpoint_policy,
        CheckpointPolicy::Enabled
    ));
}
