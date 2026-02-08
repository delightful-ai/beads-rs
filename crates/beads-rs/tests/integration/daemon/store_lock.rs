//! Store paths + lock behavior.

use std::fs;

use crate::fixtures::store_dir::TempStoreDir;
use uuid::Uuid;

use beads_rs::daemon::{StoreLock, StoreLockError, read_lock_meta};
use beads_rs::{NamespaceId, ReplicaId, StoreId};

#[test]
fn store_paths_live_under_bd_data_dir() {
    let temp = TempStoreDir::new().expect("temp store dir");
    let store_id = StoreId::new(Uuid::from_bytes([9u8; 16]));
    let namespace = NamespaceId::parse("core").expect("namespace");

    let store_root = temp.data_dir().join("stores").join(store_id.to_string());

    assert_eq!(
        beads_rs::paths::stores_dir(),
        temp.data_dir().join("stores")
    );
    assert_eq!(beads_rs::paths::store_dir(store_id), store_root);
    assert_eq!(
        beads_rs::paths::store_meta_path(store_id),
        store_root.join("meta.json")
    );
    assert_eq!(
        beads_rs::paths::store_lock_path(store_id),
        store_root.join("store.lock")
    );
    assert_eq!(
        beads_rs::paths::namespaces_path(store_id),
        store_root.join("namespaces.toml")
    );
    assert_eq!(
        beads_rs::paths::replicas_path(store_id),
        store_root.join("replicas.toml")
    );
    assert_eq!(beads_rs::paths::wal_dir(store_id), store_root.join("wal"));
    assert_eq!(
        beads_rs::paths::wal_namespace_dir(store_id, &namespace),
        store_root.join("wal").join("core")
    );
    assert_eq!(
        beads_rs::paths::wal_index_path(store_id),
        store_root.join("index").join("wal.sqlite")
    );
    assert_eq!(
        beads_rs::paths::checkpoint_cache_dir(store_id),
        store_root.join("checkpoint_cache")
    );
}

#[test]
fn store_lock_acquire_writes_metadata() {
    let _temp = TempStoreDir::new().expect("temp store dir");
    let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
    let replica_id = ReplicaId::new(Uuid::from_bytes([2u8; 16]));
    let started_at_ms = 1_726_000_000_123;

    let lock = StoreLock::acquire(store_id, replica_id, started_at_ms, "0.1.0-test")
        .expect("acquire lock");

    let meta = read_lock_meta(store_id)
        .expect("read metadata")
        .expect("metadata exists");
    assert_eq!(meta.store_id, store_id);
    assert_eq!(meta.replica_id, replica_id);
    assert_eq!(meta.pid, std::process::id());
    assert_eq!(meta.started_at_ms, started_at_ms);
    assert_eq!(meta.daemon_version, "0.1.0-test");
    assert_eq!(meta.last_heartbeat_ms, Some(started_at_ms));

    let _ = lock.release();
}

#[test]
fn store_lock_held_surfaces_metadata() {
    let _temp = TempStoreDir::new().expect("temp store dir");
    let store_id = StoreId::new(Uuid::from_bytes([3u8; 16]));
    let replica_id = ReplicaId::new(Uuid::from_bytes([4u8; 16]));
    let started_at_ms = 1_726_000_001_000;

    let _lock = StoreLock::acquire(store_id, replica_id, started_at_ms, "0.1.0-test")
        .expect("acquire lock");

    let err = StoreLock::acquire(store_id, replica_id, started_at_ms, "0.1.0-test").unwrap_err();

    match err {
        StoreLockError::Held { meta, .. } => {
            let meta = meta.expect("metadata present");
            assert_eq!(meta.store_id, store_id);
            assert_eq!(meta.replica_id, replica_id);
        }
        other => panic!("expected held error, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn store_lock_permissions_are_restricted() {
    use std::os::unix::fs::PermissionsExt;

    let _temp = TempStoreDir::new().expect("temp store dir");
    let store_id = StoreId::new(Uuid::from_bytes([5u8; 16]));
    let replica_id = ReplicaId::new(Uuid::from_bytes([6u8; 16]));
    let lock = StoreLock::acquire(store_id, replica_id, 1_726_000_002_000, "0.1.0-test")
        .expect("acquire lock");

    let meta = fs::metadata(lock.path()).expect("lock metadata");
    assert_eq!(meta.permissions().mode() & 0o777, 0o600);

    let store_meta =
        fs::metadata(beads_rs::paths::store_dir(store_id)).expect("store dir metadata");
    assert_eq!(store_meta.permissions().mode() & 0o777, 0o700);

    let _ = lock.release();
}

#[cfg(unix)]
#[test]
fn store_lock_rejects_symlink_path() {
    use std::os::unix::fs::symlink;

    let temp = TempStoreDir::new().expect("temp store dir");
    let store_id = StoreId::new(Uuid::from_bytes([7u8; 16]));
    let replica_id = ReplicaId::new(Uuid::from_bytes([8u8; 16]));
    let store_dir = beads_rs::paths::store_dir(store_id);
    fs::create_dir_all(&store_dir).expect("store dir");

    let target = temp.data_dir().join("target.lock");
    fs::write(&target, b"not a lock").expect("write target");
    let lock_path = beads_rs::paths::store_lock_path(store_id);
    symlink(&target, &lock_path).expect("create symlink");

    let err =
        StoreLock::acquire(store_id, replica_id, 1_726_000_003_000, "0.1.0-test").unwrap_err();
    assert!(matches!(err, StoreLockError::Symlink { .. }));
}
