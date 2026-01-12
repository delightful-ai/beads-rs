//! Phase 1 tests: identity persistence, namespace validation, and lock behavior.

mod fixtures;

use std::fs;
use std::io;
use std::path::Path;

use fixtures::identity::{
    invalid_namespaces, namespace_id, store_id, store_meta, valid_namespaces,
};
use fixtures::store_dir::TempStoreDir;
use uuid::Uuid;

use beads_rs::daemon::remote::normalize_url;
use beads_rs::{NamespaceId, StoreId, StoreMeta};

fn write_store_meta(path: &Path, meta: &StoreMeta) -> io::Result<()> {
    let json = serde_json::to_vec(meta).expect("serialize store meta");
    fs::write(path, json)
}

fn read_store_meta(path: &Path) -> io::Result<StoreMeta> {
    let bytes = fs::read(path)?;
    let meta = serde_json::from_slice(&bytes).expect("parse store meta");
    Ok(meta)
}

fn store_id_from_remote(remote: &str) -> StoreId {
    let normalized = normalize_url(remote);
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, normalized.as_bytes());
    StoreId::new(uuid)
}

#[test]
fn namespace_id_validation_accepts_and_rejects() {
    for name in valid_namespaces() {
        let id = NamespaceId::parse(name).unwrap();
        assert_eq!(id.as_str(), name);
    }

    for name in invalid_namespaces() {
        assert!(NamespaceId::parse(name).is_err(), "{name}");
    }
}

#[test]
fn store_meta_roundtrip_persists_identity() {
    let temp = TempStoreDir::new().expect("temp store dir");
    let meta = store_meta(1, 1_726_000_000_000);
    let path = temp.data_dir().join("store_meta.json");
    write_store_meta(&path, &meta).expect("write store meta");

    let reloaded = read_store_meta(&path).expect("read store meta");
    assert_eq!(meta.store_id(), reloaded.store_id());
    assert_eq!(meta.replica_id, reloaded.replica_id);
}

#[test]
fn store_identity_survives_reopen() {
    let temp = TempStoreDir::new().expect("temp store dir");
    let meta = store_meta(42, 1_726_000_000_001);
    let path = temp.data_dir().join("store_meta.json");
    write_store_meta(&path, &meta).expect("write store meta");

    let reopened = read_store_meta(&path).expect("read store meta");
    assert_eq!(meta.store_id(), reopened.store_id());
    assert_eq!(meta.replica_id, reopened.replica_id);
}

#[test]
fn store_discovery_normalizes_remote_url() {
    let id_ssh = store_id_from_remote("git@github.com:foo/bar.git");
    let id_https = store_id_from_remote("https://github.com/foo/bar.git");
    assert_eq!(id_ssh, id_https);

    let id_other = store_id_from_remote("https://github.com/foo/baz.git");
    assert_ne!(id_ssh, id_other);

    let _id_core = store_id(7);
    let _namespace = namespace_id("core");
}

#[test]
fn lock_file_enforces_exclusive_create() {
    let temp = TempStoreDir::new().expect("temp store dir");
    let lock_path = temp.data_dir().join("store.lock");

    let first = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
        .expect("first lock create succeeds");
    drop(first);

    let err = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
        .expect_err("second lock create fails");
    assert_eq!(err.kind(), io::ErrorKind::AlreadyExists);

    temp.force_release_lock("store.lock")
        .expect("release lock");
}
