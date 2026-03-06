use std::fs;
use std::path::{Path, PathBuf};

pub struct StoreFixture {
    pub state: Vec<u8>,
    pub tombstones: Vec<u8>,
    pub deps: Vec<u8>,
    pub notes: Option<Vec<u8>>,
    pub meta: Option<Vec<u8>>,
}

pub fn legacy_v0_1_26_minimal() -> StoreFixture {
    load_store_fixture("v0_1_26_minimal")
}

fn load_store_fixture(name: &str) -> StoreFixture {
    let root = fixture_root().join(name);
    StoreFixture {
        state: read_required(&root, "state.jsonl"),
        tombstones: read_required(&root, "tombstones.jsonl"),
        deps: read_required(&root, "deps.jsonl"),
        notes: read_optional(&root, "notes.jsonl"),
        meta: read_optional(&root, "meta.json"),
    }
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixture-data")
        .join("migration")
}

fn read_required(root: &Path, name: &str) -> Vec<u8> {
    let path = root.join(name);
    fs::read(&path).unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

fn read_optional(root: &Path, name: &str) -> Option<Vec<u8>> {
    let path = root.join(name);
    if !path.exists() {
        return None;
    }
    Some(fs::read(&path).unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display())))
}
