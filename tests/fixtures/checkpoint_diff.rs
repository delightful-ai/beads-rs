#![allow(dead_code)]

use std::collections::BTreeMap;

use bytes::Bytes;

use beads_rs::git::checkpoint::{CheckpointExport, CheckpointManifest, CheckpointMeta, CheckpointShardPayload};
use beads_rs::{ContentHash, sha256_bytes};

pub fn diff_exports(expected: &CheckpointExport, actual: &CheckpointExport) -> Vec<String> {
    let mut diffs = Vec::new();
    diffs.extend(diff_meta(&expected.meta, &actual.meta));
    diffs.extend(diff_manifest(&expected.manifest, &actual.manifest));
    diffs.extend(diff_files(&expected.files, &actual.files));
    diffs
}

pub fn assert_exports_match(expected: &CheckpointExport, actual: &CheckpointExport) {
    let diffs = diff_exports(expected, actual);
    if !diffs.is_empty() {
        panic!("checkpoint mismatch:\n{}", diffs.join("\n"));
    }
}

fn diff_meta(expected: &CheckpointMeta, actual: &CheckpointMeta) -> Vec<String> {
    if expected == actual {
        return Vec::new();
    }
    vec![format!(
        "meta mismatch: expected {:?}, got {:?}",
        expected, actual
    )]
}

fn diff_manifest(expected: &CheckpointManifest, actual: &CheckpointManifest) -> Vec<String> {
    let mut diffs = Vec::new();
    if expected.checkpoint_group != actual.checkpoint_group {
        diffs.push(format!(
            "manifest checkpoint_group mismatch: expected {}, got {}",
            expected.checkpoint_group, actual.checkpoint_group
        ));
    }
    if expected.store_id != actual.store_id {
        diffs.push(format!(
            "manifest store_id mismatch: expected {}, got {}",
            expected.store_id, actual.store_id
        ));
    }
    if expected.store_epoch != actual.store_epoch {
        diffs.push(format!(
            "manifest store_epoch mismatch: expected {}, got {}",
            expected.store_epoch, actual.store_epoch
        ));
    }

    let mut expected_ns = expected.namespaces.clone();
    expected_ns.sort();
    let mut actual_ns = actual.namespaces.clone();
    actual_ns.sort();
    if expected_ns != actual_ns {
        diffs.push(format!(
            "manifest namespaces mismatch: expected {:?}, got {:?}",
            expected_ns, actual_ns
        ));
    }

    for (path, file) in &expected.files {
        match actual.files.get(path) {
            Some(actual_file) => {
                if file != actual_file {
                    diffs.push(format!(
                        "manifest file mismatch {path}: expected {:?}, got {:?}",
                        file, actual_file
                    ));
                }
            }
            None => diffs.push(format!("manifest missing file {path}")),
        }
    }
    for path in actual.files.keys() {
        if !expected.files.contains_key(path) {
            diffs.push(format!("manifest has unexpected file {path}"));
        }
    }
    diffs
}

fn diff_files(
    expected: &BTreeMap<String, CheckpointShardPayload>,
    actual: &BTreeMap<String, CheckpointShardPayload>,
) -> Vec<String> {
    let mut diffs = Vec::new();
    for (path, payload) in expected {
        match actual.get(path) {
            Some(actual_payload) => {
                let expected_hash = hash_payload(payload);
                let actual_hash = hash_payload(actual_payload);
                if expected_hash != actual_hash || payload.bytes.len() != actual_payload.bytes.len() {
                    diffs.push(format!(
                        "file mismatch {path}: expected {} ({} bytes), got {} ({} bytes)",
                        expected_hash.to_hex(),
                        payload.bytes.len(),
                        actual_hash.to_hex(),
                        actual_payload.bytes.len()
                    ));
                }
            }
            None => diffs.push(format!("missing file {path}")),
        }
    }
    for path in actual.keys() {
        if !expected.contains_key(path) {
            diffs.push(format!("unexpected file {path}"));
        }
    }
    diffs
}

fn hash_payload(payload: &CheckpointShardPayload) -> ContentHash {
    ContentHash::from_bytes(sha256_bytes(payload.bytes.as_ref()).0)
}

pub fn corrupt_payload(payload: &CheckpointShardPayload) -> CheckpointShardPayload {
    let mut bytes = payload.bytes.as_ref().to_vec();
    if bytes.is_empty() {
        bytes.push(0);
    } else {
        bytes[0] ^= 0xFF;
    }
    CheckpointShardPayload {
        path: payload.path.clone(),
        bytes: Bytes::from(bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::fixtures::checkpoint::fixture_small_state;

    #[test]
    fn fixtures_checkpoint_diff_empty_for_equal_exports() {
        let fixture = fixture_small_state();
        let diffs = diff_exports(&fixture.export, &fixture.export);
        assert!(diffs.is_empty());
    }

    #[test]
    fn fixtures_checkpoint_diff_detects_payload_change() {
        let fixture = fixture_small_state();
        let mut mutated = fixture.export.clone();
        let path = mutated.files.keys().next().expect("file path").clone();
        let payload = mutated.files.get(&path).expect("payload").clone();
        mutated
            .files
            .insert(path.clone(), corrupt_payload(&payload));
        let diffs = diff_exports(&fixture.export, &mutated);
        assert!(diffs.iter().any(|diff| diff.contains(&path)));
    }
}
