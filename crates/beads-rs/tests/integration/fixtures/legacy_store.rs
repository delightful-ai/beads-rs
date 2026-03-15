use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use beads_core::CanonicalState;
use git2::{FetchOptions, ObjectType, Oid, Repository, Signature, Time};

use super::timing;
use super::wait;

const STORE_REF: &str = "refs/heads/beads/store";
const REMOTE_STORE_REF: &str = "refs/remotes/origin/beads/store";

pub struct LegacyStoreFixture {
    name: &'static str,
}

impl LegacyStoreFixture {
    pub fn new(name: &'static str) -> Self {
        Self { name }
    }

    pub fn install(&self, repo_path: &Path) -> Oid {
        self.install_with_parent(repo_path, None)
    }

    pub fn install_with_parent(&self, repo_path: &Path, parent_oid: Option<Oid>) -> Oid {
        let repo = Repository::open(repo_path).unwrap_or_else(|err| {
            panic!(
                "open repo failed for fixture {} at {:?}: {err}",
                self.name, repo_path
            )
        });
        write_store_ref(&repo, self, parent_oid)
    }

    fn read_blob(&self, file_name: &str) -> Option<Vec<u8>> {
        let path = fixture_dir(self.name).join(file_name);
        if !path.exists() {
            return None;
        }
        Some(
            fs::read(&path)
                .unwrap_or_else(|err| panic!("read fixture blob failed for {:?}: {err}", path)),
        )
    }
}

pub fn fixture(name: &'static str) -> LegacyStoreFixture {
    LegacyStoreFixture::new(name)
}

pub fn fetch_remote_store_ref(repo_path: &Path) -> Oid {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let mut remote = repo.find_remote("origin").expect("origin remote");
    let mut options = FetchOptions::new();
    remote
        .fetch(
            &["refs/heads/beads/store:refs/remotes/origin/beads/store"],
            Some(&mut options),
            None,
        )
        .expect("fetch remote tracking ref");
    repo.refname_to_id(REMOTE_STORE_REF)
        .expect("remote tracking store ref")
}

pub fn wait_for_fetched_remote_store_ref(repo_path: &Path, expected: Oid, timeout: Duration) {
    assert!(
        wait::poll_until_with_phase(
            "fixture.legacy_store.wait_for_fetched_remote_store_ref",
            format!("repo={} expected={expected}", repo_path.display()),
            timeout,
            || fetch_remote_store_ref(repo_path) == expected
        ),
        "remote tracking store ref under {} did not reach {expected} within {timeout:?}",
        repo_path.display()
    );
}

pub fn write_store_commit_bytes(
    repo_path: &Path,
    state_bytes: &[u8],
    tombstones_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: Option<&[u8]>,
    meta_bytes: Option<&[u8]>,
    message: &str,
) -> Oid {
    let _phase = timing::scoped_phase_with_context(
        "fixture.legacy_store.write_store_commit_bytes",
        format!("repo={} message={message}", repo_path.display()),
    );
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    write_store_commit_bytes_in_repo(
        &repo,
        state_bytes,
        tombstones_bytes,
        deps_bytes,
        notes_bytes,
        meta_bytes,
        message,
    )
}

pub fn read_store_state(repo_path: &Path) -> CanonicalState {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let oid = repo.refname_to_id(STORE_REF).expect("store ref");
    beads_git::read_state_at_oid(&repo, oid)
        .expect("strict store load")
        .state
}

pub fn store_ref_oid(repo_path: &Path, refname: &str) -> Option<Oid> {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    repo.refname_to_id(refname).ok()
}

pub fn backup_ref_oid(repo_path: &Path, oid: Oid) -> Option<Oid> {
    store_ref_oid(repo_path, &format!("refs/beads/backup/{oid}"))
}

pub fn set_ref_target(repo_path: &Path, refname: &str, oid: Oid, message: &str) {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    repo.reference(refname, oid, true, message)
        .expect("update ref");
}

pub fn backup_ref_targets(repo_path: &Path) -> BTreeSet<Oid> {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let refs = repo
        .references_glob("refs/beads/backup/*")
        .expect("backup refs");
    refs.filter_map(|reference| {
        let reference = reference.expect("backup reference");
        reference.target()
    })
    .collect()
}

pub fn create_detached_store_commit(repo_path: &Path, time_secs: i64, message: &str) -> Oid {
    let _phase = timing::scoped_phase_with_context(
        "fixture.legacy_store.create_detached_store_commit",
        format!("repo={} message={message}", repo_path.display()),
    );
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let store_oid = repo
        .refname_to_id(STORE_REF)
        .expect("store ref for detached commit");
    let tree = repo
        .find_commit(store_oid)
        .expect("store commit")
        .tree()
        .expect("store tree");
    let sig =
        Signature::new("test", "test@example.com", &Time::new(time_secs, 0)).expect("signature");
    repo.commit(None, &sig, &sig, message, &tree, &[])
        .expect("detached commit")
}

pub fn rewrite_store_ref_commit(
    repo_path: &Path,
    refname: &str,
    time_secs: i64,
    message: &str,
) -> Oid {
    let _phase = timing::scoped_phase_with_context(
        "fixture.legacy_store.rewrite_store_ref_commit",
        format!(
            "repo={} ref={refname} message={message}",
            repo_path.display()
        ),
    );
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let store_oid = repo.refname_to_id(refname).expect("store ref");
    let commit = repo.find_commit(store_oid).expect("store commit");
    let tree = commit.tree().expect("store tree");
    let mut parents = Vec::new();
    for idx in 0..commit.parent_count() {
        parents.push(commit.parent(idx).expect("parent commit"));
    }
    let parent_refs: Vec<_> = parents.iter().collect();
    let sig =
        Signature::new("test", "test@example.com", &Time::new(time_secs, 0)).expect("signature");
    let rewritten_oid = repo
        .commit(None, &sig, &sig, message, &tree, &parent_refs)
        .expect("rewrite store commit");
    repo.reference(refname, rewritten_oid, true, "rewrite store ref")
        .expect("rewrite store ref");
    rewritten_oid
}

pub fn create_backup_ref(repo_path: &Path, oid: Oid) {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    repo.reference(
        &format!("refs/beads/backup/{oid}"),
        oid,
        true,
        "test seed backup ref",
    )
    .expect("seed backup ref");
}

pub fn store_first_parent_oid(repo_path: &Path, refname: &str) -> Option<Oid> {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let oid = repo.refname_to_id(refname).ok()?;
    let commit = repo.find_commit(oid).expect("store commit");
    commit.parent_id(0).ok()
}

pub fn read_store_meta_json(repo_path: &Path) -> serde_json::Value {
    let meta_bytes = read_store_blob(repo_path, "meta.json").expect("meta.json");
    serde_json::from_slice(&meta_bytes).expect("meta json")
}

pub fn rewrite_store_meta(repo_path: &Path, meta_bytes: Option<&[u8]>) {
    let _phase = timing::scoped_phase_with_context(
        "fixture.legacy_store.rewrite_store_meta",
        repo_path.display(),
    );
    let state_bytes = read_store_blob(repo_path, "state.jsonl").expect("state.jsonl");
    let tombs_bytes = read_store_blob(repo_path, "tombstones.jsonl").expect("tombstones.jsonl");
    let deps_bytes = read_store_blob(repo_path, "deps.jsonl").expect("deps.jsonl");
    let notes_bytes = read_store_blob(repo_path, "notes.jsonl");
    write_store_commit_bytes(
        repo_path,
        &state_bytes,
        &tombs_bytes,
        &deps_bytes,
        notes_bytes.as_deref(),
        meta_bytes,
        "test store commit",
    );
}

pub fn push_store_ref(repo_path: &Path) {
    let _phase = timing::scoped_phase_with_context(
        "fixture.legacy_store.push_store_ref",
        repo_path.display(),
    );
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let mut remote = repo.find_remote("origin").expect("origin remote");
    remote
        .push(&["refs/heads/beads/store:refs/heads/beads/store"], None)
        .expect("push store ref");
}

fn write_store_ref(
    repo: &Repository,
    fixture: &LegacyStoreFixture,
    parent_oid: Option<Oid>,
) -> Oid {
    let state_bytes = fixture
        .read_blob("state.jsonl")
        .expect("fixture must include state.jsonl");
    let tombstones_bytes = fixture
        .read_blob("tombstones.jsonl")
        .expect("fixture must include tombstones.jsonl");
    let deps_bytes = fixture
        .read_blob("deps.jsonl")
        .expect("fixture must include deps.jsonl");
    let notes_bytes = fixture.read_blob("notes.jsonl");
    let meta_bytes = fixture.read_blob("meta.json");

    let state_oid = repo.blob(&state_bytes).expect("state blob");
    let tombstones_oid = repo.blob(&tombstones_bytes).expect("tombstones blob");
    let deps_oid = repo.blob(&deps_bytes).expect("deps blob");
    let notes_oid = notes_bytes
        .as_deref()
        .map(|bytes| repo.blob(bytes).expect("notes blob"));
    let meta_oid = meta_bytes
        .as_deref()
        .map(|bytes| repo.blob(bytes).expect("meta blob"));

    let mut builder = repo.treebuilder(None).expect("treebuilder");
    builder
        .insert("state.jsonl", state_oid, 0o100644)
        .expect("insert state.jsonl");
    builder
        .insert("tombstones.jsonl", tombstones_oid, 0o100644)
        .expect("insert tombstones.jsonl");
    builder
        .insert("deps.jsonl", deps_oid, 0o100644)
        .expect("insert deps.jsonl");
    if let Some(notes_oid) = notes_oid {
        builder
            .insert("notes.jsonl", notes_oid, 0o100644)
            .expect("insert notes.jsonl");
    }
    if let Some(meta_oid) = meta_oid {
        builder
            .insert("meta.json", meta_oid, 0o100644)
            .expect("insert meta.json");
    }

    let tree_oid = builder.write().expect("write tree");
    let tree = repo.find_tree(tree_oid).expect("find tree");
    let sig = Signature::now("fixture", "fixture@example.com").expect("signature");
    let parent = parent_oid
        .and_then(|oid| repo.find_commit(oid).ok())
        .or_else(|| {
            repo.refname_to_id(STORE_REF)
                .ok()
                .and_then(|oid| repo.find_commit(oid).ok())
        });
    let parent_refs: Vec<_> = parent.iter().collect();
    let commit_oid = repo
        .commit(None, &sig, &sig, fixture.name, &tree, &parent_refs)
        .expect("commit fixture store");
    repo.reference(STORE_REF, commit_oid, true, "install fixture store ref")
        .expect("update store ref");
    commit_oid
}

fn write_store_commit_bytes_in_repo(
    repo: &Repository,
    state_bytes: &[u8],
    tombstones_bytes: &[u8],
    deps_bytes: &[u8],
    notes_bytes: Option<&[u8]>,
    meta_bytes: Option<&[u8]>,
    message: &str,
) -> Oid {
    let state_oid = repo.blob(state_bytes).expect("state blob");
    let tombstones_oid = repo.blob(tombstones_bytes).expect("tombstones blob");
    let deps_oid = repo.blob(deps_bytes).expect("deps blob");
    let notes_oid = notes_bytes.map(|bytes| repo.blob(bytes).expect("notes blob"));
    let meta_oid = meta_bytes.map(|bytes| repo.blob(bytes).expect("meta blob"));

    let mut builder = repo.treebuilder(None).expect("treebuilder");
    builder
        .insert("state.jsonl", state_oid, 0o100644)
        .expect("state insert");
    builder
        .insert("tombstones.jsonl", tombstones_oid, 0o100644)
        .expect("tombs insert");
    builder
        .insert("deps.jsonl", deps_oid, 0o100644)
        .expect("deps insert");
    if let Some(notes_oid) = notes_oid {
        builder
            .insert("notes.jsonl", notes_oid, 0o100644)
            .expect("notes insert");
    }
    if let Some(meta_oid) = meta_oid {
        builder
            .insert("meta.json", meta_oid, 0o100644)
            .expect("meta insert");
    }

    let tree_oid = builder.write().expect("tree write");
    let tree = repo.find_tree(tree_oid).expect("find tree");
    let sig = Signature::now("test", "test@example.com").expect("signature");
    let parent = repo
        .refname_to_id(STORE_REF)
        .ok()
        .and_then(|oid| repo.find_commit(oid).ok());
    let parent_refs: Vec<_> = parent.iter().collect();
    let commit_oid = repo
        .commit(None, &sig, &sig, message, &tree, &parent_refs)
        .expect("commit");
    repo.reference(STORE_REF, commit_oid, true, "test update store ref")
        .expect("update store ref");
    commit_oid
}

pub fn read_store_blob(repo_path: &Path, name: &str) -> Option<Vec<u8>> {
    let repo = Repository::open(repo_path)
        .unwrap_or_else(|err| panic!("open repo failed for {:?}: {err}", repo_path));
    let oid = repo.refname_to_id(STORE_REF).expect("store ref");
    let commit = repo.find_commit(oid).expect("store commit");
    let tree = commit.tree().expect("store tree");
    let entry = tree.get_name(name)?;
    let blob = repo
        .find_object(entry.id(), Some(ObjectType::Blob))
        .expect("blob object")
        .peel_to_blob()
        .expect("blob");
    Some(blob.content().to_vec())
}

fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("fixtures")
        .join("legacy_store_corpus")
        .join(name)
}
