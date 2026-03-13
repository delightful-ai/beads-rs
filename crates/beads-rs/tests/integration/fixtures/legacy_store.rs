use std::fs;
use std::path::{Path, PathBuf};

use git2::{FetchOptions, Oid, Repository, Signature};

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

fn fixture_dir(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("fixtures")
        .join("legacy_store_corpus")
        .join(name)
}
