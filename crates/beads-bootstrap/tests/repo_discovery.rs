use std::fs;

#[test]
fn discovers_repo_root_from_nested_worktree_path() {
    let temp = tempfile::tempdir().expect("tempdir");
    let repo_root = temp.path().join("repo");
    let nested = repo_root.join("a/b/c");

    git2::Repository::init(&repo_root).expect("init repo");
    fs::create_dir_all(&nested).expect("create nested dir");

    let discovered =
        beads_bootstrap::repo::discover_root(&nested).expect("discover repo root from nested dir");

    assert_eq!(discovered, repo_root);
}
