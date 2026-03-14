use std::fs;
use std::path::{Path, PathBuf};

#[test]
fn daemon_runtime_paths_are_not_imported_outside_daemon_crate() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonical repo root");
    let crates_root = repo_root.join("crates");
    let daemon_root = crates_root.join("beads-daemon");

    let mut matches = Vec::new();
    collect_forbidden_daemon_imports(&crates_root, &daemon_root, &mut matches);

    assert!(
        matches.is_empty(),
        "found forbidden daemon internal imports outside beads-daemon:\n{}",
        matches.join("\n")
    );
}

#[test]
fn upgrade_support_is_not_exported_from_cli_or_public_beads_rs_surface() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonical repo root");
    let beads_cli_lib = repo_root.join("crates/beads-cli/src/lib.rs");
    let beads_rs_lib = repo_root.join("crates/beads-rs/src/lib.rs");

    let beads_cli_contents = fs::read_to_string(&beads_cli_lib)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", beads_cli_lib.display()));
    let beads_rs_contents = fs::read_to_string(&beads_rs_lib)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", beads_rs_lib.display()));

    assert!(
        !beads_cli_contents.contains("pub mod upgrade;"),
        "beads-cli should not publicly export upgrade support"
    );
    assert!(
        !beads_rs_contents.contains("pub mod upgrade;"),
        "beads-rs should not publicly export upgrade internals"
    );
}

#[test]
fn remaining_daemon_facing_tests_are_documented_as_assembly_owned() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonical repo root");
    let daemon_mod = repo_root.join("crates/beads-rs/tests/integration/daemon/mod.rs");
    let fixtures_mod = repo_root.join("crates/beads-rs/tests/integration/fixtures/mod.rs");

    let daemon_contents = fs::read_to_string(&daemon_mod)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", daemon_mod.display()));
    let fixtures_contents = fs::read_to_string(&fixtures_mod)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", fixtures_mod.display()));

    assert!(
        daemon_contents.contains("Assembly-owned daemon/product integration coverage."),
        "daemon integration module should explain why these tests stay in beads-rs"
    );
    assert!(
        fixtures_contents.contains("Assembly-owned integration fixtures."),
        "fixtures module should explain why these helpers stay in beads-rs"
    );
}

#[test]
fn mixed_owner_core_tests_do_not_live_under_beads_rs_integration_core() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonical repo root");
    let core_mod = repo_root.join("crates/beads-rs/tests/integration/core/mod.rs");
    if !core_mod.exists() {
        return;
    }

    let core_contents = fs::read_to_string(&core_mod)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", core_mod.display()));

    assert!(
        !core_contents.contains("mod deps;"),
        "mixed-owner deps coverage should not live under beads-rs integration/core"
    );
    assert!(
        !core_contents.contains("mod identity;"),
        "mixed-owner identity coverage should not live under beads-rs integration/core"
    );
}

fn collect_forbidden_daemon_imports(root: &Path, daemon_root: &Path, matches: &mut Vec<String>) {
    let runtime_path = ["beads_daemon", "::runtime::"].concat();
    let git_path = ["beads_daemon", "::git::"].concat();
    let entries = fs::read_dir(root).unwrap_or_else(|err| {
        panic!("failed to read {}: {err}", root.display());
    });
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| {
            panic!("failed to read dir entry in {}: {err}", root.display());
        });
        let path = entry.path();
        if path.starts_with(daemon_root) || path.ends_with("target") {
            continue;
        }
        if path.is_dir() {
            collect_forbidden_daemon_imports(&path, daemon_root, matches);
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        if path.ends_with(Path::new("beads-rs/tests/public_boundary.rs")) {
            continue;
        }

        let contents = fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!("failed to read {}: {err}", path.display());
        });
        for (line_idx, line) in contents.lines().enumerate() {
            if line.contains(&runtime_path) || line.contains(&git_path) {
                matches.push(format!(
                    "{}:{}: {}",
                    path.strip_prefix(root.parent().expect("crates root parent"))
                        .unwrap_or(&path)
                        .display(),
                    line_idx + 1,
                    line.trim()
                ));
            }
        }
    }
}
