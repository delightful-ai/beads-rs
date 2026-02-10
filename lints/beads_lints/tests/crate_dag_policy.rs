use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;

const POLICY_CRATES: &[&str] = &[
    "beads-core",
    "beads-api",
    "beads-surface",
    "beads-cli",
    "beads-daemon",
    "beads-daemon-core",
    "beads-rs",
];

const ALLOWED_EDGES: &[&str] = &[
    "beads-api -> beads-core",
    "beads-surface -> beads-core",
    "beads-surface -> beads-api",
    "beads-cli -> beads-surface",
    "beads-cli -> beads-core",
    "beads-cli -> beads-api",
    "beads-daemon -> beads-surface",
    "beads-daemon -> beads-api",
    "beads-daemon -> beads-core",
    "beads-daemon -> beads-daemon-core",
    "beads-daemon-core -> beads-core",
    "beads-rs -> beads-core",
    "beads-rs -> beads-api",
    "beads-rs -> beads-surface",
    "beads-rs -> beads-cli",
    "beads-rs -> beads-daemon",
    "beads-rs -> beads-daemon-core",
];

#[test]
fn crate_dag_matches_policy() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("canonical repo root");
    let manifest_path = repo_root.join("Cargo.toml");

    let output = Command::new("cargo")
        .current_dir(&repo_root)
        .arg("metadata")
        .arg("--format-version")
        .arg("1")
        .arg("--no-deps")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata failed for {}:\nstdout:\n{}\nstderr:\n{}",
        manifest_path.display(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse cargo metadata json");
    let packages = metadata["packages"]
        .as_array()
        .expect("metadata.packages must be an array");

    let policy_crates: BTreeSet<&str> = POLICY_CRATES.iter().copied().collect();
    let expected_edges: BTreeSet<String> = ALLOWED_EDGES.iter().map(|edge| (*edge).to_owned()).collect();

    let mut actual_crates = BTreeSet::new();
    let mut actual_edges = BTreeSet::new();

    for package in packages {
        if !package["source"].is_null() {
            continue;
        }
        let Some(package_name) = package["name"].as_str() else {
            continue;
        };
        if !policy_crates.contains(package_name) {
            continue;
        }
        actual_crates.insert(package_name.to_owned());

        let Some(dependencies) = package["dependencies"].as_array() else {
            continue;
        };
        for dependency in dependencies {
            let Some(dep_name) = dependency["name"].as_str() else {
                continue;
            };
            if !policy_crates.contains(dep_name) {
                continue;
            }
            let dep_kind = dependency["kind"].as_str();
            if dep_kind.is_none() || dep_kind == Some("build") {
                actual_edges.insert(format!("{package_name} -> {dep_name}"));
            }
        }
    }

    let expected_crates: BTreeSet<String> = POLICY_CRATES.iter().map(|name| (*name).to_owned()).collect();
    let missing_crates: Vec<String> = expected_crates.difference(&actual_crates).cloned().collect();
    let missing_edges: Vec<String> = expected_edges.difference(&actual_edges).cloned().collect();
    let extra_edges: Vec<String> = actual_edges.difference(&expected_edges).cloned().collect();

    assert!(
        missing_crates.is_empty() && missing_edges.is_empty() && extra_edges.is_empty(),
        "crate DAG policy mismatch (docs/CRATE_DAG.md)\nmissing crates: {}\nmissing edges: {}\nforbidden extra edges: {}",
        if missing_crates.is_empty() {
            "(none)".to_owned()
        } else {
            missing_crates.join(", ")
        },
        if missing_edges.is_empty() {
            "(none)".to_owned()
        } else {
            missing_edges.join(", ")
        },
        if extra_edges.is_empty() {
            "(none)".to_owned()
        } else {
            extra_edges.join(", ")
        },
    );
}
