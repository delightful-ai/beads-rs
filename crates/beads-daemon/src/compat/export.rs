//! JSONL export for Go compatibility.
//!
//! Handles writing issues.jsonl to the data directory and creating
//! symlinks in each clone's .beads/ directory.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::go_schema::{GoIssue, is_bead_blocked};
use crate::core::{BeadProjection, state::CanonicalState};

/// Context for Go-compatible exports.
///
/// Manages the export directory structure:
/// ```text
/// $XDG_DATA_HOME/beads-rs/exports/{remote_hash}/issues.jsonl
/// ```
#[derive(Clone, Debug)]
pub struct ExportContext {
    /// Base directory for all exports (e.g., ~/.local/share/beads-rs/exports)
    exports_dir: PathBuf,
}

impl ExportContext {
    /// Create a new export context.
    ///
    /// Uses `$XDG_DATA_HOME/beads-rs/exports` or `~/.local/share/beads-rs/exports`.
    pub fn new() -> io::Result<Self> {
        let data_dir = crate::paths::data_dir().join("exports");
        fs::create_dir_all(&data_dir)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o700));
        }

        Ok(Self {
            exports_dir: data_dir,
        })
    }

    /// Create an export context with a custom base directory (for testing).
    pub fn with_dir(exports_dir: PathBuf) -> io::Result<Self> {
        fs::create_dir_all(&exports_dir)?;
        Ok(Self { exports_dir })
    }

    /// Get the export path for a given remote URL.
    pub fn export_path(&self, remote_url: &str) -> PathBuf {
        let hash = hash_remote(remote_url);
        self.exports_dir.join(&hash).join("issues.jsonl")
    }

    /// Ensure the directory for a remote exists.
    fn ensure_remote_dir(&self, remote_url: &str) -> io::Result<PathBuf> {
        let hash = hash_remote(remote_url);
        let dir = self.exports_dir.join(&hash);
        fs::create_dir_all(&dir)?;
        Ok(dir)
    }
}

impl Default for ExportContext {
    fn default() -> Self {
        match Self::new() {
            Ok(ctx) => ctx,
            Err(err) => {
                let fallback = std::env::temp_dir().join("beads-rs").join("exports");
                if let Err(e) = fs::create_dir_all(&fallback) {
                    tracing::warn!(
                        "failed to create export context (primary: {}, fallback: {}): {}",
                        err,
                        fallback.display(),
                        e
                    );
                } else {
                    tracing::warn!(
                        "failed to create export context ({}); using fallback {}",
                        err,
                        fallback.display()
                    );
                }
                Self {
                    exports_dir: fallback,
                }
            }
        }
    }
}

/// Export the state to a JSONL file.
///
/// Returns the path to the exported file.
pub fn export_jsonl(
    state: &CanonicalState,
    ctx: &ExportContext,
    remote_url: &str,
) -> io::Result<PathBuf> {
    let dir = ctx.ensure_remote_dir(remote_url)?;
    let final_path = dir.join("issues.jsonl");
    let temp_path = dir.join(".issues.jsonl.tmp");

    // Collect and sort issues by ID for stable output
    let mut issues: Vec<GoIssue> = Vec::new();

    // Export live beads only - tombstones stay in CRDT layer (tombstones.jsonl)
    // but aren't exported to Go-compat format since:
    // 1. beads-go doesn't export them (deleted = removed from export)
    // 2. beads_viewer (bv) doesn't recognize status="tombstone"
    for (id, _) in state.iter_live() {
        let Some(view) = state.bead_view(id) else {
            continue;
        };
        let deps: Vec<_> = state.deps_from(id);
        let is_blocked = is_bead_blocked(id, state);
        let dep_stamp = state.dep_store().stamp();
        let projection = BeadProjection::from_view(&view);
        issues.push(GoIssue::from_projection(
            &projection,
            &deps,
            is_blocked,
            dep_stamp,
        ));
    }

    // Sort by ID for stable diffs
    issues.sort_by(|a, b| a.id.cmp(&b.id));

    // Write atomically: temp file -> fsync -> rename
    {
        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        for issue in &issues {
            serde_json::to_writer(&mut writer, issue)?;
            writeln!(writer)?;
        }

        writer.flush()?;

        // fsync for durability
        let file = writer.into_inner()?;
        file.sync_all()?;
    }

    // Atomic rename
    fs::rename(&temp_path, &final_path)?;

    tracing::debug!("Exported {} issues to {:?}", issues.len(), final_path);

    Ok(final_path)
}

/// Ensure symlinks exist from each clone's .beads/ to the canonical export file.
///
/// Creates `.beads/issues.jsonl` symlink in each known clone path.
pub fn ensure_symlinks(export_path: &Path, known_paths: &HashSet<PathBuf>) -> io::Result<()> {
    for clone_path in known_paths {
        if !clone_path.exists() {
            tracing::debug!("Skipping symlink for non-existent clone: {:?}", clone_path);
            continue;
        }

        let beads_dir = clone_path.join(".beads");
        let symlink_path = beads_dir.join("issues.jsonl");

        // Create .beads directory if it doesn't exist
        if !beads_dir.exists()
            && let Err(e) = fs::create_dir_all(&beads_dir)
        {
            tracing::warn!("Failed to create .beads dir at {:?}: {}", beads_dir, e);
            continue;
        }

        // Check if symlink already points to the right place
        if symlink_path.is_symlink() {
            if let Ok(target) = fs::read_link(&symlink_path)
                && target == export_path
            {
                continue; // Already correct
            }
            // Remove incorrect symlink
            let _ = fs::remove_file(&symlink_path);
        } else if symlink_path.exists() {
            // Regular file exists - back it up and replace
            let backup = beads_dir.join("issues.jsonl.bak");
            match fs::rename(&symlink_path, &backup) {
                Ok(()) => {
                    tracing::info!("Backed up existing issues.jsonl to {:?}", backup);
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to backup {:?}: {}, trying to remove",
                        symlink_path,
                        e
                    );
                    // If backup fails, try to just remove the file
                    if let Err(e2) = fs::remove_file(&symlink_path) {
                        tracing::warn!("Failed to remove {:?}: {}, skipping", symlink_path, e2);
                        continue;
                    }
                }
            }
        }

        // Create symlink
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            if let Err(e) = symlink(export_path, &symlink_path) {
                tracing::warn!(
                    "Failed to create symlink {:?} -> {:?}: {}",
                    symlink_path,
                    export_path,
                    e
                );
            } else {
                tracing::debug!("Created symlink {:?} -> {:?}", symlink_path, export_path);
            }
        }

        #[cfg(not(unix))]
        {
            // On Windows, copy the file instead of symlinking
            // (symlinks require admin privileges)
            if let Err(e) = fs::copy(export_path, &symlink_path) {
                tracing::warn!(
                    "Failed to copy {:?} -> {:?}: {}",
                    export_path,
                    symlink_path,
                    e
                );
            }
        }
    }

    Ok(())
}

/// Hash a remote URL to a short, filesystem-safe identifier.
fn hash_remote(remote_url: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(remote_url.as_bytes());
    let result = hasher.finalize();
    // Use first 16 bytes (32 hex chars) for reasonable uniqueness
    hex::encode(&result[..16])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_hash_remote_deterministic() {
        let url = "git@github.com:user/repo.git";
        let h1 = hash_remote(url);
        let h2 = hash_remote(url);
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 32); // 16 bytes = 32 hex chars
    }

    #[test]
    fn test_hash_remote_different_urls() {
        let h1 = hash_remote("git@github.com:user/repo1.git");
        let h2 = hash_remote("git@github.com:user/repo2.git");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_export_context_path() {
        let ctx = ExportContext {
            exports_dir: PathBuf::from("/tmp/test-exports"),
        };
        let path = ctx.export_path("git@github.com:user/repo.git");
        assert!(path.to_string_lossy().contains("issues.jsonl"));
        assert!(path.to_string_lossy().contains("/tmp/test-exports/"));
    }

    #[test]
    fn test_export_empty_state() {
        let tmp = TempDir::new().unwrap();
        let ctx = ExportContext::with_dir(tmp.path().join("exports")).unwrap();
        let state = CanonicalState::new();

        let path = export_jsonl(&state, &ctx, "git@example.com:test.git").unwrap();
        assert!(path.exists());

        // Empty state should produce empty file
        let content = fs::read_to_string(&path).unwrap();
        assert!(content.is_empty());
    }

    #[test]
    fn test_export_with_beads() {
        use crate::core::bead::{Bead, BeadCore, BeadFields};
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};
        use crate::core::identity::{ActorId, BeadId};
        use crate::core::time::{Stamp, WriteStamp};

        let tmp = TempDir::new().unwrap();
        let ctx = ExportContext::with_dir(tmp.path().join("exports")).unwrap();

        let mut state = CanonicalState::new();
        let stamp = Stamp::new(
            WriteStamp::new(1700000000000, 0),
            ActorId::new("test@host").unwrap(),
        );

        let core = BeadCore::new(BeadId::parse("bd-abc").unwrap(), stamp.clone(), None);
        let fields = BeadFields {
            title: Lww::new("Test Issue".to_string(), stamp.clone()),
            description: Lww::new("A description".to_string(), stamp.clone()),
            design: Lww::new(None, stamp.clone()),
            acceptance_criteria: Lww::new(None, stamp.clone()),
            priority: Lww::new(Priority::HIGH, stamp.clone()),
            bead_type: Lww::new(BeadType::Bug, stamp.clone()),
            external_ref: Lww::new(None, stamp.clone()),
            source_repo: Lww::new(None, stamp.clone()),
            estimated_minutes: Lww::new(None, stamp.clone()),
            workflow: Lww::new(Workflow::Open, stamp.clone()),
            claim: Lww::new(Claim::Unclaimed, stamp.clone()),
        };
        state.insert(Bead::new(core, fields)).unwrap();

        let path = export_jsonl(&state, &ctx, "git@example.com:test.git").unwrap();
        let content = fs::read_to_string(&path).unwrap();

        assert!(content.contains("bd-abc"));
        assert!(content.contains("Test Issue"));
        assert!(content.contains("\"status\":\"open\""));
        assert!(content.contains("\"issue_type\":\"bug\""));
        assert!(content.contains("\"priority\":1"));
    }

    #[test]
    fn test_tombstones_not_exported() {
        // Tombstones stay in CRDT layer (tombstones.jsonl) but are NOT exported
        // to Go-compat format - matches beads-go behavior where deleted = gone
        use crate::core::identity::{ActorId, BeadId};
        use crate::core::time::{Stamp, WriteStamp};
        use crate::core::tombstone::Tombstone;

        let tmp = TempDir::new().unwrap();
        let ctx = ExportContext::with_dir(tmp.path().join("exports")).unwrap();

        let mut state = CanonicalState::new();
        let stamp = Stamp::new(
            WriteStamp::new(1700000000000, 0),
            ActorId::new("test@host").unwrap(),
        );

        let tombstone = Tombstone::new(
            BeadId::parse("bd-xyz").unwrap(),
            stamp,
            Some("deleted by user".to_string()),
        );
        state.delete(tombstone);

        let path = export_jsonl(&state, &ctx, "git@example.com:test.git").unwrap();
        let content = fs::read_to_string(&path).unwrap();

        // Tombstones should NOT appear in export
        assert!(content.is_empty(), "tombstones should not be exported");
        assert!(!content.contains("bd-xyz"));
        assert!(!content.contains("tombstone"));
    }

    #[test]
    fn test_symlink_created_when_none_exists() {
        let tmp = TempDir::new().unwrap();
        let clone_path = tmp.path().join("clone1");
        fs::create_dir_all(&clone_path).unwrap();

        let export_file = tmp.path().join("canonical.jsonl");
        fs::write(&export_file, "{}").unwrap();

        let mut known_paths = HashSet::new();
        known_paths.insert(clone_path.clone());

        ensure_symlinks(&export_file, &known_paths).unwrap();

        let symlink_path = clone_path.join(".beads").join("issues.jsonl");
        assert!(symlink_path.is_symlink());
        assert_eq!(fs::read_link(&symlink_path).unwrap(), export_file);
    }

    #[test]
    fn test_symlink_skipped_when_correct() {
        let tmp = TempDir::new().unwrap();
        let clone_path = tmp.path().join("clone1");
        let beads_dir = clone_path.join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let export_file = tmp.path().join("canonical.jsonl");
        fs::write(&export_file, "{}").unwrap();

        let symlink_path = beads_dir.join("issues.jsonl");

        // Create correct symlink first
        #[cfg(unix)]
        std::os::unix::fs::symlink(&export_file, &symlink_path).unwrap();

        let mut known_paths = HashSet::new();
        known_paths.insert(clone_path.clone());

        // Should succeed without modifying anything
        ensure_symlinks(&export_file, &known_paths).unwrap();

        // Symlink should still exist and be correct
        assert!(symlink_path.is_symlink());
        assert_eq!(fs::read_link(&symlink_path).unwrap(), export_file);
    }

    #[test]
    fn test_symlink_replaced_when_wrong_target() {
        let tmp = TempDir::new().unwrap();
        let clone_path = tmp.path().join("clone1");
        let beads_dir = clone_path.join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let export_file = tmp.path().join("canonical.jsonl");
        fs::write(&export_file, "{}").unwrap();

        let wrong_target = tmp.path().join("wrong.jsonl");
        fs::write(&wrong_target, "{}").unwrap();

        let symlink_path = beads_dir.join("issues.jsonl");

        // Create symlink pointing to wrong target
        #[cfg(unix)]
        std::os::unix::fs::symlink(&wrong_target, &symlink_path).unwrap();

        let mut known_paths = HashSet::new();
        known_paths.insert(clone_path.clone());

        ensure_symlinks(&export_file, &known_paths).unwrap();

        // Symlink should now point to correct target
        assert!(symlink_path.is_symlink());
        assert_eq!(fs::read_link(&symlink_path).unwrap(), export_file);
    }

    #[test]
    fn test_regular_file_backed_up_and_replaced() {
        let tmp = TempDir::new().unwrap();
        let clone_path = tmp.path().join("clone1");
        let beads_dir = clone_path.join(".beads");
        fs::create_dir_all(&beads_dir).unwrap();

        let export_file = tmp.path().join("canonical.jsonl");
        fs::write(&export_file, "{\"canonical\": true}").unwrap();

        let symlink_path = beads_dir.join("issues.jsonl");
        let backup_path = beads_dir.join("issues.jsonl.bak");

        // Create regular file with some content
        fs::write(&symlink_path, "{\"original\": true}").unwrap();

        let mut known_paths = HashSet::new();
        known_paths.insert(clone_path.clone());

        ensure_symlinks(&export_file, &known_paths).unwrap();

        // Original file should be backed up
        assert!(backup_path.exists());
        let backup_content = fs::read_to_string(&backup_path).unwrap();
        assert!(backup_content.contains("original"));

        // Symlink should now exist
        assert!(symlink_path.is_symlink());
        assert_eq!(fs::read_link(&symlink_path).unwrap(), export_file);
    }

    #[test]
    fn test_nonexistent_clone_path_skipped() {
        let tmp = TempDir::new().unwrap();
        let nonexistent_path = tmp.path().join("does_not_exist");

        let export_file = tmp.path().join("canonical.jsonl");
        fs::write(&export_file, "{}").unwrap();

        let mut known_paths = HashSet::new();
        known_paths.insert(nonexistent_path.clone());

        // Should succeed without error
        ensure_symlinks(&export_file, &known_paths).unwrap();

        // No symlink should be created
        assert!(
            !nonexistent_path
                .join(".beads")
                .join("issues.jsonl")
                .exists()
        );
    }

    #[test]
    fn test_multiple_clones_same_canonical() {
        let tmp = TempDir::new().unwrap();
        let clone1 = tmp.path().join("clone1");
        let clone2 = tmp.path().join("clone2");
        let clone3 = tmp.path().join("clone3");

        fs::create_dir_all(&clone1).unwrap();
        fs::create_dir_all(&clone2).unwrap();
        fs::create_dir_all(&clone3).unwrap();

        let export_file = tmp.path().join("canonical.jsonl");
        fs::write(&export_file, "{\"shared\": true}").unwrap();

        let mut known_paths = HashSet::new();
        known_paths.insert(clone1.clone());
        known_paths.insert(clone2.clone());
        known_paths.insert(clone3.clone());

        ensure_symlinks(&export_file, &known_paths).unwrap();

        // All clones should have symlinks to the same canonical file
        for clone in [&clone1, &clone2, &clone3] {
            let symlink_path = clone.join(".beads").join("issues.jsonl");
            assert!(
                symlink_path.is_symlink(),
                "Expected symlink at {:?}",
                symlink_path
            );
            assert_eq!(
                fs::read_link(&symlink_path).unwrap(),
                export_file,
                "Symlink at {:?} points to wrong target",
                symlink_path
            );
        }
    }

    #[test]
    fn test_export_sorted_by_id() {
        use crate::core::bead::{Bead, BeadCore, BeadFields};
        use crate::core::composite::{Claim, Workflow};
        use crate::core::crdt::Lww;
        use crate::core::domain::{BeadType, Priority};
        use crate::core::identity::{ActorId, BeadId};
        use crate::core::time::{Stamp, WriteStamp};

        let tmp = TempDir::new().unwrap();
        let ctx = ExportContext::with_dir(tmp.path().join("exports")).unwrap();

        let mut state = CanonicalState::new();
        let stamp = Stamp::new(
            WriteStamp::new(1700000000000, 0),
            ActorId::new("test@host").unwrap(),
        );

        // Insert beads in non-sorted order
        for id in ["bd-zzz", "bd-aaa", "bd-mmm"] {
            let core = BeadCore::new(BeadId::parse(id).unwrap(), stamp.clone(), None);
            let fields = BeadFields {
                title: Lww::new(format!("Issue {}", id), stamp.clone()),
                description: Lww::new(String::new(), stamp.clone()),
                design: Lww::new(None, stamp.clone()),
                acceptance_criteria: Lww::new(None, stamp.clone()),
                priority: Lww::new(Priority::MEDIUM, stamp.clone()),
                bead_type: Lww::new(BeadType::Task, stamp.clone()),
                external_ref: Lww::new(None, stamp.clone()),
                source_repo: Lww::new(None, stamp.clone()),
                estimated_minutes: Lww::new(None, stamp.clone()),
                workflow: Lww::new(Workflow::Open, stamp.clone()),
                claim: Lww::new(Claim::Unclaimed, stamp.clone()),
            };
            state.insert(Bead::new(core, fields)).unwrap();
        }

        let path = export_jsonl(&state, &ctx, "git@example.com:test.git").unwrap();
        let content = fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();

        // Should be sorted: aaa, mmm, zzz
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("bd-aaa"));
        assert!(lines[1].contains("bd-mmm"));
        assert!(lines[2].contains("bd-zzz"));
    }
}
