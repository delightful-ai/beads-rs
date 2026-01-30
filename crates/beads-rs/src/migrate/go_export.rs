//! Import from beads-go `issues.jsonl` export.
//!
//! The go export schema is defined in beads-go `internal/types/types.go`.
//! We parse each JSONL line into a bead or tombstone, plus deps and comments.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;
use sha2::{Digest, Sha256 as Sha2};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::core::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadSlug, BeadType, CanonicalState, Claim,
    Closure, DepKey, DepKind, Dot, Labels, Lww, Note, NoteId, OrSetValue, Priority, ReplicaId,
    Stamp, Tombstone, Workflow, WriteStamp,
};
use crate::daemon::IpcError;
use crate::daemon::OpError;
use crate::{Error, Result};

/// Summary of an import run.
#[derive(Debug, Default, Clone)]
pub struct GoImportReport {
    pub root_slug: String,
    pub live_beads: usize,
    pub tombstones: usize,
    pub deps: usize,
    pub notes: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct GoIssue {
    pub id: String,
    pub title: String,
    pub description: String,
    #[serde(default)]
    pub design: Option<String>,
    #[serde(default)]
    pub acceptance_criteria: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    pub status: String,
    pub priority: i64,
    pub issue_type: String,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub estimated_minutes: Option<i64>,
    #[serde(default)]
    pub labels: Option<Vec<String>>,
    #[serde(default)]
    pub dependencies: Option<Vec<GoDependency>>,
    #[serde(default)]
    pub comments: Option<Vec<GoComment>>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(default)]
    pub closed_at: Option<String>,
    #[serde(default)]
    pub close_reason: Option<String>,
    #[serde(default)]
    pub external_ref: Option<String>,
    // Tombstone fields
    #[serde(default)]
    pub deleted_at: Option<String>,
    #[serde(default)]
    pub deleted_by: Option<String>,
    #[serde(default)]
    pub delete_reason: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub original_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GoDependency {
    pub issue_id: String,
    pub depends_on_id: String,
    #[serde(rename = "type")]
    pub dep_type: String,
    pub created_at: String,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GoComment {
    pub id: i64,
    pub issue_id: String,
    pub author: String,
    pub text: String,
    pub created_at: String,
}

/// Import a beads-go JSONL export into a CanonicalState.
///
/// This is a pure conversion step; writing to git happens at CLI.
pub fn import_go_export(
    path: &Path,
    actor: &ActorId,
    root_slug: Option<BeadSlug>,
) -> Result<(CanonicalState, GoImportReport)> {
    let file = File::open(path).map_err(IpcError::from)?;
    let reader = BufReader::new(file);

    let mut state = CanonicalState::new();
    let mut deps_to_insert: Vec<(DepKey, Dot, Stamp)> = Vec::new();
    let mut report = GoImportReport::default();

    let default_slug = BeadSlug::parse("bd").expect("default slug must be valid");
    let mut chosen_slug: Option<BeadSlug> = root_slug;
    let mut slug_mismatch_count: usize = 0;

    for (line_no, line_res) in reader.lines().enumerate() {
        let line = line_res.map_err(IpcError::from)?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let issue: GoIssue = serde_json::from_str(trimmed).map_err(IpcError::from)?;

        // Parse ids and times
        let id_in = BeadId::parse(&issue.id)?;
        if chosen_slug.is_none() {
            chosen_slug = Some(id_in.slug_value());
        }
        let slug = chosen_slug.as_ref().unwrap_or(&default_slug);
        if id_in.slug() != slug.as_str() {
            slug_mismatch_count += 1;
        }
        let id = id_in.with_slug(slug.as_str())?;
        let created_ms = parse_rfc3339_ms(&issue.created_at)?;
        let updated_ms = parse_rfc3339_ms(&issue.updated_at)?;
        let created_stamp = Stamp::new(WriteStamp::new(created_ms, 0), actor.clone());
        let updated_stamp = Stamp::new(WriteStamp::new(updated_ms, 0), actor.clone());

        // Tombstone?
        let status_norm = issue.status.trim().to_lowercase().replace('-', "_");
        if status_norm == "tombstone" {
            let deleted_ms = issue
                .deleted_at
                .as_deref()
                .map(parse_rfc3339_ms)
                .transpose()?
                .unwrap_or(updated_ms);
            let deleted_by_raw = issue
                .deleted_by
                .clone()
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| actor.as_str().to_string());
            let deleted_by = ActorId::new(deleted_by_raw)?;
            let deleted_stamp = Stamp::new(WriteStamp::new(deleted_ms, 0), deleted_by);
            let reason = issue.delete_reason.clone().filter(|s| !s.trim().is_empty());
            let tomb = Tombstone::new(id.clone(), deleted_stamp, reason);
            state.insert_tombstone(tomb);
            report.tombstones += 1;

            // Dependencies from tombstones are still imported.
            if let Some(deps) = issue.dependencies {
                for dep in deps {
                    if let Ok((key, dot, stamp)) = dep_to_add(&dep, actor, slug) {
                        deps_to_insert.push((key, dot, stamp));
                    } else {
                        report.warnings.push(format!(
                            "line {}: skipped invalid dep {:?}",
                            line_no + 1,
                            dep
                        ));
                    }
                }
            }
            continue;
        }

        // Map issue_type
        let bead_type = parse_issue_type(&issue.issue_type)?;
        let priority = Priority::new(issue.priority as u8)?;

        // Labels
        let labels_vec = issue.labels.unwrap_or_default();
        let mut labels = Labels::new();
        for l in labels_vec {
            let label = crate::core::Label::parse(l)?;
            labels.insert(label);
        }

        // Workflow
        let (workflow_value, workflow_stamp) = workflow_from_status(
            &status_norm,
            issue.close_reason.clone(),
            issue.closed_at.as_deref(),
            &updated_stamp,
            actor,
        )?;

        // Claim
        let mut claim_value = Claim::Unclaimed;
        if let Some(assignee_raw) = issue.assignee.clone() {
            let assignee_raw = assignee_raw.trim();
            if !assignee_raw.is_empty() {
                let assignee = ActorId::new(assignee_raw.to_string())?;
                claim_value = Claim::Claimed {
                    assignee,
                    expires: None,
                };
            }
        }

        // If claimed but status open, follow rust spec and treat as in_progress.
        let mut workflow_value = workflow_value;
        let mut workflow_stamp = workflow_stamp;
        if matches!(claim_value, Claim::Claimed { .. }) && matches!(workflow_value, Workflow::Open)
        {
            workflow_value = Workflow::InProgress;
            workflow_stamp = updated_stamp.clone();
        }

        // Fields: stamp everything at updated for simplicity.
        let fields = BeadFields {
            title: Lww::new(issue.title.clone(), updated_stamp.clone()),
            description: Lww::new(issue.description.clone(), updated_stamp.clone()),
            design: Lww::new(issue.design.clone(), updated_stamp.clone()),
            acceptance_criteria: Lww::new(issue.acceptance_criteria.clone(), updated_stamp.clone()),
            priority: Lww::new(priority, updated_stamp.clone()),
            bead_type: Lww::new(bead_type, updated_stamp.clone()),
            external_ref: Lww::new(issue.external_ref.clone(), updated_stamp.clone()),
            source_repo: Lww::new(None, updated_stamp.clone()),
            estimated_minutes: Lww::new(
                issue.estimated_minutes.and_then(|m| u32::try_from(m).ok()),
                updated_stamp.clone(),
            ),
            workflow: Lww::new(workflow_value, workflow_stamp),
            claim: Lww::new(claim_value, updated_stamp.clone()),
        };

        let core = BeadCore::new(id.clone(), created_stamp.clone(), None);
        let bead = Bead::new(core, fields);

        state.insert_live(bead);
        report.live_beads += 1;

        if labels.is_empty() {
            // No labels to insert; leave label stamp unset.
        } else {
            for label in labels.iter() {
                let dot = legacy_dot_from_bytes(label.as_str().as_bytes(), &updated_stamp);
                state.apply_label_add(
                    id.clone(),
                    label.clone(),
                    dot,
                    updated_stamp.clone(),
                    created_stamp.clone(),
                );
            }
        }

        // Legacy notes (single string) become a synthetic note.
        if let Some(notes) = issue.notes.clone().filter(|s| !s.trim().is_empty()) {
            let note_id = NoteId::new("legacy-notes".to_string())?;
            let note = Note::new(note_id, notes, actor.clone(), updated_stamp.at.clone());
            state.insert_note(id.clone(), created_stamp.clone(), note);
            report.notes += 1;
        }

        // Comments -> notes.
        if let Some(comments) = issue.comments {
            for c in comments {
                let c_issue_id = c.issue_id.trim();
                if !c_issue_id.is_empty()
                    && let Ok(cid) =
                        BeadId::parse(c_issue_id).and_then(|cid| cid.with_slug(slug.as_str()))
                    && cid.as_str() != id.as_str()
                {
                    report.warnings.push(format!(
                        "line {}: comment {} has mismatched issue_id {} (expected {})",
                        line_no + 1,
                        c.id,
                        c.issue_id,
                        id.as_str()
                    ));
                }

                let at_ms = parse_rfc3339_ms(&c.created_at)?;
                let at = WriteStamp::new(at_ms, 0);
                let author = if c.author.trim().is_empty() {
                    actor.clone()
                } else {
                    ActorId::new(c.author.clone())?
                };
                // Include issue ID to ensure global uniqueness across all imported comments
                let note_id = NoteId::new(format!("go-comment-{}-{}", id.as_str(), c.id))?;
                let note = Note::new(note_id, c.text.clone(), author, at);
                state.insert_note(id.clone(), created_stamp.clone(), note);
                report.notes += 1;
            }
        }

        // Dependencies collected for later insert.
        if let Some(deps) = issue.dependencies {
            for dep in deps {
                match dep_to_add(&dep, actor, slug) {
                    Ok((key, dot, stamp)) => deps_to_insert.push((key, dot, stamp)),
                    Err(e) => report.warnings.push(format!(
                        "line {}: invalid dep {} -> {} ({}) : {}",
                        line_no + 1,
                        dep.issue_id,
                        dep.depends_on_id,
                        dep.dep_type,
                        e
                    )),
                }
            }
        }
    }

    for (key, dot, stamp) in deps_to_insert {
        match state.check_dep_add_key(key.clone()) {
            Ok(key) => {
                state.apply_dep_add(key, dot, stamp);
                report.deps += 1;
            }
            Err(err) => report.warnings.push(format!(
                "skipping dependency {} -> {}: {}",
                key.from(),
                key.to(),
                err.reason
            )),
        }
    }

    let final_slug = chosen_slug.as_ref().unwrap_or(&default_slug);
    report.root_slug = final_slug.as_str().to_string();
    if slug_mismatch_count > 0 {
        report.warnings.push(format!(
            "some imported IDs had a different slug and were rewritten (count={slug_mismatch_count})"
        ));
    }

    Ok((state, report))
}

fn dep_to_add(
    dep: &GoDependency,
    actor: &ActorId,
    root_slug: &BeadSlug,
) -> Result<(DepKey, Dot, Stamp)> {
    let from = BeadId::parse(&dep.issue_id)?.with_slug(root_slug.as_str())?;
    let to = BeadId::parse(&dep.depends_on_id)?.with_slug(root_slug.as_str())?;
    let kind = parse_dep_kind(&dep.dep_type)?;
    let created_ms = parse_rfc3339_ms(&dep.created_at)?;
    let created_by_raw = dep
        .created_by
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| actor.as_str().to_string());
    let created_by = ActorId::new(created_by_raw)?;
    let created_stamp = Stamp::new(WriteStamp::new(created_ms, 0), created_by);
    let key = DepKey::new(from, to, kind).map_err(crate::core::CoreError::from)?;
    let dot = legacy_dot_from_bytes(&key.collision_bytes(), &created_stamp);
    Ok((key, dot, created_stamp))
}

fn parse_rfc3339_ms(raw: &str) -> Result<u64> {
    let dt = OffsetDateTime::parse(raw, &Rfc3339).map_err(|e| {
        Error::Op(OpError::ValidationFailed {
            field: "timestamp".into(),
            reason: format!("invalid rfc3339 `{}`: {}", raw, e),
        })
    })?;
    let nanos = dt.unix_timestamp_nanos();
    if nanos < 0 {
        return Ok(0);
    }
    Ok((nanos / 1_000_000) as u64)
}

fn legacy_dot_from_bytes(bytes: &[u8], stamp: &Stamp) -> Dot {
    let mut hasher = Sha2::new();
    hasher.update(bytes);
    hasher.update(stamp.at.wall_ms.to_le_bytes());
    hasher.update(stamp.at.counter.to_le_bytes());
    hasher.update(stamp.by.as_str().as_bytes());
    let digest = hasher.finalize();

    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&digest[..16]);
    let mut counter_bytes = [0u8; 8];
    counter_bytes.copy_from_slice(&digest[16..24]);

    Dot {
        replica: ReplicaId::from(Uuid::from_bytes(uuid_bytes)),
        counter: u64::from_le_bytes(counter_bytes),
    }
}

fn parse_issue_type(raw: &str) -> Result<BeadType> {
    let s = raw.trim().to_lowercase();
    match s.as_str() {
        "bug" => Ok(BeadType::Bug),
        "feature" => Ok(BeadType::Feature),
        "task" => Ok(BeadType::Task),
        "epic" => Ok(BeadType::Epic),
        "chore" => Ok(BeadType::Chore),
        other => Err(Error::Op(OpError::ValidationFailed {
            field: "issue_type".into(),
            reason: format!("unknown issue_type `{}`", other),
        })),
    }
}

fn parse_dep_kind(raw: &str) -> Result<DepKind> {
    let s = raw.trim().to_lowercase().replace('_', "-");
    match s.as_str() {
        "blocks" | "block" => Ok(DepKind::Blocks),
        "related" | "relates" => Ok(DepKind::Related),
        "parent-child" | "parent" => Ok(DepKind::Parent),
        "discovered-from" | "discovered_from" => Ok(DepKind::DiscoveredFrom),
        other => Err(Error::Op(OpError::ValidationFailed {
            field: "dep_type".into(),
            reason: format!("unknown dep type `{}`", other),
        })),
    }
}

fn workflow_from_status(
    status_norm: &str,
    close_reason: Option<String>,
    closed_at_raw: Option<&str>,
    updated_stamp: &Stamp,
    actor: &ActorId,
) -> Result<(Workflow, Stamp)> {
    match status_norm {
        "closed" => {
            let closed_ms = if let Some(raw) = closed_at_raw {
                parse_rfc3339_ms(raw)?
            } else {
                updated_stamp.at.wall_ms
            };
            let closed_stamp = Stamp::new(WriteStamp::new(closed_ms, 0), actor.clone());
            let reason = close_reason.filter(|s| !s.trim().is_empty());
            Ok((Workflow::Closed(Closure::new(reason, None)), closed_stamp))
        }
        "in_progress" | "inprogress" => Ok((Workflow::InProgress, updated_stamp.clone())),
        "open" | "blocked" => Ok((Workflow::Open, updated_stamp.clone())),
        _other => {
            // Unknown/custom statuses are treated as open; preserve via warning at caller.
            Ok((Workflow::Open, updated_stamp.clone()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn make_go_issue(id: &str, comments: Vec<(i64, &str)>) -> String {
        let comments_json: Vec<String> = comments
            .into_iter()
            .map(|(cid, text)| {
                format!(
                    r#"{{"id": {}, "issue_id": "{}", "author": "test", "text": "{}", "created_at": "2024-01-01T00:00:00Z"}}"#,
                    cid, id, text
                )
            })
            .collect();
        format!(
            r#"{{"id": "{}", "title": "Test", "description": "desc", "status": "open", "priority": 2, "issue_type": "task", "created_at": "2024-01-01T00:00:00Z", "updated_at": "2024-01-01T00:00:00Z", "comments": [{}]}}"#,
            id,
            comments_json.join(", ")
        )
    }

    #[test]
    fn test_comment_ids_globally_unique_across_issues() {
        // Two issues, each with a comment id=1
        let issue1 = make_go_issue("bd-aaa", vec![(1, "Comment on issue aaa")]);
        let issue2 = make_go_issue("bd-bbb", vec![(1, "Comment on issue bbb")]);
        let jsonl = format!("{}\n{}\n", issue1, issue2);

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(jsonl.as_bytes()).unwrap();

        let actor = ActorId::new("test-actor".to_string()).unwrap();
        let (state, report) = import_go_export(file.path(), &actor, None).unwrap();

        // Both issues should be imported
        assert_eq!(report.live_beads, 2);
        // Both comments should be imported (not merged)
        assert_eq!(report.notes, 2);

        // Verify distinct note IDs
        let bead_aaa = BeadId::parse("bd-aaa").unwrap();
        let bead_bbb = BeadId::parse("bd-bbb").unwrap();

        let notes_aaa = state.notes_for(&bead_aaa);
        let notes_bbb = state.notes_for(&bead_bbb);

        assert_eq!(notes_aaa.len(), 1);
        assert_eq!(notes_bbb.len(), 1);

        let note_id_aaa = notes_aaa[0].id.as_str();
        let note_id_bbb = notes_bbb[0].id.as_str();

        // Note IDs should be different despite same comment id=1
        assert_ne!(note_id_aaa, note_id_bbb);
        // Verify the format includes issue ID
        assert!(note_id_aaa.contains("bd-aaa"));
        assert!(note_id_bbb.contains("bd-bbb"));
    }

    #[test]
    fn test_multiple_comments_same_issue_unique() {
        // Single issue with multiple comments
        let issue = make_go_issue(
            "bd-xyz",
            vec![
                (1, "First comment"),
                (2, "Second comment"),
                (3, "Third comment"),
            ],
        );

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(issue.as_bytes()).unwrap();

        let actor = ActorId::new("test-actor".to_string()).unwrap();
        let (state, report) = import_go_export(file.path(), &actor, None).unwrap();

        assert_eq!(report.notes, 3);

        let bead = BeadId::parse("bd-xyz").unwrap();
        let notes = state.notes_for(&bead);
        assert_eq!(notes.len(), 3);

        // All note IDs should be unique
        let note_ids: std::collections::HashSet<_> =
            notes.iter().map(|note| note.id.as_str()).collect();
        assert_eq!(note_ids.len(), 3);
    }
}
