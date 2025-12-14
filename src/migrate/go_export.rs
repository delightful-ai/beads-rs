//! Import from beads-go `issues.jsonl` export.
//!
//! The go export schema is defined in beads-go `internal/types/types.go`.
//! We parse each JSONL line into a bead or tombstone, plus deps and comments.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use serde::Deserialize;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

use crate::core::{
    ActorId, Bead, BeadCore, BeadFields, BeadId, BeadType, CanonicalState, Claim, Closure, DepEdge,
    DepKey, DepKind, Labels, Lww, Note, NoteId, Priority, Stamp, Tombstone, Workflow, WriteStamp,
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
    root_slug: Option<String>,
) -> Result<(CanonicalState, GoImportReport)> {
    let file = File::open(path).map_err(IpcError::from)?;
    let reader = BufReader::new(file);

    let mut state = CanonicalState::new();
    let mut deps_to_insert: Vec<DepEdge> = Vec::new();
    let mut report = GoImportReport::default();

    let mut chosen_slug: Option<String> = root_slug
        .as_deref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            BeadId::parse(&format!("{s}-abc"))
                .map(|id| id.slug().to_string())
                .map_err(|e| {
                    Error::Op(OpError::ValidationFailed {
                        field: "root_slug".into(),
                        reason: e.to_string(),
                    })
                })
        })
        .transpose()?;
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
            chosen_slug = Some(id_in.slug().to_string());
        }
        let slug = chosen_slug.as_deref().unwrap_or("bd");
        if id_in.slug() != slug {
            slug_mismatch_count += 1;
        }
        let id = id_in.with_slug(slug)?;
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
                    if let Ok(edge) = dep_to_edge(&dep, actor, slug) {
                        deps_to_insert.push(edge);
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
            labels: Lww::new(labels, updated_stamp.clone()),
            external_ref: Lww::new(issue.external_ref.clone(), updated_stamp.clone()),
            source_repo: Lww::new(None, updated_stamp.clone()),
            estimated_minutes: Lww::new(
                issue.estimated_minutes.and_then(|m| u32::try_from(m).ok()),
                updated_stamp.clone(),
            ),
            workflow: Lww::new(workflow_value, workflow_stamp),
            claim: Lww::new(claim_value, updated_stamp.clone()),
        };

        let core = BeadCore::new(id.clone(), created_stamp, None);
        let mut bead = Bead::new(core, fields);

        // Legacy notes (single string) become a synthetic note.
        if let Some(notes) = issue.notes.clone().filter(|s| !s.trim().is_empty()) {
            let note_id = NoteId::new("legacy-notes".to_string())?;
            let note = Note::new(note_id, notes, actor.clone(), updated_stamp.at.clone());
            bead.notes.insert(note);
            report.notes += 1;
        }

        // Comments -> notes.
        if let Some(comments) = issue.comments {
            for c in comments {
                let c_issue_id = c.issue_id.trim();
                if !c_issue_id.is_empty()
                    && let Ok(cid) = BeadId::parse(c_issue_id).and_then(|cid| cid.with_slug(slug))
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
                let note_id = NoteId::new(c.id.to_string())?;
                let note = Note::new(note_id, c.text.clone(), author, at);
                bead.notes.insert(note);
                report.notes += 1;
            }
        }

        state.insert_live(bead);
        report.live_beads += 1;

        // Dependencies collected for later insert.
        if let Some(deps) = issue.dependencies {
            for dep in deps {
                match dep_to_edge(&dep, actor, slug) {
                    Ok(edge) => deps_to_insert.push(edge),
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

    for edge in deps_to_insert {
        state.insert_dep(edge);
        report.deps += 1;
    }

    report.root_slug = chosen_slug.unwrap_or_else(|| "bd".to_string());
    if slug_mismatch_count > 0 {
        report.warnings.push(format!(
            "some imported IDs had a different slug and were rewritten (count={slug_mismatch_count})"
        ));
    }

    Ok((state, report))
}

fn dep_to_edge(dep: &GoDependency, actor: &ActorId, root_slug: &str) -> Result<DepEdge> {
    let from = BeadId::parse(&dep.issue_id)?.with_slug(root_slug)?;
    let to = BeadId::parse(&dep.depends_on_id)?.with_slug(root_slug)?;
    let kind = parse_dep_kind(&dep.dep_type)?;
    let created_ms = parse_rfc3339_ms(&dep.created_at)?;
    let created_by_raw = dep
        .created_by
        .clone()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| actor.as_str().to_string());
    let created_by = ActorId::new(created_by_raw)?;
    let created_stamp = Stamp::new(WriteStamp::new(created_ms, 0), created_by);
    let key = DepKey::new(from, to, kind);
    Ok(DepEdge::new(key, created_stamp))
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
