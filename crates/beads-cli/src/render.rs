use beads_api::{
    AdminCheckpointOutput, AdminDoctorOutput, AdminFingerprintOutput, AdminFlushOutput,
    AdminFsckOutput, AdminMaintenanceModeOutput, AdminMetricsOutput, AdminRebuildIndexOutput,
    AdminReloadLimitsOutput, AdminReloadPoliciesOutput, AdminReloadReplicationOutput,
    AdminRotateReplicaIdOutput, AdminScrubOutput, AdminStatusOutput, AdminStoreLockInfoOutput,
    AdminStoreUnlockOutput, BlockedIssue, CountResult, DaemonInfo, DeletedLookup, DepCycles,
    DepEdge, EpicStatus, Issue, IssueSummary, Note, QueryResult, ReadyResult, ShowDetails,
    StatusOutput, Tombstone, TrackerIssue,
};
use beads_core::{BeadRef, WallClock, WriteStamp};
use beads_surface::OpResult;
use beads_surface::ipc::ResponsePayload;
use serde::Serialize;
use serde_json::{Map, Value, json};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub trait HumanRenderer {
    fn render_created(&self, id: &str) -> String;
    fn render_updated(&self, id: &str) -> String;
    fn render_closed(&self, id: &str) -> String;
    fn render_reopened(&self, id: &str) -> String;
    fn render_deleted_op(&self, id: &str) -> String;
    fn render_dep_added(&self, from: &str, to: &str) -> String;
    fn render_dep_removed(&self, from: &str, to: &str) -> String;
    fn render_note_added(&self, bead_id: &str) -> String;
    fn render_claimed(&self, id: &str, expires: u64) -> String;
    fn render_unclaimed(&self, id: &str) -> String;
    fn render_claim_extended(&self, id: &str, expires: u64) -> String;
    fn render_issue(&self, issue: &Issue) -> String;
    fn render_issues(&self, issues: &[IssueSummary]) -> String;
    fn render_tracker_issues(&self, issues: &[TrackerIssue]) -> String;
    fn render_dep_tree(&self, root: &BeadRef, edges: &[DepEdge]) -> String;
    fn render_deps(&self, incoming: &[DepEdge], outgoing: &[DepEdge]) -> String;
    fn render_dep_cycles(&self, out: &DepCycles) -> String;
    fn render_notes(&self, notes: &[Note]) -> String;
    fn render_status(&self, out: &StatusOutput) -> String;
    fn render_blocked(&self, blocked: &[BlockedIssue]) -> String;
    fn render_ready(&self, result: &ReadyResult) -> String;
    fn render_count(&self, result: &CountResult) -> String;
    fn render_deleted_list(&self, tombs: &[Tombstone]) -> String;
    fn render_deleted_lookup(&self, out: &DeletedLookup) -> String;
    fn render_epic_status(&self, statuses: &[EpicStatus]) -> String;
    fn render_daemon_info(&self, info: &DaemonInfo) -> String;
    fn render_admin_status(&self, status: &AdminStatusOutput) -> String;
    fn render_admin_metrics(&self, metrics: &AdminMetricsOutput) -> String;
    fn render_admin_doctor(&self, out: &AdminDoctorOutput) -> String;
    fn render_admin_scrub(&self, out: &AdminScrubOutput) -> String;
    fn render_admin_flush(&self, out: &AdminFlushOutput) -> String;
    fn render_admin_checkpoint(&self, out: &AdminCheckpointOutput) -> String;
    fn render_admin_fingerprint(&self, out: &AdminFingerprintOutput) -> String;
    fn render_admin_reload_policies(&self, out: &AdminReloadPoliciesOutput) -> String;
    fn render_admin_reload_replication(&self, out: &AdminReloadReplicationOutput) -> String;
    fn render_admin_reload_limits(&self, out: &AdminReloadLimitsOutput) -> String;
    fn render_admin_rotate_replica_id(&self, out: &AdminRotateReplicaIdOutput) -> String;
    fn render_admin_maintenance_mode(&self, out: &AdminMaintenanceModeOutput) -> String;
    fn render_admin_rebuild_index(&self, out: &AdminRebuildIndexOutput) -> String;
    fn render_admin_fsck(&self, out: &AdminFsckOutput) -> String;
    fn render_admin_store_unlock(&self, out: &AdminStoreUnlockOutput) -> String;
    fn render_admin_store_lock_info(&self, out: &AdminStoreLockInfoOutput) -> String;
}

pub fn render_human<R: HumanRenderer>(payload: &ResponsePayload, renderer: &R) -> String {
    match payload {
        ResponsePayload::Op(op) => render_op(&op.result, renderer),
        ResponsePayload::Query(q) => render_query(q, renderer),
        ResponsePayload::Synced(_) => "synced".into(),
        ResponsePayload::Refreshed(_) => "refreshed".into(),
        ResponsePayload::Initialized(_) => "initialized".into(),
        ResponsePayload::ShuttingDown(_) => "shutting down".into(),
        ResponsePayload::Subscribed(sub) => {
            format!("subscribed to {}", sub.subscribed.namespace.as_str())
        }
        ResponsePayload::Event(ev) => {
            format!("event {}", ev.event.event_id.origin_seq.get())
        }
    }
}

pub fn render_op<R: HumanRenderer>(op: &OpResult, renderer: &R) -> String {
    match op {
        OpResult::Created { id } => renderer.render_created(id.as_str()),
        OpResult::Updated { id } => renderer.render_updated(id.as_str()),
        OpResult::Closed { id } => renderer.render_closed(id.as_str()),
        OpResult::Reopened { id } => renderer.render_reopened(id.as_str()),
        OpResult::Deleted { id } => renderer.render_deleted_op(id.as_str()),
        OpResult::DepAdded { from, to } => renderer.render_dep_added(from.as_str(), to.as_str()),
        OpResult::DepRemoved { from, to } => {
            renderer.render_dep_removed(from.as_str(), to.as_str())
        }
        OpResult::NoteAdded { bead_id, .. } => renderer.render_note_added(bead_id.as_str()),
        OpResult::Claimed { id, expires } => renderer.render_claimed(id.as_str(), expires.0),
        OpResult::Unclaimed { id } => renderer.render_unclaimed(id.as_str()),
        OpResult::ClaimExtended { id, expires } => {
            renderer.render_claim_extended(id.as_str(), expires.0)
        }
    }
}

pub fn render_query<R: HumanRenderer>(q: &QueryResult, renderer: &R) -> String {
    match q {
        QueryResult::Issue(issue) => renderer.render_issue(issue),
        QueryResult::ShowDetails(details) => renderer.render_issue(&details.issue),
        QueryResult::Issues(views) => renderer.render_issues(views),
        QueryResult::TrackerIssues(issues) => renderer.render_tracker_issues(issues),
        QueryResult::DepTree { root, edges } => renderer.render_dep_tree(root, edges),
        QueryResult::Deps { incoming, outgoing } => renderer.render_deps(incoming, outgoing),
        QueryResult::DepCycles(out) => renderer.render_dep_cycles(out),
        QueryResult::Notes(notes) => renderer.render_notes(notes),
        QueryResult::Status(out) => renderer.render_status(out),
        QueryResult::Blocked(blocked) => renderer.render_blocked(blocked),
        QueryResult::Ready(result) => renderer.render_ready(result),
        QueryResult::Stale(issues) => renderer.render_issues(issues),
        QueryResult::Count(result) => renderer.render_count(result),
        QueryResult::Deleted(tombs) => renderer.render_deleted_list(tombs),
        QueryResult::DeletedLookup(out) => renderer.render_deleted_lookup(out),
        QueryResult::EpicStatus(statuses) => renderer.render_epic_status(statuses),
        QueryResult::DaemonInfo(info) => renderer.render_daemon_info(info),
        QueryResult::AdminStatus(status) => renderer.render_admin_status(status),
        QueryResult::AdminMetrics(metrics) => renderer.render_admin_metrics(metrics),
        QueryResult::AdminDoctor(out) => renderer.render_admin_doctor(out),
        QueryResult::AdminScrub(out) => renderer.render_admin_scrub(out),
        QueryResult::AdminFlush(out) => renderer.render_admin_flush(out),
        QueryResult::AdminCheckpoint(out) => renderer.render_admin_checkpoint(out),
        QueryResult::AdminFingerprint(out) => renderer.render_admin_fingerprint(out),
        QueryResult::AdminReloadPolicies(out) => renderer.render_admin_reload_policies(out),
        QueryResult::AdminRotateReplicaId(out) => renderer.render_admin_rotate_replica_id(out),
        QueryResult::AdminReloadReplication(out) => renderer.render_admin_reload_replication(out),
        QueryResult::AdminReloadLimits(out) => renderer.render_admin_reload_limits(out),
        QueryResult::AdminMaintenanceMode(out) => renderer.render_admin_maintenance_mode(out),
        QueryResult::AdminRebuildIndex(out) => renderer.render_admin_rebuild_index(out),
        QueryResult::AdminFsck(out) => renderer.render_admin_fsck(out),
        QueryResult::AdminStoreUnlock(out) => renderer.render_admin_store_unlock(out),
        QueryResult::AdminStoreLockInfo(out) => renderer.render_admin_store_lock_info(out),
        QueryResult::Validation { warnings } => {
            if warnings.is_empty() {
                "ok".into()
            } else {
                warnings.join("\n")
            }
        }
    }
}

pub fn print_line(line: &str) -> crate::Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    if let Err(err) = writeln!(stdout, "{line}")
        && err.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(beads_surface::ipc::IpcError::Transport { source: err });
    }
    Ok(())
}

pub fn print_json<T: Serialize>(value: &T) -> crate::Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, value)
        .map_err(|source| beads_surface::ipc::IpcError::PayloadEncode { source })?;
    if let Err(err) = writeln!(stdout)
        && err.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(beads_surface::ipc::IpcError::Transport { source: err });
    }
    Ok(())
}

pub fn print_cli_json(payload: &ResponsePayload) -> crate::Result<()> {
    print_json(&cli_json_payload(payload))
}

pub fn cli_json_payload(payload: &ResponsePayload) -> Value {
    match payload {
        ResponsePayload::Query(q) => cli_query_json(q),
        ResponsePayload::Op(op) => op
            .issue
            .as_ref()
            .map(issue_json_value)
            .unwrap_or_else(|| op_result_json_value(&op.result)),
        ResponsePayload::Synced(_) => json!({"result": "synced"}),
        ResponsePayload::Refreshed(_) => json!({"result": "refreshed"}),
        ResponsePayload::Initialized(_) => json!({"result": "initialized"}),
        ResponsePayload::ShuttingDown(_) => json!({"result": "shutting_down"}),
        ResponsePayload::Subscribed(sub) => json!(sub),
        ResponsePayload::Event(ev) => json!(ev),
    }
}

fn cli_query_json(q: &QueryResult) -> Value {
    match q {
        QueryResult::Issue(issue) => issue_json_value(issue),
        QueryResult::ShowDetails(details) => show_details_json_value(details),
        QueryResult::Issues(issues) | QueryResult::Stale(issues) => Value::Array(
            issues
                .iter()
                .map(issue_summary_json_value)
                .collect::<Vec<_>>(),
        ),
        QueryResult::Ready(result) => Value::Array(
            result
                .issues
                .iter()
                .map(issue_summary_json_value)
                .collect::<Vec<_>>(),
        ),
        QueryResult::Blocked(blocked) => Value::Array(
            blocked
                .iter()
                .map(|blocked| {
                    let mut value = issue_summary_json_value(&blocked.issue);
                    if let Value::Object(ref mut map) = value {
                        map.insert("blocked_by_count".into(), json!(blocked.blocked_by_count));
                        map.insert("blocked_by".into(), json!(blocked.blocked_by));
                    }
                    value
                })
                .collect::<Vec<_>>(),
        ),
        other => json!(other),
    }
}

pub fn issue_array_json_value(issues: &[Issue]) -> Value {
    Value::Array(issues.iter().map(issue_json_value).collect())
}

pub fn show_details_array_json_value(details: &[ShowDetails]) -> Value {
    Value::Array(details.iter().map(show_details_json_value).collect())
}

pub fn show_details_json_value(details: &ShowDetails) -> Value {
    let mut value = issue_json_value(&details.issue);
    let Some(map) = value.as_object_mut() else {
        return value;
    };
    let summaries = summary_map(&details.summaries);
    let dependencies = details
        .outgoing
        .iter()
        .filter_map(|edge| dependency_issue_json(&summaries, &edge.to, &edge.kind))
        .collect::<Vec<_>>();
    let dependents = details
        .incoming
        .iter()
        .filter_map(|edge| dependency_issue_json(&summaries, &edge.from, &edge.kind))
        .collect::<Vec<_>>();
    if !dependencies.is_empty() {
        map.insert("dependencies".into(), Value::Array(dependencies));
    }
    if !dependents.is_empty() {
        map.insert("dependents".into(), Value::Array(dependents));
    }
    if let Some(parent) = details
        .outgoing
        .iter()
        .find(|edge| dep_kind_to_go_wire(&edge.kind) == "parent-child")
        .map(|edge| edge.to.clone())
    {
        map.insert("parent".into(), json!(parent));
    }
    value
}

pub fn issue_summary_array_json_value(issues: &[IssueSummary]) -> Value {
    Value::Array(issues.iter().map(issue_summary_json_value).collect())
}

pub fn dependency_summary_json_value(summary: &IssueSummary, dependency_type: &str) -> Value {
    let mut value = issue_summary_json_value(summary);
    if let Value::Object(ref mut map) = value {
        map.insert(
            "dependency_type".into(),
            json!(dep_kind_to_go_wire(dependency_type)),
        );
    }
    value
}

pub fn dependency_summary_json_value_for_edge(summary: &IssueSummary, edge: &DepEdge) -> Value {
    let mut value = dependency_summary_json_value(summary, &edge.kind);
    if let Value::Object(ref mut map) = value {
        insert_dep_edge_fields(map, edge);
    }
    value
}

pub fn dep_record_json_value(edge: &DepEdge) -> Value {
    json!({
        "issue_id": edge.from.as_str(),
        "depends_on_id": edge.to.as_str(),
        "type": dep_kind_to_go_wire(&edge.kind),
        "from_namespace": edge.from_namespace.as_str(),
        "from": edge.from.as_str(),
        "to_namespace": edge.to_namespace.as_str(),
        "to": edge.to.as_str(),
    })
}

pub fn issue_json_value(issue: &Issue) -> Value {
    let mut map = base_issue_map(
        &issue.id,
        Some(issue.namespace.as_str()),
        &issue.title,
        &issue.description,
        issue.design.as_deref(),
        issue.acceptance_criteria.as_deref(),
        issue.status.as_str(),
        issue.priority,
        issue.issue_type.as_str(),
        &issue.labels,
        issue.assignee.as_deref(),
        Some(&issue.created_at),
        Some(issue.created_by.as_str()),
        Some(&issue.updated_at),
        Some(issue.updated_by.as_str()),
        issue.estimated_minutes,
        Some(issue.content_hash.as_str()),
    );
    insert_opt_stamp(&mut map, "assignee_at", issue.assignee_at.as_ref());
    insert_opt_wall_clock(&mut map, "assignee_expires", issue.assignee_expires);
    insert_opt_stamp(&mut map, "closed_at", issue.closed_at.as_ref());
    insert_opt_str(&mut map, "closed_by", issue.closed_by.as_deref());
    insert_opt_str(&mut map, "close_reason", issue.closed_reason.as_deref());
    insert_opt_str(
        &mut map,
        "created_on_branch",
        issue
            .created_on_branch
            .as_ref()
            .map(|branch| branch.as_str()),
    );
    insert_opt_str(
        &mut map,
        "closed_on_branch",
        issue
            .closed_on_branch
            .as_ref()
            .map(|branch| branch.as_str()),
    );
    insert_opt_str(&mut map, "external_ref", issue.external_ref.as_deref());
    insert_opt_str(&mut map, "source_repo", issue.source_repo.as_deref());
    if !issue.notes.is_empty() {
        map.insert(
            "comments".into(),
            Value::Array(issue.notes.iter().map(note_json_value).collect()),
        );
    }
    Value::Object(map)
}

pub fn issue_summary_json_value(issue: &IssueSummary) -> Value {
    Value::Object(base_issue_map(
        &issue.id,
        Some(issue.namespace.as_str()),
        &issue.title,
        &issue.description,
        issue.design.as_deref(),
        issue.acceptance_criteria.as_deref(),
        issue.status.as_str(),
        issue.priority,
        issue.issue_type.as_str(),
        &issue.labels,
        issue.assignee.as_deref(),
        Some(&issue.created_at),
        Some(issue.created_by.as_str()),
        Some(&issue.updated_at),
        Some(issue.updated_by.as_str()),
        issue.estimated_minutes,
        Some(issue.content_hash.as_str()),
    ))
}

#[allow(clippy::too_many_arguments)]
fn base_issue_map(
    id: &str,
    namespace: Option<&str>,
    title: &str,
    description: &str,
    design: Option<&str>,
    acceptance_criteria: Option<&str>,
    status: &str,
    priority: u8,
    issue_type: &str,
    labels: &[String],
    assignee: Option<&str>,
    created_at: Option<&WriteStamp>,
    created_by: Option<&str>,
    updated_at: Option<&WriteStamp>,
    updated_by: Option<&str>,
    estimated_minutes: Option<u32>,
    content_hash: Option<&str>,
) -> Map<String, Value> {
    let mut map = Map::new();
    map.insert("id".into(), json!(id));
    insert_opt_str(&mut map, "namespace", namespace);
    map.insert("title".into(), json!(title));
    map.insert("description".into(), json!(description));
    insert_opt_str(&mut map, "design", design);
    insert_opt_str(&mut map, "acceptance_criteria", acceptance_criteria);
    map.insert("status".into(), json!(status));
    map.insert("priority".into(), json!(priority));
    map.insert("issue_type".into(), json!(issue_type));
    if !labels.is_empty() {
        map.insert("labels".into(), json!(labels));
    }
    insert_opt_str(&mut map, "assignee", assignee);
    if let Some(created_at) = created_at {
        map.insert("created_at".into(), json!(stamp_to_rfc3339(created_at)));
    }
    insert_opt_str(&mut map, "created_by", created_by);
    if let Some(updated_at) = updated_at {
        map.insert("updated_at".into(), json!(stamp_to_rfc3339(updated_at)));
    }
    insert_opt_str(&mut map, "updated_by", updated_by);
    if let Some(minutes) = estimated_minutes {
        map.insert("estimated_minutes".into(), json!(minutes));
    }
    insert_opt_str(&mut map, "content_hash", content_hash);
    map
}

fn note_json_value(note: &Note) -> Value {
    json!({
        "id": note.id,
        "author": note.author,
        "text": note.content,
        "created_at": stamp_to_rfc3339(&note.at),
    })
}

fn op_result_json_value(result: &OpResult) -> Value {
    match result {
        OpResult::Created { id }
        | OpResult::Updated { id }
        | OpResult::Closed { id }
        | OpResult::Reopened { id }
        | OpResult::Deleted { id }
        | OpResult::Claimed { id, .. }
        | OpResult::Unclaimed { id }
        | OpResult::ClaimExtended { id, .. } => json!({ "id": id.as_str() }),
        OpResult::DepAdded { from, to } => {
            json!({ "issue_id": from.as_str(), "depends_on_id": to.as_str() })
        }
        OpResult::DepRemoved { from, to } => {
            json!({ "issue_id": from.as_str(), "depends_on_id": to.as_str() })
        }
        OpResult::NoteAdded { bead_id, note_id } => {
            json!({ "issue_id": bead_id.as_str(), "note_id": note_id })
        }
    }
}

fn summary_map(summaries: &[IssueSummary]) -> std::collections::HashMap<&str, &IssueSummary> {
    summaries
        .iter()
        .map(|summary| (summary.id.as_str(), summary))
        .collect()
}

fn dependency_issue_json(
    summaries: &std::collections::HashMap<&str, &IssueSummary>,
    id: &str,
    kind: &str,
) -> Option<Value> {
    summaries
        .get(id)
        .map(|summary| dependency_summary_json_value(summary, kind))
}

fn insert_dep_edge_fields(map: &mut Map<String, Value>, edge: &DepEdge) {
    map.insert("from_namespace".into(), json!(edge.from_namespace.as_str()));
    map.insert("from".into(), json!(edge.from.as_str()));
    map.insert("to_namespace".into(), json!(edge.to_namespace.as_str()));
    map.insert("to".into(), json!(edge.to.as_str()));
}

fn insert_opt_str(map: &mut Map<String, Value>, key: &str, value: Option<&str>) {
    if let Some(value) = value
        && !value.is_empty()
    {
        map.insert(key.into(), json!(value));
    }
}

fn insert_opt_stamp(map: &mut Map<String, Value>, key: &str, value: Option<&WriteStamp>) {
    if let Some(value) = value {
        map.insert(key.into(), json!(stamp_to_rfc3339(value)));
    }
}

fn insert_opt_wall_clock(map: &mut Map<String, Value>, key: &str, value: Option<WallClock>) {
    if let Some(value) = value {
        map.insert(key.into(), json!(wall_clock_to_rfc3339(value)));
    }
}

fn stamp_to_rfc3339(stamp: &WriteStamp) -> String {
    wall_ms_to_rfc3339(stamp.wall_ms)
}

fn wall_clock_to_rfc3339(clock: WallClock) -> String {
    wall_ms_to_rfc3339(clock.0)
}

fn wall_ms_to_rfc3339(ms: u64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH)
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

pub fn dep_kind_to_go_wire(kind: &str) -> String {
    match kind {
        "parent" => "parent-child".to_string(),
        other => other.replace('_', "-"),
    }
}
