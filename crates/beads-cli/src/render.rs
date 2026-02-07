use beads_api::{
    AdminCheckpointOutput, AdminDoctorOutput, AdminFingerprintOutput, AdminFlushOutput,
    AdminFsckOutput, AdminMaintenanceModeOutput, AdminMetricsOutput, AdminRebuildIndexOutput,
    AdminReloadLimitsOutput, AdminReloadPoliciesOutput, AdminReloadReplicationOutput,
    AdminRotateReplicaIdOutput, AdminScrubOutput, AdminStatusOutput, AdminStoreLockInfoOutput,
    AdminStoreUnlockOutput, BlockedIssue, CountResult, DaemonInfo, DeletedLookup, DepCycles,
    DepEdge, EpicStatus, Issue, IssueSummary, Note, QueryResult, ReadyResult, StatusOutput,
    Tombstone,
};
use beads_surface::OpResult;
use beads_surface::ipc::ResponsePayload;
use serde::Serialize;

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
    fn render_dep_tree(&self, root: &str, edges: &[DepEdge]) -> String;
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
        QueryResult::Issues(views) => renderer.render_issues(views),
        QueryResult::DepTree { root, edges } => renderer.render_dep_tree(root.as_str(), edges),
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
        return Err(beads_surface::ipc::IpcError::from(err));
    }
    Ok(())
}

pub fn print_json<T: Serialize>(value: &T) -> crate::Result<()> {
    use std::io::Write;
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, value).map_err(beads_surface::ipc::IpcError::from)?;
    if let Err(err) = writeln!(stdout)
        && err.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(beads_surface::ipc::IpcError::from(err));
    }
    Ok(())
}
