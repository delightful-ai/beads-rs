use std::io::Write;
use std::path::PathBuf;

use clap::{Args, Subcommand};

use beads_api::{
    AdminFsckOutput, AdminStoreLockInfoOutput, AdminStoreUnlockOutput, FsckStatus,
    StoreLockMetaOutput, UnlockAction,
};
use beads_surface::ipc::payload::{AdminStoreFsckPayload, AdminStoreUnlockPayload};
use beads_surface::ipc::types::{AdminOp, Request};

use crate::OpError;
use crate::api::QueryResult;
use crate::core::StoreId;
use crate::daemon::ipc::{Response, ResponsePayload, send_request_no_autostart};
use crate::paths;
use crate::{Error, Result};

use super::super::print_json;

#[derive(Subcommand, Debug)]
pub enum StoreCmd {
    /// Unlock a store lock file.
    Unlock(StoreUnlockArgs),
    /// Offline WAL verification and repair.
    Fsck(StoreFsckArgs),
}

#[derive(Args, Debug)]
pub struct StoreUnlockArgs {
    /// Store ID to unlock.
    #[arg(long = "store-id", alias = "id", value_name = "STORE_ID")]
    pub store_id: String,

    /// Force unlock even if a daemon appears to be running.
    #[arg(long)]
    pub force: bool,
}

#[derive(Args, Debug)]
pub struct StoreFsckArgs {
    /// Store ID to check.
    #[arg(long = "store-id", alias = "id", value_name = "STORE_ID")]
    pub store_id: String,

    /// Apply safe repairs (tail truncation, quarantine, wal.sqlite rebuild).
    #[arg(long)]
    pub repair: bool,
}

pub(crate) fn handle(json: bool, cmd: StoreCmd) -> Result<()> {
    match cmd {
        StoreCmd::Unlock(args) => handle_unlock(json, args),
        StoreCmd::Fsck(args) => handle_fsck(json, args),
    }
}

// =============================================================================
// Fsck
// =============================================================================

fn handle_fsck(json: bool, args: StoreFsckArgs) -> Result<()> {
    let store_id = StoreId::parse_str(&args.store_id)?;

    // Try IPC first; fall back to offline if daemon isn't running.
    let req = Request::Admin(AdminOp::StoreFsck {
        payload: AdminStoreFsckPayload {
            store_id,
            repair: args.repair,
        },
    });
    match send_request_no_autostart(&req) {
        Ok(Response::Ok {
            ok: ResponsePayload::Query(QueryResult::AdminFsck(output)),
        }) => {
            if json {
                return print_json(&output);
            }
            write_stdout(&render_admin_fsck(&output))?;
            return Ok(());
        }
        Ok(Response::Err { err }) => {
            return Err(Error::Op(OpError::InvalidRequest {
                field: Some("store-id".into()),
                reason: err.message,
            }));
        }
        Ok(other) => {
            tracing::debug!(
                ?other,
                "unexpected daemon response for fsck, falling back to offline"
            );
        }
        Err(err) => {
            tracing::debug!(%err, "daemon not reachable for fsck, falling back to offline");
        }
    }

    // Offline fallback: call fsck directly, convert to API types for
    // consistent rendering and JSON output.
    let config = crate::config::load_or_init();
    let options = crate::daemon::wal::fsck::FsckOptions::new(args.repair, config.limits);
    let report = crate::daemon::wal::fsck::fsck_store(store_id, options).map_err(|err| {
        Error::Op(OpError::InvalidRequest {
            field: Some("store-id".into()),
            reason: err.to_string(),
        })
    })?;
    let output = crate::daemon::admin::fsck_report_to_output(report);

    if json {
        return print_json(&output);
    }
    write_stdout(&render_admin_fsck(&output))?;
    Ok(())
}

pub fn render_admin_fsck(output: &AdminFsckOutput) -> String {
    let mut out = String::new();
    out.push_str("Store fsck:\n");
    out.push_str(&format!("  store_id: {}\n", output.store_id));
    out.push_str(&format!("  checked_at_ms: {}\n", output.checked_at_ms));
    out.push_str(&format!(
        "  stats: namespaces={} segments={} records={}\n",
        output.stats.namespaces, output.stats.segments, output.stats.records
    ));
    out.push_str(&format!(
        "  summary: risk={} safe_to_accept_writes={} safe_to_prune_wal={} safe_to_rebuild_index={}\n",
        output.summary.risk,
        output.summary.safe_to_accept_writes,
        output.summary.safe_to_prune_wal,
        output.summary.safe_to_rebuild_index
    ));

    if !output.repairs.is_empty() {
        out.push_str("  repairs:\n");
        for repair in &output.repairs {
            let path = repair
                .path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "none".to_string());
            out.push_str(&format!(
                "    - {}: {} ({path})\n",
                repair.kind, repair.detail
            ));
        }
    }

    out.push_str("  checks:\n");
    for check in &output.checks {
        out.push_str(&format!(
            "    - {}: {} (severity={}, issues={})\n",
            check.id,
            check.status,
            check.severity,
            check.evidence.len()
        ));
        if check.status != FsckStatus::Pass {
            for evidence in &check.evidence {
                let path = evidence
                    .path
                    .as_ref()
                    .map(|p| format!(" path={}", p.display()))
                    .unwrap_or_default();
                let namespace = evidence
                    .namespace
                    .as_ref()
                    .map(|ns| format!(" namespace={}", ns.as_str()))
                    .unwrap_or_default();
                let origin = evidence
                    .origin
                    .as_ref()
                    .map(|id| format!(" origin={id}"))
                    .unwrap_or_default();
                let seq = evidence
                    .seq
                    .map(|seq| format!(" seq={seq}"))
                    .unwrap_or_default();
                let offset = evidence
                    .offset
                    .map(|offset| format!(" offset={offset}"))
                    .unwrap_or_default();
                out.push_str(&format!(
                    "      * {}: {}{path}{namespace}{origin}{seq}{offset}\n",
                    evidence.code, evidence.message
                ));
            }
            if !check.suggested_actions.is_empty() {
                out.push_str("      actions:\n");
                for action in &check.suggested_actions {
                    out.push_str(&format!("        - {action}\n"));
                }
            }
        }
    }
    out
}

// =============================================================================
// Unlock
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PidState {
    Missing,
    Alive,
    Unknown,
}

fn handle_unlock(json: bool, args: StoreUnlockArgs) -> Result<()> {
    let store_id = StoreId::parse_str(&args.store_id)?;

    // Try IPC first; fall back to offline if daemon isn't running.
    let req = Request::Admin(AdminOp::StoreUnlock {
        payload: AdminStoreUnlockPayload {
            store_id,
            force: args.force,
        },
    });
    match send_request_no_autostart(&req) {
        Ok(Response::Ok {
            ok: ResponsePayload::Query(QueryResult::AdminStoreUnlock(output)),
        }) => {
            if json {
                return print_json(&output);
            }
            write_stdout(&render_admin_store_unlock(&output))?;
            return Ok(());
        }
        Ok(Response::Err { err }) => {
            // Extract field from details when the daemon returns an InvalidRequest
            // (e.g. --force required). For other error types (StoreRuntime, etc.)
            // propagate the message without assuming a field.
            let field = if err.code.as_str() == "invalid_request" {
                err.details
                    .as_ref()
                    .and_then(|d| d.get("field"))
                    .and_then(|v| v.as_str())
                    .map(String::from)
            } else {
                None
            };
            return Err(Error::Op(OpError::InvalidRequest {
                field,
                reason: err.message,
            }));
        }
        Ok(other) => {
            tracing::debug!(
                ?other,
                "unexpected daemon response for unlock, falling back to offline"
            );
        }
        Err(err) => {
            tracing::debug!(%err, "daemon not reachable for unlock, falling back to offline");
        }
    }

    // Offline fallback: do the unlock directly, convert to API types
    // for consistent JSON output regardless of online/offline path.
    let daemon_pid = daemon_pid();
    let report = unlock_store_offline(store_id, args.force, daemon_pid)?;

    if let OfflineUnlockAction::RequireForce { reason } = report.result {
        return Err(Error::Op(OpError::InvalidRequest {
            field: Some("force".into()),
            reason: format!("lock appears active ({})", reason.as_str()),
        }));
    }

    let output = offline_unlock_to_api(&report);
    if json {
        return print_json(&output);
    }
    write_stdout(&render_admin_store_unlock(&output))?;
    Ok(())
}

pub fn render_admin_store_unlock(output: &AdminStoreUnlockOutput) -> String {
    let mut out = String::new();
    out.push_str("Store lock:\n");
    out.push_str(&format!("  store_id: {}\n", output.store_id));
    out.push_str(&format!("  lock_path: {}\n", output.lock_path.display()));
    render_lock_meta_output(&mut out, output.meta.as_ref());
    if let Some(daemon_pid) = output.daemon_pid {
        out.push_str(&format!("  daemon_pid: {daemon_pid}\n"));
    }
    out.push_str(&format!("  action: {}\n", output.action));
    out
}

// =============================================================================
// Lock info
// =============================================================================

pub fn render_admin_store_lock_info(output: &AdminStoreLockInfoOutput) -> String {
    let mut out = String::new();
    out.push_str("Store lock:\n");
    out.push_str(&format!("  store_id: {}\n", output.store_id));
    out.push_str(&format!("  lock_path: {}\n", output.lock_path.display()));
    render_lock_meta_output(&mut out, output.meta.as_ref());
    out
}

fn render_lock_meta_output(out: &mut String, meta: Option<&StoreLockMetaOutput>) {
    if let Some(meta) = meta {
        out.push_str(&format!("  replica_id: {}\n", meta.replica_id));
        out.push_str(&format!("  pid: {}\n", meta.pid));
        out.push_str(&format!("  started_at_ms: {}\n", meta.started_at_ms));
        out.push_str(&format!("  daemon_version: {}\n", meta.daemon_version));
        if let Some(heartbeat) = meta.last_heartbeat_ms {
            out.push_str(&format!("  last_heartbeat_ms: {heartbeat}\n"));
        } else {
            out.push_str("  last_heartbeat_ms: none\n");
        }
    } else {
        out.push_str("  meta: none\n");
    }
}

// =============================================================================
// Offline unlock logic (fallback when daemon not running)
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnlockReason {
    PidMissing,
    PidReuse,
    LiveDaemon,
    PidUnknown,
}

impl UnlockReason {
    fn as_str(self) -> &'static str {
        match self {
            UnlockReason::PidMissing => "pid_missing",
            UnlockReason::PidReuse => "pid_reuse",
            UnlockReason::LiveDaemon => "live_daemon",
            UnlockReason::PidUnknown => "pid_unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OfflineUnlockAction {
    NoLock,
    Removed {
        forced: bool,
        reason: UnlockReason,
        removed: bool,
    },
    RequireForce {
        reason: UnlockReason,
    },
}

impl OfflineUnlockAction {
    fn describe(&self) -> String {
        match self {
            OfflineUnlockAction::NoLock => "no lock file found".to_string(),
            OfflineUnlockAction::Removed {
                forced,
                reason,
                removed,
            } => {
                let mut out = if *removed {
                    "removed".to_string()
                } else {
                    "already removed".to_string()
                };
                if *forced {
                    out.push_str(" (forced)");
                }
                out.push_str(&format!(" [reason={}]", reason.as_str()));
                out
            }
            OfflineUnlockAction::RequireForce { reason } => {
                format!("requires --force [reason={}]", reason.as_str())
            }
        }
    }
}

#[derive(Debug)]
struct OfflineUnlockReport {
    store_id: StoreId,
    lock_path: PathBuf,
    meta: Option<crate::daemon::store_lock::StoreLockMeta>,
    daemon_pid: Option<u32>,
    result: OfflineUnlockAction,
}

fn unlock_store_offline(
    store_id: StoreId,
    force: bool,
    daemon_pid: Option<u32>,
) -> Result<OfflineUnlockReport> {
    unlock_store_with(store_id, force, daemon_pid, pid_check)
}

fn unlock_store_with<F>(
    store_id: StoreId,
    force: bool,
    daemon_pid: Option<u32>,
    check_pid: F,
) -> Result<OfflineUnlockReport>
where
    F: FnOnce(u32) -> PidState,
{
    let lock_path = paths::store_lock_path(store_id);
    let meta = crate::daemon::store_lock::read_lock_meta(store_id).map_err(store_lock_error)?;
    let Some(meta) = meta else {
        return Ok(OfflineUnlockReport {
            store_id,
            lock_path,
            meta: None,
            daemon_pid,
            result: OfflineUnlockAction::NoLock,
        });
    };

    let pid_state = check_pid(meta.pid);
    let mut result = decide_unlock(pid_state, daemon_pid, meta.pid, force);
    if let OfflineUnlockAction::Removed { removed, .. } = &mut result {
        let removed_flag =
            crate::daemon::store_lock::remove_lock_file(store_id).map_err(store_lock_error)?;
        *removed = removed_flag;
        tracing::info!(
            store_id = %store_id,
            lock_path = %lock_path.display(),
            pid = meta.pid,
            forced = matches!(&result, OfflineUnlockAction::Removed { forced: true, .. }),
            reason = %result.describe(),
            "store lock removed"
        );
    }

    Ok(OfflineUnlockReport {
        store_id,
        lock_path,
        meta: Some(meta),
        daemon_pid,
        result,
    })
}

fn decide_unlock(
    pid_state: PidState,
    daemon_pid: Option<u32>,
    lock_pid: u32,
    force: bool,
) -> OfflineUnlockAction {
    match pid_state {
        PidState::Missing => OfflineUnlockAction::Removed {
            forced: false,
            reason: UnlockReason::PidMissing,
            removed: false,
        },
        PidState::Alive => {
            if daemon_pid == Some(lock_pid) {
                if force {
                    OfflineUnlockAction::Removed {
                        forced: true,
                        reason: UnlockReason::LiveDaemon,
                        removed: false,
                    }
                } else {
                    OfflineUnlockAction::RequireForce {
                        reason: UnlockReason::LiveDaemon,
                    }
                }
            } else {
                OfflineUnlockAction::Removed {
                    forced: false,
                    reason: UnlockReason::PidReuse,
                    removed: false,
                }
            }
        }
        PidState::Unknown => {
            if force {
                OfflineUnlockAction::Removed {
                    forced: true,
                    reason: UnlockReason::PidUnknown,
                    removed: false,
                }
            } else {
                OfflineUnlockAction::RequireForce {
                    reason: UnlockReason::PidUnknown,
                }
            }
        }
    }
}

fn daemon_pid() -> Option<u32> {
    let resp = send_request_no_autostart(&Request::Ping).ok()?;
    match resp {
        Response::Ok {
            ok: ResponsePayload::Query(QueryResult::DaemonInfo(info)),
        } => Some(info.pid),
        _ => None,
    }
}

fn pid_check(pid: u32) -> PidState {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let nix_pid = Pid::from_raw(pid as i32);
    match kill(nix_pid, None) {
        Ok(()) | Err(Errno::EPERM) => PidState::Alive,
        Err(Errno::ESRCH) => PidState::Missing,
        Err(e) => {
            tracing::debug!(%e, pid, "pid check returned unexpected error");
            PidState::Unknown
        }
    }
}

fn offline_unlock_to_api(report: &OfflineUnlockReport) -> AdminStoreUnlockOutput {
    let action = match report.result {
        OfflineUnlockAction::NoLock => UnlockAction::NoLock,
        OfflineUnlockAction::Removed { forced: true, .. } => UnlockAction::RemovedForced,
        OfflineUnlockAction::Removed { forced: false, .. } => UnlockAction::RemovedStale,
        OfflineUnlockAction::RequireForce { .. } => {
            unreachable!("RequireForce should be handled before conversion")
        }
    };
    AdminStoreUnlockOutput {
        store_id: report.store_id,
        lock_path: report.lock_path.clone(),
        meta: report.meta.as_ref().map(|m| StoreLockMetaOutput {
            store_id: m.store_id,
            replica_id: m.replica_id,
            pid: m.pid,
            started_at_ms: m.started_at_ms,
            daemon_version: m.daemon_version.clone(),
            last_heartbeat_ms: m.last_heartbeat_ms,
        }),
        daemon_pid: report.daemon_pid,
        action,
    }
}

fn store_lock_error(err: crate::daemon::store_lock::StoreLockError) -> Error {
    Error::Op(OpError::from(
        crate::daemon::store_runtime::StoreRuntimeError::Lock(err),
    ))
}

fn write_stdout(text: &str) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    if let Err(e) = writeln!(stdout, "{text}")
        && e.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(crate::daemon::IpcError::from(e).into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{NamespaceId, ReplicaId};
    use crate::daemon::wal::fsck::{
        FsckCheck as DaemonFsckCheck, FsckCheckId as DaemonFsckCheckId,
        FsckEvidence as DaemonFsckEvidence, FsckEvidenceCode as DaemonFsckEvidenceCode,
        FsckRepair as DaemonFsckRepair, FsckRepairKind as DaemonFsckRepairKind, FsckReport,
        FsckRisk as DaemonFsckRisk, FsckSeverity as DaemonFsckSeverity,
        FsckStats as DaemonFsckStats, FsckStatus as DaemonFsckStatus,
        FsckSummary as DaemonFsckSummary,
    };
    use std::path::Path;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use uuid::Uuid;

    #[test]
    fn unlock_decision_stale_pid_removes() {
        let action = decide_unlock(PidState::Missing, None, 42, false);
        assert!(matches!(
            action,
            OfflineUnlockAction::Removed {
                forced: false,
                reason: UnlockReason::PidMissing,
                ..
            }
        ));
    }

    #[test]
    fn unlock_decision_live_daemon_requires_force() {
        let action = decide_unlock(PidState::Alive, Some(42), 42, false);
        assert!(matches!(
            action,
            OfflineUnlockAction::RequireForce {
                reason: UnlockReason::LiveDaemon
            }
        ));
    }

    #[test]
    fn unlock_removes_stale_lock_file() {
        with_test_data_dir(|_temp| {
            let store_id = StoreId::new(Uuid::from_bytes([1u8; 16]));
            let lock_path = paths::store_lock_path(store_id);
            let meta = crate::daemon::store_lock::StoreLockMeta {
                store_id,
                replica_id: crate::core::ReplicaId::new(Uuid::from_bytes([2u8; 16])),
                pid: 99_999,
                started_at_ms: 1,
                daemon_version: "test".to_string(),
                last_heartbeat_ms: Some(2),
            };
            write_lock_meta(&lock_path, &meta);
            let report = unlock_store_offline(store_id, false, None).unwrap();
            assert!(!lock_path.exists());
            assert!(matches!(report.result, OfflineUnlockAction::Removed { .. }));
        });
    }

    #[test]
    fn unlock_requires_force_for_live_daemon() {
        with_test_data_dir(|_temp| {
            let store_id = StoreId::new(Uuid::from_bytes([3u8; 16]));
            let lock_path = paths::store_lock_path(store_id);
            let meta = crate::daemon::store_lock::StoreLockMeta {
                store_id,
                replica_id: crate::core::ReplicaId::new(Uuid::from_bytes([4u8; 16])),
                pid: 4242,
                started_at_ms: 1,
                daemon_version: "test".to_string(),
                last_heartbeat_ms: Some(2),
            };
            write_lock_meta(&lock_path, &meta);
            let report =
                unlock_store_with(store_id, false, Some(4242), |_| PidState::Alive).unwrap();
            assert!(lock_path.exists());
            assert!(matches!(
                report.result,
                OfflineUnlockAction::RequireForce {
                    reason: UnlockReason::LiveDaemon
                }
            ));
        });
    }

    fn sample_fsck_report() -> FsckReport {
        let store_id = StoreId::new(Uuid::from_bytes([9u8; 16]));
        let replica_id = ReplicaId::new(Uuid::from_bytes([7u8; 16]));
        FsckReport {
            store_id,
            checked_at_ms: 12345,
            stats: DaemonFsckStats {
                namespaces: 1,
                segments: 2,
                records: 3,
            },
            summary: DaemonFsckSummary {
                risk: DaemonFsckRisk::High,
                safe_to_accept_writes: false,
                safe_to_prune_wal: false,
                safe_to_rebuild_index: true,
            },
            repairs: vec![DaemonFsckRepair {
                kind: DaemonFsckRepairKind::TruncateTail,
                path: Some(PathBuf::from("/tmp/wal/segment-1")),
                detail: "truncated tail corruption".to_string(),
            }],
            checks: vec![
                DaemonFsckCheck {
                    id: DaemonFsckCheckId::SegmentFrames,
                    status: DaemonFsckStatus::Fail,
                    severity: DaemonFsckSeverity::High,
                    evidence: vec![DaemonFsckEvidence {
                        code: DaemonFsckEvidenceCode::FrameCrcMismatch,
                        message: "crc mismatch".to_string(),
                        path: Some(PathBuf::from("/tmp/wal/segment-1")),
                        namespace: Some(NamespaceId::core()),
                        origin: Some(replica_id),
                        seq: Some(5),
                        offset: Some(128),
                    }],
                    suggested_actions: vec![
                        "run `bd store fsck --repair` to truncate tail corruption".to_string(),
                    ],
                },
                DaemonFsckCheck {
                    id: DaemonFsckCheckId::IndexOffsets,
                    status: DaemonFsckStatus::Pass,
                    severity: DaemonFsckSeverity::Low,
                    evidence: Vec::new(),
                    suggested_actions: Vec::new(),
                },
            ],
        }
    }

    #[test]
    fn render_fsck_human_golden() {
        let report = sample_fsck_report();
        let api_output = crate::daemon::admin::fsck_report_to_output(report);
        let output = render_admin_fsck(&api_output);
        let expected = concat!(
            "Store fsck:\n",
            "  store_id: 09090909-0909-0909-0909-090909090909\n",
            "  checked_at_ms: 12345\n",
            "  stats: namespaces=1 segments=2 records=3\n",
            "  summary: risk=high safe_to_accept_writes=false safe_to_prune_wal=false safe_to_rebuild_index=true\n",
            "  repairs:\n",
            "    - truncate_tail: truncated tail corruption (/tmp/wal/segment-1)\n",
            "  checks:\n",
            "    - segment_frames: fail (severity=high, issues=1)\n",
            "      * frame_crc_mismatch: crc mismatch path=/tmp/wal/segment-1 namespace=core origin=07070707-0707-0707-0707-070707070707 seq=5 offset=128\n",
            "      actions:\n",
            "        - run `bd store fsck --repair` to truncate tail corruption\n",
            "    - index_offsets: pass (severity=low, issues=0)\n",
        );
        assert_eq!(output, expected);
    }

    fn with_test_data_dir<F>(f: F)
    where
        F: FnOnce(&TempDir),
    {
        let temp = TempDir::new().unwrap();
        let _override = paths::override_data_dir_for_tests(Some(temp.path().to_path_buf()));
        f(&temp);
    }

    fn write_lock_meta(path: &Path, meta: &crate::daemon::store_lock::StoreLockMeta) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let data = serde_json::to_vec(meta).unwrap();
        std::fs::write(path, data).unwrap();
    }
}
