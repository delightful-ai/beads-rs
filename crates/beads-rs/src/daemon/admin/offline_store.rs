use super::*;

// =========================================================================
// Store-level admin ops (no repo context required)
// =========================================================================

impl Daemon {
    pub fn admin_store_fsck(&self, store_id: StoreId, repair: bool) -> Response {
        // Repair modifies WAL files (truncate, quarantine, rebuild index).
        // Reject when running inside the daemon to avoid races with live writes.
        // Users must stop the daemon first: `bd daemon stop && bd store fsck --repair`.
        if repair {
            return Response::err_from(OpError::InvalidRequest {
                field: Some("repair".into()),
                reason:
                    "fsck --repair cannot run while the daemon is active; stop the daemon first"
                        .to_string(),
            });
        }
        let output = match offline_store_fsck_output(store_id, false) {
            Ok(output) => output,
            Err(err) => return Response::err_from(err),
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminFsck(output)))
    }

    pub fn admin_store_lock_info(&self, store_id: StoreId) -> Response {
        let output = match offline_store_lock_info_output(store_id) {
            Ok(output) => output,
            Err(err) => return Response::err_from(err),
        };
        Response::ok(ResponsePayload::query(QueryResult::AdminStoreLockInfo(
            output,
        )))
    }

    pub fn admin_store_unlock(&self, store_id: StoreId, force: bool) -> Response {
        let lock_path = crate::paths::store_lock_path(store_id);
        let meta = match crate::daemon::store::lock::read_lock_meta(store_id) {
            Ok(meta) => meta,
            Err(err) => {
                return Response::err_from(OpError::StoreRuntime(Box::new(
                    StoreRuntimeError::Lock(err),
                )));
            }
        };
        let our_pid = std::process::id();
        let Some(lock_meta) = meta else {
            return unlock_ok(store_id, lock_path, None, our_pid, UnlockAction::NoLock);
        };

        let lock_is_ours = lock_meta.pid == our_pid;

        // Check PID liveness for foreign locks to avoid removing an active lock.
        let lock_pid_alive = if lock_is_ours {
            true // We're running, so obviously alive.
        } else {
            pid_is_alive(lock_meta.pid)
        };

        // Active lock (ours or foreign alive PID) requires --force.
        if lock_pid_alive && !force {
            let reason = if lock_is_ours {
                "lock held by this daemon"
            } else {
                "lock PID is still alive"
            };
            return Response::err_from(OpError::InvalidRequest {
                field: Some("force".into()),
                reason: reason.to_string(),
            });
        }

        // Remove: either forced or stale (dead PID).
        if let Err(err) = crate::daemon::store::lock::remove_lock_file(store_id) {
            return Response::err_from(OpError::StoreRuntime(Box::new(StoreRuntimeError::Lock(
                err,
            ))));
        }

        let action = if !lock_pid_alive {
            UnlockAction::RemovedStale
        } else {
            UnlockAction::RemovedForced
        };
        unlock_ok(
            store_id,
            lock_path,
            Some(lock_meta_to_api(lock_meta)),
            our_pid,
            action,
        )
    }
}

pub(crate) fn offline_store_fsck_output(
    store_id: StoreId,
    repair: bool,
) -> Result<AdminFsckOutput, OpError> {
    let config = crate::config::load_or_init();
    let options = crate::daemon::wal::fsck::FsckOptions::new(repair, config.limits);
    let report = crate::daemon::wal::fsck::fsck_store(store_id, options).map_err(|err| {
        OpError::InvalidRequest {
            field: Some("store-id".into()),
            reason: err.to_string(),
        }
    })?;
    Ok(fsck_report_to_output(report))
}

pub(crate) fn offline_store_lock_info_output(
    store_id: StoreId,
) -> Result<AdminStoreLockInfoOutput, OpError> {
    let lock_path = crate::paths::store_lock_path(store_id);
    let meta = crate::daemon::store::lock::read_lock_meta(store_id).map_err(store_lock_op_error)?;
    Ok(AdminStoreLockInfoOutput {
        store_id,
        lock_path,
        meta: meta.map(lock_meta_to_api),
    })
}

pub(crate) fn offline_store_unlock_output(
    store_id: StoreId,
    force: bool,
    daemon_pid: Option<u32>,
) -> Result<AdminStoreUnlockOutput, OpError> {
    offline_store_unlock_with_pid_check(store_id, force, daemon_pid, pid_state_for_unlock)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OfflinePidState {
    Missing,
    Alive,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OfflineUnlockReason {
    PidMissing,
    PidReuse,
    LiveDaemon,
    PidUnknown,
}

impl OfflineUnlockReason {
    fn as_str(self) -> &'static str {
        match self {
            OfflineUnlockReason::PidMissing => "pid_missing",
            OfflineUnlockReason::PidReuse => "pid_reuse",
            OfflineUnlockReason::LiveDaemon => "live_daemon",
            OfflineUnlockReason::PidUnknown => "pid_unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OfflineUnlockAction {
    Removed {
        forced: bool,
        reason: OfflineUnlockReason,
        removed: bool,
    },
    RequireForce {
        reason: OfflineUnlockReason,
    },
}

impl OfflineUnlockAction {
    fn describe(self) -> String {
        match self {
            OfflineUnlockAction::Removed {
                forced,
                reason,
                removed,
            } => {
                let mut out = if removed {
                    "removed".to_string()
                } else {
                    "already removed".to_string()
                };
                if forced {
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

pub(super) fn offline_store_unlock_with_pid_check<F>(
    store_id: StoreId,
    force: bool,
    daemon_pid: Option<u32>,
    check_pid: F,
) -> Result<AdminStoreUnlockOutput, OpError>
where
    F: FnOnce(u32) -> OfflinePidState,
{
    let lock_path = crate::paths::store_lock_path(store_id);
    let meta = crate::daemon::store::lock::read_lock_meta(store_id).map_err(store_lock_op_error)?;
    let Some(meta) = meta else {
        return Ok(AdminStoreUnlockOutput {
            store_id,
            lock_path,
            meta: None,
            daemon_pid,
            action: UnlockAction::NoLock,
        });
    };

    let pid_state = check_pid(meta.pid);
    let mut action = decide_offline_unlock(pid_state, daemon_pid, meta.pid, force);
    let forced = matches!(action, OfflineUnlockAction::Removed { forced: true, .. });
    if let OfflineUnlockAction::Removed { removed, .. } = &mut action {
        *removed =
            crate::daemon::store::lock::remove_lock_file(store_id).map_err(store_lock_op_error)?;
        tracing::info!(
            store_id = %store_id,
            lock_path = %lock_path.display(),
            pid = meta.pid,
            forced,
            reason = %action.describe(),
            "store lock removed"
        );
    }

    match action {
        OfflineUnlockAction::RequireForce { reason } => Err(OpError::InvalidRequest {
            field: Some("force".into()),
            reason: format!("lock appears active ({})", reason.as_str()),
        }),
        OfflineUnlockAction::Removed { forced: true, .. } => Ok(AdminStoreUnlockOutput {
            store_id,
            lock_path,
            meta: Some(lock_meta_to_api(meta)),
            daemon_pid,
            action: UnlockAction::RemovedForced,
        }),
        OfflineUnlockAction::Removed { forced: false, .. } => Ok(AdminStoreUnlockOutput {
            store_id,
            lock_path,
            meta: Some(lock_meta_to_api(meta)),
            daemon_pid,
            action: UnlockAction::RemovedStale,
        }),
    }
}

fn decide_offline_unlock(
    pid_state: OfflinePidState,
    daemon_pid: Option<u32>,
    lock_pid: u32,
    force: bool,
) -> OfflineUnlockAction {
    match pid_state {
        OfflinePidState::Missing => OfflineUnlockAction::Removed {
            forced: false,
            reason: OfflineUnlockReason::PidMissing,
            removed: false,
        },
        OfflinePidState::Alive => {
            if daemon_pid == Some(lock_pid) {
                if force {
                    OfflineUnlockAction::Removed {
                        forced: true,
                        reason: OfflineUnlockReason::LiveDaemon,
                        removed: false,
                    }
                } else {
                    OfflineUnlockAction::RequireForce {
                        reason: OfflineUnlockReason::LiveDaemon,
                    }
                }
            } else {
                OfflineUnlockAction::Removed {
                    forced: false,
                    reason: OfflineUnlockReason::PidReuse,
                    removed: false,
                }
            }
        }
        OfflinePidState::Unknown => {
            if force {
                OfflineUnlockAction::Removed {
                    forced: true,
                    reason: OfflineUnlockReason::PidUnknown,
                    removed: false,
                }
            } else {
                OfflineUnlockAction::RequireForce {
                    reason: OfflineUnlockReason::PidUnknown,
                }
            }
        }
    }
}

fn pid_state_for_unlock(pid: u32) -> OfflinePidState {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;

    let nix_pid = Pid::from_raw(pid as i32);
    match kill(nix_pid, None) {
        Ok(()) | Err(Errno::EPERM) => OfflinePidState::Alive,
        Err(Errno::ESRCH) => OfflinePidState::Missing,
        Err(err) => {
            tracing::debug!(%err, pid, "pid check returned unexpected error");
            OfflinePidState::Unknown
        }
    }
}

fn store_lock_op_error(err: crate::daemon::store::lock::StoreLockError) -> OpError {
    OpError::StoreRuntime(Box::new(StoreRuntimeError::Lock(err)))
}

fn unlock_ok(
    store_id: StoreId,
    lock_path: PathBuf,
    meta: Option<StoreLockMetaOutput>,
    daemon_pid: u32,
    action: UnlockAction,
) -> Response {
    let output = AdminStoreUnlockOutput {
        store_id,
        lock_path,
        meta,
        daemon_pid: Some(daemon_pid),
        action,
    };
    Response::ok(ResponsePayload::query(QueryResult::AdminStoreUnlock(
        output,
    )))
}

/// Check if a PID is alive using `kill(pid, 0)`.
fn pid_is_alive(pid: u32) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    matches!(
        kill(Pid::from_raw(pid as i32), None),
        Ok(()) | Err(Errno::EPERM)
    )
}

fn lock_meta_to_api(meta: crate::daemon::store::lock::StoreLockMeta) -> StoreLockMetaOutput {
    StoreLockMetaOutput {
        store_id: meta.store_id,
        replica_id: meta.replica_id,
        pid: meta.pid,
        started_at_ms: meta.started_at_ms,
        daemon_version: meta.daemon_version,
        last_heartbeat_ms: meta.last_heartbeat_ms,
    }
}
