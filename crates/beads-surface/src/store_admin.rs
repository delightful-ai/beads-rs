use beads_api::{AdminFsckOutput, AdminStoreLockInfoOutput, AdminStoreUnlockOutput, QueryResult};
use beads_core::{ErrorPayload, StoreId};
use thiserror::Error;

use crate::ipc::payload::{
    AdminStoreFsckPayload, AdminStoreLockInfoPayload, AdminStoreUnlockPayload,
};
use crate::ipc::types::{AdminOp, Request};
use crate::ipc::{IpcError, Response, ResponsePayload, send_request_no_autostart};

/// Result of trying a no-autostart daemon admin call.
#[derive(Debug)]
pub enum StoreAdminCall<T> {
    /// The daemon handled the request and returned a typed output.
    Output(T),
    /// Daemon was unavailable or returned an unexpected shape; caller should run offline fallback.
    OfflineFallback,
}

#[derive(Debug, Error)]
pub enum StoreAdminCallError {
    #[error(transparent)]
    Ipc(#[from] IpcError),

    #[error("invalid request: {reason}")]
    InvalidRequest {
        field: Option<String>,
        reason: String,
    },
}

/// Try `admin store fsck` via daemon without autostart.
///
/// Returns [`StoreAdminCall::OfflineFallback`] when daemon is unavailable or response
/// shape is unexpected so the caller can run offline logic.
pub fn call_store_fsck_no_autostart(
    store_id: StoreId,
    repair: bool,
) -> Result<StoreAdminCall<AdminFsckOutput>, StoreAdminCallError> {
    call_store_fsck_with(store_id, repair, send_request_no_autostart)
}

fn call_store_fsck_with<F>(
    store_id: StoreId,
    repair: bool,
    send: F,
) -> Result<StoreAdminCall<AdminFsckOutput>, StoreAdminCallError>
where
    F: FnOnce(&Request) -> Result<Response, IpcError>,
{
    let req = Request::Admin(AdminOp::StoreFsck {
        payload: AdminStoreFsckPayload { store_id, repair },
    });
    call_store_admin_with(req, send, "fsck", |query| {
        if let QueryResult::AdminFsck(output) = query {
            Some(output)
        } else {
            None
        }
    })
}

/// Try `admin store lock-info` via daemon without autostart.
///
/// Returns [`StoreAdminCall::OfflineFallback`] when daemon is unavailable or response
/// shape is unexpected so the caller can run offline logic.
pub fn call_store_lock_info_no_autostart(
    store_id: StoreId,
) -> Result<StoreAdminCall<AdminStoreLockInfoOutput>, StoreAdminCallError> {
    call_store_lock_info_with(store_id, send_request_no_autostart)
}

fn call_store_lock_info_with<F>(
    store_id: StoreId,
    send: F,
) -> Result<StoreAdminCall<AdminStoreLockInfoOutput>, StoreAdminCallError>
where
    F: FnOnce(&Request) -> Result<Response, IpcError>,
{
    let req = Request::Admin(AdminOp::StoreLockInfo {
        payload: AdminStoreLockInfoPayload { store_id },
    });
    call_store_admin_with(req, send, "lock_info", |query| {
        if let QueryResult::AdminStoreLockInfo(output) = query {
            Some(output)
        } else {
            None
        }
    })
}

/// Try `admin store unlock` via daemon without autostart.
///
/// Returns [`StoreAdminCall::OfflineFallback`] when daemon is unavailable or response
/// shape is unexpected so the caller can run offline logic.
pub fn call_store_unlock_no_autostart(
    store_id: StoreId,
    force: bool,
) -> Result<StoreAdminCall<AdminStoreUnlockOutput>, StoreAdminCallError> {
    call_store_unlock_with(store_id, force, send_request_no_autostart)
}

fn call_store_unlock_with<F>(
    store_id: StoreId,
    force: bool,
    send: F,
) -> Result<StoreAdminCall<AdminStoreUnlockOutput>, StoreAdminCallError>
where
    F: FnOnce(&Request) -> Result<Response, IpcError>,
{
    let req = Request::Admin(AdminOp::StoreUnlock {
        payload: AdminStoreUnlockPayload { store_id, force },
    });
    call_store_admin_with(req, send, "unlock", |query| {
        if let QueryResult::AdminStoreUnlock(output) = query {
            Some(output)
        } else {
            None
        }
    })
}

/// Best-effort daemon pid probe without autostart.
pub fn daemon_pid_no_autostart() -> Option<u32> {
    daemon_pid_from_ping_with(send_request_no_autostart)
}

fn daemon_pid_from_ping_with<F>(send: F) -> Option<u32>
where
    F: FnOnce(&Request) -> Result<Response, IpcError>,
{
    let resp = send(&Request::Ping).ok()?;
    if let Response::Ok {
        ok: ResponsePayload::Query(QueryResult::DaemonInfo(info)),
    } = resp
    {
        Some(info.pid)
    } else {
        None
    }
}

fn call_store_admin_with<T, F, M>(
    req: Request,
    send: F,
    op_name: &'static str,
    map_query: M,
) -> Result<StoreAdminCall<T>, StoreAdminCallError>
where
    F: FnOnce(&Request) -> Result<Response, IpcError>,
    M: FnOnce(QueryResult) -> Option<T>,
{
    match send(&req) {
        Ok(Response::Ok {
            ok: ResponsePayload::Query(query),
        }) => {
            if let Some(output) = map_query(query) {
                return Ok(StoreAdminCall::Output(output));
            }
            tracing::debug!(
                ?req,
                op_name,
                "unexpected daemon query result for store admin op, falling back to offline"
            );
            Ok(StoreAdminCall::OfflineFallback)
        }
        Ok(Response::Err { err }) => Err(StoreAdminCallError::InvalidRequest {
            field: invalid_request_field(&err),
            reason: err.message,
        }),
        Ok(other) => {
            tracing::debug!(
                ?other,
                op_name,
                "unexpected daemon response for store admin op, falling back to offline"
            );
            Ok(StoreAdminCall::OfflineFallback)
        }
        Err(err) => {
            tracing::debug!(
                %err,
                op_name,
                "daemon not reachable for store admin op, falling back to offline"
            );
            Ok(StoreAdminCall::OfflineFallback)
        }
    }
}

fn invalid_request_field(err: &ErrorPayload) -> Option<String> {
    if err.code.as_str() != "invalid_request" {
        return None;
    }
    err.details
        .as_ref()
        .and_then(|details| details.get("field"))
        .and_then(|field| field.as_str())
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use beads_api::{
        AdminFsckOutput, DaemonInfo, FsckCheck, FsckCheckId, FsckRisk, FsckSeverity, FsckStats,
        FsckStatus, FsckSummary, StoreLockMetaOutput, UnlockAction,
    };
    use beads_core::{ErrorCode, ReplicaId};
    use serde_json::json;

    fn sample_store_id() -> StoreId {
        StoreId::parse_str("03030303-0303-0303-0303-030303030303").expect("valid store id")
    }

    fn sample_replica_id() -> ReplicaId {
        ReplicaId::parse_str("05050505-0505-0505-0505-050505050505").expect("valid replica id")
    }

    fn sample_fsck_output(store_id: StoreId) -> AdminFsckOutput {
        AdminFsckOutput {
            store_id,
            checked_at_ms: 42,
            stats: FsckStats {
                namespaces: 1,
                segments: 2,
                records: 3,
            },
            checks: vec![FsckCheck {
                id: FsckCheckId::IndexOffsets,
                status: FsckStatus::Pass,
                severity: FsckSeverity::Low,
                evidence: Vec::new(),
                suggested_actions: Vec::new(),
            }],
            summary: FsckSummary {
                risk: FsckRisk::Low,
                safe_to_accept_writes: true,
                safe_to_prune_wal: true,
                safe_to_rebuild_index: true,
            },
            repairs: Vec::new(),
        }
    }

    fn sample_lock_meta(store_id: StoreId) -> StoreLockMetaOutput {
        StoreLockMetaOutput {
            store_id,
            replica_id: sample_replica_id(),
            pid: 77,
            started_at_ms: 99,
            daemon_version: "0.1.0-test".to_string(),
            last_heartbeat_ms: Some(111),
        }
    }

    #[test]
    fn fsck_call_returns_typed_output() {
        let store_id = sample_store_id();
        let call = call_store_fsck_with(store_id, true, |req| {
            match req {
                Request::Admin(AdminOp::StoreFsck { payload }) => {
                    assert_eq!(payload.store_id, store_id);
                    assert!(payload.repair);
                }
                other => panic!("expected StoreFsck request, got {other:?}"),
            }
            Ok(Response::Ok {
                ok: ResponsePayload::Query(QueryResult::AdminFsck(sample_fsck_output(store_id))),
            })
        })
        .expect("call succeeds");

        match call {
            StoreAdminCall::Output(output) => {
                assert_eq!(output.store_id, store_id);
                assert_eq!(output.summary.risk, FsckRisk::Low);
                assert_eq!(output.checks.len(), 1);
                assert_eq!(output.checks[0].status, FsckStatus::Pass);
            }
            StoreAdminCall::OfflineFallback => panic!("expected daemon output"),
        }
    }

    #[test]
    fn fsck_call_maps_invalid_request_error_with_field() {
        let store_id = sample_store_id();
        let err = call_store_fsck_with(store_id, false, |_| {
            Ok(Response::Err {
                err: ErrorPayload::new(
                    ErrorCode::parse("invalid_request"),
                    "store_id malformed",
                    false,
                )
                .with_details(json!({ "field": "store_id" })),
            })
        })
        .expect_err("invalid request must map to typed error");

        assert!(matches!(
            err,
            StoreAdminCallError::InvalidRequest {
                field: Some(field),
                reason
            } if field == "store_id" && reason == "store_id malformed"
        ));
    }

    #[test]
    fn fsck_call_falls_back_on_unexpected_response_shape() {
        let store_id = sample_store_id();
        let call = call_store_fsck_with(store_id, false, |_| {
            Ok(Response::Ok {
                ok: ResponsePayload::synced(),
            })
        })
        .expect("fallback path is not an error");

        assert!(matches!(call, StoreAdminCall::OfflineFallback));
    }

    #[test]
    fn lock_info_call_falls_back_on_daemon_unavailable() {
        let store_id = sample_store_id();
        let call = call_store_lock_info_with(store_id, |req| {
            assert!(matches!(req, Request::Admin(AdminOp::StoreLockInfo { .. })));
            Err(IpcError::DaemonUnavailable(
                "daemon not running".to_string(),
            ))
        })
        .expect("offline fallback is expected");

        assert!(matches!(call, StoreAdminCall::OfflineFallback));
    }

    #[test]
    fn lock_info_call_returns_typed_output() {
        let store_id = sample_store_id();
        let call = call_store_lock_info_with(store_id, |_| {
            Ok(Response::Ok {
                ok: ResponsePayload::Query(QueryResult::AdminStoreLockInfo(
                    AdminStoreLockInfoOutput {
                        store_id,
                        lock_path: PathBuf::from("/tmp/beads/store.lock"),
                        meta: Some(sample_lock_meta(store_id)),
                    },
                )),
            })
        })
        .expect("call succeeds");

        match call {
            StoreAdminCall::Output(output) => {
                assert_eq!(output.store_id, store_id);
                assert_eq!(output.meta.expect("meta").pid, 77);
            }
            StoreAdminCall::OfflineFallback => panic!("expected lock info output"),
        }
    }

    #[test]
    fn unlock_call_returns_typed_output() {
        let store_id = sample_store_id();
        let call = call_store_unlock_with(store_id, true, |req| {
            match req {
                Request::Admin(AdminOp::StoreUnlock { payload }) => {
                    assert_eq!(payload.store_id, store_id);
                    assert!(payload.force);
                }
                other => panic!("expected StoreUnlock request, got {other:?}"),
            }
            Ok(Response::Ok {
                ok: ResponsePayload::Query(QueryResult::AdminStoreUnlock(AdminStoreUnlockOutput {
                    store_id,
                    lock_path: PathBuf::from("/tmp/beads/store.lock"),
                    meta: Some(sample_lock_meta(store_id)),
                    daemon_pid: Some(77),
                    action: UnlockAction::RemovedForced,
                })),
            })
        })
        .expect("call succeeds");

        match call {
            StoreAdminCall::Output(output) => {
                assert_eq!(output.store_id, store_id);
                assert_eq!(output.action, UnlockAction::RemovedForced);
                assert_eq!(output.daemon_pid, Some(77));
            }
            StoreAdminCall::OfflineFallback => panic!("expected unlock output"),
        }
    }

    #[test]
    fn unlock_call_omits_field_when_code_is_not_invalid_request() {
        let store_id = sample_store_id();
        let err = call_store_unlock_with(store_id, false, |_| {
            Ok(Response::Err {
                err: ErrorPayload::new(ErrorCode::parse("io_error"), "other failure", false)
                    .with_details(json!({ "field": "store_id" })),
            })
        })
        .expect_err("error must propagate");

        assert!(matches!(
            err,
            StoreAdminCallError::InvalidRequest { field: None, reason } if reason == "other failure"
        ));
    }

    #[test]
    fn daemon_pid_probe_returns_pid_for_typed_ping_response() {
        let pid = daemon_pid_from_ping_with(|req| {
            assert!(matches!(req, Request::Ping));
            Ok(Response::Ok {
                ok: ResponsePayload::Query(QueryResult::DaemonInfo(DaemonInfo {
                    version: "0.1.0-test".to_string(),
                    protocol_version: crate::ipc::IPC_PROTOCOL_VERSION,
                    pid: 4242,
                })),
            })
        });

        assert_eq!(pid, Some(4242));
    }

    #[test]
    fn daemon_pid_probe_returns_none_for_non_daemon_info_response() {
        let store_id = sample_store_id();
        let pid = daemon_pid_from_ping_with(|_| {
            Ok(Response::Ok {
                ok: ResponsePayload::Query(QueryResult::AdminFsck(sample_fsck_output(store_id))),
            })
        });

        assert_eq!(pid, None);
    }
}
