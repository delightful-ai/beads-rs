use beads_api::{AdminFsckOutput, AdminStoreUnlockOutput};
use beads_core::{IntoErrorPayload, StoreId};

use crate::OpError;

pub(crate) fn offline_store_fsck_output(
    store_id: StoreId,
    repair: bool,
) -> Result<AdminFsckOutput, OpError> {
    crate::daemon::admin::offline_store_fsck_output(store_id, repair).map_err(map_daemon_op)
}

pub(crate) fn offline_store_unlock_output(
    store_id: StoreId,
    force: bool,
    daemon_pid: Option<u32>,
) -> Result<AdminStoreUnlockOutput, OpError> {
    crate::daemon::admin::offline_store_unlock_output(store_id, force, daemon_pid)
        .map_err(map_daemon_op)
}

fn map_daemon_op(err: crate::daemon::OpError) -> OpError {
    match err {
        crate::daemon::OpError::ValidationFailed { field, reason } => {
            OpError::ValidationFailed { field, reason }
        }
        crate::daemon::OpError::InvalidRequest { field, reason } => {
            OpError::InvalidRequest { field, reason }
        }
        other => {
            let message = other.to_string();
            let transience = other.transience();
            let effect = other.effect();
            let payload = other.into_error_payload();
            OpError::Daemon {
                message,
                payload,
                transience,
                effect,
            }
        }
    }
}
