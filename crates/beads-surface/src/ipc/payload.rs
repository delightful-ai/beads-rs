use serde::{Deserialize, Serialize};

use beads_api::{AdminFingerprintMode, AdminFingerprintSample};
use beads_core::{BeadId, BeadType, BranchName, DepKind, NamespaceId, Priority, StoreId};

use crate::ops::BeadPatch;
use crate::query::Filters;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyPayload {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdPayload {
    pub id: BeadId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdsPayload {
    pub ids: Vec<BeadId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatePayload {
    #[serde(default)]
    pub id: Option<BeadId>,
    #[serde(default)]
    pub parent: Option<BeadId>,
    pub title: String,
    #[serde(rename = "type")]
    pub bead_type: BeadType,
    pub priority: Priority,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub design: Option<String>,
    #[serde(default)]
    pub acceptance_criteria: Option<String>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub external_ref: Option<String>,
    #[serde(default)]
    pub estimated_minutes: Option<u32>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdatePayload {
    pub id: BeadId,
    pub patch: BeadPatch,
    #[serde(default)]
    pub cas: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelsPayload {
    pub id: BeadId,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParentPayload {
    pub id: BeadId,
    #[serde(default)]
    pub parent: Option<BeadId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClosePayload {
    pub id: BeadId,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub on_branch: Option<BranchName>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletePayload {
    pub id: BeadId,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepPayload {
    pub from: BeadId,
    pub to: BeadId,
    pub kind: DepKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddNotePayload {
    pub id: BeadId,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimPayload {
    pub id: BeadId,
    #[serde(default = "super::default_lease_secs")]
    pub lease_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeasePayload {
    pub id: BeadId,
    pub lease_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListPayload {
    #[serde(default)]
    pub filters: Filters,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyPayload {
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StalePayload {
    #[serde(default)]
    pub days: u32,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountPayload {
    #[serde(default)]
    pub filters: Filters,
    #[serde(default)]
    pub group_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletedPayload {
    #[serde(default)]
    pub since_ms: Option<u64>,
    #[serde(default)]
    pub id: Option<BeadId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpicStatusPayload {
    #[serde(default)]
    pub eligible_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminDoctorPayload {
    #[serde(default)]
    pub max_records_per_namespace: Option<u64>,
    #[serde(default)]
    pub verify_checkpoint_cache: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminScrubPayload {
    #[serde(default)]
    pub max_records_per_namespace: Option<u64>,
    #[serde(default)]
    pub verify_checkpoint_cache: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminFlushPayload {
    #[serde(default)]
    pub namespace: Option<NamespaceId>,
    #[serde(default)]
    pub checkpoint_now: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminCheckpointWaitPayload {
    #[serde(default)]
    pub namespace: Option<NamespaceId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminFingerprintPayload {
    pub mode: AdminFingerprintMode,
    #[serde(default)]
    pub sample: Option<AdminFingerprintSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminMaintenanceModePayload {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminStoreFsckPayload {
    pub store_id: StoreId,
    #[serde(default)]
    pub repair: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminStoreLockInfoPayload {
    pub store_id: StoreId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminStoreUnlockPayload {
    pub store_id: StoreId,
    #[serde(default)]
    pub force: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn update_payload_rejects_closed_status() {
        let payload = json!({
            "id": "bd-xyz123",
            "patch": { "status": "closed" }
        });
        let encoded = serde_json::to_string(&payload).unwrap();
        let err = serde_json::from_str::<UpdatePayload>(&encoded).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("closed") || msg.contains("unknown variant"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn close_payload_decodes_with_reason() {
        let payload = json!({
            "id": "bd-xyz123",
            "reason": "done"
        });
        let encoded = serde_json::to_string(&payload).unwrap();
        let decoded: ClosePayload = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.reason.as_deref(), Some("done"));
        assert!(decoded.on_branch.is_none());
    }
}
