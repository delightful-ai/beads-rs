use beads_api::{AdminFingerprintMode, AdminFingerprintSample};
use beads_core::{
    BeadId, BeadSlug, BeadType, BranchName, DepKind, IssueStatus, NamespaceId, Priority, StoreId,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::ops::BeadPatch;
use crate::query::Filters;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyPayload {}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InitPayload {
    #[serde(default)]
    pub root_slug: Option<BeadSlug>,
}

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
pub struct TrackerTransitionPayload {
    pub id: BeadId,
    pub status: IssueStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerCommentPayload {
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
pub struct TrackerListPayload {
    #[serde(default)]
    pub ids: Option<Vec<BeadId>>,
    #[serde(default, deserialize_with = "deserialize_tracker_statuses")]
    pub statuses: Option<Vec<TrackerStatusFilter>>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrackerStatusFilter {
    Status(IssueStatus),
    Terminal,
}

impl TrackerStatusFilter {
    pub fn matches(&self, status: IssueStatus) -> bool {
        match self {
            Self::Status(expected) => *expected == status,
            Self::Terminal => status.is_terminal(),
        }
    }
}

impl Serialize for TrackerStatusFilter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Status(status) => status.serialize(serializer),
            Self::Terminal => serializer.serialize_str("Closed"),
        }
    }
}

fn deserialize_tracker_statuses<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<TrackerStatusFilter>>, D::Error>
where
    D: Deserializer<'de>,
{
    let Some(raw_statuses) = Option::<Vec<String>>::deserialize(deserializer)? else {
        return Ok(None);
    };

    let statuses = raw_statuses
        .into_iter()
        .map(|raw| match raw.trim() {
            "Closed" | "closed" => Ok(TrackerStatusFilter::Terminal),
            value => IssueStatus::parse(value)
                .map(TrackerStatusFilter::Status)
                .ok_or_else(|| {
                    serde::de::Error::custom(format!("unknown tracker status `{value}`"))
                }),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(statuses))
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
    pub status: Option<IssueStatus>,
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

    #[test]
    fn tracker_transition_payload_decodes_symphony_state_names() {
        let payload = json!({
            "id": "bd-xyz123",
            "status": "Human Review"
        });
        let encoded = serde_json::to_string(&payload).unwrap();
        let decoded: TrackerTransitionPayload = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.status, IssueStatus::HumanReview);
    }

    #[test]
    fn tracker_list_payload_treats_closed_as_terminal_selector() {
        let payload = json!({
            "statuses": ["Closed", "In Progress"]
        });
        let encoded = serde_json::to_string(&payload).unwrap();
        let decoded: TrackerListPayload = serde_json::from_str(&encoded).unwrap();
        let filters = decoded.statuses.expect("status filters");

        assert!(
            filters
                .iter()
                .any(|filter| filter.matches(IssueStatus::Done))
        );
        assert!(
            filters
                .iter()
                .any(|filter| filter.matches(IssueStatus::Cancelled))
        );
        assert!(
            filters
                .iter()
                .any(|filter| filter.matches(IssueStatus::Duplicate))
        );
        assert!(
            filters
                .iter()
                .any(|filter| filter.matches(IssueStatus::InProgress))
        );
        assert!(
            !filters
                .iter()
                .any(|filter| filter.matches(IssueStatus::Todo))
        );
    }

    #[test]
    fn stale_payload_rejects_blocked_pseudo_status() {
        let payload = json!({
            "days": 30,
            "status": "blocked"
        });
        let encoded = serde_json::to_string(&payload).unwrap();
        let err = serde_json::from_str::<StalePayload>(&encoded).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("blocked") || msg.contains("unknown variant"),
            "unexpected error: {msg}"
        );
    }
}
