use serde::{Deserialize, Serialize};

pub use beads_core::IssueStatus;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerBlocker {
    pub id: String,
    pub identifier: String,
    pub status: IssueStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerIssue {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub description: String,
    pub priority: u8,
    pub status: IssueStatus,
    pub labels: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assignee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_by: Vec<TrackerBlocker>,
    pub assigned_to_worker: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::IssueStatus;

    #[test]
    fn issue_status_json_uses_symphony_strings() {
        let encoded = serde_json::to_string(&IssueStatus::HumanReview).unwrap();
        assert_eq!(encoded, "\"Human Review\"");

        let decoded: IssueStatus = serde_json::from_str("\"In Progress\"").unwrap();
        assert_eq!(decoded, IssueStatus::InProgress);
    }

    #[test]
    fn issue_status_json_rejects_closed() {
        let err = serde_json::from_str::<IssueStatus>("\"Closed\"").unwrap_err();
        assert!(err.is_data());
    }
}
