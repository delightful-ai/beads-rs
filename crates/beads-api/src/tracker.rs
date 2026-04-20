use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TrackerState {
    #[serde(rename = "Todo")]
    Todo,
    #[serde(rename = "In Progress")]
    InProgress,
    #[serde(rename = "Human Review")]
    HumanReview,
    #[serde(rename = "Rework")]
    Rework,
    #[serde(rename = "Merging")]
    Merging,
    #[serde(rename = "Done")]
    Done,
    #[serde(rename = "Closed")]
    Closed,
    #[serde(rename = "Cancelled")]
    Cancelled,
    #[serde(rename = "Duplicate")]
    Duplicate,
}

impl TrackerState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Todo => "Todo",
            Self::InProgress => "In Progress",
            Self::HumanReview => "Human Review",
            Self::Rework => "Rework",
            Self::Merging => "Merging",
            Self::Done => "Done",
            Self::Closed => "Closed",
            Self::Cancelled => "Cancelled",
            Self::Duplicate => "Duplicate",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Done | Self::Closed | Self::Cancelled | Self::Duplicate
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerBlocker {
    pub id: String,
    pub identifier: String,
    pub state: TrackerState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerIssue {
    pub id: String,
    pub identifier: String,
    pub title: String,
    pub description: String,
    pub priority: u8,
    pub state: TrackerState,
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
    use super::TrackerState;

    #[test]
    fn tracker_state_json_uses_symphony_strings() {
        let encoded = serde_json::to_string(&TrackerState::HumanReview).unwrap();
        assert_eq!(encoded, "\"Human Review\"");

        let decoded: TrackerState = serde_json::from_str("\"In Progress\"").unwrap();
        assert_eq!(decoded, TrackerState::InProgress);
    }
}
