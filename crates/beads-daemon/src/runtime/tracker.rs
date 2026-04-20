use beads_api::IssueStatus;

use super::ops::OpError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackerTransitionPlan {
    pub status: IssueStatus,
}

pub(crate) fn status_transition_plan(
    status: IssueStatus,
) -> Result<TrackerTransitionPlan, OpError> {
    Ok(TrackerTransitionPlan { status })
}

pub(crate) fn terminal_status_from_close_reason(
    reason: Option<&str>,
) -> Result<IssueStatus, OpError> {
    IssueStatus::from_close_reason(reason).ok_or_else(|| OpError::ValidationFailed {
        field: "reason".into(),
        reason: format!(
            "valid close reasons: {}",
            IssueStatus::VALID_CLOSE_REASON_HELP
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transition_plan_preserves_human_review() {
        let plan = status_transition_plan(IssueStatus::HumanReview).unwrap();
        assert_eq!(plan.status, IssueStatus::HumanReview);
    }

    #[test]
    fn transition_plan_preserves_done() {
        let plan = status_transition_plan(IssueStatus::Done).unwrap();
        assert_eq!(plan.status, IssueStatus::Done);
    }

    #[test]
    fn terminal_close_reason_defaults_to_done() {
        let state = terminal_status_from_close_reason(None).unwrap();
        assert_eq!(state, IssueStatus::Done);
    }
}
