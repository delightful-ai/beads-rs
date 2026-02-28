use super::*;

pub(super) fn compute_blocked_by(
    state: &CanonicalState,
) -> std::collections::BTreeMap<BeadId, Vec<BeadId>> {
    let mut blocked: std::collections::BTreeMap<BeadId, Vec<BeadId>> =
        std::collections::BTreeMap::new();

    for key in state.dep_store().values() {
        if key.kind() != DepKind::Blocks {
            continue;
        }

        // Only count blockers that are currently open (not closed).
        if let Some(to_bead) = state.get_live(key.to()) {
            if to_bead.fields.workflow.value.is_closed() {
                continue;
            }
        } else {
            continue;
        }

        blocked
            .entry(key.from().clone())
            .or_default()
            .push(key.to().clone());
    }

    blocked
}

pub(super) fn dep_cycles_from_state(state: &CanonicalState) -> DepCycles {
    let cycles = state
        .dependency_cycles()
        .into_iter()
        .map(|cycle| {
            cycle
                .into_iter()
                .map(|id| id.as_str().to_string())
                .collect()
        })
        .collect();
    DepCycles { cycles }
}

pub(super) fn compute_epic_statuses(
    namespace: &crate::core::NamespaceId,
    state: &CanonicalState,
    eligible_only: bool,
) -> Vec<EpicStatus> {
    // Build epic -> children mapping from parent edges.
    let mut children: std::collections::BTreeMap<BeadId, Vec<BeadId>> =
        std::collections::BTreeMap::new();
    for edge in state.parent_edges() {
        children
            .entry(edge.parent().clone())
            .or_default()
            .push(edge.child().clone());
    }

    let mut out = Vec::new();
    for (id, bead) in state.iter_live() {
        if bead.fields.bead_type.value != crate::core::BeadType::Epic {
            continue;
        }
        if bead.fields.workflow.value.is_closed() {
            continue;
        }

        let Some(view) = state.bead_view(id) else {
            continue;
        };

        let child_ids = children.get(id).cloned().unwrap_or_default();
        let total_children = child_ids.len();
        let closed_children = child_ids
            .iter()
            .filter(|cid| {
                state
                    .get_live(cid)
                    .map(|b| b.fields.workflow.value.is_closed())
                    .unwrap_or(false)
            })
            .count();

        let eligible_for_close = total_children > 0 && closed_children == total_children;
        if eligible_only && !eligible_for_close {
            continue;
        }

        out.push(EpicStatus {
            epic: IssueSummary::from_view(namespace, &view),
            total_children,
            closed_children,
            eligible_for_close,
        });
    }

    out.sort_by(|a, b| a.epic.id.cmp(&b.epic.id));
    out
}

pub(super) fn sort_ready_issues(issues: &mut [IssueSummary]) {
    issues.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| a.created_at.cmp(&b.created_at))
    });
}
