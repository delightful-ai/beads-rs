#![allow(dead_code)]

use beads_rs::daemon::mutation_engine::{
    MutationContext, MutationEngine, MutationRequest, ParsedMutationRequest,
};
use beads_rs::daemon::ops::{BeadPatch, OpError, Patch};
use beads_rs::{ActorId, BeadType, DepKind, Limits, NamespaceId, Priority, TraceId};
use uuid::Uuid;

use super::identity;

pub fn default_context() -> MutationContext {
    MutationContext {
        namespace: NamespaceId::core(),
        actor_id: ActorId::new("fixture").expect("actor id"),
        client_request_id: None,
        trace_id: TraceId::new(Uuid::from_bytes([9u8; 16])),
    }
}

pub fn context_with_client_request_id(seed: u8) -> MutationContext {
    let client_request_id = identity::client_request_id(seed);
    MutationContext {
        client_request_id: Some(client_request_id),
        trace_id: TraceId::from(client_request_id),
        ..default_context()
    }
}

pub fn request_sha256(
    ctx: &MutationContext,
    request: &MutationRequest,
) -> Result<[u8; 32], OpError> {
    let engine = MutationEngine::new(Limits::default());
    let parsed = ParsedMutationRequest::parse(request.clone(), &ctx.actor_id)?;
    engine.request_sha256_for(ctx, &parsed)
}

pub fn create_request(title: impl Into<String>) -> MutationRequest {
    MutationRequest::Create {
        id: None,
        parent: None,
        title: title.into(),
        bead_type: BeadType::Task,
        priority: Priority::MEDIUM,
        description: None,
        design: None,
        acceptance_criteria: None,
        assignee: None,
        external_ref: None,
        estimated_minutes: None,
        labels: Vec::new(),
        dependencies: Vec::new(),
    }
}

pub fn update_request(id: impl Into<String>, patch: BeadPatch) -> MutationRequest {
    MutationRequest::Update {
        id: id.into(),
        patch,
        cas: None,
    }
}

pub fn add_labels_request(id: impl Into<String>, labels: Vec<String>) -> MutationRequest {
    MutationRequest::AddLabels {
        id: id.into(),
        labels,
    }
}

pub fn remove_labels_request(id: impl Into<String>, labels: Vec<String>) -> MutationRequest {
    MutationRequest::RemoveLabels {
        id: id.into(),
        labels,
    }
}

pub fn set_parent_request(id: impl Into<String>, parent: Option<String>) -> MutationRequest {
    MutationRequest::SetParent {
        id: id.into(),
        parent,
    }
}

pub fn close_request(id: impl Into<String>, reason: Option<String>) -> MutationRequest {
    MutationRequest::Close {
        id: id.into(),
        reason,
        on_branch: None,
    }
}

pub fn reopen_request(id: impl Into<String>) -> MutationRequest {
    MutationRequest::Reopen { id: id.into() }
}

pub fn delete_request(id: impl Into<String>, reason: Option<String>) -> MutationRequest {
    MutationRequest::Delete {
        id: id.into(),
        reason,
    }
}

pub fn add_dep_request(
    from: impl Into<String>,
    to: impl Into<String>,
    kind: DepKind,
) -> MutationRequest {
    MutationRequest::AddDep {
        from: from.into(),
        to: to.into(),
        kind,
    }
}

pub fn remove_dep_request(
    from: impl Into<String>,
    to: impl Into<String>,
    kind: DepKind,
) -> MutationRequest {
    MutationRequest::RemoveDep {
        from: from.into(),
        to: to.into(),
        kind,
    }
}

pub fn add_note_request(id: impl Into<String>, content: impl Into<String>) -> MutationRequest {
    MutationRequest::AddNote {
        id: id.into(),
        content: content.into(),
    }
}

pub fn claim_request(id: impl Into<String>, lease_secs: u64) -> MutationRequest {
    MutationRequest::Claim {
        id: id.into(),
        lease_secs,
    }
}

pub fn unclaim_request(id: impl Into<String>) -> MutationRequest {
    MutationRequest::Unclaim { id: id.into() }
}

pub fn extend_claim_request(id: impl Into<String>, lease_secs: u64) -> MutationRequest {
    MutationRequest::ExtendClaim {
        id: id.into(),
        lease_secs,
    }
}

pub fn patch_title(title: impl Into<String>) -> BeadPatch {
    BeadPatch {
        title: Patch::Set(title.into()),
        ..Default::default()
    }
}

pub fn sample_requests() -> Vec<MutationRequest> {
    vec![
        create_request("sample"),
        update_request("bd-1", patch_title("updated")),
        add_labels_request("bd-1", vec!["label".to_string()]),
        remove_labels_request("bd-1", vec!["label".to_string()]),
        set_parent_request("bd-1", Some("bd-2".to_string())),
        close_request("bd-1", Some("done".to_string())),
        reopen_request("bd-1"),
        delete_request("bd-1", Some("obsolete".to_string())),
        add_dep_request("bd-1", "bd-2", DepKind::Blocks),
        remove_dep_request("bd-1", "bd-2", DepKind::Blocks),
        add_note_request("bd-1", "note"),
        claim_request("bd-1", 3600),
        unclaim_request("bd-1"),
        extend_claim_request("bd-1", 7200),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builders_cover_all_mutation_variants() {
        let mut seen = [false; 14];
        for request in sample_requests() {
            match request {
                MutationRequest::Create { .. } => seen[0] = true,
                MutationRequest::Update { .. } => seen[1] = true,
                MutationRequest::AddLabels { .. } => seen[2] = true,
                MutationRequest::RemoveLabels { .. } => seen[3] = true,
                MutationRequest::SetParent { .. } => seen[4] = true,
                MutationRequest::Close { .. } => seen[5] = true,
                MutationRequest::Reopen { .. } => seen[6] = true,
                MutationRequest::Delete { .. } => seen[7] = true,
                MutationRequest::AddDep { .. } => seen[8] = true,
                MutationRequest::RemoveDep { .. } => seen[9] = true,
                MutationRequest::AddNote { .. } => seen[10] = true,
                MutationRequest::Claim { .. } => seen[11] = true,
                MutationRequest::Unclaim { .. } => seen[12] = true,
                MutationRequest::ExtendClaim { .. } => seen[13] = true,
            }
        }

        assert!(seen.into_iter().all(|flag| flag));
    }

    #[test]
    fn request_sha256_supports_all_requests() {
        let ctx = default_context();
        for request in sample_requests() {
            request_sha256(&ctx, &request).expect("request sha");
        }
    }
}
