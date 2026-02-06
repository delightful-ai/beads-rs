#![allow(dead_code)]

use beads_rs::daemon::ipc::{
    AddNotePayload, ClaimPayload, ClosePayload, CreatePayload, DeletePayload, DepPayload,
    IdPayload, LabelsPayload, LeasePayload, ParentPayload, UpdatePayload,
};
use beads_rs::daemon::mutation_engine::{MutationContext, MutationEngine, ParsedMutationRequest};
use beads_rs::daemon::ops::{BeadPatch, OpError, Patch};
use beads_rs::{ActorId, BeadId, BeadType, DepKind, Limits, NamespaceId, Priority, TraceId};
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

fn parse_bead_id(raw: impl AsRef<str>) -> BeadId {
    BeadId::parse(raw.as_ref()).expect("bead id")
}

#[derive(Clone, Debug)]
pub enum MutationPayload {
    Create(CreatePayload),
    Update(UpdatePayload),
    AddLabels(LabelsPayload),
    RemoveLabels(LabelsPayload),
    SetParent(ParentPayload),
    Close(ClosePayload),
    Reopen(IdPayload),
    Delete(DeletePayload),
    AddDep(DepPayload),
    RemoveDep(DepPayload),
    AddNote(AddNotePayload),
    Claim(ClaimPayload),
    Unclaim(IdPayload),
    ExtendClaim(LeasePayload),
}

impl MutationPayload {
    fn parse(&self, actor: &ActorId) -> Result<ParsedMutationRequest, OpError> {
        match self {
            MutationPayload::Create(payload) => {
                ParsedMutationRequest::parse_create(payload.clone(), actor)
            }
            MutationPayload::Update(payload) => {
                ParsedMutationRequest::parse_update(payload.clone())
            }
            MutationPayload::AddLabels(payload) => {
                ParsedMutationRequest::parse_add_labels(payload.clone())
            }
            MutationPayload::RemoveLabels(payload) => {
                ParsedMutationRequest::parse_remove_labels(payload.clone())
            }
            MutationPayload::SetParent(payload) => {
                ParsedMutationRequest::parse_set_parent(payload.clone())
            }
            MutationPayload::Close(payload) => ParsedMutationRequest::parse_close(payload.clone()),
            MutationPayload::Reopen(payload) => {
                ParsedMutationRequest::parse_reopen(payload.clone())
            }
            MutationPayload::Delete(payload) => {
                ParsedMutationRequest::parse_delete(payload.clone())
            }
            MutationPayload::AddDep(payload) => {
                ParsedMutationRequest::parse_add_dep(payload.clone())
            }
            MutationPayload::RemoveDep(payload) => {
                ParsedMutationRequest::parse_remove_dep(payload.clone())
            }
            MutationPayload::AddNote(payload) => {
                ParsedMutationRequest::parse_add_note(payload.clone())
            }
            MutationPayload::Claim(payload) => ParsedMutationRequest::parse_claim(payload.clone()),
            MutationPayload::Unclaim(payload) => {
                ParsedMutationRequest::parse_unclaim(payload.clone())
            }
            MutationPayload::ExtendClaim(payload) => {
                ParsedMutationRequest::parse_extend_claim(payload.clone())
            }
        }
    }
}

pub fn request_sha256(
    ctx: &MutationContext,
    request: &MutationPayload,
) -> Result<[u8; 32], OpError> {
    let engine = MutationEngine::new(Limits::default());
    let parsed = request.parse(&ctx.actor_id)?;
    engine.request_sha256_for(ctx, &parsed)
}

pub fn create_request(title: impl Into<String>) -> MutationPayload {
    MutationPayload::Create(CreatePayload {
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
    })
}

pub fn update_request(id: impl AsRef<str>, patch: BeadPatch) -> MutationPayload {
    MutationPayload::Update(UpdatePayload {
        id: parse_bead_id(id),
        patch,
        cas: None,
    })
}

pub fn add_labels_request(id: impl AsRef<str>, labels: Vec<String>) -> MutationPayload {
    MutationPayload::AddLabels(LabelsPayload {
        id: parse_bead_id(id),
        labels,
    })
}

pub fn remove_labels_request(id: impl AsRef<str>, labels: Vec<String>) -> MutationPayload {
    MutationPayload::RemoveLabels(LabelsPayload {
        id: parse_bead_id(id),
        labels,
    })
}

pub fn set_parent_request(id: impl AsRef<str>, parent: Option<impl AsRef<str>>) -> MutationPayload {
    MutationPayload::SetParent(ParentPayload {
        id: parse_bead_id(id),
        parent: parent.map(parse_bead_id),
    })
}

pub fn close_request(id: impl AsRef<str>, reason: Option<String>) -> MutationPayload {
    MutationPayload::Close(ClosePayload {
        id: parse_bead_id(id),
        reason,
        on_branch: None,
    })
}

pub fn reopen_request(id: impl AsRef<str>) -> MutationPayload {
    MutationPayload::Reopen(IdPayload {
        id: parse_bead_id(id),
    })
}

pub fn delete_request(id: impl AsRef<str>, reason: Option<String>) -> MutationPayload {
    MutationPayload::Delete(DeletePayload {
        id: parse_bead_id(id),
        reason,
    })
}

pub fn add_dep_request(
    from: impl AsRef<str>,
    to: impl AsRef<str>,
    kind: DepKind,
) -> MutationPayload {
    MutationPayload::AddDep(DepPayload {
        from: parse_bead_id(from),
        to: parse_bead_id(to),
        kind,
    })
}

pub fn remove_dep_request(
    from: impl AsRef<str>,
    to: impl AsRef<str>,
    kind: DepKind,
) -> MutationPayload {
    MutationPayload::RemoveDep(DepPayload {
        from: parse_bead_id(from),
        to: parse_bead_id(to),
        kind,
    })
}

pub fn add_note_request(id: impl AsRef<str>, content: impl Into<String>) -> MutationPayload {
    MutationPayload::AddNote(AddNotePayload {
        id: parse_bead_id(id),
        content: content.into(),
    })
}

pub fn claim_request(id: impl AsRef<str>, lease_secs: u64) -> MutationPayload {
    MutationPayload::Claim(ClaimPayload {
        id: parse_bead_id(id),
        lease_secs,
    })
}

pub fn unclaim_request(id: impl AsRef<str>) -> MutationPayload {
    MutationPayload::Unclaim(IdPayload {
        id: parse_bead_id(id),
    })
}

pub fn extend_claim_request(id: impl AsRef<str>, lease_secs: u64) -> MutationPayload {
    MutationPayload::ExtendClaim(LeasePayload {
        id: parse_bead_id(id),
        lease_secs,
    })
}

pub fn patch_title(title: impl Into<String>) -> BeadPatch {
    BeadPatch {
        title: Patch::Set(title.into()),
        ..Default::default()
    }
}

pub fn sample_requests() -> Vec<MutationPayload> {
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
                MutationPayload::Create(_) => seen[0] = true,
                MutationPayload::Update(_) => seen[1] = true,
                MutationPayload::AddLabels(_) => seen[2] = true,
                MutationPayload::RemoveLabels(_) => seen[3] = true,
                MutationPayload::SetParent(_) => seen[4] = true,
                MutationPayload::Close(_) => seen[5] = true,
                MutationPayload::Reopen(_) => seen[6] = true,
                MutationPayload::Delete(_) => seen[7] = true,
                MutationPayload::AddDep(_) => seen[8] = true,
                MutationPayload::RemoveDep(_) => seen[9] = true,
                MutationPayload::AddNote(_) => seen[10] = true,
                MutationPayload::Claim(_) => seen[11] = true,
                MutationPayload::Unclaim(_) => seen[12] = true,
                MutationPayload::ExtendClaim(_) => seen[13] = true,
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
