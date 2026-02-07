#![allow(dead_code)]

use beads_rs::surface::ipc::{
    AddNotePayload, ClaimPayload, ClosePayload, CreatePayload, DeletePayload, DepPayload,
    IdPayload, LabelsPayload, LeasePayload, ParentPayload, UpdatePayload,
};
use beads_rs::surface::ops::{BeadPatch, Patch};
use beads_rs::{
    ActorId, BeadId, BeadType, ClientRequestId, DepKind, NamespaceId, Priority, TraceId,
    sha256_bytes,
};
use uuid::Uuid;

use super::identity;

#[derive(Clone, Debug)]
pub struct MutationContext {
    pub namespace: NamespaceId,
    pub actor_id: ActorId,
    pub client_request_id: Option<ClientRequestId>,
    pub trace_id: TraceId,
}

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

pub fn request_sha256(
    ctx: &MutationContext,
    request: &MutationPayload,
) -> beads_rs::Result<[u8; 32]> {
    // Tests only need deterministic request identity for idempotency mapping checks.
    // Use a stable textual digest over typed context + payload fields.
    let canonical = format!("{ctx:?}|{request:?}");
    Ok(sha256_bytes(canonical.as_bytes()).0)
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
