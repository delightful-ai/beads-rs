#![allow(dead_code)]

use beads_rs::{
    ApplyOutcome, Bead, BeadId, CanonicalState, NoteId, NoteKey, ValidatedEventBody, apply_event,
};

pub struct ApplyHarness {
    state: CanonicalState,
}

impl ApplyHarness {
    pub fn new() -> Self {
        Self {
            state: CanonicalState::new(),
        }
    }

    pub fn state(&self) -> &CanonicalState {
        &self.state
    }

    pub fn apply(&mut self, body: &ValidatedEventBody) -> ApplyOutcome {
        apply_event(&mut self.state, body).expect("apply_event fixture")
    }

    pub fn apply_twice(&mut self, body: &ValidatedEventBody) -> (ApplyOutcome, ApplyOutcome) {
        let first = self.apply(body);
        let second = self.apply(body);
        (first, second)
    }
}

pub fn assert_bead_present<'a>(state: &'a CanonicalState, id: &BeadId) -> &'a Bead {
    state.get_live(id).expect("bead present")
}

pub fn assert_note_present(state: &CanonicalState, bead_id: &BeadId, note_id: &NoteId) {
    let _ = assert_bead_present(state, bead_id);
    assert!(state.note_id_exists(bead_id, note_id), "note present");
}

pub fn note_key(bead_id: &BeadId, note_id: &NoteId) -> NoteKey {
    NoteKey {
        bead_id: bead_id.clone(),
        note_id: note_id.clone(),
    }
}

pub fn assert_outcome_contains_bead(outcome: &ApplyOutcome, bead_id: &BeadId) {
    assert!(
        outcome.changed_beads.contains(bead_id),
        "outcome tracks bead change"
    );
}

pub fn assert_outcome_contains_note(outcome: &ApplyOutcome, bead_id: &BeadId, note_id: &NoteId) {
    assert!(
        outcome.changed_notes.contains(&note_key(bead_id, note_id)),
        "outcome tracks note change"
    );
}
