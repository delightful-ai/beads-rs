//! Event validation fixtures.

use beads_rs::{Limits, validate_event_body};

use crate::fixtures::event_body::sample_event_body;

#[test]
fn sample_event_body_validates() {
    let body = sample_event_body(1);
    validate_event_body(&body, &Limits::default()).expect("fixture should validate");
}
