//! Event validation fixtures.

use beads_rs::{Limits, ValidatedEventBody};

use crate::fixtures::event_body::sample_event_body;

#[test]
fn sample_event_body_validates() {
    let body = sample_event_body(1);
    ValidatedEventBody::try_from_raw(body, &Limits::default()).expect("fixture should validate");
}
