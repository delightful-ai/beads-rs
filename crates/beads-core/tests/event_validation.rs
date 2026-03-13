//! Event validation fixtures.

mod support;

use beads_core::{Limits, ValidatedEventBody};
use support::event_body::sample_event_body;

#[test]
fn sample_event_body_validates() {
    let body = sample_event_body(1);
    ValidatedEventBody::try_from_raw(body, &Limits::default()).expect("fixture should validate");
}
