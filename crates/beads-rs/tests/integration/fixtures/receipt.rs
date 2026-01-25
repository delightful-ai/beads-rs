#![allow(dead_code)]

use beads_rs::{DurabilityClass, DurabilityOutcome, DurabilityReceipt};

pub fn assert_receipt_identity(expected: &DurabilityReceipt, actual: &DurabilityReceipt) {
    assert_eq!(expected.store, actual.store, "store identity mismatch");
    assert_eq!(expected.txn_id, actual.txn_id, "txn id mismatch");
    assert_eq!(expected.event_ids, actual.event_ids, "event ids mismatch");
}

pub fn assert_receipt_equivalent(expected: &DurabilityReceipt, actual: &DurabilityReceipt) {
    assert_eq!(expected, actual, "receipt mismatch");
}

pub fn assert_receipt_at_least(expected: &DurabilityReceipt, actual: &DurabilityReceipt) {
    assert_receipt_identity(expected, actual);
    assert!(
        actual
            .durability_proof
            .local_fsync
            .durable_seq
            .satisfies_at_least(&expected.durability_proof.local_fsync.durable_seq),
        "durable watermarks did not advance"
    );
    assert!(
        actual.min_seen.satisfies_at_least(&expected.min_seen),
        "min_seen did not advance"
    );
}

pub fn assert_idempotent_receipts(first: &DurabilityReceipt, retry: &DurabilityReceipt) {
    assert_receipt_identity(first, retry);
    assert_eq!(first.outcome, retry.outcome, "outcome mismatch");
    assert_receipt_at_least(first, retry);
}

pub fn merge_receipts(left: &DurabilityReceipt, right: &DurabilityReceipt) -> DurabilityReceipt {
    left.merge(right).expect("receipts merge")
}

pub fn requested_durability(receipt: &DurabilityReceipt) -> DurabilityClass {
    match receipt.outcome {
        DurabilityOutcome::Achieved { requested, .. } => requested,
        DurabilityOutcome::Pending { requested } => requested,
    }
}
