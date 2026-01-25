#[allow(dead_code)]
#[path = "../examples/durability_idempotency_machine.rs"]
mod durability_idempotency_machine;

#[test]
fn durability_receipts_are_idempotent_and_monotonic() {
    durability_idempotency_machine::run_regression_check();
}
