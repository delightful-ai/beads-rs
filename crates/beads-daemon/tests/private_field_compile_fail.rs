#[cfg(feature = "test-harness")]
#[test]
fn private_field_compile_fail_guards_acknowledgement_apis() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/*_private.rs");
}
