#![feature(rustc_private)]
#![warn(unused_extern_crates)]

//! Placeholder for future beads-rs specific lints.
//!
//! To add a new lint:
//! 1. Declare it with `declare_lint!`
//! 2. Implement `LateLintPass` (or `EarlyLintPass`)
//! 3. Register it in `register_lints`

extern crate rustc_lint;
extern crate rustc_session;

dylint_linting::dylint_library!();

#[unsafe(no_mangle)]
pub fn register_lints(_sess: &rustc_session::Session, _lint_store: &mut rustc_lint::LintStore) {
    // Register lints here as they are added
}
