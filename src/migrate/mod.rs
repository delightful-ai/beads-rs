//! Migration utilities.
//!
//! Currently supports importing from beads-go JSONL exports.

pub mod go_export;

pub use go_export::{import_go_export, GoImportReport};

