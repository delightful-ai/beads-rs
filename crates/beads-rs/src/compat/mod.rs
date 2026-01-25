//! Go format compatibility layer.
//!
//! Provides types and functions to export beads in the Go beads format
//! for interoperability with tools built for the original Go implementation.

mod export;
mod go_schema;

pub use export::{ExportContext, ensure_symlinks, export_jsonl};
pub use go_schema::{GoComment, GoDependency, GoIssue};
