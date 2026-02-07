#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_session;
extern crate rustc_span;

use clippy_utils::diagnostics::span_lint_and_help;
use rustc_hir::{Item, ItemKind, PathSegment, UseKind};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_span::{symbol::kw, Span};
use std::collections::HashSet;

dylint_linting::dylint_library!();

rustc_session::declare_lint!(
    /// Disallow direct CLI dependencies on daemon internals.
    ///
    /// This lint enforces the intended crate boundary:
    /// CLI should route through `beads-surface` / `beads-api` instead of
    /// `crate::daemon::*` references.
    pub CLI_DAEMON_BOUNDARY,
    Warn,
    "direct CLI dependency on crate::daemon internals"
);

rustc_session::impl_lint_pass!(CliDaemonBoundary => [CLI_DAEMON_BOUNDARY]);

#[derive(Default)]
pub struct CliDaemonBoundary {
    emitted_at: HashSet<String>,
}

#[unsafe(no_mangle)]
pub fn register_lints(sess: &rustc_session::Session, lint_store: &mut rustc_lint::LintStore) {
    dylint_linting::init_config(sess);
    lint_store.register_lints(&[CLI_DAEMON_BOUNDARY]);
    lint_store.register_late_pass(|_| Box::new(CliDaemonBoundary::default()));
}

impl<'tcx> LateLintPass<'tcx> for CliDaemonBoundary {
    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {
        let ItemKind::Use(path, use_kind) = &item.kind else {
            return;
        };

        if matches!(use_kind, UseKind::ListStem) {
            return;
        }

        self.maybe_lint_use(cx, path.span, path.segments);
    }
}

impl CliDaemonBoundary {
    fn maybe_lint_use(&mut self, cx: &LateContext<'_>, span: Span, segments: &[PathSegment<'_>]) {
        let (filename, line, column) = file_line_col(cx, span);
        if !is_cli_filename(&filename) {
            return;
        }
        let segment_names = segment_names(segments);
        if !is_daemon_import_path(&segment_names) {
            return;
        }
        let key = format!("{filename}:{line}:{column}");
        if !self.emitted_at.insert(key) {
            return;
        }
        let import_path = segment_names.join("::");
        let message = format!("CLI import `{import_path}` bypasses the CLI/daemon boundary");
        let help = daemon_boundary_help(daemon_child_segment(&segment_names));
        span_lint_and_help(cx, CLI_DAEMON_BOUNDARY, span, message, None, help);
    }
}

fn file_line_col(cx: &LateContext<'_>, span: Span) -> (String, usize, usize) {
    let source_map = cx.sess().source_map();
    let loc = source_map.lookup_char_pos(span.lo());
    let filename = format!("{:?}", source_map.span_to_filename(span));
    let line = loc.line;
    let column = loc.col_display + 1;
    (filename, line, column)
}

fn segment_names(segments: &[PathSegment<'_>]) -> Vec<String> {
    let segments = if segments
        .first()
        .is_some_and(|segment| segment.ident.name == kw::PathRoot)
    {
        &segments[1..]
    } else {
        segments
    };
    segments
        .iter()
        .map(|segment| segment.ident.name.as_str().to_string())
        .collect()
}

fn is_cli_filename(filename: &str) -> bool {
    filename.contains("/src/cli/")
        || filename.contains("\\src\\cli\\")
        || filename.contains("/crates/beads-cli/src/")
        || filename.contains("\\crates\\beads-cli\\src\\")
}

fn is_daemon_import_path(segment_names: &[String]) -> bool {
    daemon_root_index(segment_names).is_some()
}

fn daemon_root_index(segment_names: &[String]) -> Option<usize> {
    match segment_names {
        [first, second, ..] if first == "crate" && second == "daemon" => Some(1),
        [first, ..] if first == "daemon" => Some(0),
        _ => None,
    }
}

fn daemon_child_segment(segment_names: &[String]) -> Option<&str> {
    let daemon_index = daemon_root_index(segment_names)?;
    segment_names.get(daemon_index + 1).map(String::as_str)
}

fn daemon_boundary_help(daemon_child: Option<&str>) -> &'static str {
    match daemon_child {
        Some("ipc") => {
            "Import IPC contracts from `beads_surface::ipc` and call transport through a CLI wrapper."
        }
        Some("query") => "Import query/filter types from `beads_surface::query`.",
        Some("ops") => "Import operation/result types from `beads_surface::ops`.",
        _ => {
            "Replace direct daemon imports with `beads_surface` boundary types or a dedicated CLI wrapper."
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).to_owned()).collect()
    }

    #[test]
    fn detects_crate_daemon_imports() {
        assert!(is_daemon_import_path(&names(&[
            "crate", "daemon", "ipc", "Request"
        ])));
        assert!(is_daemon_import_path(&names(&["crate", "daemon"])));
    }

    #[test]
    fn detects_bare_daemon_imports() {
        assert!(is_daemon_import_path(&names(&[
            "daemon", "query", "Filters"
        ])));
        assert!(is_daemon_import_path(&names(&[
            "daemon", "ops", "OpResult"
        ])));
    }

    #[test]
    fn ignores_non_daemon_or_non_root_paths() {
        assert!(!is_daemon_import_path(&names(&[
            "crate", "cli", "daemon", "ipc"
        ])));
        assert!(!is_daemon_import_path(&names(&["self", "daemon", "ipc"])));
        assert!(!is_daemon_import_path(&names(&["super", "daemon", "ipc"])));
    }

    #[test]
    fn chooses_child_module_for_help() {
        assert_eq!(
            daemon_child_segment(&names(&["crate", "daemon", "ipc", "Request"])),
            Some("ipc")
        );
        assert_eq!(
            daemon_child_segment(&names(&["daemon", "query", "Filters"])),
            Some("query")
        );
        assert_eq!(daemon_child_segment(&names(&["crate", "daemon"])), None);
    }

    #[test]
    fn cli_path_detection_is_cross_platform() {
        assert!(is_cli_filename("/tmp/work/crates/beads-rs/src/cli/mod.rs"));
        assert!(is_cli_filename(
            "C:\\tmp\\work\\crates\\beads-rs\\src\\cli\\mod.rs"
        ));
        assert!(is_cli_filename("/tmp/work/crates/beads-cli/src/commands/mod.rs"));
        assert!(is_cli_filename(
            "C:\\tmp\\work\\crates\\beads-cli\\src\\commands\\mod.rs"
        ));
        assert!(!is_cli_filename(
            "/tmp/work/crates/beads-rs/src/daemon/mod.rs"
        ));
    }

    #[test]
    fn help_text_points_to_surface_or_wrapper() {
        let ipc_help = daemon_boundary_help(Some("ipc"));
        let query_help = daemon_boundary_help(Some("query"));
        let fallback_help = daemon_boundary_help(None);

        assert!(ipc_help.contains("beads_surface::ipc"));
        assert!(query_help.contains("beads_surface::query"));
        assert!(fallback_help.contains("beads_surface"));
        assert!(fallback_help.contains("wrapper"));
    }
}
