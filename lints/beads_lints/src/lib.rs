#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_session;
extern crate rustc_span;

use clippy_utils::diagnostics::span_lint_and_help;
use clippy_utils::diagnostics::span_lint_hir_and_then;
use clippy_utils::source::snippet_opt;
use rustc_hir::{
    AmbigArg, Expr, ExprKind, HirId, Item, ItemKind, PathSegment, QPath, Ty, TyKind, UseKind,
};
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
    emitted_hir: HashSet<HirId>,
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

        self.maybe_lint_use(cx, item.hir_id(), path.span, path.segments);
    }

    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        let ExprKind::Path(qpath) = &expr.kind else {
            return;
        };
        self.maybe_lint_qpath(cx, expr.hir_id, expr.span, qpath);
    }

    fn check_ty(&mut self, cx: &LateContext<'tcx>, ty: &'tcx Ty<'tcx, AmbigArg>) {
        let TyKind::Path(qpath) = &ty.kind else {
            return;
        };
        self.maybe_lint_qpath(cx, ty.hir_id, ty.span, qpath);
    }
}

impl CliDaemonBoundary {
    fn maybe_lint_use(
        &mut self,
        cx: &LateContext<'_>,
        hir_id: HirId,
        span: Span,
        segments: &[PathSegment<'_>],
    ) {
        self.maybe_lint_path(cx, Some(hir_id), span, segments, true);
    }

    fn maybe_lint_qpath(
        &mut self,
        cx: &LateContext<'_>,
        hir_id: HirId,
        span: Span,
        qpath: &QPath<'_>,
    ) {
        let QPath::Resolved(_, path) = qpath else {
            return;
        };
        self.maybe_lint_path(cx, Some(hir_id), span, path.segments, false);
    }

    fn maybe_lint_path(
        &mut self,
        cx: &LateContext<'_>,
        hir_id: Option<HirId>,
        span: Span,
        segments: &[PathSegment<'_>],
        allow_bare_daemon: bool,
    ) {
        let (filename, line, column) = file_line_col(cx, span);
        if !is_cli_filename(&filename) {
            return;
        }

        let segment_names = segment_names(segments);
        let Some((root, child)) = daemon_boundary_match(&segment_names, allow_bare_daemon) else {
            return;
        };

        if let Some(hir_id) = hir_id
            && !self.emitted_hir.insert(hir_id)
        {
            return;
        }

        let key = format!("{filename}:{line}:{column}");
        if !self.emitted_at.insert(key) {
            return;
        }

        let import_path = snippet_opt(cx, span)
            .map(|snippet| snippet.trim().to_owned())
            .filter(|snippet| !snippet.is_empty())
            .unwrap_or_else(|| segment_names.join("::"));
        let message = format!("CLI reference `{import_path}` bypasses the CLI/daemon boundary");
        let help = daemon_boundary_help(root, child);

        if let Some(hir_id) = hir_id {
            span_lint_hir_and_then(cx, CLI_DAEMON_BOUNDARY, hir_id, span, message, |diag| {
                diag.help(help);
            });
        } else {
            span_lint_and_help(cx, CLI_DAEMON_BOUNDARY, span, message, None, help);
        }
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

#[cfg(test)]
fn is_daemon_import_path(segment_names: &[String]) -> bool {
    daemon_boundary_match(segment_names, true).is_some()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundaryRoot {
    Daemon,
    BeadsDaemon,
    BeadsDaemonCore,
}

fn daemon_boundary_match(
    segment_names: &[String],
    allow_bare_daemon: bool,
) -> Option<(BoundaryRoot, Option<&str>)> {
    let (root, daemon_index) = daemon_root_index(segment_names, allow_bare_daemon)?;
    let child = segment_names.get(daemon_index + 1).map(String::as_str);
    Some((root, child))
}

fn daemon_root_index(
    segment_names: &[String],
    allow_bare_daemon: bool,
) -> Option<(BoundaryRoot, usize)> {
    match segment_names {
        [first, second, ..] if first == "crate" && second == "daemon" => {
            Some((BoundaryRoot::Daemon, 1))
        }
        [first, ..] if allow_bare_daemon && first == "daemon" => Some((BoundaryRoot::Daemon, 0)),
        [first, second, ..] if first == "crate" && second == "beads_daemon" => {
            Some((BoundaryRoot::BeadsDaemon, 1))
        }
        [first, ..] if first == "beads_daemon" => Some((BoundaryRoot::BeadsDaemon, 0)),
        [first, second, ..] if first == "crate" && second == "beads_daemon_core" => {
            Some((BoundaryRoot::BeadsDaemonCore, 1))
        }
        [first, ..] if first == "beads_daemon_core" => Some((BoundaryRoot::BeadsDaemonCore, 0)),
        _ => None,
    }
}

#[cfg(test)]
fn daemon_child_segment(segment_names: &[String]) -> Option<&str> {
    let (_, daemon_index) = daemon_root_index(segment_names, true)?;
    segment_names.get(daemon_index + 1).map(String::as_str)
}

fn daemon_boundary_help(root: BoundaryRoot, daemon_child: Option<&str>) -> &'static str {
    match root {
        BoundaryRoot::BeadsDaemonCore => {
            "Import protocol/frame contracts from `beads_surface` boundary APIs; CLI must not depend on `beads_daemon_core` directly."
        }
        BoundaryRoot::BeadsDaemon => {
            "Import daemon-facing contracts from `beads_surface` or `beads_api`; CLI must not depend on `beads_daemon` directly."
        }
        BoundaryRoot::Daemon => match daemon_child {
            Some("ipc") => {
                "Import IPC contracts from `beads_surface::ipc` and call transport through a CLI wrapper."
            }
            Some("query") => "Import query/filter types from `beads_surface::query`.",
            Some("ops") => "Import operation/result types from `beads_surface::ops`.",
            _ => {
                "Replace direct daemon imports with `beads_surface` boundary types or a dedicated CLI wrapper."
            }
        },
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
        assert!(is_daemon_import_path(&names(&[
            "beads_daemon",
            "remote",
            "RemoteUrl",
        ])));
        assert!(is_daemon_import_path(&names(&[
            "beads_daemon_core",
            "repl",
            "proto",
        ])));
        assert!(is_daemon_import_path(&names(&[
            "crate",
            "beads_daemon",
            "remote",
        ])));
        assert!(is_daemon_import_path(&names(&[
            "crate",
            "beads_daemon_core",
            "repl",
        ])));
    }

    #[test]
    fn ignores_non_daemon_or_non_root_paths() {
        assert!(!is_daemon_import_path(&names(&[
            "crate", "cli", "daemon", "ipc"
        ])));
        assert!(!is_daemon_import_path(&names(&["self", "daemon", "ipc"])));
        assert!(!is_daemon_import_path(&names(&["super", "daemon", "ipc"])));
        assert!(!is_daemon_import_path(&names(&[
            "commands", "daemon", "DaemonCmd"
        ])));
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
        let ipc_help = daemon_boundary_help(BoundaryRoot::Daemon, Some("ipc"));
        let query_help = daemon_boundary_help(BoundaryRoot::Daemon, Some("query"));
        let fallback_help = daemon_boundary_help(BoundaryRoot::Daemon, None);
        let daemon_help = daemon_boundary_help(BoundaryRoot::BeadsDaemon, None);
        let daemon_core_help = daemon_boundary_help(BoundaryRoot::BeadsDaemonCore, None);

        assert!(ipc_help.contains("beads_surface::ipc"));
        assert!(query_help.contains("beads_surface::query"));
        assert!(fallback_help.contains("beads_surface"));
        assert!(fallback_help.contains("wrapper"));
        assert!(daemon_help.contains("beads_daemon"));
        assert!(daemon_core_help.contains("beads_daemon_core"));
    }
}
