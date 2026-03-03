use clippy_utils::diagnostics::span_lint_hir_and_then;
use clippy_utils::source::snippet_opt;
use rustc_hir::{Expr, ExprKind, HirId};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_span::Span;
use std::collections::HashSet;

rustc_session::declare_lint!(
    /// Disallow compatibility gates that only warn and continue.
    ///
    /// Scoped v1 policy:
    /// - `runtime/core/checkpoint_import.rs`
    /// - `runtime/core/repo_load.rs`
    ///
    /// In those files, compatibility mismatch branches must hard-fail
    /// (`Err`/early return/`?`) rather than log-only continuation.
    pub NO_WARNING_ONLY_COMPATIBILITY_GATES,
    Warn,
    "compatibility mismatch branch only warns and continues"
);

rustc_session::impl_lint_pass!(
    NoWarningOnlyCompatibilityGates => [NO_WARNING_ONLY_COMPATIBILITY_GATES]
);

#[derive(Default)]
pub struct NoWarningOnlyCompatibilityGates {
    emitted_hir: HashSet<HirId>,
}

impl<'tcx> LateLintPass<'tcx> for NoWarningOnlyCompatibilityGates {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        let ExprKind::If(cond, then_expr, else_expr) = expr.kind else {
            return;
        };

        if !is_scoped_file(cx, expr.span) {
            return;
        }
        if !is_compatibility_mismatch_condition(cx, cond.span) {
            return;
        }
        let Some(mismatch_branch) = mismatch_branch(cx, cond.span) else {
            return;
        };
        let mismatch_span = match mismatch_branch {
            MismatchBranch::Then => then_expr.span,
            MismatchBranch::Else => {
                let Some(else_expr) = else_expr else {
                    return;
                };
                else_expr.span
            }
        };
        if !is_warn_only_branch(cx, mismatch_span) {
            return;
        }
        if !self.emitted_hir.insert(expr.hir_id) {
            return;
        }

        span_lint_hir_and_then(
            cx,
            NO_WARNING_ONLY_COMPATIBILITY_GATES,
            expr.hir_id,
            expr.span,
            "compatibility mismatch is warning-only and does not hard-fail",
            |diag| {
                diag.help(
                    "return `Err(...)` (or propagate with `?`) before accepting/importing data",
                );
            },
        );
    }
}

pub fn register(lint_store: &mut rustc_lint::LintStore) {
    lint_store.register_lints(&[NO_WARNING_ONLY_COMPATIBILITY_GATES]);
    lint_store.register_late_pass(|_| Box::new(NoWarningOnlyCompatibilityGates::default()));
}

fn is_scoped_file(cx: &LateContext<'_>, span: Span) -> bool {
    let filename = format!("{:?}", cx.sess().source_map().span_to_filename(span));
    filename.contains("/runtime/core/checkpoint_import.rs")
        || filename.contains("\\runtime\\core\\checkpoint_import.rs")
        || filename.contains("/runtime/core/repo_load.rs")
        || filename.contains("\\runtime\\core\\repo_load.rs")
}

fn is_compatibility_mismatch_condition(cx: &LateContext<'_>, span: Span) -> bool {
    let Some(cond) = snippet_opt(cx, span) else {
        return false;
    };
    let cond = cond.replace(char::is_whitespace, "");
    let has_hash_subject = cond.contains("policy_hash") || cond.contains("roster_hash");
    let has_mismatch = cond.contains("!=") || cond.contains("==");
    has_hash_subject && has_mismatch
}

#[derive(Clone, Copy)]
enum MismatchBranch {
    Then,
    Else,
}

fn mismatch_branch(cx: &LateContext<'_>, span: Span) -> Option<MismatchBranch> {
    let cond = snippet_opt(cx, span)?.replace(char::is_whitespace, "");
    if cond.contains("!=") {
        Some(MismatchBranch::Then)
    } else if cond.contains("==") {
        Some(MismatchBranch::Else)
    } else {
        None
    }
}

fn is_warn_only_branch(cx: &LateContext<'_>, span: Span) -> bool {
    let Some(block) = snippet_opt(cx, span) else {
        return false;
    };
    let has_warn = block.contains("warn!(") || block.contains("tracing::warn!(");
    if !has_warn {
        return false;
    }

    // We only lint branches that do not hard-fail.
    let has_hard_fail = block.contains("return Err(")
        || block.contains("=> Err(")
        || block.contains("?;")
        || block.contains(")?")
        || block.contains("}?");
    !has_hard_fail
}
