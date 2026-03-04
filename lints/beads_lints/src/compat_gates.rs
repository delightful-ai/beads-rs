use clippy_utils::diagnostics::span_lint_hir_and_then;
use clippy_utils::source::snippet_opt;
use rustc_hir::{BinOpKind, Expr, ExprKind, HirId, StmtKind, UnOp};
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
        let Some(mismatch_branch) = mismatch_branch(cx, cond) else {
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

#[derive(Clone, Copy, Eq, PartialEq)]
enum MismatchBranch {
    Then,
    Else,
}

fn mismatch_branch(cx: &LateContext<'_>, cond: &Expr<'_>) -> Option<MismatchBranch> {
    let mut branches = Vec::new();
    collect_hash_comparison_branches(cx, cond, false, &mut branches);
    let first = branches.first().copied()?;
    if branches.iter().all(|branch| *branch == first) {
        Some(first)
    } else {
        None
    }
}

fn collect_hash_comparison_branches(
    cx: &LateContext<'_>,
    expr: &Expr<'_>,
    negated: bool,
    branches: &mut Vec<MismatchBranch>,
) {
    match expr.kind {
        ExprKind::Unary(UnOp::Not, inner) => {
            collect_hash_comparison_branches(cx, inner, !negated, branches);
        }
        ExprKind::Binary(op, lhs, rhs) => {
            if matches!(op.node, BinOpKind::Eq | BinOpKind::Ne)
                && (contains_hash_subject_expr(cx, lhs) || contains_hash_subject_expr(cx, rhs))
            {
                branches.push(comparison_mismatch_branch(op.node, negated));
                return;
            }
            collect_hash_comparison_branches(cx, lhs, negated, branches);
            collect_hash_comparison_branches(cx, rhs, negated, branches);
        }
        ExprKind::DropTemps(inner)
        | ExprKind::AddrOf(_, _, inner)
        | ExprKind::Field(inner, _)
        | ExprKind::Cast(inner, _)
        | ExprKind::Type(inner, _)
        | ExprKind::Unary(_, inner) => collect_hash_comparison_branches(cx, inner, negated, branches),
        ExprKind::Let(let_expr) => {
            collect_hash_comparison_branches(cx, let_expr.init, negated, branches);
        }
        ExprKind::Block(block, _) => {
            collect_hash_comparison_branches_in_block(cx, block, negated, branches);
        }
        _ => {}
    }
}

fn collect_hash_comparison_branches_in_block(
    cx: &LateContext<'_>,
    block: &rustc_hir::Block<'_>,
    negated: bool,
    branches: &mut Vec<MismatchBranch>,
) {
    for stmt in block.stmts {
        match stmt.kind {
            StmtKind::Let(let_stmt) => {
                if let Some(init) = let_stmt.init {
                    collect_hash_comparison_branches(cx, init, negated, branches);
                }
                if let Some(else_block) = let_stmt.els {
                    collect_hash_comparison_branches_in_block(cx, else_block, negated, branches);
                }
            }
            StmtKind::Expr(stmt_expr) | StmtKind::Semi(stmt_expr) => {
                collect_hash_comparison_branches(cx, stmt_expr, negated, branches);
            }
            StmtKind::Item(_) => {}
        }
    }
    if let Some(tail) = block.expr {
        collect_hash_comparison_branches(cx, tail, negated, branches);
    }
}

fn contains_hash_subject_expr(cx: &LateContext<'_>, expr: &Expr<'_>) -> bool {
    let Some(snippet) = snippet_opt(cx, expr.span) else {
        return false;
    };
    let text = snippet.replace(char::is_whitespace, "");
    text.contains("policy_hash") || text.contains("roster_hash")
}

fn comparison_mismatch_branch(op: BinOpKind, negated: bool) -> MismatchBranch {
    let branch = match op {
        BinOpKind::Ne => MismatchBranch::Then,
        BinOpKind::Eq => MismatchBranch::Else,
        _ => unreachable!("only Eq/Ne should call comparison_mismatch_branch"),
    };
    if negated {
        match branch {
            MismatchBranch::Then => MismatchBranch::Else,
            MismatchBranch::Else => MismatchBranch::Then,
        }
    } else {
        branch
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
