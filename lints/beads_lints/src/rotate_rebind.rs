use clippy_utils::diagnostics::span_lint_and_help;
use rustc_hir::intravisit::{walk_expr, Visitor};
use rustc_hir::{Expr, ExprKind, ImplItem, ImplItemKind, QPath, TyKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::Span;

rustc_session::declare_lint!(
    /// Require explicit replication runtime reload after replica-id rotation.
    ///
    /// This targets `admin_rotate_replica_id` and ensures success paths do not
    /// return before calling `reload_replication_runtime(store_id)`.
    pub ROTATE_REQUIRES_REBIND,
    Warn,
    "admin_rotate_replica_id must reload replication runtime before success return"
);

rustc_session::impl_lint_pass!(RotateRequiresRebind => [ROTATE_REQUIRES_REBIND]);

#[derive(Default)]
pub struct RotateRequiresRebind;

pub fn register(lint_store: &mut rustc_lint::LintStore) {
    lint_store.register_lints(&[ROTATE_REQUIRES_REBIND]);
    lint_store.register_late_pass(|_| Box::new(RotateRequiresRebind));
}

impl<'tcx> LateLintPass<'tcx> for RotateRequiresRebind {
    fn check_impl_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx ImplItem<'tcx>) {
        if item.ident.name.as_str() != "admin_rotate_replica_id" {
            return;
        }

        let ImplItemKind::Fn(_, body) = item.kind else {
            return;
        };

        let body = cx.tcx.hir_body(body);
        let mut finder = RotateReloadFinder::default();
        finder.visit_expr(&body.value);

        let Some(rotate_span) = first_span(&finder.rotate_calls) else {
            return;
        };
        let success_returns = spans_after(&finder.success_returns, rotate_span);
        if success_returns.is_empty() {
            return;
        }

        for success_span in success_returns {
            let has_reload = finder
                .reload_calls
                .iter()
                .any(|span| span_between(*span, rotate_span, success_span));
            if has_reload {
                continue;
            }

            span_lint_and_help(
                cx,
                ROTATE_REQUIRES_REBIND,
                success_span,
                "`admin_rotate_replica_id` returns success without reloading replication runtime after rotation",
                None,
                "call `reload_replication_runtime(store_id)` after `rotate_replica_id` and before `Response::ok(...)`",
            );
        }
    }
}

#[derive(Default)]
struct RotateReloadFinder {
    rotate_calls: Vec<Span>,
    reload_calls: Vec<Span>,
    success_returns: Vec<Span>,
}

impl<'v> Visitor<'v> for RotateReloadFinder {
    fn visit_expr(&mut self, expr: &'v Expr<'v>) {
        match expr.kind {
            ExprKind::MethodCall(seg, ..) => {
                let name = seg.ident.name.as_str();
                if name == "rotate_replica_id" {
                    self.rotate_calls.push(expr.span);
                } else if name == "reload_replication_runtime" {
                    self.reload_calls.push(expr.span);
                }
            }
            ExprKind::Call(callee, ..) => {
                if is_response_ok_call(callee) {
                    self.success_returns.push(expr.span);
                }
            }
            _ => {}
        }

        walk_expr(self, expr);
    }
}

fn is_response_ok_call(expr: &Expr<'_>) -> bool {
    let ExprKind::Path(ref qpath) = expr.kind else {
        return false;
    };
    match qpath {
        QPath::Resolved(_, path) => {
            if path.segments.len() < 2 {
                return false;
            }
            let response = path.segments[path.segments.len() - 2].ident.name.as_str();
            let ok = path.segments[path.segments.len() - 1].ident.name.as_str();
            response == "Response" && ok == "ok"
        }
        QPath::TypeRelative(ty, seg) => {
            if seg.ident.name.as_str() != "ok" {
                return false;
            }
            let TyKind::Path(QPath::Resolved(_, path)) = ty.kind else {
                return false;
            };
            path.segments
                .last()
                .is_some_and(|segment| segment.ident.name.as_str() == "Response")
        }
    }
}

fn first_span(spans: &[Span]) -> Option<Span> {
    spans.iter().copied().min_by_key(|span| span.lo())
}

fn spans_after(spans: &[Span], pivot: Span) -> Vec<Span> {
    let mut filtered = spans
        .iter()
        .copied()
        .filter(|span| span.lo() > pivot.lo())
        .collect::<Vec<_>>();
    filtered.sort_unstable_by_key(|span| span.lo());
    filtered
}

fn span_between(candidate: Span, low: Span, high: Span) -> bool {
    candidate.lo() > low.lo() && candidate.lo() < high.lo()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_between_is_exclusive() {
        let low = Span::with_root_ctxt(rustc_span::BytePos(5), rustc_span::BytePos(6));
        let mid = Span::with_root_ctxt(rustc_span::BytePos(7), rustc_span::BytePos(8));
        let high = Span::with_root_ctxt(rustc_span::BytePos(9), rustc_span::BytePos(10));

        assert!(span_between(mid, low, high));
        assert!(!span_between(low, low, high));
        assert!(!span_between(high, low, high));
    }
}
