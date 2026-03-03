use clippy_utils::diagnostics::span_lint_and_help;
use clippy_utils::source::snippet_opt;
use rustc_hir::intravisit::{walk_expr, Visitor};
use rustc_hir::{Arm, Expr, ExprKind, Item, ItemKind, QPath};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_span::Span;

rustc_session::declare_lint!(
    /// Guard replay catch-up atomicity in `wal/replay.rs::replay_index`.
    ///
    /// Catch-up must not split rows/segments and frontier updates across multiple
    /// transaction lifecycles, and must not commit before frontier updates.
    pub REPLAY_ATOMIC_FRONTIER_COMMIT_GUARD,
    Warn,
    "replay catch-up path splits transaction lifecycle or commits before frontier updates"
);

rustc_session::impl_lint_pass!(
    ReplayAtomicFrontierCommitGuard => [REPLAY_ATOMIC_FRONTIER_COMMIT_GUARD]
);

#[derive(Default)]
pub struct ReplayAtomicFrontierCommitGuard;

pub fn register(lint_store: &mut rustc_lint::LintStore) {
    lint_store.register_lints(&[REPLAY_ATOMIC_FRONTIER_COMMIT_GUARD]);
    lint_store.register_late_pass(|_| Box::new(ReplayAtomicFrontierCommitGuard));
}

impl<'tcx> LateLintPass<'tcx> for ReplayAtomicFrontierCommitGuard {
    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {
        let ItemKind::Fn { ident, body, .. } = item.kind else {
            return;
        };
        if ident.name.as_str() != "replay_index" {
            return;
        }
        if !is_scoped_replay_file(cx, item.span) {
            return;
        }

        let body = cx.tcx.hir_body(body);
        let mut analyzer = ReplayAnalyzer::new(cx);
        analyzer.visit_expr(body.value);

        let begin_sorted = sorted_by_lo(&analyzer.catchup_begin_txn_calls);
        if begin_sorted.len() > 1 {
            let second_begin = begin_sorted[1];
            span_lint_and_help(
                cx,
                REPLAY_ATOMIC_FRONTIER_COMMIT_GUARD,
                second_begin,
                "`ReplayMode::CatchUp` in `replay_index` uses multiple transaction lifecycles",
                None,
                "keep catch-up rows/segments and frontier updates in one transaction with a single terminal commit",
            );
            return;
        }

        let frontier_sorted = sorted_by_lo(&analyzer.catchup_frontier_update_calls);
        let commit_sorted = sorted_by_lo(&analyzer.catchup_commit_calls);
        let Some(first_frontier) = frontier_sorted.first().copied() else {
            return;
        };
        if let Some(early_commit) = commit_sorted
            .into_iter()
            .find(|commit| commit.lo() < first_frontier.lo())
        {
            span_lint_and_help(
                cx,
                REPLAY_ATOMIC_FRONTIER_COMMIT_GUARD,
                early_commit,
                "`ReplayMode::CatchUp` commits before frontier updates are applied",
                None,
                "apply frontier updates before the single catch-up commit",
            );
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ModeContext {
    Unknown,
    CatchUp,
    Rebuild,
}

struct ReplayAnalyzer<'a, 'tcx> {
    cx: &'a LateContext<'tcx>,
    mode_stack: Vec<ModeContext>,
    catchup_begin_txn_calls: Vec<Span>,
    catchup_commit_calls: Vec<Span>,
    catchup_frontier_update_calls: Vec<Span>,
}

impl<'a, 'tcx> ReplayAnalyzer<'a, 'tcx> {
    fn new(cx: &'a LateContext<'tcx>) -> Self {
        Self {
            cx,
            mode_stack: vec![ModeContext::Unknown],
            catchup_begin_txn_calls: Vec::new(),
            catchup_commit_calls: Vec::new(),
            catchup_frontier_update_calls: Vec::new(),
        }
    }

    fn current_mode(&self) -> ModeContext {
        *self
            .mode_stack
            .last()
            .expect("mode context stack is never empty")
    }

    fn with_mode<F>(&mut self, mode: ModeContext, f: F)
    where
        F: FnOnce(&mut Self),
    {
        self.mode_stack.push(mode);
        f(self);
        self.mode_stack.pop();
    }
}

impl<'v, 'tcx> Visitor<'v> for ReplayAnalyzer<'_, 'tcx> {
    fn visit_expr(&mut self, expr: &'v Expr<'v>) {
        if self.current_mode() == ModeContext::CatchUp {
            match expr.kind {
                ExprKind::MethodCall(segment, ..) => match segment.ident.name.as_str() {
                    "begin_txn" => self.catchup_begin_txn_calls.push(expr.span),
                    "commit" => self.catchup_commit_calls.push(expr.span),
                    "apply_frontier_updates" => self.catchup_frontier_update_calls.push(expr.span),
                    _ => {}
                },
                ExprKind::Call(callee, ..) => {
                    if call_name(callee) == Some("apply_frontier_updates") {
                        self.catchup_frontier_update_calls.push(expr.span);
                    }
                }
                _ => {}
            }
        }

        match expr.kind {
            ExprKind::If(cond, then_expr, else_expr) => {
                self.visit_expr(cond);
                match condition_modes(self.cx, cond.span) {
                    Some((then_mode, else_mode)) => {
                        self.with_mode(then_mode, |this| this.visit_expr(then_expr));
                        if let Some(else_expr) = else_expr {
                            self.with_mode(else_mode, |this| this.visit_expr(else_expr));
                        }
                    }
                    None => {
                        self.visit_expr(then_expr);
                        if let Some(else_expr) = else_expr {
                            self.visit_expr(else_expr);
                        }
                    }
                }
                return;
            }
            ExprKind::Match(scrutinee, arms, _) => {
                self.visit_expr(scrutinee);
                for arm in arms {
                    if let Some(guard_expr) = arm.guard {
                        self.visit_expr(guard_expr);
                    }
                    let arm_mode = arm_mode(self.cx, arm).unwrap_or(self.current_mode());
                    self.with_mode(arm_mode, |this| this.visit_expr(arm.body));
                }
                return;
            }
            _ => {}
        }

        walk_expr(self, expr);
    }
}

fn call_name<'hir>(expr: &'hir Expr<'hir>) -> Option<&'hir str> {
    let ExprKind::Path(ref qpath) = expr.kind else {
        return None;
    };
    match qpath {
        QPath::Resolved(_, path) => path
            .segments
            .last()
            .map(|segment| segment.ident.name.as_str()),
        QPath::TypeRelative(_, segment) => Some(segment.ident.name.as_str()),
    }
}

fn condition_modes(cx: &LateContext<'_>, span: Span) -> Option<(ModeContext, ModeContext)> {
    let normalized = normalize(&snippet_opt(cx, span)?);
    if is_mode_equality(&normalized, "ReplayMode::CatchUp") {
        return Some((ModeContext::CatchUp, ModeContext::Rebuild));
    }
    if is_mode_inequality(&normalized, "ReplayMode::CatchUp") {
        return Some((ModeContext::Rebuild, ModeContext::CatchUp));
    }
    if is_mode_equality(&normalized, "ReplayMode::Rebuild") {
        return Some((ModeContext::Rebuild, ModeContext::CatchUp));
    }
    if is_mode_inequality(&normalized, "ReplayMode::Rebuild") {
        return Some((ModeContext::CatchUp, ModeContext::Rebuild));
    }
    None
}

fn is_mode_equality(normalized: &str, variant: &str) -> bool {
    normalized.contains(&format!("mode=={variant}"))
        || normalized.contains(&format!("{variant}==mode"))
}

fn is_mode_inequality(normalized: &str, variant: &str) -> bool {
    normalized.contains(&format!("mode!={variant}"))
        || normalized.contains(&format!("{variant}!=mode"))
}

fn arm_mode(cx: &LateContext<'_>, arm: &Arm<'_>) -> Option<ModeContext> {
    let normalized = normalize(&snippet_opt(cx, arm.pat.span)?);
    if normalized.contains("ReplayMode::CatchUp") {
        return Some(ModeContext::CatchUp);
    }
    if normalized.contains("ReplayMode::Rebuild") {
        return Some(ModeContext::Rebuild);
    }
    None
}

fn is_scoped_replay_file(cx: &LateContext<'_>, span: Span) -> bool {
    let filename = format!("{:?}", cx.sess().source_map().span_to_filename(span));
    let in_wal_dir = filename.contains("/crates/beads-daemon-core/src/wal/")
        || filename.contains("\\crates\\beads-daemon-core\\src\\wal\\");
    let is_replay_file = filename.contains("/replay.rs") || filename.contains("\\replay.rs");
    in_wal_dir && is_replay_file
}

fn sorted_by_lo(spans: &[Span]) -> Vec<Span> {
    let mut out = spans.to_vec();
    out.sort_unstable_by_key(|span| span.lo());
    out
}

fn normalize(text: &str) -> String {
    text.replace(char::is_whitespace, "")
}
