use clippy_utils::diagnostics::span_lint_and_help;
use rustc_hir::{
    Arm, Block, Expr, ExprKind, ImplItem, ImplItemKind, QPath, Stmt, StmtKind, TyKind,
};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::Span;
use std::collections::HashSet;

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
        let mut analyzer = RotateReloadAnalyzer::default();
        let _ = analyzer.analyze_expr(body.value, initial_states());
        if analyzer.violation_spans.is_empty() {
            return;
        }

        analyzer
            .violation_spans
            .sort_unstable_by_key(|span| (span.lo(), span.hi()));
        analyzer
            .violation_spans
            .dedup_by_key(|span| (span.lo(), span.hi()));
        for success_span in analyzer.violation_spans {
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
struct RotateReloadAnalyzer {
    violation_spans: Vec<Span>,
}

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
struct PathState {
    saw_rotation: bool,
    reloaded_after_rotation: bool,
}

impl PathState {
    fn on_rotate(self) -> Self {
        Self {
            saw_rotation: true,
            reloaded_after_rotation: false,
        }
    }

    fn on_reload(self) -> Self {
        if self.saw_rotation {
            Self {
                reloaded_after_rotation: true,
                ..self
            }
        } else {
            self
        }
    }
}

type PathStates = HashSet<PathState>;

fn initial_states() -> PathStates {
    HashSet::from([PathState::default()])
}

impl RotateReloadAnalyzer {
    fn analyze_expr(&mut self, expr: &Expr<'_>, states: PathStates) -> PathStates {
        match expr.kind {
            ExprKind::Block(block, _) => self.analyze_block(block, states),
            ExprKind::If(cond, then_expr, else_expr) => {
                let states_after_cond = self.analyze_expr(cond, states);
                let then_states = self.analyze_expr(then_expr, states_after_cond.clone());
                let else_states = if let Some(else_expr) = else_expr {
                    self.analyze_expr(else_expr, states_after_cond)
                } else {
                    states_after_cond
                };
                union_states(then_states, else_states)
            }
            ExprKind::Match(scrutinee, arms, _) => {
                let states_after_scrutinee = self.analyze_expr(scrutinee, states);
                self.analyze_arms(arms, states_after_scrutinee)
            }
            ExprKind::MethodCall(seg, receiver, args, _) => {
                let mut states = self.analyze_expr(receiver, states);
                for arg in args {
                    states = self.analyze_expr(arg, states);
                }

                let name = seg.ident.name.as_str();
                if name == "rotate_replica_id" {
                    return states.into_iter().map(PathState::on_rotate).collect();
                } else if name == "reload_replication_runtime" {
                    return states.into_iter().map(PathState::on_reload).collect();
                }

                states
            }
            ExprKind::Call(callee, args) => {
                let mut states = self.analyze_expr(callee, states);
                for arg in args {
                    states = self.analyze_expr(arg, states);
                }
                if is_response_ok_call(callee)
                    && states
                        .iter()
                        .any(|state| state.saw_rotation && !state.reloaded_after_rotation)
                {
                    self.push_violation(expr.span);
                }
                states
            }
            ExprKind::Ret(Some(value)) => {
                let _ = self.analyze_expr(value, states);
                PathStates::new()
            }
            ExprKind::Ret(None) => PathStates::new(),
            ExprKind::Break(_, Some(value)) => {
                let _ = self.analyze_expr(value, states);
                PathStates::new()
            }
            ExprKind::Break(_, None) | ExprKind::Continue(_) => PathStates::new(),
            ExprKind::DropTemps(inner)
            | ExprKind::AddrOf(_, _, inner)
            | ExprKind::Field(inner, _)
            | ExprKind::Cast(inner, _)
            | ExprKind::Type(inner, _)
            | ExprKind::Unary(_, inner) => self.analyze_expr(inner, states),
            ExprKind::Binary(_, lhs, rhs)
            | ExprKind::Assign(lhs, rhs, _)
            | ExprKind::AssignOp(_, lhs, rhs)
            | ExprKind::Index(lhs, rhs, _) => {
                let states = self.analyze_expr(lhs, states);
                self.analyze_expr(rhs, states)
            }
            ExprKind::Array(exprs) | ExprKind::Tup(exprs) => {
                let mut states = states;
                for item in exprs {
                    states = self.analyze_expr(item, states);
                }
                states
            }
            ExprKind::Struct(_, fields, base) => {
                let mut states = states;
                for field in fields {
                    states = self.analyze_expr(field.expr, states);
                }
                let _ = base;
                states
            }
            ExprKind::Repeat(value, _) => self.analyze_expr(value, states),
            ExprKind::Let(let_expr) => self.analyze_expr(let_expr.init, states),
            ExprKind::Loop(block, ..) => {
                let _ = self.analyze_block(block, states.clone());
                states
            }
            _ => states,
        }
    }

    fn analyze_block(&mut self, block: &Block<'_>, mut states: PathStates) -> PathStates {
        for stmt in block.stmts {
            states = self.analyze_stmt(stmt, states);
            if states.is_empty() {
                return states;
            }
        }
        if let Some(tail_expr) = block.expr {
            self.analyze_expr(tail_expr, states)
        } else {
            states
        }
    }

    fn analyze_stmt(&mut self, stmt: &Stmt<'_>, states: PathStates) -> PathStates {
        match stmt.kind {
            StmtKind::Expr(expr) | StmtKind::Semi(expr) => self.analyze_expr(expr, states),
            StmtKind::Let(local) => {
                let mut states = states;
                if let Some(init) = local.init {
                    states = self.analyze_expr(init, states);
                }
                if let Some(els) = local.els {
                    let _ = self.analyze_block(els, states.clone());
                }
                states
            }
            StmtKind::Item(_) => states,
        }
    }

    fn analyze_arms(&mut self, arms: &[Arm<'_>], states: PathStates) -> PathStates {
        let mut merged = PathStates::new();
        for arm in arms {
            let mut arm_states = states.clone();
            if let Some(guard) = arm.guard {
                arm_states = self.analyze_expr(guard, arm_states);
            }
            merged.extend(self.analyze_expr(arm.body, arm_states));
        }
        merged
    }

    fn push_violation(&mut self, span: Span) {
        if self
            .violation_spans
            .iter()
            .all(|existing| existing.lo() != span.lo() || existing.hi() != span.hi())
        {
            self.violation_spans.push(span);
        }
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

fn union_states(mut left: PathStates, right: PathStates) -> PathStates {
    left.extend(right);
    left
}
