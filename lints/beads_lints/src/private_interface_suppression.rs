use clippy_utils::diagnostics::span_lint_hir_and_then;
use clippy_utils::{is_cfg_test, is_in_test};
use rustc_ast::Path;
use rustc_hir::{self as hir, ForeignItem, ImplItem, Item, TraitItem};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_span::sym;

rustc_session::declare_lint!(
    /// Forbid suppressing `private_interfaces` outside true test context.
    ///
    /// This catches:
    /// - `#[allow(private_interfaces)]`
    /// - `#[allow(rustc::private_interfaces)]`
    /// - crate-level `#![allow(...)]`
    pub FORBID_PRIVATE_INTERFACE_SUPPRESSION,
    Warn,
    "forbidden suppression of private_interfaces outside test-only context"
);

rustc_session::impl_lint_pass!(
    ForbidPrivateInterfaceSuppression => [FORBID_PRIVATE_INTERFACE_SUPPRESSION]
);

#[derive(Default)]
pub struct ForbidPrivateInterfaceSuppression;

pub fn register(lint_store: &mut rustc_lint::LintStore) {
    lint_store.register_lints(&[FORBID_PRIVATE_INTERFACE_SUPPRESSION]);
    lint_store.register_late_pass(|_| Box::new(ForbidPrivateInterfaceSuppression));
}

impl<'tcx> LateLintPass<'tcx> for ForbidPrivateInterfaceSuppression {
    fn check_crate(&mut self, cx: &LateContext<'tcx>) {
        self.check_owner_attrs(cx, hir::CRATE_HIR_ID, true);
    }

    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {
        self.check_owner_attrs(cx, item.hir_id(), false);
    }

    fn check_trait_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx TraitItem<'tcx>) {
        self.check_owner_attrs(cx, item.hir_id(), false);
    }

    fn check_impl_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx ImplItem<'tcx>) {
        self.check_owner_attrs(cx, item.hir_id(), false);
    }

    fn check_foreign_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx ForeignItem<'tcx>) {
        self.check_owner_attrs(cx, item.hir_id(), false);
    }
}

impl ForbidPrivateInterfaceSuppression {
    fn check_owner_attrs(&mut self, cx: &LateContext<'_>, owner_hir_id: hir::HirId, is_crate: bool) {
        if is_exempt_test_context(cx, owner_hir_id, is_crate) {
            return;
        }

        for attr in cx.tcx.hir_attrs(owner_hir_id) {
            let Some(kind) = private_interfaces_suppression_kind(attr) else {
                continue;
            };

            let message = match kind {
                SuppressionKind::Plain => {
                    "`allow(private_interfaces)` is forbidden outside test-only code"
                }
                SuppressionKind::Namespaced => {
                    "`allow(rustc::private_interfaces)` is forbidden outside test-only code"
                }
            };

            span_lint_hir_and_then(
                cx,
                FORBID_PRIVATE_INTERFACE_SUPPRESSION,
                owner_hir_id,
                attr.span(),
                message,
                |diag| {
                    diag.help(
                        "remove this suppression and narrow item visibility or narrow re-export visibility instead",
                    );
                },
            );
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SuppressionKind {
    Plain,
    Namespaced,
}

fn is_exempt_test_context(cx: &LateContext<'_>, owner_hir_id: hir::HirId, is_crate: bool) -> bool {
    if is_crate {
        // Crate-level inner attrs don't have a parent node. In test harness builds,
        // treat crate-level suppressions as test-context.
        return cx.sess().opts.test || is_cfg_test(cx.tcx, owner_hir_id);
    }

    is_cfg_test(cx.tcx, owner_hir_id) || is_in_test(cx.tcx, owner_hir_id)
}

fn private_interfaces_suppression_kind(attr: &hir::Attribute) -> Option<SuppressionKind> {
    if !attr.has_name(sym::allow) {
        return None;
    }

    let entries = attr.meta_item_list()?;
    for entry in entries {
        let Some(meta) = entry.meta_item() else {
            continue;
        };

        if is_plain_private_interfaces_path(&meta.path) {
            return Some(SuppressionKind::Plain);
        }
        if is_namespaced_private_interfaces_path(&meta.path) {
            return Some(SuppressionKind::Namespaced);
        }
    }

    None
}

fn is_plain_private_interfaces_path(path: &Path) -> bool {
    matches!(path.segments.as_slice(), [leaf] if leaf.ident.name.as_str() == "private_interfaces")
}

fn is_namespaced_private_interfaces_path(path: &Path) -> bool {
    matches!(
        path.segments.as_slice(),
        [prefix, leaf]
            if prefix.ident.name == sym::rustc && leaf.ident.name.as_str() == "private_interfaces"
    )
}