use clippy_utils::diagnostics::span_lint_and_help;
use clippy_utils::source::snippet_opt;
use rustc_hir::{ImplItem, ImplItemKind, Item, ItemKind, TraitItem, TraitItemKind};
use rustc_lint::{LateContext, LateLintPass, LintContext};
use rustc_span::Span;
use std::collections::HashSet;

rustc_session::declare_lint!(
    /// Enforce typed WAL boundary APIs (`WalCursorOffset` / `WatermarkPair`)
    /// instead of raw scalar boundary signatures.
    ///
    /// Scoped v1:
    /// - `crates/beads-daemon-core/src/wal/*`
    pub TYPED_WAL_BOUNDARIES_ONLY,
    Warn,
    "raw WAL boundary scalars used where typed forms exist"
);

rustc_session::impl_lint_pass!(TypedWalBoundariesOnly => [TYPED_WAL_BOUNDARIES_ONLY]);

#[derive(Default)]
pub struct TypedWalBoundariesOnly {
    emitted_at: HashSet<String>,
}

impl<'tcx> LateLintPass<'tcx> for TypedWalBoundariesOnly {
    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {
        if !is_scoped_wal_file(cx, item.span) {
            return;
        }

        match &item.kind {
            ItemKind::Struct(_, _, def) => {
                for field in def.fields() {
                    if field.ident.name.as_str() == "last_indexed_offset"
                        && is_raw_u64(cx, field.ty.span)
                    {
                        self.emit(
                            cx,
                            field.span,
                            "WAL boundary `last_indexed_offset` must use `WalCursorOffset`, not `u64`",
                            "replace `u64` with `WalCursorOffset` at this boundary",
                        );
                    }
                }
            }
            ItemKind::Enum(_, _, def) => {
                for variant in def.variants {
                    for field in variant.data.fields() {
                        if field.ident.name.as_str() == "last_indexed_offset"
                            && is_raw_u64(cx, field.ty.span)
                        {
                            self.emit(
                                cx,
                                field.span,
                                "WAL boundary `last_indexed_offset` must use `WalCursorOffset`, not `u64`",
                                "replace `u64` with `WalCursorOffset` at this boundary",
                            );
                        }
                    }
                }
            }
            ItemKind::Fn { .. } => {
                self.check_signature_text(cx, item.span);
            }
            _ => {}
        }
    }

    fn check_trait_item(&mut self, cx: &LateContext<'tcx>, trait_item: &'tcx TraitItem<'tcx>) {
        if !is_scoped_wal_file(cx, trait_item.span) {
            return;
        }
        if matches!(trait_item.kind, TraitItemKind::Fn(_, _)) {
            self.check_signature_text(cx, trait_item.span);
        }
    }

    fn check_impl_item(&mut self, cx: &LateContext<'tcx>, impl_item: &'tcx ImplItem<'tcx>) {
        if !is_scoped_wal_file(cx, impl_item.span) {
            return;
        }
        if matches!(impl_item.kind, ImplItemKind::Fn(..)) {
            self.check_signature_text(cx, impl_item.span);
        }
    }
}

impl TypedWalBoundariesOnly {
    fn check_signature_text(&mut self, cx: &LateContext<'_>, span: Span) {
        let Some(sig) = snippet_opt(cx, span) else {
            return;
        };
        let normalized = normalize(&sig);

        if normalized.contains("last_indexed_offset:u64")
            || (normalized.contains("fnlast_indexed_offset(") && normalized.contains(")->u64"))
        {
            self.emit(
                cx,
                span,
                "WAL cursor boundary uses raw `u64`",
                "use `WalCursorOffset` for WAL cursor boundary parameters/returns",
            );
        }

        if normalized.contains("fnupdate_watermark(")
            && normalized.contains("applied:")
            && normalized.contains("durable:")
            && !normalized.contains("watermarks:")
        {
            self.emit(
                cx,
                span,
                "`update_watermark` splits applied/durable scalars at WAL boundary",
                "use a single `WatermarkPair` parameter at WAL boundary APIs",
            );
        }
    }

    fn emit(
        &mut self,
        cx: &LateContext<'_>,
        span: Span,
        message: &'static str,
        help: &'static str,
    ) {
        let source_map = cx.sess().source_map();
        let loc = source_map.lookup_char_pos(span.lo());
        let key = format!(
            "{:?}:{}:{}:{message}",
            source_map.span_to_filename(span),
            loc.line,
            loc.col_display + 1
        );
        if !self.emitted_at.insert(key) {
            return;
        }
        span_lint_and_help(cx, TYPED_WAL_BOUNDARIES_ONLY, span, message, None, help);
    }
}

pub fn register(lint_store: &mut rustc_lint::LintStore) {
    lint_store.register_lints(&[TYPED_WAL_BOUNDARIES_ONLY]);
    lint_store.register_late_pass(|_| Box::new(TypedWalBoundariesOnly::default()));
}

fn is_scoped_wal_file(cx: &LateContext<'_>, span: Span) -> bool {
    let filename = format!("{:?}", cx.sess().source_map().span_to_filename(span));
    filename.contains("/crates/beads-daemon-core/src/wal/")
        || filename.contains("\\crates\\beads-daemon-core\\src\\wal\\")
}

fn is_raw_u64(cx: &LateContext<'_>, span: Span) -> bool {
    snippet_opt(cx, span)
        .map(|text| normalize(&text) == "u64")
        .unwrap_or(false)
}

fn normalize(text: &str) -> String {
    text.replace(char::is_whitespace, "")
}
