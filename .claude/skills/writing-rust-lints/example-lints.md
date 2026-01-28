# Example Lints

Real-world examples from the Dylint repository showing common lint patterns.

## 1. Basic Dead Store (Beginner Example)

Detects dead stores in arrays - when an array position is assigned twice without reading.

### Source: `examples/general/basic_dead_store/src/lib.rs`

```rust
#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_ast;
extern crate rustc_hir;
extern crate rustc_span;

use clippy_utils::{diagnostics::span_lint_and_help, get_parent_expr};
use rustc_ast::LitKind;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::Span;

dylint_linting::impl_late_lint! {
    /// ### What it does
    ///
    /// Finds instances of dead stores in arrays: array positions that are assigned twice without a
    /// use or read in between.
    ///
    /// ### Why is this bad?
    ///
    /// A dead store might indicate a logic error in the program or an unnecessary assignment.
    ///
    /// ### Known problems
    ///
    /// This lint only checks for literal indices and will not try to find instances where an array
    /// is indexed by a variable.
    ///
    /// ### Example
    ///
    /// ```rust
    /// let mut arr = [0u64; 2];
    /// arr[0] = 1;
    /// arr[0] = 2;
    /// ```
    ///
    /// Use instead:
    ///
    /// ```rust
    /// let mut arr = [0u64; 2];
    /// arr[0] = 2;
    /// arr[1] = 1;
    /// ```
    pub BASIC_DEAD_STORE,
    Warn,
    "An array element is assigned twice without a use or read in between",
    BasicDeadStore::default()
}

#[derive(Default)]
pub struct BasicDeadStore {
    /// Stores instances of array-indexing with literal (array name, index, span)
    arr_and_idx_vec: Vec<(String, u128, Span)>,
}

impl BasicDeadStore {
    fn clear_stores_of(&mut self, string: &str) {
        self.arr_and_idx_vec
            .retain(|(arr_string, _idx, _span)| arr_string != string);
    }

    fn get_pairs_with_same_name_idx(
        &self,
        string: &String,
        idx: &u128,
    ) -> Vec<&(String, u128, Span)> {
        self.arr_and_idx_vec
            .iter()
            .filter(|(arr_string, arr_idx, _span)| arr_string == string && arr_idx == idx)
            .collect()
    }
}

fn is_assignment_to_array_indexed_by_literal(
    expr: &Expr,
    tcx: &LateContext<'_>,
) -> Option<(u128, Span)> {
    let index_expr = get_parent_expr(tcx, expr)?;
    if let ExprKind::Index(array, index, _span) = index_expr.kind
        && array.hir_id == expr.hir_id
        && let assign_expr = get_parent_expr(tcx, index_expr)?
        && let ExprKind::Assign(target, _value, assignment_span) = assign_expr.kind
        && target.hir_id == index_expr.hir_id
        && let ExprKind::Lit(lit) = index.kind
        && let LitKind::Int(index, _type) = lit.node
    {
        return Some((index.get(), assignment_span));
    }
    None
}

impl<'tcx> LateLintPass<'tcx> for BasicDeadStore {
    fn check_expr(&mut self, ctx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        if let ExprKind::Path(ref qpath) = expr.kind {
            let array_resolution = ctx.qpath_res(qpath, expr.hir_id);
            let array_name = format!("{array_resolution:?}");

            if let Some((v, span)) = is_assignment_to_array_indexed_by_literal(expr, ctx) {
                let in_common = self.get_pairs_with_same_name_idx(&array_name, &v);
                if in_common.is_empty() {
                    self.arr_and_idx_vec.push((array_name, v, span));
                } else {
                    span_lint_and_help(
                        ctx,
                        BASIC_DEAD_STORE,
                        span,
                        "reassigning the same array position without using it",
                        Some(in_common.first().unwrap().2),
                        "original assignment was here",
                    );
                }
            } else {
                self.clear_stores_of(&array_name);
            }
        }
    }
}

#[test]
fn ui() {
    dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
}
```

### Key Patterns:
- Uses `impl_late_lint!` for custom state (`BasicDeadStore::default()`)
- Maintains state across expression checks (`arr_and_idx_vec`)
- Uses `get_parent_expr` to navigate expression tree
- Pattern matching on `ExprKind::Index` and `ExprKind::Assign`
- Uses `LitKind::Int` to check literal index values

---

## 2. Env Literal (Path Matching Example)

Detects environment variables referred to with string literals instead of constants.

### Source: `examples/restriction/env_literal/src/lib.rs`

```rust
#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_ast;
extern crate rustc_hir;

use clippy_utils::{
    diagnostics::span_lint_and_help,
    paths::{PathLookup, PathNS, lookup_path_str},
    res::MaybeResPath,
    sym, value_path,
};
use dylint_internal::paths;
use rustc_ast::LitKind;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};

dylint_linting::declare_late_lint! {
    /// ### What it does
    ///
    /// Checks for environment variables referred to with string literals.
    ///
    /// ### Why is this bad?
    ///
    /// A typo in the string literal will result in a runtime error, not a compile time error.
    ///
    /// ### Example
    ///
    /// ```rust
    /// let _ = std::env::var("RUSTFLAGS");
    /// unsafe {
    ///     std::env::remove_var("RUSTFALGS"); // Oops
    /// }
    /// ```
    ///
    /// Use instead:
    ///
    /// ```rust
    /// const RUSTFLAGS: &str = "RUSTFLAGS";
    /// let _ = std::env::var(RUSTFLAGS);
    /// unsafe {
    ///     std::env::remove_var(RUSTFLAGS);
    /// }
    /// ```
    pub ENV_LITERAL,
    Warn,
    "environment variables referred to with string literals"
}

static ENV_VAR: PathLookup = value_path!(std::env::var);

impl<'tcx> LateLintPass<'tcx> for EnvLiteral {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &Expr<'tcx>) {
        if let ExprKind::Call(callee, args) = expr.kind
            && let Some(def_id) = callee.basic_res().opt_def_id()
            && (lookup_path_str(cx.tcx, PathNS::Value, &paths::ENV_REMOVE_VAR.join("::"))
                == [def_id]
                || lookup_path_str(cx.tcx, PathNS::Value, &paths::ENV_SET_VAR.join("::"))
                    == [def_id]
                || ENV_VAR.matches_path(cx, callee))
            && !args.is_empty()
            && let ExprKind::Lit(lit) = &args[0].kind
            && let LitKind::Str(symbol, _) = lit.node
            && let s = symbol.to_ident_string()
            && is_upper_snake_case(&s)
        {
            span_lint_and_help(
                cx,
                ENV_LITERAL,
                args[0].span,
                "referring to an environment variable with a string literal is error prone",
                None,
                format!("define a constant `{s}` and use that instead"),
            );
        }
    }
}

fn is_upper_snake_case(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_uppercase() || c == '_')
}

#[test]
fn ui() {
    dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
}
```

### Key Patterns:
- Uses `declare_late_lint!` (no custom state needed)
- `PathLookup` with `value_path!` for matching function paths
- `MaybeResPath::basic_res().opt_def_id()` to get definition ID
- `lookup_path_str` for dynamic path matching
- Checking literal string arguments with `LitKind::Str`

---

## 3. Configurable Lint Example

Lint with configuration read from `dylint.toml`.

### Source: `examples/general/non_local_effect_before_error_return/src/lib.rs` (simplified)

```rust
#![feature(rustc_private)]

extern crate rustc_hir;
extern crate rustc_middle;
extern crate rustc_span;

use rustc_hir::intravisit::FnKind;
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::Span;
use serde::Deserialize;

dylint_linting::impl_late_lint! {
    /// ### What it does
    ///
    /// Checks for non-local effects before return of an error.
    ///
    /// ### Why is this bad?
    ///
    /// Functions that make changes to the program state before returning an error
    /// are difficult to reason about.
    ///
    /// ### Configuration
    ///
    /// - `public_only: bool` (default `true`): Whether to check only public functions.
    /// - `work_limit: u64` (default 500000): Maximum search path extensions.
    pub NON_LOCAL_EFFECT_BEFORE_ERROR_RETURN,
    Warn,
    "non-local effects before return of an error",
    NonLocalEffectBeforeErrorReturn::new()
}

#[derive(Deserialize)]
struct Config {
    public_only: Option<bool>,
    work_limit: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            public_only: Some(true),
            work_limit: Some(500_000),
        }
    }
}

struct NonLocalEffectBeforeErrorReturn {
    config: Config,
}

impl NonLocalEffectBeforeErrorReturn {
    pub fn new() -> Self {
        Self {
            config: dylint_linting::config_or_default(env!("CARGO_PKG_NAME")),
        }
    }
}

impl<'tcx> LateLintPass<'tcx> for NonLocalEffectBeforeErrorReturn {
    fn check_fn(
        &mut self,
        cx: &LateContext<'tcx>,
        fn_kind: FnKind<'tcx>,
        _: &'tcx rustc_hir::FnDecl<'_>,
        body: &'tcx rustc_hir::Body<'_>,
        span: Span,
        local_def_id: rustc_hir::def_id::LocalDefId,
    ) {
        // Skip macro-generated code
        if span.from_expansion() {
            return;
        }

        // Check public_only configuration
        if self.config.public_only.unwrap_or(true)
            && !cx.effective_visibilities.is_exported(local_def_id)
        {
            return;
        }

        // ... rest of lint implementation
    }
}

#[test]
fn ui() {
    dylint_testing::ui_test_example(env!("CARGO_PKG_NAME"), "ui");
}

#[test]
fn ui_with_config() {
    dylint_testing::ui::Test::example(env!("CARGO_PKG_NAME"), "ui_public_only")
        .dylint_toml("non_local_effect_before_error_return.public_only = false")
        .run();
}
```

### Configuration in `dylint.toml`:
```toml
[non_local_effect_before_error_return]
public_only = false
work_limit = 1000000
```

### Key Patterns:
- Config struct with `serde::Deserialize`
- `Option<T>` fields with defaults via `Default` impl
- `dylint_linting::config_or_default(env!("CARGO_PKG_NAME"))`
- Testing with custom config via `.dylint_toml()`

---

## 4. Template (Starter)

The template generated by `cargo dylint new`.

### Source: `internal/template/src/lib.rs`

```rust
#![feature(rustc_private)]

extern crate rustc_arena;
extern crate rustc_ast;
extern crate rustc_ast_pretty;
extern crate rustc_data_structures;
extern crate rustc_errors;
extern crate rustc_hir;
extern crate rustc_hir_pretty;
extern crate rustc_index;
extern crate rustc_infer;
extern crate rustc_lexer;
extern crate rustc_middle;
extern crate rustc_mir_dataflow;
extern crate rustc_parse;
extern crate rustc_span;
extern crate rustc_target;
extern crate rustc_trait_selection;

use rustc_lint::LateLintPass;

dylint_linting::declare_late_lint! {
    /// ### What it does
    ///
    /// ### Why is this bad?
    ///
    /// ### Known problems
    ///
    /// Remove if none.
    ///
    /// ### Example
    ///
    /// ```rust
    /// // example code where a warning is issued
    /// ```
    ///
    /// Use instead:
    ///
    /// ```rust
    /// // example code that does not raise a warning
    /// ```
    pub FILL_ME_IN,
    Warn,
    "description goes here"
}

impl<'tcx> LateLintPass<'tcx> for FillMeIn {
    // A list of things you might check can be found here:
    // https://doc.rust-lang.org/stable/nightly-rustc/rustc_lint/trait.LateLintPass.html
}

#[test]
fn ui() {
    dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
}
```

### Template UI test: `internal/template/ui/main.rs`

```rust
fn main() {}
```

---

## Test File Patterns

### Simple Test Case

`ui/main.rs`:
```rust
fn dead_store() {
    let mut arr = [0u64; 4];
    arr[0] = 1;  // First assignment
    arr[0] = 2;  // Dead store - triggers lint
}

fn no_dead_store() {
    let mut arr = [0u64; 4];
    arr[0] = 1;
    arr[1] = 2;  // Different index - OK
}

fn main() {}
```

`ui/main.stderr`:
```
warning: reassigning the same array position without using it
  --> $DIR/main.rs:4:12
   |
LL |     arr[0] = 2;
   |            ^
   |
help: original assignment was here
  --> $DIR/main.rs:3:12
   |
LL |     arr[0] = 1;
   |            ^
   = note: `#[warn(basic_dead_store)]` on by default

warning: 1 warning emitted

```

### Key Test Patterns:
- `$DIR` - placeholder for test directory
- `LL` - placeholder for the line content
- Include blank line at end of `.stderr`
- Test both positive cases (triggers lint) and negative cases (no lint)
