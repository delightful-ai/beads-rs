# clippy_utils Reference

Essential utilities from `clippy_utils` for writing lints.

## Dependencies

```toml
[dependencies]
clippy_utils = { git = "https://github.com/rust-lang/rust-clippy", rev = "CLIPPY_REV" }
```

The `rev` must match the Rust toolchain version. Check dylint's template for current rev.

## Diagnostic Functions

### span_lint

Basic lint emission:

```rust
use clippy_utils::diagnostics::span_lint;

span_lint(
    cx,
    MY_LINT,
    span,
    "message describing the issue",
);
```

### span_lint_and_help

Adds help text:

```rust
use clippy_utils::diagnostics::span_lint_and_help;

// Without a help span
span_lint_and_help(
    cx,
    MY_LINT,
    span,
    "message describing the issue",
    None,
    "suggestion for how to fix",
);

// With a help span (pointing to related code)
span_lint_and_help(
    cx,
    MY_LINT,
    span,
    "message describing the issue",
    Some(other_span),
    "this is related",
);
```

### span_lint_and_sugg

Provides machine-applicable fix:

```rust
use clippy_utils::diagnostics::span_lint_and_sugg;
use rustc_errors::Applicability;

span_lint_and_sugg(
    cx,
    MY_LINT,
    span,
    "message describing the issue",
    "try",  // prefix before suggestion
    "replacement_code".to_string(),
    Applicability::MachineApplicable,
);
```

### span_lint_and_then

For complex diagnostics with multiple notes/suggestions:

```rust
use clippy_utils::diagnostics::span_lint_and_then;

span_lint_and_then(
    cx,
    MY_LINT,
    span,
    "message describing the issue",
    |diag| {
        diag.span_note(other_span, "additional note");
        diag.help("help text");
        diag.span_suggestion(
            span,
            "try this",
            "replacement".to_string(),
            Applicability::MaybeIncorrect,
        );
    },
);
```

## Applicability Levels

```rust
use rustc_errors::Applicability;

// Can be applied automatically with no human review
Applicability::MachineApplicable

// Might not work in all cases, needs review
Applicability::MaybeIncorrect

// Contains placeholders like `/* ... */`
Applicability::HasPlaceholders

// Unknown/unspecified
Applicability::Unspecified
```

## Source Code Extraction

### snippet

Get source code as string:

```rust
use clippy_utils::source::snippet;

let code = snippet(cx, span, "default");
// Returns source code, or "default" if not available
```

### snippet_with_applicability

Get source with applicability tracking:

```rust
use clippy_utils::source::snippet_with_applicability;

let mut applicability = Applicability::MachineApplicable;
let code = snippet_with_applicability(cx, span, "default", &mut applicability);
// If snippet fails, applicability is downgraded
```

### snippet_opt

Optional snippet:

```rust
use clippy_utils::source::snippet_opt;

if let Some(code) = snippet_opt(cx, span) {
    // Use the code
}
```

## Path Matching

### PathLookup

For matching function/type paths:

```rust
use clippy_utils::paths::{PathLookup, PathNS};
use clippy_utils::{value_path, type_path};

// Match a function path
static STD_ENV_VAR: PathLookup = value_path!(std::env::var);
static STD_ENV_SET_VAR: PathLookup = value_path!(std::env::set_var);

// Match a type path
static OPTION_TYPE: PathLookup = type_path!(core::option::Option);

// Usage
if STD_ENV_VAR.matches_path(cx, expr) {
    // expr is a call to std::env::var
}

if OPTION_TYPE.matches_ty(cx, ty) {
    // ty is Option
}
```

### lookup_path_str

For dynamic path lookup:

```rust
use clippy_utils::paths::{lookup_path_str, PathNS};

let path = &["std", "env", "var"];
let def_ids = lookup_path_str(cx.tcx, PathNS::Value, &path.join("::"));

if def_ids == [some_def_id] {
    // Matches
}
```

### match_def_path (from dylint_internal)

```rust
use dylint_internal::match_def_path;

if match_def_path(cx, def_id, &["std", "env", "var"]) {
    // It's std::env::var
}
```

## Expression Utilities

### get_parent_expr

Get parent expression:

```rust
use clippy_utils::get_parent_expr;

if let Some(parent) = get_parent_expr(cx, expr) {
    // Work with parent
}
```

### expr_or_init

Get the initializer of an expression (follows let bindings):

```rust
use clippy_utils::expr_or_init;

let init_expr = expr_or_init(cx, expr);
```

### is_expr_path_def_path

Check if expression resolves to a specific path:

```rust
use clippy_utils::is_expr_path_def_path;

if is_expr_path_def_path(cx, expr, &["std", "mem", "drop"]) {
    // expr is std::mem::drop
}
```

## Type Utilities

### implements_trait

Check if type implements a trait:

```rust
use clippy_utils::ty::implements_trait;

if let Some(clone_trait) = cx.tcx.lang_items().clone_trait() {
    if implements_trait(cx, ty, clone_trait, &[]) {
        // Type implements Clone
    }
}

// With generic args
if implements_trait(cx, ty, iterator_trait, &[element_ty.into()]) {
    // Type implements Iterator<Item = element_ty>
}
```

### is_type_diagnostic_item

Check diagnostic items:

```rust
use clippy_utils::ty::is_type_diagnostic_item;
use rustc_span::sym;

if is_type_diagnostic_item(cx, ty, sym::Option) {
    // It's Option
}

if is_type_diagnostic_item(cx, ty, sym::Result) {
    // It's Result
}

if is_type_diagnostic_item(cx, ty, sym::Vec) {
    // It's Vec
}
```

### is_type_lang_item

Check language items:

```rust
use clippy_utils::ty::is_type_lang_item;
use rustc_hir::LangItem;

if is_type_lang_item(cx, ty, LangItem::OwnedBox) {
    // It's Box
}
```

### walk_ptrs_ty

Dereference pointer types:

```rust
use clippy_utils::ty::walk_ptrs_ty;

let inner_ty = walk_ptrs_ty(ty);
// Strips &, &mut, *const, *mut
```

## Macro Utilities

### in_external_macro

Check if span is from external macro:

```rust
use clippy_utils::in_external_macro;

if in_external_macro(cx.sess(), span) {
    return; // Don't lint external macro code
}
```

### is_from_proc_macro

Check if from proc macro:

```rust
use clippy_utils::is_from_proc_macro;

if is_from_proc_macro(cx, expr) {
    return;
}
```

## HIR Utilities

### SpanlessEq

Compare expressions ignoring spans:

```rust
use clippy_utils::SpanlessEq;

if SpanlessEq::new(cx).eq_expr(expr1, expr2) {
    // Expressions are structurally equal
}
```

### path_to_local

Get local variable from path:

```rust
use clippy_utils::path_to_local;

if let Some(hir_id) = path_to_local(expr) {
    // expr refers to local variable with hir_id
}
```

### path_to_local_id

Check if path refers to specific local:

```rust
use clippy_utils::path_to_local_id;

if path_to_local_id(expr, local_hir_id) {
    // expr refers to the specific local
}
```

## Method Resolution

### MaybeResPath trait

```rust
use clippy_utils::res::MaybeResPath;

if let Some(def_id) = expr.basic_res().opt_def_id() {
    // Got the DefId of what the expression refers to
}
```

## Constants

### constant

Evaluate constant expressions:

```rust
use clippy_utils::consts::{constant, Constant};

if let Some(Constant::Int(n)) = constant(cx, cx.typeck_results(), expr) {
    // expr evaluates to integer constant n
}
```

## Common Patterns

### Checking Function Calls

```rust
fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
    if let ExprKind::Call(callee, args) = expr.kind {
        // Get what the callee resolves to
        if let Some(def_id) = callee.basic_res().opt_def_id() {
            // Check against known functions
            if KNOWN_FN.matches(cx, def_id) {
                // Handle known function call
            }
        }
    }
}
```

### Checking Method Calls

```rust
fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
    if let ExprKind::MethodCall(path, receiver, args, span) = expr.kind {
        // Check method name
        if path.ident.name == sym::unwrap {
            // Check receiver type
            let receiver_ty = cx.typeck_results().expr_ty(receiver);
            if is_type_diagnostic_item(cx, receiver_ty, sym::Option) {
                // It's Option::unwrap
            }
        }
    }
}
```

### Checking Literals

```rust
use rustc_ast::LitKind;
use rustc_hir::ExprKind;

if let ExprKind::Lit(lit) = expr.kind {
    match lit.node {
        LitKind::Str(symbol, _) => {
            let s = symbol.as_str();
            // Handle string literal
        }
        LitKind::Int(n, _) => {
            // Handle integer literal
        }
        LitKind::Bool(b) => {
            // Handle bool literal
        }
        _ => {}
    }
}
```

### Checking Assignments

```rust
if let ExprKind::Assign(lhs, rhs, span) = expr.kind {
    // lhs = rhs
}

if let ExprKind::AssignOp(op, lhs, rhs) = expr.kind {
    // lhs op= rhs (e.g., +=, -=)
}
```

### Checking If Expressions

```rust
if let ExprKind::If(cond, then_block, else_opt) = expr.kind {
    // Handle condition
    // Handle then block
    if let Some(else_expr) = else_opt {
        // Handle else
    }
}
```

## Import Summary

Common imports for lint writing:

```rust
use clippy_utils::{
    diagnostics::{span_lint, span_lint_and_help, span_lint_and_sugg, span_lint_and_then},
    get_parent_expr,
    in_external_macro,
    paths::{PathLookup, PathNS},
    res::MaybeResPath,
    source::snippet,
    ty::{implements_trait, is_type_diagnostic_item},
    value_path, type_path,
};
use rustc_errors::Applicability;
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::{Span, sym};
```
