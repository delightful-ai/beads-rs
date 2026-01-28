# Rustc Internals Reference for Lint Writers

Quick reference for navigating rustc's internal APIs when writing lints.

## Key Crates

| Crate | Purpose |
|-------|---------|
| `rustc_hir` | High-level IR (HIR) - desugared AST |
| `rustc_middle` | MIR, type system, queries |
| `rustc_ast` | Abstract Syntax Tree |
| `rustc_span` | Source code locations (Span) |
| `rustc_lint` | Lint infrastructure |
| `rustc_session` | Compiler session, lint declaration macros |
| `rustc_errors` | Diagnostic emission |

## Extern Crate Declarations

```rust
#![feature(rustc_private)]

// Core crates for most lints
extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_middle;
extern crate rustc_session;
extern crate rustc_span;

// For AST-level lints
extern crate rustc_ast;

// Additional as needed
extern crate rustc_data_structures;
extern crate rustc_errors;
extern crate rustc_index;
extern crate rustc_target;
extern crate rustc_trait_selection;
```

## HIR (High-level IR)

The HIR is the primary representation for late lints. Key types:

### Expr (Expressions)

```rust
use rustc_hir::{Expr, ExprKind};

match expr.kind {
    ExprKind::Call(callee, args) => { /* function call */ }
    ExprKind::MethodCall(path_seg, receiver, args, span) => { /* method call */ }
    ExprKind::Binary(op, lhs, rhs) => { /* binary operation */ }
    ExprKind::Unary(op, operand) => { /* unary operation */ }
    ExprKind::Lit(lit) => { /* literal */ }
    ExprKind::Block(block, label) => { /* block expression */ }
    ExprKind::If(cond, then, else_opt) => { /* if expression */ }
    ExprKind::Match(scrutinee, arms, source) => { /* match expression */ }
    ExprKind::Loop(body, label, source, span) => { /* loop */ }
    ExprKind::Closure(closure) => { /* closure */ }
    ExprKind::Path(qpath) => { /* path expression */ }
    ExprKind::Assign(lhs, rhs, span) => { /* assignment */ }
    ExprKind::AssignOp(op, lhs, rhs) => { /* compound assignment */ }
    ExprKind::Field(base, ident) => { /* field access */ }
    ExprKind::Index(base, index, span) => { /* indexing */ }
    ExprKind::AddrOf(borrow_kind, mutability, operand) => { /* &expr */ }
    ExprKind::Ret(value) => { /* return */ }
    ExprKind::Struct(path, fields, base) => { /* struct literal */ }
    ExprKind::Array(elements) => { /* array literal */ }
    ExprKind::Tup(elements) => { /* tuple literal */ }
    // ... many more
    _ => {}
}
```

### Stmt (Statements)

```rust
use rustc_hir::{Stmt, StmtKind};

match stmt.kind {
    StmtKind::Let(local) => { /* let binding */ }
    StmtKind::Item(item_id) => { /* item definition */ }
    StmtKind::Expr(expr) => { /* expression statement */ }
    StmtKind::Semi(expr) => { /* expression with semicolon */ }
}
```

### Pat (Patterns)

```rust
use rustc_hir::{Pat, PatKind};

match pat.kind {
    PatKind::Binding(mode, hir_id, ident, sub_pattern) => { /* variable binding */ }
    PatKind::Struct(path, fields, rest) => { /* struct pattern */ }
    PatKind::TupleStruct(path, pats, pos) => { /* tuple struct pattern */ }
    PatKind::Path(qpath) => { /* path pattern */ }
    PatKind::Tuple(pats, pos) => { /* tuple pattern */ }
    PatKind::Ref(pat, mutability) => { /* reference pattern */ }
    PatKind::Lit(expr) => { /* literal pattern */ }
    PatKind::Range(start, end, end_kind) => { /* range pattern */ }
    PatKind::Wild => { /* _ pattern */ }
    _ => {}
}
```

### Item (Items)

```rust
use rustc_hir::{Item, ItemKind};

match item.kind {
    ItemKind::Fn(sig, generics, body_id) => { /* function */ }
    ItemKind::Struct(variant_data, generics) => { /* struct */ }
    ItemKind::Enum(enum_def, generics) => { /* enum */ }
    ItemKind::Impl(impl_) => { /* impl block */ }
    ItemKind::Trait(is_auto, safety, generics, bounds, items) => { /* trait */ }
    ItemKind::Mod(module) => { /* module */ }
    ItemKind::Use(path, kind) => { /* use statement */ }
    ItemKind::Const(ty, generics, body_id) => { /* const */ }
    ItemKind::Static(ty, mutability, body_id) => { /* static */ }
    ItemKind::TyAlias(ty, generics) => { /* type alias */ }
    _ => {}
}
```

## LateContext

The context passed to `LateLintPass` methods:

```rust
impl<'tcx> LateLintPass<'tcx> for MyLint {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        // Access type information
        let ty = cx.typeck_results().expr_ty(expr);

        // Access the type context
        let tcx = cx.tcx;

        // Get HIR
        let hir = tcx.hir();

        // Resolve paths
        let res = cx.qpath_res(qpath, expr.hir_id);

        // Check visibility
        let is_public = cx.effective_visibilities.is_exported(def_id);
    }
}
```

### Key LateContext Methods

```rust
// Type of an expression
cx.typeck_results().expr_ty(expr)

// Type of a pattern
cx.typeck_results().pat_ty(pat)

// Node type
cx.typeck_results().node_type(hir_id)

// Resolve a QPath
cx.qpath_res(qpath, hir_id)

// Get DefId from resolution
if let Res::Def(def_kind, def_id) = res { ... }
```

## Type System

### Ty (Types)

```rust
use rustc_middle::ty::{self, Ty, TyKind};

match ty.kind() {
    ty::Bool => { /* bool */ }
    ty::Int(int_ty) => { /* i8, i16, i32, i64, i128, isize */ }
    ty::Uint(uint_ty) => { /* u8, u16, u32, u64, u128, usize */ }
    ty::Float(float_ty) => { /* f32, f64 */ }
    ty::Str => { /* str */ }
    ty::Char => { /* char */ }
    ty::Array(elem_ty, len) => { /* [T; N] */ }
    ty::Slice(elem_ty) => { /* [T] */ }
    ty::Ref(region, ty, mutability) => { /* &T or &mut T */ }
    ty::RawPtr(ty, mutability) => { /* *const T or *mut T */ }
    ty::Tuple(substs) => { /* (T1, T2, ...) */ }
    ty::Adt(adt_def, substs) => { /* struct, enum, union */ }
    ty::Closure(def_id, substs) => { /* closure */ }
    ty::Fn(poly_fn_sig) => { /* fn(...) -> ... */ }
    ty::FnDef(def_id, substs) => { /* specific function */ }
    ty::Dynamic(predicates, region) => { /* dyn Trait */ }
    ty::Never => { /* ! */ }
    ty::Param(param_ty) => { /* generic type parameter */ }
    _ => {}
}
```

### Common Type Checks

```rust
// Check if it's a specific type
ty.is_bool()
ty.is_integral()
ty.is_floating_point()
ty.is_char()
ty.is_str()
ty.is_unit()
ty.is_never()

// Check reference
ty.is_ref()
ty.is_mutable_ptr()

// Check diagnostic items
if let ty::Adt(adt, _) = ty.kind() {
    cx.tcx.is_diagnostic_item(sym::Option, adt.did())
    cx.tcx.is_diagnostic_item(sym::Result, adt.did())
    cx.tcx.is_diagnostic_item(sym::Vec, adt.did())
    cx.tcx.is_diagnostic_item(sym::String, adt.did())
}

// Peel references
ty.peel_refs()
```

## Span

Source code locations:

```rust
use rustc_span::Span;

// Get span of an expression
let span: Span = expr.span;

// Check if from macro expansion
span.from_expansion()

// Get the expansion context
span.ctxt()

// Check if in external macro
span.in_external_macro(cx.tcx.sess.source_map())

// Combine spans
span.to(other_span)

// Get source text
cx.tcx.sess.source_map().span_to_snippet(span)
```

## LateLintPass Methods

All methods you can implement:

```rust
impl<'tcx> LateLintPass<'tcx> for MyLint {
    // Items
    fn check_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {}
    fn check_item_post(&mut self, cx: &LateContext<'tcx>, item: &'tcx Item<'tcx>) {}

    // Functions
    fn check_fn(
        &mut self,
        cx: &LateContext<'tcx>,
        kind: FnKind<'tcx>,
        decl: &'tcx FnDecl<'tcx>,
        body: &'tcx Body<'tcx>,
        span: Span,
        def_id: LocalDefId,
    ) {}
    fn check_fn_post(/* same args */) {}

    // Expressions
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {}
    fn check_expr_post(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {}

    // Statements
    fn check_stmt(&mut self, cx: &LateContext<'tcx>, stmt: &'tcx Stmt<'tcx>) {}

    // Blocks
    fn check_block(&mut self, cx: &LateContext<'tcx>, block: &'tcx Block<'tcx>) {}
    fn check_block_post(&mut self, cx: &LateContext<'tcx>, block: &'tcx Block<'tcx>) {}

    // Patterns
    fn check_pat(&mut self, cx: &LateContext<'tcx>, pat: &'tcx Pat<'tcx>) {}

    // Local variables
    fn check_local(&mut self, cx: &LateContext<'tcx>, local: &'tcx LetStmt<'tcx>) {}

    // Arms (in match)
    fn check_arm(&mut self, cx: &LateContext<'tcx>, arm: &'tcx Arm<'tcx>) {}

    // Impl blocks
    fn check_impl_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx ImplItem<'tcx>) {}
    fn check_impl_item_post(&mut self, cx: &LateContext<'tcx>, item: &'tcx ImplItem<'tcx>) {}

    // Trait items
    fn check_trait_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx TraitItem<'tcx>) {}
    fn check_trait_item_post(&mut self, cx: &LateContext<'tcx>, item: &'tcx TraitItem<'tcx>) {}

    // Foreign items
    fn check_foreign_item(&mut self, cx: &LateContext<'tcx>, item: &'tcx ForeignItem<'tcx>) {}

    // Generics
    fn check_generics(&mut self, cx: &LateContext<'tcx>, generics: &'tcx Generics<'tcx>) {}

    // Attributes
    fn check_attribute(&mut self, cx: &LateContext<'tcx>, attr: &'tcx Attribute) {}

    // Crate-level
    fn check_crate(&mut self, cx: &LateContext<'tcx>) {}
    fn check_crate_post(&mut self, cx: &LateContext<'tcx>) {}
}
```

## MIR (Mid-level IR)

For advanced analysis (like `non_local_effect_before_error_return`):

```rust
use rustc_middle::mir::{
    BasicBlock, Body, Local, Location,
    Operand, Place, Rvalue, Statement, StatementKind,
    Terminator, TerminatorKind,
};

// Get MIR for a function
let mir: &Body<'tcx> = cx.tcx.optimized_mir(def_id);

// Iterate basic blocks
for (bb, block_data) in mir.basic_blocks.iter_enumerated() {
    // Statements
    for statement in &block_data.statements {
        match &statement.kind {
            StatementKind::Assign(box (place, rvalue)) => {}
            StatementKind::StorageLive(local) => {}
            StatementKind::StorageDead(local) => {}
            _ => {}
        }
    }

    // Terminator
    if let Some(terminator) = &block_data.terminator {
        match &terminator.kind {
            TerminatorKind::Call { func, args, destination, .. } => {}
            TerminatorKind::Return => {}
            TerminatorKind::SwitchInt { discr, targets } => {}
            _ => {}
        }
    }
}
```

## Useful Diagnostic Symbols

Common symbols from `rustc_span::sym`:

```rust
use rustc_span::sym;

sym::Option
sym::Result
sym::Some
sym::None
sym::Ok
sym::Err
sym::Vec
sym::String
sym::Clone
sym::Copy
sym::Drop
sym::Default
sym::Debug
sym::unwrap
sym::expect
sym::into
sym::from
sym::new
```

## Documentation Links

- [rustc_hir](https://doc.rust-lang.org/nightly/nightly-rustc/rustc_hir/index.html)
- [rustc_middle](https://doc.rust-lang.org/nightly/nightly-rustc/rustc_middle/index.html)
- [rustc_lint::LateContext](https://doc.rust-lang.org/nightly/nightly-rustc/rustc_lint/struct.LateContext.html)
- [rustc_lint::LateLintPass](https://doc.rust-lang.org/nightly/nightly-rustc/rustc_lint/trait.LateLintPass.html)
- [Rustc Dev Guide](https://rustc-dev-guide.rust-lang.org/)
