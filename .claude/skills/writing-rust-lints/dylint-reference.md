# Dylint Reference Guide

Comprehensive reference for writing Dylint lints - custom Rust lints loaded from dynamic libraries.

## Overview

Dylint is a Rust linting tool that runs lints from user-specified dynamic libraries. Unlike Clippy's fixed lint set, Dylint allows developers to maintain custom lint collections.

## Quick Start

### Installation

```sh
cargo install cargo-dylint dylint-link
```

### Creating a New Lint

```sh
cargo dylint new my_lint_name
cd my_lint_name
cargo build
cargo dylint list --path .
```

### Running Lints

```sh
# From command line
cargo dylint --git https://github.com/user/lints --pattern path/to/lint

# Using workspace metadata (in Cargo.toml or dylint.toml)
cargo dylint --all
```

## Library Requirements

A Dylint library must:

1. **Filename format**: `DLL_PREFIX LIBRARY_NAME '@' TOOLCHAIN DLL_SUFFIX`
   - Example (Linux): `libquestion_mark_in_expression@nightly-2021-04-08-x86_64-unknown-linux-gnu.so`

2. **Export `dylint_version` function**:
   ```rust
   extern "C" fn dylint_version() -> *mut std::os::raw::c_char
   ```
   Should return `0.1.0`.

3. **Export `register_lints` function**:
   ```rust
   fn register_lints(sess: &rustc_session::Session, lint_store: &mut rustc_lint::LintStore)
   ```

4. **Link against `rustc_driver`**:
   ```rust
   extern crate rustc_driver;
   ```

The `dylint_library!` macro and `dylint-link` tool handle these automatically.

## Project Structure

```
my_lint/
├── .cargo/
│   └── config.toml          # Points linker to dylint-link
├── Cargo.toml                # Library configuration
├── src/
│   └── lib.rs                # Lint implementation
└── ui/
    ├── main.rs               # Test cases
    └── main.stderr           # Expected warnings
```

### Cargo.toml Template

```toml
[package]
name = "my_lint"
version = "0.1.0"
edition = "2024"
publish = false

[lib]
crate-type = ["cdylib"]

[dependencies]
clippy_utils = { git = "https://github.com/rust-lang/rust-clippy", rev = "CLIPPY_REV" }
dylint_linting = "5.0.0"

[dev-dependencies]
dylint_testing = "5.0.0"

[workspace]

[package.metadata.rust-analyzer]
rustc_private = true
```

### .cargo/config.toml

```toml
[target.x86_64-unknown-linux-gnu]
linker = "dylint-link"

[target.x86_64-apple-darwin]
linker = "dylint-link"

[target.aarch64-apple-darwin]
linker = "dylint-link"
```

## Macros

### dylint_library!

Expands to:
```rust
#[allow(unused_extern_crates)]
extern crate rustc_driver;

#[unsafe(no_mangle)]
pub extern "C" fn dylint_version() -> *mut std::os::raw::c_char {
    std::ffi::CString::new($crate::DYLINT_VERSION)
        .unwrap()
        .into_raw()
}
```

### declare_late_lint!

For single-lint libraries using `LateLintPass`:
```rust
dylint_linting::declare_late_lint! {
    /// ### What it does
    /// Description of what the lint detects.
    ///
    /// ### Why is this bad?
    /// Explanation of why this pattern is problematic.
    ///
    /// ### Example
    /// ```rust
    /// // Bad code example
    /// ```
    ///
    /// Use instead:
    /// ```rust
    /// // Good code example
    /// ```
    pub MY_LINT_NAME,
    Warn,
    "brief description"
}
```

Expands to:
- `dylint_library!()` call
- `register_lints` function implementation
- `declare_lint!` call
- `declare_lint_pass!` call

### impl_late_lint!

Like `declare_late_lint!` but with custom initial state:
```rust
dylint_linting::impl_late_lint! {
    /// Documentation...
    pub MY_LINT_NAME,
    Warn,
    "brief description",
    MyLintName::new()  // Custom initialization
}
```

Uses `impl_lint_pass!` instead of `declare_lint_pass!`.

### Early and Pre-expansion Variants

- `declare_early_lint!` / `impl_early_lint!` - For AST-level lints
- `declare_pre_expansion_lint!` / `impl_pre_expansion_lint!` - For pre-macro-expansion lints

## Lint Pass Types

### LateLintPass (Most Common)

Runs after type checking. Has access to type information via `LateContext`.

```rust
impl<'tcx> LateLintPass<'tcx> for MyLint {
    fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
        // Check expressions
    }

    fn check_fn(
        &mut self,
        cx: &LateContext<'tcx>,
        fn_kind: FnKind<'tcx>,
        fn_decl: &'tcx FnDecl<'_>,
        body: &'tcx Body<'_>,
        span: Span,
        local_def_id: LocalDefId,
    ) {
        // Check function definitions
    }

    // Many more check_* methods available
}
```

### EarlyLintPass

Runs before type checking. Only has AST information.

```rust
impl EarlyLintPass for MyLint {
    fn check_fn(
        &mut self,
        cx: &EarlyContext<'_>,
        fn_kind: FnKind<'_>,
        span: Span,
        node_id: NodeId,
    ) {
        // Check functions at AST level
    }
}
```

## Configuration

### Reading Configuration

Libraries can be configured via `dylint.toml` in the workspace root:

```toml
[my_lint_name]
option1 = true
strings = ["foo", "bar"]
```

Implementation:
```rust
#[derive(Default, serde::Deserialize)]
struct Config {
    option1: Option<bool>,
    strings: Option<Vec<String>>,
}

struct MyLint {
    config: Config,
}

impl MyLint {
    pub fn new() -> Self {
        Self {
            config: dylint_linting::config_or_default(env!("CARGO_PKG_NAME")),
        }
    }
}
```

### Configuration Functions

- `config_or_default(pkg_name)` - Read config or use Default
- `config(pkg_name)` - Read config (may panic if missing)
- `config_toml()` - Get raw TOML
- `init_config(sess)` - Initialize in register_lints

## Testing

### Basic UI Test

```rust
#[test]
fn ui() {
    dylint_testing::ui_test(env!("CARGO_PKG_NAME"), "ui");
}
```

### Test File Structure

`ui/main.rs`:
```rust
fn problematic_code() {
    // Code that should trigger the lint
}

fn acceptable_code() {
    // Code that should NOT trigger the lint
}

fn main() {}
```

`ui/main.stderr`:
```
warning: lint message
  --> $DIR/main.rs:2:5
   |
LL |     problematic_code();
   |     ^^^^^^^^^^^^^^^^^^
   |
   = note: `#[warn(my_lint_name)]` on by default

warning: 1 warning emitted

```

### Advanced Testing

For tests with dependencies:
```rust
#[test]
fn ui() {
    dylint_testing::ui_test_example(env!("CARGO_PKG_NAME"), "example_name");
}
```

With configuration:
```rust
#[test]
fn ui_with_config() {
    dylint_testing::ui::Test::example(env!("CARGO_PKG_NAME"), "ui")
        .dylint_toml("my_lint.option = true")
        .run();
}
```

With rustc flags:
```rust
#[test]
fn ui_with_flags() {
    dylint_testing::ui::Test::src_base(env!("CARGO_PKG_NAME"), "ui")
        .rustc_flags(["--edition=2021"])
        .run();
}
```

## Diagnostic Functions

From `clippy_utils::diagnostics`:

### span_lint

Basic lint emission:
```rust
span_lint(cx, MY_LINT, span, "message");
```

### span_lint_and_help

With help text:
```rust
span_lint_and_help(
    cx,
    MY_LINT,
    span,
    "message",
    None,  // Or Some(help_span)
    "help text",
);
```

### span_lint_and_sugg

With code suggestion:
```rust
span_lint_and_sugg(
    cx,
    MY_LINT,
    span,
    "message",
    "try this",
    suggested_code.to_string(),
    Applicability::MachineApplicable,
);
```

### span_lint_and_then

For multiple notes/suggestions:
```rust
span_lint_and_then(
    cx,
    MY_LINT,
    span,
    "message",
    |diag| {
        diag.span_note(other_span, "note");
        diag.help("help text");
    },
);
```

## Applicability Levels

- `MachineApplicable` - Can be applied automatically
- `MaybeIncorrect` - May not be correct in all cases
- `HasPlaceholders` - Contains placeholders like `/* ... */`
- `Unspecified` - Unknown correctness

## Common Patterns

### Checking Function Calls

```rust
fn check_expr(&mut self, cx: &LateContext<'tcx>, expr: &'tcx Expr<'tcx>) {
    if let ExprKind::Call(callee, args) = expr.kind {
        if let Some(def_id) = callee.basic_res().opt_def_id() {
            // Check against known paths
        }
    }
}
```

### Checking Method Calls

```rust
if let ExprKind::MethodCall(path, receiver, args, span) = expr.kind {
    if path.ident.name == sym::unwrap {
        // Handle .unwrap() calls
    }
}
```

### Path Matching

Using clippy_utils PathLookup:
```rust
use clippy_utils::{paths::PathLookup, value_path};

static STD_ENV_VAR: PathLookup = value_path!(std::env::var);

if STD_ENV_VAR.matches_path(cx, callee) {
    // It's a call to std::env::var
}
```

### Type Checking

```rust
let ty = cx.typeck_results().expr_ty(expr);
match ty.kind() {
    ty::Adt(adt_def, _) if cx.tcx.is_diagnostic_item(sym::Result, adt_def.did()) => {
        // It's a Result type
    }
    _ => {}
}
```

### Checking Trait Implementation

```rust
use clippy_utils::ty::implements_trait;

if let Some(trait_id) = cx.tcx.get_diagnostic_item(sym::Clone) {
    if implements_trait(cx, ty, trait_id, &[]) {
        // Type implements Clone
    }
}
```

## Workspace Metadata

Configure in `Cargo.toml` or `dylint.toml`:

```toml
[workspace.metadata.dylint]
libraries = [
    { git = "https://github.com/user/lints", pattern = "path/to/lints" },
    { path = "../local-lints" },
]
```

Git options:
```toml
{ git = "...", branch = "main", pattern = "..." }
{ git = "...", tag = "v1.0.0", pattern = "..." }
{ git = "...", rev = "abc123", pattern = "..." }
```

## Conditional Compilation

Allow a lint only when Dylint is used:
```rust
#[cfg_attr(dylint_lib = "my_lint", allow(my_lint_name))]
fn code_that_triggers_lint() {}
```

For pre-expansion lints:
```rust
#[allow(unknown_lints)]
#[allow(my_pre_expansion_lint)]
fn code() {}
```

## VS Code Integration

In `settings.json`:
```json
{
    "rust-analyzer.check.overrideCommand": [
        "cargo", "dylint", "--all", "--",
        "--all-targets", "--message-format=json"
    ],
    "rust-analyzer.rustc.source": "discover"
}
```

## Common Imports

```rust
#![feature(rustc_private)]
#![warn(unused_extern_crates)]

extern crate rustc_ast;
extern crate rustc_hir;
extern crate rustc_lint;
extern crate rustc_middle;
extern crate rustc_session;
extern crate rustc_span;

use clippy_utils::diagnostics::{span_lint, span_lint_and_help, span_lint_and_sugg};
use rustc_hir::{Expr, ExprKind};
use rustc_lint::{LateContext, LateLintPass};
use rustc_span::Span;
```

## Debugging

Set environment variable for verbose output:
```sh
MY_LINT_DEBUG=1 cargo dylint my_lint
```

In lint code:
```rust
#[must_use]
fn enabled(opt: &str) -> bool {
    let key = env!("CARGO_PKG_NAME").to_uppercase() + "_" + opt;
    std::env::var(key).is_ok_and(|value| value != "0")
}

if enabled("DEBUG") {
    eprintln!("Debug info: {:?}", something);
}
```
