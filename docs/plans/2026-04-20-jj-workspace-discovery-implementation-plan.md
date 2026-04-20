# jj Workspace Discovery Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `bd` and related repo-aware flows work from non-colocated `jj` workspaces that have `.jj/` but no checkout-local `.git/`.

**Architecture:** Add one bootstrap-owned discovery/opening seam that resolves either a normal Git checkout or a Git-backed `jj` workspace into the same canonical result. Keep repo-root semantics aligned with the workspace root for config/request paths, while still opening libgit2 against the backing Git directory when the checkout is `jj`-only.

**Tech Stack:** Rust, `git2`, `jj` CLI, cargo test

---

### Task 1: Prove bootstrap discovery fails for a real `jj workspace`

**Kind:** implementation plan

**Files:**
- Modify: `crates/beads-bootstrap/tests/repo_discovery.rs`

**Step 1: Write the failing test**

Add a test that:
- creates a colocated repo with `jj git init --colocate`
- creates a non-colocated workspace with `jj workspace add`
- calls `beads_bootstrap::repo::discover_root()` from the workspace path
- expects the discovered root to equal the `jj` workspace root
- calls `beads_bootstrap::repo::discover()` and expects the returned repository handle to open successfully even though the workspace has no `.git/`

**Step 2: Run test to verify it fails**

Run: `cargo test -p beads-bootstrap repo_discovery -- --nocapture`

Expected: the new test fails because bootstrap discovery still bottoms out in Git-only discovery.

**Step 3: Write minimal implementation**

Implement the smallest bootstrap change that can:
- detect `.jj/` while walking ancestor directories
- resolve the workspace root
- resolve the backing Git directory for Git-backed `jj` workspaces
- open libgit2 against that backing Git directory

**Step 4: Run test to verify it passes**

Run: `cargo test -p beads-bootstrap repo_discovery -- --nocapture`

Expected: the new `jj` workspace test passes alongside the existing Git discovery tests.

### Task 2: Cut direct Git-only callers over to bootstrap ownership

**Files:**
- Modify: `crates/beads-bootstrap/src/repo.rs`
- Modify: `crates/beads-daemon/src/runtime/store/discovery.rs`
- Modify: `crates/beads-rs/src/cli/backend.rs`

**Step 1: Add failing or coverage-extending assertions if a caller still bypasses bootstrap**

Add or extend tests only where needed to prove the caller path accepts a `jj` workspace root without re-running raw `Repository::discover()` on that path.

**Step 2: Run targeted test to verify the gap**

Run the narrowest relevant test command for the new assertion.

Expected: failure until the caller uses the bootstrap seam.

**Step 3: Write minimal implementation**

Replace direct Git-only opens/discovery with the bootstrap-owned helper so callers no longer have to know whether the request path is a normal Git checkout or a `jj` workspace.

**Step 4: Run tests to verify they pass**

Run the narrowest relevant test command for the touched caller path.

Expected: caller-specific coverage passes and the `jj` workspace path works end-to-end for the tested surface.

### Task 3: Verify the cutover and guard against regressions

**Files:**
- Modify: `crates/beads-bootstrap/tests/repo_discovery.rs`
- Modify: `docs/plans/2026-04-20-jj-workspace-discovery-implementation-plan.md`

**Step 1: Run focused verification**

Run:
- `cargo test -p beads-bootstrap`
- `cargo check -p beads-bootstrap -p beads-daemon -p beads-rs`

Expected: all touched crates compile and bootstrap tests pass.

**Step 2: Run hygiene gates for touched code**

Run:
- `cargo fmt --all`
- `cargo clippy -p beads-bootstrap -p beads-daemon -p beads-rs --all-features -- -D warnings`

Expected: formatting is clean and clippy is green for the touched crates.

**Step 3: Record anything still out of scope**

If some `jj`-workspace flow still requires a separate follow-up, file or link the bead immediately instead of burying it in prose.
