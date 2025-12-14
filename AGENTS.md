# Repository Guidelines

## Issue Tracking

**bd** is infrastructure for you, the agent. It's your external memory.

A bead is a **promise**: you WILL get to this, just not now. When you're in the middle of something and notice tech debt, bugs, slop, or follow-on work that's out of scope—file a bead. Capture enough context that anyone (including future-you) can pick it up cold. Then keep going.

```bash
bd ready              # What can I work on?
bd create "..."       # Promise to handle this later
bd claim <id>         # I'm on it
bd close <id>         # Done
```

Run `bd prime` for full workflow.

## beads-rs

`beads-rs` is a distributed work-item database for agent swarms, using git as the sync layer. It’s a Rust rewrite of the original Go beads.

Core idea: beads are a CRDT. Most fields are last-writer-wins via `Lww<T>` (timestamp + actor); merges are deterministic and require no manual conflict resolution. State syncs through a dedicated git ref (`refs/heads/beads/store`), kept separate from normal code branches.

A local daemon holds canonical state in memory and schedules git sync after mutations (~500ms). The CLI talks to it over a Unix socket and auto-starts it on first use (`bd daemon run`). One daemon can serve many local clones by keying state on the normalized remote URL; `Workflow` and `CanonicalState` make invalid state unrepresentable.

## Architecture Overview (Directory Map)

- `src/core/`: CRDT/domain model (“Layers 0–9”), including `Lww<T>`, `Workflow`, `CanonicalState` invariants
- `src/git/`: sync protocol over `git2` (typestate machine, wire format, collision resolution) targeting `refs/heads/beads/store`
- `src/daemon/`: serialization point (ops + queries), Unix-socket IPC, sync scheduling, per-remote state sharing
- `src/api/`: canonical IPC + `--json` schemas (avoid lossy “view” structs)
- `src/cli/`: `clap` parsing, per-command handlers, and human/JSON rendering; should stay thin and delegate to daemon
- `src/migrate/`: import utilities (currently beads-go JSONL export)
- `src/bin/`: `bd` entrypoint and tracing setup
- `.github/`: CI workflows and issue templates
- `.cargo/config.toml`: local `cargo` aliases (CI currently calls `cargo xtask coverage`; see note below)
- `flake.nix` / `shell.nix`: optional Nix dev shells

## AGENTS.md Hierarchy

AGENTS files are layered “executable context”: when editing `src/daemon/ipc.rs`, follow rules from `AGENTS.md` → `src/AGENTS.md` → `src/daemon/AGENTS.md` (deeper files refine, not contradict).

## Key Files

- `src/bin/main.rs`: `bd` entrypoint + tracing init
- `src/lib.rs`: library exports
- `SPEC.md`: data model + invariants
- `CLI_SPEC.md`: CLI surface and compatibility notes

## Build, Test, and Development Commands

- `cargo check`: typecheck quickly (CI runs this)
- `cargo build`: build debug binaries/library
- `cargo build --release`: optimized build
- `cargo run --bin bd -- <args>`: run the CLI locally
- `cargo test`: run unit tests (tests are co-located under `#[cfg(test)]`)
- `cargo fmt --all`: format code
- `cargo clippy -- -D warnings`: lint (CI treats warnings as errors)

## Coding Style & Naming Conventions

- Follow `rustfmt` (`rustfmt.toml`) and keep `clippy` clean.
- Prefer explicit error types (`thiserror`) and `Result<T, beads_rs::Error>` in library APIs.
- Naming: modules/files in `snake_case.rs`, types in `CamelCase`, CLI flags in `kebab-case`.

## Testing Guidelines

- Add a regression test for bug fixes; keep tests deterministic and OS-independent.
- Prefer temp directories/fixtures over touching real repositories or user state.

## Commit & Pull Request Guidelines

- Use Conventional Commits (seen in history): `feat(scope): ...`, `fix: ...`, `test(scope): ...`, `chore: ...`.
- Include the tracker reference when available, e.g. `(bd-abc123)` or `(#123)`.
- PRs should explain “why” and “what”, link relevant issues/spec changes, and pass: `cargo fmt`, `cargo clippy`, `cargo test`.

## Debugging & Configuration Tips

- Logging uses `tracing`: set `LOG=debug` (or a module filter like `LOG=beads_rs=trace`) and/or pass `-v/-vv` to `bd`.

## CI Note

`.github/workflows/build.yml` runs `cargo xtask coverage`, but this repo currently only has the alias in `.cargo/config.toml` and no `xtask` package. If you touch coverage, either add the missing `xtask/` crate or update the workflow.
