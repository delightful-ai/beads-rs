## Boundary
This directory holds one file per Go `bd` subcommand. Each file is the semantic contract beads-rs ports against.
NEVER: skip the provenance header or the live JSON capture. A cmd doc without a cited Go tag AND actual command output is speculation, not a spec.
NEVER: document beads-rs-only commands here. Native-to-Rust surfaces belong in `crates/beads-cli/`-local docs or `docs/IPC_PROTOCOL.md`.
NEVER: hand-write JSON shapes from memory or paraphrase `bd --help`. Always copy verbatim from the pinned binary and cite the exact command you ran.

## Template

Every cmd doc follows the shape below. `show.md` is the canonical read example; `create.md` is the canonical write example. When in doubt, match their structure.

```markdown
# bd <name>

**Go source:** `cmd/bd/<name>.go` (+ any `internal/<pkg>/` helpers)
**Pin:** v1.0.2 (c446a2ef)
**Parity status:** {faithful | extended | simplified | deferred | declined}
**Rust source:** `crates/beads-cli/src/commands/<path>` (or "not implemented")

## Purpose

One paragraph. What is the user-observable job of this command? Who calls it, and what do they get back? If the command has multiple modes (batch vs single, interactive vs JSON), name them here.

## Invocation

```
bd <name> [args] [flags]
```

Aliases (if any).

## Arguments

| Name | Type | Required | Semantics |
|------|------|----------|-----------|
| ...  | ...  | ...      | ...       |

## Flags

Verbatim from `bd <name> --help` against the pin, annotated. Do not paraphrase flag descriptions — copy them and add context below the table if a flag has non-obvious behavior.

| Flag | Type | Default | Semantics |
|------|------|---------|-----------|
| ...  | ...  | ...     | ...       |

## Output (live capture)

State the exact command you ran, then paste the exact JSON it produced. Multiple captures if the command has modes (e.g. single-ID vs multi-ID, default vs --long).

```bash
$ /tmp/go-bd-playground/bd-go <name> ... --json
```

```json
{ ... }
```

## Data model impact

- **Reads:** which fields/tables the command inspects.
- **Writes:** which fields/tables the command mutates.
- **Invariants:** any cross-field invariants the command enforces or assumes (e.g. "assignee must be set when status transitions to in_progress").
- **Hooks fired:** which `HookFiringStore` hooks run (v1.0.0+).

## Error cases

Enumerate the user-visible error classes. If the JSON output includes an error envelope, paste a captured example.

## Rust parity notes

- **Where beads-rs matches:** link to the Rust command implementation and note which parts line up.
- **Where beads-rs diverges:** name the architectural forcing function (CRDT vs Dolt, daemon vs embedded, `WriteStamp` vs `time.Time`).
- **Known gaps:** what's missing or different, in concrete terms.
```

## Local rules

- **Live captures only.** When documenting JSON shape, run the command against `/tmp/go-bd-playground/bd-go` in `/tmp/go-bd-play/` (or a fresh throwaway init) and paste the exact output. No typed-from-memory JSON.
- **Copy `--help` verbatim.** The Flags table is derived from `bd <name> --help` against the pinned binary. If flag descriptions are terse, expand them below the table, but don't rewrite them in-table.
- **One file per command.** Related subcommands (`bd mol create`, `bd mol bond`, etc.) get an index file `mol.md` plus one file per subcommand: `mol-create.md`, `mol-bond.md`. Keep the subcommand prefix explicit in the filename so globs work.
- **Cross-link primitives.** When a command implements a primitive (molecules, formulas, slots, hooks), link to the relevant `primitives/*.md` file rather than re-explaining the concept.
- **Parity status reflects reality, not ambition.** If a Rust impl exists but diverges, don't mark `faithful` — mark `extended` or `simplified` and spell it out.
- **On-touch rule.** When a Rust implementation lands, update the cmd doc's Rust source pointer AND its parity status in the same change. Stale `not implemented` notes after work lands is a sev defect of this subtree.

## Verification

- The live capture's version line (when available, e.g. `bd --version`) must match `docs/go-parity/README.md`'s pin.
- `/tmp/go-bd-playground/bd-go` should exist and respond to `--version`; if not, rebuild per the parent README's refresh procedure before authoring.
