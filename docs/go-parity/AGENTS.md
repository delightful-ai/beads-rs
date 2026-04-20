## Boundary
This subtree is the living specification of Go beads' surface, pinned to a specific upstream tag. It captures what beads-rs intentionally yoinks from Go (commands, flags, JSON field names, data-model shapes, error classes, type variants) and the parity decisions made per concept.
NEVER: treat this subtree as an execution plan, a roadmap, or a promise that beads-rs will match Go field-for-field. Parity is a deliberate yes/no per concept; divergences are load-bearing and must be documented here, not erased.
NEVER: let docs here go stale against Go without refreshing the pin. Pointing at a months-old tag in cmd docs is actively unsafe guidance; refresh or retire.

## Routing
- `README.md` — what this subtree is, current Go pin (tag + commit), the refresh procedure, and the layout.
- `cmds/` — one file per Go `bd` subcommand, each capturing semantics, flags, live JSON captures, side effects, and beads-rs parity status. Read the child `AGENTS.md` there for the per-command template.
- `primitives/` — per-concept design notes (molecule, formula, wisp, slot, hook, custom-types) — what the concept IS in Go and how beads-rs chooses to realize it.
- `types/` — per bead type variant (task, epic, spike, story, milestone, message, convoy, gate, role, rig, agent, merge-request, slot, event).
- `data-model.md` — canonical Go `Issue` JSON shape pinned to the tag, field by field, with Rust parity notes.

## Local rules
- Pin provenance is mandatory. Every cmd/primitive/type doc names the Go tag and commit it was derived from. Refreshes bump the pin inline, not in a sidecar changelog.
- JSON shapes are ground truth. If a doc claims a field exists, it cites an actual command + output captured from the pinned binary at `/tmp/go-bd-playground/bd-go`. No paraphrased fields.
- Parity status per concept is one of: `faithful` (surface matches Go), `extended` (beads-rs adds fields/semantics without breaking Go callers), `simplified` (intentionally narrower than Go; explain why), `deferred` (on the port list, not yet implemented), `declined` (explicitly won't port; explain why).
- Rust divergences are architectural, not apologetic. When the CRDT engine or daemon forces a different shape (e.g. typed `WriteStamp` vs RFC3339, per-issue slots vs freeform metadata), explain the forcing function; don't wave it off as "idiomatic Rust".
- On-touch rule: when a Rust implementation lands, the cmd/primitive doc gets a `Rust source` pointer and a parity-status update in the same change. Stale `not implemented` notes after work lands is a sev defect of this subtree.
- Out of scope: beads-rs-only surfaces (daemon IPC, library crates, crate DAG). Those live in `docs/IPC_PROTOCOL.md`, `docs/CRATE_DAG.md`, and crate-local `AGENTS.md`.

## Verification
- `git -C ~/vendor/github.com/gastownhall/beads describe --tags --abbrev=0` must match the pin in `README.md`.
- `/tmp/go-bd-playground/bd-go --version` must match the pin in `README.md`.
- `grep -RE "^\*\*Pin:\*\*" docs/go-parity/` should show one line per cmd doc with the same tag.
