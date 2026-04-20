# Go Parity Specification

This subtree is the living specification of [Go beads](https://github.com/gastownhall/beads)' surface, pinned to a specific upstream tag and used as the contract beads-rs ports against.

The goal is to make every porting decision explicit and grounded in real behavior — not paraphrased documentation, not stale changelog entries. Every command, primitive, and type gets its own file with a live-captured JSON shape, a flag table derived from `bd --help` against the pinned binary, and a parity-status decision for beads-rs.

## Current pin

| Field | Value |
|-------|-------|
| Tag | `v1.0.2` |
| Commit | `c446a2ef` |
| Release date | 2026-04-18 |
| Upstream clone | `~/vendor/github.com/gastownhall/beads` |
| Local binary | `/tmp/go-bd-playground/bd-go` |
| Build | `go build -tags gms_pure_go -o /tmp/go-bd-playground/bd-go ./cmd/bd` |

## Scope

**Faithful at the semantic layer.** Command names, flag names, JSON field names, error classes, typed field additions (e.g. `started_at`, Spike/Story/Milestone, Slots, custom types). If Go ships typed state, beads-rs ports that type — the whole point of both codebases is "types should tell the truth."

**Idiomatic at the engine layer.** CRDT merge rules, daemon architecture, library crate seams. Where Go's Dolt-flavored shape conflicts with beads-rs's CRDT substrate (e.g. `HookFiringStore` as a storage decorator, `time.Time` vs `WriteStamp`), beads-rs re-derives the semantics against its own engine. Those divergences are called out per-file.

**Explicitly not in scope.** beads-rs-only surfaces — daemon IPC wire, library crate boundaries, internal test fixtures. Those live in `docs/IPC_PROTOCOL.md`, `docs/CRATE_DAG.md`, and crate-local `AGENTS.md`.

## Layout

- `cmds/` — one file per Go `bd` subcommand. Covers semantics, flags, live JSON captures, side effects, parity status. See `cmds/AGENTS.md` for the per-cmd template.
- `primitives/` — per-concept design notes (molecules, formulas, wisps, slots, hooks, custom types). Captures what the concept IS in Go, and the chosen beads-rs realization.
- `types/` — per bead type variant (task, epic, spike, story, milestone, message, convoy, gate, role, rig, agent, merge-request, slot, event). Fields specific to each type, lifecycle states, exclusion rules.
- `data-model.md` — canonical Go `Issue` JSON shape, field by field, pinned to the tag. This is the reference that individual cmd/type docs cite.

## Parity status vocabulary

Every command, primitive, and type carries one of:

- **`faithful`** — beads-rs matches Go's surface (command exists, same flags, same JSON shape, same errors). Differences are implementation-level only.
- **`extended`** — beads-rs preserves Go's surface as a subset and adds fields/flags/semantics that Go doesn't have. Old Go callers still work.
- **`simplified`** — beads-rs intentionally narrower than Go. Document the forcing function.
- **`deferred`** — on the port list, not yet implemented. Doc exists because the target shape matters.
- **`declined`** — explicitly won't port. Document the reason — usually "depends on Dolt" or "superseded by CRDT semantics."

## Refreshing the pin

When Go ships a new release and beads-rs needs to realign:

1. Update the upstream clone:
   ```bash
   cd ~/vendor/github.com/gastownhall/beads
   git fetch origin && git checkout vX.Y.Z
   ```
2. Rebuild the playground binary:
   ```bash
   go build -tags gms_pure_go -o /tmp/go-bd-playground/bd-go ./cmd/bd
   /tmp/go-bd-playground/bd-go --version  # confirm vX.Y.Z (dev)
   ```
3. Read the changelog for `vPREV..vX.Y.Z`. For each new/removed/changed command or field:
   - Net-new commands → add a file under `cmds/`.
   - Retired commands → mark the existing file `declined` or delete with a note in the index.
   - Changed flags/shapes → re-run the capture commands in the affected files and diff the JSON.
4. Bump the pin at the top of this file AND in the `**Pin:**` line of every cmd/primitive/type doc touched.
5. Verify:
   ```bash
   grep -REh '^\*\*Pin:\*\*' docs/go-parity/ | sort -u   # should show one tag
   ```

## Playground

All live captures come from `/tmp/go-bd-playground/bd-go` (v1.0.2). Never run live-capture commands against beads-rs's own `bd` (`~/.cargo/bin/bd`) — that's the thing being ported to, not the reference.

To init a throwaway playground:

```bash
mkdir -p /tmp/go-bd-play && cd /tmp/go-bd-play && git init -q
/tmp/go-bd-playground/bd-go init --non-interactive
```

Delete and recreate freely; nothing important lives there.

## Why this exists

The old approach was a 54-child epic (`bd-ze0x`, closed 2026-04-20) that catalogued Go features via a changelog scrape. It went stale within three months because Go kept shipping and the beads-rs design itself shifted (CRDT overhaul, library-first surface, crate DAG settlement). Living specs that cite real binary behavior and get refreshed on each Go release are the antidote. Every doc here is dated, provenance-ful, and actively maintained — or it gets retired.
