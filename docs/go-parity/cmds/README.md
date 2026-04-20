# Commands Index

One file per Go `bd` subcommand, pinned to the tag recorded in `../README.md` (v1.0.2, c446a2ef).

All captures in this subtree come from `/tmp/go-bd-playground/bd-go` against `/tmp/go-bd-play/`. See `AGENTS.md` in this directory for the per-command template.

## Seeded canonical examples

- [show](show.md) — canonical read. Establishes the `Issue` JSON shape and the multi-ID array convention.
- [create](create.md) — canonical write. Covers metadata, parent hierarchies, typed deps, batch modes, lifecycle flags.

## Working with issues

- [assign](assign.md) — _deferred_
- [children](children.md) — _deferred_
- [close](close.md) — _deferred_
- [comment](comment.md) — _deferred_
- [comments](comments.md) — _deferred_
- [create](create.md)
- [create-form](create-form.md) — _deferred_
- [delete](delete.md) — _deferred_
- [edit](edit.md) — _deferred_
- [gate](gate.md) — _deferred_ (subcommand family)
- [label](label.md) — _deferred_
- [link](link.md) — _deferred_
- [list](list.md) — _deferred_
- [merge-slot](merge-slot.md) — _deferred_ (subcommand family)
- [note](note.md) — _deferred_
- [priority](priority.md) — _deferred_
- [promote](promote.md) — _deferred_
- [q](q.md) — _deferred_
- [query](query.md) — _deferred_
- [reopen](reopen.md) — _deferred_
- [search](search.md) — _deferred_
- [set-state](set-state.md) — _deferred_
- [show](show.md)
- [state](state.md) — _deferred_
- [tag](tag.md) — _deferred_
- [todo](todo.md) — _deferred_
- [update](update.md) — _deferred_

## Views & reports

- [count](count.md) — _deferred_
- [diff](diff.md) — _deferred_
- [find-duplicates](find-duplicates.md) — _deferred_
- [history](history.md) — _deferred_
- [lint](lint.md) — _deferred_
- [stale](stale.md) — _deferred_
- [status](status.md) — _deferred_
- [statuses](statuses.md) — _deferred_
- [types](types.md) — _deferred_

## Dependencies & structure

- [dep](dep.md) — _deferred_ (subcommand family: add, remove, list, tree, ...)
- [duplicate](duplicate.md) — _deferred_
- [duplicates](duplicates.md) — _deferred_
- [epic](epic.md) — _deferred_ (subcommand family)
- [graph](graph.md) — _deferred_
- [supersede](supersede.md) — _deferred_
- [swarm](swarm.md) — _deferred_ (subcommand family)

## Sync & data

- [backup](backup.md) — _deferred_
- [branch](branch.md) — _deferred_
- [export](export.md) — _deferred_
- [federation](federation.md) — _deferred_ (subcommand family)
- [import](import.md) — _deferred_
- [restore](restore.md) — _deferred_
- [vc](vc.md) — _deferred_ (subcommand family)

## Setup & configuration

- [bootstrap](bootstrap.md) — _deferred_
- [config](config.md) — _deferred_ (subcommand family: set, get, unset, list, drift, apply, show, ...)
- [context](context.md) — _deferred_
- [dolt](dolt.md) — _deferred_ (subcommand family, likely _declined_ in beads-rs)
- [forget](forget.md) — _deferred_
- [hooks](hooks.md) — _deferred_ (subcommand family)
- [human](human.md) — _deferred_
- [info](info.md) — _deferred_
- [init](init.md) — _deferred_
- [kv](kv.md) — _deferred_ (subcommand family)
- [memories](memories.md) — _deferred_
- [onboard](onboard.md) — _deferred_
- [prime](prime.md) — _deferred_
- [quickstart](quickstart.md) — _deferred_
- [recall](recall.md) — _deferred_
- [remember](remember.md) — _deferred_
- [setup](setup.md) — _deferred_
- [where](where.md) — _deferred_

## Maintenance

- [batch](batch.md) — _deferred_
- [compact](compact.md) — _deferred_
- [doctor](doctor.md) — _deferred_
- [flatten](flatten.md) — _deferred_
- [gc](gc.md) — _deferred_
- [migrate](migrate.md) — _deferred_ (subcommand family)
- [preflight](preflight.md) — _deferred_
- [purge](purge.md) — _deferred_
- [rename-prefix](rename-prefix.md) — _deferred_
- [rules](rules.md) — _deferred_ (subcommand family: audit, compact)
- [sql](sql.md) — _deferred_ (likely _declined_ in beads-rs)
- [upgrade](upgrade.md) — _deferred_
- [worktree](worktree.md) — _deferred_ (subcommand family)

## Integrations & advanced

- [admin](admin.md) — _deferred_ (subcommand family)
- [jira](jira.md) — _deferred_ (subcommand family)
- [linear](linear.md) — _deferred_ (subcommand family)
- [repo](repo.md) — _deferred_ (subcommand family)

## Additional commands

- [ado](ado.md) — _deferred_ (Azure DevOps subcommand family)
- [audit](audit.md) — _deferred_
- [blocked](blocked.md) — _deferred_
- [completion](completion.md) — _deferred_
- [cook](cook.md) — _deferred_
- [defer](defer.md) — _deferred_
- [formula](formula.md) — _deferred_ (subcommand family: list, show, ...)
- [github](github.md) — _deferred_ (subcommand family)
- [gitlab](gitlab.md) — _deferred_ (subcommand family)
- [mail](mail.md) — _deferred_
- [mol](mol.md) — _deferred_ (subcommand family: create, bond, distill, wisp, squash, burn, ...)
- [notion](notion.md) — _deferred_ (subcommand family)
- [orphans](orphans.md) — _deferred_
- [ready](ready.md) — _deferred_
- [rename](rename.md) — _deferred_
- [ship](ship.md) — _deferred_
- [undefer](undefer.md) — _deferred_

## Status legend

- (no tag) — written and maintained against the current pin.
- _deferred_ — on the port list; file not yet written.
- _declined_ — explicitly won't port; file should exist with reasoning.
- _extended_ — beads-rs adds to Go's surface; divergence documented.
- _simplified_ — beads-rs is intentionally narrower; divergence documented.

## Count

102 top-level commands in the Go v1.0.2 surface. Subcommand families expand this to roughly 150–200 discrete docs at full coverage.
