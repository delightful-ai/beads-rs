## 3. `bd purge --json [--dry-run]`

**Invocation** (`bdstore.go:149-184`): runs from `filepath.Dir(beadsDir)` with `BEADS_DIR=<beadsDir>` env override.

### Go v1.0.2 behavior

```json
{
  "message": "No closed ephemeral beads to purge",
  "purged_count": 0
}
```

With actual purge work, `purged_count` becomes nonzero and more stats fields appear.

### beads-rs behavior

```text
$ bd purge --help
error: unrecognized subcommand 'purge'
```

Not implemented. Rust does not currently have an ephemeral/wisp subsystem at all — no TTL compaction, no `Ephemeral` flag on beads.

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| `--dry-run` | yes | missing | — |
| `--force` | yes (required for non-dry-run outside safe defaults) | missing | — |
| `--older-than <dur>` | yes | missing | — |
| `--pattern <glob>` | yes | missing | — |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| `purged_count` | int (nullable on gascity side via `*int`) | — | gascity parses `purged_count`; must be an integer. |
| `message` | string | — | Optional — gascity doesn't read it. |

### Required work in beads-rs

1. **Decide whether beads-rs has ephemeral beads at all.** Gascity uses purge to clean closed wisps/transient molecules. Rust currently has no ephemeral bit on the bead state, so "purge" has nothing to do. Either:
   - (a) Add an `ephemeral` flag to the core bead state, plumbed through apply/event/wire, and implement real purge semantics. Large piece of work.
   - (b) Ship a no-op `bd purge --json` that returns `{"purged_count": 0, "message": "not implemented"}`. Makes gascity's doctor path happy without doing anything.
   - (c) Decline purge in Rust and teach gascity to skip it on Rust backends.
2. **Honor the `BEADS_DIR` env override** if purge lands as a real implementation. Rust currently discovers repo from cwd or `--repo`; `BEADS_DIR` would need to be recognized as a compat env in bootstrap.
3. **JSON field shape must be `{"purged_count": <int>}`** at minimum; gascity's parser tolerates a nullable `*int` but will fail on a non-object or non-integer.

### Honor-no-metadata notes

Not applicable; purge operates on bead lifecycle state, not metadata.

---


