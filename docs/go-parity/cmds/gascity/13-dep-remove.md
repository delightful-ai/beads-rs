## 13. `bd dep remove <issueID> <dependsOnID>`

**Invocation** (`bdstore.go:768-774`).

### Go v1.0.2 behavior

Succeeds silently; gascity discards body.

### beads-rs behavior

Subcommand is named `bd dep rm`, not `bd dep remove`.

```text
$ bd dep remove id1 id2
error: unrecognized subcommand 'remove'
```

### Flag delta

| Flag | Go | Rust | Gap |
|------|----|------|-----|
| subcommand name | `remove` | `rm` | **Rename: need `remove` alias.** |
| `<issueID> <dependsOnID>` positional | yes | yes | faithful |
| `--type <depType>` (to distinguish kinds) | yes | `--kind` | Same issue as command 12. Gascity doesn't pass `--type` on remove, so secondary. |

### Required work in beads-rs

1. **Add `remove` as a subcommand alias for `rm`.** Single-line clap change.
2. **Accept `--type` as alias for `--kind`** (same as command 12).
3. **Bare-object success envelope** `{"issue_id":..., "depends_on_id":..., "status":"removed"}` in compat mode, matching Go.

### Honor-no-metadata notes

N/A — remove takes no metadata.

---


