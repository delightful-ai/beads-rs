## 2. `bd config set <key> <value>` and `bd config get --json <key>`

**Invocation** (`bdstore.go:132-138`, `checks_custom_types.go:152-158`, `178-182`):

```go
// set
s.runner(s.dir, "bd", "config", "set", key, value)
// get (parsed as {"key":..., "value":...} with types.custom's value a comma-joined string)
exec.Command("bd", "config", "get", "--json", "types.custom")
```

### Go v1.0.2 behavior

```text
$ bd config set types.custom molecule,convoy
Set types.custom = molecule,convoy

$ bd config get --json types.custom
{
  "key": "types.custom",
  "value": "molecule,convoy"
}
```

If unset, `value` is an empty string (never the human sentinel `(not set)` — that's the point of `--json`).

### beads-rs behavior

```text
$ bd config --help
error: unrecognized subcommand 'config'
  tip: a similar subcommand exists: 'count'
```

Not implemented. Rust has **no** `config` subcommand; configuration is file-based (see `crates/beads-bootstrap/src/config.rs` for the schema, which is TOML at `~/.config/beads-rs/config.toml`).

### Flag delta

| Flag | Go v1.0.2 | Rust | Gap |
|------|-----------|------|-----|
| `config set` subcmd | yes | missing | Need full subcommand. |
| `config get --json <key>` subcmd | yes | missing | Need full subcommand. |

### JSON field delta

| Field | Go | Rust | Gap |
|-------|----|----|-----|
| `key` | string | — | add |
| `value` | string ("" when unset) | — | add |

### Required work in beads-rs

1. **Add a `bd config` subcommand family** covering at minimum `set <key> <value>`, `get [--json] <key>`, and `list [--json]`.
2. **Decide the storage layer.** Three options:
   - (a) Write into the bootstrap TOML (`~/.config/beads-rs/config.toml`). Clean; matches existing precedence rules in `beads-bootstrap`.
   - (b) Write into a new per-repo config file under `.beads/config.toml`. Matches Go's per-project semantics but requires a second config schema.
   - (c) Treat config keys as well-known events in the CRDT log. Correct for the project philosophy but over-engineered for gascity's current `types.custom`-only use.
   Recommended: (b) for gascity compatibility — `.beads/config.toml`, with a documented list of keys (`types.custom`, plus whatever else shows up).
3. **Implement `types.custom` specifically** as a list of validated custom type names. Parse the comma-joined input, store as a `Vec<ValidatedBeadTypeName>`, emit back as a single comma-joined string on `get --json` to match gascity's parser (`parseCustomTypesJSON` in `checks_custom_types.go`).
4. **Must honor the "empty value" contract.** When `types.custom` is unset, `{"key":"types.custom","value":""}` is what gascity expects. Do not emit `null` or a missing field.

### Honor-no-metadata notes

`config set`/`config get` are structured k/v, not metadata, and are acceptable. The gascity consumer in `checks_custom_types.go` only ever touches `types.custom`; we can implement a narrow, typed first version and grow the key surface as needed. A follow-up bead should enumerate the full Go key namespace (export.*, jira.*, linear.*, github.*, custom.*, status.*, doctor.suppress.*) and decide which land in Rust and which get declined.

---


