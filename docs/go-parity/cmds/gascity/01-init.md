## 1. `bd init --server -p <prefix> --skip-hooks [--server-host H] [--server-port P]`

**Invocation** (`bdstore.go:116-129`):

```go
args := []string{"init", "--server", "-p", prefix, "--skip-hooks"}
// optionally: --server-host, --server-port
```

### Go v1.0.2 behavior

Non-interactive init succeeds; no JSON output (`--json` flag is global but `init` does not honor it).

```text
✓ bd initialized successfully!
  Backend: dolt
  Mode: embedded
  Database: gpi
  Issue prefix: gpi
```

### beads-rs behavior

`bd init --json` emits a minimal envelope:

```json
{ "result": "initialized" }
```

But requires a git remote to be configured (`ERROR error: no origin remote configured`) — gascity currently passes a fresh `s.dir` that has only `git init -q` done, so this blows up immediately.

### Flag delta

| Flag | Go v1.0.2 | Rust | Gap |
|------|-----------|------|-----|
| `--server` | yes (embedded dolt server off, use external) | no | Rust has no notion of a dolt SQL server — its backing store is the CRDT log plus git. Gascity must stop passing `--server`. |
| `-p/--prefix` | yes | no | Rust derives prefix from repo path / normalized remote URL; cannot override. |
| `--skip-hooks` | yes | no | Rust has no post-commit git hook to skip. |
| `--server-host` | yes | no | Same as `--server`: not applicable. |
| `--server-port` | yes | no | Same. |
| `--non-interactive` | yes | no (never interactive) | Harmless to ignore. |
| `--repo` | no | yes | Rust-specific. |
| `--namespace` | no | yes | Rust-specific. |

### JSON field delta

Not applicable: Go init doesn't emit JSON. Rust's `{"result":"initialized"}` is informational only.

### Required work in beads-rs

1. **Accept and ignore the dolt-server flags** when Rust is running in drop-in mode. `--server`, `--server-host=<h>`, `--server-port=<p>`, `--skip-hooks`: parse them, silently ignore. Alternative: hard-error with a clear message pointing gascity at the compat shim.
2. **Accept `-p <prefix>` as an alias for namespace/prefix override.** Even though Rust usually derives prefix from the repo URL, gascity passes `-p $namespace` and expects the resulting bead IDs to use that prefix. Either wire `-p` into the prefix derivation or hard-error with a documented migration note.
3. **Gate the "no origin remote configured" error.** Gascity inits in a directory with a clean `git init` and no remote; Rust rejects this. Two options: (a) infer a synthetic remote URL from the repo path when no remote is set (risky — collides with the beads-daemon keying), or (b) document that gascity must `git remote add origin ...` before calling `bd init`. Tracked separately in `primitives/ids-and-prefixes.md`.
4. **No envelope-unwrapping needed** since gascity discards init's stdout.

### Honor-no-metadata notes

None — init takes no metadata.

---


