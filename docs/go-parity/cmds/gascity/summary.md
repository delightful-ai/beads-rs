## Summary

**Count (of 14 commands):**

- **0 faithful** — no command is fully compatible today. Field renames, envelope wrappers, or timestamp shapes block every single one.
- **6 require wire-compat only** — `show`, `list`, `ready`, `dep list` (once subcommand added), `close` (once batch added), `dep add` (once `--type` alias added). These need the compat shim listed above (bare envelope, `issue_type` field, RFC3339 timestamps, etc.), plus small flag renames/aliases.
- **4 require wire-compat plus a missing flag or semantic** — `create` needs custom-type acceptance + `--metadata` or a typed replacement, `update` needs `--set-metadata` or typed replacement, `dep remove` needs an alias for `rm`, `delete` needs `--force` no-op.
- **4 commands/subcommands are entirely absent** — `config set`, `config get`, `purge`, `create --graph`.

**Critical blockers (in implementation order):**

1. **`bd config set` / `bd config get --json types.custom`** — the doctor check runs on every `gc doctor` invocation. Without it, gascity cannot even declare Rust-bd as "usable." (Command 2.)
2. **Custom bead types.** Without `types.custom`, `bd create -t molecule` / `-t convoy` / `-t gate` etc. fail immediately. This is a core-crate change (close enum `BeadType` to validated string). (Command 4 + `primitives/custom-types.md`.)
3. **Wire-compat mode** covering the envelope drop, `issue_type` rename, RFC3339 timestamps, and `dependency_type` on dep-list rows. Ships a flag (`--compat-wire=go`) or env var gate. (Commands 4, 6, 7, 8, 11, 14.)
4. **`bd dep list`** — subcommand is missing entirely. (Command 14.)
5. **`bd dep add --type` alias** and `bd dep remove` as alias for `rm`. (Commands 12, 13.)
6. **`bd create --graph <file>`** — without graph-apply, gascity cannot instantiate molecules atomically. (Command 5.)
7. **`bd close id1 id2 ...` batch** and `bd ready` bare-array output. (Commands 9, 11.)
8. **Metadata flag replacements** — the single largest semantic design pass. See `primitives/metadata-remapping.md` for the full flag catalog. Affects commands 4, 5, 7, 8, 14.
9. **`bd purge`** — either implement, stub as no-op, or explicitly decline. (Command 3.)
10. **`bd init` flag acceptance** — ignore `--server`, `--skip-hooks`, `--server-host`, `--server-port`, accept `-p <prefix>`, and fix the "no origin remote" fatal. (Command 1.)

**Non-blockers (important but not gating drop-in use):**

- Durability-proof bloat in mutation responses (trim or gate).
- Assignee compat mode actually honoring the passed value.
- `--include-infra` / `--include-gates` / `--include-templates` on list (accept-and-ignore until types exist).
- `dependencies_removed` / `references_updated` fields on delete.

