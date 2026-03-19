#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fixture_dir="$repo_root/crates/beads-rs/tests/integration/fixtures/legacy_store_corpus/v0_1_26_minimal"

mkdir -p "$fixture_dir"

cat >"$fixture_dir/README.md" <<'EOF'
# v0.1.26 Minimal Store Fixture

This fixture encodes the historical `v0.1.26` store wire shape for a minimal
repo with:

- one live bead
- one embedded bead note
- one line-per-edge dependency
- no `notes.jsonl`
- `meta.json` without checksum fields

It exists so current migration smoke tests can exercise an exact old-store
artifact without rebuilding the historical binary in CI.

Regenerate with:

```bash
scripts/generate-v0_1_26-store-fixture.sh
```

Provenance:

- source tag: `v0.1.26`
- state shape: `src/git/wire.rs` `WireBead`
- dep shape: `src/git/wire.rs` `WireDep`
- meta shape: `src/git/wire.rs` `WireMeta`

`tombstones.jsonl` is intentionally a zero-byte file.
EOF

cat >"$fixture_dir/state.jsonl" <<'EOF'
{"id":"bd-abc1","created_at":[1,0],"created_by":"alice","title":"Legacy migration task","description":"state emitted by v0.1.26","priority":1,"type":"task","labels":["legacy","migration"],"status":"open","notes":[{"id":"note-abc1","content":"preserve this note","author":"alice","at":[2,0]}],"_at":[1,0],"_by":"alice"}
EOF

: >"$fixture_dir/tombstones.jsonl"

cat >"$fixture_dir/deps.jsonl" <<'EOF'
{"from":"bd-abc1","to":"bd-abc2","kind":"blocks","created_at":[3,0],"created_by":"alice"}
EOF

cat >"$fixture_dir/meta.json" <<'EOF'
{
  "format_version": 1,
  "root_slug": "bd"
}
EOF

printf 'wrote fixture files to %s\n' "$fixture_dir"
