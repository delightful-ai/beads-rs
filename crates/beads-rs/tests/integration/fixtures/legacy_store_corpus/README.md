# Legacy Store Fixture Corpus

These fixtures are small, checked-in `refs/heads/beads/store` snapshots used by
the migration integration tests. They are not raw repo copies; they are reduced
fixtures anchored to one real local source store plus a small number of
documented derived variants.

## Source anchor

- Source repo path at capture time: `/Users/darin/Projects/mcp_dspy_eveals`
- Capture date: `2026-03-11`
- Source repo `HEAD` at capture time: `d770293d43889b4b3b61eedf05ae4f757eccebf6`
- Source `refs/heads/beads/store` OID at capture time:
  `f585e7883ad025b4ee1461fcbbfc6cf14f558b00`

## Fixture inventory

| Fixture | Kind | Source / derivation | Purpose |
| --- | --- | --- | --- |
| `rich_workflow` | Reduced capture | Reduced by hand from `/Users/darin/Projects/mcp_dspy_eveals` store `f585e7883ad025b4ee1461fcbbfc6cf14f558b00`; titles/content shortened while preserving field shape. | Claimed + closed beads, embedded notes, legacy labels array, legacy alias-shaped deps, missing `notes.jsonl`, and `meta.json` without checksums. |
| `rich_workflow_peer` | Derived variant | Built locally on `2026-03-11` from the `rich_workflow` reduced capture, then edited to add peer-only divergence: later `_at` on `bd-rich-open`, extra `api` label, peer-side note/label changes on `bd-rich-claimed`, a peer-only bead, and a peer-only dep edge. The peer variant also removes the base alias-shaped related dep edge so the merge tests prove that local-only legacy deps survive related divergence instead of being accidentally recreated by the peer fixture. | Related/unrelated local-vs-remote divergence coverage with realistic peer-side updates. |
| `tombstone_deleted_dep` | Derived variant | Built locally on `2026-03-11` from the same source-store shape family as `rich_workflow`, then minimized into a tombstone-focused scenario with one active dep and one deleted legacy dep row following the current parser contract in `crates/beads-git/src/wire.rs`. The fixture also omits `notes.jsonl` while keeping one embedded bead note so migration must backfill notes from state. A scan across current repos under `/Users/darin/Projects` and `/Users/darin/src/personal` found no live `refs/heads/beads/store` that still carried deleted legacy dep rows, so this remains contract-derived for now; tracked follow-up: `beads-rs-qin2`. | Tombstones, deleted legacy dep rows, embedded-note backfill, active dep preservation, and missing `meta.json`. |

## Extraction / refresh recipe

To refresh the real-source anchor or derive new reduced fixtures:

1. Resolve the source store ref:

   ```bash
   SOURCE_REPO=/path/to/source/repo
   git -C "$SOURCE_REPO" show-ref refs/heads/beads/store
   ```

2. Extract the canonical store files from the chosen store commit:

   ```bash
   STORE_OID=<store-oid-from-show-ref>
   git -C "$SOURCE_REPO" show "$STORE_OID:state.jsonl" > state.jsonl
   git -C "$SOURCE_REPO" show "$STORE_OID:tombstones.jsonl" > tombstones.jsonl
   git -C "$SOURCE_REPO" show "$STORE_OID:deps.jsonl" > deps.jsonl
   git -C "$SOURCE_REPO" show "$STORE_OID:meta.json" > meta.json
   ```

3. Reduce the extracted files by deleting unrelated rows and shortening large
   free-text content, but do not change the wire shapes the fixture is meant to
   cover.

4. If a fixture is synthetic or derived, record the exact transformation in the
   inventory table above instead of describing it as a raw capture.

## Corpus invariants

- `deps.jsonl` is intentionally legacy line-per-edge JSONL in every fixture.
- `notes.jsonl` is intentionally absent from the rich fixtures and from
  `tombstone_deleted_dep`, so migration must backfill notes from embedded bead
  notes in both paths.
- `rich_workflow_peer` and `tombstone_deleted_dep` are derived variants, not raw
  source-store extracts.
- `tombstone_deleted_dep` currently proves the implemented deleted-row parser
  contract, not an independently captured historical encoding. `beads-rs-qin2`
  tracks replacing it with a real-source capture.
