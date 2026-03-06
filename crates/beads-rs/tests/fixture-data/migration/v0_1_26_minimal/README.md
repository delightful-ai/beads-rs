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
