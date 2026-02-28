# bd-r39f Implementation Plan

**Goal:** Make fsck repair salvage the valid WAL prefix by truncating at the first bad frame/record boundary, and quarantine whole segments only when no trustworthy prefix boundary exists.  
**Architecture:** Hard-cutover the fsck repair model from `kind + string detail` to typed repair outcomes. Centralize repair decisions in fsck scanning so all recoverable corruption paths use one default behavior: prefix salvage + truncate.  
**Tech Stack:** Rust, `beads-daemon-core` WAL fsck, `beads-api` admin schema, daemon admin mapping, CLI renderer, integration tests.

## Locked Decisions

1. Hard cutover only: remove old repair shape (`FsckRepairKind` + `FsckRepair { path, detail }`) and replace it everywhere in one change.
2. “Valid prefix” for frame/record scan failures is the bytes up to the current `offset` (including segment header), so recoverable scan corruption truncates to `offset`.
3. Whole-segment quarantine is only for segment-header-invalid/unreadable cases where a safe truncate boundary cannot be trusted.
4. Fsck report output must carry typed repair metadata (offsets/paths/cause), not free-form strings.

## Exact Files, Functions, Types To Change

1. Modify [fsck.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon-core/src/wal/fsck.rs).  
Functions: `fsck_store_dir`, `scan_segment`, `list_segments`, `truncate_tail`, `quarantine_segment`.  
Types: `FsckRepairKind` (remove), `FsckRepair` (replace), `SegmentScanResult` (update).  
Changes:
- Replace repair model with tagged enum (see typed API section).
- Remove per-branch `can_truncate` quarantine fallback in `scan_segment`; all recoverable scan corruption branches truncate to `offset`.
- Keep quarantine only in `list_segments` header-invalid branch.
- Return repaired final length from `scan_segment` and write it back before index checks, so post-repair `check_index_offsets` compares against true file length.
- Update suggested-action text in scan branches to salvage/truncate wording, and keep quarantine wording only for header-invalid/no-prefix branch.

2. Modify [admin.rs](/Users/darin/src/personal/beads-rs/crates/beads-api/src/admin.rs).  
Types: `FsckRepairKind` (remove), `FsckRepair` (replace).  
Changes:
- Mirror the new typed fsck repair enum in API DTOs.
- Keep serde `snake_case` kind tagging for stable JSON shape after cutover.

3. Modify [admin.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/admin.rs).  
Function: `fsck_repair_to_api`.  
Changes:
- Replace old kind mapping with variant-to-variant mapping for new typed repair enum.
- Add/adjust unit coverage in existing `#[cfg(test)]` module to assert all repair variants map correctly.

4. Modify [store.rs](/Users/darin/src/personal/beads-rs/crates/beads-cli/src/commands/store.rs).  
Functions/tests: `render_admin_fsck`, `sample_fsck_output`, `render_fsck_human_golden`.  
Changes:
- Render typed repair variants explicitly (truncate outcome with offsets/bytes, quarantine outcome with source/destination, rebuild index outcome with path).
- Update golden output to new typed render format.
- Update fsck help text from generic “tail truncation, quarantine” to prefix-salvage-first wording.

5. Modify [fsck.rs](/Users/darin/src/personal/beads-rs/crates/beads-rs/tests/integration/wal/fsck.rs).  
Tests:
- Update current mid-file test to assert typed truncate outcome fields, not just old `FsckRepairKind`.
- Add explicit “quarantine only when no valid prefix exists” test by creating header-invalid segment and asserting quarantine typed outcome.
- Assert no quarantine outcome is emitted in mandatory mid-file-prefix-salvage scenario.

## Typed API Changes (Hard Cutover)

Use this shape in both daemon-core fsck report and beads-api admin output:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FsckRepair {
    PrefixSalvageTruncate {
        segment_path: PathBuf,
        truncate_to_offset: u64,
        discarded_suffix_bytes: u64,
        cause: FsckEvidenceCode,
    },
    QuarantineNoValidPrefix {
        original_segment_path: PathBuf,
        quarantined_path: PathBuf,
        cause: FsckEvidenceCode,
    },
    RebuildIndex {
        index_path: PathBuf,
    },
}
```

Semantics:
- `PrefixSalvageTruncate` is the default recoverable repair in `scan_segment`.
- `QuarantineNoValidPrefix` is emitted only from header-invalid/unreadable segment handling.
- `RebuildIndex` remains after successful repair-mode index rebuild.

## Test Changes/Additions (Mandatory Scenario Included)

1. Update `fsck_repair_truncates_mid_file_corruption_to_preserve_prefix` in [fsck.rs](/Users/darin/src/personal/beads-rs/crates/beads-rs/tests/integration/wal/fsck.rs):
- Corrupt frame index `1` (mid-file).
- Assert repair includes `PrefixSalvageTruncate` with `truncate_to_offset == segment.frame_offset(1)`.
- Assert `QuarantineNoValidPrefix` is absent.
- Assert repaired segment still exists and length equals `segment.frame_offset(1)`.

2. Add `fsck_repair_quarantines_only_when_no_valid_prefix_exists` in [fsck.rs](/Users/darin/src/personal/beads-rs/crates/beads-rs/tests/integration/wal/fsck.rs):
- Make segment header unreadable (truncate below header length).
- Run repair.
- Assert repair includes `QuarantineNoValidPrefix`.
- Assert original segment path moved into `.../quarantine/...`.

3. Update CLI golden unit test `render_fsck_human_golden` in [store.rs](/Users/darin/src/personal/beads-rs/crates/beads-cli/src/commands/store.rs) for new typed repair rendering.

4. Add daemon mapping test in [admin.rs](/Users/darin/src/personal/beads-rs/crates/beads-daemon/src/runtime/admin.rs) to cover all new repair variants.

## Verification Commands

```bash
cargo test -p beads-rs --test integration wal::fsck
cargo test -p beads-cli render_fsck_human_golden
cargo test -p beads-daemon runtime::admin::tests

cargo check
cargo fmt --all
just dylint
cargo clippy --all-features -- -D warnings
cargo test
cargo test --features slow-tests
```

## Risks and Rollback Notes

1. Risk: API shape break across fsck output consumers.  
Mitigation: apply cutover in one atomic workspace change (`beads-daemon-core` + `beads-api` + `beads-daemon` + `beads-cli` + tests).

2. Risk: Incorrect repaired-length propagation can cause false index-offset failures post-repair.  
Mitigation: update `SegmentScanResult` to carry final length and use it before index checks; cover with integration assertions.

3. Risk: Operator guidance drift in human output.  
Mitigation: update CLI render/golden and fsck suggested-action text in the same change.

4. Rollback plan:
- Code rollback: `jj backout` the cutover change if regressions appear.
- Data rollback after a bad repair run: restore from backup or quarantined segment copies, then re-run fsck with fixed build.
