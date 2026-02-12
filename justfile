# Default recipe - point to available commands
default:
    @echo "Run 'just --list' to see available commands."

# Run all checks (fmt, clippy, test)
check: fmt-check lint test

# Format code
fmt:
    cargo fmt --all

# Check formatting without changing files
fmt-check:
    cargo fmt --all -- --check

# Run custom dylint lints
dylint:
    cargo dylint --path lints --pattern beads_lints --all
    cd lints && cargo test -p beads_lints --test crate_dag_policy

# Run clippy lints
lint: dylint
    cargo clippy --all-features -- \
        -D warnings \
        -D clippy::match_wildcard_for_single_variants \
        -D clippy::wildcard_in_or_patterns
    cargo clippy \
        -p beads-core \
        -p beads-api \
        -p beads-surface \
        -p beads-cli \
        -p beads-daemon \
        -p beads-daemon-core \
        --all-features -- \
        -D warnings \
        -D clippy::match_wildcard_for_single_variants \
        -D clippy::wildcard_in_or_patterns

# Run tests
test:
    cargo test --all-features

# Run the fast test tier (default)
test-fast:
    cargo test --all-features

# Run slow tests (opt-in)
test-slow:
    cargo test --all-features --features slow-tests

# Generate all-features line coverage and print area rollup.
coverage-areas:
    #!/usr/bin/env bash
    set -euo pipefail
    summary="target/llvm-cov-summary.all-features.json"

    cargo llvm-cov clean --workspace
    cargo llvm-cov \
        --workspace \
        --all-features \
        --json \
        --summary-only \
        --output-path "$summary" \
        -- \
        --skip daemon::lifecycle::test_concurrent_restart_safety \
        --skip daemon::lifecycle::test_thundering_herd_single_daemon \
        --skip daemon::repl_e2e::repl_daemon_crash_restart_tailnet_roundtrip \
        --skip daemon::repl_e2e::repl_daemon_roster_reload_and_epoch_bump_roundtrip \
        --skip daemon::repl_e2e::repl_daemon_to_daemon_tailnet_roundtrip

    render_cmd='
      def pct($covered; $count):
        if $count == 0 then 100 else (100 * $covered / $count) end;
      def pct_fmt($covered; $count):
        (((pct($covered; $count) * 100) | round) / 100 | tostring) + "%";
      def area($name; $re):
        ([.data[0].files[]
          | select(.filename | test($re))
          | .summary.lines] // []) as $rows
        | {
            name: $name,
            covered: (($rows | map(.covered) | add) // 0),
            count: (($rows | map(.count) | add) // 0)
          };

      ([
        area("workspace_total"; ".*"),
        area("beads-api"; "/crates/beads-api/src/"),
        area("beads-core"; "/crates/beads-core/src/"),
        area("beads-surface"; "/crates/beads-surface/src/"),
        area("beads-cli"; "/crates/beads-cli/src/"),
        area("beads-daemon"; "/crates/beads-daemon/src/"),
        area("beads-daemon-core"; "/crates/beads-daemon-core/src/"),
        area("beads-rs_cli"; "/crates/beads-rs/src/cli/"),
        area("beads-rs_daemon"; "/crates/beads-rs/src/daemon/")
      ]) as $areas
      | (["area", "covered", "total", "line_percent"] | @tsv),
        ($areas[] | [.name, (.covered|tostring), (.count|tostring), pct_fmt(.covered; .count)] | @tsv)
    '

    if command -v column >/dev/null 2>&1; then
        jq -r "$render_cmd" "$summary" | column -t -s $'\t'
    else
        jq -r "$render_cmd" "$summary"
    fi

    echo "coverage summary: $summary"

# Show daemon-focused files with lowest line coverage.
coverage-daemon-gaps:
    #!/usr/bin/env bash
    set -euo pipefail
    summary="target/llvm-cov-summary.all-features.json"
    if [ ! -f "$summary" ]; then
        just coverage-areas >/dev/null
    fi

    render_cmd='
      def pct($covered; $count):
        if $count == 0 then 100 else (100 * $covered / $count) end;
      def pct_fmt($covered; $count):
        (((pct($covered; $count) * 100) | round) / 100 | tostring) + "%";
      ([
        .data[0].files[]
        | select(.filename | test("/crates/beads-rs/src/daemon/|/crates/beads-daemon/src/|/crates/beads-daemon-core/src/"))
        | {
            file: (.filename | sub("^.*/beads-rs/"; "")),
            covered: .summary.lines.covered,
            count: .summary.lines.count,
            pct: pct(.summary.lines.covered; .summary.lines.count)
          }
        | select(.pct < 80)
      ] | sort_by(.pct)) as $rows
      | (["file", "covered", "total", "line_percent"] | @tsv),
        ($rows[] | [.file, (.covered|tostring), (.count|tostring), pct_fmt(.covered; .count)] | @tsv)
    '

    if command -v column >/dev/null 2>&1; then
        jq -r "$render_cmd" "$summary" | column -t -s $'\t'
    else
        jq -r "$render_cmd" "$summary"
    fi

# Run a specific test
test-one NAME:
    cargo test --all-features {{NAME}}

# Build debug
build:
    cargo build --all-features

# Build release
build-release:
    cargo build --release --all-features

# Run the CLI
run *ARGS:
    cargo run --bin bd -- {{ARGS}}

# Benchmark common CLI/daemon hotpaths with reproducible artifacts.
# Env knobs:
# - BD_BIN=./target/debug/bd (default)
# - RUNS=15 WARMUP=3 HOTPATH_ITERS=20
bench-hotpaths:
    #!/usr/bin/env bash
    set -euo pipefail
    BD_BIN="${BD_BIN:-./target/debug/bd}"
    if [[ "$BD_BIN" == "./target/debug/bd" && ! -x "$BD_BIN" ]]; then
        cargo build --bin bd
    fi
    BD_BIN="$BD_BIN" LOG="${LOG:-error}" HOTPATH_ITERS="${HOTPATH_ITERS:-20}" ./scripts/profile-hotpaths.sh

# Compare two hotpath benchmark artifact directories.
# Usage: just bench-compare tmp/perf/hotpaths-old tmp/perf/hotpaths-new
bench-compare BASE NEW:
    ./scripts/compare-hotpaths.sh {{BASE}} {{NEW}}

# Fail the run if critical hotpaths regress above threshold.
# Usage: just bench-guard tmp/perf/hotpaths-old tmp/perf/hotpaths-new
# Optional env overrides:
# - READ_THRESHOLD_PCT=25
# - WRITE_THRESHOLD_PCT=35
bench-guard BASE NEW:
    ./scripts/check-hotpaths-regression.sh {{BASE}} {{NEW}}

# Clean build artifacts
clean:
    cargo clean

# Watch for changes and run tests
watch:
    cargo watch -x 'test'

# Pre-commit hook - run before committing
pre-commit: fmt lint test
    @echo "All checks passed!"

# Prepare for release (run all checks)
release-prep: check
    @echo "Ready for release!"

# Release a new version (bump minor: 0.1.0 -> 0.2.0)
release-minor: check
    #!/usr/bin/env bash
    set -e
    VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
    MAJOR=$(echo $VERSION | cut -d. -f1)
    MINOR=$(echo $VERSION | cut -d. -f2)
    PATCH=$(echo $VERSION | cut -d. -f3)
    NEW_MINOR=$((MINOR + 1))
    NEW_VERSION="$MAJOR.$NEW_MINOR.0"
    echo "Bumping version: $VERSION -> $NEW_VERSION"
    perl -pi -e "s/^version = \".*\"/version = \"$NEW_VERSION\"/" Cargo.toml
    perl -pi -e "s/version = \".*\";/version = \"$NEW_VERSION\";/" flake.nix
    cargo check
    git add Cargo.toml Cargo.lock flake.nix
    git commit -m "chore: bump version to $NEW_VERSION"
    git tag "v$NEW_VERSION"
    git push && git push --tags
    echo "Released v$NEW_VERSION!"

# Release a new version (bump patch: 0.1.0 -> 0.1.1)
release-patch: check
    #!/usr/bin/env bash
    set -e
    VERSION=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')
    MAJOR=$(echo $VERSION | cut -d. -f1)
    MINOR=$(echo $VERSION | cut -d. -f2)
    PATCH=$(echo $VERSION | cut -d. -f3)
    NEW_PATCH=$((PATCH + 1))
    NEW_VERSION="$MAJOR.$MINOR.$NEW_PATCH"
    echo "Bumping version: $VERSION -> $NEW_VERSION"
    perl -pi -e "s/^version = \".*\"/version = \"$NEW_VERSION\"/" Cargo.toml
    perl -pi -e "s/version = \".*\";/version = \"$NEW_VERSION\";/" flake.nix
    cargo check
    git add Cargo.toml Cargo.lock flake.nix
    git commit -m "chore: bump version to $NEW_VERSION"
    git tag "v$NEW_VERSION"
    git push && git push --tags
    echo "Released v$NEW_VERSION!"
