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

# Run clippy lints
lint:
    cargo clippy --all-features -- -D warnings

# Run tests
test:
    cargo test --all-features

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

# Clean build artifacts
clean:
    cargo clean

# Watch for changes and run tests
watch:
    cargo watch -x 'test --all-features'

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
