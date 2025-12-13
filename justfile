# Default recipe - show available commands
default:
    @just --list

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
