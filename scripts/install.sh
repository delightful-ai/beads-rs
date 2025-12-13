#!/usr/bin/env bash
#
# beads-rs (bd) installation script
# Usage: curl -fsSL https://raw.githubusercontent.com/delightful-ai/beads-rs/main/scripts/install.sh | bash
#

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}==>${NC} $1"; }
log_success() { echo -e "${GREEN}==>${NC} $1"; }
log_warning() { echo -e "${YELLOW}==>${NC} $1"; }
log_error() { echo -e "${RED}Error:${NC} $1" >&2; }

detect_platform() {
    local os arch

    case "$(uname -s)" in
        Darwin)
            os="apple-darwin"
            arch="aarch64"  # Only Apple Silicon supported
            ;;
        Linux)
            os="unknown-linux-gnu"
            arch="x86_64"  # Only x86_64 supported
            ;;
        *)
            log_error "Unsupported OS: $(uname -s)"
            exit 1
            ;;
    esac

    # Verify architecture matches what we support
    local actual_arch="$(uname -m)"
    case "$os" in
        apple-darwin)
            if [[ "$actual_arch" != "arm64" && "$actual_arch" != "aarch64" ]]; then
                log_error "Only Apple Silicon Macs are supported (got: $actual_arch)"
                log_info "Intel Mac users: cargo install beads-rs"
                exit 1
            fi
            ;;
        unknown-linux-gnu)
            if [[ "$actual_arch" != "x86_64" && "$actual_arch" != "amd64" ]]; then
                log_error "Only x86_64 Linux is supported (got: $actual_arch)"
                log_info "ARM Linux users: cargo install beads-rs"
                exit 1
            fi
            ;;
    esac

    echo "${arch}-${os}"
}

install_from_release() {
    log_info "Installing bd from GitHub releases..."

    local platform=$1
    local tmp_dir
    tmp_dir=$(mktemp -d)

    # Get latest release version
    log_info "Fetching latest release..."
    local latest_url="https://api.github.com/repos/delightful-ai/beads-rs/releases/latest"
    local version

    if command -v curl &> /dev/null; then
        version=$(curl -fsSL "$latest_url" | grep '"tag_name"' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')
    elif command -v wget &> /dev/null; then
        version=$(wget -qO- "$latest_url" | grep '"tag_name"' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')
    else
        log_error "Neither curl nor wget found"
        return 1
    fi

    if [ -z "$version" ]; then
        log_error "Failed to fetch latest version"
        return 1
    fi

    log_info "Latest version: $version"

    # Download
    local archive_name="beads-rs-${platform}.tar.gz"
    local download_url="https://github.com/delightful-ai/beads-rs/releases/download/${version}/${archive_name}"

    log_info "Downloading $archive_name..."

    cd "$tmp_dir"
    if command -v curl &> /dev/null; then
        curl -fsSL -o "$archive_name" "$download_url" || { log_error "Download failed"; rm -rf "$tmp_dir"; return 1; }
    else
        wget -q -O "$archive_name" "$download_url" || { log_error "Download failed"; rm -rf "$tmp_dir"; return 1; }
    fi

    # Extract
    log_info "Extracting..."
    tar -xzf "$archive_name" || { log_error "Extraction failed"; rm -rf "$tmp_dir"; return 1; }

    # Install
    local install_dir
    if [[ -w /usr/local/bin ]]; then
        install_dir="/usr/local/bin"
    else
        install_dir="$HOME/.local/bin"
        mkdir -p "$install_dir"
    fi

    log_info "Installing to $install_dir..."
    if [[ -w "$install_dir" ]]; then
        mv bd "$install_dir/"
    else
        sudo mv bd "$install_dir/"
    fi

    # Check PATH
    if [[ ":$PATH:" != *":$install_dir:"* ]]; then
        log_warning "$install_dir is not in your PATH"
        echo ""
        echo "Add to your shell profile (~/.bashrc, ~/.zshrc):"
        echo "  export PATH=\"\$PATH:$install_dir\""
        echo ""
    fi

    cd - > /dev/null
    rm -rf "$tmp_dir"

    log_success "bd installed to $install_dir/bd"
    return 0
}

install_with_cargo() {
    log_info "Installing with cargo..."

    if ! command -v cargo &> /dev/null; then
        return 1
    fi

    if cargo install beads-rs; then
        log_success "bd installed via cargo"
        return 0
    fi

    return 1
}

verify_installation() {
    if command -v bd &> /dev/null; then
        log_success "bd is installed and ready!"
        echo ""
        bd --version 2>/dev/null || echo "bd (beads-rs)"
        echo ""
        echo "Get started:"
        echo "  cd your-project"
        echo "  bd init"
        echo "  bd create 'My first issue' --type=task"
        echo ""
        return 0
    else
        log_error "bd installed but not in PATH"
        return 1
    fi
}

main() {
    echo ""
    echo "beads-rs (bd) Installer"
    echo ""

    log_info "Detecting platform..."
    local platform
    platform=$(detect_platform)
    log_info "Platform: $platform"

    # Try GitHub release first
    if install_from_release "$platform"; then
        verify_installation
        exit 0
    fi

    # Fallback to cargo
    log_warning "Release download failed, trying cargo..."
    if install_with_cargo; then
        verify_installation
        exit 0
    fi

    # Failed
    log_error "Installation failed"
    echo ""
    echo "Manual options:"
    echo "  1. cargo install beads-rs"
    echo "  2. Download from https://github.com/delightful-ai/beads-rs/releases"
    echo ""
    exit 1
}

main "$@"
