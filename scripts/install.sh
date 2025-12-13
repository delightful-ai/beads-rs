#!/usr/bin/env bash
#
# beads-rs (bd) installation script
# Usage: curl -fsSL https://raw.githubusercontent.com/delightful-ai/beads-rs/main/scripts/install.sh | bash
#
# Prebuilt binaries: x86_64 Linux, Apple Silicon
# Other platforms: auto-fallback to cargo install
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

# Re-sign binary for macOS to avoid slow Gatekeeper checks
resign_for_macos() {
    local binary_path=$1

    [[ "$(uname -s)" != "Darwin" ]] && return 0
    command -v codesign &> /dev/null || return 0

    log_info "Re-signing binary for macOS..."
    codesign --remove-signature "$binary_path" 2>/dev/null || true
    if codesign --force --sign - "$binary_path" 2>/dev/null; then
        log_success "Binary re-signed (faster Gatekeeper)"
    fi
}

detect_platform() {
    local actual_arch
    actual_arch="$(uname -m)"

    case "$(uname -s)" in
        Darwin)
            if [[ "$actual_arch" == "arm64" || "$actual_arch" == "aarch64" ]]; then
                echo "aarch64-apple-darwin"
                return 0
            fi
            ;;
        Linux)
            if [[ "$actual_arch" == "x86_64" || "$actual_arch" == "amd64" ]]; then
                echo "x86_64-unknown-linux-gnu"
                return 0
            fi
            ;;
    esac

    return 1
}

install_from_release() {
    log_info "Installing bd from GitHub releases..."

    local platform=$1
    local tmp_dir
    tmp_dir=$(mktemp -d)

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

    local archive_name="beads-rs-${platform}.tar.gz"
    local download_url="https://github.com/delightful-ai/beads-rs/releases/download/${version}/${archive_name}"

    log_info "Downloading $archive_name..."

    cd "$tmp_dir"
    if command -v curl &> /dev/null; then
        curl -fsSL -o "$archive_name" "$download_url" || { log_error "Download failed"; rm -rf "$tmp_dir"; return 1; }
    else
        wget -q -O "$archive_name" "$download_url" || { log_error "Download failed"; rm -rf "$tmp_dir"; return 1; }
    fi

    log_info "Extracting..."
    tar -xzf "$archive_name" || { log_error "Extraction failed"; rm -rf "$tmp_dir"; return 1; }

    # Determine install location
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

    resign_for_macos "$install_dir/bd"

    # PATH check
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
    if ! command -v cargo &> /dev/null; then
        log_warning "cargo not found"
        return 1
    fi

    log_info "Building with cargo (this may take a minute)..."
    if cargo install beads-rs; then
        log_success "bd installed via cargo"
        return 0
    fi

    return 1
}

verify_installation() {
    if command -v bd &> /dev/null; then
        echo ""
        log_success "bd is installed and ready!"
        echo ""
        bd --version 2>/dev/null || echo "bd (beads-rs)"
        echo ""
        echo "Get started:"
        echo "  cd your-git-repo"
        echo "  bd init"
        echo "  bd create 'My first task' --type=task"
        echo "  bd ready"
        echo ""
        return 0
    else
        log_warning "bd installed but not found in PATH"
        echo "You may need to restart your shell or add the install directory to PATH"
        return 1
    fi
}

main() {
    echo ""
    echo "beads-rs (bd) Installer"
    echo ""

    log_info "Detecting platform..."
    local platform
    if platform=$(detect_platform); then
        log_info "Platform: $platform (prebuilt binary available)"

        if install_from_release "$platform"; then
            verify_installation
            exit 0
        fi
        log_warning "Binary install failed, falling back to cargo..."
    else
        log_info "Platform: $(uname -s) $(uname -m)"
        log_info "No prebuilt binary available, using cargo..."
    fi

    if install_with_cargo; then
        verify_installation
        exit 0
    fi

    # All methods failed
    log_error "Installation failed"
    echo ""
    echo "Manual installation options:"
    echo ""
    echo "  1. Install Rust and use cargo:"
    echo "     curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    echo "     cargo install beads-rs"
    echo ""
    echo "  2. Use mise (if installed):"
    echo "     mise use -g ubi:delightful-ai/beads-rs[exe=bd]"
    echo ""
    echo "  3. Use nix (if installed):"
    echo "     nix run github:delightful-ai/beads-rs"
    echo ""
    echo "  4. Download binary manually:"
    echo "     https://github.com/delightful-ai/beads-rs/releases"
    echo ""
    exit 1
}

main "$@"
