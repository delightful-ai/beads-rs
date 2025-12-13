{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    rustup
    openssl
    pkg-config
    zlib
    libgit2

    # Dev tools
    just
    cargo-watch
  ];

  OPENSSL_DIR = "${pkgs.openssl.dev}";
  OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
  OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";

  shellHook = ''
    echo "beads-rs dev shell (shell.nix)"
    echo ""
    echo "Note: Using rustup. Run 'rustup default stable' if needed."
    echo ""
    echo "Available commands:"
    echo "  just         - Run common tasks (just --list for all)"
    echo "  just check   - Run fmt, lint, test"
    echo ""

    # Set up git hooks
    if [ -d .git ]; then
      git config core.hooksPath .githooks
    fi
  '';
}
