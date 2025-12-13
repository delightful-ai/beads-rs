{
  description = "beads-rs - Rust implementation of beads issue tracker";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            rustToolchain
            pkgs.pkg-config
            pkgs.openssl
            pkgs.zlib
            pkgs.libgit2

            # Dev tools
            pkgs.just
            pkgs.cargo-watch
          ];

          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";

          shellHook = ''
            echo "beads-rs dev shell"
            echo "Rust: $(rustc --version)"
            echo ""
            echo "Available commands:"
            echo "  just         - Run common tasks (just --list for all)"
            echo "  just check   - Run fmt, lint, test"
            echo "  just watch   - Watch for changes and run tests"
            echo ""

            # Set up git hooks
            if [ -d .git ]; then
              git config core.hooksPath .githooks
              echo "Git hooks configured (.githooks/pre-commit)"
            fi
          '';
        };
      }
    );
}
