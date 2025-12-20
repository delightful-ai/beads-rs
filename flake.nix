{
  description = "beads-rs - distributed issue tracker for AI agent swarms";

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
        version = "0.1.21";
      in
      {

        # Dev shell - `nix develop`
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
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Security
            pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
          ];

          OPENSSL_DIR = "${pkgs.openssl.dev}";
          OPENSSL_LIB_DIR = "${pkgs.openssl.out}/lib";
          OPENSSL_INCLUDE_DIR = "${pkgs.openssl.dev}/include";
          BEADS_RS_VERSION = version;

          shellHook = ''
            echo "beads-rs dev shell"
            echo "Rust: $(rustc --version)"
            echo ""
            echo "Commands: just --list"
            echo ""

            # Set up git hooks
            if [ -d .git ]; then
              git config core.hooksPath .githooks
            fi
          '';
        };
      }
    );
}
