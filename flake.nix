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

        # The beads-rs package
        beads-rs = pkgs.rustPlatform.buildRustPackage {
          pname = "beads-rs";
          version = "0.1.6";

          src = ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = [ pkgs.pkg-config ];

          buildInputs = [
            pkgs.openssl
            pkgs.zlib
            pkgs.libgit2
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Security
            pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
          ];

          # The binary is named 'bd'
          meta = with pkgs.lib; {
            description = "Distributed issue tracker for AI agent swarms";
            homepage = "https://github.com/delightful-ai/beads-rs";
            license = licenses.mit;
            maintainers = [ ];
            mainProgram = "bd";
          };
        };
      in
      {
        # Package output - `nix build` or depend on in other flakes
        packages = {
          default = beads-rs;
          beads-rs = beads-rs;
        };

        # App output - `nix run github:delightful-ai/beads-rs`
        apps.default = {
          type = "app";
          program = "${beads-rs}/bin/bd";
        };

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
    ) // {
      # Overlay for use in other flakes
      overlays.default = final: prev: {
        beads-rs = self.packages.${prev.system}.beads-rs;
        bd = self.packages.${prev.system}.beads-rs;
      };
    };
}
