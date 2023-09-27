{
  description = "Tox21 biobrick";

  inputs = {
    nixpkgs.url = "https://flakehub.com/f/NixOS/nixpkgs/*.tar.gz";
    flake-utils.url = "https://flakehub.com/f/numtide/flake-utils/*.tar.gz";
    biobricks-R = {
      url = "github:biobricks-ai/biobricks-R";
      inputs.flake-utils.follows = "flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    hdt-cpp = {
      url = "github:insilica/nix-hdt";
      inputs.flake-utils.follows = "flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, biobricks-R, hdt-cpp }:
    flake-utils.lib.eachDefaultSystem (system:
      with import nixpkgs { inherit system; };
      let
        # tmducken requires duckdb 0.8.1 or later, and nixos-23.05 only had 0.7
        duckdb-version = "0.8.1";
        duckdb = (pkgs.duckdb.overrideAttrs (oldAttrs: rec {
          version = duckdb-version;
          src = fetchFromGitHub {
            owner = "duckdb";
            repo = "duckdb";
            rev = "v${duckdb-version}";
            sha256 = "sha256-LEv9yURkYvONObTbIA4CS+umwCRMH8gRQaDtzbCzID4=";
          };
        }));
      in {
        devShells.default = mkShell {
          buildInputs = [
            biobricks-R.packages.${system}.rEnv
            clojure
            hdt-cpp.packages.${system}.default
            duckdb
            jdk
          ];
          env = { DUCKDB_HOME = "${duckdb}/lib"; };
        };
      });
}
