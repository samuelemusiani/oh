{
  outputs = { crane, flake-parts, nixnet, ... }@inputs: flake-parts.lib.mkFlake { inherit inputs; } {
    systems = nixnet.supportedSystems;
    perSystem = { pkgs, inputs', lib, ... }: let
      nixnet = inputs'.nixnet.legacyPackages;
      oh-go = pkgs.callPackage ./go { };
      oh-rs = pkgs.callPackage ./rust { craneLib = (crane.mkLib pkgs); };
    in {
      packages =
        import ./nix/topologies.nix { inherit pkgs nixnet oh-go oh-rs; }
        // { inherit oh-go oh-rs; };
      devShells.default = pkgs.mkShell {
        strictDeps = true;
        nativeBuildInputs = [ pkgs.typst ];
        TYPST_FONT_PATHS = lib.concatStringsSep ":" [ pkgs.newcomputermodern ];
      };
    };
  };
  inputs = {
    crane.url = "github:ipetkov/crane";
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixnet.inputs.nixpkgs.follows = "nixpkgs";
    nixnet.url = "github:hendrikcech/nixnet?ref=protocol-design";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    pimd.inputs.nixpkgs.follows = "nixpkgs";
    pimd.url = "github:hendrikcech/pimd";
  };
}
