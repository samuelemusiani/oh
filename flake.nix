{
  inputs = {
    crane.url = "github:ipetkov/crane";
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixnet.url = "github:hendrikcech/nixnet?ref=protocol-design";
    nixnet.inputs.nixpkgs.follows = "nixpkgs";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    pimd.url = "github:hendrikcech/pimd";
    pimd.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs = inputs@{ flake-parts, ... }: flake-parts.lib.mkFlake { inherit inputs; } {
    systems = inputs.nixnet.supportedSystems;
    perSystem = { pkgs, inputs', lib, ... }: let
      nixnet = inputs'.nixnet.legacyPackages;
      oh-go = pkgs.callPackage ./go { };
      #oh-rs = pkgs.callPackage ./rust { craneLib = (inputs.crane.mkLib pkgs); };
      #rust-sender-cmd = files: "oh-rs ${args} send ${files}";
      #rust-receiver-cmd = dir: "oh-rs ${args} recieve ${dir}";
    in {
      packages =
        import ./nix/topologies.nix { inherit pkgs nixnet oh-go; }
        // { inherit oh-go oh-rs; };
    };
  };
}
