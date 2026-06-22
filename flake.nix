{
  inputs = {
    crane.url = "github:ipetkov/crane";
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixnet.url = "github:birneee/nixnet";
    nixnet.inputs.nixpkgs.follows = "nixpkgs";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    pimd.url = "github:hendrikcech/pimd";
    pimd.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs = inputs@{ flake-parts, ... }: flake-parts.lib.mkFlake { inherit inputs; } {
    systems = inputs.nixnet.supportedSystems;
    perSystem = { pkgs, inputs', lib, ... }: let
      util = import ./nix { inherit pkgs lib; pimd-pkg = inputs'.pimd.packages.default; };
      inherit (util) mkStarTopology;

      go = pkgs.callPackage ./go { };
      rust = pkgs.callPackage ./rust { craneLib = (inputs.crane.mkLib pkgs); };

      testFile = pkgs.writeText "test.txt" "random text for testing purposes.";
      goTest = mkStarTopology {} {
        sender.scripts.send = {
          exec = "${lib.getBin go}/bin/cmd -a 239.1.1.1 -t 5000 -i veth0 -s ${testFile}";
          await = true;
        };
        receiver.scripts.recv.exec = "${lib.getBin go}/bin/cmd -a 239.1.1.1 -t 5000 -i veth0 .";
      };
      rustTest = mkStarTopology {} {
        sender.scripts.send = {
          exec = "${lib.getBin rust}/bin/cmd -a 239.1.1.1 -t 5000 -i veth0 -s ${testFile}";
          await = true;
        };
        receiver.scripts.recv.exec = "${lib.getBin rust}/bin/cmd -a 239.1.1.1 -t 5000 -i veth0 .";
      };

      inherit (inputs'.nixnet.legacyPackages) mkExperiment;
    in {
      packages = {
        inherit go rust;
        goTest = mkExperiment goTest;
        rustTest = mkExperiment rustTest;
      };
    };
  };
}
