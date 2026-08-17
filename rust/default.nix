{ craneLib }: 
let
  commonArgs = {
    src = craneLib.cleanCargoSource ./.;
    strictDeps = true;
    cargoLock = ./Cargo.lock;
  };
in
craneLib.buildPackage (commonArgs // {
  name = "The rust implementation of OH";
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
})
