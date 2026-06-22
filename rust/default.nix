{ craneLib }:
let commonArgs = {
  src = craneLib.cleanCargoSource ./.;
  strictDeps = true;
  cargoLock = ./Cargo.lock;
}; in craneLib.buildPackage (
  commonArgs // {
    cargoArtifacts = craneLib.buildDepsOnly commonArgs;
  }
)

