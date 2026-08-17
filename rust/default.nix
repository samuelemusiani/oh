{ craneLib }: craneLib.buildPackage {
  name = "The rust implementation of OH";
  src = craneLib.cleanCargoSource ./.;
  strictDeps = true;
  cargoLock = ./Cargo.lock;
  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
}
