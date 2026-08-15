{ pkgs, lib }: pkgs.buildGoModule {
  name = "The go implementation of OH";
  src = lib.fileset.toSource {
    root = ../.;
    fileset = ../go;
  };
  modRoot = "./go";
  goSum = ./go.sum;
  subPackages = [ "cmd" ];
  vendorHash = "sha256-QCQiqddt/2bQ8Hgmm68udXfnnFMek36a0CMhwxw03eU=";
}
