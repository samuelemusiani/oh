{ pkgs }: pkgs.buildGoModule {
  name = "The go implementation of OH";
  src = ./.;
  modRoot = ".";
  goSum = ./go.sum;
  subPackages = [ "cmd" ];
  vendorHash = "sha256-QCQiqddt/2bQ8Hgmm68udXfnnFMek36a0CMhwxw03eU=";
}
