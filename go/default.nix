{ pkgs }: pkgs.buildGoModule {
  name = "The go implementation of OH";
  src = ./.;
  modRoot = ".";
  goSum = ./go.sum;
  subPackages = [ "cmd" ];
  vendorHash = "sha256-uORF2w10osTjCXrJ4WCVM3Tfof3Q1VWV5RPVR/ODO8c=";
}

