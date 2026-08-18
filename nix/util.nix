{pkgs, nixnet, oh-go, oh-rs }:
let
  inherit (pkgs) lib;
in {
  # A leaf node (sender or receiver) with a single interface veth0
  mkLeaf = { addr, gw, scripts ? { } }: {
    networking.interfaces.veth0.ipv4 = {
      addresses = [ { address = addr; prefixLength = 24; } ];
      routes = [ { address = "0.0.0.0"; prefixLength = 0; via = gw; } ];
    };
    inherit scripts;
  };

  mkAll = lib.foldl' (
    acc: v:
    acc // {
      ${v.name} = nixnet.mkExperiment (v.mkCfg false);
      "${v.name}-tmux" = nixnet.mkExperiment (v.mkCfg true);
      "${v.name}-svg" = nixnet.mkMermaidSvg (v.mkCfg false);
    }
  ) {};

  # Queue limit for a link in packets: the bandwidth-delay product (rounded up,
  # assuming 1500-byte MTU; mirrors nixnet's netem.autoLimit formula) plus a
  # random extra buffer Q ~ U(20, 100).
  bdpLimit = { uniformInt, rateMbit, delayMs, label }:
    let
      bdp = rateMbit * 1000000 / 8 * delayMs / 1000.0 / 1500;
      q = uniformInt "${label}:q" 20 100;
    in builtins.ceil bdp + q;
}
