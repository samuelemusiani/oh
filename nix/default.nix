{ pkgs, lib }:
let
  inherit (lib) range imap1 mergeAttrsList;
  inherit (builtins) length head toString;

  idxsToIp = i: j: "10.0.${toString i}.${toString j}";

  mkPimdConf = rpAddress: pkgs.writeText "pimd.conf" ''
    bsr-candidate disable
    rp-candidate disable
    rp-address ${rpAddress}
  '';
in rec {
  netemProfiles = {
    ideal      = { delayMs =   0; rateMbit = 100;  lossPercent =  0;  };
    long-fat   = { delayMs =  20; rateMbit = 100;  lossPercent =  0;  };
    fast-lossy = { delayMs =   0; rateMbit = 100;  lossPercent = 10;  };
    normal     = { delayMs =  20; rateMbit =  10;  lossPercent =  0;  };
    lossy      = { delayMs =  20; rateMbit =  10;  lossPercent = 10;  };
    lossy-slow = { delayMs = 100; rateMbit =   2;  lossPercent = 10;  };
  };

  mkRouter = {
    subnetIdxs,
    rpAddress    ? idxsToIp (head subnetIdxs) 1,
    extraIfaces  ? {},
  }:
  let
    idxToVeth = i: {
      "veth${toString i}".ipv4.addresses = [{
        address = idxsToIp i 1;
        prefixLength = 24;
      }];
    };
    veths = mergeAttrsList (map idxToVeth subnetIdxs);
  in {
    networking.interfaces = veths // extraIfaces;
    sysctl = {
      "net.ipv4.ip_forward" = true;
      "net.ipv6.conf.all.disable_ipv6" = 1;
    };
    postSetup = "pimd --config ${mkPimdConf rpAddress}";
    scripts.tcpdump.exec = "tcpdump -i any -w router.pcap";
  };

  mkLeafNet = subnetIdx: {
    networking.interfaces.veth0.ipv4 = {
      addresses = [{
        address = idxsToIp subnetIdx 2;
        prefixLength = 24;
      }];
      routes = [{
        address = "0.0.0.0";
        prefixLength = 0;
        via = idxsToIp subnetIdx 1;
      }];
    };
  };

  mkLeafVeth = {
    subnetIdx,
    node ? "node-${toString subnetIdx}",
    router ? "router",
    netem ? netemProfiles.normal,
  }: {
    a = { node = router; iface = "veth${toString subnetIdx}"; };
    b = { inherit node;  iface = "veth0"; };
    inherit netem;
  };

  mkStarTopology = nodes:
    let
      nodeList = lib.attrsToList nodes;
      idxs = range 1 (length nodeList);
      indexed = imap1 (idx: {name, value}: {
        inherit idx name;
        netem = value.netem or netemProfiles.normal;
        node = removeAttrs value ["netem"];
      }) nodeList;
    in {
      arp = false;
      arpPrefill = true;

      nodes = let
        mkNode = { idx, name, node, ... }: {
          ${name} = mkLeafNet idx // node;
        };
        router = mkRouter { subnetIdxs = idxs; };
      in { inherit router; } // mergeAttrsList (map mkNode indexed);

      veths = let
        mkVeth = { idx, name, netem, ... }: {
          "eth${toString idx}" = mkLeafVeth { subnetIdx = idx; inherit netem; node = name; };
        };
      in mergeAttrsList (map mkVeth indexed);
    };
}
