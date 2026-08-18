{ pkgs, nixnet, oh-go, oh-rs }@args:
let
  inherit (pkgs) lib;
  inherit (import ./config.nix { inherit pkgs; }) commonConfig routerSysctl mcastAddr mcastPort smcrouteSetup commonLoss;
  inherit (import ./util.nix args)  mkAll bdpLimit mkLeaf;

  seed = "v1";
  inherit (import ./seeded-random.nix { inherit lib pkgs; } seed) rand uniform uniformInt testFiles uniformLoss;

  goCmd = "${oh-go}/bin/cmd";
  goCmdNullFEC =
    let
      oh-go-patched = oh-go.overrideAttrs (oldAttrs: {
        patches = (oldAttrs.patches or []) ++ [ ./null-fec.patch ];
      });
    in  "${oh-go-patched}/bin/cmd";
  rsCmd = "${oh-rs}/bin/oh-rs";
  logSuff = ""; #"1> out.log 2> err.log";

  senderScripts = cmd: fileSizes: enableTmux: lib.optionalAttrs (!enableTmux) {
    sender.exec = "${cmd} -a ${mcastAddr} -t ${mcastPort} -i veth0 -s ${testFiles fileSizes} ${logSuff}";
  };
  receiverScripts = cmd: {
    receiver = {
      exec = "${cmd} -a ${mcastAddr} -t ${mcastPort} -i veth0 . ${logSuff}";
      await = true;
    };
  };

  # --- star topology (topologies 1 and 2) -----------------------------------
  # Central router; the sender is on 10.0.1.0/24, receiver k on 10.0.(k+1).0/24.
  mkStarConfig = { name, enableTmux, senderLink, receiverLinks, sender, receiver }:
    let
      n = lib.length receiverLinks;
      routerAddr = i: "10.0.${toString i}.1";
      mkVeth = i: ns: link: {
        a = { node = "router"; iface = "veth${toString i}"; };
        b = { node = ns; iface = "veth0"; };
        netem = link;
      };
    in
    commonConfig enableTmux // {
      inherit name;
      nodes = {
        router = {
          networking.interfaces = lib.listToAttrs (
            map (i: {
              name = "veth${toString i}";
              value.ipv4.addresses = [ { address = routerAddr i; prefixLength = 24; } ];
            }) (lib.range 1 (n + 1))
          );
          sysctl = routerSysctl;
          # veth1 faces the sender; veth2..veth(n+1) each face a receiver.
          postSetup = smcrouteSetup {
            ns = "router";
            iif = "veth1";
            oifs = map (i: "veth${toString i}") (lib.range 2 (n + 1));
          };
          #scripts.tcpdump.exec = "tcpdump -i any -w router.pcap";
        };
        sender = mkLeaf {
          addr = "10.0.1.2";
          gw = routerAddr 1;
          scripts = sender enableTmux;
        };
      } // lib.listToAttrs (
        lib.imap1 (k: link: {
          name = "recv-${toString k}";
          value = mkLeaf {
            addr = "10.0.${toString (k + 1)}.2";
            gw = routerAddr (k + 1);
            scripts = receiver;
          };
        }) receiverLinks
      );
      veths = { sender = mkVeth 1 "sender" senderLink; } // lib.listToAttrs (
        lib.imap1 (k: link: {
          name = "recv-${toString k}";
          value = mkVeth (k + 1) "recv-${toString k}" link;
        }) receiverLinks
      );
    };

  # --- topology 1: homogeneous star -----------------------------------------
  topo1Variants =
    let
      senderLosses = commonLoss;
      delays = [ 1 10 100 ];
      recvLosses = commonLoss ++ [{
        label = "p001q01";
        loss = {
          model = "gemodel";
          percent = 0.01;
          r = 1 - 0.1;
          h = 100;
          k = 0;
        };
      }];
      nReceivers = [ 1 2 5 10 ];
      receivers = [
        { name = "go"; cmd = receiverScripts goCmd; }
        { name = "goN"; cmd = receiverScripts goCmdNullFEC; }
        { name = "rs"; cmd = receiverScripts rsCmd; }
      ];
      senders = [
        { name = "go"; cmd = senderScripts goCmd; }
        { name = "goN"; cmd = senderScripts goCmdNullFEC; }
        { name = "rs"; cmd = senderScripts rsCmd; }
      ];
      fileSizes = [
        [ 1 ]
        [ 1 1 ]
        [ 256 ]
        [ 256 256 ]
        [ 256 1 ]
        [ 2560 ]
      ];
    in
    map
      (
        { s, d, r, n, senders, receivers, fileSizes }: rec {
          name = let
            fss = builtins.concatStringsSep "_" (map builtins.toString fileSizes);
          in "topo1-s${s.label}-d${toString d}-r${r.label}-n${toString n}-s${senders.name}-r${receivers.name}-f_${fss}";
          mkCfg =
            enableTmux:
            mkStarConfig {
              inherit name enableTmux;
              sender = senders.cmd fileSizes;
              receiver = receivers.cmd;
              senderLink = {
                delayMs = 10;
                rateMbit = 10;
                limit = 20;
                inherit (s) loss;
              };
              receiverLinks = lib.replicate n {
                delayMs = d;
                rateMbit = 10;
                limit = 20;
                inherit (r) loss;
              };
            };
        }
      )
      (
        lib.cartesianProduct {
          s = senderLosses;
          d = delays;
          r = recvLosses;
          n = nReceivers;
          inherit senders receivers fileSizes;
        }
      );

  # --- topology 2: heterogeneous star ----------------------------------------
  topo2Variants =
    map
      (
        { senders, receivers, n, lossVariant, fileSizes }: rec {
          name = let
            fss = builtins.concatStringsSep "_" (map builtins.toString fileSizes);
          in "topo2-${lossVariant.label}-${seed}-n${toString n}-s${senders.name}-r${receivers.name}-f_${fss}";
          mkCfg = enableTmux: mkStarConfig {
            inherit name enableTmux;
            sender = senders.cmd fileSizes;
            receiver = receivers.cmd;
            senderLink = {
              delayMs = 10;
              rateMbit = 20;
              limit = 20;
              loss.percent = uniformLoss "topo2:sender:pq" 0.0 0.05;
            };
            receiverLinks = map (
              k:
              let
                rateMbit = uniformInt "topo2:recv${toString k}:rate" 5 20;
                delayMs = uniformInt "topo2:recv${toString k}:delay" 1 100;
              in
              {
                inherit rateMbit delayMs;
                inherit (lossVariant.for k) loss;
                limit = bdpLimit {
                  inherit uniformInt rateMbit delayMs;
                  label = "topo2:recv${toString k}";
                };
              }
            ) (lib.range 1 n);
          };
        }
      )
      (
        lib.cartesianProduct {
          lossVariant = [
            { label = "a"; for = k: { loss.percent = uniformLoss "topo2:recv${toString k}:p" 0.0 0.1; }; }
            { label = "b"; for = k: {
               loss = {
                 model = "gemodel";
                 percent = uniformLoss "topo2:recv${toString k}:p" 0.0 0.1;
                 r = 1 - uniformLoss "topo2:recv${toString k}:q" 0.05 0.2;
                 h = 100;
                 k = 0;
               };
             };
            }
          ];
          n = [ 1 2 5 10 ];
          receivers = [
            { name = "go"; cmd = receiverScripts goCmd; }
            { name = "goN"; cmd = receiverScripts goCmdNullFEC; }
            { name = "rs"; cmd = receiverScripts rsCmd; }
          ];
          senders = [
            { name = "go"; cmd = senderScripts goCmd; }
            { name = "goN"; cmd = senderScripts goCmdNullFEC; }
            { name = "rs"; cmd = senderScripts rsCmd; }
          ];
          fileSizes = [
            [ 1 ]
            [ 1 1 ]
            [ 256 ]
            [ 256 256 ]
            [ 256 1 ]
            [ 2560 ]
          ];
        }
      );

  # --- topology 3: multihop, heterogeneous ----------------------------------
  # sender -> router0 -> { router1, recv-4, recv-5, router2 }
  # router1 -> { recv-1, recv-2, recv-3 }
  # router2 -> { recv-6, recv-7, recv-8, recv-9 }
  # Subnets: 10.0.x.0/24 for links at router0, 10.1.k.0/24 for router1 leaves,
  # 10.2.k.0/24 for router2 leaves.
  topo3Variant =
    let
      recvLink = k: let
          rateMbit = uniformInt "topo3:recv${toString k}:rate" 5 20;
          delayMs = uniformInt "topo3:recv${toString k}:delay" 5 50;
        in {
          inherit rateMbit delayMs;
          limit = bdpLimit {
            inherit uniformInt rateMbit delayMs;
            label = "topo3:recv${toString k}";
          };
          loss = {
            model = "gemodel";
            percent = uniformLoss "topo3:recv${toString k}:p" 0.001 0.01;
            r = 1 - uniformLoss "topo3:recv${toString k}:q" 0.01 0.05;
            h = 100;
            k = 0;
          };
        };
      recvKeys = lib.range 1 9;
      recvRouter = k:
        if k <= 3 then
          "router1"
        else if k <= 5 then
          "router0"
        else
          "router2";
      recvSubnet = k:
        if k <= 3 then
          "10.1.${toString k}"
        else if k <= 5 then
          "10.0.${toString k}"
        else
          "10.2.${toString k}";
      # The interfaces of `router` that face its receivers.
      routerLeafIfaces = router:
        lib.listToAttrs (
          map (k: {
            name = "veth${toString k}";
            value.ipv4.addresses = [
              {
                address = "${recvSubnet k}.1";
                prefixLength = 24;
              }
            ];
          }) (lib.filter (k: recvRouter k == router) recvKeys)
        );
    in {
      name = "topo3-${seed}";
      mkCfg = enableTmux:
        commonConfig enableTmux // {
          name = "topo3-${seed}";
          nodes = {
            router0 = {
              networking.interfaces = {
                veth1.ipv4.addresses = [ { address = "10.0.1.1"; prefixLength = 24; } ];
                veth2.ipv4 = {
                  addresses = [ { address = "10.0.2.1"; prefixLength = 24; } ];
                  routes = [ { address = "10.1.0.0"; prefixLength = 16; via = "10.0.2.2"; } ];
                };
                veth3.ipv4 = {
                  addresses = [ { address = "10.0.3.1"; prefixLength = 24; } ];
                  routes = [ { address = "10.2.0.0"; prefixLength = 16; via = "10.0.3.2"; } ];
                };
              } // routerLeafIfaces "router0";
              sysctl = routerSysctl;
              # veth1 faces the sender; forward out to router1 (veth2),
              # router2 (veth3), and router0's own receivers (veth4, veth5).
              postSetup = smcrouteSetup {
                ns = "router0";
                iif = "veth1";
                oifs = [ "veth2" "veth3" ]
                ++ lib.attrNames (routerLeafIfaces "router0");
              };
              #scripts.tcpdump.exec = "tcpdump -i any -w router0.pcap";
            };
            router1 = {
              networking.interfaces = {
                veth0.ipv4 = {
                  addresses = [ { address = "10.0.2.2"; prefixLength = 24; } ];
                  routes = [ { address = "0.0.0.0"; prefixLength = 0; via = "10.0.2.1"; } ];
                };
              } // routerLeafIfaces "router1";
              sysctl = routerSysctl;
              # veth0 faces router0 (toward the sender); forward out to this
              # router's receivers (recv-1..recv-3).
              postSetup = smcrouteSetup {
                ns = "router1";
                iif = "veth0";
                oifs = lib.attrNames (routerLeafIfaces "router1");
              };
              #scripts.tcpdump.exec = "tcpdump -i any -w router1.pcap";
            };
            router2 = {
              networking.interfaces = {
                veth0.ipv4 = {
                  addresses = [ { address = "10.0.3.2"; prefixLength = 24; } ];
                  routes = [ { address = "0.0.0.0"; prefixLength = 0; via = "10.0.3.1"; } ];
                };
              } // routerLeafIfaces "router2";
              sysctl = routerSysctl;
              # veth0 faces router0 (toward the sender); forward out to this
              # router's receivers (recv-6..recv-9).
              postSetup = smcrouteSetup {
                ns = "router2";
                iif = "veth0";
                oifs = lib.attrNames (routerLeafIfaces "router2");
              };
              #scripts.tcpdump.exec = "tcpdump -i any -w router2.pcap";
            };
            sender = mkLeaf {
              addr = "10.0.1.2";
              gw = "10.0.1.1";
              scripts = senderScripts goCmd [1 64] enableTmux;
            };
          } // lib.listToAttrs (
            map (k: {
              name = "recv-${toString k}";
              value = mkLeaf {
                addr = "${recvSubnet k}.2";
                gw = "${recvSubnet k}.1";
                scripts = receiverScripts goCmd;
              };
            }) recvKeys
          );
          veths = {
            sender = {
              a = { node = "router0"; iface = "veth1"; };
              b = { node = "sender"; iface = "veth0"; };
              netem = {
                delayMs = 10;
                rateMbit = 30;
                limit = 20;
                loss.percent = uniformLoss "topo3:sender:pq" 0.0 0.05;
              };
            };
            router1 = {
              a = { node = "router0"; iface = "veth2"; };
              b = { node = "router1"; iface = "veth0"; };
              netem = {
                delayMs = 40;
                rateMbit = 15;
                limit = 20;
                loss = {
                  model = "gemodel";
                  percent = 0.01;
                  r = 1 - 0.03;
                  h = 100;
                  k = 0;
                };
              };
            };
            router2 = {
              a = { node = "router0"; iface = "veth3"; };
              b = { node = "router2"; iface = "veth0"; };
              netem = {
                delayMs = 20;
                rateMbit = 20;
                limit = 20;
                loss = {
                  model = "gemodel";
                  percent = 0.02;
                  r = 1 - 0.05;
                  h = 100;
                  k = 0;
                };
              };
            };
          } // lib.listToAttrs (
            map (k: {
              name = "recv-${toString k}";
              value = {
                a = { node = recvRouter k; iface = "veth${toString k}"; };
                b = { node = "recv-${toString k}"; iface = "veth0"; };
                netem = recvLink k;
              };
            }) recvKeys
          );
        };
    };
in mkAll (topo1Variants ++ topo2Variants ++ [ topo3Variant ])
