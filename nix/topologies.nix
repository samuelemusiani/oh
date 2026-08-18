# Multicast test topologies
#
# Every topology/parameter combination is exposed as one flake package, plus a
# "-tmux" variant for interactive use and a "-svg" variant that renders the
# topology to an SVG diagram (build it, don't run it):
#   nix flake show                    # list all topologies
#   nix run .#topo1-s1-d10-r5
#   nix run .#topo2-a-tmux
#   nix build .#topo3-svg
#
# Names carry the parameters that determine the topology's shape, so different
# settings yield distinct, self-describing derivations:
#   topo1-s<senderLoss%>-d<recvDelayMs>-r<recvLoss%>-n<nReceivers>
#   (the Gilbert-Elliott case p=0.01, q=0.1 is labeled rp1q10; topo1 draws no
#    random links, so its seed is not part of the name)
#   topo2-a-<seed>-n<nReceivers>: p = q ~ U(0, 0.1)
#   topo2-b-<seed>-n<nReceivers>: p ~ U(0, 0.1), q ~ U(0.05, 0.2)
#   topo3-<seed> (fixed 9-receiver structure, independent of nReceivers)
{ pkgs, nixnet, oh-go, oh-rs }:
let
  inherit (pkgs) lib;

  #testFile = pkgs.writeText "test.txt" "random text for testing purposes.";
  testFile = pkgs.runCommand "reproducible-10mb-file.bin" {
    nativeBuildInputs = [ pkgs.openssl ];
  } ''
    (openssl enc -aes-256-ctr -pass pass:"fixed-seed-phrase" -nosalt </dev/zero 2>/dev/null || true) | dd bs=4k iflag=fullblock count=5120 of=$out
  ''; # 4k x 2560

  # The seed determines all randomized link (topo 2 and 3)
  seed = "v1";
  # Number of receivers in topologies 1 and 2
  nReceivers = 5;

  # How the sender and the receivers are invoked, shared by all topologies.
  mcastAddr = "239.1.1.1";
  mcastPort = "5000";
  senderCmd = { iface }: "${oh-go}/bin/cmd -a ${mcastAddr} -t ${mcastPort} -i ${iface} -s ${testFile}";
  #~ senderCmd = { iface }: "${oh-rs}/bin/oh-rs -a ${mcastAddr} -t ${mcastPort} -i ${iface} -s ${testFile}";
  receiverCmd = { iface }: "${oh-go}/bin/cmd -a ${mcastAddr} -t ${mcastPort} -i ${iface} .";
  #~ receiverCmd = { iface }: "${oh-rs}/bin/oh-rs -a ${mcastAddr} -t ${mcastPort} -i ${iface} .";

  # --- seeded pseudo-randomness --------------------------------------------
  inherit (import ./seeded-random.nix { inherit lib; } seed) rand uniform uniformInt;

  # Loss probabilities are drawn uniformly but rounded to 3 decimal digits
  uniformLoss =
    label: lo: hi:
    builtins.floor (uniform label lo hi * 1000 + 0.5) / 1000.0;

  # Queue limit for a link in packets: the bandwidth-delay product (rounded up,
  # assuming 1500-byte MTU; mirrors nixnet's netem.autoLimit formula) plus a
  # random extra buffer Q ~ U(20, 100).
  bdpLimit = { rateMbit, delayMs, label }:
    let
      bdp = rateMbit * 1000000 / 8 * delayMs / 1000.0 / 1500;
      q = uniformInt "${label}:q" 20 100;
    in builtins.ceil bdp + q;

  # --- shared testbed configuration ----------------------------------------
  commonConfig = enableTmux: {
    arp = false;
    arpPrefill = true;
    nodePackages = with pkgs; [
      coreutils bash
      iproute2 iputils
      tcpdump smcroute
    ];
    testbedPackages = lib.mkOptionDefault (
      [ pkgs.smcroute ] ++ lib.optionals enableTmux [ pkgs.tmux ]
    );
    scripts = lib.optionalAttrs enableTmux {
      tmux-menu = {
        # Taken from https://github.com/birneee/nixnet/blob/main/examples/tmux/flake.nix
        foreground = true;
        exec = ''
          SOCKET=/tmp/nixnet.sock
          SESSION=nixnet
          MENU_CMD='display-menu -T " New " -x 0 -y S'
          _new_session=true
          i=0
          for ns in /pwd/*; do
            if [ ! -d "$ns" ]; then continue; fi
            ns="$(basename -- "$ns")"
            if  [ "$_new_session" = true ]; then
                tmux -S $SOCKET -f ${./tmux.conf} new-session -d -s $SESSION -n "$ns" -- jail enter "$ns" bash
                _new_session=false
            else
                tmux -S $SOCKET new-window -t $SESSION -n "$ns" -- jail enter "$ns" bash
            fi
            MENU_CMD="$MENU_CMD \"$ns\" \"$i\" \"new-window -n $ns -- jail enter $ns bash\""
            i=$((i+1))
          done
          MENU_CMD="$MENU_CMD \"\"" # divider in menu
          MENU_CMD="$MENU_CMD \"Exit Lab\" q \"confirm-before -p \\\"Exit session? (y/n)\\\" detach-client\""
          tmux -S $SOCKET bind-key m "$MENU_CMD"
          tmux -S $SOCKET bind-key -n MouseDown1StatusLeft "$MENU_CMD"
          tmux -S /tmp/nixnet.sock attach-session -t $SESSION
        '';
      };
    };
  };

  routerSysctl = {
    "net.ipv4.ip_forward" = true;
    "net.ipv4.conf.all.rp_filter" = 0;
    "net.ipv4.conf.default.rp_filter" = 0;
    # Reduce noise by disabling IPv6
    "net.ipv6.conf.all.disable_ipv6" = 1;
  };

  # Install static multicast forwarding
  smcrouteSetup = { ns, iif, oifs }:
    let
      allIfs = [ iif ] ++ oifs;
      mkRoute = inIf: ''
        echo 'mroute from ${inIf} group ${mcastAddr} to ${lib.concatStringsSep " " (lib.remove inIf allIfs)}' >> "$conf"
      '';
    in
    ''
      conf=/tmp/smcroute-${ns}.conf
      > "$conf"
      ${lib.concatMapStrings mkRoute allIfs}
      smcrouted -I smcroute-${ns} -f "$conf"
    '';

  # A leaf node (sender or receiver) with a single interface veth0
  mkLeaf = { addr, gw, scripts ? { } }: {
    networking.interfaces.veth0.ipv4 = {
      addresses = [ { address = addr; prefixLength = 24; } ];
      routes = [ { address = "0.0.0.0"; prefixLength = 0; via = gw; } ];
    };
    inherit scripts;
  };

  senderScripts =
    enableTmux:
    lib.optionalAttrs (!enableTmux) {
      sender = { exec = senderCmd { iface = "veth0"; }; };
    };
  receiverScripts.receiver = {
    exec = receiverCmd { iface = "veth0"; };
    await = true;
  };

  # --- star topology (topologies 1 and 2) -----------------------------------
  # Central router; the sender is on 10.0.1.0/24, receiver k on 10.0.(k+1).0/24.
  mkStarConfig = { name, enableTmux, senderLink, receiverLinks }:
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
          scripts = senderScripts enableTmux;
        };
      } // lib.listToAttrs (
        lib.imap1 (k: link: {
          name = "recv-${toString k}";
          value = mkLeaf {
            addr = "10.0.${toString (k + 1)}.2";
            gw = routerAddr (k + 1);
            scripts = receiverScripts;
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
      senderLosses = [
        { label = "s0"; loss.percent = 0.0; }
        { label = "s001"; loss.percent = 0.01; }
        { label = "s005"; loss.percent = 0.05; }
      ];
      delays = [ 1 10 100 ];
      recvLosses = [
        { label = "r0"; loss.percent = 0; }
        { label = "r001"; loss.percent = 0.01; }
        { label = "r005"; loss.percent = 0.05; }
        {
          label = "rp001q01";
          loss = {
            model = "gemodel";
            percent = 0.01;
            r = 1 - 0.1;
            h = 100;
            k = 0;
          };
        }
      ];
    in
    map
      (
        { s, d, r }: rec {
          name = "topo1-${s.label}-d${toString d}-${r.label}-n${toString nReceivers}";
          mkCfg =
            enableTmux:
            mkStarConfig {
              inherit name enableTmux;
              senderLink = {
                delayMs = 10;
                rateMbit = 10;
                limit = 20;
                inherit (s) loss;
              };
              receiverLinks = lib.replicate nReceivers {
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
        }
      );

  # --- topology 2: heterogeneous star ----------------------------------------
  topo2Variants =
    let
      mkVariant = label: lossFor: rec {
        name = "topo2-${label}-${seed}-n${toString nReceivers}";
        mkCfg =
          enableTmux:
          mkStarConfig {
            inherit name enableTmux;
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
                inherit (lossFor k) loss;
                limit = bdpLimit {
                  inherit rateMbit delayMs;
                  label = "topo2:recv${toString k}";
                };
              }
            ) (lib.range 1 nReceivers);
          };
      };
    in
    [
      # Loss variant a: p = q ~ U(0, 0.1)
      (mkVariant "a" (k: {
        loss.percent = uniformLoss "topo2:recv${toString k}:p" 0.0 0.1;
      }))
      # Loss variant b: p ~ U(0, 0.1), q ~ U(0.05, 0.2)
      (mkVariant "b" (k: {
        loss = {
          model = "gemodel";
          percent = uniformLoss "topo2:recv${toString k}:p" 0.0 0.1;
          r = 1 - uniformLoss "topo2:recv${toString k}:q" 0.05 0.2;
          h = 100;
          k = 0;
        };
      }))
    ];

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
            inherit rateMbit delayMs;
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
              scripts = senderScripts enableTmux;
            };
          } // lib.listToAttrs (
            map (k: {
              name = "recv-${toString k}";
              value = mkLeaf {
                addr = "${recvSubnet k}.2";
                gw = "${recvSubnet k}.1";
                scripts = receiverScripts;
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

  allVariants = topo1Variants ++ topo2Variants ++ [ topo3Variant ];
in lib.foldl' (
  acc: v:
  acc // {
    ${v.name} = nixnet.mkExperiment (v.mkCfg false);
    "${v.name}-tmux" = nixnet.mkExperiment (v.mkCfg true);
    "${v.name}-svg" = nixnet.mkMermaidSvg (v.mkCfg false);
  }
) { } allVariants
