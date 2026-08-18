{ pkgs }: let inherit (pkgs) lib; in rec {
  mcastAddr = "239.1.1.1";
  mcastPort = "5000";

  commonLoss = [
    { label = "0"; loss.percent = 0.0; }
    { label = "001"; loss.percent = 0.01; }
    { label = "005"; loss.percent = 0.05; }
  ];

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

  routerSysctl = {
    "net.ipv4.ip_forward" = true;
    "net.ipv4.conf.all.rp_filter" = 0;
    "net.ipv4.conf.default.rp_filter" = 0;
    # Reduce noise by disabling IPv6
    "net.ipv6.conf.all.disable_ipv6" = 1;
  };

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
}
