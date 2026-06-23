{
  inputs = {
    crane.url = "github:ipetkov/crane";
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixnet.url = "github:birneee/nixnet";
    nixnet.inputs.nixpkgs.follows = "nixpkgs";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";
    pimd.url = "github:hendrikcech/pimd";
    pimd.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs = inputs@{ flake-parts, ... }: flake-parts.lib.mkFlake { inherit inputs; } {
    systems = inputs.nixnet.supportedSystems;
    perSystem = { pkgs, inputs', lib, ... }: let
      util = import ./nix { inherit pkgs lib; };
      inherit (util) mkStarTopology;

      oh-go = pkgs.callPackage ./go { };
      oh-rust = pkgs.callPackage ./rust { craneLib = (inputs.crane.mkLib pkgs); };

      testFile = pkgs.writeText "test.txt" "random text for testing purposes.";
      goTest = mkStarTopology {
        sender.scripts.send = {
          exec = "cmd -a 239.1.1.1 -t 5000 -i veth0 -s ${testFile}";
          await = true;
        };
        receiver.scripts.recv.exec = "cmd -a 239.1.1.1 -t 5000 -i veth0 .";
      } // {
        nodePackages = with pkgs; [
          coreutils bash iproute2 iputils tcpdump
          inputs'.pimd.packages.default
          oh-go
        ];
        testbedPackages = with pkgs; lib.mkOptionDefault [ inputs'.pimd.packages.default tmux ];
        scripts.tmux = {
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
      rustTest = mkStarTopology {
        sender.scripts.send = {
          exec = "cmd -a 239.1.1.1 -t 5000 -i veth0 -s ${testFile}";
          await = true;
        };
        receiver.scripts.recv.exec = "cmd -a 239.1.1.1 -t 5000 -i veth0 .";
      } // {
        nodePackages = with pkgs; [
          coreutils bash iproute2 iputils tcpdump
          inputs'.pimd.packages.default
          oh-go
        ];
      };

      inherit (inputs'.nixnet.legacyPackages) mkExperiment;
    in {
      packages = {
        inherit oh-go oh-rust;
        goTest = mkExperiment goTest;
        rustTest = mkExperiment rustTest;
      };
    };
  };
}
