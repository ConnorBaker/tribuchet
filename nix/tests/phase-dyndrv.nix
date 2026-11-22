# The dyndrv phase's additions to nix/test.nix's nodes
# (crates/tribuchet/tests/e2e/dyndrv.rs): dynamic-derivation producers
# (builder-rpc-v0) built through Tribuchet.
#
# - The hub VM runs the farm's Nix (flake.nix `nix-dyndrv`, which carries
#   builder-rpc-v0 for external builders, the 222 decline and
#   canBuildLocally), a worker of its own serving the handoff socket, and
#   attach with it.
# - The worker VM's worker serves a handoff socket too, so both workers
#   advertise builder-rpc-v0: test-only, for VM step 8's placement. A
#   producer placed there reaches no attach.
# - /etc/tt/producer.nix: producer.nix with this Nix; /etc/tt/load.nix:
#   step 8's load, a build that sleeps; /run/tt-drvpath, if
#   present, is written into each build.json as drvPath (this Nix writes
#   none), the client "whose Nix carries drvPath" of step 8.
{
  pkgs,
  lib,
  tribuchet,
  nixDyndrv,
  ...
}:
let
  handoff = "/run/tribuchet-worker/handoff.sock";
  experimental = [
    "dynamic-derivations"
    "ca-derivations"
    "nix-command"
  ];
in
{
  hub =
    { config, ... }:
    {
      virtualisation.cores = 2;
      # the build record's drv_path table, for step 8
      environment.systemPackages = [ pkgs.sqlite ];
      # Formals, not `args:`: nix-build's --arg/--argstr reach only a
      # function with formals.
      environment.etc."tt/producer.nix".text = ''
        {
          name,
          seconds ? 0,
          other ? false,
          features ? [ ],
          hash ? null,
        }:
        import ${./producer.nix} {
          inherit name seconds other features hash;
          bash = "${pkgs.bash}";
          coreutils = "${pkgs.coreutils}";
          nix = "${nixDyndrv}";
        }
      '';
      # Step 8's load: a build that sleeps `seconds`.
      environment.etc."tt/load.nix".text = ''
        { name, seconds }:
        derivation {
          inherit name;
          system = "x86_64-linux";
          builder = builtins.storePath "${pkgs.bash}" + "/bin/bash";
          args = [ "-c" "''${builtins.storePath "${pkgs.coreutils}"}/bin/sleep ''${seconds}; echo > $out" ];
        }
      '';
      nix.settings.experimental-features = experimental;
      services.tribuchet-hub.externalBuilders = {
        nixPackage = nixDyndrv;
        patchNix = false;
      };
      # test.nix's attach (keeps each build.json), with the handoff socket
      # and, while /run/tt-drvpath exists, its drvPath.
      nix.settings.external-builders = lib.mkOverride 40 (
        builtins.toJSON [
          {
            systems = config.services.tribuchet-hub.externalBuilders.systems;
            program = pkgs.writeShellScript "tt-attach" ''
              set -e
              json="$1"
              if [ -e /run/tt-drvpath ]; then
                json=$(mktemp)
                ${lib.getExe pkgs.jq} --arg d "$(cat /run/tt-drvpath)" '. + {drvPath: $d}' "$1" > "$json"
              fi
              cp "$json" "$(mktemp /var/lib/tt-buildjson/XXXXXX)"
              exec ${lib.getExe' tribuchet "tribuchet"} attach "$json" \
                --socket ${config.services.tribuchet-hub.socketPath} --handoff-socket ${handoff}
            '';
            args = [ ];
          }
        ]
      );
      services.tribuchet-worker = {
        enable = true;
        package = tribuchet;
        agents = 2;
        handoffSocket = handoff;
        keyFile = "/etc/tribuchet/tls/worker.key";
        settings = {
          hub = "https://hub:7437";
          cert = "/etc/tribuchet/tls/worker.crt";
          ca-cert = "/etc/tribuchet/tls/ca.crt";
          max-jobs = 2;
        };
      };
      # started by the test once it has the worker VM's certificates
      systemd.services.tribuchet-worker.wantedBy = lib.mkForce [ ];
      systemd.services.tribuchet-hub.serviceConfig.Environment = lib.mkForce [
        "RUST_LOG=info,tribuchet::hub=debug"
        "XDG_CACHE_HOME=/var/cache"
        "XDG_STATE_HOME=/var/lib"
      ];
    };
  worker = {
    nix.settings.experimental-features = experimental;
    services.tribuchet-worker.handoffSocket = handoff;
  };
}
