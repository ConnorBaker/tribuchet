# The arbiter phase's additions to nix/test.nix's nodes
# (crates/tribuchet/tests/e2e/arbiter.rs): a 4-core worker with a slot
# and an agent for each of 5 builds, its cgroup monitor, and the builds
# (arbiter.nix: SPINNERS busy loops for SECONDS of wall clock, then one
# until UNTIL), each /etc/tt/arb-NAME.nix.
{ pkgs, lib, ... }:
let
  # NAME = "SPINNERS SECONDS UNTIL SLEEPERS"
  builds =
    lib.mapAttrs
      (
        _: a:
        lib.listToAttrs (
          lib.zipListsWith lib.nameValuePair [ "spinners" "seconds" "until" "sleepers" ] (
            map lib.toInt (lib.splitString " " a)
          )
        )
      )
      {
        # leader, follower, serial, cancelled
        L = "4 30 40 0";
        F = "4 50 50 0";
        S = "1 50 50 0";
        C = "4 50 50 0";
        # idle builds for C's slot: one takes the last slot, one waits for it
        Q1 = "0 30 30 1";
        Q2 = "0 10 10 1";
        # a worker killed while the follower is frozen, its agent running
        # or stopped
        L2 = "4 40 40 0";
        F2 = "4 40 40 0";
        L3 = "4 40 40 0";
        F3 = "4 40 40 0";
        # who may write a build's cgroup.freeze
        P1 = "1 20 20 0";
        P2 = "1 20 20 0";
        # the bound
        L4 = "4 42 60 0";
        F4 = "4 60 60 0";
        # a client's timeout and max-silent-time
        T = "1 30 30 0";
        U = "1 30 30 0";
      };
in
{
  hub = {
    environment.etc = lib.mapAttrs' (
      name: args:
      lib.nameValuePair "tt/arb-${name}.nix" {
        text = ''
          import ${./arbiter.nix} (${
            lib.generators.toPretty { } (
              args
              // {
                inherit name;
                bash = "${pkgs.bash}";
                coreutils = "${pkgs.coreutils}";
              }
            )
          })
        '';
      }
    ) builds;
    # the hub's default: a worker restart that waits on a stopped agent
    # outlasts the 2 s others use
    services.tribuchet-hub.settings.worker-grace-secs = lib.mkForce 30;
  };
  worker = {
    virtualisation.cores = lib.mkForce 4;
    services.tribuchet-worker = {
      agents = 5;
      settings.max-jobs = lib.mkForce 5;
    };
    environment.etc."tt/arb-monitor.py".source = ./arb-monitor.py;
  };
}
