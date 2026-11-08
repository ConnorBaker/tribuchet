# The history phase's additions to nix/test.nix's nodes
# (crates/tribuchet/tests/e2e/history.rs): a second worker, on the hub
# VM, started by the test; three agents on two cores on either worker;
# a build record of its own on the hub; and on the hub, builds that declare no system feature, each running
# `secs` seconds: /etc/tt/hot.nix keeps both cores busy, /etc/tt/nap.nix
# sleeps on none.
{
  pkgs,
  lib,
  tribuchet,
  ...
}:
let
  bash = ''builtins.storePath "${pkgs.bash}" + "/bin/bash"'';
  sleep = ''builtins.storePath "${pkgs.coreutils}" + "/bin/sleep"'';
  worker = {
    agents = 3;
    settings.max-jobs = lib.mkForce 3;
  };
in
{
  worker.services.tribuchet-worker = worker;
  hub = {
    virtualisation.cores = 2;
    # a record of its own, so that the test sees the setting used
    services.tribuchet-hub.settings.placement.record = "/var/lib/tt-history/record.sqlite";
    environment.etc."tt/hot.nix".text = ''
      { name, secs }:
      derivation {
        inherit name;
        system = "x86_64-linux";
        builder = ${bash};
        args = [ "-c" "for i in 1 2; do (while [ $SECONDS -lt ''${toString secs} ]; do :; done) & done; wait; echo ok > $out" ];
      }
    '';
    environment.etc."tt/nap.nix".text = ''
      { name, secs }:
      derivation {
        inherit name;
        system = "x86_64-linux";
        builder = ${bash};
        args = [ "-c" "''${${sleep}} ''${toString secs}; echo ok > $out" ];
      }
    '';
    services.tribuchet-worker = lib.recursiveUpdate worker {
      enable = true;
      package = tribuchet;
      keyFile = "/etc/tribuchet/tls/worker.key";
      settings = {
        hub = "https://hub:7437";
        cert = "/etc/tribuchet/tls/worker.crt";
        ca-cert = "/etc/tribuchet/tls/ca.crt";
      };
    };
    # started by the test once it has the worker VM's certificates
    systemd.services.tribuchet-worker.wantedBy = lib.mkForce [ ];
    # the hub's "capacity declared" lines (debug), which the test waits
    # for from both workers
    systemd.services.tribuchet-hub.serviceConfig.Environment = lib.mkForce [
      "RUST_LOG=info,tribuchet::hub=debug"
      "XDG_CACHE_HOME=/var/cache"
      "XDG_STATE_HOME=/var/lib"
    ];
  };
}
