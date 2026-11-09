# The spread phase's additions to nix/test.nix's nodes
# (crates/tribuchet/tests/e2e/spread.rs): a second worker, on the hub VM,
# like the worker VM's with two cores and two agents, started by the
# test; and heavy builds, /etc/tt/heavy.nix: build `name`, which declares
# big-parallel and keeps both cores busy for 20 s.
{
  pkgs,
  lib,
  tribuchet,
  ...
}:
{
  hub = {
    virtualisation.cores = 2;
    nix.settings.system-features = [ "big-parallel" ];
    environment.etc."tt/heavy.nix".text = ''
      { name }:
      derivation {
        inherit name;
        system = "x86_64-linux";
        builder = builtins.storePath "${pkgs.bash}" + "/bin/bash";
        requiredSystemFeatures = [ "big-parallel" ];
        args = [ "-c" "for i in 1 2; do (while [ $SECONDS -lt 20 ]; do :; done) & done; wait; echo ok > $out" ];
      }
    '';
    services.tribuchet-worker = {
      enable = true;
      package = tribuchet;
      agents = 2;
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
    # the hub's "capacity declared" lines (debug), which the test waits
    # for from both workers
    systemd.services.tribuchet-hub.serviceConfig.Environment = lib.mkForce [
      "RUST_LOG=info,tribuchet::hub=debug"
      "XDG_CACHE_HOME=/var/cache"
    ];
  };
}
