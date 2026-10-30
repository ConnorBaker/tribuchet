# The pairs phase's additions to nix/test.nix's nodes
# (crates/tribuchet/tests/e2e/pairs.rs): a 4-core worker whose agents'
# slice is capped and swaps to zram at four times its RAM (the slice
# through zswap in front of it), and the builds' workloads by name:
# nix-build /etc/tt/pair.nix --argstr name NAME --argstr work WORK.
{ pkgs, lib, ... }:
let
  python = lib.getExe pkgs.python3;
  # 4 threads spinning until $1 s of wall clock, printing a line every 2 s.
  spinners = pkgs.writeShellScript "pair-spinners" ''
    for i in 1 2 3 4; do (while [ $SECONDS -lt $1 ]; do :; done) & done
    while [ $SECONDS -lt $1 ]; do echo "at $SECONDS s"; sleep 2; done
    wait
  '';
  work = {
    # deadline: 18 s of wall clock; 15 s of running time
    K = "${spinners} 18";
    D = "${python} ${./pair-spin.py} 15";
    # A: 1400 MiB more at 10 CPU-s; 4 x 300 MiB for the first 6 CPU-s
    AL = "${python} ${./pair-hog.py} '4:64:20:1400@10'";
    AF = "${python} ${./pair-hog.py} '4:300:6;1:64:10'";
    # cache: 40 s of wall clock; 4 x 128 MiB
    CL = "${spinners} 40";
    CF = "${python} ${./pair-hog.py} '4:128:10'";
  };
  deps = [
    pkgs.bash
    pkgs.python3
    spinners
    ./pair-spin.py
    ./pair-hog.py
  ];
in
{
  hub = {
    virtualisation.additionalPaths = deps;
    environment.etc."tt/pair.nix".text = ''
      { name, work }:
      derivation {
        inherit name;
        system = "x86_64-linux";
        builder = builtins.storePath "${pkgs.bash}" + "/bin/bash";
        deps = map builtins.storePath [ ${lib.concatMapStringsSep " " (d: "\"${d}\"") deps} ];
        args = [ "-c" ((${lib.generators.toPretty { } work}).''${work} + " && echo ok > $out") ];
      }
    '';
  };
  worker = {
    boot.kernel.sysctl = {
      # usual with zram: no watermark boost or swap readahead, swap first
      "vm.watermark_boost_factor" = 0;
      "vm.watermark_scale_factor" = 125;
      "vm.page-cluster" = 0;
      "vm.swappiness" = 190;
    };
    zramSwap = {
      enable = true;
      memoryPercent = 400;
    };
    virtualisation.cores = lib.mkForce 4;
    # room for the page cache the cache row writes
    virtualisation.diskSize = lib.mkForce 6144;
    virtualisation.additionalPaths = deps;
    services.tribuchet-worker = {
      agentMemoryReserve = lib.mkForce (1536 * 1024 * 1024);
      # what each row starts from (pairs.rs writes it, with the row's
      # timeouts, over /etc/tribuchet/worker.toml)
      settings = lib.mkForce {
        hub = "https://hub:7437";
        cert = "/etc/tribuchet/tls/worker.crt";
        ca-cert = "/etc/tribuchet/tls/ca.crt";
        max-jobs = 4;
      };
    };
  };
}
