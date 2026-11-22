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
    virtualisation.additionalPaths = deps;
    # The agents' slice as a host's agent-memory policy left it on this
    # VM with a 1536 MiB reserve: A and S (MemTotal 4,010,648 kB, from
    # the worker's memory=2239651840 then), swapping through zswap with
    # a 100 % pool and writeback off, so that pages zswap refuses (random
    # bytes) stay in RAM, in the swap cache. Not kept: that policy's clamp
    # guard and MemoryZSwapMax=0 for every other unit.
    boot.kernelParams = [
      "zswap.enabled=1"
      "zswap.max_pool_percent=100"
    ];
    systemd.slices."system-tribuchet\\x2dagent".sliceConfig = {
      MemoryMax = 2239651840;
      MemorySwapMax = 4106223616;
      MemoryZSwapWriteback = false;
    };
    services.tribuchet-worker = {
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
