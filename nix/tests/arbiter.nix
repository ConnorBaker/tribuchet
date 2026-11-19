# A build of `spinners` busy loops for `seconds` of wall clock, then one
# loop until `until`: what the CPU arbiter sees as a parallel phase,
# then a serial one. `sleepers` processes of coreutils' sleep run
# beside them for `seconds`: threads the arbiter samples every 200 ms
# that never run. Nothing marks a build for the arbiter: every build is
# subject to it.
{
  bash,
  name,
  spinners,
  seconds,
  until ? seconds,
  sleepers ? 0,
  coreutils ? null,
}:
derivation {
  name = "tt-arb-${name}";
  system = "x86_64-linux";
  builder = builtins.storePath bash + "/bin/bash";
  args = [
    "-c"
    ''
      spin() { while [ $SECONDS -lt $1 ]; do :; done; }
      for ((i = 0; i < ${toString sleepers}; i++)); do
        ${if sleepers > 0 then builtins.storePath coreutils + "/bin/sleep" else ":"} ${toString seconds} &
      done
      for ((i = 0; i < ${toString spinners}; i++)); do spin ${toString seconds} & done
      wait
      spin ${toString until}
      echo ok > $out
    ''
  ];
}
