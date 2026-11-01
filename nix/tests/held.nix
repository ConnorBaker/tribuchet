# The held phase's build (crates/tribuchet/tests/e2e/held.rs): sleeps
# `seconds`, then writes `files` empty files to $out, enough that its
# agent's Finish (make-readable) and Cleanup (remove) run long enough to
# be caught and stopped by the test.
{
  bash,
  coreutils,
  name,
  seconds,
  files,
}:
derivation {
  name = "tt-held-${name}";
  system = "x86_64-linux";
  builder = builtins.storePath bash + "/bin/bash";
  args = [
    "-c"
    ''
      echo "held-${name}: sleeping ${toString seconds} s"
      ${builtins.storePath coreutils}/bin/sleep ${toString seconds}
      ${builtins.storePath coreutils}/bin/mkdir $out
      cd $out
      for ((i = 0; i < ${toString files}; i++)); do : > f$i; done
      echo ok > $out/ok
      echo "held-${name}: wrote ${toString files} files"
    ''
  ];
}
