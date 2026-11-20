# Runs the system-call probe (syscalls.c) as the builder's only work.
# variant keeps the stock nix-daemon's build and tribuchet's apart.
{
  bash,
  probe,
  variant,
}:
derivation {
  name = "tt-syscalls-${variant}";
  system = "x86_64-linux";
  builder = builtins.storePath bash + "/bin/bash";
  args = [
    "-c"
    "${builtins.storePath probe} > $out"
  ];
}
