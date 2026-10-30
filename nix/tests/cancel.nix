# A build that logs about once a second for `seconds` and then succeeds:
# one whose client a test kills or brings back late. Its log names it
# (`<name>-marker-running`) so that the test can find its builder.
{
  bash,
  name,
  seconds,
}:
derivation {
  name = "tt-${name}";
  system = "x86_64-linux";
  builder = builtins.storePath bash + "/bin/bash";
  args = [
    "-c"
    # logs roughly once a second: continuous output must not keep the
    # hub from noticing the departed client
    ''
      echo ${name}-marker-running >&2
      last=-1
      while [ $SECONDS -lt ${toString seconds} ]; do
        if [ $SECONDS -gt $last ]; then last=$SECONDS; echo "still running $SECONDS" >&2; fi
      done
      echo ${name}-done > $out
    ''
  ];
}
