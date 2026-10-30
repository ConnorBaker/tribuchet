# A build's OOM score: what it starts at, and whether it may lower it
# (an agent runs at -1000; a build must not keep or regain that).
{ bash }:
derivation {
  name = "tt-slice-adj";
  system = "x86_64-linux";
  builder = builtins.storePath bash + "/bin/bash";
  args = [
    "-c"
    ''
      read -r adj </proc/self/oom_score_adj
      if (echo -1 >/proc/self/oom_score_adj) 2>/dev/null; then lower=allowed; else lower=refused; fi
      echo "adj=$adj lower=$lower" >$out
    ''
  ];
}
