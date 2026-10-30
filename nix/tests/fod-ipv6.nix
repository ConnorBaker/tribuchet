# Checks the sandbox's IPv6 against what the worker's should give it
# and fetches from the hub over IPv4 like fod.nix. The sandbox must have
# an IPv6 default route and global address exactly when `ipv6`, and a
# connect to an IPv6 address the worker cannot reach must fail at once
# with the given error, not hang through the guest's SYN retransmits
# (~130 s). Every check runs and reports before the build fails.
{
  bash,
  coreutils,
  hubIp,
  # the worker's IPv6 for this build, which fod_ipv6.rs sets up:
  # "unrouted" or "routed"
  state,
}:
let
  # whether the sandbox gets IPv6 (presto-pasta's
  # netdev::host_has_ipv6_default_route)
  ipv6 = state == "routed";
  # each connect's error: with no IPv6 route in the sandbox, unreachable;
  # with one, the worker's own, at once for its unreachable route and
  # later, when neighbor discovery gives up
  connects =
    if ipv6 then
      {
        "2001:db8:ffff::1" = "No route to host";
        "2001:db8:fffe::1" = "No route to host";
      }
    else
      { "2001:db8:ffff::1" = "Network is unreachable"; };
  yesno = if ipv6 then "yes" else "no";
  check = to: error: ''
    rc=0
    timeout 10 bash -c 'exec 3<>/dev/tcp/${to}/443' 2> connect.err || rc=$?
    err=$(< connect.err)
    echo "$err" >&2
    case $rc,$err in
      0,*) echo "FAIL: connect to ${to} succeeded" >&2; failed=1 ;;
      124,*) echo "FAIL: connect to ${to} hung" >&2; failed=1 ;;
      *'${error}'*) echo "ok: connect to ${to} failed at once: ${error}" >&2 ;;
      *) echo "FAIL: connect to ${to}: expected '${error}'" >&2; failed=1 ;;
    esac
  '';
in
derivation {
  name = "tt-fod-ipv6-${state}";
  system = "x86_64-linux";
  builder = builtins.storePath bash + "/bin/bash";
  PATH = "${builtins.storePath coreutils}/bin:${builtins.storePath bash}/bin";
  args = [
    "-c"
    (
      ''
        failed=0
        route=no
        while read -r _ len _ _ _ _ _ _ _ dev; do
          if [ "$len" = 00 ] && [ "$dev" = eth0 ]; then route=yes; fi
        done < /proc/net/ipv6_route
        global=no
        while read -r _ _ _ scope _ dev; do
          if [ "$scope" = 00 ] && [ "$dev" = eth0 ]; then global=yes; fi
        done < /proc/net/if_inet6
        echo "sandbox IPv6 default route: $route, global address: $global" >&2
        if [ "$route,$global" != "${yesno},${yesno}" ]; then
          echo "FAIL: expected both: ${yesno}" >&2
          failed=1
        fi
      ''
      + builtins.concatStringsSep "" (map (to: check to connects.${to}) (builtins.attrNames connects))
      + ''
        exec 3<>/dev/tcp/${hubIp}/8765
        while IFS= read -r l <&3; do printf '%s\n' "$l"; done > $out
        echo "ok: fetched from ${hubIp} over IPv4" >&2
        exit $failed
      ''
    )
  ];
  outputHashAlgo = "sha256";
  outputHashMode = "flat";
  outputHash = "fba0ea84c93fbcbfff10a9b33bc33409b5fd15eff0540b7b4389d691cde59fe8";
}
