# The held phase's additions to nix/test.nix's nodes
# (crates/tribuchet/tests/e2e/held.rs): its builds (held.nix), each
# /etc/tt/held-NAME.nix, and a hub grace that outlasts a worker restart
# waiting AGENT_ANSWER on an agent.
{ pkgs, lib, ... }:
let
  # NAME = [ seconds files ]: a busy Finish, a stopped agent
  builds = {
    F = [
      5
      100000
    ];
    S = [
      40
      0
    ];
  };
in
{
  hub = {
    environment.etc = lib.mapAttrs' (
      name: a:
      lib.nameValuePair "tt/held-${name}.nix" {
        text = ''
          import ${./held.nix} {
            bash = "${pkgs.bash}";
            coreutils = "${pkgs.coreutils}";
            name = "${name}";
            seconds = ${toString (lib.elemAt a 0)};
            files = ${toString (lib.elemAt a 1)};
          }
        '';
      }
    ) builds;
    services.tribuchet-hub.settings.worker-grace-secs = lib.mkForce 30;
  };
  worker = { };
}
