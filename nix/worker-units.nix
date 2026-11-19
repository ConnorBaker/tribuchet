# Worker and agent units shared by the NixOS module and the flakelet.
# The worker runs unprivileged and leases every build to a per-uid,
# socket-activated agent, which owns the builder process and its user
# namespace, so builds survive worker restarts. The worker is DynamicUser,
# each agent a userdb record its generator writes (nss-systemd): a host
# that defines tribuchet / tribuchet-agent-N statically wins.
{
  pkgs,
  lib,
  package,
  # unit names differ: tribuchet-worker / tribuchet-agent@ vs <name> / <name>-agent@
  workerUnit,
  agentUnit,
  # integer or "auto" (one per CPU, decided by a generator on the machine)
  agents,
  agentUidBase,
  keyFile,
  workerToml,
}:
let
  auto = agents == "auto";
  # agent i's user and group, a userdb record the generator writes
  agentUser = "tribuchet-agent-";
  agentSocket = i: "${agentUnit}${i}.socket";
  stateDir = "/var/lib/tribuchet/a%i";
  # ExecStart cannot do arithmetic or resolve the worker's uid
  agentStart = pkgs.writeShellScript "tribuchet-agent" ''
    exec ${lib.getExe' package "tribuchet"} agent \
      --state-dir "/var/lib/tribuchet/a$1" \
      --uid-base "$(( ${toString agentUidBase} + ($1 - 1) * 65536 ))" \
      --worker-uid "$(${lib.getExe' pkgs.coreutils "id"} -u tribuchet)"
  '';
  # The worker's ExecStopPost: thaw every frozen build cgroup of this
  # worker's agents, whoever froze it, even on a host where the arbiter is
  # off. A build to keep frozen across a worker stop is
  # frozen through its agent's unit (systemctl freeze), whose own
  # cgroup.freeze this leaves alone.
  thawBuilds = pkgs.writeShellScript "${workerUnit}-thaw-builds" ''
    systemctl=${lib.getExe' pkgs.systemd "systemctl"}
    "$systemctl" list-units --all --plain --no-legend --type=service '${agentUnit}*.service' |
      while read -r unit _; do
        cg=$("$systemctl" show -P ControlGroup "$unit") && [ -n "$cg" ] || continue
        for f in "/sys/fs/cgroup$cg"/build-*/cgroup.freeze; do
          [ -e "$f" ] && read -r v <"$f" && [ "$v" = 1 ] || continue
          if echo 0 >"$f"; then
            echo "thawed ''${f%/cgroup.freeze} after the worker stopped"
          else
            echo "could not thaw ''${f%/cgroup.freeze}" >&2
          fi
        done
      done
    exit 0
  '';
  # The agents' users are userdb records: without nss-systemd no agent
  # could start. Refuse to start the worker instead, saying why.
  checkAgentUser = pkgs.writeShellScript "${workerUnit}-check-agent-user" ''
    for db in passwd group; do
      ${lib.getExe' pkgs.getent "getent"} "$db" ${agentUser}1 >/dev/null || {
        echo "agent user ${agentUser}1 not in $db: the agents' users are systemd userdb records in /run/userdb, which resolve only on systemd 249 or later with systemd (nss-systemd) on the $db line of /etc/nsswitch.conf" >&2
        exit 1
      }
    done
  '';
in
{
  # Not under a RuntimeDirectory: systemd creates the socket parents and
  # nothing removes them while agents keep running across worker stops.
  agentSocketsDir = "/run/tribuchet/agents";

  generators.agents = pkgs.writeShellScript "tribuchet-agents" ''
    exec ${lib.getExe' package "tribuchet"} agent-generator \
      --worker-unit ${workerUnit}.service --template ${agentSocket ""} \
      ${lib.optionalString (!auto) "--count ${toString agents}"} \
      --user-prefix ${agentUser} --uid-base ${toString agentUidBase} "$@"
  '';

  # The socket mode is open because the agent itself only accepts
  # connections from the worker uid.
  agentSocketConfig = {
    description = "tribuchet build agent %i socket";
    wantedBy = [ "sockets.target" ];
    before = [ "${workerUnit}.service" ];
    socketConfig = {
      ListenStream = "/run/tribuchet/agents/%i.sock";
      SocketMode = "0666";
    };
  };

  agentServiceConfig = {
    description = "tribuchet build agent %i";
    # Exiting after every build is the agent's normal lifecycle.
    unitConfig.StartLimitIntervalSec = 0;
    # One build per activation: let it finish, the next lease runs the
    # new ExecStart.
    restartIfChanged = false;
    serviceConfig = {
      ExecStart = "${agentStart} %i";
      # Not DynamicUser: it forces RestrictSUIDSGID, whose filter
      # narrows the builds' system calls below Nix's (openat2 ENOSYS).
      # What else it implied stays, written out.
      User = "${agentUser}%i";
      # true, not DynamicUser's "disconnected": systemd before 257
      # ignores that value and leaves /tmp shared
      PrivateTmp = true;
      RemoveIPC = true;
      ProtectSystem = "strict";
      ProtectHome = "read-only";
      NoNewPrivileges = true;
      # The worker and the uid block reach into the state dir.
      # Traverse-only: the per-build scratch dirs are world-writable for
      # the block, but their names are random.
      StateDirectory = "tribuchet/a%i";
      StateDirectoryMode = "0711";
      # scratch a killed agent left
      ExecStartPre = "+${pkgs.coreutils}/bin/rm -rf ${stateDir}/scratch";
      ReadWritePaths = "/var/lib/tribuchet";
      # Writing the uid/gid maps of the agent's user namespace needs
      # CAP_SETUID/CAP_SETGID over the uid block, dropped right after.
      # CAP_CHOWN stays: each build cgroup is handed to its mapped root.
      AmbientCapabilities = [
        "CAP_SETUID"
        "CAP_SETGID"
        "CAP_CHOWN"
      ];
      CapabilityBoundingSet = [
        "CAP_SETUID"
        "CAP_SETGID"
        "CAP_CHOWN"
      ];
      # the per-build cgroup the sandbox roots its cgroup namespace in
      Delegate = true;
      # Builders inherit this; match nix-daemon to avoid EMFILE.
      LimitNOFILE = 1048576;
      Environment = "RUST_LOG=info";
    };
  };

  workerServiceConfig = {
    description = "tribuchet build worker";
    wantedBy = [ "multi-user.target" ];
    # checkAgentUser needs user lookups up, NixOS's nscd included
    # (systemd.special(7))
    after = [ "nss-user-lookup.target" ];
    serviceConfig = {
      Type = "notify";
      DynamicUser = true;
      User = "tribuchet";
      WatchdogSec = "30";
      ExecStartPre = checkAgentUser;
      ExecStart = "${lib.getExe' package "tribuchet"} worker --config ${workerToml}";
      ExecStopPost = "+${thawBuilds}";
      # Running builds live in the agent services and are re-adopted by
      # the next worker instance.
      StateDirectory = "tribuchet/worker";
      Environment = [
        "RUST_LOG=info"
      ]
      ++ lib.optional (keyFile != null) "TRIBUCHET_KEY=%d/worker-key";
      ProtectHome = true;
      Restart = "on-failure";
    }
    // lib.optionalAttrs (keyFile != null) {
      LoadCredential = "worker-key:${keyFile}";
    };
  };
}
