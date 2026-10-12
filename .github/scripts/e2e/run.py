#!/usr/bin/env python3
"""Drive the two-runner e2e workflow.

One script with subcommands keeps the orchestration in one place and
out of the YAML; the workflow steps just call ``run.py <cmd>``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import textwrap
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[3]
BIN = REPO / "result" / "bin" / "tribuchet"
HUB_LOG = REPO / "hub.log"
WORKER_LOG = REPO / "worker.log"
AGENT_LOG = REPO / "agent.log"
AGENT_SOCKET = Path("/var/lib/tribuchet-agent/agent.sock")
AGENT_STATE = Path("/var/lib/tribuchet-agent/state")
# Same default uid block as the NixOS module.
AGENT_UID_BASE = 1325400064


def run(cmd: list[str], **kw: object) -> subprocess.CompletedProcess[str]:
    print("+", " ".join(cmd), file=sys.stderr)
    cp: subprocess.CompletedProcess[str] = subprocess.run(  # type: ignore[call-overload]
        cmd, check=True, text=True, **kw
    )
    return cp


def out(cmd: list[str]) -> str:
    return run(cmd, stdout=subprocess.PIPE).stdout.strip()


def sudo_write(path: str, content: str, *, append: bool = False) -> None:
    """Write a root-owned config file without shell quoting games."""
    run(
        ["sudo", "tee", "-a" if append else "--", path],
        input=content,
        stdout=subprocess.DEVNULL,
    )


def wait_for(
    pred: Callable[[], bool], *, timeout: int, interval: float = 1.0, what: str
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(interval)
    raise SystemExit(f"timed out after {timeout}s waiting for {what}")


def log_contains(path: Path, needle: str) -> bool:
    return path.exists() and needle in path.read_text(errors="replace")


def spawn_daemon(argv: list[str], log: Path) -> subprocess.Popen[bytes]:
    """Start a long-running process with its output captured to *log*
    (owned by the unprivileged runner so later steps can read it).
    ``start_new_session`` detaches it from this Python process so it
    survives the step boundary."""
    print("+", " ".join(argv), ">", log, file=sys.stderr)
    with log.open("wb") as fh:
        # Popen dups the fd; closing our handle is fine afterwards.
        return subprocess.Popen(
            argv, stdout=fh, stderr=subprocess.STDOUT, start_new_session=True
        )


def start_daemon(
    argv: list[str], log: Path, ready: Callable[[], bool], what: str
) -> None:
    """Spawn *argv* and wait up to 30 s for *ready*. A daemon that exited
    has its log dumped and fails the step, even if it got ready first."""
    proc = spawn_daemon(argv, log)
    wait_for(lambda: ready() or proc.poll() is not None, timeout=30, what=what)
    if proc.poll() is not None or not ready():
        sys.stderr.write(log.read_text())
        raise SystemExit(f"{what} failed")


# ---------------------------------------------------------------- hub ----


def hub_start(systems: list[str]) -> None:
    run(["sudo", "mkdir", "-p", "/etc/tribuchet", "/run/tribuchet"])

    # auth=tailscale: no TLS material; identity via tailscaled whois.
    # Restrict to tag:ci so only this workflow's runners can register.
    # worker-grace-secs covers the slowest matrix worker (macOS install
    # + cold cache) so per-system builds wait instead of declining.
    sudo_write(
        "/etc/tribuchet/hub.toml",
        textwrap.dedent(
            """\
            auth = "tailscale"
            listen = "0.0.0.0:7437"
            socket = "/run/tribuchet/hub.sock"
            config-dir = "/etc/tribuchet"
            worker-grace-secs = 600
            tailscale-allowed-tags = ["tag:ci"]
            """
        ),
    )

    # nix-daemon execs this as a nixbld* user, which cannot traverse
    # /home/runner; resolve to the world-readable /nix/store path.
    sudo_write(
        "/usr/local/bin/tribuchet-attach",
        f'#!/bin/sh\nexec {BIN.resolve()} attach "$1" --socket /run/tribuchet/hub.sock\n',
    )
    run(["sudo", "chmod", "+x", "/usr/local/bin/tribuchet-attach"])

    # `args` is required by the external-builders schema even when empty.
    eb = json.dumps(
        [
            {
                "systems": systems,
                "program": "/usr/local/bin/tribuchet-attach",
                "args": [],
            }
        ]
    )
    sudo_write(
        "/etc/nix/nix.conf",
        f"extra-experimental-features = external-builders\nexternal-builders = {eb}\n",
        append=True,
    )
    run(["sudo", "systemctl", "restart", "nix-daemon"])

    # Hub binds the attach socket to group nixbld and reads topTmpDir
    # owned by build users; both need root.
    start_daemon(
        [
            "sudo",
            "RUST_LOG=info",
            str(BIN),
            "hub",
            "--config",
            "/etc/tribuchet/hub.toml",
        ],
        HUB_LOG,
        lambda: log_contains(HUB_LOG, "hub running"),
        "hub start",
    )

    (REPO / "addr.txt").write_text(out(["tailscale", "ip", "-4"]) + "\n")
    print(HUB_LOG.read_text())


def hub_build(systems: list[str]) -> None:
    wait_for(
        lambda: log_contains(HUB_LOG, "worker registered"),
        timeout=360,
        interval=2,
        what="a worker to register",
    )

    # The flake-locked nixpkgs is what nixbot built and cached.
    nixpkgs = out(["nix", "eval", "--raw", "--inputs-from", ".", "nixpkgs#path"])

    def build(probe: str, suffix: str) -> None:
        # One nix-build for all systems: the hub queues each derivation
        # and dispatches it once the matching worker registers (within
        # worker-grace-secs), so per-system arrival order does not matter.
        paths = out(
            [
                "nix-build",
                "--no-out-link",
                "--max-jobs",
                str(len(systems)),
                "-I",
                f"nixpkgs={nixpkgs}",
                "--argstr",
                "runId",
                # Re-runs share GITHUB_RUN_ID; without the attempt the
                # previous attempt's outputs get substituted from the CI
                # cache and the hub never sees a build.
                f"{os.environ['GITHUB_RUN_ID']}-{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}",
                "--arg",
                "systems",
                "[ " + " ".join(f'"{s}"' for s in systems) + " ]",
                str(REPO / ".github/scripts/e2e" / probe),
            ]
        ).splitlines()
        if len(paths) != len(systems):
            raise SystemExit(f"expected {len(systems)} outputs, got {paths!r}")
        for path in paths:
            payload = Path(path).read_text().strip()
            if not payload.endswith(suffix):
                raise SystemExit(f"unexpected build output in {path}: {payload!r}")

    build("probe.nix", "-via-tailnet")
    build("probe-fod.nix", "fod-dns-ok")  # DNS + TLS from inside the sandbox
    if not log_contains(HUB_LOG, "dispatching build"):
        raise SystemExit("hub never dispatched a build")


# ------------------------------------------------------------- worker ----


def fetch_artifact(name: str, dest: str) -> None:
    """Poll for a sibling job's artifact in the same workflow run."""
    run_id = os.environ["GITHUB_RUN_ID"]

    def attempt() -> bool:
        r = subprocess.run(
            ["gh", "run", "download", run_id, "-n", name, "-D", dest],
            capture_output=True,
            check=False,
        )
        return r.returncode == 0

    wait_for(attempt, timeout=600, interval=5, what=f"artifact {name}")


def gh_api(path: str) -> Any | None:
    """GET the GitHub REST API via ``gh``; ``None`` on transient errors so
    poll-loop callers treat them as "not yet". ``gh`` carries its own CA
    bundle, which the macOS runner's Python lacks."""
    r = subprocess.run(
        ["gh", "api", f"repos/{os.environ['GITHUB_REPOSITORY']}/{path}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if r.returncode != 0:
        return None
    try:
        return json.loads(r.stdout)
    except json.JSONDecodeError:
        return None


def wait_buildbot(sha: str) -> None:
    """Block until the newest buildbot check run for *sha* succeeds, so the
    build below fetches nixbot's closure instead of rebuilding. nixbot
    replaces a commit's check run when it restarts a build, so older runs
    are ignored and a missing one (push fires before buildbot registers) is
    waited for."""
    name = "buildbot/nix-build"

    def ready() -> bool:
        data = gh_api(f"commits/{sha}/check-runs?check_name={name}")
        runs = data["check_runs"] if data else []
        latest = max(runs, key=lambda r: r["id"], default=None)
        return latest is not None and latest["conclusion"] == "success"

    wait_for(ready, timeout=1800, interval=15, what=f"{name} on {sha[:8]}")


def job_conclusion(name: str) -> str | None:
    """Conclusion of a sibling job in this run, or ``None`` while it is
    still running / on transient API errors."""
    data = gh_api(f"actions/runs/{os.environ['GITHUB_RUN_ID']}/jobs")
    jobs = data["jobs"] if data else []
    return next((j["conclusion"] for j in jobs if j["name"] == name), None)


def start_agent(worker_uid: int) -> None:
    """One build agent as root (which has the capabilities the agent's
    user namespaces need) with a self-bound socket."""
    run(["sudo", "mkdir", "-p", str(AGENT_STATE)])
    agent_cmd = [
        "sudo",
        "RUST_LOG=info",
        str(BIN),
        "agent",
        "--socket",
        str(AGENT_SOCKET),
        "--state-dir",
        str(AGENT_STATE),
        "--worker-uid",
        str(worker_uid),
    ]
    if sys.platform == "linux":
        agent_cmd += ["--uid-base", str(AGENT_UID_BASE)]
    start_daemon(agent_cmd, AGENT_LOG, AGENT_SOCKET.exists, "agent start")
    # The unprivileged worker must connect to the root-owned socket.
    run(["sudo", "chmod", "666", str(AGENT_SOCKET)])


def worker_run(hub_ip: str) -> None:
    run(["sudo", "mkdir", "-p", "/etc/tribuchet", "/var/lib/tribuchet"])
    sudo_write(
        "/etc/tribuchet/worker.toml",
        textwrap.dedent(
            f"""\
            hub = "http://{hub_ip}:7437"
            auth = "tailscale"
            max-jobs = 1
            state-dir = "/var/lib/tribuchet"
            agent-sockets = ["{AGENT_SOCKET}"]
            """
        ),
    )

    worker_cmd = [
        "RUST_LOG=info",
        str(BIN),
        "worker",
        "--config",
        "/etc/tribuchet/worker.toml",
    ]
    if sys.platform == "linux":
        # The worker runs unprivileged; builds run in the root agent's
        # user namespaces. Importing inputs through nix-daemon without
        # signatures needs a trusted user.
        user = out(["id", "-un"])
        run(["sudo", "chown", user, "/var/lib/tribuchet"])
        # Ubuntu 24.04 blocks unprivileged user namespaces by default
        run(["sudo", "sysctl", "-w", "kernel.apparmor_restrict_unprivileged_userns=0"])
        sudo_write("/etc/nix/nix.conf", f"trusted-users = root {user}\n", append=True)
        run(["sudo", "systemctl", "restart", "nix-daemon"])
        start_agent(worker_uid=os.getuid())
        proc = spawn_daemon(["env", *worker_cmd], WORKER_LOG)
    else:
        # macOS: worker and agent run as root (Seatbelt, nix-daemon trust).
        start_agent(worker_uid=0)
        proc = spawn_daemon(["sudo", *worker_cmd], WORKER_LOG)

    def finished() -> bool:
        if proc.poll() is not None:
            return True
        c = job_conclusion("hub")
        if c is None:
            return False
        if c != "success":
            sys.stderr.write(WORKER_LOG.read_text())
            raise SystemExit(f"hub job concluded {c!r}; aborting")
        return True

    wait_for(finished, timeout=900, interval=5, what="hub to finish")
    if not log_contains(WORKER_LOG, "builder finished"):
        sys.stderr.write(WORKER_LOG.read_text())
        raise SystemExit("worker never ran a build")


# ---------------------------------------------------------------- cli ----


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("hub-start")
    p.add_argument("--systems", nargs="+", required=True)
    p = sub.add_parser("hub-build")
    p.add_argument("--systems", nargs="+", required=True)
    p = sub.add_parser("worker")
    p.add_argument("--hub-ip", required=True)
    p = sub.add_parser("fetch-artifact")
    p.add_argument("name")
    p.add_argument("dest")
    p = sub.add_parser("wait-buildbot")
    p.add_argument("sha")
    args = ap.parse_args()

    match args.cmd:
        case "hub-start":
            hub_start(args.systems)
        case "hub-build":
            hub_build(args.systems)
        case "worker":
            worker_run(args.hub_ip)
        case "fetch-artifact":
            fetch_artifact(args.name, args.dest)
        case "wait-buildbot":
            wait_buildbot(args.sha)


if __name__ == "__main__":
    main()
