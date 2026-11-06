# tribuchet — remote build execution for Nix

RBE-style remote builder service driven by Nix's experimental
`external-builders` feature. A shim on the client machine receives the
build environment from Nix as JSON, forwards it to a local hub, which
schedules the build on a remote worker that executes the builder inside
its own sandbox.

## Why not the build-hook / `--builders` protocol?

The classic remote build protocol requires SSH reachability, nix on every
worker, and copies the full closure each time without a scheduler. The
external-builders interface gives us the complete, already-rewritten build
environment (builder, args, env, input closure, scratch output paths) and
lets us own transfer, scheduling, and execution.

## Key insight: scratch paths are identical on both sides

Nix hands the external builder *scratch* output paths and expects them to
be populated on exit. The worker sandbox writes to the very same scratch
paths; the shim unpacks the returned outputs at those paths unchanged. Nix
then performs self-reference rewriting, hashing, and registration itself.
No store-path rewriting in tribuchet and no drvPath needed. Workers run a
nix-daemon of their own: inputs are imported through it (AddToStoreNar),
so they are registered in the worker's Nix database and protected from
GC by per-build temp roots. The worker must be a trusted daemon user
(imports skip signature checks; transport authenticity comes from mTLS).

## Components (single binary, subcommands)

![Architecture](docs/architecture.svg)

## Build flow

1. Nix invokes `tribuchet attach <build.json>`. The shim parses build.json
   (version 1: builder, args, env, inputPaths, outputs, system, topTmpDir,
   tmpDirInSandbox) and submits a build request to the hub over a unix
   socket, followed by a zstd tar of its own `topTmpDir`: structured
   attrs / `passAsFile` place files there that env refers to via
   `tmpDirInSandbox` (`/build/.attrs.json` etc.). The hub only buffers
   and forwards that archive. It never reads client directories off
   disk.
2. Hub validates the request (store dir pinned to `/nix/store`, store-path
   basenames restricted to Nix's name charset, absolute builder,
   `tmpDirInSandbox` pinned to `/build`, no duplicate or input-aliasing
   outputs, tmp dir archive size capped) and dedupes by a hash of the
   request. Nix derives the scratch outputs deterministically from
   the drvPath, so submissions of the same derivation hash identically:
   a matching request attaches to the in-flight build's log and result,
   while a *different* request claiming an in-flight scratch path is
   rejected.
   Otherwise it queues the request for a worker matching `system` and
   its required features. A build without network for a system that no
   connected worker serves is declined, and Nix builds it locally, once
   no worker is expected for it: `worker-grace-secs` after the hub's
   start or after the last worker serving it left. A fixed-output build
   is never declined, since a local build would run in the hub host's
   network: it waits for a worker, and its clients are told. Otherwise
   submitters block and Nix's max-jobs bounds parallelism.
3. Staging: the assignment carries the input closure, with a manifest
   (Nix db metadata plus chunk list) inline for paths the hub has
   chunked before and does not know the worker to have. The worker asks
   its nix-daemon (taking temp roots so GC cannot race the build) and
   answers with one Need: the paths it lacks and the chunks its store
   lacks. The hub streams manifests for those paths as it computes them
   and the chunks of every Need (see Chunked staging).
4. Worker imports missing inputs through its nix-daemon (which verifies
   the NAR hash and registers the path), constructs the sandbox, and
   executes `builder args…` with the env from build.json, cwd
   `/build`. Logs stream back live through hub to the shim's stdout/stderr
   (Nix shows them as ordinary build output).
5. On success the worker chunks every scratch output's NAR (bounded in
   size and by the build deadline) and announces the chunk manifests.
   The hub requests the chunks its cache lacks and assembles each NAR,
   checking every chunk's BLAKE3 against the manifest as it relays. The shim unpacks into a temp
   path next to each scratch path and renames into place only after the
   verified end-of-stream event, then exits 0. Builder failure ⇒ shim
   exits with the builder's status; Nix reports a normal build failure.

## Sandbox

We re-implement the sandbox rather than driving builds through the
worker's Nix (its daemon serves only as the input store).
Reference implementations: `nix/src/libstore/unix/build/` and
`nix/src/libstore/darwin/build/sandbox-defaults.sb`.

* Linux: `unshare(CLONE_NEWUSER|NEWNS|NEWPID|NEWIPC|NEWUTS|NEWNET)`,
  then a fork so the builder execs as PID 1 of the
  new PID namespace — its death kills every descendant, so daemonized
  builder children cannot outlive the build. Input paths are bind-mounted
  read-only (`MS_NOSUID|MS_NODEV`) at their store paths inside a private
  root, scratch outputs are created in a writable store dir, the shipped
  tmp dir is bind-mounted at `tmpDirInSandbox`, minimal `/dev` (nodes,
  `/dev/shm` tmpfs, devpts), fresh `/proc`, loopback brought up, stub
  `/etc/passwd`, then `pivot_root` + detach of the old root. The uid is
  remapped via the user namespace (no separate build uid yet). When the
  worker's cgroup is delegated (systemd `Delegate=yes`), each build runs
  in its own cgroup with an optional `memory.max`, torn
  down via `cgroup.kill`. `--sandbox-bin-sh` binds a static shell at
  `/bin/sh` like Nix's busybox sandbox path. Builds requiring the
  `uid-range` system feature get a disjoint 65536-uid block (Nix's
  auto-allocate-uids scheme, root worker required), run as in-namespace
  root, and see their own delegated cgroup subtree at `/sys/fs/cgroup`
  — enough for systemd-nspawn inside the sandbox. `--emulate
  system=/path/to/static-qemu` advertises foreign systems; such builds
  get the emulator bound into the sandbox and registered in a per-userns
  binfmt_misc instance (kernel 6.7+); a nested user namespace drops the
  registration-time root back to uid 1000 for the build. A build with
  network gets user-mode NAT into its namespace (the embedded
  [presto-pasta](https://github.com/Mic92/presto-pasta) datapath, run
  in a thread of the build's agent), never the host's network; a
  worker without `/dev/net/tun` refuses such a build, and does not
  advertise the `tribuchet-network` feature that the hub requires of
  a worker for one, so it is sent none. Loopback,
  unspecified and the worker's own addresses are refused before any
  rule; other non-public destinations, among them any on the worker's
  own networks, unless a rule allows them; and the sandbox has IPv6
  only when the worker has a usable IPv6 default route
  (`netpolicy.rs`). The worker's `[fod-network]` setting adds an ordered
  allow/deny rule list (a destination, required: an address, a CIDR,
  `private` or `any`; protocol, ports/port ranges; first match wins)
  evaluated for every outbound connection of such builds.
* macOS: no mount namespace, but inputs already live at their real
  /nix/store paths thanks to the daemon import. The worker leases each
  build to a per-uid agent (`tribuchet agent`, a socket-activated
  launchd daemon running as one of the `_tribuchetbldN` build users):
  the agent unpacks the shipped tmp dir into its own scratch dir,
  which becomes the cwd, and env values referencing the hub's
  `tmpDirInSandbox` (e.g. `/build` from a Linux hub) are rewritten to
  it, so no symlink is created at a hub-chosen path. The agent applies
  a deny-default Seatbelt profile via `sandbox_init_with_parameters`
  before exec'ing the builder, modeled on Nix's `sandbox-defaults.sb`
  (reads stay permissive; writes are scoped to the scratch dir,
  outputs, and specific device nodes; signals are limited to the
  sandbox). The agent and its builder share a uid, so the profile is
  the only wall between them: an escaped builder could tamper with its
  agent, but not with the worker or other builds. Since the agent, not
  the worker, owns the builder process, running builds survive worker
  restarts and are re-adopted.
* A build has network iff it is fixed-output, told from the path Nix
  chose for its one output, `out`, never from the environment it sets
  for itself (`build_json.rs`): fixed-output where that path is the
  store path of the content hash the derivation declares. An `__impure`
  one gets none. A worker gives it a network of its own only on Linux
  with `/dev/net/tun`, so no current macOS worker advertises
  `tribuchet-network` (one refuses such a build): a darwin one waits,
  its clients told, until such a worker registers or the client gives
  up.

Accepted tradeoffs: no recursive-nix, sandbox parity is ours to maintain,
trusted worker pool assumed (output authenticity rests on the
authenticated transport).

## Security

* Transport: mTLS by default; `tribuchet ca` issues the CA and
  per-worker certs (finite validity: 10y CA, 2y leaves; no revocation
  yet — rotate the CA if a worker key leaks). With `auth =
  "tailscale"` the listener runs plaintext and the hub asks
  tailscaled's LocalAPI `whois` for the peer's node name and ACL tags
  on each session, so WireGuard provides confidentiality/integrity
  and the tailnet provides identity (optionally gated to
  `tailscale-allowed-tags`).
* Output authenticity comes from the authenticated worker session
  (mTLS or tailnet identity). The hub checks every output chunk against
  the worker's manifest, the client's Nix computes narHash on import.
* The attach socket is group-restricted to `nixbld` (the hub refuses to
  start without that group). Request validation pins every client-chosen
  path.
* The worker validates everything the hub sends (build ids, store paths,
  builder, sandbox dir) before using it in filesystem operations — a
  compromised hub does not get filesystem primitives on workers.

## Scale & state

MVP targets 2–10 workers and a few clients: all scheduler state is in
memory (no database). The hub's replay buffer is capped at 256 MiB per
build. An attach client that is slow, or attaches late, may miss log
lines but receives every output (`hub/state/replay.rs`).

### Chunked staging

A manifest is a path's FastCDC chunk boundaries and BLAKE3 hashes; the
worker answers with the chunks it lacks and only those are transferred.
Warm workers skip most bytes this way and re-staging a cached closure
costs one round trip. The hub remembers per session which paths a
worker has, so repeat builds carry bare path names only.

Both sides back this with an on-disk chunk store (hub:
`chunk-cache-bytes`, worker: `chunk-store-bytes`, 10 GiB default): append-only packs with S3-FIFO eviction, chosen over LRU
because cold stagings are one-hit-wonder scans. At rest the store is
never a source of truth — a lost or corrupt chunk costs a re-transfer
and the daemon's NAR-hash check backstops correctness, so the whole
directory can be deleted at rest. A chunk evicted before use is
requested again. An output delivery pins its chunks in the hub's cache
until the build ends, and while a late client still catches up on it.
No lock that a lookup or pin takes is held across file I/O, and the
hub's runtime does no store I/O; systemd is pinged only while the
runtime's own timer fires (`sd.rs`), and tokio's eager driver hand-off
keeps one blocked worker from stopping that timer (`rt.rs`).

Hub restarts cancel nothing, without any state handoff: on SIGTERM the
hub saves the placement history and exits, and the replacement
reconstructs its state from the edges and that file. Workers re-register
and announce the dedupe keys of builds they still hold (running, or
finished but undelivered); attach clients reconnect and resubmit the
identical request, whose deterministic dedupe key routes it back to the
worker holding the build, which resumes (or just re-delivers the
finished result) instead of building again.

Worker redeploys are plain unit restarts: builds run under the
per-uid agent services, which hold the log and exit status until a
worker collects them, and resume state is persisted in the build
dir of each run (`builds/<id>.<ns>`, one per run of a build, which no
other run removes). The replacement worker re-adopts running builds
from their agents, supervises them to completion and redelivers finished
results. A worker runs min(`max-jobs`, reachable agents) builds; a
running build whose agent does not answer keeps its key, its slot and
its agent until the agent answers, and its clients are told it waits.
An agent answers Adopt and Status at once whatever Kill or Finish it runs
(each on a thread of its own; Kill, Finish and Cleanup take turns, so no
kill sweep kills another's helper), and once a Start has registered its
build. It does not while a Start stages its build, while it runs a
Cleanup (sent once the result is on disk, which a restarted worker
delivers without asking the agent), or for up to 10 s behind a client
that connects and sends nothing. Beyond those, an agent that does not
answer is stopped or stalled from outside.
The hub covers the
session gap by requeueing instead of failing jobs whose worker
session died; the attached client sees a pause, not an error. For
`worker-grace-secs`, or until that worker registers again, such a
job, once its assignment was sent, is kept for its worker (by name):
a quick reconnect resumes it instead of another worker running it a
second time. A full
stop behaves the same way: builds keep running, and their results
wait on disk until a worker starts again (or expire undelivered).
A queued build that no connected worker can serve stays queued until
one registers, and its clients are told so. A loss the worker resumes costs a build nothing; the fourth
genuine loss (followed by a start-over) fails it, and so does the
thirteenth loss on a resume (`hub/losses.rs`).
The hub knows a reconnecting worker by name: the hostname it reports
(not anything in its certificate), or with `auth = "tailscale"` its
tailscale node's short name. No two workers may share a name, nor two
workers one tailscale node. The hub does not check, so a shared name
goes unnoticed, and a build kept for its worker through the reconnect
grace can be released while that worker still runs it, and run twice.
Give each worker its own hostname, and a container `--hostname`:
podman otherwise uses its ID, new on every recreate, or with
`--network=host` the host's own hostname.

A build with no attach client is kept while no waiting build wants its
slot. Under demand, one whose clients have all been gone 14 s is given
up, the one with the least work sunk (its run time) first, and no more
than the waiting builds can use, counted hub-wide
(`hub/state/clientless.rs`). None that a worker holds from before a hub
restart is given up: it runs to completion.

The hub's own nix-daemon fails a build, naming the cause, once a
failure of class c has repeated θ(c) times in a row on the same
request, as that build counts them; otherwise the build waits and its
clients are told once (`hub/relay/daemon.rs`):

| class | θ |
|---|---|
| a refusal of the request, a handshake the hub cannot speak, a new connection closed before the handshake as the daemon's journal says it refuses the hub's user | 1 |
| a message the hub cannot read | 2 |
| a request the daemon drops as its journal says a worker crashed | 3 |
| anything else: a drop or close with no such word in the journal, no answer within a minute, a silent daemon's included | ∞ |

## Placement

A queued build goes to the least full worker with an idle agent that
serves it, by what it and the builds already there are expected to use
against what the worker has (`hub/placement/rank.rs`). Fullness is the
larger of the expected cores of the worker's unsettled builds, this one
counted, over its cores, and their expected memory over the memory its
builds may use. Each worker reports both when it registers
(`worker/caps.rs`): the CPUs its own affinity and CPU quota allow, which
its agents are taken to share, and the lowest `memory.max` of its own
cgroup and those above it (a container's limit), its agents' slice, and
MemTotal; hub.toml holds no table of workers. Memory counts in a
comparison only where every worker compared reported it, and a worker
that reported no cores is counted by its agents, as a worker older than
this field is during a rolling upgrade; a worker counts as one core at
least: one that registered none, as it may with no agents yet, can have
idle agents later, and as no cores it would be infinitely full, or NaN
(which sorts first on x86). Both are as registered, for the session: one
that registered no cores and `max_jobs` 0 counts as one core until it
registers again. A hand-edited history with a peak of 0 on a worker
reporting 0 bytes for its builds makes its memory ratio 0/0, a NaN,
which `f64::max` discards for the cores' ratio: History::load does not
check runs, as Run::of does. Memory has no such floor: a worker that
reports 0 bytes for its builds is infinitely full by memory, and is not
chosen where memory is compared. A build counts against its worker from
its dispatch, just after the worker takes it from the queue, until it
settles: a worker that asks in between ranks without it. After a hub
restart the builds still running count against no worker until each is
submitted again by its client and resumed by its worker; a worker
registering again does not count them.

What a build is expected to use comes from what builds of its key used
on this farm (`hub/placement/history.rs`): its system and pname, the
name up to its version or a hash. Each key keeps its last 8 runs that
succeeded or were OOM-killed, as the workers measured them: the largest
peak memory among them (an OOM-killed run's peak is a lower bound), and
the medians of cores (CPU time over running time, frozen time not
counted) and running time over the runs that succeeded. A failed run
teaches nothing. A key with no run takes the longest dash-prefix of its
pname that has one, then its class (its system and whether it declares
`big-parallel`), then a fixed prior. A `big-parallel` declaration picks
the class (its runs, and the cores a key without a succeeded run takes)
and the prior; nothing else. Placement ranks by memory and cores only:
the running time, the OOM kills and where an expectation came from are
learned and logged at dispatch, for a replay of the journal to weigh,
and unused. A run with no running time (a worker can report none)
teaches no timing. Keys are never dropped. A build's expectation is
taken once, as it is first queued; a requeued build keeps it.

The history is kept in `placement-history.json` ([placement] `history`;
a path ending .tmp or .lock, a history file's own temporary file's or
lock's name, the hub warns of and neither loads nor saves, its "loaded …
keys=0" then reading as if the file were empty). As the hub starts it
takes the file's lock (`placement-history.json.lock`) and loads the
file; a hub that finds the lock held, or cannot open it, warns, loads
the file as it is, and never saves for its lifetime. It does not take
the lock again at a later save: whatever holds the lock (another hub,
the swap, seed-history writing a new file) can write the file meanwhile,
so its copy may be stale, which it would save over the newer file if it
took the lock later. It saves every 5 minutes when the history changed
and at SIGTERM, off the runtime. Runs learned after the SIGTERM save are
lost, and a hub that ends otherwise (its servers failing, the watchdog's
abort) loses up to 5 minutes of them; a save that fails is not tried
again until the history changes. A file that cannot be read or parsed,
for any reason but its absence (the previous format, permissions, I/O
errors), is warned of, and the hub starts empty and never saves over it:
the file is the only record older than the journal's retention. Neither
the permissions case nor the warning's text is covered by a test. A file
that is a symlink becomes a regular file at the first save, its target
left as it was; a dangling one counts as absent.

To seed it from the hub's journal, while the hub runs: run `tribuchet
seed-history --history SIDE` from the hub's package (the NixOS module
puts no tribuchet on PATH) on `journalctl -u tribuchet-hub -o cat`, SIDE
a new file beside the hub's, LIVE (it refuses one that exists, and a
name ending .tmp or .lock, as a hub's temporary file and lock are, at
which a hub takes no history either); it takes SIDE's lock, and the hub
serves on. Then, from a root shell (`sudo -s`), swap SIDE in, as one
command:

```sh
systemctl stop tribuchet-hub.socket tribuchet-hub.service; \
flock -n LIVE.lock sh -ec 'test -f SIDE; if [ -e LIVE ]; then ln -T LIVE LIVE.$(date +%s); fi; mv -T SIDE LIVE' || echo 'NOT SWAPPED' >&2; \
systemctl start tribuchet-hub.socket
```

One command, because `tribuchet attach` retries a build's connects 30
times 2 s apart in all, not per outage. systemd stops the hub before its
socket, which the NixOS module's hub unit requires (After= is systemd's
own for a socket's service; without Requires=, the socket goes inactive
while the hub is still saving, and whether connects are then refused is
not settled: on systemd 255 user units they still succeeded, the hub
holding the listening socket it inherited; the farm's is untested): the
socket listens through the hub's SIGTERM save, connections queued then
are closed unanswered as it stops (attach retries them), and connects
are refused from then through the renames to the start. As the hub
requires its socket, restarting the socket restarts the hub too. How
long that save adds is not measured, and a wedged hub takes systemd's
stop timeout, 90 s by default, longer than attach's budget. Where a hub
holds the lock, SIDE is not a file (missing, a directory or a dangling
symlink), or a backup of LIVE of the same second exists, the swap prints
NOT SWAPPED and leaves LIVE as it was. The command exits 0 either way,
its last step being the start, so read its output. Where it swaps, the
old LIVE stays as LIVE.<seconds>. A directory or a symlink to one at
that name, or a directory at LIVE, refuses the swap (`ln -T`, `mv -T`;
plain mv would move SIDE into it and exit 0); a symlink at LIVE to a
file or a directory is replaced and kept as LIVE.<seconds>, a dangling
one replaced and not kept. Where SIDE is a symlink to LIVE or a hard
link of it, the swap refuses at mv, after ln has left a stray
LIVE.<seconds>, which refuses a retry in that second. A SIDE that is a
symlink to a file is moved in as LIVE, the symlink itself (a relative
one not beside LIVE then dangles, or names another file), and an empty
or garbage SIDE is moved in too: the hub warns, runs empty and never
saves over it. Only a hand-made SIDE gets there; seed-history writes a
regular file. LIVE and SIDE go into the command unquoted (no spaces or
quotes); SIDE beside LIVE is not enforced, and across filesystems mv
copies, so that swap is not atomic. The command, the journal's unit and
the cache directory are the NixOS module's names; the flakelet's hub is
named by its `name`. The socket starts again either way; where that
fails (its port taken), systemctl says so and exits 1, the swap made or
not, and the hub stays down until the socket is started again (after
`systemctl reset-failed tribuchet-hub.socket` if the start refuses).
SIDE.lock, seed-history's, stays behind: leave it, and LIVE.lock, in
place, as a lock file deleted while held lets the next taker lock a new
file, and two writers write. What the hub learned since the journal was
read is lost. The swap replaces the history and does not merge into it:
SIDE holds only what the journal holds, so keys and runs older than the
journal's retention are kept only in LIVE.<seconds>. Seed and swap only
for a first deploy, or where the hub cannot read LIVE ("placement
history unreadable"), not over a readable LIVE. Stopping the service but
not the socket lets a queued connection start a hub during the swap: it
finds the lock held, loads LIVE as it then is, and never saves, and the
file stays intact.

The worker that asks takes the first queued build it can, unless
another connected worker that serves it has an idle agent (a free slot,
not the one build a worker stages ahead while every slot is busy) and
would be less full with it, or the asker has only that lookahead. That
worker is woken, and takes it when it asks; the asker goes on to the
next build. A tie stays with the asker. A build is passed over for at
most 2 s from the first time, then the next worker that asks takes it,
so none waits on another worker's ask.

Nothing holds a build back for memory. A dispatch whose worker's
expected memory, this build counted, is more than the worker's memory
is logged as such ("placed over predicted memory"), and its dispatch
line carries the worker's expected load and size. Holding builds for
memory was replayed on an earlier model of this code and left out: no
build in the copy of the hub's journal the figures come from
(`hub/placement/history.rs`) was OOM-killed, and the holds delayed the
builds they held.

## CPU arbitration

The worker's CPU arbiter (`worker/arbiter.rs`) is always on; nothing
turns it off: where parallel builds each fill every CPU, it finishes
the leader, and the builds on average, earlier, on less CPU. While the
oldest parallel build (by its start, persisted with it, so the same
after a worker restart; ties by key) keeps every CPU busy, the other
parallel builds are frozen. No build is
frozen more than 10 s in any 30 s, and builds stay frozen only while
what they pin together (each its memory.current less its page cache;
swap is no room) fits each capped cgroup's headroom and the host's
MemAvailable (`worker/arbiter/memory.rs`). The worker freezes and thaws
a build only through the write-only handle on its cgroup.freeze that
the build's agent hands over with the lease; the file is the agent's
(mode 0644), so neither the worker nor any build can open it to write.
The agent thaws its build when the lease ends, however the worker ended
it, and the worker unit's ExecStopPost thaws every build as root once
the worker has exited, should an agent not. Frozen time, by the
cgroup's own count, counts toward neither `build-timeout-secs` nor
`max-silent-time-secs`, nor a client's `timeout` and `max-silent-time`,
which reach the worker through Nix's build.json (patched Nix) and the
hub, the lesser of the two applying.
A kernel without that count (`frozen_usec`, before Linux 6.18) is
reported at start, and nothing is frozen there.

## Known limitations (MVP)

* Workers run up to `max-jobs` concurrent builds over one session.
* The hub's tmp-dir tar and the worker's unpack walk their trees
  through directory fds with O_NOFOLLOW, but output packing (our NAR
  serializer) and unpacking (harmonia-file-nar) resolve paths. Output
  packing therefore trusts that nothing rewrites the finished build's output tree while
  it is being packed (builds run under disjoint uids, so only root or
  the same build could).
* Stopping the worker does not stop its running builds (KillMode=
  process); an operator who wants them gone must kill the build
  cgroups too.
* Results are kept until the hub acknowledges them, but log replay
  offsets advance when a chunk is handed to the session, so a few log
  lines in flight when a session dies are skipped on resume.
* An abandoned queued job is dropped when a worker would have picked
  it up, not immediately.
* The attach socket sets no HTTP/2 keepalive: an attach client that
  stays connected but reads nothing keeps its stream until it exits.
* An output delivery fails its build when the hub's chunk cache cannot
  store or read back a chunk of it.
* A freeze still counts against wall-clock timers the worker does not
  own: a build's own (test drivers, watchdogs), and Nix's `timeout`
  where Nix is not patched to leave it to the external builder.
* The frozen builds' memory rule is a margin, not a bound: a leader that
  grows faster than one 200 ms sample can still fill a capped slice.
* Within the agents' slice the kernel's OOM killer kills one process,
  the largest, which may be a co-tenant's; a build whose child is
  killed but whose builder exits 0 succeeds (`oom_killed=true`). From
  Linux 7.0, and 6.19.9, its memcg scan skips an exiting victim
  (a72f73c4dd9b) and can kill a second task.
* Dedupe attaches duplicates to the first attempt, so a transient
  failure propagates to all attached submitters (same as Buck2's RE
  dedupe behaviour).
* The Linux builder keeps the worker's kernel uid (remapped in the user
  namespace); there is no dedicated unprivileged build user yet.
* A build's network is told from its output path, which build.json
  does not say the type of: a fixed-output build Nix runs at another
  path (`--check`, `--rebuild` or `--repair` of a valid output) or
  under git hashing is not told fixed-output, and gets none.
* Input chunks are verified against the hub's manifest and the daemon
  checks the NAR hash the hub declared, but both come from the hub: the
  worker trusts the mTLS-authenticated hub for input content.
* With `externalBuilders.dynamic`, the hub's nix.conf fragment lists
  only the systems a connected worker serves, and nix.conf lists none
  of its own: with no connected worker for a system, Nix never asks the
  hub, and the hub host's own nix-daemon builds a fixed-output build
  for it, in the host's network.

## Deployment

Hub and worker read their settings from TOML config files
(`--config`, default `/etc/tribuchet/{hub,worker}.toml`); only the
one-shot `attach` and `ca` commands take their parameters on the
command line. `nixosModules.default` ships hub and worker services
(`services.tribuchet-hub`, `services.tribuchet-worker`): the hub is
socket-activated, the worker unit delegates its cgroup subtree for
per-build limits and restarts on package bumps and settings changes
while running builds keep going (KillMode=process plus re-adoption).
Workers are upgraded before or with the hub: a hub sends a
fixed-output build only to a worker advertising `tribuchet-network`,
which workers older than it never do, even with `/dev/net/tun`, so
until an upgraded worker with `/dev/net/tun` registers every
fixed-output build waits.
The agents run at OOMScoreAdjust=-1000 and their builds at 0, which a
build cannot lower, so no agent is an OOM victim. Every agent and its
builds run in `system-tribuchet\x2dagent.slice`, whose RAM plus the
RAM zswap holds for it outside its charge stays at or below MemTotal
less `agentMemoryReserve` (`nix/agent-memory.nix`): a generator caps it
at A = MemTotal − reserve − S / 4096 × 256 at boot and at each
daemon-reload, and it swaps only through zswap, with writeback off, up
to S (`agentSwapPercent` of MemTotal); every other unit has
MemoryZSwapMax=0. `tribuchet-agent-swap` turns zswap on and then gives
the slice S; its stop takes S to 0 and only then turns zswap off, so
that S is never in force with zswap off. `tribuchet-agent-guard`,
bound to it, clamps the slice's swap at what it holds once the slice,
at its cap, has reclaimed next to nothing for 30 s, so that the kernel
OOM-kills a runaway as at S, and gives S back after the kill. A host
configuration that also sets these properties on the slice fails
evaluation.
The e2e test consumes the same module. macOS hosts use the
`darwinModules.default` nix-darwin module, which ships both services:
the hub adopts its listeners from launchd (`launch_activate_socket`,
the analogue of the socket-activated NixOS unit), the worker's
launchd daemon execs a stable symlink that activation flips before
restarting the daemon, and one socket-activated agent daemon per
build user runs the builds. Both hub and worker run as the
unprivileged `_tribuchet` user.
