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
   network. A client whose Nix cannot build it, every fixed-output
   build's among them, waits for a worker, told why, while a known
   worker (one that registered and was not decommissioned since) serves
   it; if none does, it is refused once no worker is expected for it
   (`hub/known.rs`). Otherwise submitters block and Nix's max-jobs
   bounds parallelism.
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
* A gap, fixable: a fixed-output derivation that passes `outputHash`
  as a file (`passAsFile`) is not told fixed-output by the rule below.
  Its builder gets `outputHashPath`, a file in the build's tmp dir,
  which attach does not read yet, so it sees no declared hash: the
  build gets no network and no log line, and may be declined like any
  other. attach could read the hash from that file under
  `top_tmp_dir/build`.
* A build has network iff it is fixed-output, told from the path Nix
  chose for its one output, `out`, never from the environment it sets
  for itself (`build_json.rs`): fixed-output where that path is the
  store path of the content hash the derivation declares. An `__impure`
  one gets none. A worker gives it a network of its own only on Linux
  with `/dev/net/tun`, so no current macOS worker advertises
  `tribuchet-network` (one refuses such a build), and the hub answers
  such a build as step 2 says.

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
hub commits what is queued to its build record and exits, and the
replacement reconstructs its state from the edges and that record. Workers re-register
and announce the dedupe keys of builds they still hold (running, or
finished but undelivered); attach clients reconnect and resubmit the
identical request, whose deterministic dedupe key routes it back to the
worker holding the build, which resumes (or just re-delivers the
finished result) instead of building again.

Worker redeploys are plain unit restarts: builds run under the
per-uid agent services, which hold the log and exit status until a
worker collects them, and resume state is persisted in the build
dir of each run (`builds/<id>.<ns>`, one per run of a build, which no
other run removes but the launch that displaces its result). The replacement worker re-adopts running builds
from their agents, supervises them to completion and redelivers finished
results. A build leases a slot and its agent together, only while fewer
than min(`max-jobs`, reachable agents) leases are held; a build that runs
already when it starts (adopted, held, an orphan's cleanup) leases its
own agent past that, until it ends, and takes a slot
from the pool even when that agent is outside it. A running build whose
agent does not answer keeps its key and its lease until the agent
answers, and its clients are told it waits. A build whose agent did not
answer its start keeps a lease with no agent (its agent set down), its
clients told, until another agent is free or the build is cancelled.
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
genuine loss (a run that started, its worker back without it) fails
it, and the thirteenth loss in transport (every other loss) hands it
back to its clients' Nix (`hub/losses.rs`).
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

Each failure of the hub's own nix-daemon is counted by class c and
request, as the build counts them; short of θ(c) the build waits, its
clients told once (`hub/relay/daemon.rs`). At θ(c) each client whose
Nix builds it is declined. The others fail, naming the cause, if c is a
fact about the hub (θ 1, the hub's verdict, `hub/losses.rs`); otherwise
they wait on, as below θ:

| class | θ |
|---|---|
| a refusal of the request, a handshake the hub cannot speak, a new connection closed before the handshake as the daemon's journal says it refuses the hub's user | 1 |
| a message the hub cannot read | 2 |
| a request the daemon drops as its journal says a worker crashed | 3 |
| anything else: a drop or close with no such word in the journal, no answer within a minute, a silent daemon's included | ∞ |

An input the hub's store lacks (its daemon knows no such path) is no
verdict either: the clients whose Nix builds it are declined, and the
build is held for the others until a client of its key joins, whose
Nix had that input as it submitted (`hub/state/hold.rs`).

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
registers again. Memory has no such floor: a worker that
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
and the prior; nothing else. Placement ranks workers by memory and cores
only. The running time goes into the path of the builds a key's builds
are learned to follow (Order, below), not into the key's own. The count
of a
key's OOM kills and where an expectation came from are logged at
dispatch; nothing ranks by them.
A run with no running time (a worker can report none) teaches no timing.
Keys are never dropped. A build's expectation is taken once, as it is
first queued; a requeued build keeps it.

The history is the fold of the hub's build record (`hub/record.rs`), a
SQLite database at [placement] `record`, by default
`$XDG_STATE_HOME/tribuchet/record.sqlite` (`/var/lib/tribuchet` under
the NixOS module, its StateDirectory): state, not cache, as it is the
only copy. It has two tables, appended to and never changed: `result`,
a row per worker result that carries usage (the "build usage" line's
fields, and whether the build declares `big-parallel`), and `dispatch`,
a row per dispatch (the "dispatching build" line's values, unrounded).
Other processes may query it while the hub runs (`sqlite3 -readonly`):
it is in WAL mode, where readers and the one writer do not wait for each
other. As the hub starts, before it serves, it folds every `result`
row, in rowid order, into an empty history ("build record folded", with
the rows read and the keys and entries learned); after, each result is
learned and handed to the record in one step under the placement lock,
so the history is the fold of the record's rows and those queued to it.
A result row with no system (an imported build queued before its
journal began) teaches nothing.

One thread writes the record, all the rows waiting in one transaction;
handing a row to it never waits, so no runtime thread does the record's
file I/O. Each commit is durable (`synchronous=FULL`): a crash loses the
rows handed over and not yet committed, never a committed row nor part
of one. A commit that fails (a full disk, an I/O error) is logged as an
error and its rows kept, to be committed with the next rows; at SIGTERM,
or when its servers fail, the hub stops handing rows over and exits once
the writer has committed what it holds, or logged as lost what it could
not. The metrics show `tribuchet_hub_record_writing` (0 where the hub
records nothing), `tribuchet_hub_record_uncommitted_rows` and
`tribuchet_hub_record_failed_commits_total`. The retry has no bound:
while commits fail, the rows held, and each attempt's work, grow with
every row learned, so alert on `tribuchet_hub_record_uncommitted_rows`. One hub writes a record: it
holds `record.sqlite.lock` while it runs, and a second hub, finding it
held, folds what it reads and records nothing, logged as an error. A
record that is not a SQLite database, or not one of a schema the hub
knows (`user_version` 1 and the two tables), leaves the hub with an
empty history, recording nothing, logged as an error; the file is left
byte for byte as it is. A missing record, and its directory, are
created. The hub serves either way: placement then falls back on its
priors, and builds are not held for the record. The fold's cost at
start is measured, not bounded: 338,559 rows, 116 MB, took 249 ms of
CPU (release build, one run, 2026-11-08).

The record replaces `placement-history.json` ([placement] `history`),
which nothing reads now: a hub.toml that still sets `history` fails to
load (an unknown field). The file's runs were
not carried over: the farm's journal of 2026-10-29..11-08 was imported
into the record once instead, by a command removed since, before the
first hub that keeps a record started. Its rows are the record's first;
those of builds queued before that journal began have no system.

The worker that asks takes the first queued build it can, in the
queue's order (Order, below), unless
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

### Order

The queue is kept in an order, stated in `hub/placement/objective.rs`,
and a worker takes the first build in it it can (above). A waiting build
b's urgency is `wait(b) + path(b)`:

- `path(b)`: `below(key(b))`, the longest chain of expected running
  seconds learned to depend on its key: the upward rank of HEFT
  (Topcuoglu, Hariri and Wu, IEEE TPDS 13(3), 2002) and HLFET's static
  level, with expected running seconds as costs, less b's own seconds.
  It is taken with the expectation, as b is first queued.
- `wait(b)`: seconds since it last entered the queue; a requeued build
  enters it again and its wait starts over.

The most urgent goes first; equal ones in the order they came. As
`wait(b) = now - queued(b)`, `urgency(b) = now - due(b)` with
`due(b) = queued(b) - path(b)`: the order is by due, earliest first, and
the same at every instant. So nothing is ordered at a take: a build is
put into the queue at its place by due as it is queued or requeued
(`objective::enqueue`, a binary search), and the take scans from the
front, as before. With every path equal the order is the one builds came
in, FIFO. Each dispatch logs the build's `path` and `waited` beside
`expect_below`.

*Why not its own seconds.* With b's own seconds in its path, builds
nothing follows go longest first, and in a class few workers serve the
shorter wait out the longer that came after them. Replayed on the hub's
trace of 2026-11-16..19 with the hub's own estimates (8 KVM agents a
host, summed batch makespan against FIFO's 1,097.5 h, 95% interval by
bootstrap over batches), own seconds plus `below` was +0.17% (+0.10% to
+0.24%), about 88% of it in KVM batches (1.6 h of the 1.82 h, +0.30%
there): the KVM builds' summed wait
grew from 23.6 h to 30.5 h, each waiting behind KVM builds of longer own
seconds that came after it. `below` alone was -0.02% (-0.05% to +0.01%;
-0.09%, -0.19% to +0.00%, with memory admission), no measured gain or
loss on that trace, and no batch more than 10% + 60 s slower; on
synthetic graphs where chains compete with other work it kept the gain
of the whole rank (in the commit message). It still makes KVM builds
wait longer in sum, if not finish their batches later: 24.61 h against
FIFO's 23.62 h, and 41.11 h against 36.50 h with memory admission
(where the other builds wait 41.98 h against 44.03 h).

*Starvation bound.* A build c queued after b goes before it only if it
came less than `path(c) - path(b)` seconds after b
(`a_later_build_passes_only_within_its_path_difference`). No stream of
later builds keeps b waiting: one that arrives more than the longest
path the hub has learned after b goes behind it. The bound is as long as
the longest `below` learned, which pname keys joining chains
overestimate; the hub exports by system the `below` of each key
something was learned to follow, a minute to a day
(`tribuchet_hub_below_seconds`), so that it can be watched. A build no asking
worker admits is outside this: admission skips a build, it does not
order it, and under memory admission a large build can be skipped while
smaller ones behind it fill a worker; that wait is admission's.

*Share, logged, not in the order.* Each dispatch logs, beside `path`,
`waited` and `expect_below`, the build's `share`: the least, over the
clients still waiting for it, of the client's dominant share (Dominant
Resource Fairness, Ghodsi et al., NSDI 2011), the larger of the expected
cores and the expected memory that the builds placed and not settled
hold for the client, over the connected workers' cores and memory (a
worker that reports none counts none). A placed build holds for the
clients waiting for it as it is placed, each once, until it settles or
its worker's session closes (kept as builds are placed and settle,
`objective::Charges`, not recomputed). The clients no request names are
one client, unknown to the hub, charged as one. Share is not in the
order because nix-grpc-store, whose builder opens a nix-daemon
connection per build, makes each build its own client, holding nothing
while it waits: a client the hub names by its connection to the hub's
daemon, holding a running build, would wait behind every one of them
without bound. Share enters the order once nix-grpc-store forwards each
build's originating client. nix-grpc-store also decides fairness itself
(its scheduler places the next build for the client with the fewest
placed, 37094d0); with the hub the one admission and placement owner,
that layer's fairness should then pass builds through.

*What the hub learns of dependants* (`hub/placement/downstream.rs`). Nix
hands the hub a build only once its inputs exist, so a waiting build's
dependants are never queued beside it. A run that succeeds lists its
inputs (build.json's `inputPaths`, its closure). An input that is an
output of one of a key's last 8 runs here was made by that key, of its
own system; each such key is then followed by this build's expected
seconds and the chain that follows this build's key: `below(k) = max
over builds d, finished since k last ran, with an input made by k, of
secs(d) + below(key(d))`, where the chain through d is one of keys none
of which is k; one that would come back to k counts d alone. While none
has finished since k's last run, what the runs before left stands:
`below` follows a key's latest run, not the longest any of its runs
started (a key that runs often, as `source` does, keeps its latest).
Keys are pnames and inputs are closures, so keys can name each other;
every `below` is the learned seconds of a chain of distinct keys, and a
cycle cannot add to it. A chain of n builds is learned over at most n -
1 runs of it. Below a key no run has followed, `below` is 0, and so is
the path of its builds. Pname keys also
join chains no one build is on: if one build of b follows a and another
build of b is followed by c, `below(a)` counts b then c, longer than any
chain one run went down. Never learned, and counted only as a run
without a producer here (`tribuchet_hub_dependants_unmatched_total`,
against `_matched_total`): a producer whose output path is not the one
its dependants list (floating content-addressed, whose scratch path is
not its final one), a producer this hub did not run (substituted, or
built elsewhere), and one older than its key's last 8 runs. It is not in
the record: a restarted hub learns it anew. It keeps the output paths of
each key's last 8 runs and a chain of keys for each.

*Who the client is.* A request carries `client`: build.json's, where the
Nix writing it says, else the nix-daemon connection that runs attach (Nix
starts an external builder from the process serving the connection that
asked for the build), named by boot, pid and start time. A client of the
hub's daemon is then a client here; the hub logs it as `owner` on the
build's queueing line. No Nix writes build.json's field yet. It is not
part of the build: a second client joins the first's build, and a
worker's assignment does not carry it (it is built field by field from
the request, this left out). The clients not named are one client,
charged as one: an attach older than the field, during a mixed rollout,
and an attach on a hub without /proc (darwin), or whose /proc hides its
parent's (hidepid). An attach whose parent exited before it read it is
named, by its new parent (init, or a subreaper): it is charged as that
client, shared with every other such attach.

*Keys are pnames.* `below` is kept per history key, as the history keeps
seconds, so builds of one pname share it: every fetchFromGitHub source is
named `source`. A build is known as its key's producer by its outputs,
of every output name, not by the input's name.

The replays and synthetic runs behind this order, by models kept outside
this repository, are recorded in the message of the commit that added it.

## CPU arbitration

The worker's CPU arbiter (`worker/arbiter.rs`) is always on; nothing
turns it off: where parallel builds each fill every CPU, it finishes
the leader, and the builds on average, earlier, on less CPU. While the
oldest parallel build (by its start, persisted with it, so the same
after a worker restart; ties by key) keeps every CPU busy, the other
parallel builds are frozen. A frozen build is thawed at the first
200 ms sample at which the leader leaves a CPU spare or has ended, when
the memory rule below needs the room, when its lease ends, or when the
freeze is not reached within 5 s; nothing bounds how long it may stay
frozen otherwise. An earlier bound (no build frozen more than 10 s in
any 30 s, so that a follower ran at least two thirds of the time) is
not kept, by decision of 2026-11-07. Measured on synthetic workloads
before it was added (2026-10-27), arbitration finished builds 7.1-8.2%
earlier on average and the oldest 33-34% earlier; re-measured with it,
the mean gain was gone and the oldest finished 18-21% earlier, and a
later run with it on a 4-vCPU mix of small builds found no measurable
benefit, a harness that could not test oversubscription. The workloads
differed between those runs, so how much of the loss was the bound's
is not cleanly attributed. Builds stay frozen only while
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

## Dynamic-derivation producers

A producer is a derivation requiring `builder-rpc-v0`: its builder gets
a socket to the client's store (`NIX_REMOTE`), through which it adds
objects and submits its outputs while it runs. Under Tribuchet its
builder runs on a worker, and attach carries each connection on that
socket to the client's. The code cites these labels: DD for the design,
DT for its tests, apart from SEMANTICS-7's D and U.

* **DD1, detection.** A build is a producer iff its
  requiredSystemFeatures has `builder-rpc-v0` (`proto::producer`, which
  attach, hub and worker all ask). `NIX_REMOTE` never decides: a
  recursive-nix build has one too.
* **DD2, sole judge.** The client's Nix judges every operation a producer
  makes; Tribuchet carries each connection's bytes unchanged, in order,
  both ways, and parses none.
* **DD3, sandbox socket.** A producer's `NIX_REMOTE` must be
  `unix://<tmpDirInSandbox>/<name>`, as Nix sets it; anything else is
  refused. Only a producer gets a listening socket there, bound by the
  agent as the builder's uid, mode 0600.
* **DD4, a connection each.** attach splices each accepted connection to
  one of its own to the client's socket, by a task of its own, both
  ways, half-close as half-close: a stalled connection delays no other.
* **DD5, client socket.** build.json's tmpDir (topTmpDir/build) joined
  with `NIX_REMOTE`'s basename; never a constant.
* **DD6, no outputs.** A producer's run returns no scratch outputs and
  claims no scratch path: what it builds it submits to the client's Nix.
* **DD7, attach exits last.** At a worker's exit code, attach splices each
  connection still queued on the listener and exits once every splice
  ended, since Nix checks the submissions once attach exits. At the
  hub's decline (222) the run was lost and may still run: attach aborts
  its splices instead, so nothing of it reaches the client after.
* **DD8, RelayStarted.** Before a run's first byte reaches the client,
  attach sends RelayStarted naming a handoff token of the run it holds
  and waits for the hub's acknowledgement naming it, or for the run's end
  (a worker's exit code), after which no fresh run follows. The hub
  records and acknowledges only a token of the run current now (its
  fresh start's or a resume's), so a splice of a run gone never marks
  another: a run lost or requeued is current no more. A resubmission
  after a hub restart carries `relay_started`, applied before any take
  can see the build. A fresh run closes attach's gate: its bytes wait
  for an acknowledgement naming it.
* **DD8a, one farm run behind one attach.** Once RelayStarted was
  acknowledged, a loss of the run is never followed by a fresh run; a
  resume, the same run, goes on. The hub answers as `hub_cannot` does: a
  client whose build.json says `canBuildLocally` is declined (222), and
  its Nix rebuilds the producer in a fresh builder; every other client
  fails, told why. The acknowledgement and every loss take one lock: an
  acknowledgement first, and the loss settles the build so; a loss first
  (or a requeue), and the lost run's tokens are acknowledged no more. The
  loss is decided at three points: a loss of a started run its worker
  says is gone, a fresh take of the build, and a worker staging afresh a
  build handed it by key. A loss before the first byte follows the loss
  policy. A fixed-output producer is never declined, so its
  relayed loss fails the build (DD1b). This is a policy, not Nix's: a
  rerun behind the same client socket would find the lost run's added
  objects valid, and a second submission of an output is refused.
  Whether a fresh run before the first byte is worth RelayStarted (its
  gate, acknowledgements and token lists), rather than declining every
  producer whose started run is lost, is unmeasured: producers were not
  deployed when it was built. The fleet's record then, over 21.6 days,
  had 290 of 1,560,434 builds (0.019%) dispatched afresh after a loss,
  an upper bound that counts losses before Started too. Re-measure it
  once producers run, on the hub's record (producers dispatched afresh
  after a loss, of all producers):
  `SELECT count(*), sum(n > 1) FROM (SELECT id, count(*) AS n FROM
  dispatch WHERE resumed = 0 AND ',' || features || ',' LIKE
  '%,builder-rpc-v0,%' GROUP BY id);`
* **DD9, identity includes the client.** A producer's request carries
  `attach_id`, 16 random bytes per attach process kept across its
  reconnects, which is in the dedupe key (`relay_started`, drvPath and
  canLocal are not): an attach reconnecting rejoins its own run, another
  goal never joins it. A producer's request without one is refused.
* **DD10, listener custody.** The listener goes from the agent to the
  worker to attach, every holder but the last closing its copy; it is
  close-on-exec, so neither the setup stage nor the builder holds it. The
  worker records `handed_off` in the run's dir only once the send
  succeeded. It expects the run before it says Started, so attach's
  request waits for the listener however long the agent takes to start
  the builder. Unfetched `HANDOFF_DEADLINE` (60 s) after the StartReply,
  or found by a restarted worker without `handed_off`, the run is killed
  and reported `handoff_expired` (as is a producer on a worker serving no
  handoff socket): a loss before the first byte, in transport (how the
  farm moved it, never counted against the run nor isolating its key).
  A resume retoken gives a listener not yet handed off its deadline
  again. A listener attach fails to fetch is such a loss, not a failure:
  attach goes on. RelayReady{fresh} of another run makes attach close the
  listener it holds, abort its splices and close its gate (none carried a
  byte, by DD8: a fresh run follows only a loss before any
  acknowledgement); one naming a token of the run held changes nothing.
  A client joining a producer in flight (its attach reconnecting) is
  replayed the latest fresh RelayReady and each resume's after it, as
  published, none of a run a fresh start replaced.
* **DD11, handoff authority.** The worker hands a run's listener, once,
  only to a request with that run's handoff token: 16 random bytes per
  dispatch, which the hub sends the worker in the assignment and the
  build's attach in RelayReady, and never logs. A resume retokens the
  run. An empty token names no run.
* **DD12, routing.** Only a worker serving the handoff socket advertises
  `builder-rpc-v0`, for its native system. Set that socket only on the
  hub's host, where attach runs; the module then passes attach its path.
  Where attach has none, it declines a producer (222) whose Nix can build
  it or does not say. A producer placed on another worker is never
  fetched: attach is refused, "no such run on the local worker", and the
  run expires (DD10). That worker is not gated on its hub's version:
  on the hub's host both come from one configuration, so one predating
  producers meets an advertising worker only within a switch.
* **DD1b, fixed-output producers.** Nix lets a fixed-output derivation be
  a producer; Tribuchet builds it as a fixed-output build with the
  socket, refusing nothing of its own. Stock Nix completes none (an
  object named as the output waits on that output's path; one named
  otherwise is refused), and Tribuchet answers as stock Nix does.
* **R1, drvPath.** build.json's drvPath, where Nix writes it, goes to the
  `queueing build` log line and the record's `drv_path` table, and to
  nothing the hub decides.
* **OQ1, a relayed loss for a client that cannot build it.** (c): attach
  refuses, before submitting anything, a producer whose build.json says
  `canBuildLocally: false`. (a) for a client whose Nix does not say, at a
  relayed loss only: the hub takes it for one that cannot there, so a
  loss after the first byte fails the build, told why. Everywhere else
  (admission, a loss before the first byte) unsaid is taken as can, as
  for any build without network.

Their tests are labelled in their doc comments: DT1 and DT2 (DD4), DT3
(DD7), DT4 (DD8), DT7 (DD9, DD6), DT8 (DD11), DT9 (DD10's record), DT10 (DD6), DT11
(DD5), DT12 (DD1, DD3, DD12), DT13 (DD3's mode), DT14 (DD10's copies), DT15 (DD10's
deadline), DT16 (DD10's fresh), DT17 and DT20 (DD8a), DT19 (R1).

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
* A producer's attach reads no hub event while it fetches a run's
  listener. A fetch that comes before the agent started the builder
  waits on the worker until the listener arrives or the run ends, a wait
  bounded by the worker's progress, not by a deadline of its own.
* A freeze still counts against wall-clock timers the worker does not
  own. A build's own (test drivers, watchdogs): one whose tests time
  out by the wall clock fails if it is frozen longer than their slack.
  A frozen fixed-output build's connections: the remote side's send or
  idle timeout, and presto-pasta's reclaim of a flow with no traffic
  either way for 3 min (a stalled transfer keeps its flow alive with
  zero-window probes, so the remote's send timeout is the likelier
  end). A Nix client's `timeout` and `max-silent-time` where Nix is
  not patched to leave them to the external builder: such a client
  counts both by the wall clock, and a frozen build writes no log, so
  its max-silent-time can end the attach during a freeze, and the
  clientless rule may then give the build up. Each build's frozen time
  is recorded with its exit code (`frozen_ms` and `exit_code` on the
  hub's `build usage` line), so the failure rate of frozen builds is
  measured on the farm.
* With no bound on frozen time, a follower waits on the parallel
  builds older than it. A leader that hangs while spinning on every
  CPU holds every younger parallel build on its worker frozen until
  its running-time limit ends it: its client's `timeout`, or the
  worker's `build-timeout-secs` if set (none by default), or, once every
  client of it is gone, the clientless rule giving it up for a build
  with a client that wants its room; the next oldest then leads. A
  build started later never leads over an older one, but the wait can
  reach that limit once for each older parallel build, and is unbounded
  for a leader with neither a limit nor a client while no build with a
  client wants its room.
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
  under git hashing is not told fixed-output, and gets none; attach
  says so in its log, where Nix reports the fetch or hash mismatch.
* Attach declines every build in a diverted store (`realStoreDir`
  other than `storeDir`) before looking at it, fixed-output ones
  included, though a fixed-output build is otherwise never declined:
  a client Nix that can build locally then builds it in its host's
  network.
* Input chunks are verified against the hub's manifest and the daemon
  checks the NAR hash the hub declared, but both come from the hub: the
  worker trusts the mTLS-authenticated hub for input content.
* With `externalBuilders.dynamic`, the hub's nix.conf fragment lists
  only the systems a known worker serves, and nix.conf lists none of
  its own: for a system no known worker serves, Nix never asks the
  hub, and the hub host's own nix-daemon builds a fixed-output build
  for it, in the host's network. A known system stays listed while its
  workers are away, so its builds go to the hub, where a fixed-output
  one waits for a worker. No VM test runs a hub in dynamic mode: only
  unit tests cover the fragment's rendering, writing and startup.

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
builds run in `system-tribuchet\x2dagent.slice`, where a host may cap
their memory; the worker reports the slice's memory.max, when lower than
MemTotal, as its memory.
The e2e test consumes the same module. macOS hosts use the
`darwinModules.default` nix-darwin module, which ships both services:
the hub adopts its listeners from launchd (`launch_activate_socket`,
the analogue of the socket-activated NixOS unit), the worker's
launchd daemon execs a stable symlink that activation flips before
restarting the daemon, and one socket-activated agent daemon per
build user runs the builds. Both hub and worker run as the
unprivileged `_tribuchet` user.
