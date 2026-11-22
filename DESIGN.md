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
and the prior; nothing else. Placement ranks by memory and cores only:
the running time, the OOM kills and where an expectation came from are
learned and logged at dispatch, for a replay of the journal to weigh,
and unused. A run with no running time (a worker can report none)
teaches no timing. Keys are never dropped. A build's expectation is
taken once, as it is first queued; a requeued build keeps it.

The history is the fold of the hub's build record (`hub/record.rs`), a
SQLite database at [placement] `record`, by default
`$XDG_STATE_HOME/tribuchet/record.sqlite` (`/var/lib/tribuchet` under
the NixOS module, its StateDirectory): state, not cache, as it is the
only copy. Its tables are appended to and never changed: `result`, a
row per worker result that carries usage (the "build usage" line's
fields, and whether the build declares `big-parallel`), `dispatch`, a
row per dispatch (the "dispatching build" line's values, unrounded),
and `taught`, a row per result by its rowid, whether its run teaches
worker speed (Worker speed), committed with the result. A result with
no `taught` row, in a record from before it or written by a hub that
predates it (a rollback), gains one from the dispatches as a hub that
writes the record takes it (below); a hub that only reads such a record
folds those results untaught, and a hub that predates `taught` ignores
it. The rowids `taught` names stay their results': the record refuses
a delete or update of a `result` row (triggers), and VACUUM renumbers
a table without an INTEGER PRIMARY KEY only past a deleted row. That
is the bundled SQLite's behaviour, not its documentation's promise
(rowids "may change"); a test VACUUMs a record and pins it. An
INTEGER PRIMARY KEY on `result` would change the column count every
hub inserts by.
Other processes may query it while the hub runs (`sqlite3 -readonly`):
it is in WAL mode, where readers and the one writer do not wait for each
other. As the hub starts, before it serves, it folds every `result`
row, in rowid order, into an empty history ("build record folded", with
the rows read and the keys and entries learned); after, each result is
learned and handed to the record in one step under the placement lock,
so the history is the fold of the record's rows and those queued to it.
A result row with no system (an imported build queued before its
journal began) teaches nothing.

One thread writes the record, all the rows waiting in one transaction,
the rows of one result handed over as one, so committed together;
handing rows to it never waits, so no runtime thread does the record's
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
CPU (release build, one run, 2026-11-08). Speed adds about 30 %: on
a record of 1,559,881 results and 1,561,097 dispatch rows (706 MB, 13
workers), the fold, reading each result's `taught` row by its rowid,
ran 27.8 billion user-space instructions, against 21.5 billion without
speed; peak RSS 29 MB. On synthetic records of 1,500,000 results it ran
34.7 billion with 20 workers and 53.3 billion with 60, the fit costing
O(workers^2) a taught run (speed.rs). Renewing the fit after every
taught run instead ran 39.4 billion on the record (42 % more) and 100.5
and 775.3 billion on the synthetic ones (2.9x and 14.5x). The fold runs
before the hub tells systemd it is ready, within the unit's default
90 s start timeout: 27.8 billion instructions fit in it at 0.31 billion
a second, and each result adds about 18,000. (Release build, perf on
one core, two runs each, 2026-11-22.) Giving each result of that record
its `taught` row, as a hub first takes it, ran 105.0 billion with the
fold of the day before, wrote 292 MB and grew the file 14 MB (83.0 and
93.7 billion on the synthetic ones); a take of a record with every row
ran 32.5 billion (one run, 2026-11-20). While it opens the record, the
hub asks systemd, at once and then every 10 s, to extend its start
timeout by 30 s (EXTEND_TIMEOUT_USEC), the later asks only if the open
has progressed since the last: a count its SQL advances (SQLite's
progress handler on its connections, every 1,000 VM steps, the fold's
reading of the rows included). A long migration or fold is not killed;
an open that hangs (a stuck filesystem, a syscall that never returns)
stops being extended and is killed at the start timeout, or 30 to 40 s
after its last progress (its last ask is at the first 10 s tick after
it) if later. In that first take of the snapshot, the longest stretch
without a progress callback is the migration's commit and checkpoint:
30 MB read and written and 3 fsyncs (strace); between callbacks the
hub ran at most 145 million user-space instructions (perf_event_open).
A migration that fails, as space for its sort runs out (SQLite's
temporary files, under `SQLITE_TMPDIR`, `TMPDIR` or `/var/tmp`), is
logged as an error and changes nothing: the hub folds and records as
it would, those results untaught until a hub takes the record again.

The record replaces `placement-history.json` ([placement] `history`),
which nothing reads now: a hub.toml that still sets `history` fails to
load (an unknown field). The file's runs were
not carried over: the farm's journal of 2026-10-29..11-08 was imported
into the record once instead, by a command removed since, before the
first hub that keeps a record started. Its rows are the record's first;
those of builds queued before that journal began have no system.

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

### Worker speed

Workers differ in how fast they run the same build, and a worker can
change. The history models them as uniformly related machines
(`hub/placement/speed.rs`): build b takes p_b / s_w on worker w, a time
of its own over a speed factor of the worker's, the same for every
build; 1 is the farm's typical worker. A speed per build and worker
(unrelated machines) would need runs of one key on every pair of
workers, which the record has for too few keys.

The factors are learned from runs whose dispatch was quiet. A run
teaches speed if it succeeded with running time and its dispatch was
quiet and not resumed. Quiet is tested once, when the build is
dispatched, on what the hub knows then: the expected cores of the other
builds committed to the worker at most a quarter of its cores, not what
they used. Builds dispatched to the worker later, while the run goes
on, are not seen: a busy worker's runs teach under load the test
missed, and it reads slower than it is. A dispatch is resumed when the
hub hands a worker a build it still holds, or when the worker answers
an assignment that it already holds the build (`Resumed`): its run may
have ended before the dispatch, under a load it did not see, and its
result arrives as if the run had started after it. On the record, 51
of the 611 results of resumed dispatches would otherwise have taught.
Taught from every run, contention reads as speed: on the replay below
the compared workers' factors spread from 0.37 to 2.43, against 0.90 to
1.18 taught from quiet runs. The rule assumes a decider that adds a
worker's load to its speed, which none does yet: until one does,
factors taught from every run predict running times better (1.79 % per
run on the replay below), as those times include the load the dispatch
did not see. The rule stays for what the factors are to be, a worker's
speed; whether it still costs prediction once a decider reads load is
an open question (Known limitations).

Each run that teaches is compared with each other worker's latest such
run of its key that came since this worker's previous run of it (ln t_a
- ln t_b = ln s_b - ln s_a, the build's own time cancelling): each pair
of runs is compared once, so one run counts in at most two comparisons
per other worker, with its latest run before and its next after. On
the record 5 of the 13 workers it names are compared; the others keep
factor 1: 7 ran only before dispatch rows recorded load, so none of
their runs is quiet, and one aarch64 worker shares no key with the
rest. The factors fit the comparisons by least squares,
with two departures, each chosen by a replay of the record (below):

- A comparison is clamped to within 0.5 in ln of the fit when it is made
  (`BOUND`). One comparison's deviation from the fit has a standard
  deviation of 2.0 but a median of 0.17: runs timed at a millisecond,
  and pnames that pool unlike inputs. The clamp is not a Huber
  M-estimate: a comparison is not revisited as the fit moves, so the
  factors depend on the order runs arrive in, which the fold keeps.
- Each pair of workers' comparisons decay with a window of 1,000
  (`WINDOW`), counted in comparisons of either of its workers: each
  comparison either takes part in weighs the pair's earlier ones by
  (1 - 1/1000)^(1/2), so a pair compared only with itself decays by
  1 - 1/1000 a comparison. A pair no longer compared, as of a worker
  retired, fades as its other worker is compared with others: decayed
  only when compared, it kept its weight, and in a cycle of pairs held
  a worker that changed at its old ratio (a retired worker's two pairs
  held one that doubled at 1.59x after 30,000 comparisons). A worker
  that is not compared, as when it is offline, or online with runs
  never quiet, ages none of its pairs itself, but the others'
  comparisons age them: it loses its factor, and relearns it when it is
  compared again. On the replay below, the worker that halves has no
  result between 34.05 h (its last before) and 41.46 h (its first
  after); the trace, sampled every 5,000 runs, reads it 2.05x at
  34.1 h, then 1.91, 1.68, 1.23 and 1.00x, and 0.95x at 37.0 h (0.94x
  at a window of 300; decayed only when compared, it kept 1.95x); back,
  1.54x at 43.6 h, 1.37 to 1.77x until 50.0 h, and 2.06x at 50.7 h,
  1,546 of its taught runs after it returned. A test pins it: a worker
  at 2x, away while two others are compared 15,000 rounds, reads within
  2 % of 1, and within 5 % of 2x again after 20 rounds with each. A
  pair ages by its own workers' comparisons, not the farm's: c and d,
  compared with each other once per 1,000 comparisons of others, keep
  their comparisons (aged by every comparison, c at 2x d read 1.73x).

A prior pulls each ln s toward 0 with the weight of 1.17 comparisons
(`PRIOR`): one comparison's variance over the prior's, a prior of 0.25
covering the compared workers' ln factors (0.90 to 1.18x: all within
0.17 of 0).
Clamped at `BOUND`, one comparison deviates from the fit with a standard
deviation of 0.281 on the replay below; 1.17 is (0.27 / 0.25)^2 =
1.1664, rounded, from 0.27, an earlier fit's deviation truncated; 1.22
and 1.32 replay alike. A worker never compared has factor 1,
and one compared a few times lies between 1 and what its comparisons
say. Factor 1 is the mean, in ln, of each group of workers connected
by comparisons, directly or through others, those no longer serving
included: their factors stay in the fit. Factors are relative within
a group only; a worker never compared is a group of one. The
tests pin what the prior and the window do: few comparisons read
between 1 and what they say, more the more there are, and one of 1.2x,
too small to be clamped, reads 1.12x (without a prior 1.2x); a worker that
doubles is relearned within 10 % after 3,000 comparisons, which a
window of 3,000 fails; and an unchanged worker through noisy runs stays
within e^0.03 of another, which a window of 300 fails (e^0.045). The
square root in the decay is the window's unit, a pair's own comparison
counting twice: without it a self-compared pair decays as at a window
of 500, which the steadiness test also fails. Only the replays chose
the values; `PRIOR`'s value is not pinned, as the replays read it alike
from 1.17 to 1.32.

The fit is renewed once per as many taught runs as there are workers:
between, comparisons are clamped around, and factors read from, the
last fit. Solving costs O(workers^3), so renewing it so costs
O(workers^2) a taught run; renewing it every run costs 42 % more on the
record and 14.5x on the 60-worker one (above).

A build's expectation keeps its median running seconds as they ran
(`Expected::secs`, the dispatch row's `expect_secs`), and adds those of
a worker of factor 1 (`Expected::unit_secs`): the median over its
level's succeeded runs of each one's running time times its worker's
factor. On worker w it is expected to run them over w's factor:

- `History::speed(worker) -> f64`: worker's factor, 1 if never compared;
- `Expected::secs_on(speed) -> Option<f64>`: the build's seconds on a
  worker of that factor, where a run of its level succeeded;
- `Placement::secs_on(job, worker) -> Option<f64>`: the two composed,
  for a decider weighing when a build would finish on each worker.

No decider reads them yet: the dispatch log line adds the worker's
`speed` and `expect_secs_on`, and "build record folded" lists every
worker's factor and comparisons, for the placement solver to come.
Contention is not in these figures: they are a quiet worker's, and a
decider that counts a worker's queue or load adds it.

Both are folded from the record. A result row's run is on the worker it
names, and teaches as its `taught` row says: the hub commits that row
with the result's own as it learns the run, so the fold reads the
decision the hub made, with no join. The decision is the build's, kept
from its latest dispatch (`Job::placed`) and cleared by a `Resumed`
answer. A result with no `taught` row gets one as a hub takes the
record: it is taught if its id's last `dispatch` row by `at_ms` at or
before it (a dispatch first at a tie) was quiet by its `load_cores`,
`expect_cores` and `cores`, and not `resumed`; `Resumed` answers were
not recorded then. On the record the migration teaches the same runs
the fold of the earlier join did: the replays below are figure for
figure alike.

The replays are of a snapshot of the farm hub's record (its last
result 2026-11-20 01:53 UTC, taken by a hub so that every result has
its `taught` row: the tool refuses a record that has not been), not
committed, by the replay tool
`hub/record/replay.rs`: an ignored test that predicts each run by the
fold before it is learned, with speed and without (its settings and
command in its header). Those marked (knobs) ran it with `PRIOR`,
`WINDOW` and `BOUND` read from the environment by a patch to
`speed.rs` and the tool, not committed (`knobs9.patch`, kept with this
change's review evidence, sha256
92d46ed1e554cacbb7342a299075024c36436392cec3e9a8dd4b69859de4df33),
which turns each into a `LazyLock` of `SPEED_PRIOR`, `SPEED_WINDOW` or
`SPEED_BOUND`; at the committed values it replays the same, figure for
figure. Some ran on the record before its migration,
folded by a join to the dispatches: the eleven rerun on the migrated
record replayed the same. From 2026-10-28 to 2026-11-20 less 2026-11-17
to 19 (a KVM queueing anomaly), 1,328,851 succeeded runs:

- Mean |ln error| per run falls 3.91 % (3.74 % on the whole record);
  runs within 2x of their prediction are 88.16 % against 88.12 %.
  Hour-weighted it rises 0.27 %: the factors fit the farm's many short
  builds, and one worker's hour-weighted error rises 1.40 %.
- With three workers' times divided by 1, 1.5 and 2, the fitted ratios
  to the first are 1.45 and 2.05, against 1.44 and 2.04 expected from
  their factors on the record, and the cut per run is 19.05 %.
- A worker whose times halve from 2026-11-10: relative to the workers
  that did not change, its factor reaches 1.92x in 0.3 h (1,526 of its
  taught runs) and 1.97x in 1.0 h; sampled every 5,000 runs after, it
  lies between 1.37x and 2.15x while it ran, and 0.95x to 2.05x at the
  samples from 34.1 to 43.6 h, around its results' gap from 34.05 to
  41.46 h, to the record's end, at 241.8 h, where it is 1.76x. The cut
  per run is 11.41 %.
- (knobs) Windows of 30, 100, 300, 1,000, 3,000 and 10,000, and none,
  cut 3.13, 3.72, 3.87, 3.91, 3.83, 3.71 and 3.24 % on the record, and
  10.59, 11.24, 11.39, 11.41, 11.28, 10.99 and 6.36 % with the worker
  that halves (with no window it is 1.46x at 241.8 h). Its readings
  while it ran, from 1 h on, spread with a standard deviation in ln of
  0.221, 0.159, 0.087, 0.065, 0.054 and 0.079, the lowest 0.63, 0.70,
  1.24, 1.37, 1.50 and 1.21x; at 3,000 and more it reads 1.49 and
  1.16x at 0.3 h. 1,000 cuts most, and of the windows that relearn in
  under an hour spreads least.
- (knobs) Plain least squares, no clamp and no window, cuts 3.29 %; a
  clamp of 1 instead of 0.5 cuts 3.80 %. A `PRIOR` of 1.22 or 1.32
  replays as 1.17 does: the same figures and factors to four places.
- Factors taught from every run, against quiet runs' only: scored on
  the quiet runs, every run's predict them 0.98 % better per run and
  within 2x 91.76 % against 91.37 %, and hour-weighted 0.07 % worse;
  on all runs 1.79 % better per run, within 2x 88.83 % against 88.16 %,
  and hour-weighted 0.79 % better. They read load as speed: they spread
  from 0.37 to 2.43, against 0.90 to 1.18 (`a_loaded_worker_is_not_read_as_slow`
  is the mechanism). The rule stays for the factors' meaning, at that
  cost in prediction while no decider adds load. (With this round's
  window the comparison turned: with pairs decayed only when compared,
  at 300, the quiet rule's predicted quiet runs 3.47 % better.)

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

## Known limitations (MVP)

* Speed's quiet test reads the load at dispatch only: builds dispatched
  to the worker later, while the run goes on, are not seen (Worker
  speed). An open question for a later change: test the load over the
  run's whole lifetime, as the result arrives.
* Speed's quiet rule assumes a decider that adds a worker's load to its
  factor; until one does, factors taught from every run predict running
  times better on the replay (Worker speed). An open question for when a
  decider reads load: rerun that comparison with the decider's
  prediction, load included, and keep the rule only if it still wins.
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
  its own running-time timeout ends it (`build-timeout-secs`, 24 h by
  default, or its client's lesser one); the next oldest then leads. The
  wait is bounded, since a build started later never leads over an
  older one, but can reach that timeout once for each older parallel
  build.
* The frozen builds' memory rule is a margin, not a bound: a leader that
  grows faster than one 200 ms sample can still fill a capped slice.
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
Every agent and its builds run in `system-tribuchet\x2dagent.slice`,
where a host may cap their memory; the worker reports the slice's
memory.max, when lower than MemTotal, as its memory.
The e2e test consumes the same module. macOS hosts use the
`darwinModules.default` nix-darwin module, which ships both services:
the hub adopts its listeners from launchd (`launch_activate_socket`,
the analogue of the socket-activated NixOS unit), the worker's
launchd daemon execs a stable symlink that activation flips before
restarting the daemon, and one socket-activated agent daemon per
build user runs the builds. Both hub and worker run as the
unprivileged `_tribuchet` user.
