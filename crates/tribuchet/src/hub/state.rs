//! In-memory hub state: replay buffers, the job queue, worker capabilities.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, mpsc};
use tonic::Status;

use super::Loss;
use super::chunkcache::ChunkCache;
use super::losses;
use super::metrics::Metrics;
use super::placement::Placement;
use super::relay::{DAEMON_ANSWER, DaemonEvidence};
use crate::config::NixConfig;
use crate::fsutil::io_ctx;

use crate::proto::{AttachEvent, BuildRequest, attach_event};

#[path = "state/clientless.rs"]
mod clientless;
#[path = "state/inflight.rs"]
mod inflight;
#[path = "state/replay.rs"]
mod replay;

pub(super) use clientless::Dispatched;
#[cfg(test)]
pub(super) use clientless::FLOOR;
pub(super) use inflight::{Inflight, Listing};
pub(super) use replay::{KeptOutputs, Replay, Wait};

type EventTx = mpsc::Sender<Result<AttachEvent, Status>>;

/// Cap on the replay buffer of one build. Without it a worker that
/// streams chunks forever grows root-hub memory without bound.
const MAX_REPLAY_BYTES: usize = 256 * 1024 * 1024;

/// Per-subscriber channel headroom beyond the buffered backlog. An
/// attach client that falls this far behind gets no live events until
/// it has room again, or catches up once its build ends (replay.rs),
/// instead of the hub buffering the whole build a second time for it.
const SUB_CHANNEL_SLACK: usize = 1024;

pub(super) struct Job {
    pub(super) id: String,
    pub(super) key: String,
    pub(super) req: BuildRequest,
    /// The client's zstd-packed build tmp dir, buffered so redispatch can
    /// resend it without another round-trip to the client.
    pub(super) tmp_dir_pack: Arc<Vec<u8>>,
    /// requiredSystemFeatures; only workers advertising them get the job.
    pub(super) features: Vec<String>,
    pub(super) replay: Arc<Replay>,
    /// Dedupe registration, dropped on cancel, when given up, or with
    /// the job.
    pub(super) listing: Arc<StdMutex<Option<Listing>>>,
    /// Genuine losses (hub/losses.rs): capped so a crash-looping
    /// build cannot bounce forever.
    pub(super) attempts: u32,
    /// Losses on a resume (hub/losses.rs): capped so a worker that
    /// loses every resume cannot keep the build forever.
    pub(super) resumed_losses: u32,
    /// When the job last entered the queue, for the queue-wait histogram.
    pub(super) queued_at: Instant,
    /// The loss of the build with its worker's session, which the next
    /// dispatch settles: a resume by that worker clears it; a
    /// start-over, or an assignment the hub could not hand over, counts
    /// it as genuine; that worker's session dying before either counts
    /// it as a loss on a resume. Until the loss's grace ends, or the
    /// worker is back, the build is kept for that worker (Loss::awaited).
    pub(super) loss: Option<Loss>,
}

impl Job {
    /// Stop deduping new submissions onto this job.
    pub(super) fn unlist(&self) {
        drop(self.listing.lock().unwrap().take());
    }

    /// Whether no fresh take may have it: a worker's key holds it, or its
    /// loss keeps it for a worker still awaited.
    fn kept(&self, workers: &HashMap<u64, WorkerCaps>, holding: &HashMap<u64, Holding>) -> bool {
        holding.values().any(|h| h.holds(&self.key))
            || (self.loss.as_ref()).is_some_and(|r| r.awaited(workers, holding))
    }
}

pub(super) enum Servability {
    Now,
    ExpectedBy(Instant),
    Never,
}

const DAEMON_SOCKET: &str = "/nix/var/nix/daemon-socket/socket";

pub(super) struct HubState {
    pub(super) queue: Mutex<VecDeque<Job>>,
    pub(super) inflight: Arc<StdMutex<Inflight>>,
    pub(super) notify: Notify,
    /// Pooled connections to the local nix-daemon (path metadata
    /// queries); jobs are frequent enough that per-job handshakes
    /// would add up.
    pub(super) daemon_pool: harmonia_store_remote::ConnectionPool,
    /// What the hub reads of the daemon to tell whether waiting clears
    /// what it does (its journal).
    pub(super) daemon_evidence: DaemonEvidence,
    /// Build id -> its dispatch, while supervised (clientless.rs).
    pub(super) dispatched: StdMutex<HashMap<String, Dispatched>>,
    /// The clientless decider runs, from the first dispatch on.
    deciding: atomic::AtomicBool,
    /// Connected workers' capabilities, keyed by a per-connection id;
    /// submissions no worker can serve fail fast instead of queueing
    /// forever.
    pub(super) worker_caps: StdMutex<HashMap<u64, WorkerCaps>>,
    pub(super) next_worker_id: atomic::AtomicU64,
    /// How long a build waits for a platform expected back (hub
    /// restart, worker reconnect). Never-seen platforms decline at once.
    pub(super) worker_grace: Duration,
    /// Hub start time and the capabilities of workers whose session
    /// ended: together they tell `expected_deadline` which platforms are
    /// still worth waiting for after a restart or a worker drop.
    pub(super) started_at: Instant,
    pub(super) departed: StdMutex<Vec<(WorkerCaps, Instant)>>,
    /// worker id -> the builds it holds from before (Holding).
    pub(super) holding: StdMutex<HashMap<u64, Holding>>,
    /// Woken when worker capabilities change so waiting submissions
    /// re-check servability without polling.
    pub(super) caps_changed: Notify,
    /// Build lifecycle counters scraped by the metrics endpoint.
    pub(super) metrics: Metrics,
    /// When set, the connected-worker set is mirrored to this nix.conf
    /// fragment on every register/deregister.
    pub(super) nix_config: Option<NixConfig>,
    /// The fragment's renderings, numbered in worker_caps order, and the
    /// newest written (write_newest).
    nix_rendered: atomic::AtomicU64,
    nix_written: Arc<StdMutex<u64>>,
    pub(super) chunks: Arc<ChunkCache>,
    /// Each worker session's admission (placement.rs).
    pub(super) placement: StdMutex<Placement>,
}

/// The builds a worker session holds from before, by dedupe key: the
/// keys it listed last (Register's, then each Heartbeat's
/// resumable_keys), after `beats` heartbeats, less those a dispatch took
/// since: each is honoured once. A key a later heartbeat leaves out is
/// no longer held, which `unlisted` tells a resumed build.
#[derive(Default)]
pub(super) struct Holding {
    beats: u64,
    keys: HashSet<String>,
    taken: HashSet<String>,
}

impl Holding {
    pub(super) fn holds(&self, key: &str) -> bool {
        self.keys.contains(key) && !self.taken.contains(key)
    }
}

#[derive(Clone)]
pub(super) struct WorkerCaps {
    /// Registered worker name, listed on the metrics port's /workers page.
    pub(super) name: String,
    pub(super) connected_at: Instant,
    /// system -> features the worker honors for it
    pub(super) systems: HashMap<String, HashSet<String>>,
    /// Advisory concurrent-build capacity, summed to size the
    /// generated nix.conf max-jobs.
    pub(super) max_jobs: u32,
}

/// nix.conf fragment for the connected workers: external-builders over
/// every served system, and max-jobs at the oversubscribed, capped
/// aggregate capacity. max-jobs is omitted with no workers, leaving the
/// base nix.conf default to govern local fallback builds.
fn render_nix_config(caps: &HashMap<u64, WorkerCaps>, cfg: &NixConfig) -> String {
    let systems: BTreeSet<&str> = caps
        .values()
        .flat_map(|c| c.systems.keys().map(String::as_str))
        .collect();
    // System names are peer-supplied; Rust Debug is not valid JSON.
    let builders = serde_json::json!([{
        "systems": systems,
        "program": cfg.attach_program.to_string_lossy(),
        "args": [],
    }]);
    let mut out = format!("external-builders = {builders}\n");
    let capacity: u64 = caps.values().map(|c| u64::from(c.max_jobs)).sum();
    if capacity > 0 {
        let scaled = capacity * u64::from(cfg.oversubscribe_percent) / 100;
        let jobs = scaled.clamp(1, u64::from(cfg.max_jobs_cap));
        let _ = writeln!(out, "max-jobs = {jobs}");
    }
    out
}

pub(super) fn write_atomic(path: &Path, content: &str) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = fs::File::create(&tmp).map_err(io_ctx("creating", &tmp))?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path).map_err(io_ctx("renaming into", path))
}

impl WorkerCaps {
    /// Never a build no worker serves (unserved).
    pub(super) fn serves(&self, system: &str, features: &[String]) -> bool {
        self.systems
            .get(system)
            .is_some_and(|have| !unserved(features) && features.iter().all(|f| have.contains(f)))
    }
}

/// Whether `features` require recursive-nix, which no worker supports
/// though an older one may advertise it: no worker serves such a build.
pub(super) fn unserved(features: &[String]) -> bool {
    features.iter().any(|f| f == "recursive-nix")
}

impl HubState {
    pub(super) fn new(
        worker_grace: Duration,
        nix_config: Option<NixConfig>,
        chunks: Arc<ChunkCache>,
    ) -> Self {
        Self {
            queue: Mutex::default(),
            inflight: Arc::default(),
            notify: Notify::default(),
            daemon_pool: harmonia_store_remote::ConnectionPool::new(
                DAEMON_SOCKET,
                // A loaded nix-daemon can exceed the 10s default.
                harmonia_store_remote::PoolConfig {
                    connection_timeout: DAEMON_ANSWER,
                    ..Default::default()
                },
            ),
            daemon_evidence: DaemonEvidence::default(),
            dispatched: StdMutex::default(),
            deciding: atomic::AtomicBool::default(),
            worker_caps: StdMutex::default(),
            next_worker_id: atomic::AtomicU64::default(),
            worker_grace,
            started_at: Instant::now(),
            departed: StdMutex::default(),
            holding: StdMutex::default(),
            caps_changed: Notify::default(),
            metrics: Metrics::default(),
            nix_config,
            nix_rendered: atomic::AtomicU64::default(),
            nix_written: Arc::default(),
            chunks,
            placement: StdMutex::default(),
        }
    }

    /// Mirror the connected-worker set to the nix.conf fragment. Called
    /// after a register/deregister mutates `worker_caps`. The fragment is
    /// rendered and numbered under that lock, so concurrent register/
    /// deregister number their renderings in the order they changed it,
    /// and written on a blocking thread, outside it: no file I/O on the
    /// runtime or under a lock.
    pub(super) fn regen_nix_config(&self) {
        let Some(cfg) = &self.nix_config else {
            return;
        };
        let (generation, content) = {
            let caps = self.worker_caps.lock().unwrap();
            let generation = self.nix_rendered.fetch_add(1, atomic::Ordering::Relaxed) + 1;
            (generation, render_nix_config(&caps, cfg))
        };
        let (written, path) = (self.nix_written.clone(), cfg.path.clone());
        tokio::task::spawn_blocking(move || write_newest(&written, generation, &path, &content));
    }
}

/// Write `content`, rendering number `generation`, to `path` unless a
/// newer rendering was written: whatever order the writes run in, the
/// newest rendering is the one left, never stale content.
fn write_newest(written: &StdMutex<u64>, generation: u64, path: &Path, content: &str) {
    let mut newest = written.lock().unwrap();
    if *newest > generation {
        return;
    }
    *newest = generation;
    // Skip the write, and the daemon restart the watching path unit
    // would trigger, when nothing changed (e.g. a worker reconnects
    // with the same capabilities).
    if fs::read_to_string(path).is_ok_and(|cur| cur == content) {
        return;
    }
    if let Err(e) = write_atomic(path, content) {
        tracing::warn!(
            error = %e,
            path = %path.display(),
            "failed to write nix.conf fragment"
        );
    }
}

impl HubState {
    /// Worker session `worker` lists `keys` as held: at registration,
    /// or in a heartbeat (`beat`), which also counts it.
    pub(super) fn hold(&self, worker: u64, keys: HashSet<String>, beat: bool) {
        let mut holding = self.holding.lock().unwrap();
        let h = holding.entry(worker).or_default();
        (h.beats, h.keys) = (h.beats + u64::from(beat), keys);
        h.taken.clear();
    }

    /// The heartbeats worker session `worker` has sent so far.
    pub(super) fn heartbeats(&self, worker: u64) -> u64 {
        self.holding
            .lock()
            .unwrap()
            .get(&worker)
            .map_or(0, |h| h.beats)
    }

    /// Whether a heartbeat of worker session `worker` after its first
    /// `after` left `key` out: the worker no longer holds that build.
    pub(super) fn unlisted(&self, worker: u64, key: &str, after: u64) -> bool {
        self.holding
            .lock()
            .unwrap()
            .get(&worker)
            .is_some_and(|h| h.beats > after && !h.keys.contains(key))
    }

    /// Remember a departed worker's capabilities so a build for the
    /// platform it served waits for it to reconnect rather than
    /// declining at once.
    pub(super) fn record_departed(&self, caps: WorkerCaps) {
        let now = Instant::now();
        let mut departed = self.departed.lock().unwrap();
        // Prune here: expected_deadline() only runs for unservable
        // submissions, so a healthy fleet would otherwise accumulate
        // an entry per worker reconnect indefinitely.
        departed.retain(|(_, at)| now < *at + self.worker_grace);
        departed.push((caps, now));
    }

    /// Whether a platform can be built now, once an expected worker is
    /// back, or never.
    pub(super) fn servability(&self, system: &str, features: &[String]) -> Servability {
        if self
            .worker_caps
            .lock()
            .unwrap()
            .values()
            .any(|c| c.serves(system, features))
        {
            return Servability::Now;
        }
        self.expected_deadline(system, features)
            .map_or(Servability::Never, Servability::ExpectedBy)
    }

    fn expected_deadline(&self, system: &str, features: &[String]) -> Option<Instant> {
        let now = Instant::now();
        // During the startup window no worker has re-registered yet, so
        // every platform is awaited; afterwards only one a worker served
        // until it dropped within the reconnect window (reload/crash).
        let startup = self.started_at + self.worker_grace;
        if now < startup {
            return Some(startup);
        }
        let mut departed = self.departed.lock().unwrap();
        departed.retain(|(_, at)| now < *at + self.worker_grace);
        departed
            .iter()
            .filter(|(c, _)| c.serves(system, features))
            .map(|(_, at)| *at + self.worker_grace)
            .max()
    }

    /// The first queued job `caps`'s worker can take.
    pub(super) async fn take_job(&self, caps: &WorkerCaps) -> Option<Job> {
        loop {
            let mut job = {
                let mut queue = self.queue.lock().await;
                let holding = self.holding.lock().unwrap();
                // A held key goes to its holder by take_job_by_key, this
                // worker included: a fresh take would count the loss its
                // Resumed is about to clear.
                let workers = self.worker_caps.lock().unwrap();
                let pos = queue.iter().position(|j| {
                    caps.serves(&j.req.system, &j.features) && !j.kept(&workers, &holding)
                });
                queue.remove(pos?)?
            };
            // Abandoned while queued (every attach client gone): drop it
            // here, at the moment it would have occupied a build slot.
            if !job.replay.has_subscribers().await {
                tracing::info!(id = job.id, "dropping queued build: no client attached");
                self.finish(&job).await;
                continue;
            }
            // Its last worker did not resume it: this starts it over.
            if job.loss.take().is_some() {
                let Some(j) = losses::count(self, job, true).await else {
                    continue;
                };
                job = j;
            }
            return Some(job);
        }
    }

    /// Take a queued job `worker` can resume, regardless of its
    /// capacity. Each held key is honoured once.
    pub(super) async fn take_job_by_key(&self, worker: u64) -> Option<Job> {
        let mut job = {
            let mut queue = self.queue.lock().await;
            let mut holding = self.holding.lock().unwrap();
            let held = holding.get_mut(&worker)?;
            let pos = queue.iter().position(|j| held.holds(&j.key))?;
            let job = queue.remove(pos)?;
            held.taken.insert(job.key.clone());
            job
        };
        // Held by a worker other than the one that lost it: that loss
        // stands. The same worker's resume is settled by run_job.
        let name = self
            .worker_caps
            .lock()
            .unwrap()
            .get(&worker)
            .map(|c| c.name.clone());
        if job
            .loss
            .as_ref()
            .is_some_and(|r| Some(&r.worker) != name.as_ref())
        {
            job.loss = None;
            job = losses::count(self, job, true).await?;
        }
        Some(job)
    }

    /// Put a job back in the queue after its worker session died,
    /// telling attach clients to drop any half-streamed output NARs
    /// (the next attempt re-streams them from the start), and leaving
    /// their chunks to the cache while it waits, held or not.
    pub(super) async fn requeue(self: &Arc<Self>, mut job: Job) {
        job.queued_at = Instant::now();
        for path in job.req.outputs.values() {
            job.replay
                .publish(attach_event::Event::OutputRestart(path.clone()))
                .await;
        }
        job.replay.unpin_outputs().await;
        self.queue.lock().await.push_back(job);
        self.notify.notify_waiters();
        self.hold_unservable().await;
    }

    /// Hold queued jobs no connected worker serves, telling their
    /// clients, and re-run when the latest expected worker is due.
    pub(super) async fn hold_unservable(self: &Arc<Self>) {
        let Some(mut due) = self.hold_unservable_now().await else {
            return;
        };
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep_until((due + Duration::from_secs(1)).into()).await;
                match state.hold_unservable_now().await {
                    Some(next) => due = next,
                    None => break,
                }
            }
        });
    }

    pub(super) async fn finish(&self, job: &Job) {
        job.unlist();
        job.replay.finish().await;
    }

    /// Returns the latest deadline a queued job is still waiting for.
    /// A job no worker is expected for stays queued until one
    /// registers: its build is not the failure. Its client leaving
    /// drops it, as at any other point in the queue.
    async fn hold_unservable_now(&self) -> Option<Instant> {
        let mut due: Option<Instant> = None;
        let mut unservable = Vec::new();
        for j in self.queue.lock().await.iter() {
            match self.servability(&j.req.system, &j.features) {
                Servability::Now => {}
                Servability::ExpectedBy(at) => due = Some(due.map_or(at, |d| d.max(at))),
                Servability::Never => unservable.push((
                    j.id.clone(),
                    j.replay.clone(),
                    j.req.system.clone(),
                    j.features.join(","),
                )),
            }
        }
        // Outside the queue lock: both wait on the job's replay.
        let mut abandoned = HashSet::new();
        for (id, replay, system, features) in unservable {
            if !replay.has_subscribers().await {
                abandoned.insert(id);
                continue;
            }
            let note = format!(
                "tribuchet: no connected worker builds {system} [{features}]; waiting for one\n"
            );
            if replay.tell_waiting(Wait::Worker, note).await {
                tracing::warn!(
                    id,
                    system,
                    features,
                    "holding queued build: no capable worker"
                );
            }
        }
        if !abandoned.is_empty() {
            let gone: Vec<Job> = {
                let mut queue = self.queue.lock().await;
                let (gone, kept) = queue
                    .drain(..)
                    .partition::<VecDeque<Job>, _>(|j| abandoned.contains(&j.id));
                *queue = kept;
                gone.into()
            };
            for job in gone {
                tracing::info!(id = job.id, "dropping queued build: no client attached");
                self.finish(&job).await;
            }
        }
        due
    }
}

#[cfg(test)]
#[path = "state/tests.rs"]
mod tests;
