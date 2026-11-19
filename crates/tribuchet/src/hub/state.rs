//! In-memory hub state: replay buffers, the job queue, worker capabilities.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::Write as _;
use std::fs;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify, mpsc, watch};
use tonic::Status;

use super::chunkcache::ChunkCache;
use super::losses::{self, Loss};
use super::metrics::Metrics;
use super::placement::{Expected, Placement};
use super::relay::{DAEMON_ANSWER, DaemonEvidence};
use super::runs::Runs;
use crate::config::NixConfig;
use crate::fsutil::write_atomic;

use crate::proto::{AttachEvent, BuildRequest, attach_event};

#[path = "state/clientless.rs"]
mod clientless;
#[path = "state/inflight.rs"]
mod inflight;
#[path = "state/replay.rs"]
mod replay;
#[path = "state/take.rs"]
mod take;

pub(super) use clientless::Dispatched;
#[cfg(test)]
pub(super) use clientless::FLOOR;
pub(super) use inflight::{Inflight, Listing};
pub(super) use replay::{KeptOutputs, Replay, Wait};
pub(super) use take::Holding;

type EventTx = mpsc::Sender<Result<AttachEvent, Status>>;

/// Cap on the replay buffer of one build. Without it a worker that
/// streams chunks forever grows root-hub memory without bound.
const MAX_REPLAY_BYTES: usize = 256 * 1024 * 1024;

/// Events read from the replay for an attach client that it has not
/// taken yet; its reader waits there, not the build (replay.rs).
const SUB_CHANNEL_SLACK: usize = 1024;

pub(super) struct Job {
    pub(super) id: String,
    pub(super) key: String,
    pub(super) req: BuildRequest,
    /// The client's zstd-packed build tmp dir, buffered so redispatch can
    /// resend it without another round-trip to the client.
    pub(super) tmp_dir_pack: Arc<Vec<u8>>,
    /// requiredSystemFeatures, and NETWORK for a build with network; only
    /// workers advertising them get the job.
    pub(super) features: Vec<String>,
    pub(super) replay: Arc<Replay>,
    /// Dedupe registration, dropped on cancel, when given up, or with
    /// the job.
    pub(super) listing: Arc<StdMutex<Option<Listing>>>,
    /// Genuine losses (hub/losses.rs): capped so a crash-looping
    /// build cannot bounce forever.
    pub(super) attempts: u32,
    /// Losses in transport (hub/losses.rs): capped so a build the farm
    /// cannot move goes back to its clients.
    pub(super) transport_losses: u32,
    /// When the job last entered the queue, for the queue-wait histogram.
    pub(super) queued_at: Instant,
    /// The loss of the build with its worker's session, which the next
    /// dispatch settles (hub/losses.rs), with how far its run got. Until
    /// the loss's grace ends, or the worker is back, the build is kept
    /// for that worker (Loss::awaited).
    pub(super) loss: Option<Loss>,
    /// When it was first passed over for another worker (placement::
    /// Rooms): none since it last entered the queue.
    pub(super) deferred: Option<Instant>,
    /// What it is expected to use, from the history as it was submitted:
    /// take_job's scan reads it under the queue lock, where it takes no
    /// placement lock.
    pub(super) expected: Expected,
}

impl Job {
    /// Stop deduping new submissions onto this job.
    pub(super) fn unlist(&self) {
        drop(self.listing.lock().unwrap().take());
    }

    /// Tell its clients to drop any half-streamed output NARs: the next
    /// attempt, or their own build, writes them from the start.
    pub(super) async fn restart_outputs(&self) {
        for path in self.req.outputs.values() {
            let restart = attach_event::Event::OutputRestart(path.clone());
            self.replay.publish(restart).await;
        }
        self.replay.unpin_outputs().await;
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
    /// Worker name -> build id -> key, of the builds dispatched to it,
    /// until no build wants the key and the worker lists it no more (strays).
    ran: StdMutex<HashMap<String, HashMap<String, String>>>,
    /// Woken when worker capabilities change so waiting submissions
    /// re-check servability without polling.
    pub(super) caps_changed: Notify,
    /// Build lifecycle counters scraped by the metrics endpoint.
    pub(super) metrics: Metrics,
    /// When set, the connected-worker set is mirrored to this nix.conf
    /// fragment on every register/deregister, through its one writer.
    pub(super) nix_config: Option<(NixConfig, watch::Sender<String>)>,
    pub(super) chunks: Arc<ChunkCache>,
    /// Each worker session's admission (placement.rs).
    pub(super) placement: StdMutex<Placement>,
    /// Numbers each dispatch's run.
    pub(super) runs: Runs,
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
            ran: StdMutex::default(),
            caps_changed: Notify::default(),
            metrics: Metrics::default(),
            nix_config: nix_config.map(Self::nix_config_writer),
            chunks,
            placement: StdMutex::default(),
            runs: Runs::default(),
        }
    }

    /// Mirror the connected-worker set to the nix.conf fragment. Called
    /// after a register/deregister mutates `worker_caps`.
    pub(super) fn regen_nix_config(&self) {
        let Some((cfg, tx)) = &self.nix_config else {
            return;
        };
        // Render and send under the lock: the last sent is the newest.
        let caps = self.worker_caps.lock().unwrap();
        tx.send_replace(render_nix_config(&caps, cfg));
    }

    /// The fragment's one writer: each time it wakes it writes the newest
    /// rendering (the watch keeps no other), off the runtime.
    pub(super) fn nix_config_writer(cfg: NixConfig) -> (NixConfig, watch::Sender<String>) {
        let (tx, mut rx) = watch::channel(String::new());
        let writer = cfg.clone();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                let (cfg, content) = (writer.clone(), rx.borrow_and_update().clone());
                let write = move || Self::write_nix_config(&cfg, &content);
                drop(tokio::task::spawn_blocking(write).await);
            }
        });
        (cfg, tx)
    }

    fn write_nix_config(cfg: &NixConfig, content: &str) {
        // Skip the write, and the daemon restart the watching path unit
        // would trigger, when nothing changed (e.g. a worker reconnects
        // with the same capabilities).
        if fs::read_to_string(&cfg.path).is_ok_and(|cur| cur == content) {
            return;
        }
        if let Err(e) = write_atomic(&cfg.path, content) {
            tracing::warn!(
                error = %e,
                path = %cfg.path.display(),
                "failed to write nix.conf fragment"
            );
        }
    }
}

impl HubState {
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

    /// Put a job back in the queue after its worker session died,
    /// telling attach clients to drop any half-streamed output NARs
    /// (the next attempt re-streams them from the start), and leaving
    /// their chunks to the cache while it waits, held or not.
    pub(super) async fn requeue(self: &Arc<Self>, mut job: Job) {
        (job.queued_at, job.deferred) = (Instant::now(), None);
        job.restart_outputs().await;
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
mod queue_tests;

#[cfg(test)]
mod tests {
    use super::queue_tests::queued;
    use super::*;

    #[tokio::test]
    async fn queued_job_survives_worker_restart_within_grace() {
        let mut state = Arc::new(HubState::for_test(Duration::from_secs(30)));
        Arc::get_mut(&mut state).unwrap().started_at =
            Instant::now().checked_sub(Duration::from_mins(1)).unwrap();
        let replay = Arc::new(Replay::default());
        let _rx = replay.subscribe().await;
        state
            .queue
            .lock()
            .await
            .push_back(queued("x86_64-linux", replay));
        state.record_departed(caps("x86_64-linux", &[]));
        state.hold_unservable().await;
        assert_eq!(state.queue.lock().await.len(), 1);
    }

    fn caps(system: &str, features: &[&str]) -> WorkerCaps {
        WorkerCaps {
            name: "w".into(),
            connected_at: Instant::now(),
            systems: [(
                system.to_owned(),
                features.iter().map(|f| (*f).to_owned()).collect(),
            )]
            .into(),
            max_jobs: 1,
        }
    }

    #[test]
    fn startup_window_awaits_then_unseen_platform_declines() {
        // Within the startup window any platform is awaited (workers
        // have not re-registered yet); once it lapses a never-seen
        // platform with no departed worker declines at once.
        let within = HubState::for_test(Duration::from_secs(30));
        assert!(within.expected_deadline("aarch64-linux", &[]).is_some());
        let lapsed = HubState::for_test(Duration::ZERO);
        assert!(lapsed.expected_deadline("aarch64-linux", &[]).is_none());
    }

    #[test]
    fn departed_worker_keeps_its_platform_expected() {
        // Past the startup window but inside the reconnect window.
        let mut state = HubState::for_test(Duration::from_secs(30));
        state.started_at = Instant::now().checked_sub(Duration::from_mins(1)).unwrap();
        state.record_departed(caps("x86_64-linux", &["kvm"]));
        let kvm = vec!["kvm".to_owned()];
        let kvm_bp = vec!["kvm".to_owned(), "big-parallel".to_owned()];
        // The exact platform it served is still awaited.
        assert!(state.expected_deadline("x86_64-linux", &kvm).is_some());
        // A feature it never offered, or another system, is not.
        assert!(state.expected_deadline("x86_64-linux", &kvm_bp).is_none());
        assert!(state.expected_deadline("aarch64-linux", &[]).is_none());
    }

    #[test]
    fn nix_config_union_systems_and_capped_oversubscribed_jobs() {
        let cfg = NixConfig {
            path: "/run/x".into(),
            attach_program: "/nix/store/attach".into(),
            oversubscribe_percent: 200,
            max_jobs_cap: 256,
        };
        let mut workers = HashMap::new();
        workers.insert(1, {
            let mut c = caps("x86_64-linux", &[]);
            c.max_jobs = 100;
            c
        });
        workers.insert(2, {
            let mut c = caps("aarch64-linux", &[]);
            c.max_jobs = 50;
            c
        });
        let out = render_nix_config(&workers, &cfg);
        assert!(
            out.contains(r#""systems":["aarch64-linux","x86_64-linux"]"#),
            "{out}"
        );
        assert!(out.contains(r#""program":"/nix/store/attach""#), "{out}");
        // (100 + 50) * 2 = 300, clamped to the cap.
        assert!(out.contains("max-jobs = 256\n"), "{out}");
    }

    #[test]
    fn nix_config_json_escapes_peer_supplied_systems() {
        let cfg = NixConfig {
            path: "/run/x".into(),
            attach_program: r"/nix/store/at\tach".into(),
            oversubscribe_percent: 100,
            max_jobs_cap: 1,
        };
        let mut workers = HashMap::new();
        workers.insert(1, caps("x86_64-linux\u{2028}\"", &[]));
        let out = render_nix_config(&workers, &cfg);
        let json = out
            .strip_prefix("external-builders = ")
            .and_then(|s| s.lines().next())
            .unwrap();
        // Nix parses this as JSON; must be valid regardless of what a
        // worker registered or how the attach path is spelled.
        let v: serde_json::Value = serde_json::from_str(json).unwrap();
        assert_eq!(v[0]["systems"][0], "x86_64-linux\u{2028}\"");
        assert_eq!(v[0]["program"], r"/nix/store/at\tach");
    }

    #[test]
    fn nix_config_without_workers_omits_max_jobs() {
        let cfg = NixConfig {
            path: "/run/x".into(),
            attach_program: "/nix/store/attach".into(),
            oversubscribe_percent: 200,
            max_jobs_cap: 256,
        };
        let out = render_nix_config(&HashMap::new(), &cfg);
        assert!(out.contains(r#""systems":[]"#), "{out}");
        // No worker -> leave max-jobs to the base nix.conf default so a
        // local fallback build is not run at the offload capacity.
        assert!(!out.contains("max-jobs"), "{out}");
    }

    #[test]
    fn worker_caps_feature_matching() {
        let caps = WorkerCaps {
            name: "w1".into(),
            connected_at: Instant::now(),
            systems: [
                ("x86_64-linux".to_owned(), ["kvm".to_owned()].into()),
                ("aarch64-linux".to_owned(), [].into()),
            ]
            .into(),
            max_jobs: 1,
        };
        assert!(caps.serves("x86_64-linux", &[]));
        assert!(caps.serves("x86_64-linux", &["kvm".into()]));
        assert!(!caps.serves("x86_64-linux", &["kvm".into(), "uid-range".into()]));
        assert!(caps.serves("aarch64-linux", &[]));
        // emulated system must not inherit the host's kvm
        assert!(!caps.serves("aarch64-linux", &["kvm".into()]));
        assert!(!caps.serves("i686-linux", &[]));
    }
}
