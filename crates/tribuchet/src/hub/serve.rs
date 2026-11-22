//! Hub bootstrap: sockets, TLS, auth configuration and server startup.

use std::fs;
use std::net::SocketAddr;
use std::os::unix::fs as unix_fs;
#[cfg(target_os = "macos")]
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use nix::unistd::Group;
use rustix::fs::Mode;
use rustix::process::umask;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

use super::admin::AdminSvc;
use super::chunkcache::ChunkCache;
use super::known::Known;
use super::lost::Lost;
use super::metrics;
use super::record;
use super::runs::Runs;
use super::state::HubState;
use super::submit::AttachSvc;
use super::{PeerAuth, WorkerSvc};
use crate::config::{Auth, HubConfig, default_chunk_cache_dir, default_record};
use crate::errors::{Result, chain, err_ctx, err_msg};
use crate::fsutil::io_ctx;
use crate::proto::worker_hub_server::WorkerHubServer;
use crate::proto::{MAX_MSG_SIZE, attach_hub_server, hub_admin_server};
use crate::{chunkio, rt, sd};

/// Bind the attach socket ourselves (no socket activation).
///
/// attach runs as a nix build user: restrict the socket to that group
/// (anyone who can reach it can have store paths packed and shipped).
/// Resolve the group *before* binding and bind with a tight umask so
/// the socket is never connectable by others, not even briefly.
fn bind_attach_socket(socket: &Path) -> Result<tokio::net::UnixListener> {
    if let Some(parent) = socket.parent() {
        fs::create_dir_all(parent).map_err(io_ctx("creating", parent))?;
    }
    // Refuse to replace the socket of a live hub: unlinking it would
    // leave all new attaches with ECONNREFUSED while the old hub runs.
    if UnixStream::connect(socket).is_ok() {
        return Err(err_msg(format!(
            "another hub is already serving {}",
            socket.display()
        )));
    }
    let _ = fs::remove_file(socket);
    let Ok(Some(group)) = Group::from_name("nixbld") else {
        return Err(err_msg(
            "group nixbld not found; refusing to serve a hub socket without a group to restrict it to",
        ));
    };
    let old_umask = umask(Mode::from_bits_truncate(0o117));
    let uds = tokio::net::UnixListener::bind(socket);
    umask(old_umask);
    let uds = uds?;
    {
        unix_fs::chown(socket, None, Some(group.gid.as_raw()))
            .map_err(io_ctx("chowning", socket))?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o660))
            .map_err(io_ctx("setting permissions on", socket))?;
    }
    Ok(uds)
}

/// Verify that a launchd-bound attach socket sits in a directory only
/// the nixbld group can reach. launchd's `Sockets` dictionary has a
/// mode key but no group key and the rootless hub cannot chown the
/// root-owned socket, so the parent directory carries the restriction
/// bind_attach_socket() puts on the socket itself.
#[cfg(target_os = "macos")]
fn check_attach_socket_dir(socket: &Path) -> Result<()> {
    let Some(group) = Group::from_name("nixbld").map_err(err_ctx("looking up group nixbld"))?
    else {
        return Err(err_msg(
            "group nixbld not found; refusing to serve a hub socket without a group to restrict it to",
        ));
    };
    let dir = socket
        .parent()
        .ok_or_else(|| err_msg("attach socket has no parent"))?;
    let meta = fs::metadata(dir).map_err(io_ctx("inspecting", dir))?;
    if meta.gid() != group.gid.as_raw() || meta.mode() & 0o007 != 0 {
        return Err(err_msg(format!(
            "{} must be group nixbld with no access for others to restrict the attach socket",
            dir.display()
        )));
    }
    Ok(())
}

fn configure_auth(
    cfg: &HubConfig,
    config_dir: &Path,
) -> Result<(Option<ServerTlsConfig>, PeerAuth)> {
    Ok(match cfg.auth {
        Auth::Mtls => {
            let ca_dir = config_dir.join("ca");
            let identity = Identity::from_pem(
                fs::read(ca_dir.join("hub.crt")).map_err(err_ctx("reading hub.crt"))?,
                fs::read(ca_dir.join("hub.key")).map_err(err_ctx("reading hub.key"))?,
            );
            let ca = Certificate::from_pem(
                fs::read(ca_dir.join("ca.crt")).map_err(err_ctx("reading ca.crt"))?,
            );
            (
                Some(ServerTlsConfig::new().identity(identity).client_ca_root(ca)),
                PeerAuth::Mtls,
            )
        }
        Auth::Tailscale => {
            tracing::info!(
                socket = %cfg.tailscale_socket.display(),
                allowed_tags = ?cfg.tailscale_allowed_tags,
                "tailscale auth: TLS disabled, identity via tailscaled whois"
            );
            (
                None,
                PeerAuth::Tailscale {
                    socket: cfg.tailscale_socket.clone(),
                    allowed_tags: cfg.tailscale_allowed_tags.clone(),
                },
            )
        }
    })
}

/// How often the hub, opening its record, asks systemd for more time
/// (sd::extending_start).
const EXTEND_EVERY: Duration = Duration::from_secs(10);

pub fn run(cfg: HubConfig) -> Result<()> {
    run_extending(cfg, EXTEND_EVERY)
}

/// The hub, asking for more start time every `period` while its record's
/// open progresses.
fn run_extending(cfg: HubConfig, period: Duration) -> Result<()> {
    let rt = rt::runtime("trib-hub").map_err(err_ctx("creating the tokio runtime"))?;
    rt.block_on(run_async(cfg, period))
}

/// The hub's state as `cfg` sets it, its nix.conf fragment listing the known
/// workers before any connects (/run is emptied by a reboot). The run epoch,
/// the known workers and the lost keys are kept beside the record at
/// `record`: all are the hub's state.
fn open_state(cfg: &HubConfig, chunks: Arc<ChunkCache>, record: &Path) -> HubState {
    let grace = Duration::from_secs(cfg.worker_grace_secs);
    let mut state = HubState::new(grace, cfg.nix_config.clone(), chunks);
    state.runs = Runs::open(&record.with_file_name("run-epoch"));
    state.known = Known::open(&record.with_file_name("known-workers"));
    state.lost = Lost::open(&record.with_file_name("lost-keys"));
    state.regen_nix_config();
    state
}

async fn run_async(cfg: HubConfig, period: Duration) -> Result<()> {
    let socket = cfg.socket.as_path();
    let listen = cfg.listen.as_str();
    let config_dir = cfg.config_dir.as_path();
    let chunks = open_chunk_cache(&cfg)?;

    // The record is folded into the history before the hub serves: a
    // migration of it may outlast the start timeout, so extend it while
    // it progresses.
    let path = cfg.placement.record.clone().unwrap_or_else(default_record);
    let state = Arc::new(open_state(&cfg, chunks, &path));
    let opened = sd::extending_start(
        tokio::task::spawn_blocking(move || record::open(&path)),
        &record::PROGRESS,
        period,
    )
    .await
    .map_err(err_ctx("opening the build record"))?;
    let writer = opened.install(&state);

    let (tls, peer_auth) = configure_auth(&cfg, config_dir)?;

    // Listeners come from systemd socket activation when available
    // (they survive hub restarts; clients queue instead of getting
    // ECONNREFUSED), otherwise we bind ourselves.
    let activated = sd::activated_sockets()?;
    let tcp = match activated.tcp {
        Some(l) => tokio::net::TcpListener::from_std(l)
            .map_err(err_ctx("adopting activated TCP socket"))?,
        // Bind TCP eagerly: a second hub instance must fail here on
        // EADDRINUSE *before* it clobbers the live hub's unix socket
        // below.
        None => tokio::net::TcpListener::bind(
            listen
                .parse::<SocketAddr>()
                .map_err(err_ctx("parsing listen address"))?,
        )
        .await
        .map_err(err_ctx("binding worker listen address"))?,
    };
    let mut builder = chunkio::session_server();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    let worker_server = builder
        .add_service(
            WorkerHubServer::new(WorkerSvc {
                state: state.clone(),
                auth: Arc::new(peer_auth),
                pending: Default::default(),
            })
            .max_decoding_message_size(MAX_MSG_SIZE)
            .max_encoding_message_size(MAX_MSG_SIZE),
        )
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(tcp));

    let uds = match activated.unix {
        // Activated socket: systemd owns the path, mode and group
        // (SocketGroup=/SocketMode= in the .socket unit). launchd has
        // no group key, so on macOS the socket's directory carries the
        // group restriction.
        Some(l) => {
            #[cfg(target_os = "macos")]
            check_attach_socket_dir(socket)?;
            tokio::net::UnixListener::from_std(l)
                .map_err(err_ctx("adopting activated unix socket"))?
        }
        None => bind_attach_socket(socket)?,
    };
    let attach_server = Server::builder()
        .initial_stream_window_size(Some(chunkio::H2_STREAM_WINDOW))
        .initial_connection_window_size(Some(chunkio::H2_CONNECTION_WINDOW))
        .add_service(
            attach_hub_server::AttachHubServer::new(AttachSvc {
                state: state.clone(),
            })
            .max_decoding_message_size(MAX_MSG_SIZE)
            .max_encoding_message_size(MAX_MSG_SIZE),
        )
        .add_service(hub_admin_server::HubAdminServer::new(AdminSvc {
            state: state.clone(),
        }))
        .serve_with_incoming(UnixListenerStream::new(uds));

    if let Some(metrics_addr) = cfg.metrics_listen.clone() {
        let state = state.clone();
        tokio::spawn(async move {
            if let Err(e) = metrics::serve(state, metrics_addr).await {
                let e = chain(&e);
                tracing::error!("metrics endpoint stopped: {e}");
            }
        });
    }

    tracing::info!(listen, socket = %socket.display(), "hub running");
    sd::notify_ready();
    sd::spawn_watchdog();
    let servers = async {
        tokio::try_join!(
            async { worker_server.await.map_err(err_ctx("worker gRPC server")) },
            async { attach_server.await.map_err(err_ctx("attach gRPC server")) },
        )
    };
    // No drain on SIGTERM: hub state is reconstructed by the
    // replacement instance from worker re-registration (resumable
    // build keys), attach resubmission (deterministic dedupe keys) and
    // the build record, whose queued rows are committed here, so
    // exiting cancels nothing.
    let res = tokio::select! {
        res = servers => res.map(|_| ()),
        () = sd::stop_requested() => {
            tracing::info!("SIGTERM: exiting, builds resume against the replacement instance");
            Ok(())
        }
    };
    let held = state.clone();
    let _ = tokio::task::spawn_blocking(move || record::close(&held, writer)).await;
    res
}

fn open_chunk_cache(cfg: &HubConfig) -> Result<Arc<ChunkCache>> {
    let dir = cfg
        .chunk_cache_dir
        .clone()
        .unwrap_or_else(default_chunk_cache_dir);
    let cache = ChunkCache::open(dir.clone(), cfg.chunk_cache_bytes)
        .map_err(io_ctx("opening chunk cache", &dir))?;
    Ok(Arc::new(cache))
}

#[cfg(test)]
mod tests {
    use std::os::unix::net::UnixDatagram;
    use std::process::{Command, Stdio};
    use std::time::Instant;
    use std::{env, str};

    use super::*;
    use crate::config;
    use crate::hub::tests::caps;

    /// The hub opens its known workers as it starts, and lists their systems
    /// in the nix.conf fragment before any of them connects.
    #[tokio::test]
    async fn a_starting_hub_lists_its_known_workers() {
        let dir = tempfile::tempdir().unwrap();
        let (record, path) = (dir.path().join("record"), dir.path().join("nix.conf"));
        Known::open(&record.with_file_name("known-workers")).learn(&caps("w"));
        let set = format!("[nix-config]\npath = {path:?}\nattach-program = '/a'");
        let cfg: HubConfig = toml::from_str(&set).unwrap();
        let chunks = Arc::new(ChunkCache::open(dir.path().join("chunks"), 1 << 20).unwrap());
        let (_, rendered) = open_state(&cfg, chunks, &record).nix_config.unwrap();
        let listed = rendered.borrow().clone();
        assert!(listed.contains(r#""systems":["x86_64-linux"]"#), "{listed}");
    }

    /// A hub, as `tribuchet hub` runs it but asking for more start time
    /// every 100 ms, on the config TT_HUB_CONFIG names: the child of the
    /// tests below.
    #[test]
    #[ignore = "run alone, in its own process, by the tests below"]
    fn hub_alone() {
        let path = env::var_os("TT_HUB_CONFIG").expect("TT_HUB_CONFIG");
        let cfg = config::load(Path::new(&path)).unwrap();
        run_extending(cfg, Duration::from_millis(100)).unwrap();
    }

    /// How a hub's notifications are read: until `enough` holds of them,
    /// the hub exits, or `quiet` ends the wait. A hub that blocks where no
    /// test expects it (not in the FIFO's open) spends no CPU and sends
    /// nothing, and is waited for to the test runner's timeout: no count
    /// of polls or CPU tells it from a lawful hub that sleeps or waits on
    /// slow I/O.
    enum Quiet {
        /// After the hub has spent this much CPU since the last one, or
        /// since it started: a hub working without asking. A freeze
        /// spends none.
        Cpu(Duration),
        /// This long after the hub is first seen waiting in the open of
        /// the FIFO at the record's path: a hang. A hub that spends 10 s
        /// of CPU and is not seen there is working elsewhere, and given up
        /// on, which can only fail the test; a frozen or slow one is
        /// waited for.
        InOpen(Duration),
    }

    /// The hub's CPU time, user and system, from /proc.
    fn cpu(pid: u32) -> Duration {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
        let fields: Vec<&str> = stat
            .rsplit(')')
            .next()
            .unwrap_or("")
            .split_whitespace()
            .collect();
        let tick = |i: usize| {
            fields
                .get(i)
                .and_then(|f| f.parse::<u64>().ok())
                .unwrap_or(0)
        };
        // utime and stime, fields 14 and 15, in clock ticks of 10 ms.
        Duration::from_millis(10 * (tick(11) + tick(12)))
    }

    /// Where each of the hub's threads waits (its wchan).
    fn wchans(pid: u32) -> Vec<String> {
        let tasks = fs::read_dir(format!("/proc/{pid}/task"))
            .into_iter()
            .flatten();
        let wchan = |t: fs::DirEntry| fs::read_to_string(t.path().join("wchan")).ok();
        tasks.flatten().filter_map(wchan).collect()
    }

    /// Whether a thread of the hub waits in the open of a FIFO for a
    /// writer (the kernel's `wait_for_partner`). Nothing opens one: a
    /// writer would let the hub read the FIFO as an empty record.
    fn in_fifo_open(pid: u32) -> bool {
        wchans(pid).iter().any(|w| w == "wait_for_partner")
    }

    /// What a hub did as it started: the notifications it sent, whether
    /// it exited, and whether it still waits in the open of a FIFO.
    struct Started {
        seen: Vec<String>,
        exited: bool,
        in_open: bool,
    }

    /// What a hub does as it starts on the record `make` leaves at the
    /// path it is given, its notifications read as `quiet` says. The
    /// socket's read timeout is only how often the hub is looked at. The
    /// hub is killed after.
    fn notifications(
        make: impl FnOnce(&Path),
        enough: impl Fn(&[String]) -> bool,
        quiet: &Quiet,
    ) -> Started {
        let dir = tempfile::tempdir().unwrap();
        let at = |name: &str| dir.path().join(name).display().to_string();
        let record = dir.path().join("record.sqlite");
        make(&record);
        let notify = UnixDatagram::bind(dir.path().join("notify")).unwrap();
        notify
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let toml = format!(
            "auth = \"tailscale\"\nsocket = \"{}\"\nlisten = \"127.0.0.1:0\"\n\
             chunk-cache-dir = \"{}\"\n[placement]\nrecord = \"{}\"\n",
            at("attach.sock"),
            at("chunks"),
            at("record.sqlite"),
        );
        fs::write(dir.path().join("hub.toml"), toml).unwrap();
        let mut hub = Command::new(env::current_exe().unwrap())
            .args(["--exact", "hub::serve::tests::hub_alone"])
            .args(["--ignored", "--test-threads=1"])
            .env("TT_HUB_CONFIG", at("hub.toml"))
            .env("NOTIFY_SOCKET", at("notify"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let (mut seen, mut buf, mut exited) = (Vec::new(), [0; 256], false);
        let (mut spent, mut held) = (Duration::ZERO, None::<Instant>);
        while !enough(&seen) {
            if let Quiet::InOpen(_) = quiet
                && held.is_none()
                && in_fifo_open(hub.id())
            {
                held = Some(Instant::now());
            }
            match notify.recv(&mut buf) {
                Ok(n) => {
                    seen.push(str::from_utf8(&buf[..n]).unwrap().to_owned());
                    spent = cpu(hub.id());
                }
                // Gone, and every notification it sent read.
                Err(_) if exited => break,
                Err(_) => {
                    exited = hub.try_wait().unwrap().is_some();
                    // Once reaped, its /proc entry is gone: no CPU to read.
                    let over = !exited
                        && match (quiet, &held) {
                            (Quiet::Cpu(q), _) => cpu(hub.id()).saturating_sub(spent) >= *q,
                            (Quiet::InOpen(q), Some(since)) => since.elapsed() >= *q,
                            (Quiet::InOpen(_), None) => cpu(hub.id()) >= Duration::from_secs(10),
                        };
                    if over {
                        break;
                    }
                }
            }
        }
        let in_open = in_fifo_open(hub.id());
        let _ = hub.kill();
        let _ = hub.wait();
        Started {
            seen,
            exited,
            in_open,
        }
    }

    const EXTEND: &str = "EXTEND_TIMEOUT_USEC=300000\n";

    /// A hub asks systemd to extend its start timeout as it opens its
    /// record, before it says it is ready (or exits after, as where no
    /// nixbld group exists). One that spends 10 s of CPU between
    /// notifications, ready or not, fails it.
    #[test]
    fn a_starting_hub_extends_its_start_timeout() {
        let ready = |seen: &[String]| seen.iter().any(|m| m.contains("READY=1"));
        let s = notifications(|_| {}, ready, &Quiet::Cpu(Duration::from_secs(10)));
        assert!(
            s.seen.first().is_some_and(|m| m.contains(EXTEND)),
            "{:?}",
            s.seen
        );
    }

    /// A hub whose record's open hangs (in the open of a FIFO no one
    /// writes, as of a stuck filesystem) asks for more time once, at once,
    /// and never again, still in it: systemd's start timeout kills it.
    /// Absence is waited for, 2 s (20 of the hub's ticks) after the hub
    /// is seen in the FIFO's open: a test frozen through them could pass
    /// a hub that asks again, never fail one that does not.
    #[test]
    fn a_hung_open_is_not_extended() {
        let fifo = |path: &Path| nix::unistd::mkfifo(path, nix::sys::stat::Mode::S_IRWXU).unwrap();
        let two = |seen: &[String]| seen.len() > 1;
        let wait = Quiet::InOpen(Duration::from_secs(2));
        let s = notifications(fifo, two, &wait);
        assert_eq!(
            (s.seen, s.exited, s.in_open),
            (vec![EXTEND.to_owned()], false, true)
        );
    }

    /// A hub whose record's open works without end (a trigger counting
    /// without bound on the one `taught` row the migration gives) asks
    /// for more time again and again: three asks, or 2 s of its CPU with
    /// no ask.
    #[test]
    fn a_long_open_that_progresses_is_extended() {
        let endless = |path: &Path| {
            let db = rusqlite::Connection::open(path).unwrap();
            db.execute_batch(record::SCHEMA).unwrap();
            db.execute_batch(record::TAUGHT).unwrap();
            db.execute_batch(
                "INSERT INTO result (at_ms, id, declared, exit_code, wall_ms, cpu_usec, \
                 peak_memory_bytes, oom_killed, frozen_ms) VALUES (1, 'a', 0, 0, 1, 1, 1, 0, 0); \
                 CREATE TRIGGER endless BEFORE INSERT ON taught BEGIN SELECT count(*) FROM \
                 (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c) \
                 SELECT x FROM c); END;",
            )
            .unwrap();
        };
        let three = |seen: &[String]| seen.len() >= 3;
        let s = notifications(endless, three, &Quiet::Cpu(Duration::from_secs(2)));
        assert_eq!(s.seen, [EXTEND; 3]);
    }
}
