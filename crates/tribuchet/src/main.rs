mod attach;
mod build_json;
mod ca;
mod chunker;
mod chunkio;
mod chunkstore;
mod config;
mod errors;
mod fsutil;
mod hub;
mod nar;
mod netpolicy;
mod proto;
mod rt;
mod sd;
mod sockpath;
#[cfg(test)]
mod stalltest;
mod store;
mod tailscale;
mod tmpdir;
mod worker;

#[cfg(target_os = "linux")]
use std::env;
use std::fs;
use std::io::{self, IsTerminal};
use std::num::NonZero;
use std::os::unix::fs::symlink;
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread;

use clap::{Parser, Subcommand};
use errors::{Error, err_ctx, err_msg};

/// seed-history's swap, one command. systemd stops the hub before its
/// socket, which listens through the hub's last save; `tribuchet attach`
/// retries a build's connects 30 times 2 s apart in all.
const SWAP: &str = "Then, under systemd and from a root shell (`sudo -s`), swap SIDE in \
for LIVE in one command. systemd stops the hub before its socket where the hub's unit \
requires and is ordered after it, as the NixOS module's is: the socket listens through the \
hub's last save, connections queued then are closed unanswered as it stops, and connects \
are refused from then until the start. `tribuchet attach` retries a build's connects 30 \
times 2 s apart in all, so an outage longer than that (a wedged hub's stop takes up to \
systemd's default 90 s) fails the builds connecting. Where a hub holds LIVE's lock, SIDE \
is not a file (missing, a directory, a dangling symlink), LIVE's backup of this second \
exists, a directory or a symlink to one is at the backup's name, a directory is at LIVE, or \
SIDE is a symlink to LIVE or a hard link of it (refused after ln, which leaves a stray \
LIVE.<seconds>), it prints NOT SWAPPED, leaves LIVE as it was, and still exits 0: read its \
output. Where it swaps, the old LIVE stays as LIVE.<seconds> (a \
symlink at LIVE to a file or directory is replaced and kept so; a dangling one, replaced and \
not kept). The socket is started either way; where that \
fails (its port taken), systemctl says so and exits 1, and the hub stays down until the \
socket is started again (after `systemctl reset-failed tribuchet-hub.socket` if the start \
refuses). A hub that finds LIVE's lock held as it starts, or cannot load LIVE, never saves \
until it is restarted. Leave SIDE.lock and LIVE.lock in place: a lock file deleted while \
held lets the next taker lock a new file, and two writers write. The swap replaces LIVE's \
history with the journal's, which keeps less: keys and runs older than the journal's \
retention stay only in LIVE.<seconds>. So seed and swap only for a first deploy or where \
LIVE cannot be read (the hub warns that it is unreadable), not over a readable LIVE:

  systemctl stop tribuchet-hub.socket tribuchet-hub.service; \\
  flock -n LIVE.lock sh -ec 'test -f SIDE; if [ -e LIVE ]; then ln -T LIVE LIVE.$(date +%s); fi; mv -T SIDE LIVE' || echo 'NOT SWAPPED' >&2; \\
  systemctl start tribuchet-hub.socket";

/// RBE-style remote build execution for Nix, driven by the
/// `external-builders` experimental feature.
#[derive(Parser)]
#[command(name = "tribuchet", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// External-builders shim invoked by Nix; forwards the build to the hub.
    Attach {
        /// Path to the build.json written by Nix.
        build_json: PathBuf,
        /// Hub unix socket.
        #[arg(long, default_value = "/run/tribuchet/hub.sock")]
        socket: PathBuf,
    },
    /// Scheduler and NAR relay; runs next to nix-daemon.
    Hub {
        /// TOML configuration file.
        #[arg(long, default_value = "/etc/tribuchet/hub.toml")]
        config: PathBuf,
    },
    /// Build worker; dials the hub and executes sandboxed builds.
    Worker {
        /// TOML configuration file; re-read on every reload.
        #[arg(long, default_value = "/etc/tribuchet/worker.toml")]
        config: PathBuf,
    },
    /// Seed a new placement history file from the hub's journal on stdin
    /// (`journalctl -u tribuchet-hub -o cat`), beside the hub's file while
    /// the hub runs; then swap it in with the hub stopped, by the one
    /// command --help gives. Refused at a name ending .tmp or .lock, where
    /// the file exists, where a hub or seed-history holds its lock, and
    /// where it would seed nothing. The swap replaces the hub's history
    /// with the journal's: only for a first deploy or an unreadable one.
    #[command(after_long_help = SWAP)]
    SeedHistory {
        /// The new file, SIDE, beside the hub's, LIVE: hub.toml's
        /// [placement] history, else placement-history.json in the hub's
        /// $XDG_CACHE_HOME/tribuchet (`systemctl cat tribuchet-hub` shows
        /// it; /var/cache/tribuchet under the NixOS module).
        #[arg(long)]
        history: PathBuf,
    },
    /// Certificate authority management (init CA, issue worker certs).
    Ca {
        #[command(subcommand)]
        action: ca::CaAction,
    },
    /// Per-uid build agent, one per pool user.
    Agent {
        /// Unix socket to bind when not socket-activated.
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Inherited listening socket (worker-spawned agents).
        #[arg(long)]
        listen_fd: Option<i32>,
        /// Directory for per-build scratch dirs.
        #[arg(long)]
        state_dir: PathBuf,
        /// Uid allowed to lease builds (the worker user).
        #[arg(long)]
        worker_uid: Option<u32>,
        /// First uid of the agent's 65536-uid block (Linux).
        #[arg(long)]
        uid_base: Option<u32>,
        /// The agent owns its uid even without socket activation.
        #[arg(long)]
        dedicated_uid: bool,
    },
    /// systemd generator: want one agent socket per CPU.
    AgentGenerator {
        /// Unit the sockets are ordered before and wanted by, e.g. tribuchet-worker.service.
        #[arg(long)]
        worker_unit: String,
        /// Socket template, e.g. tribuchet-agent@.socket.
        #[arg(long)]
        template: String,
        /// Upper bound on the count.
        #[arg(long)]
        max: Option<usize>,
        /// normal, early and late output directories passed by systemd.
        dirs: Vec<PathBuf>,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("tribuchet: {}", errors::chain(&e));
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Error> {
    // Builds re-exec this binary as the sandbox setup stage; divert
    // before clap and tracing touch anything.
    #[cfg(target_os = "linux")]
    if env::args().nth(1).as_deref() == Some(worker::sandbox::SETUP_STAGE_ARG) {
        worker::sandbox::setup_stage();
    }
    // Same for the build agent's userns filesystem helper.
    #[cfg(target_os = "linux")]
    if env::args().nth(1).as_deref() == Some(worker::agent::FS_HELPER_ARG) {
        worker::agent::fs_helper_stage();
    }
    // And for the agent's userns holder child.
    #[cfg(target_os = "linux")]
    if env::args().nth(1).as_deref() == Some(worker::USERNS_HOLD_ARG) {
        worker::userns_hold_stage();
    }
    // Colour only on a terminal: the hub's journal is read back by
    // `seed-history`, and by anyone grepping it.
    tracing_subscriber::fmt()
        .with_ansi(io::stdout().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Attach { build_json, socket } => attach::run(&build_json, &socket),
        Command::Hub { config } => {
            let cfg: config::HubConfig = config::load(&config)?;
            hub::run(cfg)
        }
        Command::Worker { config } => {
            let mut cfg: config::WorkerConfig = config::load(&config)?;
            cfg.apply_env_overrides();
            tracing::info!(?cfg, "worker configuration");
            worker::run(cfg)
        }
        Command::SeedHistory { history } => hub::seed_history(&history, io::stdin().lock()),
        Command::Ca { action } => Ok(ca::run(action)?),
        Command::Agent {
            socket,
            listen_fd,
            state_dir,
            worker_uid,
            uid_base,
            dedicated_uid,
        } => Ok(worker::agent::run(&worker::agent::Options {
            socket,
            listen_fd,
            state_dir,
            worker_uid,
            uid_base,
            dedicated_uid,
        })?),
        Command::AgentGenerator {
            worker_unit,
            template,
            max,
            dirs,
        } => agent_generator(&worker_unit, &template, max, &dirs),
    }
}

fn agent_generator(
    worker_unit: &str,
    template: &str,
    max: Option<usize>,
    dirs: &[PathBuf],
) -> Result<(), Error> {
    let out = dirs
        .first()
        .ok_or_else(|| err_msg("systemd passes the output directory"))?;
    let (prefix, suffix) = template
        .split_once("@.")
        .ok_or_else(|| err_msg("template must look like name@.socket"))?;
    let mut n = thread::available_parallelism().map_or(1, NonZero::get);
    if let Some(max) = max {
        n = n.min(max);
    }
    for wants in [
        "sockets.target.wants".to_string(),
        format!("{worker_unit}.wants"),
    ] {
        let dir = out.join(wants);
        fs::create_dir_all(&dir).map_err(err_ctx(format!("mkdir {}", dir.display())))?;
        for i in 1..=n {
            let link = dir.join(format!("{prefix}@{i}.{suffix}"));
            symlink(format!("../{template}"), &link)
                .map_err(err_ctx(format!("symlink {}", link.display())))?;
        }
    }
    Ok(())
}

/// The doc comment above Cli is the description --help prints.
#[test]
fn help_describes_the_program() {
    let cli = <Cli as clap::CommandFactory>::command();
    let about = cli.get_about().map(ToString::to_string);
    assert!(about.is_some_and(|a| a.starts_with("RBE-style")));
}
