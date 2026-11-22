//! `tribuchet attach`: shim executed by Nix (external-builders).
//!
//! Parses build.json, submits the build to the local hub over a unix
//! socket, streams logs to stderr, and unpacks returned output NARs at
//! the scratch output paths (identical on client and worker; Nix
//! performs self-reference rewriting and registration afterwards).
//! Exits with the builder's exit code.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::future;
use std::io::{self, Write};
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Once;
use std::time::Duration;

use hyper_util::rt::TokioIo;
use tokio::sync::mpsc;
use tonic::Code;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;

use crate::build_json::{BuildJson, flag, required_system_features};
use crate::chunkio;
use crate::errors::{Error, Result, chain, err_ctx, err_msg};
use crate::nar;
use crate::proto::{
    BuildMessage, BuildRequest, DECLINE_EXIT_CODE, MAX_MSG_SIZE, OutputNar, RelayStarted,
    TmpDirChunk, attach_event, attach_hub_client::AttachHubClient, build_message, producer,
};
use crate::rt;
use crate::tmpdir;

#[path = "attach/client.rs"]
mod client;
use client::daemon_connection;

pub fn run(build_json: &Path, socket: &Path, handoff: Option<&Path>) -> Result<()> {
    let build = BuildJson::load(build_json)?;
    let rt = rt::runtime("trib-attach").map_err(err_ctx("creating the tokio runtime"))?;
    let handoff = handoff.map(Path::to_owned);
    let code = rt.block_on(run_async(build, socket.to_owned(), handoff))?;
    // Unix exposes only the low 8 bits of the exit status; never let a
    // nonzero code collapse to an observed 0.
    process::exit(if code != 0 && code.trailing_zeros() >= 8 {
        1
    } else {
        code
    });
}

/// Reconnect budget across the whole build: a restarting hub is back
/// within seconds, and the worker holds finished results for resume
/// far longer than this.
const RECONNECT_ATTEMPTS: u32 = 30;
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

/// What attach says of a producer whose client's Nix cannot build it
/// itself (OQ1, decided (c)): a lost run of it, once it reached the
/// client, could only fail; so it never starts.
pub(crate) const PRODUCER_REFUSED: &str = "tribuchet refuses a dynamic-derivation producer \
    (builder-rpc-v0) that this Nix cannot build itself (build.json canBuildLocally: false): \
    once its run reached this Nix, a lost run could only fail the build";

async fn run_async(build: BuildJson, socket: PathBuf, handoff: Option<PathBuf>) -> Result<i32> {
    if build
        .real_store_dir
        .as_deref()
        .is_some_and(|r| r != build.store_dir)
    {
        tracing::info!("diverted store; declining so Nix builds locally");
        return Ok(DECLINE_EXIT_CODE);
    }
    // Nix places everything the builder consumes in topTmpDir/build.
    let dir = build.top_tmp_dir.join("build");
    let ControlFlow::Continue(mut relay) = relay::of(&build, handoff)? else {
        tracing::info!("no handoff socket on this host; declining a producer so Nix builds it");
        return Ok(DECLINE_EXIT_CODE);
    };
    let (mut req, told) = request(build);
    // Packed once and resent verbatim on every reconnect attempt.
    let tmp_dir_pack = tokio::task::spawn_blocking(move || tmpdir::pack_zstd_dir(&dir)).await??;
    // A producer's outputs are what it submits to Nix itself (DD6).
    let expected_outputs: Vec<String> = match relay {
        Some(_) => Vec::new(),
        None => req.outputs.values().cloned().collect(),
    };

    // When the hub restarts mid-build we reconnect and resubmit the
    // identical request; its build state is not kept across restarts.
    // Its dedupe key matches the build still running on the worker,
    // which resumes instead of building twice.
    let mut attempts = 0u32;
    let mut written = HashSet::new();
    loop {
        // Once the hub acknowledged it, every resubmission says so (DD8a).
        req.relay_started = relay.as_ref().is_some_and(|r| r.gate.acked());
        match attempt_build(
            (&req, &tmp_dir_pack),
            &socket,
            &expected_outputs,
            &mut written,
            relay.as_mut(),
        )
        .await?
        {
            Outcome::Done(code) => {
                if let Some(hint) = untold_hint(told) {
                    eprintln!("tribuchet: {hint}");
                }
                return Ok(code);
            }
            Outcome::Retry(e) => {
                attempts += 1;
                if attempts > RECONNECT_ATTEMPTS {
                    return Err(err_ctx("giving up reconnecting to the hub")(e));
                }
                eprintln!(
                    "tribuchet: hub connection lost ({}); reconnecting",
                    chain(&e)
                );
                tokio::time::sleep(RECONNECT_DELAY).await;
            }
        }
    }
}

/// What attach sends the hub for `build`, and whether it is told
/// fixed-output (BuildJson::fixed_output).
fn request(build: BuildJson) -> (BuildRequest, Option<bool>) {
    let attrs = build.attrs();
    let told = build.fixed_output(attrs.as_ref());
    let fixed_output = told == Some(true);
    let required_features = required_system_features(&build.env, attrs.as_ref());
    let local_networking = flag(&build.env, attrs.as_ref(), "__darwinAllowLocalNetworking");
    // Never hand back one declaring a hash, told or not: the client's Nix
    // would build it in its host's network (DESIGN.md), whatever Nix wrote.
    let can_local = told.map_or(build.can_build_locally, |_| Some(false));
    // A producer's goal is this attach's: another never joins its run (DD9).
    let attach_id = if producer(&required_features) {
        relay::attach_id()
    } else {
        Vec::new()
    };
    tracing::info!(fixed_output, can_local, system = %build.system, "submitting build");
    let req = BuildRequest {
        system: build.system,
        builder: build.builder,
        args: build.args,
        env: build.env.into_iter().collect(),
        outputs: build.outputs.into_iter().collect(),
        input_paths: build.input_paths,
        tmp_dir_in_sandbox: build.tmp_dir_in_sandbox.to_string_lossy().into_owned(),
        store_dir: build.store_dir,
        fixed_output,
        required_features,
        local_networking,
        timeout_secs: build.timeout,
        max_silent_secs: build.max_silent_time,
        can_local,
        client: build.client.or_else(daemon_connection),
        attach_id,
        relay_started: false,
        drv_path: build.drv_path,
    };
    (req, told)
}

/// The line for the log of a build that ended, as fixed_output `told` it:
/// one declaring a hash it is not at gets no network (DESIGN.md, Known
/// limitations), whether its builder ran or a worker failed first. Nix
/// checks its output's hash only after attach exits, so whatever its exit
/// code. None is declined: `request` sends it can_local false, and the hub
/// passes a worker's decline code on as 1.
fn untold_hint(told: Option<bool>) -> Option<&'static str> {
    let hint = "tribuchet gives it no network: it found its output not at a path it derives \
                from the outputHash it declares (as under --check, --rebuild or --repair); it \
                derives none for git hashing or a hash it cannot read (DESIGN.md)";
    (told == Some(false)).then_some(hint)
}

enum Outcome {
    Done(i32),
    /// Transport-level failure: hub restarting or briefly unreachable.
    /// Build failures never take this path.
    Retry(Error),
}

/// gRPC channel over the hub's local unix socket; tonic only knows
/// HTTP URIs, so the connector ignores the URI and dials the path.
pub(crate) async fn connect(socket: &Path) -> Result<tonic::transport::Channel> {
    let socket = socket.to_owned();
    Endpoint::try_from("http://hub.invalid")?
        .initial_stream_window_size(Some(chunkio::H2_STREAM_WINDOW))
        .initial_connection_window_size(Some(chunkio::H2_CONNECTION_WINDOW))
        .connect_with_connector(service_fn(move |_: Uri| {
            let socket = socket.clone();
            async move {
                Ok::<_, io::Error>(TokioIo::new(tokio::net::UnixStream::connect(socket).await?))
            }
        }))
        .await
        .map_err(err_ctx("connecting to hub socket"))
}

/// The submission stream: the request, then the tmp dir entries with
/// its eof marker.
fn submission(req: &BuildRequest, tmp_dir_pack: &[u8]) -> Vec<BuildMessage> {
    let tmp_dir = |zstd_chunk: Vec<u8>, eof| BuildMessage {
        msg: Some(build_message::Msg::TmpDir(TmpDirChunk { zstd_chunk, eof })),
    };
    let mut msgs = vec![BuildMessage {
        msg: Some(build_message::Msg::Request(req.clone())),
    }];
    msgs.extend(
        tmp_dir_pack
            .chunks(chunkio::CHUNK_SIZE)
            .map(|c| tmp_dir(c.to_vec(), false)),
    );
    msgs.push(tmp_dir(Vec::new(), true));
    msgs
}

async fn attempt_build(
    (req, tmp_dir_pack): (&BuildRequest, &[u8]),
    socket: &Path,
    expected_outputs: &[String],
    written: &mut HashSet<String>,
    mut relay: Option<&mut relay::Relay>,
) -> Result<Outcome> {
    let channel = match connect(socket).await {
        Ok(c) => c,
        Err(e) => return Ok(Outcome::Retry(e)),
    };
    let mut client = AttachHubClient::new(channel)
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE);

    // Ready marker for Nix; emitted only after a hub connection
    // exists so persistent connect failures surface as setup errors,
    // not build failures.
    ready_marker()?;

    // Kept open by a producer's attach, which sends RelayStarted on it
    // later; any other's ends after the submission, as it always did.
    let (tx, rx) = mpsc::channel(4);
    let tx = relay.is_some().then_some(tx);
    let sends = submission(req, tmp_dir_pack);
    let requests = tokio_stream::StreamExt::chain(
        tokio_stream::iter(sends),
        tokio_stream::wrappers::ReceiverStream::new(rx),
    );
    let mut stream = match client.build(requests).await {
        Ok(s) => s.into_inner(),
        Err(e) if retryable(&e) => {
            return Ok(Outcome::Retry(err_ctx("submitting build")(e)));
        }
        Err(e) => return Err(err_ctx("submitting build")(e)),
    };

    let mut unpackers: HashMap<String, Unpacker> = HashMap::default();
    let gate = relay.as_ref().map(|r| r.gate.clone());
    let mut told_started = false;

    loop {
        let asked = async {
            match &gate {
                Some(g) if !told_started => g.asked().await,
                _ => future::pending().await,
            }
        };
        let message = tokio::select! {
            m = stream.message() => m,
            () = asked => {
                told_started = true;
                // Naming the run held, so the hub never takes a splice of
                // a run gone for one of this run (DD8).
                let token = relay.as_ref().and_then(|r| r.token());
                if let (Some(tx), Some(handoff_token)) = (&tx, token) {
                    let started = RelayStarted { handoff_token };
                    let msg = Some(build_message::Msg::RelayStarted(started));
                    let _ = tx.send(BuildMessage { msg }).await;
                }
                continue;
            }
        };
        let ev = match message {
            Ok(Some(ev)) => ev,
            // Stream ended or broke without a result: the hub went
            // away; clean up partial output trees and resubmit.
            Ok(None) => {
                cleanup_unpackers(&mut unpackers).await;
                return Ok(Outcome::Retry(err_msg(
                    "hub closed event stream without a result",
                )));
            }
            Err(e) if retryable(&e) => {
                cleanup_unpackers(&mut unpackers).await;
                return Ok(Outcome::Retry(err_ctx("event stream")(e)));
            }
            Err(e) => {
                cleanup_unpackers(&mut unpackers).await;
                return Err(err_ctx("build event stream")(e));
            }
        };
        match ev.event {
            Some(attach_event::Event::Log(data)) => {
                io::stderr().write_all(&data)?;
            }
            Some(attach_event::Event::Output(out)) => {
                let path = out.store_path.clone();
                handle_output_chunk(&mut unpackers, expected_outputs, out).await?;
                written.insert(path);
            }
            Some(attach_event::Event::Dispatched(worker)) => {
                eprintln!("tribuchet: building on {worker}");
            }
            Some(attach_event::Event::OutputRestart(path)) => {
                // The previous worker attempt died mid-NAR; the next
                // attempt streams this output again from the start.
                if let Some((tx, task)) = unpackers.remove(&path) {
                    drop(tx);
                    let _ = task.await;
                }
            }
            Some(attach_event::Event::ExitCode(code)) => {
                // A worker's: every connection of the run has reached the
                // client (DD7). The hub's decline: the run was lost, and
                // may run still; none of it reaches the client after.
                if let Some(relay) = relay.as_mut() {
                    if code == DECLINE_EXIT_CODE {
                        relay.close().await;
                    } else {
                        relay.drain().await;
                    }
                }
                if !unpackers.is_empty() {
                    return Err(err_msg("hub closed build with unfinished output transfers"));
                }
                if code == DECLINE_EXIT_CODE {
                    // Nix now builds it itself, without clearing the
                    // output paths first: some of its builders write in
                    // place. Leave nothing there of ours.
                    for path in written.drain() {
                        remove_tree(Path::new(&path));
                    }
                }
                return Ok(Outcome::Done(code));
            }
            Some(attach_event::Event::Error(e)) => {
                cleanup_unpackers(&mut unpackers).await;
                return Err(err_msg(format!("remote build failed: {e}")));
            }
            Some(attach_event::Event::RelayReady(ready)) => {
                if let Some(relay) = relay.as_mut() {
                    // Not fetched, the run is lost (DD10): the hub settles
                    // that once its worker reports it expired.
                    if let Err(e) = relay.ready(ready).await {
                        eprintln!("tribuchet: {}; the run is lost", chain(&e));
                    }
                    // A splice still waiting asks again, naming this run.
                    told_started = false;
                }
            }
            Some(attach_event::Event::RelayStartedAck(token)) => {
                if let Some(relay) = relay.as_ref() {
                    relay.acked(&token);
                }
            }
            None => {}
        }
    }
}

/// Deliberate hub rejections (no capable worker, bad request, output
/// path conflict) are final; everything else is the transport dying
/// around a hub restart and worth resubmitting.
fn retryable(status: &tonic::Status) -> bool {
    !matches!(
        status.code(),
        Code::FailedPrecondition
            | Code::InvalidArgument
            | Code::PermissionDenied
            | Code::NotFound
            | Code::AlreadyExists
            | Code::ResourceExhausted
            | Code::Unimplemented
    )
}

/// Print Nix's \x02 ready marker exactly once, however many
/// reconnect attempts the build takes.
fn ready_marker() -> Result<()> {
    static ONCE: Once = Once::new();
    let mut res = Ok(());
    ONCE.call_once(|| {
        res = io::stderr().write_all(b"\x02\n").map_err(Into::into);
    });
    res
}

/// (chunk sender, unpack task) for one in-flight output transfer.
type Unpacker = (mpsc::Sender<Vec<u8>>, tokio::task::JoinHandle<Result<()>>);

fn remove_tree(path: &Path) {
    let _ = fs::remove_dir_all(path);
    let _ = fs::remove_file(path);
}

/// Unpack directly into the output path. Nix holds the path lock and
/// deletes invalid leftovers before the next builder run; a killed
/// attach leaves nothing behind that Nix does not already clean up.
async fn handle_output_chunk(
    unpackers: &mut HashMap<String, Unpacker>,
    expected: &[String],
    out: OutputNar,
) -> Result<()> {
    if !expected.contains(&out.store_path) {
        return Err(err_msg(format!(
            "hub sent unexpected output {}",
            out.store_path
        )));
    }
    let (tx, _) = unpackers.entry(out.store_path.clone()).or_insert_with(|| {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(8);
        let dest = PathBuf::from(&out.store_path);
        // An earlier attempt in this process may have left a partial
        // tree here; the re-delivered NAR replaces it.
        remove_tree(&dest);
        let task =
            tokio::spawn(
                async move { nar::unpack_zstd_chunks(rx, &dest).await.map_err(Into::into) },
            );
        (tx, task)
    });
    if !out.zstd_nar_chunk.is_empty() && tx.send(out.zstd_nar_chunk).await.is_err() {
        // The unpacker only stops early on error; report that error.
        let (_, task) = unpackers.remove(&out.store_path).unwrap();
        let err = task
            .await?
            .err()
            .unwrap_or_else(|| err_msg("unpacker exited before eof"));
        return Err(err_ctx(format!("unpacking output {}", out.store_path))(err));
    }
    if out.eof {
        let (tx, task) = unpackers.remove(&out.store_path).unwrap();
        drop(tx);
        task.await??;
        tracing::debug!(path = out.store_path, "output unpacked");
    }
    Ok(())
}

/// Stop in-flight unpackers; partial trees stay at the output paths
/// and are removed before the next unpack of the same path, or by a
/// decline.
async fn cleanup_unpackers(unpackers: &mut HashMap<String, Unpacker>) {
    for (_, (tx, task)) in unpackers.drain() {
        drop(tx);
        let _ = task.await;
    }
}

mod relay;

#[cfg(test)]
mod tests;
