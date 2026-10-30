//! Build submission: request validation, dedupe keys, the AttachHub service.

use std::collections::HashSet;
use std::path::{Component, Path};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use prost::Message;
use sha2::{Digest, Sha256};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::metrics::Metrics;
use super::naming::drv_name;
use super::state::{HubState, Inflight, Job, Replay, Servability, unserved};
use crate::proto::{
    AttachEvent, BuildMessage, BuildRequest, DECLINE_EXIT_CODE, attach_event, attach_hub_server,
    build_message,
};
use crate::store::{STORE_DIR, valid_store_path};

fn validate_request(req: &BuildRequest) -> Result<(), Status> {
    let bad = |what: &str, p: &str| {
        Status::invalid_argument(format!("{what} is not a valid store path: {p}"))
    };
    // A client-chosen store_dir would turn the root hub into an
    // arbitrary-file-read (and the worker sandbox into worse).
    if req.store_dir != STORE_DIR {
        return Err(Status::invalid_argument("invalid store dir"));
    }
    let mut seen_inputs = HashSet::new();
    for p in &req.input_paths {
        if !valid_store_path(&req.store_dir, p) {
            return Err(bad("input path", p));
        }
        if !seen_inputs.insert(p) {
            return Err(Status::invalid_argument(format!(
                "duplicate input path {p}"
            )));
        }
    }
    let mut seen_outputs = HashSet::new();
    for p in req.outputs.values() {
        if !valid_store_path(&req.store_dir, p) {
            return Err(bad("output path", p));
        }
        if !seen_outputs.insert(p) {
            return Err(Status::invalid_argument(format!(
                "duplicate output path {p}"
            )));
        }
        if seen_inputs.contains(p) {
            return Err(Status::invalid_argument(format!(
                "output path {p} is also an input"
            )));
        }
    }
    // Nix builders are absolute store paths; anything else would also be
    // option-injectable into sandbox-exec on Darwin workers.
    if !req.builder.starts_with('/') {
        return Err(Status::invalid_argument("builder must be an absolute path"));
    }
    // Where the worker mounts/symlinks the shipped build dir: "/build"
    // from Linux clients, the real per-build topTmpDir from Darwin.
    let tmp_in_sandbox = Path::new(&req.tmp_dir_in_sandbox);
    if !tmp_in_sandbox.is_absolute()
        || tmp_in_sandbox
            .components()
            .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
        || req.tmp_dir_in_sandbox.starts_with(STORE_DIR)
    {
        return Err(Status::invalid_argument("invalid tmpDirInSandbox"));
    }
    Ok(())
}

/// Cap on the client-shipped tmp dir archive: it is buffered in hub
/// memory for the lifetime of the job (redispatch resends it).
const MAX_TMP_DIR_BYTES: usize = 64 * 1024 * 1024;

/// Read the client submission stream: the request first, then the
/// zstd-packed build tmp dir up to its eof chunk.
async fn read_submission(
    stream: &mut Streaming<BuildMessage>,
) -> Result<(BuildRequest, Vec<u8>), Status> {
    let bad = |what: &str| Status::invalid_argument(format!("malformed submission stream: {what}"));
    let Some(build_message::Msg::Request(req)) = stream.message().await?.and_then(|m| m.msg) else {
        return Err(bad("expected the build request first"));
    };
    let mut pack = Vec::new();
    loop {
        let Some(build_message::Msg::TmpDir(chunk)) = stream.message().await?.and_then(|m| m.msg)
        else {
            return Err(bad("expected tmp dir archive chunks"));
        };
        if pack.len() + chunk.zstd_chunk.len() > MAX_TMP_DIR_BYTES {
            return Err(Status::resource_exhausted("tmp dir archive too large"));
        }
        pack.extend(chunk.zstd_chunk);
        if chunk.eof {
            return Ok((req, pack));
        }
    }
}

/// Hash of the whole request but its client's limits, plus the tmp dir
/// pack. prost maps are BTreeMaps, so the encoding is deterministic.
pub(super) fn dedupe_key(req: &BuildRequest, tmp_dir_pack: &[u8]) -> String {
    // The build, not its client's limits: a client asking other limits joins
    // the build in flight, which keeps those of the request that dispatched
    // it, as Nix's own path locks do. Zero fields are not encoded, so keys
    // are those from before the limits were sent.
    let req = BuildRequest {
        timeout_secs: 0,
        max_silent_secs: 0,
        ..req.clone()
    };
    let mut h = Sha256::new();
    let buf = req.encode_to_vec();
    h.update((buf.len() as u64).to_le_bytes());
    h.update(&buf);
    h.update(tmp_dir_pack);
    hex::encode(h.finalize())
}

fn new_id() -> String {
    let mut buf = [0u8; 16];
    getrandom::fill(&mut buf).expect("read system randomness");
    hex::encode(buf)
}

pub(super) struct AttachSvc {
    pub(super) state: Arc<HubState>,
}

type BuildStream = ReceiverStream<Result<AttachEvent, Status>>;

impl AttachSvc {
    /// Block until a worker can serve `system`+`features`, or return a
    /// decline stream (single exit-code event, so a patched Nix falls
    /// back to a local build). `None` means a worker is now available.
    /// Platforms we never expect to see decline without any wait.
    async fn await_capable_worker(
        &self,
        system: &str,
        features: &[String],
    ) -> Option<Response<BuildStream>> {
        let decline = || {
            tracing::info!(system, "no capable worker; declining");
            Metrics::inc(&self.state.metrics.declined);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let _ = tx.try_send(Ok(AttachEvent {
                event: Some(attach_event::Event::ExitCode(DECLINE_EXIT_CODE)),
            }));
            Response::new(ReceiverStream::new(rx))
        };
        loop {
            // Arm the wakeup before checking, else a worker registering
            // in the gap would be missed.
            let notified = self.state.caps_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let deadline = match self.state.servability(system, features) {
                Servability::Now => return None,
                Servability::Never => return Some(decline()),
                Servability::ExpectedBy(at) => at,
            };
            tracing::info!(system, "no capable worker yet; waiting");
            tokio::select! {
                () = &mut notified => {}
                () = tokio::time::sleep_until(deadline.into()) => {}
            }
        }
    }
}

#[tonic::async_trait]
impl attach_hub_server::AttachHub for AttachSvc {
    type BuildStream = ReceiverStream<Result<AttachEvent, Status>>;

    async fn build(
        &self,
        request: Request<Streaming<BuildMessage>>,
    ) -> Result<Response<Self::BuildStream>, Status> {
        let (req, tmp_dir_pack) = read_submission(&mut request.into_inner()).await?;
        if req.outputs.is_empty() {
            return Err(Status::invalid_argument("build request without outputs"));
        }
        validate_request(&req)?;
        // Queued, a build with network that no worker serves would wait
        // forever; declined, it would run on this host's own network.
        if req.fixed_output && unserved(&req.required_features) {
            return Err(Status::failed_precondition(
                "no worker builds a fixed-output derivation that requires recursive-nix, and \
                 the hub's host builds none with network",
            ));
        }
        let tmp_dir_pack = Arc::new(tmp_dir_pack);
        let key = dedupe_key(&req, &tmp_dir_pack);

        let features = req.required_features.clone();
        // Declined, a build with network would run on this host's own
        // daemon, in its network, outside every fod-network policy: it
        // waits for a worker, queued and its clients told (hold_unservable).
        if !req.fixed_output
            && let Some(declined) = self.await_capable_worker(&req.system, &features).await
        {
            return Ok(declined);
        }

        let paths: Vec<String> = req.outputs.values().cloned().collect();
        let rx = loop {
            // Registered before the checks so a release in between
            // is not missed.
            let released = self.state.inflight.lock().unwrap().released.clone();
            let notified = released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let existing = self
                .state
                .inflight
                .lock()
                .unwrap()
                .by_key
                .get(&key)
                .cloned();
            if let Some(replay) = existing {
                tracing::info!(key, "deduplicating build submission");
                break replay.subscribe().await;
            }
            let id = new_id();
            let replay = Arc::new(Replay::new(id.clone()));
            // A different request claiming an in-flight scratch path
            // would race the other client's unpack at the same dest, so
            // wait for it to finish (e.g. an abandoned earlier attempt).
            let Some(listing) = Inflight::list(&self.state.inflight, &key, paths.clone(), &replay)
            else {
                tracing::info!(
                    key,
                    "output path held by a different in-flight build; waiting"
                );
                notified.await;
                continue;
            };
            let rx = replay.subscribe().await;
            let job = Job {
                id,
                key,
                req,
                tmp_dir_pack,
                features,
                replay,
                listing: Arc::new(Mutex::new(Some(listing))),
                attempts: 0,
                resumed_losses: 0,
                queued_at: Instant::now(),
                loss: None,
            };
            tracing::info!(
                id = job.id,
                system = job.req.system,
                drv = drv_name(&job.req).unwrap_or_default(),
                "queueing build"
            );
            Metrics::inc(&self.state.metrics.submitted);
            self.state.queue.lock().await.push_back(job);
            self.state.notify.notify_waiters();
            break rx;
        };
        // Close the check-then-queue race: the last capable worker may
        // have disconnected (and swept the queue) between the capability
        // check above and the push.
        self.state.hold_unservable().await;
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::hub::state::WorkerCaps;
    use crate::hub::tests::{submit, try_submit};

    /// 32-char base32 hash part for synthetic store paths.
    const H: &str = "00000000000000000000000000000000";

    fn base_request() -> BuildRequest {
        BuildRequest {
            system: "x86_64-linux".into(),
            builder: format!("/nix/store/{H}-bash/bin/bash"),
            args: vec!["-c".into(), "true".into()],
            env: BTreeMap::default(),
            outputs: [("out".to_string(), format!("/nix/store/{H}-out"))].into(),
            input_paths: vec![format!("/nix/store/{H}-dep")],
            tmp_dir_in_sandbox: "/build".into(),
            store_dir: "/nix/store".into(),
            ..Default::default()
        }
    }

    #[test]
    fn request_validation() {
        assert!(validate_request(&base_request()).is_ok());

        let mut req = base_request();
        req.store_dir = "/etc".into();
        req.input_paths = vec!["/etc/shadow".into()];
        req.outputs = [("out".to_string(), "/etc/out".to_string())].into();
        assert!(validate_request(&req).is_err());

        let mut req = base_request();
        req.builder = "-p".into();
        assert!(validate_request(&req).is_err());

        let mut req = base_request();
        req.tmp_dir_in_sandbox = "relative".into();
        assert!(validate_request(&req).is_err());

        // Darwin clients send the real per-build tmp dir path.
        let mut req = base_request();
        req.tmp_dir_in_sandbox = "/private/tmp/nix-build-foo.drv-0".into();
        assert!(validate_request(&req).is_ok());

        let mut req = base_request();
        req.tmp_dir_in_sandbox = "/build/../etc".into();
        assert!(validate_request(&req).is_err());

        let mut req = base_request();
        req.tmp_dir_in_sandbox = format!("/nix/store/{H}-x");
        assert!(validate_request(&req).is_err());

        let mut req = base_request();
        req.input_paths = vec![format!("/nix/store/{H}-dep"), format!("/nix/store/{H}-dep")];
        assert!(validate_request(&req).is_err());

        let mut req = base_request();
        req.outputs
            .insert("doc".into(), format!("/nix/store/{H}-out"));
        assert!(validate_request(&req).is_err());

        let mut req = base_request();
        req.outputs = [("out".to_string(), format!("/nix/store/{H}-dep"))].into();
        assert!(validate_request(&req).is_err());
    }

    #[test]
    fn dedupe_key_binds_full_request() {
        let a = dedupe_key(&base_request(), b"");
        assert_eq!(a, dedupe_key(&base_request(), b""));
        let mut req = base_request();
        req.args = vec!["-c".into(), "false".into()];
        assert_ne!(a, dedupe_key(&req, b""));
        let mut req = base_request();
        req.env.insert("X".into(), "1".into());
        assert_ne!(a, dedupe_key(&req, b""));
        assert_ne!(a, dedupe_key(&base_request(), b"other .attrs.sh"));
        let req = BuildRequest {
            timeout_secs: 600,
            max_silent_secs: 60,
            ..base_request()
        };
        assert_eq!(a, dedupe_key(&req, b""));
    }

    /// A hub with one x86_64-linux worker registered.
    fn hub() -> Arc<HubState> {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let caps = WorkerCaps {
            name: "w".into(),
            connected_at: Instant::now(),
            systems: [("x86_64-linux".to_owned(), HashSet::new())].into(),
            max_jobs: 1,
        };
        state.worker_caps.lock().unwrap().insert(1, caps);
        state
    }

    /// A submitted build's replay carries the id the build is queued
    /// with, which the replay's warnings log.
    #[tokio::test]
    async fn a_submitted_builds_replay_carries_its_id() {
        let state = hub();
        let _events = submit(&state, base_request()).await;
        let queue = state.queue.lock().await;
        assert_eq!(queue[0].replay.id(), queue[0].id);
    }

    /// The same request submitted again while its build is in flight
    /// joins that build (a chain of bounded clients).
    /// So does one asking other limits: the build keeps those of the
    /// request that dispatched it.
    #[tokio::test]
    async fn a_resubmission_joins_the_build_in_flight() {
        let state = hub();
        let limited = BuildRequest {
            timeout_secs: 600,
            max_silent_secs: 60,
            ..base_request()
        };
        let _first = submit(&state, limited).await;
        let second =
            tokio::time::timeout(Duration::from_secs(10), submit(&state, base_request())).await;
        assert!(second.is_ok(), "the resubmission did not join the build");
        assert_eq!(state.queue.lock().await.len(), 1, "queued twice");
    }

    /// With no worker for its system, a build with network is not declined,
    /// which would have Nix run it on the hub's host: it is queued, its
    /// client told why it waits. Any other build is declined.
    #[tokio::test]
    async fn only_a_build_without_network_is_declined() {
        let state = Arc::new(HubState::for_test(Duration::ZERO));
        let first = |mut events: tonic::Streaming<AttachEvent>| async move {
            let ev = tokio::time::timeout(Duration::from_secs(10), events.message()).await;
            ev.unwrap().unwrap().unwrap().event.unwrap()
        };
        let declined = first(submit(&state, base_request()).await).await;
        assert_eq!(declined, attach_event::Event::ExitCode(DECLINE_EXIT_CODE));
        let fod = BuildRequest {
            fixed_output: true,
            ..base_request()
        };
        let attach_event::Event::Log(told) = first(submit(&state, fod).await).await else {
            panic!("a fixed-output build was not told it waits");
        };
        let told = String::from_utf8(told).unwrap();
        assert!(told.contains("waiting for one"), "{told}");
        assert_eq!(state.queue.lock().await.len(), 1);
    }

    /// A build that requires recursive-nix, which no worker serves,
    /// though one serves its system. With network it fails at submit,
    /// naming why: queued, it would wait forever, and declined, Nix
    /// would build it on the hub's host. Without, it is declined, as any
    /// build no worker serves, and Nix builds it locally.
    #[tokio::test]
    async fn a_build_requiring_recursive_nix_fails_with_network_and_is_declined_without() {
        let state = hub();
        let fod = BuildRequest {
            fixed_output: true,
            required_features: vec!["recursive-nix".into()],
            ..base_request()
        };
        let Err(failed) = try_submit(&state, fod.clone()).await else {
            panic!("the build was taken");
        };
        assert_eq!(failed.code(), tonic::Code::FailedPrecondition);
        assert!(failed.message().contains("recursive-nix"), "{failed}");
        assert_eq!(state.queue.lock().await.len(), 0);
        let local = BuildRequest {
            fixed_output: false,
            ..fod
        };
        let mut events = try_submit(&state, local).await.expect("not declined");
        let first = tokio::time::timeout(Duration::from_secs(10), events.message()).await;
        let first = first.unwrap().unwrap().unwrap().event.unwrap();
        assert_eq!(first, attach_event::Event::ExitCode(DECLINE_EXIT_CODE));
    }

    /// Strings shifted between adjacent sections must not collide:
    /// args `["-c", "K", "V"]` with no env and args `["-c"]` with
    /// env `{K: V}` would feed identical bytes without section counts.
    #[test]
    fn dedupe_key_separates_sections() {
        let mut a = base_request();
        a.args = vec!["-c".into(), "K".into(), "V".into()];
        a.env.clear();
        let mut b = base_request();
        b.args = vec!["-c".into()];
        b.env = [("K".to_string(), "V".to_string())].into();
        assert_ne!(dedupe_key(&a, b""), dedupe_key(&b, b""));
    }
}
