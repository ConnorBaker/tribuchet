//! Build submission: request validation, dedupe keys, the AttachHub service.

use std::collections::HashSet;
use std::path::{Component, Path};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::known::refusal;
use super::metrics::Metrics;
use super::naming::drv_name;
use super::placement::Expected;
use super::placement::objective::enqueue;
use super::producer;
use super::sources;
use super::state::{HubState, Inflight, Job, Replay, Servability, unserved};
use crate::proto::{
    AttachEvent, BuildMessage, BuildRequest, DECLINE_EXIT_CODE, NETWORK, attach_event,
    attach_hub_server, build_message,
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

/// Hash of the whole request but its client's limits and name, canLocal, a
/// producer's relay_started and drvPath (none part of the build), plus the
/// tmp dir pack. A producer's attach_id is in it (DD9). prost maps are
/// BTreeMaps, so the encoding is deterministic.
pub(super) fn dedupe_key(req: &BuildRequest, tmp_dir_pack: &[u8]) -> String {
    // The build, not its client's limits: a client asking other limits joins
    // the build in flight, which keeps those of the request that dispatched
    // it, as Nix's own path locks do; and a client of another name. Zero
    // fields are not encoded, so keys are those from before these were sent.
    let req = BuildRequest {
        timeout_secs: 0,
        max_silent_secs: 0,
        can_local: None,
        client: None,
        relay_started: false,
        drv_path: None,
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
    /// Block until a worker can serve `system`+`features` (`None`), unless
    /// none is connected or expected back (L11). Then a client whose Nix
    /// can build the request is declined (a single exit-code event, so a
    /// patched Nix builds it locally). One whose Nix cannot is refused if
    /// no known worker serves it, and else waits for one, queued (`None`).
    async fn await_capable_worker(
        &self,
        (key, system): (&str, &str),
        features: &[String],
        can_local: bool,
    ) -> Option<Result<Response<BuildStream>, Status>> {
        let decline = || {
            tracing::info!(system, can_local, "no capable worker; declining");
            self.state
                .shadow
                .declined(key, (system, features), can_local);
            Metrics::inc(&self.state.metrics.declined);
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let _ = tx.try_send(Ok(AttachEvent {
                event: Some(attach_event::Event::ExitCode(DECLINE_EXIT_CODE)),
            }));
            Response::new(ReceiverStream::new(rx))
        };
        let refuse = || {
            tracing::info!(system, "no known worker; refusing");
            self.state.shadow.declined(key, (system, features), false);
            Status::failed_precondition(refusal(system, features))
        };
        loop {
            // Arm the wakeup before checking, else a worker registering
            // in the gap would be missed.
            let notified = self.state.caps_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let deadline = match self.state.servability(system, features) {
                Servability::Never if can_local => return Some(Ok(decline())),
                Servability::Never if !self.state.known.serves(system, features) => {
                    return Some(Err(refuse()));
                }
                Servability::Now | Servability::Never => return None,
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
        let mut inbound = request.into_inner();
        let (req, tmp_dir_pack) = read_submission(&mut inbound).await?;
        if req.outputs.is_empty() {
            return Err(Status::invalid_argument("build request without outputs"));
        }
        validate_request(&req)?;
        if producer::is_request(&req) && req.attach_id.is_empty() {
            return Err(Status::failed_precondition(producer::NO_ATTACH_ID));
        }
        if req.relay_started && !producer::is_request(&req) {
            return Err(Status::invalid_argument(producer::NOT_RELAYED));
        }
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
        let key_for_reader = key.clone();
        // Unsaid (a Nix or attach predating canLocal): today's rule, a
        // build without network. Never one with: declined, it would run on
        // this host's own daemon, in its network, outside every
        // fod-network policy (§1). A producer's too, until its run reached
        // the client, as a resubmission says (OQ1's fallback (a)).
        let is_producer = producer::is_request(&req);
        let can_local = producer::can_local(&req, req.relay_started);

        // A build with network goes only to a worker that gives it a
        // network of its own; one that does not would refuse it.
        let mut features = req.required_features.clone();
        if req.fixed_output {
            features.push(NETWORK.to_owned());
        }
        if let Some(answer) =
            (self.await_capable_worker((&key, &req.system), &features, can_local)).await
        {
            return answer;
        }

        let paths = producer::claims(&req);
        let owner: Option<Arc<str>> = req.client.as_deref().map(Arc::from);
        let once_relayed = producer::can_local(&req, true);
        let asks = |sys| {
            let client = self.state.shadow.submit(&key, sys, can_local);
            client.owned(owner.as_ref()).relayed(once_relayed)
        };
        let relayed = req.relay_started;
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
                // Its relay_started is this build's already: only the
                // attach that listed it joins it (DD9), and it says so
                // only after this hub acknowledged RelayStarted (DD8a).
                let sys = (req.system.as_str(), features.as_slice());
                let client = || {
                    let client = asks(sys);
                    tracing::info!(key, client = client.id, "deduplicating build submission");
                    client
                };
                // Answered before this client came, and so unlisted: anew.
                let Some(rx) = replay.join(client).await else {
                    continue;
                };
                // A build held for an input this client brought is free.
                self.state.notify.notify_waiters();
                break (rx, replay);
            }
            let id = new_id();
            let replay = Arc::new(Replay::new(id.clone()));
            // A resubmission after a hub restart carries what the hub
            // before acknowledged (DD8a), before any take can see it.
            if relayed {
                replay.relay_started();
            }
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
            let sys = (req.system.as_str(), features.as_slice());
            let client = asks(sys);
            let first = client.id;
            let joined = replay.clone();
            // Listed, the build is queued whatever becomes of this handler,
            // as a client may join it from now on: the lookup and the push
            // are a task, begun before this handler next yields, that no
            // cancelled submission drops. It waits for this handler's
            // subscription (or its end), so its push's check cannot end the
            // build as clientless first. A joining submission looks nothing up.
            let (subscribed, on_subscribed) = oneshot::channel::<()>();
            let state = self.state.clone();
            let listing = Arc::new(Mutex::new(Some(listing)));
            tokio::spawn(async move {
                let unqueued = Unqueued(Some((id.clone(), replay.clone())));
                let _ = on_subscribed.await;
                let sources = sources::of(&state.daemon_pool, &req).await;
                let mut job = Job {
                    id,
                    key,
                    req,
                    tmp_dir_pack,
                    features,
                    replay,
                    listing,
                    attempts: 0,
                    transport_losses: 0,
                    queued_at: Instant::now(),
                    loss: None,
                    deferred: None,
                    expected: Expected::prior(false),
                    expected_at: None,
                    sources,
                };
                state.placement.lock().unwrap().refresh(&mut job);
                tracing::info!(
                    id = job.id,
                    key = job.key,
                    client = first,
                    system = job.req.system,
                    drv = drv_name(&job.req).unwrap_or_default(),
                    owner = job.req.client.as_deref().unwrap_or_default(),
                    drv_path = job.req.drv_path.as_deref().unwrap_or_default(),
                    "queueing build"
                );
                if let Some(row) = producer::drv_path_row(&job)
                    && let Some(record) = &state.placement.lock().unwrap().record
                {
                    record.send(row);
                }
                Metrics::inc(&state.metrics.submitted);
                enqueue(&mut *state.queue.lock().await, job);
                unqueued.queued();
                state.notify.notify_waiters();
                // Close the check-then-queue race: the last capable worker
                // may have disconnected (and swept the queue) between the
                // capability check above and this push.
                state.hold_unservable().await;
            });
            let rx = joined.subscribe_as(client).await;
            let _ = subscribed.send(());
            break (rx, joined);
        };
        let (rx, replay) = rx;
        if is_producer {
            tokio::spawn(producer::relay_reader(inbound, replay, key_for_reader));
        }
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

/// A listed build's replay, until its build is queued: should the task
/// that queues it end first (a panic), the build ends, unlisted as its
/// listing drops, so that no client waits on it forever.
struct Unqueued(Option<(String, Arc<Replay>)>);

impl Unqueued {
    fn queued(mut self) {
        self.0 = None;
    }
}

impl Drop for Unqueued {
    fn drop(&mut self) {
        let Some((id, replay)) = self.0.take() else {
            return;
        };
        tracing::error!(id, "a listed build was never queued; ending it");
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move { replay.finish().await });
        }
    }
}

#[cfg(test)]
mod tests;
