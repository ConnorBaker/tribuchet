//! attach: requests, output delivery, declines, and producers' relays.

use std::sync::Mutex;
use std::vec::IntoIter;

use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use super::*;
use crate::build_json::tests::{HASH, INPUT_ADDRESSED, SRI, doc};
use crate::proto::AttachEvent;
use crate::proto::attach_hub_server::{AttachHub, AttachHubServer};

/// A request names the client build.json names, else the nix-daemon
/// connection running attach.
#[test]
fn a_request_names_its_client() {
    let mut build = doc(&serde_json::json!({}));
    build.client = Some("asked".into());
    let (req, _) = request(build);
    assert_eq!(req.client.as_deref(), Some("asked"), "build.json's client");
    let (req, _) = request(doc(&serde_json::json!({})));
    assert_eq!(req.client, daemon_connection(), "the daemon connection");
}

/// zstd-compressed NAR of a single regular file, as the hub streams it.
fn zstd_nar_of_file(dir: &Path, body: &[u8]) -> Vec<u8> {
    let src = dir.join("src");
    fs::write(&src, body).unwrap();
    let nar = nar::pack::to_vec(&src).unwrap();
    zstd::encode_all(&nar[..], 3).unwrap()
}

async fn deliver(
    unpackers: &mut HashMap<String, Unpacker>,
    out: &str,
    chunk: Vec<u8>,
) -> Result<()> {
    let expected = vec![out.to_owned()];
    handle_output_chunk(
        unpackers,
        &expected,
        OutputNar {
            store_path: out.to_owned(),
            zstd_nar_chunk: chunk,
            eof: false,
        },
    )
    .await?;
    handle_output_chunk(
        unpackers,
        &expected,
        OutputNar {
            store_path: out.to_owned(),
            zstd_nar_chunk: Vec::new(),
            eof: true,
        },
    )
    .await
}

/// Regression: a leftover tree at the output path (earlier attempt in
/// this process) must be replaced, not fail with EEXIST.
#[tokio::test]
async fn replaces_existing_output_path() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    let out_str = out.to_string_lossy().into_owned();
    fs::create_dir(&out).unwrap();
    fs::write(out.join("stale"), b"junk").unwrap();

    let chunk = zstd_nar_of_file(dir.path(), b"fresh");
    let mut unpackers = HashMap::default();
    deliver(&mut unpackers, &out_str, chunk).await.unwrap();

    assert!(unpackers.is_empty());
    assert_eq!(fs::read(&out).unwrap(), b"fresh");
}

/// A hub answering each build with the next of its scripts of events,
/// and all after the last with the last.
struct Scripted(Mutex<Vec<Vec<attach_event::Event>>>);

#[tonic::async_trait]
impl AttachHub for Scripted {
    type BuildStream = tokio_stream::Iter<IntoIter<Result<AttachEvent, Status>>>;

    async fn build(
        &self,
        request: Request<Streaming<BuildMessage>>,
    ) -> Result<Response<Self::BuildStream>, Status> {
        let mut submission = request.into_inner();
        while let Ok(Some(_)) = submission.message().await {}
        let event = |e| Ok(AttachEvent { event: Some(e) });
        let mut scripts = self.0.lock().unwrap();
        let script = if scripts.len() > 1 {
            scripts.remove(0)
        } else {
            scripts[0].clone()
        };
        let events: Vec<_> = script.into_iter().map(event).collect();
        Ok(Response::new(tokio_stream::iter(events)))
    }
}

/// The socket of a hub in `dir` answering its builds with `scripts`.
fn hub(dir: &Path, scripts: Vec<Vec<attach_event::Event>>) -> PathBuf {
    let socket = dir.join("hub.sock");
    let incoming = UnixListenerStream::new(tokio::net::UnixListener::bind(&socket).unwrap());
    let scripted = Scripted(Mutex::new(scripts));
    tokio::spawn(
        Server::builder()
            .add_service(AttachHubServer::new(scripted))
            .serve_with_incoming(incoming),
    );
    socket
}

/// A build declined after one output was delivered and another in
/// part (OutputRestart for both, then exit 222) leaves neither tree,
/// nor one written in part to a hub gone before: Nix builds it next,
/// in place on Darwin or without a sandbox. The hub goes away once,
/// and run_async connects again.
#[tokio::test]
async fn a_decline_leaves_no_output_tree_attach_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let outs = ["earlier", "whole", "part"].map(|o| dir.path().join(o));
    let [earlier, whole, part] = outs.map(|o| o.to_string_lossy().into_owned());
    let chunk = zstd_nar_of_file(dir.path(), b"output");
    let output = |path: &str, chunk: &[u8], eof| {
        attach_event::Event::Output(OutputNar {
            store_path: path.to_owned(),
            zstd_nar_chunk: chunk.to_vec(),
            eof,
        })
    };
    let gone = vec![output(&earlier, &chunk, false)];
    let declining = vec![
        output(&whole, &chunk, false),
        output(&whole, &[], true),
        output(&part, &chunk, false),
        attach_event::Event::OutputRestart(whole.clone()),
        attach_event::Event::OutputRestart(part.clone()),
        attach_event::Event::ExitCode(DECLINE_EXIT_CODE),
    ];
    let socket = hub(dir.path(), vec![gone, declining]);
    let top = dir.path().join("top");
    fs::create_dir_all(top.join("build")).unwrap();
    let build = serde_json::json!({
        "version": 1, "builder": "", "args": [], "env": {}, "inputPaths": [],
        "topTmpDir": top, "tmpDirInSandbox": "/build", "storeDir": "/nix/store",
        "system": "x86_64-linux",
        "outputs": {"earlier": earlier, "whole": whole, "part": part},
    });
    let build = serde_json::from_value(build).unwrap();
    let code = run_async(build, socket, None).await.unwrap();
    assert_eq!(code, DECLINE_EXIT_CODE);
    let left = [earlier, whole, part].map(|p| fs::symlink_metadata(&p).map(|_| p).ok());
    assert_eq!(left, [None, None, None], "trees left");
}

/// Attach sends fixed_output as build_json tells it, true for an output
/// at the path of the hash it declares; and can_local false for any
/// declaring a hash, which the client's Nix would build in its host's
/// network, whatever Nix wrote; for any other, what Nix wrote.
#[test]
fn attach_sends_fixed_output_only_for_an_output_at_its_hash() {
    let hash = serde_json::json!({"outputHash": HASH});
    let cases = [
        (&hash, SRI, Some(true)),
        (&hash, INPUT_ADDRESSED, Some(true)),
        (&hash, INPUT_ADDRESSED, None),
        (&serde_json::json!({}), INPUT_ADDRESSED, Some(true)),
        (&serde_json::json!({}), INPUT_ADDRESSED, None),
    ];
    let sent = cases.map(|(env, out, can)| {
        let mut build = doc(env);
        build.outputs = [("out".to_owned(), out.to_owned())].into();
        build.can_build_locally = can;
        let (req, _) = request(build);
        (req.fixed_output, req.can_local)
    });
    let want = [
        (true, Some(false)),
        (false, Some(false)),
        (false, Some(false)),
        (false, Some(true)),
        (false, None),
    ];
    assert_eq!(sent, want);
}

/// Only a build declaring a hash it is not at gets the line.
#[test]
fn the_untold_hint_is_for_a_hash_at_another_path_only() {
    let got = [None, Some(true), Some(false)].map(|told| untold_hint(told).is_some());
    assert_eq!(got, [false, false, true]);
}
