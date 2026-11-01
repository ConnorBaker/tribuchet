use std::os::unix::fs::{PermissionsExt, symlink};

use super::*;
use crate::tmpdir::pack_zstd_dir;
use crate::worker::agents::tests::start_request;
use crate::worker::agents::{AgentBuild, cleanup, current_build};

#[test]
fn env_rewrite_replaces_tmp_dir_references() {
    let mut env = HashMap::from([
        ("NIX_BUILD_TOP".to_string(), "/build".to_string()),
        ("ATTRS".to_string(), "/build/.attrs.json".to_string()),
        ("OTHER".to_string(), "/buildings".to_string()),
    ]);
    rewrite_tmp_dir_env(&mut env, "/build", "/scratch/b1/build");
    assert_eq!(env["NIX_BUILD_TOP"], "/scratch/b1/build");
    assert_eq!(env["ATTRS"], "/scratch/b1/build/.attrs.json");
    assert_eq!(env["OTHER"], "/buildings");
}

#[test]
fn make_readable_skips_symlinks() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    fs::create_dir(&out).unwrap();
    fs::write(out.join("file"), "x").unwrap();
    symlink("/etc/passwd", out.join("link")).unwrap();
    make_readable(&out);
    assert_ne!(
        fs::metadata(out.join("file")).unwrap().permissions().mode() & 0o444,
        0
    );
}

/// A Finish at work (here waiting on `work`, as behind an earlier Finish's
/// long make-readable) keeps no Status or Adopt unanswered: both are
/// accepted after it and answered before it ends.
#[test]
fn status_and_adopt_are_answered_while_a_finish_works() -> Result<()> {
    const ID: &str = "0123456789abcdef0123456789abcdea";
    let dir = tempfile::tempdir()?;
    let socket = dir.path().join("agent.sock");
    let listener = sockpath::bind(&socket)?;
    let state_dir = dir.path().join("state");
    fs::create_dir(&state_dir)?;
    let opts = Options {
        socket: None,
        listen_fd: None,
        state_dir: state_dir.canonicalize()?,
        worker_uid: None,
        uid_base: None,
        dedicated_uid: false,
    };
    let agent = Arc::new(Agent {
        state_dir: opts.state_dir.clone(),
        worker_uid: getuid().as_raw(),
        confinement: platform::Confinement::init(&opts)?,
        current: Mutex::new(None),
        work: Mutex::new(()),
        dedicated_uid: false,
    });
    let served = agent.clone();
    thread::spawn(move || {
        let _ = serve(&served, &listener, false);
    });
    let top = dir.path().join("top");
    fs::create_dir(&top)?;
    let pack = dir.path().join("top.zst");
    fs::write(&pack, pack_zstd_dir(&top)?)?;
    let log = fs::File::create(dir.path().join("build.log"))?;
    let req = start_request(ID, "true".into(), vec![]);
    let build = AgentBuild::start(&socket, &req, &fs::File::open(&pack)?, &log)?;
    assert_eq!(build.wait_exit()?.exit_code, 0);

    let work = agent.work.lock().unwrap();
    let finish = sockpath::connect(&socket)?;
    let call = call::Call::Finish(FinishRequest {
        build_id: ID.into(),
    });
    framing::send_call(&finish, call, &[])?;
    let status = current_build(&socket).map_err(|e| chain(&e));
    let adopt = AgentBuild::adopt(&socket, ID)
        .map(|(_, exit)| exit)
        .map_err(|e| chain(&e));
    drop(work);
    let finished = framing::recv_reply(&finish)
        .map(|(r, _)| r)
        .map_err(|e| e.to_string());
    assert_eq!(
        (status, adopt, finished),
        (
            Ok(Some(ID.to_string())),
            Ok(Some(0)),
            Ok(reply::Reply::Empty(Empty {}))
        ),
        "(Status, Adopt, Finish) with a Finish at work"
    );
    cleanup(&socket, ID)
}
