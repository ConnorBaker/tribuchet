use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

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

/// DT13, DT14 (DD3, DD10) on a real agent: a producer's socket is in its build
/// dir, mode 0600, and its listener reaches the worker only. Neither its
/// builder nor the agent holds a copy: the builder's fds and this process's
/// (the agent's and the worker's) name the listener's inode once, the
/// worker's.
#[cfg(target_os = "linux")]
#[test]
fn a_producers_listener_reaches_the_worker_only() -> Result<()> {
    const ID: &str = "0123456789abcdef0123456789abcdeb";
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
    thread::spawn(move || {
        let _ = serve(&agent, &listener, false);
    });
    let top = dir.path().join("top");
    fs::create_dir(&top)?;
    let pack = dir.path().join("top.zst");
    fs::write(&pack, pack_zstd_dir(&top)?)?;
    let log = fs::File::create(dir.path().join("build.log"))?;
    let script = "for f in /proc/self/fd/*; do readlink $f; done > fds; true".to_owned();
    let mut req = start_request(ID, script, vec![]);
    req.rpc_socket = Some(".nix-socket".into());
    let mut build = AgentBuild::start(&socket, &req, &fs::File::open(&pack)?, &log)?;
    let held = build
        .listener
        .take()
        .expect("no listener in the StartReply");
    assert_eq!(build.wait_exit()?.exit_code, 0);
    let path = build.scratch_dir.join(".nix-socket");
    let mode = fs::metadata(&path)?.mode() & 0o777;
    assert_eq!(mode, 0o600, "the producer's socket's mode");
    let inode = fs::metadata(format!("/proc/self/fd/{}", held.as_raw_fd()))?.ino();
    let name = format!("socket:[{inode}]");
    let builders = fs::read_to_string(build.scratch_dir.join("fds"))?;
    assert!(
        !builders.contains(&name),
        "the builder holds the listener:\n{builders}"
    );
    let ours = (fs::read_dir("/proc/self/fd")?.flatten())
        .filter(|f| fs::read_link(f.path()).is_ok_and(|l| l.to_string_lossy() == name))
        .count();
    assert_eq!(ours, 1, "the agent kept a copy of the listener");
    cleanup(&socket, ID)
}
