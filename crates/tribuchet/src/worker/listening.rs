//! Free build agents whose socket nothing listens on, set aside as a Start
//! that found them so would: a stopped socket unit leaves its file behind
//! (agents::unreachable), so a free agent is otherwise found dead only by a
//! build started on it. Read from /proc/net/unix, which, unlike a connect,
//! activates no agent. It lists the sockets of the reader's network
//! namespace, which the worker shares with its agents: the socket units'
//! systemd, or the worker itself, which spawns them.

use std::fs;
use std::path::{Path, PathBuf};

use super::WorkerCtx;

/// /proc/net/unix's __SO_ACCEPTCON flag: the socket listens.
const ACCEPTCON: u32 = 0x1_0000;

/// Set aside the free agents nothing listens on, if /proc/net/unix reads.
pub(super) fn set_aside_unlistened(ctx: &WorkerCtx) {
    if let Ok(table) = fs::read_to_string("/proc/net/unix") {
        set_aside(ctx, &table);
    }
}

/// Set aside the free agents with no listening socket in `table`,
/// /proc/net/unix's text.
fn set_aside(ctx: &WorkerCtx, table: &str) {
    let listening = listening(table);
    for socket in ctx.agents.idle_sockets() {
        if !listens(&socket, &listening) && !spaced(&socket) && ctx.agents.mark_down(&socket) {
            tracing::warn!(
                agent = %socket.display(),
                "nothing listens on the build agent's socket; not leasing builds to it"
            );
        }
    }
}

/// Whether one of the `listening` paths is `socket`'s. Each is listed as
/// its binder spelled it: one of its file name is its own if both resolve
/// to the same file (another spelling of /run, a symlinked directory, not
/// a bind mount at another path, which resolving does not see), or if it
/// is relative, as sockpath binds a path too long from its directory,
/// which cannot be told apart.
fn listens(socket: &Path, listening: &[PathBuf]) -> bool {
    let own = fs::canonicalize(socket).ok();
    let named = |l: &&PathBuf| l.file_name() == socket.file_name();
    (listening.iter().filter(named))
        .any(|l| l.is_relative() || (own.is_some() && fs::canonicalize(l).ok() == own))
}

/// Whether `socket`'s path holds whitespace, which /proc/net/unix prints
/// raw: a newline splits its line, a leading space reads as padding. One
/// not found listening is left to the Start rather than set aside.
fn spaced(socket: &Path) -> bool {
    socket.to_string_lossy().contains(char::is_whitespace)
}

/// The paths of the listening sockets in `table`, whole, spaces and all.
fn listening(table: &str) -> Vec<PathBuf> {
    let listens = |line: &str| {
        let (fields, path) = fields(line)?;
        let flags = u32::from_str_radix(fields[3], 16).ok()?;
        (flags & ACCEPTCON != 0).then(|| PathBuf::from(path))
    };
    table.lines().skip(1).filter_map(listens).collect()
}

/// A line's seven fields before its path, and its path; None for a socket
/// without one. The fields are padded (the inode's to five places).
fn fields(line: &str) -> Option<([&str; 7], &str)> {
    let (mut fields, mut rest) = ([""; 7], line);
    for field in &mut fields {
        (*field, rest) = rest.trim_start().split_once(' ')?;
    }
    let path = rest.trim_start();
    (!path.is_empty()).then_some((fields, path))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::os::unix::net::UnixListener;

    use super::*;
    use crate::errors::Result;
    use crate::worker::resume::rescan_agents;

    /// /proc/net/unix as it lists, under `dir`, sockets bound through the
    /// symlinked directory alias/: a listener, one that does not listen,
    /// one with a space in its path; one bound by its file name alone, and
    /// a socket without a path (the inode padded, as the kernel does).
    fn table(dir: &Path) -> String {
        let alias = dir.join("alias");
        let row = |flags: &str, path: &str| {
            format!("0000000000000000: 00000002 00000000 {flags} 0001 01  1234 {path}\n")
        };
        [
            "Num       RefCount Protocol Flags    Type St Inode Path\n".to_owned(),
            row("00010000", &alias.join("1.sock").to_string_lossy()),
            row("00000000", &alias.join("2.sock").to_string_lossy()),
            row("00010000", &alias.join("5 x.sock").to_string_lossy()),
            row("00010000", "4.sock"),
            row("00010000", ""),
        ]
        .concat()
    }

    #[test]
    fn the_listening_sockets_are_read_whole() {
        let dir = Path::new("/run/agents");
        let alias = dir.join("alias");
        let want = [
            alias.join("1.sock"),
            alias.join("5 x.sock"),
            "4.sock".into(),
        ];
        assert_eq!(listening(&table(dir)), want);
    }

    /// The free agents with no listening socket are set aside: 2, which
    /// does not listen, and 3, not listed. 1 and "5 x", configured by
    /// their real directory and bound through its alias, 4, bound by its
    /// file name alone, and "6 y", not listed but with whitespace in its
    /// path, stay free.
    #[test]
    fn the_free_agents_not_listening_are_set_aside() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let real = dir.path().join("real");
        fs::create_dir(&real)?;
        symlink(&real, dir.path().join("alias"))?;
        let agents = [
            "1.sock", "2.sock", "3.sock", "4.sock", "5 x.sock", "6 y.sock",
        ]
        .map(|s| real.join(s));
        for socket in &agents {
            fs::write(socket, "")?;
        }
        let ctx = WorkerCtx::for_test(dir.path(), 6, agents.to_vec())?;
        set_aside(&ctx, &table(dir.path()));
        let mut down = ctx.agents.down_sockets();
        down.sort();
        assert_eq!(down, agents[1..3]);
        Ok(())
    }

    /// The rescan reads the worker's /proc/net/unix: an agent whose
    /// socket listens, bound through a symlinked directory, stays free;
    /// one whose file nothing listens on (as a stopped socket unit leaves
    /// it) is set aside.
    #[test]
    fn the_rescan_sets_aside_an_agent_nothing_listens_on() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let real = dir.path().join("real");
        fs::create_dir(&real)?;
        symlink(&real, dir.path().join("alias"))?;
        let (live, dead) = (real.join("live.sock"), real.join("dead.sock"));
        let _listener = UnixListener::bind(dir.path().join("alias/live.sock"))?;
        drop(UnixListener::bind(&dead)?);
        let ctx = WorkerCtx::for_test(dir.path(), 2, vec![live.clone(), dead.clone()])?;
        rescan_agents(&ctx, None);
        let seen = (ctx.agents.idle_sockets(), ctx.agents.down_sockets());
        assert_eq!(seen, (vec![live], vec![dead]));
        Ok(())
    }
}
