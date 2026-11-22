//! A producer's (builder-rpc-v0) socket in its build dir (DD3, DD10): the
//! path its NIX_REMOTE names, a listening socket owned by the uid its
//! builder runs as, mode 0600. The agent hands the listener to the worker
//! with StartReply and keeps no copy; it is close-on-exec, so neither the
//! setup stage nor the builder ever holds it (an orphan's connect() would
//! queue on a backlog nobody accepts).

use std::fs;
use std::os::fd::OwnedFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::UnixListener;
use std::path::{Component, Path};

use crate::errors::{Result, err_msg};
use crate::fsutil::io_ctx;

/// The listener at `basename` in `build_dir`, made by `bind` (as the
/// build's uid where the agent has a namespace for it), mode 0600.
pub(super) fn listen(build_dir: &Path, basename: &str) -> Result<OwnedFd> {
    let mut parts = Path::new(basename).components();
    let (Some(Component::Normal(_)), None) = (parts.next(), parts.next()) else {
        return Err(err_msg(format!("invalid rpc socket name {basename:?}")));
    };
    let path = build_dir.join(basename);
    let listener = UnixListener::bind(&path).map_err(io_ctx("binding", &path))?;
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
        .map_err(io_ctx("setting permissions on", &path))?;
    Ok(OwnedFd::from(listener))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustix::io::{FdFlags, fcntl_getfd};

    /// DT13, DT14 (DD3, DD10): the listener is mode 0600 and close-on-exec; a
    /// name that is not one path component is refused.
    #[test]
    fn the_listener_is_private_and_close_on_exec() {
        let dir = tempfile::tempdir().unwrap();
        let fd = listen(dir.path(), ".nix-socket").unwrap();
        assert!(fcntl_getfd(&fd).unwrap().contains(FdFlags::CLOEXEC));
        let mode = fs::metadata(dir.path().join(".nix-socket"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        for bad in ["a/b", "..", "", "/x"] {
            assert!(listen(dir.path(), bad).is_err(), "{bad:?}");
        }
    }
}
