//! Validation of hub-supplied assignment strings before they touch
//! the filesystem.

use std::path::{Component, Path};

use crate::errors::{Result, err_msg};
use crate::proto::{BuildAssignment, producer};
use crate::store::{STORE_DIR, valid_store_path};

/// The worker must not trust the hub for filesystem-relevant strings:
/// build ids become path components, output paths are packed (and on
/// macOS deleted) on the host.
pub(in crate::worker) fn validate_assignment(a: &BuildAssignment) -> Result<()> {
    if a.build_id.len() != 32 || !a.build_id.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(err_msg(format!("invalid build id {:?}", a.build_id)));
    }
    if !a.builder.starts_with('/') {
        return Err(err_msg("builder must be an absolute path"));
    }
    let tmp = Path::new(&a.tmp_dir_in_sandbox);
    if !tmp.is_absolute()
        || tmp
            .components()
            .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
    {
        return Err(err_msg(format!(
            "invalid tmpDirInSandbox {:?}",
            a.tmp_dir_in_sandbox
        )));
    }
    for p in a.outputs.values() {
        if !valid_store_path(STORE_DIR, p) {
            return Err(err_msg(format!("invalid output path {p:?}")));
        }
    }
    Ok(())
}

/// The basename of a producer's (builder-rpc-v0) socket in its tmp dir
/// (DD3): its NIX_REMOTE must be `unix://<tmp_dir_in_sandbox>/<name>`,
/// as Nix sets it; anything else is refused. None for every other build,
/// whatever its NIX_REMOTE (a recursive-nix build has one too, DD1).
pub(in crate::worker) fn rpc_socket(a: &BuildAssignment) -> Result<Option<String>> {
    if !producer(&a.required_features) {
        return Ok(None);
    }
    let remote = a.env.get("NIX_REMOTE").map(String::as_str).unwrap_or("");
    let path = remote.strip_prefix("unix://").map(Path::new);
    let name = path.and_then(|p| {
        let in_tmp = p.parent() == Some(Path::new(&a.tmp_dir_in_sandbox));
        let mut parts = Path::new(p.file_name()?).components();
        let single = matches!(
            (parts.next(), parts.next()),
            (Some(Component::Normal(_)), None)
        );
        (in_tmp && single).then(|| p.file_name()?.to_str().map(str::to_owned))?
    });
    name.map(Some).ok_or_else(|| {
        err_msg(format!(
            "a producer's NIX_REMOTE must name a socket in its tmp dir, not {remote:?}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::BUILDER_RPC;

    fn producer(remote: Option<&str>, features: &[&str]) -> BuildAssignment {
        BuildAssignment {
            tmp_dir_in_sandbox: "/build".into(),
            required_features: features.iter().map(|f| (*f).to_owned()).collect(),
            env: remote
                .map(|r| ("NIX_REMOTE".to_owned(), r.to_owned()))
                .into_iter()
                .collect(),
            ..Default::default()
        }
    }

    /// DT12 (DD1, DD3): a producer gets the socket its NIX_REMOTE names in its
    /// tmp dir, whatever the name; one outside it, or none, is refused. A
    /// recursive-nix build or a plain one gets none, NIX_REMOTE or not.
    #[test]
    fn a_producer_socket_is_in_its_tmp_dir_and_only_for_a_producer() {
        let rpc = [BUILDER_RPC];
        let got = |remote, features: &[&str]| rpc_socket(&producer(remote, features)).ok();
        assert_eq!(
            got(Some("unix:///build/.nix-socket"), &rpc),
            Some(Some(".nix-socket".into()))
        );
        assert_eq!(
            got(Some("unix:///build/rpc"), &rpc),
            Some(Some("rpc".into()))
        );
        for bad in [
            "unix:///tmp/.nix-socket",
            "unix:///build/a/b",
            "daemon",
            "unix:///build/..",
        ] {
            assert_eq!(got(Some(bad), &rpc), None, "{bad}");
        }
        assert_eq!(got(None, &rpc), None);
        let socket = Some("unix:///build/.nix-socket");
        assert_eq!(got(socket, &["recursive-nix"]), Some(None));
        assert_eq!(got(socket, &[]), Some(None));
    }
}
