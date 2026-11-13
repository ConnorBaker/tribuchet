//! Builds the hub declines (exit 222), and what the client's Nix says of
//! building them itself (canBuildLocally), which the hub is told.

use crate::harness::{Node, assert_journal, build_grep, fail, run, succeed};

#[test]
fn build_kvm() {
    if run(Node::Worker, "test -e /dev/kvm").code == 0 {
        build_grep("/etc/tt/kvm.nix", "kvm-ok");
    } else {
        // No worker serves the kvm feature and the hub cannot build it
        // locally, so the declined build (exit 222) fails.
        let err = fail(Node::Hub, "nix-build /etc/tt/kvm.nix --no-out-link 2>&1");
        assert!(err.contains("exit code 222"), "{err}");
    }
}

#[test]
fn build_kvm_emulated() {
    // An emulated system must not inherit the host's kvm feature.
    let err = fail(
        Node::Hub,
        "nix-build /etc/tt/kvm-emulated.nix --no-out-link 2>&1",
    );
    assert!(err.contains("exit code 222"), "{err}");
    declined("tt-kvm.*aarch64-linux", "aarch64-linux", false);
}

/// The hub declined the build of `drv` for `system`, told by its client
/// that its Nix would build it itself or not (`can_local`), as Nix wrote in
/// each build.json for it (nix/test.nix keeps a copy of each).
pub(crate) fn declined(drv: &str, system: &str, can_local: bool) {
    let said = succeed(
        Node::Hub,
        &format!(
            "grep -l '{drv}' /var/lib/tt-buildjson/* | xargs -r grep -ho '\"canBuildLocally\":[a-z]*' \
             | sort -u"
        ),
    );
    let said = said.trim().replace("\"canBuildLocally\":", "");
    assert_eq!(said, can_local.to_string());
    let line = format!("no capable worker; declining.*{system}.*can_local={can_local}");
    assert_journal(Node::Hub, "tribuchet-hub", &line);
}
