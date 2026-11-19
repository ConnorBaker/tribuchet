//! Builds no worker serves: the hub declines them (exit 222) to a client
//! whose Nix says it builds them itself (canBuildLocally), and refuses them
//! to one whose Nix cannot, while no known worker serves them (L11).

use crate::harness::{Node, assert_journal, build_grep, fail, run, succeed};

#[test]
fn build_kvm() {
    if run(Node::Worker, "test -e /dev/kvm").code == 0 {
        build_grep("/etc/tt/kvm.nix", "kvm-ok");
    } else {
        // No worker serves the kvm feature and the hub cannot build it
        // locally, so the build is refused.
        let err = fail(Node::Hub, "nix-build /etc/tt/kvm.nix --no-out-link 2>&1");
        assert!(
            err.contains("no worker this hub knows builds x86_64-linux"),
            "{err}"
        );
    }
}

#[test]
fn build_kvm_emulated() {
    // An emulated system must not inherit the host's kvm feature.
    let err = fail(
        Node::Hub,
        "nix-build /etc/tt/kvm-emulated.nix --no-out-link 2>&1",
    );
    assert!(
        err.contains("no worker this hub knows builds aarch64-linux"),
        "{err}"
    );
    declined("tt-kvm.*aarch64-linux", "aarch64-linux", false);
}

/// No worker served the build of `drv` for `system`, and its client said
/// its Nix would build it itself or not (`can_local`), as Nix wrote in each
/// build.json for it (nix/test.nix keeps a copy of each): the hub declined
/// it, or refused it.
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
    let line = if can_local {
        format!("no capable worker; declining.*{system}.*can_local=true")
    } else {
        format!("no known worker; refusing.*{system}")
    };
    assert_journal(Node::Hub, "tribuchet-hub", &line);
}
