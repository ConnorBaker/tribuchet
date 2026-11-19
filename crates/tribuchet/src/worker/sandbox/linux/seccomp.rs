//! Nix's build-sandbox system-call filter (`setupSeccomp` in Nix's
//! linux-derivation-builder.cc), rule for rule through the same
//! library, so a build gets the answers a stock Nix build gets.

use std::io;

use libseccomp::error::SeccompError;
use libseccomp::{
    ScmpAction, ScmpArch, ScmpArgCompare, ScmpCompareOp, ScmpFilterContext, ScmpSyscall,
};

pub(super) fn load() -> io::Result<()> {
    filter().map_err(|e| io::Error::other(format!("seccomp: {e}")))
}

fn filter() -> Result<(), SeccompError> {
    let mut ctx = ScmpFilterContext::new(ScmpAction::Allow)?;
    // The compat ABIs a native build can run code for: the filter kills
    // a system call of an architecture it lacks. Nix only warns when
    // adding ARM fails, but libseccomp fails that only out of memory.
    #[cfg(target_arch = "x86_64")]
    ctx.add_arch(ScmpArch::X86)?.add_arch(ScmpArch::X32)?;
    #[cfg(target_arch = "aarch64")]
    ctx.add_arch(ScmpArch::Arm)?;
    // No setuid/setgid files. The mode is argument 1 of chmod and
    // fchmod, argument 2 of fchmodat and fchmodat2.
    for perm in [libc::S_ISUID, libc::S_ISGID].map(u64::from) {
        for (call, arg) in [
            ("chmod", 1),
            ("fchmod", 1),
            ("fchmodat", 2),
            ("fchmodat2", 2),
        ] {
            ctx.add_rule_conditional(
                ScmpAction::Errno(libc::EPERM),
                ScmpSyscall::from_name(call)?,
                &[ScmpArgCompare::new(
                    arg,
                    ScmpCompareOp::MaskedEqual(perm),
                    perm,
                )],
            )?;
        }
    }
    // No extended attributes or ACLs: NARs cannot represent them.
    for call in [
        "listxattr",
        "llistxattr",
        "flistxattr",
        "getxattr",
        "lgetxattr",
        "fgetxattr",
        "setxattr",
        "lsetxattr",
        "fsetxattr",
    ] {
        ctx.add_rule(
            ScmpAction::Errno(libc::ENOTSUP),
            ScmpSyscall::from_name(call)?,
        )?;
    }
    // no_new_privs, which libseccomp also sets, is already on
    ctx.load()
}
