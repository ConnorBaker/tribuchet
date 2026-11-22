//! Keys by stem, and by the sources a build consumes within its stem.

use super::*;

/// The stem keeps every dash-part that starts with a letter and is no
/// hash, before a version and after it.
#[test]
fn the_stem_drops_versions_and_hashes_wherever_they_are() {
    assert_eq!(stem("ghc-9.14.1"), "ghc");
    assert_eq!(stem("ghc-9.14.1-with-packages"), "ghc-with-packages");
    assert_eq!(stem("ghc-9.10.3-source"), "ghc-source");
    assert_eq!(stem("linux-6.18.59-modules-shrunk"), "linux-modules-shrunk");
    assert_eq!(stem("tu-bbed23782ae68907-x"), "tu-x");
    assert_eq!(stem("7z-2-x"), "7z-x");
}

/// Where Nix's cut finds the name before its version, the stem is it.
#[test]
fn the_stem_of_a_name_ending_in_its_version_is_the_cut() {
    assert_eq!(stem("nix-expr-2.36.0pre"), "nix-expr");
    assert_eq!(stem("python3.14-torch-2.9.1"), "python3.14-torch");
    assert_eq!(stem("content-observation.json"), "content-observation.json");
    assert_eq!(stem("cuda13.4-libnvshmem-3.4"), "cuda13.4-libnvshmem");
    // A hash ends it too; a word with a digit not.
    assert_eq!(stem("tu-bbed23782ae68907"), "tu");
    assert_eq!(stem("jctrim-rel-f8bdb806db3a"), "jctrim-rel");
    assert_eq!(stem("cargo-package-chacha20-0.9"), "cargo-package-chacha20");
    assert_eq!(stem("python3.13-secp256k1-0.14"), "python3.13-secp256k1");
    assert_eq!(stem("x-goldres2"), "x-goldres2");
    assert_eq!(
        stem("python3.14-jax-cuda13-plugin-0.7"),
        "python3.14-jax-cuda13-plugin"
    );
    assert_eq!(stem("jca-mut-s1"), "jca-mut-s1");
    assert_eq!(
        stem("server-aarch64-unknown-linux-gnu-0.1"),
        "server-aarch64-unknown-linux-gnu"
    );
    assert_eq!(stem("tu-Bbed23782ae68907"), "tu-Bbed23782ae68907");
    assert_eq!(stem("tu-bbedabcd"), "tu-bbedabcd");
    assert_eq!(stem("tu-bbed2378"), "tu");
    assert_eq!(stem("tu-bbed237"), "tu-bbed237");
}

/// A compiler and its wrapper share Nix's cut, `ghc`, and not a stem:
/// each expects its own runs (the hub's record, 2026-11-20: the
/// compiler, 5,129 s, was expected to take the wrapper's 3.1 s). A stem
/// with no run takes the cut's runs, a prefix of it, pooled.
#[test]
fn a_wrapper_and_its_compiler_are_two_keys() {
    let mut h = History::default();
    h.record(&build("ghc-9.14.1", false), 0, &usage(2, 5000, 4), None);
    for _ in 0..5 {
        h.record(
            &build("ghc-9.14.1-with-packages", false),
            0,
            &usage(1, 2, 1),
            None,
        );
    }
    let e = h.expect(&build("ghc-9.14.1", false));
    assert_eq!(
        (e.level, e.secs),
        (Level::Key, Some(5000.0)),
        "the compiler"
    );
    let e = h.expect(&build("ghc-9.14.1-with-packages", false));
    assert_eq!((e.level, e.secs), (Level::Key, Some(2.0)), "its wrapper");
    let e = h.expect(&build("ghc-9.14.1-doc", false));
    assert_eq!((e.level, e.secs, gib(e)), (Level::Prefix, Some(2.0), 4.0));
}

/// A build of a stem that consumed the same content-addressed inputs
/// before expects those runs; one of other inputs, or of none known,
/// its stem's, all of them pooled; and its runs teach the stem too.
#[test]
fn the_runs_of_its_sources_come_first() {
    let mut h = History::default();
    let of = |sources| Build {
        sources,
        ..build("program-observation.json", false)
    };
    h.record(&of(Some("big")), 0, &usage(2, 1000, 9), None);
    for _ in 0..4 {
        h.record(&of(Some("small")), 0, &usage(1, 1, 1), None);
    }
    let e = h.expect(&of(Some("big")));
    assert_eq!(
        (e.level, e.secs, gib(e)),
        (Level::Source, Some(1000.0), 9.0)
    );
    let e = h.expect(&of(Some("small")));
    assert_eq!((e.level, e.secs, gib(e)), (Level::Source, Some(1.0), 1.0));
    for unseen in [Some("other"), None] {
        let e = h.expect(&of(unseen));
        assert_eq!(
            (e.level, e.secs, gib(e)),
            (Level::Key, Some(1.0), 9.0),
            "{unseen:?}"
        );
    }
    // Sources are a stem's: another stem's runs of the same inputs are
    // not this one's.
    let e = h.expect(&Build {
        sources: Some("big"),
        ..build("program-listing.json", false)
    });
    assert_eq!(e.level, Level::Prefix, "another stem's sources");
}

/// Sources without runs, or with no run that succeeded, never read as
/// nothing ran: the build takes its key's runs, else a prefix's, else
/// its class's, as one without sources does, with at least the memory
/// its sources' OOM-killed runs peaked at.
#[test]
fn sources_without_a_succeeded_run_fall_through_every_level() {
    let mut h = History::default();
    let of = |name, sources| Build {
        sources,
        ..build(name, false)
    };
    h.record(&build("other-1", false), 0, &usage(1, 7, 1), None);
    let e = h.expect(&of("tool-run-1", Some("d")));
    assert_eq!((e.level, e.secs), (Level::Class, Some(7.0)));
    h.record(&build("tool-check-1", false), 0, &usage(1, 5, 1), None);
    let e = h.expect(&of("tool-run-1", Some("d")));
    assert_eq!((e.level, e.secs), (Level::Prefix, Some(5.0)));
    h.record(&build("tool-run-1", false), 0, &usage(1, 3, 1), None);
    let mut oom = usage(1, 1, 9);
    oom.oom_killed = true;
    h.record(&of("tool-run-2", Some("d")), 0, &oom, None);
    let e = h.expect(&of("tool-run-3", Some("d")));
    assert_eq!((e.level, e.secs, gib(e)), (Level::Key, Some(3.0), 9.0));
    h.record(&of("tool-run-4", Some("d")), 0, &usage(1, 40, 2), None);
    let e = h.expect(&of("tool-run-5", Some("d")));
    assert_eq!((e.level, e.secs, gib(e)), (Level::Source, Some(40.0), 9.0));
}

/// A source whose only run was OOM-killed at 9 GiB is expected at least
/// that, though its stem's window has since filled with 1 GiB runs of
/// other sources: an OOM-killed run's peak is a lower bound on what it
/// needs, and the only one (from the blind review's probe).
#[test]
fn an_oom_killed_source_keeps_its_lower_bound() {
    let mut h = History::default();
    let of = |sources| Build {
        sources,
        ..build("program-observation.json", false)
    };
    let mut oom = usage(1, 1, 9);
    oom.oom_killed = true;
    h.record(&of(Some("big")), 0, &oom, None);
    for n in 0..WINDOW {
        let small = format!("small{n}");
        let b = Build {
            sources: Some(&small),
            ..build("program-observation.json", false)
        };
        h.record(&b, 0, &usage(1, 1, 1), None);
    }
    let e = h.expect(&of(Some("big")));
    assert_eq!((e.level, e.secs, gib(e)), (Level::Key, Some(1.0), 9.0));
}
