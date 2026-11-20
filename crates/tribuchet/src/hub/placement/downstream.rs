//! What follows a build: the running seconds of the longest chain of
//! builds learned to depend on its key, the tail of its upward rank
//! (HEFT, Topcuoglu, Hariri and Wu, IEEE TPDS 13(3), 2002; HLFET's static
//! level with expected seconds as costs).
//!
//! Nix hands the hub a build only once its inputs exist, so a waiting
//! build's dependants are never in the queue with it: the hub learns them.
//! A run that succeeds lists its inputs, the closure its builder sees
//! (build.json's inputPaths). An input that is an output of one of the
//! last WINDOW runs of a key here was made by that key (its own system
//! and pname); each such key is then followed by this build's expected
//! seconds and the chain that follows this build's key:
//!
//!   below(k) = max over builds d, finished since k's own last run, with an
//!              input made by k, of secs(d) + below(key(d)), the chain
//!              through d being a chain of keys none of which is k;
//!
//! and, where none has finished since that run, what the runs before it
//! left: what followed k's latest run, not the longest any run of k ever
//! started (a key that runs often, as `source` does, keeps its latest).
//! Keys are pnames, and a closure lists transitive inputs, so keys can
//! name each other: a chain that would come back to k counts d alone.
//! Every `below` is thus the learned seconds of a chain of distinct keys,
//! and a cycle of keys cannot add to it. A closure's transitive inputs
//! give shorter chains than the direct one, so the max is unchanged. A
//! chain of n builds is learned over at most n - 1 runs of it; below a
//! key no run has followed it is 0.
//!
//! Keys join chains no one build is on: if one build of b follows a, and
//! another build of b is followed by c, below(a) counts b then c. It is
//! then longer than any chain one run went down.
//!
//! What is never learned: a producer whose output path is not the one its
//! dependants list (a floating content-addressed build, whose scratch path
//! is not its final one), a producer this hub did not run (substituted, or
//! built elsewhere), and one whose run is older than its key's last WINDOW.
//! `matched` and `unmatched` count succeeded runs with and without an input
//! a run here made.
//!
//! Not part of the record's fold: a restarted hub learns it anew.

use std::collections::{HashMap, VecDeque};
use std::iter;
use std::sync::Arc;

// Runs whose outputs are kept per key: those the history keeps.
use super::history::WINDOW;

/// What follows each key, by history key, and who made which output.
#[derive(Default)]
pub(in crate::hub) struct Downstream {
    below: HashMap<Arc<str>, Below>,
    /// Output path -> the key whose run made it, for each key's last
    /// WINDOW runs.
    made: HashMap<String, Arc<str>>,
    /// Each key's last WINDOW runs' outputs, oldest first.
    runs: HashMap<Arc<str>, VecDeque<Vec<String>>>,
    /// Succeeded runs learned from: with an input a run here made, and
    /// without one.
    pub(in crate::hub) matched: u64,
    pub(in crate::hub) unmatched: u64,
}

#[derive(Default)]
struct Below {
    secs: f64,
    /// The keys of the chain `secs` is the seconds of, first first.
    chain: Vec<Arc<str>>,
    /// The key ran since a build depending on it last finished: the next
    /// such build starts its max afresh.
    ran: bool,
}

impl Downstream {
    /// The running seconds learned to follow a build of `key`.
    pub(in crate::hub) fn below(&self, key: &str) -> f64 {
        self.below.get(key).map_or(0.0, |b| b.secs)
    }

    /// Every key's learned seconds, by key.
    pub(in crate::hub) fn belows(&self) -> impl Iterator<Item = (&str, f64)> {
        (self.below.iter()).map(|(k, b)| (&**k, b.secs))
    }

    /// A build of `key`, expected to run `secs`, succeeded, making
    /// `outputs` from `inputs` (store paths).
    pub(in crate::hub) fn learn(
        &mut self,
        key: &str,
        secs: f64,
        outputs: impl IntoIterator<Item = String>,
        inputs: &[String],
    ) {
        let key: Arc<str> = key.into();
        let mine = self.below.get(&key);
        let (after, tail) = mine.map_or((0.0, Vec::new()), |b| (b.secs, b.chain.clone()));
        let through = secs + after;
        let chain: Vec<Arc<str>> = iter::once(key.clone())
            .chain(tail.iter().cloned())
            .collect();
        // A producer's several outputs relax it alike, more than once.
        let made = |p: &String| self.made.get(p).filter(|k| **k != key).cloned();
        let by: Vec<Arc<str>> = inputs.iter().filter_map(made).collect();
        if by.is_empty() {
            self.unmatched += 1;
        } else {
            self.matched += 1;
        }
        for input in by {
            // A chain that comes back to `input` counts this build alone.
            let (s, c) = if tail.contains(&input) {
                (secs, vec![key.clone()])
            } else {
                (through, chain.clone())
            };
            let b = self.below.entry(input).or_default();
            if b.ran || s > b.secs {
                (b.secs, b.chain) = (s, c);
            }
            b.ran = false;
        }
        self.below.entry(key.clone()).or_default().ran = true;
        self.made_by(&key, outputs.into_iter().collect());
    }

    /// `key`'s run made `outputs`: its oldest run's are forgotten past
    /// WINDOW, but for those a later run made again. An output path names
    /// its derivation, so one key alone makes it.
    fn made_by(&mut self, key: &Arc<str>, outputs: Vec<String>) {
        for p in &outputs {
            self.made.insert(p.clone(), key.clone());
        }
        let runs = self.runs.entry(key.clone()).or_default();
        runs.push_back(outputs);
        if runs.len() > WINDOW
            && let Some(old) = runs.pop_front()
        {
            for p in old {
                if !runs.iter().any(|r| r.contains(&p)) {
                    self.made.remove(&p);
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp)]
mod tests {
    use super::*;

    /// A run of `key` making `/out/<key>` from the outputs of `inputs`.
    fn run(d: &mut Downstream, key: &str, secs: f64, inputs: &[&str]) {
        let inputs: Vec<String> = inputs.iter().map(|i| format!("/out/{i}")).collect();
        d.learn(key, secs, [format!("/out/{key}")], &inputs);
    }

    #[test]
    fn a_chain_is_learned_one_link_a_run() {
        // a <- b <- c: b depends on a, c on b, 10 s and 20 s.
        let mut d = Downstream::default();
        let all = |d: &mut Downstream| {
            run(d, "a", 1.0, &[]);
            run(d, "b", 10.0, &["a"]);
            run(d, "c", 20.0, &["b", "a"]);
        };
        all(&mut d);
        let below = |d: &Downstream| (d.below("a"), d.below("b"), d.below("c"));
        assert_eq!(below(&d), (20.0, 20.0, 0.0));
        all(&mut d);
        // The closure's transitive edge (c on a) is shorter than a <- b <- c.
        assert_eq!(below(&d), (30.0, 20.0, 0.0));
    }

    #[test]
    fn the_longest_dependant_counts_and_a_new_run_forgets_old_ones() {
        let mut d = Downstream::default();
        run(&mut d, "a", 1.0, &[]);
        run(&mut d, "long", 100.0, &["a"]);
        run(&mut d, "short", 5.0, &["a"]);
        assert_eq!(d.below("a"), 100.0);
        // a runs again and only `short` follows it this time.
        run(&mut d, "a", 1.0, &[]);
        assert_eq!(d.below("a"), 100.0, "kept until a dependant finishes");
        run(&mut d, "short", 5.0, &["a"]);
        assert_eq!(d.below("a"), 5.0);
    }

    /// Two keys each among the other's inputs (pnames of two builds of a
    /// bootstrap, or a closure naming its consumer's pname), built in
    /// turn: below stays the seconds of a chain of distinct keys.
    #[test]
    fn a_cycle_of_keys_stays_bounded() {
        let mut d = Downstream::default();
        for _ in 0..50 {
            run(&mut d, "a", 1.0, &["b"]);
            run(&mut d, "b", 1.0, &["a"]);
        }
        let (a, b) = (d.below("a"), d.below("b"));
        assert!(a <= 2.0 && b <= 2.0, "a {a} b {b}");
        // gcc on glibc and glibc on gcc: a chain of the two at most.
        let mut d = Downstream::default();
        for round in 0..20 {
            run(&mut d, "gcc", 10.0, &["glibc"]);
            run(&mut d, "glibc", 5.0, &["gcc"]);
            let b = d.below("gcc");
            assert!(b <= 15.0, "round {round}: below(gcc) = {b}");
        }
    }

    /// A key that runs often keeps what followed its latest run, not the
    /// longest any of its runs started.
    #[test]
    fn a_key_keeps_its_latest_followers_not_its_longest() {
        let mut d = Downstream::default();
        run(&mut d, "source", 1.0, &[]);
        run(&mut d, "long", 3_600.0, &["source"]);
        d.learn("source", 1.0, [String::from("/out/source2")], &[]);
        d.learn(
            "short",
            5.0,
            [String::from("/out/short")],
            &[String::from("/out/source2")],
        );
        assert_eq!(d.below("source"), 5.0);
    }

    /// Keys join chains no one build is on: one build of b follows a,
    /// another is followed by c, and below(a) counts b then c.
    #[test]
    fn keys_join_chains_no_build_is_on() {
        let mut d = Downstream::default();
        run(&mut d, "a", 1.0, &[]);
        d.learn(
            "b",
            10.0,
            [String::from("/out/b1")],
            &[String::from("/out/a")],
        );
        d.learn("b", 10.0, [String::from("/out/b2")], &[]);
        d.learn(
            "c",
            100.0,
            [String::from("/out/c")],
            &[String::from("/out/b2")],
        );
        d.learn(
            "b",
            10.0,
            [String::from("/out/b3")],
            &[String::from("/out/a")],
        );
        assert_eq!(d.below("a"), 110.0);
    }

    /// A run with no input a run here made, as a floating content-addressed
    /// producer's dependant (its scratch output is not the path listed),
    /// teaches nothing and is counted unmatched; one with such an input,
    /// matched.
    #[test]
    fn runs_are_counted_by_whether_a_producer_was_found() {
        let mut d = Downstream::default();
        d.learn("lib", 1.0, [String::from("/nix/store/scratch-lib")], &[]);
        d.learn(
            "app",
            600.0,
            [String::from("/out/app")],
            &[String::from("/nix/store/ca-lib")],
        );
        assert_eq!(d.below("lib"), 0.0);
        assert_eq!((d.matched, d.unmatched), (0, 2));
        d.learn(
            "app",
            600.0,
            [String::from("/out/app2")],
            &[String::from("/nix/store/scratch-lib")],
        );
        assert_eq!((d.matched, d.unmatched), (1, 2));
    }

    /// An output of any of a key's last WINDOW runs names it, not only
    /// its latest's; one a later run made again outlives the run that made
    /// it first.
    #[test]
    fn a_window_of_runs_names_their_outputs() {
        let mut d = Downstream::default();
        d.learn("a", 1.0, [String::from("/out/a1")], &[]);
        d.learn("a", 1.0, [String::from("/out/a2")], &[]);
        d.learn(
            "c",
            9.0,
            [String::from("/out/c")],
            &[String::from("/out/a1")],
        );
        assert_eq!(d.below("a"), 9.0, "an earlier run in the window");
        let mut d = Downstream::default();
        for _ in 0..=WINDOW {
            d.learn("a", 1.0, [String::from("/out/a")], &[]);
        }
        d.learn(
            "c",
            9.0,
            [String::from("/out/c")],
            &[String::from("/out/a")],
        );
        assert_eq!(d.below("a"), 9.0, "made again within the window");
    }

    #[test]
    fn a_key_does_not_follow_itself() {
        let mut d = Downstream::default();
        run(&mut d, "a", 7.0, &["a"]);
        run(&mut d, "a", 7.0, &["a"]);
        assert_eq!(d.below("a"), 0.0);
    }

    /// Only an output of a key's run here names its producer: an input
    /// the hub never made, or made by a run long gone, teaches nothing.
    #[test]
    fn an_input_is_known_by_the_run_that_made_it() {
        let mut d = Downstream::default();
        run(&mut d, "c", 9.0, &["a"]);
        assert_eq!(d.below("a"), 0.0, "never made here");
        run(&mut d, "a", 1.0, &[]);
        for _ in 0..WINDOW {
            d.learn("a", 1.0, [String::from("/other")], &[]);
        }
        run(&mut d, "c", 9.0, &["a"]);
        assert_eq!(d.below("a"), 0.0, "past the window");
        d.learn(
            "c",
            9.0,
            [String::from("/out/c")],
            &[String::from("/other")],
        );
        assert_eq!(d.below("a"), 9.0);
    }
}
