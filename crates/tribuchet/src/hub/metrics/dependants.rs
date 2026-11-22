//! What the hub learns of dependants (placement/downstream.rs), as metrics.

use super::histogram::{Series, observe, render_histogram};
use super::metric;
use crate::hub::state::HubState;

/// Seconds of a learned chain, a minute to a day: the starvation bound is
/// as long as the longest.
const BELOW: &[f64] = &[
    60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0, 14400.0, 28800.0, 43200.0, 86400.0,
];

/// The succeeded runs that named a producer the hub ran, and those that
/// did not (placement/downstream.rs): what teaches a key's critical path;
/// and, by system, the learned seconds of each key something was learned
/// to follow (a key with none is ordered as it came, and bounds no wait),
/// the paths the queue's order compares and so its starvation bound
/// (placement/objective.rs). Copied out under the placement lock, observed
/// after it.
pub(super) fn render(out: &mut String, state: &HubState) {
    let (followed, learned): (Vec<(String, f64)>, _) = {
        let p = state.placement.lock().unwrap();
        let system = |key: &str| key.split_once('/').map_or(key, |k| k.0).to_owned();
        let followed = p.downstream.belows().filter(|(_, s)| *s > 0.0);
        let learned = [
            ("matched", p.downstream.matched),
            ("unmatched", p.downstream.unmatched),
        ];
        (followed.map(|(k, s)| (system(k), s)).collect(), learned)
    };
    let below = Series::default();
    for (system, secs) in followed {
        observe(&below, BELOW, &system, secs);
    }
    let help = "Seconds learned to follow each key: its builds' path in the queue's order.";
    render_histogram(out, "tribuchet_hub_below_seconds", help, BELOW, &below);
    let help =
        "Succeeded runs with (matched) and without (unmatched) an input a run on this hub made.";
    for (which, n) in learned {
        let name = format!("tribuchet_hub_dependants_{which}_total");
        metric(out, &name, "counter", help, n);
    }
}
