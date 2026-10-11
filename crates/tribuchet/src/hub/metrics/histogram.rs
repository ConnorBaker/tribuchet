//! Per-system histograms rendered in the Prometheus text format.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::Mutex;

use super::label;

/// Seconds, from a quick cache hit to a long build.
pub(super) const SECONDS: &[f64] = &[
    0.05, 0.25, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 900.0, 3600.0,
];
/// Bytes, 1 MiB to 64 GiB in steps of 4.
pub(super) const BYTES: &[f64] = &[
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
    268_435_456.0,
    1_073_741_824.0,
    4_294_967_296.0,
    17_179_869_184.0,
    68_719_476_736.0,
];

#[derive(Default)]
pub(super) struct Histogram {
    /// Observations at or below each bound, not cumulative.
    counts: Vec<u64>,
    over: u64,
    sum: f64,
}

pub(super) type Series = Mutex<BTreeMap<String, Histogram>>;

/// Bytes as f64 without a lossy cast: whole MiB through u32, then the rest.
pub(super) fn bytes_f64(v: u64) -> f64 {
    let mib = u32::try_from(v >> 20).unwrap_or(u32::MAX);
    f64::from(mib) * 1_048_576.0 + f64::from(u32::try_from(v & 0xF_FFFF).unwrap_or(0))
}

pub(super) fn observe(series: &Series, bounds: &[f64], system: &str, value: f64) {
    let mut map = series.lock().unwrap();
    let h = map.entry(system.to_owned()).or_default();
    h.counts.resize(bounds.len(), 0);
    match bounds.iter().position(|b| value <= *b) {
        Some(i) => h.counts[i] += 1,
        None => h.over += 1,
    }
    h.sum += value;
}

pub(super) fn render_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    bounds: &[f64],
    series: &Series,
) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} histogram");
    for (system, h) in series.lock().unwrap().iter() {
        let system = label(system);
        let mut seen = 0;
        for (bound, n) in bounds.iter().zip(&h.counts) {
            seen += n;
            let _ = writeln!(
                out,
                "{name}_bucket{{system=\"{system}\",le=\"{bound}\"}} {seen}"
            );
        }
        let total = seen + h.over;
        let _ = writeln!(
            out,
            "{name}_bucket{{system=\"{system}\",le=\"+Inf\"}} {total}"
        );
        let _ = writeln!(out, "{name}_sum{{system=\"{system}\"}} {}", h.sum);
        let _ = writeln!(out, "{name}_count{{system=\"{system}\"}} {total}");
    }
}
