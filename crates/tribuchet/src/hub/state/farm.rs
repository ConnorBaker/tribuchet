//! The known workers as a farm, for a nix-grpc-daemon that hands this hub
//! its builds and admits them by class (its --farm-file).

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::slice;

use tokio::sync::watch;

use super::super::known::Systems;
use super::{HubState, WorkerCaps, unserved};
use crate::fsutil::write_atomic;

/// One machine per known worker: the features it honors for each system,
/// less unserved ones, and its jobs: 0 exactly while it is not connected, so
/// its builds wait for it there as here; once connected, its max-jobs scaled
/// by `percent`, at least 1, since a product rounding to 0 (max-jobs 1 at 50%)
/// would read there as away and none of its builds would be placed while it
/// serves. Ordered by name, so the same workers write the same bytes.
pub(super) fn render_farm(
    known: &BTreeMap<String, Systems>,
    caps: &HashMap<u64, WorkerCaps>,
    percent: u32,
) -> String {
    let mut jobs: HashMap<&str, u64> = HashMap::new();
    for c in caps.values() {
        let scaled = (u64::from(c.max_jobs) * u64::from(percent) / 100).max(1);
        let have = jobs.entry(c.name.as_str()).or_default();
        *have = (*have).max(scaled);
    }
    let machines: Vec<serde_json::Value> = (known.iter())
        .map(|(name, systems)| {
            let systems: BTreeMap<&str, Vec<&str>> = (systems.iter())
                .map(|(system, have)| {
                    let offered = have.iter().filter(|f| !unserved(slice::from_ref(f)));
                    (system.as_str(), offered.map(String::as_str).collect())
                })
                .collect();
            let jobs = jobs.get(name.as_str()).copied().unwrap_or(0);
            serde_json::json!({"systems": systems, "jobs": jobs.min(u64::from(u32::MAX))})
        })
        .collect();
    format!("{}\n", serde_json::json!({ "machines": machines }))
}

impl HubState {
    /// Mirror the known workers to the farm file, from regen_nix_config,
    /// under the known set's lock and then the connected set's, as there.
    pub(in crate::hub) fn regen_farm(&self) {
        let Some((percent, tx)) = &self.farm else {
            return;
        };
        self.known.with_workers(|known| {
            let caps = self.worker_caps.lock().unwrap();
            tx.send_replace(render_farm(known, &caps, *percent));
        });
    }

    /// The farm file's one writer, as nix_config_writer's.
    pub(in crate::hub) fn farm_writer(path: PathBuf, percent: u32) -> (u32, watch::Sender<String>) {
        let (tx, mut rx) = watch::channel(String::new());
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                let (path, content) = (path.clone(), rx.borrow_and_update().clone());
                let write = move || {
                    if fs::read_to_string(&path).is_ok_and(|cur| cur == content) {
                        return;
                    }
                    if let Err(e) = write_atomic(&path, &content) {
                        tracing::warn!(error = %e, path = %path.display(), "failed to write the farm");
                    }
                };
                drop(tokio::task::spawn_blocking(write).await);
            }
        });
        (percent, tx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Instant;

    use super::*;

    fn caps(name: &str, max_jobs: u32) -> WorkerCaps {
        WorkerCaps {
            name: name.into(),
            connected_at: Instant::now(),
            systems: HashMap::new(),
            max_jobs,
        }
    }

    fn systems(of: &[(&str, &[&str])]) -> Systems {
        (of.iter())
            .map(|(s, f)| {
                (
                    (*s).to_owned(),
                    f.iter().map(|f| (*f).to_owned()).collect::<BTreeSet<_>>(),
                )
            })
            .collect()
    }

    /// The farm file's bytes, in the format nix-grpc-daemon's --farm-file
    /// takes: a machine per known worker with every system it builds, its
    /// features less unserved ones, its jobs scaled, and 0 for one away; by
    /// name.
    #[test]
    fn farm_lists_each_known_worker_with_its_systems_and_scaled_jobs() {
        let known = BTreeMap::from([
            ("slow".to_owned(), systems(&[("aarch64-linux", &["kvm"])])),
            (
                "kvm".to_owned(),
                systems(&[
                    ("x86_64-linux", &["nixos-test", "kvm", "recursive-nix"]),
                    ("i686-linux", &[]),
                ]),
            ),
            (
                "big".to_owned(),
                systems(&[("x86_64-linux", &["big-parallel"])]),
            ),
        ]);
        let workers = HashMap::from([
            (7, caps("kvm", 2)),
            (5, caps("big", 16)),
            (6, caps("big", 8)),
        ]);
        assert_eq!(
            render_farm(&known, &workers, 200),
            concat!(
                r#"{"machines":["#,
                r#"{"jobs":32,"systems":{"x86_64-linux":["big-parallel"]}},"#,
                r#"{"jobs":4,"systems":{"i686-linux":[],"x86_64-linux":["kvm","nixos-test"]}},"#,
                r#"{"jobs":0,"systems":{"aarch64-linux":["kvm"]}}"#,
                "]}\n"
            )
        );
        assert_eq!(
            render_farm(&BTreeMap::new(), &workers, 200),
            "{\"machines\":[]}\n"
        );
    }

    /// A connected worker takes at least 1, whatever its max-jobs and the
    /// percent: only one away takes 0.
    #[test]
    fn a_connected_worker_takes_at_least_one() {
        let known = BTreeMap::from([("w".to_owned(), systems(&[("x86_64-linux", &[])]))]);
        let one = r#"{"machines":[{"jobs":1,"systems":{"x86_64-linux":[]}}]}"#;
        let none = HashMap::from([(1, caps("w", 0))]);
        assert_eq!(render_farm(&known, &none, 200).trim_end(), one);
        let half = HashMap::from([(1, caps("w", 1))]);
        assert_eq!(render_farm(&known, &half, 50).trim_end(), one);
    }
}
