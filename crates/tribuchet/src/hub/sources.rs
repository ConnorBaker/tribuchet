//! What a build consumes by content: its derivation's content-addressed
//! direct inputs, the input sources and the outputs of its input
//! derivations that are content-addressed (sources but those
//! `builtins.storePath` gives; fixed outputs, as fetches and
//! `builtins.path` copies give). History keys a
//! build's runs by them within its stem (placement/history.rs): blob-deps'
//! per-file observations are all named `program-observation.json`, and
//! which program file one reads, a fixed output of its bytes, sets its
//! cost. Replayed on the hub's record of 2026-10-29..11-20, that key
//! took the hour-weighted mean |ln| of their expected over running
//! seconds from 5.09 to 0.64 (placement/history.rs).
//!
//! A request's drv_path (build.json's drvPath, where Nix writes it) may
//! be empty, as the VM tests' Nix leaves it, and build.json's inputPaths
//! are the inputs' closure, not the inputs; the hub's daemon knows both: the derivation
//! whose output the build writes (QueryValidDerivers), its references
//! (its sources and input derivations), each input derivation's outputs
//! (QueryDerivationOutputMap), and which of those it consumes, those in
//! inputPaths, are content-addressed (their path info's `ca`). A lookup
//! is 12 daemon operations for an observation, on one of the hub's
//! pooled connections, as the build is submitted.

use std::collections::HashSet;

use harmonia_store_path::{StoreDir, StorePath};
use harmonia_store_remote::{ConnectionPool, DaemonStore as _, PooledConnectionGuard};
use sha2::{Digest, Sha256};

use super::naming::named_output;
use super::relay::DAEMON_ANSWER as ANSWER;
use crate::proto::BuildRequest;

/// What the lookup asks of a store; None where it does not say.
pub(in crate::hub) trait Store {
    /// The derivations whose output is `path`.
    async fn derivers(&mut self, path: &str) -> Option<Vec<String>>;
    /// `path`'s references, and whether it is content-addressed.
    async fn info(&mut self, path: &str) -> Option<(Vec<String>, bool)>;
    /// The outputs of derivation `drv` that the store knows a path of.
    async fn outputs(&mut self, drv: &str) -> Option<Vec<String>>;
}

/// The digest of the content-addressed direct inputs of `req`'s
/// derivation, sorted: None where it has none, or where `store` does not
/// say (an output already valid is no derivation's in build.json: Nix
/// gives the build a scratch path then).
pub(in crate::hub) async fn sources(store: &mut impl Store, req: &BuildRequest) -> Option<String> {
    let (_, out) = named_output(req)?;
    // Of several, the first the store names: an output path is one
    // derivation's but where two give it alike.
    let drv = store.derivers(out).await?.into_iter().next()?;
    let (references, _) = store.info(&drv).await?;
    let consumed: HashSet<&str> = req.input_paths.iter().map(String::as_str).collect();
    let mut found = Vec::new();
    for r in references {
        if !r.ends_with(".drv") {
            // A source, unless `builtins.storePath` made it one.
            if store.info(&r).await?.1 {
                found.push(r);
            }
            continue;
        }
        for o in store.outputs(&r).await? {
            // An output the build does not consume need not be valid.
            if consumed.contains(o.as_str()) && store.info(&o).await?.1 {
                found.push(o);
            }
        }
    }
    found.sort_unstable();
    (!found.is_empty()).then(|| hex::encode(Sha256::digest(found.join("\n"))))
}

/// The hub's daemon as a Store, on one connection.
struct Daemon(PooledConnectionGuard, StoreDir);

impl Daemon {
    fn parse(&self, path: &str) -> Option<StorePath> {
        self.1.parse(path).ok()
    }

    fn shown(&self, path: &StorePath) -> String {
        self.1.display(path).to_string()
    }
}

impl Store for Daemon {
    async fn derivers(&mut self, path: &str) -> Option<Vec<String>> {
        let p = self.parse(path)?;
        let found = self.0.execute(|c| c.query_valid_derivers(&p)).await.ok()?;
        Some(found.iter().map(|d| self.shown(d)).collect())
    }

    async fn info(&mut self, path: &str) -> Option<(Vec<String>, bool)> {
        let p = self.parse(path)?;
        let info = self.0.execute(|c| c.query_path_info(&p)).await.ok()??;
        let references = info.references.iter().map(|r| self.shown(r)).collect();
        Some((references, info.ca.is_some()))
    }

    async fn outputs(&mut self, drv: &str) -> Option<Vec<String>> {
        let p = self.parse(drv)?;
        let map = (self.0)
            .execute(|c| c.query_derivation_output_map(&p))
            .await
            .ok()?;
        Some(map.values().flatten().map(|o| self.shown(o)).collect())
    }
}

/// `req`'s sources digest from the hub's daemon, as it is submitted:
/// None where it has none, the daemon knows none, or no connection to
/// it comes or it does not answer in ANSWER (those two said in the log).
/// The digest only refines an expectation, so the build goes on without.
pub(in crate::hub) async fn of(pool: &ConnectionPool, req: &BuildRequest) -> Option<String> {
    let lookup = async {
        let conn = match pool.acquire().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::warn!("no content-addressed inputs read: connecting to the daemon: {e}");
                return None;
            }
        };
        sources(&mut Daemon(conn, StoreDir::default()), req).await
    };
    let found = tokio::time::timeout(ANSWER, lookup).await;
    if found.is_err() {
        tracing::warn!(
            secs = ANSWER.as_secs(),
            "no content-addressed inputs read: the daemon did not answer"
        );
    }
    found.ok().flatten()
}

#[cfg(test)]
#[path = "sources/tests.rs"]
mod tests;
