//! Parser for the build.json document written by Nix's external-builders
//! feature (version 1). See
//! `nix/src/libstore/unix/build/external-derivation-builder.cc`.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use harmonia_store_content_address::{
    ContentAddress, ContentAddressMethod, make_store_path_from_ca,
};
use harmonia_store_path::{StoreDir, StorePath};
use harmonia_utils_hash::fmt::Any;
use harmonia_utils_hash::{Algorithm, Hash};
use serde::Deserialize;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("reading {path}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("parsing build.json")]
    Parse(#[from] serde_json::Error),
    #[error("unsupported build.json version {0}")]
    UnsupportedVersion(u32),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildJson {
    pub version: u32,
    pub builder: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub top_tmp_dir: PathBuf,
    pub tmp_dir_in_sandbox: PathBuf,
    pub store_dir: String,
    #[serde(default)]
    pub real_store_dir: Option<String>,
    pub system: String,
    pub input_paths: Vec<String>,
    /// Output name -> scratch store path. The same scratch paths must be
    /// populated on the client; Nix rewrites and registers them afterwards.
    pub outputs: BTreeMap<String, String>,
    /// Nix's timeout and max-silent-time, 0 for none, which Nix leaves to
    /// the external builder (nix/patches/external-builders-timeouts.patch);
    /// absent, Nix enforces them itself.
    #[serde(default)]
    pub timeout: u64,
    #[serde(default)]
    pub max_silent_time: u64,
    /// Whether Nix builds the derivation itself if attach declines it,
    /// by the rule it then applies; absent from a Nix that does not say.
    #[serde(default)]
    pub can_build_locally: Option<bool>,
    /// Who asked for the build (BuildRequest's client), where the Nix
    /// that wrote this knows; absent, attach names the nix-daemon
    /// connection running it.
    #[serde(default)]
    pub client: Option<String>,
    /// The derivation's store path, where Nix writes it: for logging and
    /// the hub's record only (DESIGN.md, R1).
    #[serde(default)]
    pub drv_path: Option<String>,
}

impl BuildJson {
    pub fn load(path: &Path) -> Result<Self, Error> {
        let data = fs::read_to_string(path).map_err(|source| Error::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let parsed: Self = serde_json::from_str(&data)?;
        if parsed.version != 1 {
            return Err(Error::UnsupportedVersion(parsed.version));
        }
        Ok(parsed)
    }

    pub fn attrs(&self) -> Option<serde_json::Value> {
        let has = self.env.contains_key("NIX_ATTRS_JSON_FILE");
        read_attrs(&self.top_tmp_dir.join("build"), has)
    }

    /// Whether the build is told fixed-output, and so gets network, if it
    /// declares a content hash (fixed_output). An `__impure` one gets none: Nix
    /// takes that attr at evaluation, so it never reaches build.json, and one
    /// there was set by hand.
    pub fn fixed_output(&self, attrs: Option<&serde_json::Value>) -> Option<bool> {
        let env = |name: &str| self.env.get(name).map(String::as_str);
        fixed_output(&self.store_dir, &self.outputs, env, attrs)
    }
}

/// The structured attrs Nix wrote in the build's tmp dir `build_dir`, if
/// it `has` them (NIX_ATTRS_JSON_FILE in its env).
pub fn read_attrs(build_dir: &Path, has: bool) -> Option<serde_json::Value> {
    let data = fs::read(build_dir.join(".attrs.json"))
        .ok()
        .filter(|_| has)?;
    serde_json::from_slice(&data).ok()
}

/// Whether a derivation that declares a content hash (outputHash, from
/// `env` or structured attrs; None if none) is fixed-output, from what
/// reaches its builder: its one output, `out`, is at that hash's store
/// path (by outputHashAlgo, outputHashMode). No other derivation's output
/// lands there, whatever its env says; one at another path, or hashed with
/// git, is not told (Some(false)), and attach says so in its log.
pub fn fixed_output<'a>(
    store_dir: &str,
    outputs: impl IntoIterator<Item = (&'a String, &'a String)>,
    env: impl Fn(&str) -> Option<&'a str>,
    attrs: Option<&serde_json::Value>,
) -> Option<bool> {
    let attr = |name: &str| {
        let from_attrs = || attrs?.get(name)?.as_str().map(str::to_owned);
        env(name).map(str::to_owned).or_else(from_attrs)
    };
    let hash = attr("outputHash")?;
    let mut outputs = outputs.into_iter();
    let (Some((name, out)), None) = (outputs.next(), outputs.next()) else {
        return Some(false);
    };
    let fixed = || -> Option<bool> {
        let store = StoreDir::new(store_dir).ok()?;
        let out: StorePath = store.parse(out).ok()?;
        let algo = attr("outputHashAlgo").unwrap_or_default();
        // An empty hash is the zero hash of outputHashAlgo, as Nix takes it
        // (newHashAllowEmpty), as in buildGoModule's `vendorHash = ""`.
        let hash = match hash.as_str() {
            "" => Ok(Hash::new(algo.parse::<Algorithm>().ok()?, &[0; 64])),
            _ => hash
                .parse::<Any<Hash>>()
                .or_else(|_| format!("{algo}:{hash}").parse())
                .map(Any::into_hash),
        };
        let method = match attr("outputHashMode").as_deref() {
            None | Some("" | "flat") => ContentAddressMethod::Flat,
            Some("recursive" | "nar") => ContentAddressMethod::NixArchive,
            Some("text") => ContentAddressMethod::Text,
            Some(_) => return None,
        };
        let ca = ContentAddress::from_hash(method, hash.ok()?).ok()?;
        Some(make_store_path_from_ca(&store, out.name().clone(), ca) == out)
    };
    Some(name == "out" && fixed() == Some(true))
}

/// A boolean derivation attr from the env (`"1"`) or structured attrs.
pub fn flag(env: &BTreeMap<String, String>, attrs: Option<&serde_json::Value>, name: &str) -> bool {
    env.get(name).map(String::as_str) == Some("1")
        || attrs
            .and_then(|a| a.get(name))
            .and_then(serde_json::Value::as_bool)
            == Some(true)
}

/// `requiredSystemFeatures` from the plain env var or structured attrs.
pub fn required_system_features(
    env: &BTreeMap<String, String>,
    attrs: Option<&serde_json::Value>,
) -> Vec<String> {
    if let Some(features) = env.get("requiredSystemFeatures") {
        return features.split_whitespace().map(str::to_owned).collect();
    }
    if let Some(features) = attrs
        .and_then(|a| a.get("requiredSystemFeatures"))
        .and_then(|v| v.as_array())
    {
        return features
            .iter()
            .filter_map(|f| f.as_str().map(str::to_owned))
            .collect();
    }
    Vec::new()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn system_features_from_plain_env() {
        let env = BTreeMap::from([(
            "requiredSystemFeatures".to_owned(),
            "kvm big-parallel".to_owned(),
        )]);
        assert_eq!(
            required_system_features(&env, None),
            ["kvm", "big-parallel"]
        );
    }

    #[test]
    fn system_features_from_structured_attrs() {
        let attrs = serde_json::json!({"requiredSystemFeatures": ["kvm"]});
        assert_eq!(
            required_system_features(&BTreeMap::new(), Some(&attrs)),
            ["kvm"]
        );
        assert!(required_system_features(&BTreeMap::new(), None).is_empty());
    }

    pub(crate) fn doc(env: &serde_json::Value) -> BuildJson {
        serde_json::from_value(written(env)).unwrap()
    }

    /// A build.json as Nix writes it, of a build with `env`.
    fn written(env: &serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "version": 1,
            "builder": "/bin/sh",
            "args": [],
            "env": env,
            "topTmpDir": "/tmp/x",
            "tmpDirInSandbox": "/build",
            "storeDir": "/nix/store",
            "system": "x86_64-linux",
            "inputPaths": [],
            "outputs": {},
        })
    }

    /// Nix's own output paths (`nix eval` of each derivation's outPath):
    /// a fetchurl-like flat sha256 in base16, an SRI one, the same hash
    /// recursive, and an input-addressed derivation.
    const FLAT: &str = "/nix/store/j04yfblg6sk5abb4n067xv0x0dfraf73-libssh2-1.11.1.tar.gz";
    pub(crate) const SRI: &str = "/nix/store/vsh9lc2l099l6fwp6rxcfvr2v7pbgp0y-src";
    const NAR: &str = "/nix/store/gsk17yzsgr98iadxvr4nq89nzyy81x8f-src";
    pub(crate) const INPUT_ADDRESSED: &str = "/nix/store/3am3whmm9lr7m3mi4w3ahdh4q12iyzk0-src";
    pub(crate) const HASH: &str = "sha256-2ex2y+NNuY7sNTn+LImdJrDIN8s+tGalsPEJyr9mWPc=";
    /// An empty sha256 outputHash, flat for `x` and recursive for `e`.
    const EMPTY_FLAT: &str = "/nix/store/yplc6kklcg6k8aln037jq8jxq5v92wln-x";
    const EMPTY_NAR: &str = "/nix/store/lzvy3q0z642kia65iz3h8yzb10dqv03y-e";
    /// HASH git-hashed, at the path Nix gives it, which this does not derive.
    const GIT: &str = "/nix/store/j5vd9d8h1fdinlacxdi4i012b3gaybbv-src";

    /// Whether `env` gives a build whose output is `out` network.
    fn network(env: &serde_json::Value, out: &str) -> Option<bool> {
        let mut doc = doc(env);
        doc.outputs = BTreeMap::from([("out".to_owned(), out.to_owned())]);
        doc.fixed_output(None)
    }

    /// Network is the derivation's being fixed-output, which its output
    /// path proves; what its env says does not.
    #[test]
    fn a_build_has_network_iff_its_output_is_fixed_by_its_hash() {
        let net = |env, out| network(&env, out) == Some(true);
        let flat = serde_json::json!({
            "outputHash": "d9ec76cbe34db98eec3539fe2c899d26b0c837cb3eb466a56b0f109cabf658f7",
            "outputHashAlgo": "sha256",
        });
        assert!(net(flat, FLAT));
        assert!(net(serde_json::json!({"outputHash": HASH}), SRI));
        let nar = serde_json::json!({"outputHash": HASH, "outputHashMode": "recursive"});
        assert!(net(nar.clone(), NAR));
        let empty = serde_json::json!({"outputHash": "", "outputHashAlgo": "sha256"});
        assert!(net(empty.clone(), EMPTY_FLAT));
        let mut empty_nar = empty;
        empty_nar["outputHashMode"] = "recursive".into();
        assert!(net(empty_nar, EMPTY_NAR));
        // Not told: a hash at another path (a flat one for a recursive claim)
        // or git hashing; none declared is not.
        let git = serde_json::json!({"outputHash": HASH, "outputHashMode": "git"});
        let told = [(nar, SRI), (git, GIT), (serde_json::json!({}), NAR)];
        let want = [Some(false), Some(false), None];
        assert_eq!(told.map(|(env, out)| network(&env, out)), want);
        // An input-addressed derivation that sets what Nix sets for a
        // fixed-output one, and a hash, gets none.
        let spoof = serde_json::json!({"NIX_OUTPUT_CHECKED": "1", "outputHash": HASH});
        assert!(!net(spoof, INPUT_ADDRESSED));
        assert!(!net(serde_json::json!({}), INPUT_ADDRESSED));
        // Nor does one declaring itself impure, which no real one does.
        assert!(!net(serde_json::json!({"__impure": "1"}), INPUT_ADDRESSED));
        // From structured attrs too.
        let mut doc = doc(&serde_json::json!({}));
        doc.outputs = BTreeMap::from([("out".to_owned(), NAR.to_owned())]);
        let attrs = serde_json::json!({"outputHash": HASH, "outputHashMode": "nar"});
        assert_eq!(doc.fixed_output(Some(&attrs)), Some(true));
        doc.outputs = BTreeMap::from([("out".to_owned(), INPUT_ADDRESSED.to_owned())]);
        assert!(doc.fixed_output(Some(&serde_json::json!({"__impure": true}))) != Some(true));
    }
}
