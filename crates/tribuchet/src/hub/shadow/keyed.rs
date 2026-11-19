//! A map reached only by key: its storage is private and its methods take
//! one key each (`slot`, `insert`, `remove`, `get`), so no code outside
//! this file can iterate, retain or drain it. Every per-build map of
//! the shadow's `Mirror` is one (`runs`, `hub_runs`, `pending`,
//! `pending_of`, `seen`), so a shadow call touches the builds it is about
//! and scans none of the others in these maps (11343c84 removed such
//! scans). Not covered: the core's own `Hub` (semantics.rs, its `builds`
//! and `runs` public; `Mirror::touches` scans its `runs`), and the
//! Mirror's per-worker and per-system maps (`names`, `systems`, `caps`).

use std::collections::HashMap;
use std::hash::Hash;

pub(super) struct Keyed<K, V>(HashMap<K, V>);

impl<K, V> Default for Keyed<K, V> {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl<K: Hash + Eq, V> Keyed<K, V> {
    /// `k`'s value, a default one put in if it had none.
    pub(super) fn slot(&mut self, k: K) -> &mut V
    where
        V: Default,
    {
        self.0.entry(k).or_default()
    }

    pub(super) fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.0.insert(k, v)
    }

    pub(super) fn remove(&mut self, k: &K) -> Option<V> {
        self.0.remove(k)
    }

    pub(super) fn get(&self, k: &K) -> Option<&V> {
        self.0.get(k)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
}
