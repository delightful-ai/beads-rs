//! Namespaced store state wrapper.

use std::collections::{BTreeMap, BTreeSet};

use crate::crdt::Crdt;

use super::{CanonicalState, NamespaceId, WriteStamp};

#[derive(Clone, Debug)]
pub struct StoreState {
    by_namespace: BTreeMap<NamespaceId, CanonicalState>,
}

impl Crdt for StoreState {
    fn join(&self, other: &Self) -> Self {
        let mut merged = StoreState::new();
        let mut namespaces: BTreeSet<NamespaceId> = BTreeSet::new();
        namespaces.extend(self.namespaces().map(|(ns, _)| ns.clone()));
        namespaces.extend(other.namespaces().map(|(ns, _)| ns.clone()));

        for namespace in namespaces {
            let left = self.get(&namespace);
            let right = other.get(&namespace);
            let out = match (left, right) {
                (Some(a_state), Some(b_state)) => a_state.join(b_state),
                (Some(state), None) | (None, Some(state)) => state.clone(),
                (None, None) => CanonicalState::default(),
            };
            merged.set_namespace_state(namespace, out);
        }
        merged
    }
}

impl StoreState {
    pub fn new() -> Self {
        let mut by_namespace = BTreeMap::new();
        by_namespace.insert(NamespaceId::core(), CanonicalState::default());
        Self { by_namespace }
    }

    pub fn core(&self) -> &CanonicalState {
        self.by_namespace
            .get(&NamespaceId::core())
            .expect("core namespace missing")
    }

    pub fn core_mut(&mut self) -> &mut CanonicalState {
        self.by_namespace
            .get_mut(&NamespaceId::core())
            .expect("core namespace missing")
    }

    pub fn set_core_state(&mut self, state: CanonicalState) {
        self.by_namespace.insert(NamespaceId::core(), state);
    }

    pub fn get(&self, namespace: &NamespaceId) -> Option<&CanonicalState> {
        self.by_namespace.get(namespace)
    }

    pub fn get_mut(&mut self, namespace: &NamespaceId) -> Option<&mut CanonicalState> {
        self.by_namespace.get_mut(namespace)
    }

    pub fn get_or_default(&self, namespace: &NamespaceId) -> CanonicalState {
        self.by_namespace
            .get(namespace)
            .cloned()
            .unwrap_or_default()
    }

    pub fn ensure_namespace(&mut self, namespace: NamespaceId) -> &mut CanonicalState {
        self.by_namespace.entry(namespace).or_default()
    }

    pub fn set_namespace_state(&mut self, namespace: NamespaceId, state: CanonicalState) {
        self.by_namespace.insert(namespace, state);
    }

    pub fn namespaces(&self) -> impl Iterator<Item = (&NamespaceId, &CanonicalState)> {
        self.by_namespace.iter()
    }

    pub fn max_write_stamp(&self) -> Option<WriteStamp> {
        self.by_namespace
            .values()
            .filter_map(|state| state.max_write_stamp())
            .max()
    }

    /// Deprecated: Use Crdt::join instead.
    pub fn join(a: &Self, b: &Self) -> Self {
        <Self as Crdt>::join(a, b)
    }
}

impl Default for StoreState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_namespace_is_always_present() {
        let state = StoreState::new();
        let core = NamespaceId::core();
        assert_eq!(core.as_str(), "core");
        let core_state = state.core();
        assert_eq!(core_state.live_count(), 0);
        assert_eq!(core_state.tombstone_count(), 0);
        assert_eq!(core_state.dep_count(), 0);
        assert!(state.get(&core).is_some());
    }

    #[test]
    fn core_namespace_rejects_non_core_wrapper() {
        let core = NamespaceId::core();
        assert!(core.try_non_core().is_none());
    }

    #[test]
    fn non_core_namespace_behaves_like_core() {
        let mut state = StoreState::new();
        let namespace = NamespaceId::parse("alpha").unwrap();
        assert!(state.get(&namespace).is_none());

        let default_state = state.get_or_default(&namespace);
        assert_eq!(default_state.live_count(), 0);

        state.ensure_namespace(namespace.clone());
        assert!(state.get(&namespace).is_some());
        assert!(state.get(&NamespaceId::core()).is_some());
    }
}
