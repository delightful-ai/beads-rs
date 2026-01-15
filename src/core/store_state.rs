//! Namespaced store state wrapper.

use std::collections::BTreeMap;

use super::{CanonicalState, NamespaceId, WriteStamp};

#[derive(Clone, Debug, Default)]
pub struct StoreState {
    namespaces: BTreeMap<NamespaceId, CanonicalState>,
}

impl StoreState {
    pub fn new() -> Self {
        Self {
            namespaces: BTreeMap::new(),
        }
    }

    pub fn get(&self, namespace: &NamespaceId) -> Option<&CanonicalState> {
        self.namespaces.get(namespace)
    }

    pub fn get_mut(&mut self, namespace: &NamespaceId) -> Option<&mut CanonicalState> {
        self.namespaces.get_mut(namespace)
    }

    pub fn get_or_default(&self, namespace: &NamespaceId) -> CanonicalState {
        self.namespaces.get(namespace).cloned().unwrap_or_default()
    }

    pub fn ensure_namespace(&mut self, namespace: NamespaceId) -> &mut CanonicalState {
        self.namespaces.entry(namespace).or_default()
    }

    pub fn set_namespace_state(&mut self, namespace: NamespaceId, state: CanonicalState) {
        *self.ensure_namespace(namespace) = state;
    }

    pub fn namespaces(&self) -> impl Iterator<Item = (&NamespaceId, &CanonicalState)> {
        self.namespaces.iter()
    }

    pub fn max_write_stamp(&self) -> Option<WriteStamp> {
        self.namespaces
            .values()
            .filter_map(|state| state.max_write_stamp())
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_namespace_is_explicit() {
        let mut state = StoreState::new();
        let core = NamespaceId::core();
        assert_eq!(core.as_str(), "core");
        assert!(state.get(&core).is_none());

        state.ensure_namespace(core.clone());
        assert!(state.get(&core).is_some());
    }
}
