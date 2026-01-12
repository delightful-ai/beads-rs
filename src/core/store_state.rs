//! Namespaced store state wrapper.

use std::collections::BTreeMap;

use super::{CanonicalState, NamespaceId};

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

    pub fn ensure_namespace(&mut self, namespace: NamespaceId) -> &mut CanonicalState {
        self.namespaces.entry(namespace).or_default()
    }

    pub fn namespaces(&self) -> impl Iterator<Item = (&NamespaceId, &CanonicalState)> {
        self.namespaces.iter()
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
