//! Namespaced store state wrapper.

use std::collections::BTreeMap;

use super::{CanonicalState, NamespaceId, NonCoreNamespaceId, WriteStamp};

#[derive(Clone, Debug)]
pub struct StoreState {
    core: CanonicalState,
    other: BTreeMap<NamespaceId, CanonicalState>,
}

impl StoreState {
    pub fn new() -> Self {
        Self {
            core: CanonicalState::default(),
            other: BTreeMap::new(),
        }
    }

    pub fn core(&self) -> &CanonicalState {
        &self.core
    }

    pub fn core_mut(&mut self) -> &mut CanonicalState {
        &mut self.core
    }

    pub fn set_core_state(&mut self, state: CanonicalState) {
        self.core = state;
    }

    pub fn get(&self, namespace: &NamespaceId) -> Option<&CanonicalState> {
        if namespace.is_core() {
            Some(&self.core)
        } else {
            self.other.get(namespace)
        }
    }

    pub fn get_mut(&mut self, namespace: &NamespaceId) -> Option<&mut CanonicalState> {
        if namespace.is_core() {
            Some(&mut self.core)
        } else {
            self.other.get_mut(namespace)
        }
    }

    pub fn get_or_default(&self, namespace: &NamespaceId) -> CanonicalState {
        if namespace.is_core() {
            self.core.clone()
        } else {
            self.other.get(namespace).cloned().unwrap_or_default()
        }
    }

    pub fn ensure_namespace(&mut self, namespace: NonCoreNamespaceId) -> &mut CanonicalState {
        self.other.entry(namespace.into_namespace()).or_default()
    }

    pub fn set_namespace_state(&mut self, namespace: NonCoreNamespaceId, state: CanonicalState) {
        self.other.insert(namespace.into_namespace(), state);
    }

    pub fn namespaces(&self) -> impl Iterator<Item = (NamespaceId, &CanonicalState)> {
        std::iter::once((NamespaceId::core(), &self.core)).chain(
            self.other
                .iter()
                .map(|(namespace, state)| (namespace.clone(), state)),
        )
    }

    pub fn max_write_stamp(&self) -> Option<WriteStamp> {
        self.other
            .values()
            .filter_map(|state| state.max_write_stamp())
            .chain(self.core.max_write_stamp().into_iter())
            .max()
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
        assert!(state.get(&core).is_some());
        assert_eq!(state.core(), &CanonicalState::default());
    }

    #[test]
    fn core_namespace_rejects_non_core_wrapper() {
        let core = NamespaceId::core();
        assert!(core.try_non_core().is_none());
    }
}
