//! Namespaced store state wrapper.

use std::collections::{BTreeMap, BTreeSet};

use crate::bead::Bead;
use crate::crdt::Crdt;
use crate::dep::{AcyclicDepKey, DepAddKey, DepKey, FreeDepKey, NoCycleProof};
use crate::error::InvalidDependency;
use crate::identity::BeadRef;

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

    /// Resolve a namespace-qualified bead reference to a live bead.
    pub fn resolve_ref(&self, bead_ref: &BeadRef) -> Option<&Bead> {
        self.get(bead_ref.namespace())?.get_live(bead_ref.id())
    }

    /// Returns true when a namespace-qualified bead reference resolves live.
    pub fn contains_live_ref(&self, bead_ref: &BeadRef) -> bool {
        self.resolve_ref(bead_ref).is_some()
    }

    /// Get active outgoing deps from a namespace-qualified bead reference.
    ///
    /// This scans the store-level dep graph instead of consulting a single
    /// `CanonicalState` index, because cross-namespace incoming edges may live
    /// outside the target namespace's local indexes.
    pub fn deps_from(&self, from: &BeadRef) -> Vec<DepKey> {
        if !self.contains_live_ref(from) {
            return Vec::new();
        }
        self.active_deps()
            .filter(|key| key.from_ref() == from)
            .filter(|key| self.contains_live_ref(key.to_ref()))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get active incoming deps to a namespace-qualified bead reference.
    pub fn deps_to(&self, to: &BeadRef) -> Vec<DepKey> {
        if !self.contains_live_ref(to) {
            return Vec::new();
        }
        self.active_deps()
            .filter(|key| key.to_ref() == to)
            .filter(|key| self.contains_live_ref(key.from_ref()))
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Convert a raw dep key into a typed add key, enforcing store-wide DAG rules.
    pub fn check_dep_add_key(&self, key: DepKey) -> Result<DepAddKey, InvalidDependency> {
        if key.kind().requires_dag() {
            let proof = self.check_no_cycle(key.from_ref(), key.to_ref())?;
            let key = AcyclicDepKey::from_dep_key(key, proof)?;
            Ok(DepAddKey::Acyclic(key))
        } else {
            let key = FreeDepKey::from_dep_key(key)?;
            Ok(DepAddKey::Free(key))
        }
    }

    /// Prove that adding `from -> to` would not create a store-wide DAG cycle.
    pub fn check_no_cycle(
        &self,
        from: &BeadRef,
        to: &BeadRef,
    ) -> Result<NoCycleProof, InvalidDependency> {
        if from == to {
            return Err(InvalidDependency::SelfDependency(from.to_string()));
        }

        let mut stack = vec![to.clone()];
        let mut visited = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            if &current == from {
                return Err(InvalidDependency::CycleDetected {
                    from: from.to_string(),
                    to: to.to_string(),
                });
            }
            for key in self.deps_from(&current) {
                if key.kind().requires_dag() {
                    stack.push(key.to_ref().clone());
                }
            }
        }

        Ok(NoCycleProof::new())
    }

    /// Detect store-wide dependency cycles among active DAG dependency kinds.
    pub fn dependency_cycles(&self) -> Vec<Vec<BeadRef>> {
        let mut cycles = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let mut stack = Vec::new();

        for key in self.active_deps().filter(|key| key.kind().requires_dag()) {
            self.visit_cycle_node(key.from_ref(), &mut visited, &mut stack, &mut cycles);
        }

        cycles.into_iter().collect()
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

    fn active_deps(&self) -> impl Iterator<Item = &DepKey> {
        self.by_namespace
            .values()
            .flat_map(|state| state.dep_store().values())
    }

    fn visit_cycle_node(
        &self,
        node: &BeadRef,
        visited: &mut BTreeSet<BeadRef>,
        stack: &mut Vec<BeadRef>,
        cycles: &mut BTreeSet<Vec<BeadRef>>,
    ) {
        if let Some(pos) = stack.iter().position(|entry| entry == node) {
            cycles.insert(canonical_cycle(&stack[pos..], node));
            return;
        }
        if visited.contains(node) {
            return;
        }

        stack.push(node.clone());
        for key in self.deps_from(node) {
            if key.kind().requires_dag() {
                self.visit_cycle_node(key.to_ref(), visited, stack, cycles);
            }
        }
        stack.pop();
        visited.insert(node.clone());
    }
}

fn canonical_cycle(path: &[BeadRef], repeated: &BeadRef) -> Vec<BeadRef> {
    let mut body = path.to_vec();
    if body.is_empty() {
        return vec![repeated.clone(), repeated.clone()];
    }

    let min_pos = body
        .iter()
        .enumerate()
        .min_by(|(_, left), (_, right)| left.cmp(right))
        .map(|(idx, _)| idx)
        .unwrap_or(0);
    body.rotate_left(min_pos);
    body.push(body[0].clone());
    body
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
