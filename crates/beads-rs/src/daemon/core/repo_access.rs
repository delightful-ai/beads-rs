use super::*;

impl Daemon {
    pub(crate) fn loaded_store(&mut self, store_id: StoreId, remote: RemoteUrl) -> LoadedStore<'_> {
        let store = self
            .stores
            .get_mut(&store_id)
            .expect("loaded store missing from state");
        let lane = self
            .git_lanes
            .get_mut(&store_id)
            .expect("loaded store missing from state");
        LoadedStore::new(store_id, remote, store, lane)
    }

    pub(crate) fn try_loaded_store(
        &mut self,
        store_id: StoreId,
        remote: RemoteUrl,
    ) -> Option<LoadedStore<'_>> {
        let store = self.stores.get_mut(&store_id)?;
        let lane = self.git_lanes.get_mut(&store_id)?;
        Some(LoadedStore::new(store_id, remote, store, lane))
    }

    #[cfg(feature = "test-harness")]
    pub(crate) fn store_runtime_by_id(&self, store_id: StoreId) -> Option<&StoreRuntime> {
        self.stores.get(&store_id)
    }

    #[cfg(feature = "test-harness")]
    pub(crate) fn store_runtime_by_id_mut(
        &mut self,
        store_id: StoreId,
    ) -> Option<&mut StoreRuntime> {
        self.stores.get_mut(&store_id)
    }

    pub(crate) fn namespace_state<'a>(
        loaded: &'a LoadedStore<'_>,
        namespace: &NamespaceId,
    ) -> &'a CanonicalState {
        static EMPTY_STATE: OnceLock<CanonicalState> = OnceLock::new();
        loaded
            .runtime()
            .state
            .get(namespace)
            .unwrap_or_else(|| EMPTY_STATE.get_or_init(CanonicalState::new))
    }

    pub(crate) fn namespace_state_mut<'a>(
        loaded: &'a mut LoadedStore<'_>,
        namespace: NamespaceId,
    ) -> &'a mut CanonicalState {
        let store = loaded.runtime_mut();
        store.state.ensure_namespace(namespace)
    }

    pub(crate) fn store_id_for_remote(&self, remote: &RemoteUrl) -> Option<StoreId> {
        self.store_caches
            .remote_to_store
            .get(remote)
            .map(|resolution| resolution.store_id)
    }

    pub(crate) fn store_and_lane_by_id_mut(
        &mut self,
        store_id: StoreId,
    ) -> Option<(&mut StoreRuntime, &mut GitLaneState)> {
        let store = self.stores.get_mut(&store_id)?;
        let lane = self.git_lanes.get_mut(&store_id)?;
        Some((store, lane))
    }

    pub(crate) fn drop_store_state(&mut self, store_id: StoreId) {
        self.stores.remove(&store_id);
        self.git_lanes.remove(&store_id);
    }

    pub(crate) fn resolve_store(&mut self, repo: &Path) -> Result<ResolvedStore, OpError> {
        self.store_caches.resolve_store(repo)
    }

    pub(crate) fn scheduler(&self) -> &SyncScheduler {
        &self.scheduler
    }

    pub(crate) fn scheduler_mut(&mut self) -> &mut SyncScheduler {
        &mut self.scheduler
    }

    pub(crate) fn checkpoint_scheduler_mut(&mut self) -> &mut CheckpointScheduler {
        &mut self.checkpoint_scheduler
    }

    /// Get git lane state by raw remote URL (for internal sync waiters, etc.).
    /// Returns None if not loaded.
    pub(crate) fn git_lane_state_by_url(&self, remote: &RemoteUrl) -> Option<&GitLaneState> {
        self.store_caches
            .remote_to_store
            .get(remote)
            .and_then(|resolution| self.git_lanes.get(&resolution.store_id))
    }

    pub(crate) fn primary_remote_for_store(&self, store_id: &StoreId) -> Option<&RemoteUrl> {
        self.stores.get(store_id).map(|store| &store.primary_remote)
    }
}
