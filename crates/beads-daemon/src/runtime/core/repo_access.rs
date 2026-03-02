use super::*;

impl Daemon {
    pub(crate) fn loaded_store(&mut self, store_id: StoreId, remote: RemoteUrl) -> LoadedStore<'_> {
        let session = self
            .store_sessions
            .get_mut(&store_id)
            .expect("loaded store missing from state");
        LoadedStore::new(store_id, remote, session)
    }

    pub(crate) fn try_loaded_store(
        &mut self,
        store_id: StoreId,
        remote: RemoteUrl,
    ) -> Option<LoadedStore<'_>> {
        let session = self.store_sessions.get_mut(&store_id)?;
        Some(LoadedStore::new(store_id, remote, session))
    }

    #[cfg(feature = "test-harness")]
    #[allow(dead_code)]
    pub fn store_runtime_by_id(&self, store_id: StoreId) -> Option<&StoreRuntime> {
        self.store_sessions
            .get(&store_id)
            .map(StoreSession::runtime)
    }

    #[cfg(feature = "test-harness")]
    #[allow(dead_code)]
    pub fn store_runtime_by_id_mut(&mut self, store_id: StoreId) -> Option<&mut StoreRuntime> {
        self.store_sessions
            .get_mut(&store_id)
            .map(StoreSession::runtime_mut)
    }

    pub(crate) fn store_session_by_id_mut(
        &mut self,
        store_id: StoreId,
    ) -> Option<&mut StoreSession> {
        self.store_sessions.get_mut(&store_id)
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

    pub(crate) fn drop_store_state(&mut self, store_id: StoreId) {
        if let Some(session) = self.store_sessions.get(&store_id) {
            self.scheduler.cancel(&session.runtime().primary_remote);
        }
        self.checkpoint_scheduler.drop_store(store_id);
        if let Some(mut session) = self.store_sessions.remove(&store_id)
            && let Some(handles) = session.take_repl_handles()
        {
            handles.shutdown();
        }
        self.export_pending.remove(&store_id);
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
            .and_then(|resolution| self.store_sessions.get(&resolution.store_id))
            .map(StoreSession::lane)
    }

    pub(crate) fn primary_remote_for_store(&self, store_id: &StoreId) -> Option<&RemoteUrl> {
        self.store_sessions
            .get(store_id)
            .map(|session| &session.runtime().primary_remote)
    }
}
