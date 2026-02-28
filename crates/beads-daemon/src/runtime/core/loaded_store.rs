use super::*;

/// Proof that a repo is loaded. Only created by `Daemon::ensure_repo_loaded`,
/// `Daemon::ensure_repo_loaded_strict`, or `Daemon::ensure_repo_fresh`.
///
/// The handle borrows the backing runtime + git lane entries, so it cannot exist
/// without a loaded store.
pub struct LoadedStore<'a> {
    store_id: StoreId,
    remote: RemoteUrl,
    session: &'a mut StoreSession,
}

impl<'a> LoadedStore<'a> {
    pub(super) fn new(store_id: StoreId, remote: RemoteUrl, session: &'a mut StoreSession) -> Self {
        Self {
            store_id,
            remote,
            session,
        }
    }

    pub(crate) fn session_token(&self) -> StoreSessionToken {
        self.session.token()
    }

    #[allow(dead_code)]
    pub(crate) fn generation(&self) -> StoreGeneration {
        self.session.token().generation()
    }

    pub fn store_id(&self) -> StoreId {
        self.store_id
    }

    pub fn remote(&self) -> &RemoteUrl {
        &self.remote
    }

    pub fn runtime(&self) -> &StoreRuntime {
        self.session.runtime()
    }

    pub fn runtime_mut(&mut self) -> &mut StoreRuntime {
        self.session.runtime_mut()
    }

    pub fn lane(&self) -> &GitLaneState {
        self.session.lane()
    }

    pub fn lane_mut(&mut self) -> &mut GitLaneState {
        self.session.lane_mut()
    }

    pub fn split_mut(&mut self) -> (&mut StoreRuntime, &mut GitLaneState) {
        self.session.split_mut()
    }

    /// Get the store identity.
    pub fn store_identity(&self) -> StoreIdentity {
        self.session.runtime().meta.identity
    }
}

impl fmt::Debug for LoadedStore<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadedStore")
            .field("store_id", &self.store_id)
            .field("remote", &self.remote)
            .finish()
    }
}
