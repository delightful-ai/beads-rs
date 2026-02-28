use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct StoreGeneration(u64);

impl StoreGeneration {
    pub(crate) fn new(raw: u64) -> Self {
        Self(raw)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct StoreSessionToken {
    store_id: StoreId,
    generation: StoreGeneration,
}

impl StoreSessionToken {
    pub(crate) fn new(store_id: StoreId, generation: StoreGeneration) -> Self {
        Self {
            store_id,
            generation,
        }
    }

    pub(crate) fn store_id(self) -> StoreId {
        self.store_id
    }

    #[allow(dead_code)]
    pub(crate) fn generation(self) -> StoreGeneration {
        self.generation
    }
}

pub(crate) struct StoreSession {
    token: StoreSessionToken,
    runtime: StoreRuntime,
    lane: GitLaneState,
    repl_handles: Option<ReplicationHandles>,
}

impl StoreSession {
    pub(crate) fn new(token: StoreSessionToken, runtime: StoreRuntime, lane: GitLaneState) -> Self {
        Self {
            token,
            runtime,
            lane,
            repl_handles: None,
        }
    }

    pub(crate) fn token(&self) -> StoreSessionToken {
        self.token
    }

    pub(crate) fn runtime(&self) -> &StoreRuntime {
        &self.runtime
    }

    pub(crate) fn runtime_mut(&mut self) -> &mut StoreRuntime {
        &mut self.runtime
    }

    pub(crate) fn lane(&self) -> &GitLaneState {
        &self.lane
    }

    pub(crate) fn lane_mut(&mut self) -> &mut GitLaneState {
        &mut self.lane
    }

    pub(crate) fn split_mut(&mut self) -> (&mut StoreRuntime, &mut GitLaneState) {
        (&mut self.runtime, &mut self.lane)
    }

    pub(crate) fn repl_handles(&self) -> Option<&ReplicationHandles> {
        self.repl_handles.as_ref()
    }

    pub(crate) fn take_repl_handles(&mut self) -> Option<ReplicationHandles> {
        self.repl_handles.take()
    }

    pub(crate) fn set_repl_handles(&mut self, handles: ReplicationHandles) {
        self.repl_handles = Some(handles);
    }
}
