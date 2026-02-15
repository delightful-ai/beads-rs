use super::*;

impl LoadedStore<'_> {
    pub(crate) fn parse_mutation_meta(
        &self,
        meta: MutationMeta,
        actor: &ActorId,
    ) -> Result<ParsedMutationMeta, OpError> {
        let namespace = self.normalize_namespace(meta.namespace)?;
        let durability = meta.durability.unwrap_or(DurabilityClass::LocalFsync);
        let client_request_id = meta.client_request_id;
        let trace_id = client_request_id
            .map(TraceId::from)
            .unwrap_or_else(|| TraceId::new(Uuid::new_v4()));
        let actor_id = meta.actor_id.unwrap_or_else(|| actor.clone());

        Ok(ParsedMutationMeta {
            namespace,
            durability,
            client_request_id,
            trace_id,
            actor_id,
        })
    }

    pub(crate) fn read_scope(
        &self,
        read: crate::daemon::ipc::ReadConsistency,
    ) -> Result<ReadScope, OpError> {
        ReadScope::new(read, &self.runtime().policies)
    }

    pub(crate) fn check_read_gate(&self, read: &ReadScope) -> Result<(), OpError> {
        match self.read_gate_status(read)? {
            ReadGateStatus::Satisfied => Ok(()),
            ReadGateStatus::Unsatisfied {
                required,
                current_applied,
            } => {
                if read.wait_timeout_ms() > 0 {
                    return Err(OpError::RequireMinSeenTimeout {
                        waited_ms: read.wait_timeout_ms(),
                        required: Box::new(required),
                        current_applied: Box::new(current_applied),
                    });
                }
                Err(OpError::RequireMinSeenUnsatisfied {
                    required: Box::new(required),
                    current_applied: Box::new(current_applied),
                })
            }
        }
    }

    pub(crate) fn read_gate_status(&self, read: &ReadScope) -> Result<ReadGateStatus, OpError> {
        let Some(required) = read.require_min_seen() else {
            return Ok(ReadGateStatus::Satisfied);
        };
        let current_applied = self.runtime().watermarks_applied.clone();
        if current_applied.satisfies_at_least(required) {
            return Ok(ReadGateStatus::Satisfied);
        }
        Ok(ReadGateStatus::Unsatisfied {
            required: required.clone(),
            current_applied,
        })
    }

    pub(crate) fn normalize_namespace(
        &self,
        raw: Option<NamespaceId>,
    ) -> Result<NamespaceId, OpError> {
        ReadScope::normalize_namespace(raw, &self.runtime().policies)
    }

    pub(crate) fn maybe_start_sync(
        &mut self,
        git_sync_policy: crate::daemon::core::GitSyncPolicy,
        actor: ActorId,
        git_tx: &Sender<GitOp>,
    ) {
        if !git_sync_policy.allows_sync() {
            return;
        }
        let repo_state = self.lane_mut();
        if !repo_state.dirty || repo_state.sync_in_progress {
            return;
        }

        let path = match repo_state.any_valid_path() {
            Some(p) => p.clone(),
            None => return,
        };

        repo_state.start_sync();

        let _ = git_tx.send(GitOp::Sync {
            repo: path,
            remote: self.remote().clone(),
            store_id: self.store_id(),
            state: self.runtime().state.core().clone(),
            actor,
        });
    }
}
