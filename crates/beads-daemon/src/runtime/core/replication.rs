use std::path::Path;
use std::sync::Arc;

use super::helpers::{replication_backoff, replication_listen_addr, replication_max_connections};
use super::{Daemon, ReplicationHandles, StoreSession};
use crate::core::error::details as error_details;
use crate::core::{ProtocolErrorCode, ReplicateMode, StoreId};
use crate::runtime::ops::OpError;
use crate::runtime::repl::{
    PeerConfig, ReplError, ReplErrorDetails, ReplIngestRequest, ReplSessionStore,
    ReplicationManager, ReplicationManagerConfig, ReplicationServer, ReplicationServerConfig,
    SharedSessionStore, WalRangeReader,
};
use crate::runtime::store::runtime::load_replica_roster;
use crate::runtime::wal::WalIndex;

const REPL_RUNTIME_RETRY_AFTER_MS: u64 = 100;

struct ReplicationRuntimeInputs {
    runtime_version: crate::runtime::store::runtime::ReplicationRuntimeVersion,
    session_store: SharedSessionStore<ReplSessionStore>,
    manager_config: ReplicationManagerConfig,
    server_config: ReplicationServerConfig,
}

impl Daemon {
    pub(in crate::runtime) fn replication_config(&self) -> &crate::config::ReplicationConfig {
        &self.replication
    }

    pub(in crate::runtime) fn set_replication_config(
        &mut self,
        config: crate::config::ReplicationConfig,
    ) {
        self.replication = config;
    }

    fn replication_peers(&self) -> Vec<PeerConfig> {
        self.replication
            .peers
            .iter()
            .map(|peer| PeerConfig {
                replica_id: peer.replica_id,
                addr: peer.addr.clone(),
                role: peer.role,
                allowed_namespaces: peer.allowed_namespaces.clone(),
            })
            .collect()
    }

    fn build_replication_runtime_inputs(
        &self,
        store_id: StoreId,
    ) -> Result<Option<ReplicationRuntimeInputs>, OpError> {
        let Some(ingest_tx) = self.repl_ingest_tx.clone() else {
            tracing::warn!("replication ingest channel not initialized");
            return Ok(None);
        };

        let Some(session) = self.store_sessions.get(&store_id) else {
            return Ok(None);
        };
        let store = session.runtime();
        let runtime_version = store.replication_runtime_version();

        let wal_index: Arc<dyn WalIndex> = store.wal_index.clone();
        let wal_reader = Some(WalRangeReader::new(
            self.layout().store_dir(&store_id),
            wal_index.clone(),
            self.limits.clone(),
        ));
        let session_store = SharedSessionStore::new(ReplSessionStore::new(
            session.token(),
            runtime_version,
            wal_index,
            ingest_tx,
        ));

        let roster = load_replica_roster(self.layout(), store_id)
            .map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
        let peers = self.replication_peers();
        let manager_config = ReplicationManagerConfig {
            local_store: store.meta.identity,
            local_replica_id: store.meta.replica_id,
            admission: store.admission.clone(),
            broadcaster: store.broadcaster.clone(),
            peer_acks: store.peer_acks.clone(),
            policies: store.policies.clone(),
            roster: roster.clone(),
            peers,
            wal_reader: wal_reader.clone(),
            limits: self.limits.clone(),
            backoff: replication_backoff(&self.replication),
        };

        let max_connections = if roster.is_some() {
            None
        } else {
            replication_max_connections(&self.replication)
        };
        let server_config = ReplicationServerConfig {
            listen_addr: replication_listen_addr(&self.replication),
            local_store: store.meta.identity,
            local_replica_id: store.meta.replica_id,
            admission: store.admission.clone(),
            broadcaster: store.broadcaster.clone(),
            peer_acks: store.peer_acks.clone(),
            policies: store.policies.clone(),
            roster,
            wal_reader,
            limits: self.limits.clone(),
            max_connections,
        };

        Ok(Some(ReplicationRuntimeInputs {
            runtime_version,
            session_store,
            manager_config,
            server_config,
        }))
    }

    fn start_replication_server(
        &self,
        session_store: SharedSessionStore<ReplSessionStore>,
        server_config: ReplicationServerConfig,
    ) -> Option<crate::runtime::repl::ReplicationServerHandle> {
        match ReplicationServer::new(session_store, server_config).start() {
            Ok(handle) => Some(handle),
            Err(err) => {
                tracing::warn!("replication server failed to start: {err}");
                None
            }
        }
    }

    pub(super) fn ensure_replication_runtime(&mut self, store_id: StoreId) -> Result<(), OpError> {
        if let Some(session) = self.store_sessions.get_mut(&store_id) {
            let current_runtime_version = session.runtime().replication_runtime_version();
            if let Some(handles) = session.take_repl_handles() {
                if handles.runtime_version() == current_runtime_version {
                    session.set_repl_handles(handles);
                    return Ok(());
                }
                handles.shutdown();
            }
        } else {
            return Ok(());
        }

        let Some(inputs) = self.build_replication_runtime_inputs(store_id)? else {
            return Ok(());
        };
        let manager_handle =
            ReplicationManager::new(inputs.session_store.clone(), inputs.manager_config).start();
        let server_handle =
            self.start_replication_server(inputs.session_store, inputs.server_config);

        if let Some(session) = self.store_sessions.get_mut(&store_id) {
            session.set_repl_handles(ReplicationHandles {
                runtime_version: inputs.runtime_version,
                manager: Some(manager_handle),
                server: server_handle,
            });
        }

        Ok(())
    }

    pub(crate) fn handle_repl_ingest(&mut self, request: ReplIngestRequest) {
        if !self.session_matches(request.session) {
            let _ = request.respond.send(Err(ReplError::new(
                ProtocolErrorCode::Overloaded.into(),
                "stale store session",
                true,
            )));
            return;
        }

        let store_id = request.session.store_id();
        let (store_epoch, local_replica_id, bound_runtime_version) = self
            .store_sessions
            .get(&store_id)
            .map(|session| {
                (
                    Some(session.runtime().meta.identity.store_epoch),
                    Some(session.runtime().meta.replica_id),
                    session
                        .repl_handles()
                        .map(ReplicationHandles::runtime_version),
                )
            })
            .unwrap_or((None, None, None));
        let first_event = request.batch.first_event();
        let origin_seq = Some(first_event.body.origin_seq.get());
        let txn_id = Some(first_event.body.txn_id);
        let client_request_id = first_event.body.client_request_id;
        let trace_id = first_event.body.trace_id;
        let namespace = request.batch.namespace();
        let origin = request.batch.origin();
        let span = tracing::info_span!(
            "repl_ingest_request",
            store_id = %store_id,
            store_epoch = ?store_epoch.map(|epoch| epoch.get()),
            replica_id = ?local_replica_id,
            namespace = %namespace,
            origin_replica_id = %origin,
            origin_seq = ?origin_seq,
            txn_id = ?txn_id,
            client_request_id = ?client_request_id,
            trace_id = ?trace_id,
            request_runtime_version = ?request.runtime_version,
            bound_runtime_version = ?bound_runtime_version,
            batch_len = request.batch.len()
        );
        let _guard = span.enter();

        let Some(bound_runtime_version) = bound_runtime_version else {
            let error = Self::replication_runtime_missing_binding_error();
            let _ = request.respond.send(Err(error));
            return;
        };
        if request.runtime_version != bound_runtime_version {
            let error = Self::replication_runtime_stale_binding_error();
            let _ = request.respond.send(Err(error));
            return;
        }

        if self.shutting_down {
            let error = ReplError::new(
                ProtocolErrorCode::MaintenanceMode.into(),
                "shutting down",
                true,
            );
            let _ = request.respond.send(Err(error));
            return;
        }
        if let Some(error) = self
            .store_sessions
            .get(&store_id)
            .map(StoreSession::runtime)
            .and_then(|store| {
                if store.maintenance_mode {
                    Some(
                        ReplError::new(
                            ProtocolErrorCode::MaintenanceMode.into(),
                            "maintenance mode enabled",
                            true,
                        )
                        .with_details(ReplErrorDetails::MaintenanceMode(
                            error_details::MaintenanceModeDetails {
                                reason: Some("maintenance mode enabled".into()),
                                until_ms: None,
                            },
                        )),
                    )
                } else if let Some(policy) = store.policies.get(namespace) {
                    if policy.replicate_mode == ReplicateMode::None {
                        Some(
                            ReplError::new(
                                ProtocolErrorCode::NamespacePolicyViolation.into(),
                                "namespace replication disabled by policy",
                                false,
                            )
                            .with_details(
                                ReplErrorDetails::NamespacePolicyViolation(
                                    error_details::NamespacePolicyViolationDetails {
                                        namespace: namespace.clone(),
                                        rule: "replicate_mode".to_string(),
                                        reason: Some("replicate_mode=none".to_string()),
                                    },
                                ),
                            ),
                        )
                    } else {
                        None
                    }
                } else {
                    Some(
                        ReplError::new(
                            ProtocolErrorCode::NamespaceUnknown.into(),
                            "namespace not configured",
                            false,
                        )
                        .with_details(ReplErrorDetails::NamespaceUnknown(
                            error_details::NamespaceUnknownDetails {
                                namespace: namespace.clone(),
                            },
                        )),
                    )
                }
            })
        {
            let _ = request.respond.send(Err(error));
            return;
        }
        let outcome = self.ingest_remote_batch(request.session, request.batch, request.now_ms);
        let _ = request.respond.send(outcome);
    }

    pub(crate) fn shutdown_replication(&mut self) {
        for session in self.store_sessions.values_mut() {
            if let Some(handles) = session.take_repl_handles() {
                handles.shutdown();
            }
        }
    }

    pub(crate) fn reload_replication_runtime(&mut self, store_id: StoreId) -> Result<(), OpError> {
        if let Some(session) = self.store_sessions.get_mut(&store_id)
            && let Some(handles) = session.take_repl_handles()
        {
            handles.shutdown();
        }
        self.ensure_replication_runtime(store_id)
    }

    pub(crate) fn reload_replication_peers(&mut self, store_id: StoreId) -> Result<(), OpError> {
        let preserved_server = if let Some(session) = self.store_sessions.get_mut(&store_id) {
            if let Some(mut handles) = session.take_repl_handles() {
                if let Some(manager) = handles.manager.take() {
                    manager.shutdown();
                }
                handles.server.take()
            } else {
                None
            }
        } else {
            return Ok(());
        };

        let Some(inputs) = self.build_replication_runtime_inputs(store_id)? else {
            return Ok(());
        };
        let manager_handle =
            ReplicationManager::new(inputs.session_store.clone(), inputs.manager_config).start();
        let server_handle = preserved_server
            .or_else(|| self.start_replication_server(inputs.session_store, inputs.server_config));

        if let Some(session) = self.store_sessions.get_mut(&store_id) {
            session.set_repl_handles(ReplicationHandles {
                runtime_version: inputs.runtime_version,
                manager: Some(manager_handle),
                server: server_handle,
            });
        }

        Ok(())
    }

    pub(crate) fn reload_replication_config(&mut self, repo_path: &Path) -> Result<(), OpError> {
        let config = crate::config::load_for_repo(Some(repo_path)).map_err(|e| {
            OpError::ValidationFailed {
                field: "replication".into(),
                reason: format!("failed to reload config: {e}"),
            }
        })?;
        let runtime = crate::config::daemon_runtime_from_config(&config);
        self.replication = runtime.replication;
        Ok(())
    }

    fn replication_runtime_missing_binding_error() -> ReplError {
        ReplError::new(
            ProtocolErrorCode::Overloaded.into(),
            "replication runtime binding missing",
            true,
        )
        .with_details(ReplErrorDetails::Overloaded(
            error_details::OverloadedDetails {
                subsystem: Some(error_details::OverloadedSubsystem::Repl),
                retry_after_ms: Some(REPL_RUNTIME_RETRY_AFTER_MS),
                queue_bytes: None,
                queue_events: None,
            },
        ))
    }

    fn replication_runtime_stale_binding_error() -> ReplError {
        ReplError::new(
            ProtocolErrorCode::Overloaded.into(),
            "stale replication runtime binding",
            true,
        )
        .with_details(ReplErrorDetails::Overloaded(
            error_details::OverloadedDetails {
                subsystem: Some(error_details::OverloadedSubsystem::Repl),
                retry_after_ms: Some(REPL_RUNTIME_RETRY_AFTER_MS),
                queue_bytes: None,
                queue_events: None,
            },
        ))
    }
}
