use std::path::Path;
use std::sync::Arc;

use super::helpers::{replication_backoff, replication_listen_addr, replication_max_connections};
use super::{Daemon, ReplicationHandles};
use crate::core::error::details as error_details;
use crate::core::{ProtocolErrorCode, ReplicateMode, StoreId};
use crate::daemon::ops::OpError;
use crate::daemon::repl::{
    PeerConfig, ReplError, ReplErrorDetails, ReplIngestRequest, ReplSessionStore,
    ReplicationManager, ReplicationManagerConfig, ReplicationServer, ReplicationServerConfig,
    SharedSessionStore, WalRangeReader,
};
use crate::daemon::store::runtime::load_replica_roster;
use crate::daemon::wal::WalIndex;

impl Daemon {
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

    pub(super) fn ensure_replication_runtime(&mut self, store_id: StoreId) -> Result<(), OpError> {
        if self.repl_handles.contains_key(&store_id) {
            return Ok(());
        }

        let Some(ingest_tx) = self.repl_ingest_tx.clone() else {
            tracing::warn!("replication ingest channel not initialized");
            return Ok(());
        };

        let store = self
            .stores
            .get(&store_id)
            .expect("loaded store missing from state");

        let wal_index: Arc<dyn WalIndex> = store.wal_index.clone();
        let wal_reader = Some(WalRangeReader::new(
            store_id,
            wal_index.clone(),
            self.limits.clone(),
        ));
        let session_store =
            SharedSessionStore::new(ReplSessionStore::new(store_id, wal_index, ingest_tx));

        let roster =
            load_replica_roster(store_id).map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
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
        let manager_handle = ReplicationManager::new(session_store.clone(), manager_config).start();

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

        let server_handle = match ReplicationServer::new(session_store, server_config).start() {
            Ok(handle) => Some(handle),
            Err(err) => {
                tracing::warn!("replication server failed to start: {err}");
                None
            }
        };

        self.repl_handles.insert(
            store_id,
            ReplicationHandles {
                manager: Some(manager_handle),
                server: server_handle,
            },
        );

        Ok(())
    }

    pub(crate) fn handle_repl_ingest(&mut self, request: ReplIngestRequest) {
        let (store_epoch, local_replica_id) = self
            .stores
            .get(&request.store_id)
            .map(|store| {
                (
                    Some(store.meta.identity.store_epoch),
                    Some(store.meta.replica_id),
                )
            })
            .unwrap_or((None, None));
        let first_event = request.batch.first_event();
        let origin_seq = Some(first_event.body.origin_seq.get());
        let txn_id = Some(first_event.body.txn_id);
        let client_request_id = first_event.body.client_request_id;
        let trace_id = first_event.body.trace_id;
        let namespace = request.batch.namespace();
        let origin = request.batch.origin();
        let span = tracing::info_span!(
            "repl_ingest_request",
            store_id = %request.store_id,
            store_epoch = ?store_epoch.map(|epoch| epoch.get()),
            replica_id = ?local_replica_id,
            namespace = %namespace,
            origin_replica_id = %origin,
            origin_seq = ?origin_seq,
            txn_id = ?txn_id,
            client_request_id = ?client_request_id,
            trace_id = ?trace_id,
            batch_len = request.batch.len()
        );
        let _guard = span.enter();

        if self.shutting_down {
            let error = ReplError::new(
                ProtocolErrorCode::MaintenanceMode.into(),
                "shutting down",
                true,
            );
            let _ = request.respond.send(Err(error));
            return;
        }
        if let Some(error) = self.stores.get(&request.store_id).and_then(|store| {
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
        }) {
            let _ = request.respond.send(Err(error));
            return;
        }
        let outcome = self.ingest_remote_batch(request.store_id, request.batch, request.now_ms);
        let _ = request.respond.send(outcome);
    }

    pub(crate) fn shutdown_replication(&mut self) {
        let handles = std::mem::take(&mut self.repl_handles);
        for (_, handle) in handles {
            handle.shutdown();
        }
    }

    pub(crate) fn reload_replication_runtime(&mut self, store_id: StoreId) -> Result<(), OpError> {
        if let Some(handles) = self.repl_handles.remove(&store_id) {
            handles.shutdown();
        }
        self.ensure_replication_runtime(store_id)
    }

    pub(crate) fn reload_replication_config(&mut self, repo_path: &Path) -> Result<(), OpError> {
        let config = crate::config::load_for_repo(Some(repo_path)).map_err(|e| {
            OpError::ValidationFailed {
                field: "replication".into(),
                reason: format!("failed to reload config: {e}"),
            }
        })?;
        self.replication = config.replication;
        Ok(())
    }
}
