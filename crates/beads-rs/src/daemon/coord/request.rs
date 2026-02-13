use super::*;

impl Daemon {
    /// Handle a request from IPC.
    ///
    /// Dispatches to appropriate handler based on request type.
    pub(crate) fn handle_request(&mut self, req: Request, git_tx: &Sender<GitOp>) -> HandleOutcome {
        match req {
            // Mutations - delegate to executor module
            Request::Create { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_create(&repo, meta, payload, git_tx)
            }

            Request::Update { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_update(&repo, meta, payload, git_tx)
            }

            Request::AddLabels { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_add_labels(&repo, meta, payload, git_tx)
            }

            Request::RemoveLabels { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_remove_labels(&repo, meta, payload, git_tx)
            }

            Request::SetParent { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_set_parent(&repo, meta, payload, git_tx)
            }

            Request::Close { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_close(&repo, meta, payload, git_tx)
            }

            Request::Reopen { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_reopen(&repo, meta, payload, git_tx)
            }

            Request::Delete { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_delete(&repo, meta, payload, git_tx)
            }

            Request::AddDep { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_add_dep(&repo, meta, payload, git_tx)
            }

            Request::RemoveDep { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_remove_dep(&repo, meta, payload, git_tx)
            }

            Request::AddNote { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_add_note(&repo, meta, payload, git_tx)
            }

            Request::Claim { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_claim(&repo, meta, payload, git_tx)
            }

            Request::Unclaim { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_unclaim(&repo, meta, payload, git_tx)
            }

            Request::ExtendClaim { ctx, payload } => {
                let repo = ctx.repo.path;
                let meta = ctx.meta;
                self.apply_extend_claim(&repo, meta, payload, git_tx)
            }

            // Queries - delegate to query_executor module
            Request::Show { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_show(&repo, &payload.id, read, git_tx).into()
            }

            Request::ShowMultiple { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_show_multiple(&repo, &payload.ids, read, git_tx)
                    .into()
            }

            Request::ShowDetails { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_show_details(&repo, &payload.id, read, git_tx)
                    .into()
            }

            Request::List { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_list(&repo, &payload.filters, read, git_tx)
                    .into()
            }

            Request::Ready { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_ready(&repo, payload.limit, read, git_tx).into()
            }

            Request::DepTree { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_dep_tree(&repo, &payload.id, read, git_tx).into()
            }

            Request::DepCycles { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_dep_cycles(&repo, read, git_tx).into()
            }

            Request::Deps { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_deps(&repo, &payload.id, read, git_tx).into()
            }

            Request::Notes { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_notes(&repo, &payload.id, read, git_tx).into()
            }

            Request::Blocked { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_blocked(&repo, read, git_tx).into()
            }

            Request::Stale { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_stale(
                    &repo,
                    payload.days,
                    payload.status.as_deref(),
                    payload.limit,
                    read,
                    git_tx,
                )
                .into()
            }

            Request::Count { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_count(
                    &repo,
                    &payload.filters,
                    payload.group_by.as_deref(),
                    read,
                    git_tx,
                )
                .into()
            }

            Request::Deleted { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_deleted(&repo, payload.since_ms, payload.id.as_ref(), read, git_tx)
                    .into()
            }

            Request::EpicStatus { ctx, payload } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_epic_status(&repo, payload.eligible_only, read, git_tx)
                    .into()
            }

            Request::Status { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_status(&repo, read, git_tx).into()
            }

            Request::Admin(op) => match op {
                AdminOp::Status { ctx, .. } => {
                    let repo = ctx.repo.path;
                    let read = ctx.read;
                    self.admin_status(&repo, read, git_tx).into()
                }
                AdminOp::Metrics { ctx, .. } => {
                    let repo = ctx.repo.path;
                    let read = ctx.read;
                    self.admin_metrics(&repo, read, git_tx).into()
                }
                AdminOp::Doctor { ctx, payload } => {
                    let repo = ctx.repo.path;
                    let read = ctx.read;
                    self.admin_doctor(
                        &repo,
                        read,
                        payload.max_records_per_namespace,
                        payload.verify_checkpoint_cache,
                        git_tx,
                    )
                    .into()
                }
                AdminOp::Scrub { ctx, payload } => {
                    let repo = ctx.repo.path;
                    let read = ctx.read;
                    self.admin_scrub_now(
                        &repo,
                        read,
                        payload.max_records_per_namespace,
                        payload.verify_checkpoint_cache,
                        git_tx,
                    )
                    .into()
                }
                AdminOp::Flush { ctx, payload } => self
                    .admin_flush(&ctx.path, payload.namespace, payload.checkpoint_now, git_tx)
                    .into(),
                AdminOp::CheckpointWait { .. } => {
                    unreachable!("AdminCheckpointWait is handled by the daemon state loop")
                }
                AdminOp::Fingerprint { ctx, payload } => {
                    let repo = ctx.repo.path;
                    let read = ctx.read;
                    self.admin_fingerprint(&repo, read, payload.mode, payload.sample, git_tx)
                        .into()
                }
                AdminOp::ReloadPolicies { ctx, .. } => {
                    self.admin_reload_policies(&ctx.path, git_tx).into()
                }
                AdminOp::ReloadLimits { ctx, .. } => {
                    self.admin_reload_limits(&ctx.path, git_tx).into()
                }
                AdminOp::ReloadReplication { ctx, .. } => {
                    self.admin_reload_replication(&ctx.path, git_tx).into()
                }
                AdminOp::RotateReplicaId { ctx, .. } => {
                    self.admin_rotate_replica_id(&ctx.path, git_tx).into()
                }
                AdminOp::MaintenanceMode { ctx, payload } => self
                    .admin_maintenance_mode(&ctx.path, payload.enabled, git_tx)
                    .into(),
                AdminOp::RebuildIndex { ctx, .. } => {
                    self.admin_rebuild_index(&ctx.path, git_tx).into()
                }
                AdminOp::StoreFsck { payload } => self
                    .admin_store_fsck(payload.store_id, payload.repair)
                    .into(),
                AdminOp::StoreLockInfo { payload } => {
                    self.admin_store_lock_info(payload.store_id).into()
                }
                AdminOp::StoreUnlock { payload } => self
                    .admin_store_unlock(payload.store_id, payload.force)
                    .into(),
            },

            Request::Validate { ctx, .. } => {
                let repo = ctx.repo.path;
                let read = ctx.read;
                self.query_validate(&repo, read, git_tx).into()
            }

            Request::Subscribe { .. } => Response::err_from(error_payload(
                ProtocolErrorCode::InvalidRequest.into(),
                "subscribe must be handled by the streaming IPC path",
                false,
            ))
            .into(),

            // Control
            Request::Refresh { ctx, .. } => {
                // Force reload from git (invalidates cached state).
                // Used after external changes like migration.
                match self.force_reload(&ctx.path, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::refreshed()),
                    Err(e) => Response::err_from(e),
                }
                .into()
            }

            Request::Sync { ctx, .. } => {
                // Force immediate sync (used for graceful shutdown)
                match self.ensure_loaded_and_maybe_start_sync(&ctx.path, git_tx) {
                    Ok(_) => Response::ok(ResponsePayload::synced()),
                    Err(e) => Response::err_from(e),
                }
                .into()
            }

            Request::SyncWait { .. } => {
                unreachable!("SyncWait is handled by the daemon state loop")
            }

            Request::Init { ctx, .. } => {
                let repo = ctx.path;
                let timeout = init_timeout();
                let (respond_tx, respond_rx) = crossbeam::channel::bounded(1);
                if git_tx
                    .send(GitOp::Init {
                        repo: repo.clone(),
                        respond: respond_tx,
                    })
                    .is_err()
                {
                    return Response::err_from(error_payload(
                        CliErrorCode::Internal.into(),
                        "git thread not responding",
                        false,
                    ))
                    .into();
                }

                match respond_rx.recv_timeout(timeout) {
                    Ok(Ok(())) => match self.ensure_repo_loaded(&repo, git_tx) {
                        Ok(_) => Response::ok(ResponsePayload::initialized()),
                        Err(e) => Response::err_from(e),
                    },
                    Ok(Err(e)) => Response::err_from(error_payload(
                        CliErrorCode::InitFailed.into(),
                        &e.to_string(),
                        false,
                    )),
                    Err(crossbeam::channel::RecvTimeoutError::Timeout) => Response::err_from(
                        error_payload(CliErrorCode::Internal.into(), "git init timed out", true),
                    ),
                    Err(crossbeam::channel::RecvTimeoutError::Disconnected) => Response::err_from(
                        error_payload(CliErrorCode::Internal.into(), "git thread died", false),
                    ),
                }
                .into()
            }

            Request::Ping => Response::ok(ResponsePayload::query(QueryResult::DaemonInfo(
                ApiDaemonInfo {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    protocol_version: crate::daemon::ipc::IPC_PROTOCOL_VERSION,
                    pid: std::process::id(),
                },
            )))
            .into(),

            Request::Shutdown => {
                self.begin_shutdown();
                Response::ok(ResponsePayload::shutting_down()).into()
            }
        }
    }
}
