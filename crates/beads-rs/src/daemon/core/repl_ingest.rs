use super::*;

impl Daemon {
    pub(super) fn ingest_remote_batch(
        &mut self,
        store_id: StoreId,
        batch: ContiguousBatch,
        now_ms: u64,
    ) -> Result<IngestOutcome, ReplError> {
        let namespace = batch.namespace().clone();
        let origin = batch.origin();
        let actor_stamps: Vec<(ActorId, WriteStamp)> = batch
            .events()
            .iter()
            .map(|event| {
                let EventKindV1::TxnV1(txn) = &event.body.kind;
                (
                    txn.hlc_max.actor_id.clone(),
                    WriteStamp::new(txn.hlc_max.physical_ms, txn.hlc_max.logical),
                )
            })
            .collect();
        let (stores, git_lanes) = (&mut self.stores, &mut self.git_lanes);
        let store = stores.get_mut(&store_id).ok_or_else(|| {
            ReplError::new(CliErrorCode::Internal.into(), "store not loaded", true)
        })?;
        let git_lane = git_lanes.get_mut(&store_id).ok_or_else(|| {
            ReplError::new(CliErrorCode::Internal.into(), "store not loaded", true)
        })?;

        let store_identity = store.meta.identity;
        let origin_seq_first = Some(batch.first().get());
        let origin_seq_last = Some(batch.last().get());
        let span = tracing::info_span!(
            "repl_ingest",
            store_id = %store_identity.store_id,
            store_epoch = store_identity.store_epoch.get(),
            namespace = %namespace,
            origin_replica_id = %origin,
            origin_seq_first = ?origin_seq_first,
            origin_seq_last = ?origin_seq_last,
            batch_len = batch.len()
        );
        let _guard = span.enter();

        let store_dir = paths::store_dir(store_id);

        let wal_index = Arc::clone(&store.wal_index);
        let mut txn = wal_index
            .begin_wal_txn()
            .map_err(|err| wal_index_error_payload(&err))?;

        let mut canonical_shas = Vec::with_capacity(batch.len());
        for event in batch.events() {
            let payload = encode_event_body_canonical(event.body.as_ref()).map_err(|_| {
                ReplError::new(
                    CliErrorCode::Internal.into(),
                    "event body canonical encode failed",
                    false,
                )
            })?;
            let sha = hash_event_body(&payload).0;
            canonical_shas.push(sha);
            let record = VerifiedRecord::new(
                RecordHeader {
                    origin_replica_id: origin,
                    origin_seq: event.body.origin_seq,
                    event_time_ms: event.body.event_time_ms,
                    txn_id: event.body.txn_id,
                    request_proof: event
                        .body
                        .client_request_id
                        .map(|client_request_id| RequestProof::ClientNoHash { client_request_id })
                        .unwrap_or(RequestProof::None),
                    sha256: sha,
                    prev_sha256: event.prev.prev.map(|sha| sha.0),
                },
                payload,
                event.body.clone(),
            )
            .map_err(|err| {
                tracing::error!(error = ?err, "record verification failed");
                ReplError::new(
                    CliErrorCode::Internal.into(),
                    "record verification failed",
                    false,
                )
            })?;

            let append_start = Instant::now();
            let append = match store.event_wal.wal_append(&namespace, &record, now_ms) {
                Ok(append) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_ok(elapsed);
                    metrics::wal_fsync_ok(elapsed);
                    append
                }
                Err(err) => {
                    let elapsed = append_start.elapsed();
                    metrics::wal_append_err(elapsed);
                    metrics::wal_fsync_err(elapsed);
                    return Err(event_wal_error_payload(&namespace, None, None, err));
                }
            };
            let segment_snapshot =
                store
                    .event_wal
                    .segment_snapshot(&namespace)
                    .ok_or_else(|| {
                        ReplError::new(
                            CliErrorCode::Internal.into(),
                            "missing active wal segment",
                            false,
                        )
                    })?;
            let last_indexed_offset = append.offset + append.len as u64;
            let segment_row = SegmentRow::open(
                namespace.clone(),
                append.segment_id,
                segment_rel_path(&store_dir, &segment_snapshot.path),
                segment_snapshot.created_at_ms,
                last_indexed_offset,
            );

            txn.upsert_segment(&segment_row)
                .map_err(|err| wal_index_error_payload(&err))?;
            if let Some(sealed) = append.sealed.as_ref() {
                let sealed_row = SegmentRow::sealed(
                    namespace.clone(),
                    sealed.segment_id,
                    segment_rel_path(&store_dir, &sealed.path),
                    sealed.created_at_ms,
                    sealed.final_len,
                    sealed.final_len,
                );
                txn.upsert_segment(&sealed_row)
                    .map_err(|err| wal_index_error_payload(&err))?;
            }
            txn.record_event(
                &namespace,
                &event_id_for(origin, namespace.clone(), event.body.origin_seq),
                sha,
                event.prev.prev.map(|sha| sha.0),
                append.segment_id,
                append.offset,
                append.len,
                event.body.event_time_ms,
                event.body.txn_id,
                event.body.client_request_id,
            )
            .map_err(|err| wal_index_error_payload(&err))?;

            let EventKindV1::TxnV1(txn_body) = &event.body.kind;
            txn.update_hlc(&HlcRow {
                actor_id: txn_body.hlc_max.actor_id.clone(),
                last_physical_ms: txn_body.hlc_max.physical_ms,
                last_logical: txn_body.hlc_max.logical,
            })
            .map_err(|err| wal_index_error_payload(&err))?;
        }

        txn.commit().map_err(|err| wal_index_error_payload(&err))?;

        let (remote, max_stamp, durable, applied) = {
            let mut max_stamp = git_lane.last_seen_stamp.clone();
            for (event, canonical_sha) in batch.events().iter().zip(canonical_shas.iter().copied())
            {
                let apply_start = Instant::now();
                let apply_result = {
                    let state = store.state.ensure_namespace(namespace.clone());
                    apply_event(state, &event.body)
                };
                let outcome = match apply_result {
                    Ok(outcome) => {
                        metrics::apply_ok(apply_start.elapsed());
                        outcome
                    }
                    Err(err) => {
                        metrics::apply_err(apply_start.elapsed());
                        return Err(apply_event_error_payload(&namespace, &origin, err));
                    }
                };
                store.record_checkpoint_dirty_shards(&namespace, &outcome);

                let EventKindV1::TxnV1(txn_body) = &event.body.kind;
                let stamp = WriteStamp::new(txn_body.hlc_max.physical_ms, txn_body.hlc_max.logical);
                max_stamp = max_write_stamp(max_stamp, Some(stamp));

                let event_id = event_id_for(origin, namespace.clone(), event.body.origin_seq);
                let prev_sha = event.prev.prev.map(|sha| Sha256(sha.0));
                let canonical_sha = Sha256(canonical_sha);
                let broadcast = BroadcastEvent::new(
                    event_id,
                    canonical_sha,
                    prev_sha,
                    event.bytes.clone().into(),
                );
                if let Err(err) = store.broadcaster.publish(broadcast) {
                    tracing::warn!("event broadcast failed: {err}");
                }

                store
                    .watermarks_applied
                    .advance_contiguous(&namespace, &origin, event.body.origin_seq, canonical_sha.0)
                    .map_err(|err| watermark_error_payload(&namespace, &origin, err))?;
                store
                    .watermarks_durable
                    .advance_contiguous(&namespace, &origin, event.body.origin_seq, canonical_sha.0)
                    .map_err(|err| watermark_error_payload(&namespace, &origin, err))?;
            }

            if let Some(stamp) = max_stamp.clone() {
                let now_wall_ms = WallClock::now().0;
                git_lane.last_seen_stamp = Some(stamp.clone());
                git_lane.last_clock_skew = detect_clock_skew(now_wall_ms, stamp.wall_ms);
            }
            git_lane.mark_dirty();

            let durable = store
                .watermarks_durable
                .get(&namespace, &origin)
                .copied()
                .unwrap_or_else(Watermark::genesis);
            let applied = store
                .watermarks_applied
                .get(&namespace, &origin)
                .copied()
                .unwrap_or_else(Watermark::genesis);
            let remote = store.primary_remote.clone();

            (remote, max_stamp, durable, applied)
        };

        for (actor_id, stamp) in actor_stamps {
            self.clock_for_actor_mut(&actor_id).receive(&stamp);
        }

        if let Some(stamp) = max_stamp.clone() {
            self.clock.receive(&stamp);
        }
        self.mark_checkpoint_dirty(store_id, &namespace, batch.len() as u64);
        self.schedule_sync(remote);

        let mut watermark_txn = wal_index
            .begin_wal_txn()
            .map_err(|err| wal_index_error_payload(&err))?;
        watermark_txn
            .update_watermark(&namespace, &origin, applied, durable)
            .map_err(|err| wal_index_error_payload(&err))?;
        watermark_txn
            .commit()
            .map_err(|err| wal_index_error_payload(&err))?;

        Ok(IngestOutcome { durable, applied })
    }

    #[cfg(feature = "test-harness")]
    pub(crate) fn ingest_remote_batch_for_tests(
        &mut self,
        store_id: StoreId,
        batch: ContiguousBatch,
        now_ms: u64,
    ) -> Result<IngestOutcome, ReplError> {
        self.ingest_remote_batch(store_id, batch, now_ms)
    }
}
