use super::*;

pub(super) fn load_timeout() -> Duration {
    let override_secs = std::env::var("BD_LOAD_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|v| *v > 0);
    Duration::from_secs(override_secs.unwrap_or(LOAD_TIMEOUT_SECS))
}

pub(super) const CLOCK_SKEW_WARN_MS: u64 = 5 * 60 * 1000;

pub(crate) fn detect_clock_skew(now_ms: u64, reference_ms: u64) -> Option<ClockSkewRecord> {
    let delta_ms = now_ms as i64 - reference_ms as i64;
    if delta_ms.unsigned_abs() >= CLOCK_SKEW_WARN_MS {
        Some(ClockSkewRecord {
            delta_ms,
            wall_ms: now_ms,
        })
    } else {
        None
    }
}

pub(crate) fn max_write_stamp(a: Option<WriteStamp>, b: Option<WriteStamp>) -> Option<WriteStamp> {
    match (a, b) {
        (Some(a), Some(b)) => Some(std::cmp::max(a, b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

pub(super) fn apply_checkpoint_watermarks(
    store: &mut StoreRuntime,
    imports: &[CheckpointImport],
) -> Result<(), StoreRuntimeError> {
    if imports.is_empty() {
        return Ok(());
    }

    let mut origins: BTreeMap<NamespaceId, BTreeMap<ReplicaId, u64>> = BTreeMap::new();
    for import in imports {
        for (namespace, origin_map) in &import.included {
            for (origin, seq) in origin_map {
                let head =
                    checkpoint_head_status(import.included_heads.as_ref(), namespace, origin, *seq)
                        .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                            kind: "applied",
                            namespace: namespace.clone(),
                            origin: *origin,
                            source: Box::new(source),
                        })?;
                store
                    .watermarks_applied
                    .observe_at_least(namespace, origin, Seq0::new(*seq), head)
                    .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                        kind: "applied",
                        namespace: namespace.clone(),
                        origin: *origin,
                        source: Box::new(source),
                    })?;
                store
                    .watermarks_durable
                    .observe_at_least(namespace, origin, Seq0::new(*seq), head)
                    .map_err(|source| StoreRuntimeError::WatermarkInvalid {
                        kind: "durable",
                        namespace: namespace.clone(),
                        origin: *origin,
                        source: Box::new(source),
                    })?;
                origins
                    .entry(namespace.clone())
                    .or_default()
                    .insert(*origin, *seq);
            }
        }
    }

    if origins.is_empty() {
        return Ok(());
    }

    let wal_index = store.wal_index.clone();
    let mut max_event_seq: BTreeMap<(NamespaceId, ReplicaId), Seq0> = BTreeMap::new();
    for (namespace, origin_map) in &origins {
        for origin in origin_map.keys() {
            let max_seq = wal_index.reader().max_origin_seq(namespace, origin)?;
            max_event_seq.insert((namespace.clone(), *origin), max_seq);
        }
    }

    let mut txn = wal_index.writer().begin_txn()?;
    for (namespace, origin_map) in origins {
        for (origin, _) in origin_map {
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

            let max_seq = max_event_seq
                .get(&(namespace.clone(), origin))
                .copied()
                .unwrap_or(Seq0::ZERO);
            let next_base = durable.seq().get().max(max_seq.get());
            let next_seq_raw =
                next_base
                    .checked_add(1)
                    .ok_or_else(|| WalIndexError::OriginSeqOverflow {
                        namespace: namespace.to_string(),
                        origin,
                    })?;
            let next_seq = Seq1::from_u64(next_seq_raw).ok_or_else(|| {
                WalIndexError::EventIdDecode("origin_seq must be >= 1".to_string())
            })?;
            txn.set_next_origin_seq(&namespace, &origin, next_seq)?;

            txn.update_watermark(&namespace, &origin, applied, durable)?;
        }
    }
    txn.commit()?;
    Ok(())
}

pub(super) fn checkpoint_head_status(
    included_heads: Option<&IncludedHeads>,
    namespace: &NamespaceId,
    origin: &ReplicaId,
    seq: u64,
) -> Result<HeadStatus, WatermarkError> {
    if seq == 0 {
        return Ok(HeadStatus::Genesis);
    }
    if let Some(heads) = included_heads
        && let Some(origins) = heads.get(namespace)
        && let Some(head) = origins.get(origin)
    {
        return Ok(HeadStatus::Known(*head.as_bytes()));
    }
    Err(WatermarkError::MissingHead {
        seq: Seq0::new(seq),
    })
}

pub(super) fn checkpoint_ref_oid(
    repo: &Repository,
    git_ref: &str,
) -> Result<Option<Oid>, git2::Error> {
    if let Some(oid) = refname_to_id_optional(repo, git_ref)? {
        return Ok(Some(oid));
    }
    let Some(remote_ref) = checkpoint_remote_tracking_ref(git_ref) else {
        return Ok(None);
    };
    refname_to_id_optional(repo, &remote_ref)
}

pub(super) fn checkpoint_remote_tracking_ref(git_ref: &str) -> Option<String> {
    let suffix = git_ref.strip_prefix("refs/")?;
    Some(format!("refs/remotes/origin/{suffix}"))
}

pub(super) fn refname_to_id_optional(
    repo: &Repository,
    name: &str,
) -> Result<Option<Oid>, git2::Error> {
    match repo.refname_to_id(name) {
        Ok(oid) => Ok(Some(oid)),
        Err(err) if err.code() == GitErrorCode::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

pub(super) fn write_checkpoint_tree(
    repo: &Repository,
    tree: &git2::Tree,
    dir: &Path,
) -> Result<(), CheckpointTreeError> {
    let mut outcome: Result<(), CheckpointTreeError> = Ok(());
    tree.walk(TreeWalkMode::PreOrder, |root, entry| {
        if outcome.is_err() {
            return TreeWalkResult::Abort;
        }
        if entry.kind() != Some(ObjectType::Blob) {
            return TreeWalkResult::Ok;
        }
        let name = match entry.name() {
            Some(name) => name,
            None => {
                outcome = Err(CheckpointTreeError::InvalidPath {
                    path: root.to_string(),
                });
                return TreeWalkResult::Abort;
            }
        };
        let rel_path = Path::new(root).join(name);
        if rel_path.components().any(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::ParentDir
            )
        }) {
            outcome = Err(CheckpointTreeError::InvalidPath {
                path: rel_path.display().to_string(),
            });
            return TreeWalkResult::Abort;
        }
        let full_path = dir.join(&rel_path);
        if let Some(parent) = full_path.parent()
            && let Err(err) = fs::create_dir_all(parent)
        {
            outcome = Err(CheckpointTreeError::Io {
                path: parent.to_path_buf(),
                source: err,
            });
            return TreeWalkResult::Abort;
        }
        let blob = match repo.find_blob(entry.id()) {
            Ok(blob) => blob,
            Err(err) => {
                outcome = Err(CheckpointTreeError::Git(err));
                return TreeWalkResult::Abort;
            }
        };
        if let Err(err) = fs::write(&full_path, blob.content()) {
            outcome = Err(CheckpointTreeError::Io {
                path: full_path,
                source: err,
            });
            return TreeWalkResult::Abort;
        }
        TreeWalkResult::Ok
    })?;

    outcome
}

pub(crate) fn replay_event_wal(
    store_id: StoreId,
    wal_index: &dyn WalIndex,
    state: &mut StoreState,
    limits: &Limits,
) -> Result<bool, StoreRuntimeError> {
    let store_dir = crate::paths::store_dir(store_id);
    let rows = wal_index.reader().load_watermarks()?;
    if rows.is_empty() {
        return Ok(false);
    }

    let mut segment_cache: HashMap<NamespaceId, HashMap<SegmentId, PathBuf>> = HashMap::new();
    let mut applied_any = false;

    for row in rows {
        if row.applied.seq().get() == 0 {
            continue;
        }
        let namespace = row.namespace.clone();
        if !segment_cache.contains_key(&namespace) {
            let segments = segment_paths_for_namespace(&store_dir, wal_index, &namespace)?;
            segment_cache.insert(namespace.clone(), segments);
        }
        let segments = segment_cache.get(&namespace).ok_or_else(|| {
            StoreRuntimeError::WalIndex(WalIndexError::SegmentRowDecode(
                "segment cache missing".to_string(),
            ))
        })?;

        let state_for_namespace = state.ensure_namespace(namespace.clone());
        let mut from_seq_excl = Seq0::ZERO;
        while from_seq_excl.get() < row.applied.seq().get() {
            let items = wal_index.reader().iter_from(
                &namespace,
                &row.origin,
                from_seq_excl,
                limits.max_event_batch_bytes,
            )?;
            if items.is_empty() {
                return Err(StoreRuntimeError::WalReplay(Box::new(
                    WalReplayError::NonContiguousSeq {
                        namespace: row.namespace.clone(),
                        origin: row.origin,
                        expected: from_seq_excl.next(),
                        got: Seq0::ZERO,
                    },
                )));
            }
            for item in items {
                let seq = item.event_id.origin_seq.get();
                if seq > row.applied.seq().get() {
                    from_seq_excl = row.applied.seq();
                    break;
                }
                let segment_path = segments.get(&item.segment_id).ok_or_else(|| {
                    StoreRuntimeError::WalIndex(WalIndexError::SegmentRowDecode(format!(
                        "missing segment {} for {}",
                        item.segment_id,
                        namespace.as_str(),
                    )))
                })?;
                let event_body = load_event_body_at(segment_path, item.offset, limits)?;
                apply_event(state_for_namespace, &event_body).map_err(|err| {
                    StoreRuntimeError::WalReplay(Box::new(WalReplayError::RecordDecode {
                        path: segment_path.clone(),
                        source: EventWalError::RecordHeaderInvalid {
                            reason: format!("apply_event failed: {err}"),
                        },
                    }))
                })?;
                from_seq_excl = Seq0::new(seq);
                applied_any = true;
            }
        }
    }

    Ok(applied_any)
}

pub(super) fn segment_paths_for_namespace(
    store_dir: &Path,
    wal_index: &dyn WalIndex,
    namespace: &NamespaceId,
) -> Result<HashMap<SegmentId, PathBuf>, StoreRuntimeError> {
    let segments = wal_index.reader().list_segments(namespace)?;
    let mut map = HashMap::new();
    for segment in segments {
        let path = if segment.segment_path().is_absolute() {
            segment.segment_path().to_path_buf()
        } else {
            store_dir.join(segment.segment_path())
        };
        map.insert(segment.segment_id(), path);
    }
    Ok(map)
}

pub(super) fn load_event_body_at(
    path: &Path,
    offset: u64,
    limits: &Limits,
) -> Result<ValidatedEventBody, StoreRuntimeError> {
    let mut reader = open_segment_reader(path).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(match source {
            EventWalError::Io { source, .. } => WalReplayError::Io {
                path: path.to_path_buf(),
                source,
            },
            other => WalReplayError::RecordDecode {
                path: path.to_path_buf(),
                source: other,
            },
        }))
    })?;
    reader.seek(SeekFrom::Start(offset)).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(WalReplayError::Io {
            path: path.to_path_buf(),
            source,
        }))
    })?;

    let mut reader = FrameReader::new(reader, limits.policy().max_wal_record_bytes());
    let record = reader
        .read_next()
        .map_err(|source| {
            StoreRuntimeError::WalReplay(Box::new(WalReplayError::RecordDecode {
                path: path.to_path_buf(),
                source,
            }))
        })?
        .ok_or_else(|| {
            StoreRuntimeError::WalReplay(Box::new(WalReplayError::RecordDecode {
                path: path.to_path_buf(),
                source: EventWalError::FrameLengthInvalid {
                    reason: "unexpected eof while reading record".to_string(),
                },
            }))
        })?;

    let (_, event_body) = decode_event_body(record.payload_bytes(), limits).map_err(|source| {
        StoreRuntimeError::WalReplay(Box::new(WalReplayError::EventBodyDecode {
            path: path.to_path_buf(),
            offset,
            source,
        }))
    })?;
    Ok(event_body)
}

pub(super) fn replication_listen_addr(config: &crate::config::ReplicationConfig) -> String {
    let trimmed = config.listen_addr.trim();
    if trimmed.is_empty() {
        "127.0.0.1:0".to_string()
    } else {
        trimmed.to_string()
    }
}

pub(super) fn replication_max_connections(
    config: &crate::config::ReplicationConfig,
) -> Option<NonZeroUsize> {
    match config.max_connections {
        Some(0) => None,
        Some(value) => NonZeroUsize::new(value),
        None => NonZeroUsize::new(DEFAULT_REPL_MAX_CONNECTIONS),
    }
}

pub(super) fn replication_backoff(config: &crate::config::ReplicationConfig) -> BackoffPolicy {
    let base = Duration::from_millis(config.backoff_base_ms);
    let mut max = Duration::from_millis(config.backoff_max_ms);
    if max < base {
        max = base;
    }
    BackoffPolicy { base, max }
}

pub(super) fn resolve_checkpoint_git_ref(
    store_id: StoreId,
    group: &str,
    git_ref: Option<&str>,
) -> String {
    let raw = git_ref.unwrap_or("").trim();
    if raw.is_empty() {
        return format!("refs/beads/{store_id}/{group}");
    }
    raw.replace("{store_id}", &store_id.to_string())
        .replace("{group}", group)
}

pub(super) fn event_id_for(origin: ReplicaId, namespace: NamespaceId, origin_seq: Seq1) -> EventId {
    EventId::new(origin, namespace, origin_seq)
}

pub(super) fn segment_rel_path(store_dir: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(store_dir).unwrap_or(path).to_path_buf()
}

pub(super) fn event_wal_error_payload(
    namespace: &NamespaceId,
    segment_id: Option<SegmentId>,
    offset: Option<u64>,
    err: EventWalError,
) -> ReplError {
    ReplError::new(ProtocolErrorCode::WalCorrupt.into(), "wal error", true).with_details(
        ReplErrorDetails::WalCorrupt(error_details::WalCorruptDetails {
            namespace: namespace.clone(),
            segment_id,
            offset,
            reason: err.to_string(),
        }),
    )
}

pub(super) fn apply_event_error_payload(
    namespace: &NamespaceId,
    origin: &ReplicaId,
    err: ApplyError,
) -> ReplError {
    let reason = format!("apply_event rejected for {namespace}/{origin}: {err}");
    ReplError::new(
        ProtocolErrorCode::Corruption.into(),
        "apply_event rejected",
        false,
    )
    .with_details(ReplErrorDetails::Corruption(
        error_details::CorruptionDetails { reason },
    ))
}

pub(super) fn wal_index_error_payload(err: &WalIndexError) -> ReplError {
    match err {
        WalIndexError::Equivocation {
            namespace,
            origin,
            seq,
            existing_sha256,
            new_sha256,
        } => ReplError::new(
            ProtocolErrorCode::Equivocation.into(),
            "equivocation",
            false,
        )
        .with_details(ReplErrorDetails::Equivocation(
            error_details::EquivocationDetails {
                eid: error_details::EventIdDetails {
                    namespace: namespace.clone(),
                    origin_replica_id: *origin,
                    origin_seq: *seq,
                },
                existing_sha256: hex::encode(existing_sha256),
                new_sha256: hex::encode(new_sha256),
            },
        )),
        WalIndexError::ClientRequestIdReuseMismatch {
            namespace,
            client_request_id,
            expected_request_sha256,
            got_request_sha256,
            ..
        } => ReplError::new(
            ProtocolErrorCode::ClientRequestIdReuseMismatch.into(),
            "client_request_id reuse mismatch",
            false,
        )
        .with_details(ReplErrorDetails::ClientRequestIdReuseMismatch(
            error_details::ClientRequestIdReuseMismatchDetails {
                namespace: namespace.clone(),
                client_request_id: *client_request_id,
                expected_request_sha256: hex::encode(expected_request_sha256),
                got_request_sha256: hex::encode(got_request_sha256),
            },
        )),
        _ => ReplError::new(ProtocolErrorCode::IndexCorrupt.into(), "index error", true)
            .with_details(ReplErrorDetails::IndexCorrupt(
                error_details::IndexCorruptDetails {
                    reason: err.to_string(),
                },
            )),
    }
}

pub(super) fn watermark_error_payload(
    namespace: &NamespaceId,
    origin: &ReplicaId,
    err: WatermarkError,
) -> ReplError {
    match err {
        WatermarkError::NonContiguous { expected, got } => {
            let durable_seen = expected.prev_seq0().get();
            ReplError::new(ProtocolErrorCode::GapDetected.into(), "gap detected", false)
                .with_details(ReplErrorDetails::GapDetected(
                    error_details::GapDetectedDetails {
                        namespace: namespace.clone(),
                        origin_replica_id: *origin,
                        durable_seen,
                        got_seq: got.get(),
                    },
                ))
        }
        other => ReplError::new(CliErrorCode::Internal.into(), other.to_string(), false),
    }
}

#[cfg(any(test, feature = "test-harness"))]
pub(crate) fn insert_store_for_tests(
    daemon: &mut Daemon,
    store_id: StoreId,
    remote: RemoteUrl,
    repo_path: &Path,
) -> Result<(), OpError> {
    let open = StoreRuntime::open(
        store_id,
        remote.clone(),
        WallClock::now().0,
        env!("CARGO_PKG_VERSION"),
        daemon.limits(),
        &daemon.namespace_defaults,
    )
    .map_err(|err| OpError::StoreRuntime(Box::new(err)))?;
    daemon.seed_actor_clocks(&open.runtime)?;
    daemon.stores.insert(store_id, open.runtime);
    daemon.git_lanes.insert(
        store_id,
        GitLaneState::with_path(None, repo_path.to_owned()),
    );
    daemon.store_caches.remote_to_store.insert(
        remote.clone(),
        StoreIdResolution::verified(store_id, StoreIdSource::GitMeta),
    );
    daemon.store_caches.path_to_store.insert(
        repo_path.to_owned(),
        StoreIdResolution::verified(store_id, StoreIdSource::GitMeta),
    );
    daemon
        .store_caches
        .path_to_remote
        .insert(repo_path.to_owned(), remote);
    daemon.register_default_checkpoint_groups(store_id)?;
    Ok(())
}
