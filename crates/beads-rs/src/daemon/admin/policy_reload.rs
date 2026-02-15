use super::*;

pub(super) struct PolicyReloadSummary {
    pub(super) applied: Vec<AdminPolicyDiff>,
    pub(super) requires_restart: Vec<AdminPolicyDiff>,
    pub(super) updated: BTreeMap<NamespaceId, NamespacePolicy>,
}

pub(super) fn diff_policy_reload(
    current: &BTreeMap<NamespaceId, NamespacePolicy>,
    desired: &BTreeMap<NamespaceId, NamespacePolicy>,
) -> PolicyReloadSummary {
    let mut applied = Vec::new();
    let mut requires_restart = Vec::new();
    let mut updated = current.clone();

    let mut namespaces = BTreeSet::new();
    namespaces.extend(current.keys().cloned());
    namespaces.extend(desired.keys().cloned());

    for namespace in namespaces {
        match (current.get(&namespace), desired.get(&namespace)) {
            (Some(old), Some(new)) => {
                let mut updated_policy = old.clone();
                let (safe, restart) = diff_namespace_policy(old, new, &mut updated_policy);
                if !safe.is_empty() {
                    applied.push(AdminPolicyDiff {
                        namespace: namespace.clone(),
                        changes: safe,
                    });
                }
                if !restart.is_empty() {
                    requires_restart.push(AdminPolicyDiff {
                        namespace: namespace.clone(),
                        changes: restart,
                    });
                }
                updated.insert(namespace.clone(), updated_policy);
            }
            (None, Some(_)) => {
                requires_restart.push(AdminPolicyDiff {
                    namespace: namespace.clone(),
                    changes: vec![AdminPolicyChange {
                        field: "namespace".to_string(),
                        before: "absent".to_string(),
                        after: "added".to_string(),
                    }],
                });
            }
            (Some(_), None) => {
                requires_restart.push(AdminPolicyDiff {
                    namespace: namespace.clone(),
                    changes: vec![AdminPolicyChange {
                        field: "namespace".to_string(),
                        before: "present".to_string(),
                        after: "removed".to_string(),
                    }],
                });
            }
            (None, None) => {}
        }
    }

    PolicyReloadSummary {
        applied,
        requires_restart,
        updated,
    }
}

fn diff_namespace_policy(
    current: &NamespacePolicy,
    desired: &NamespacePolicy,
    updated: &mut NamespacePolicy,
) -> (Vec<AdminPolicyChange>, Vec<AdminPolicyChange>) {
    let mut safe = Vec::new();
    let mut restart = Vec::new();

    if current.visibility != desired.visibility {
        safe.push(policy_change(
            "visibility",
            format_visibility(current.visibility),
            format_visibility(desired.visibility),
        ));
        updated.visibility = desired.visibility;
    }

    if current.ready_eligible != desired.ready_eligible {
        safe.push(policy_change(
            "ready_eligible",
            format_bool(current.ready_eligible),
            format_bool(desired.ready_eligible),
        ));
        updated.ready_eligible = desired.ready_eligible;
    }

    if current.retention != desired.retention {
        safe.push(policy_change(
            "retention",
            format_retention(current.retention),
            format_retention(desired.retention),
        ));
        updated.retention = desired.retention;
    }

    if current.ttl_basis != desired.ttl_basis {
        safe.push(policy_change(
            "ttl_basis",
            format_ttl_basis(current.ttl_basis),
            format_ttl_basis(desired.ttl_basis),
        ));
        updated.ttl_basis = desired.ttl_basis;
    }

    if current.persist_to_git != desired.persist_to_git {
        restart.push(policy_change(
            "persist_to_git",
            format_bool(current.persist_to_git),
            format_bool(desired.persist_to_git),
        ));
    }

    if current.replicate_mode != desired.replicate_mode {
        restart.push(policy_change(
            "replicate_mode",
            format_replicate_mode(current.replicate_mode),
            format_replicate_mode(desired.replicate_mode),
        ));
    }

    if current.gc_authority != desired.gc_authority {
        restart.push(policy_change(
            "gc_authority",
            format_gc_authority(current.gc_authority),
            format_gc_authority(desired.gc_authority),
        ));
    }

    (safe, restart)
}

fn policy_change(field: &str, before: String, after: String) -> AdminPolicyChange {
    AdminPolicyChange {
        field: field.to_string(),
        before,
        after,
    }
}

fn format_bool(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

fn format_visibility(value: crate::core::NamespaceVisibility) -> String {
    match value {
        crate::core::NamespaceVisibility::Normal => "normal".to_string(),
        crate::core::NamespaceVisibility::Pinned => "pinned".to_string(),
    }
}

fn format_replicate_mode(value: crate::core::ReplicateMode) -> String {
    match value {
        crate::core::ReplicateMode::None => "none".to_string(),
        crate::core::ReplicateMode::Anchors => "anchors".to_string(),
        crate::core::ReplicateMode::Peers => "peers".to_string(),
        crate::core::ReplicateMode::P2p => "p2p".to_string(),
    }
}

fn format_retention(value: crate::core::RetentionPolicy) -> String {
    match value {
        crate::core::RetentionPolicy::Forever => "forever".to_string(),
        crate::core::RetentionPolicy::Ttl { ttl_ms } => format!("ttl:{ttl_ms}ms"),
        crate::core::RetentionPolicy::Size { max_bytes } => format!("size:{max_bytes}bytes"),
    }
}

fn format_ttl_basis(value: crate::core::TtlBasis) -> String {
    match value {
        crate::core::TtlBasis::LastMutationStamp => "last_mutation_stamp".to_string(),
        crate::core::TtlBasis::EventTime => "event_time".to_string(),
        crate::core::TtlBasis::ExplicitField => "explicit_field".to_string(),
    }
}

fn format_gc_authority(value: crate::core::GcAuthority) -> String {
    match value {
        crate::core::GcAuthority::CheckpointWriter => "checkpoint_writer".to_string(),
        crate::core::GcAuthority::ExplicitReplica { replica_id } => {
            format!("explicit_replica:{replica_id}")
        }
        crate::core::GcAuthority::None => "none".to_string(),
    }
}
