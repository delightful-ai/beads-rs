use super::*;

#[derive(Clone, Debug)]
pub(crate) struct ReadScope {
    namespace: NamespaceId,
    require_min_seen: Option<Watermarks<Applied>>,
    wait_timeout_ms: u64,
}

impl ReadScope {
    pub(crate) fn new(
        read: ReadConsistency,
        policies: &BTreeMap<NamespaceId, NamespacePolicy>,
    ) -> Result<Self, OpError> {
        let namespace = Self::normalize_namespace(read.namespace, policies)?;
        Ok(Self {
            namespace,
            require_min_seen: read.require_min_seen,
            wait_timeout_ms: read.wait_timeout_ms.unwrap_or(0),
        })
    }

    pub(crate) fn normalize_namespace(
        raw: Option<NamespaceId>,
        policies: &BTreeMap<NamespaceId, NamespacePolicy>,
    ) -> Result<NamespaceId, OpError> {
        let namespace = raw.unwrap_or_else(NamespaceId::core);
        if policies.contains_key(&namespace) {
            Ok(namespace)
        } else {
            Err(OpError::NamespaceUnknown { namespace })
        }
    }

    pub(crate) fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }

    pub(crate) fn require_min_seen(&self) -> Option<&Watermarks<Applied>> {
        self.require_min_seen.as_ref()
    }

    pub(crate) fn wait_timeout_ms(&self) -> u64 {
        self.wait_timeout_ms
    }
}

#[derive(Clone, Debug)]
pub(crate) enum ReadGateStatus {
    Satisfied,
    Unsatisfied {
        required: Watermarks<Applied>,
        current_applied: Watermarks<Applied>,
    },
}
