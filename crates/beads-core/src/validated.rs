//! Validated boundary types for parsing canonical identifiers.
//!
//! These types perform trimming/canonicalization so boundary parsing is
//! consistent across CLI, protocol, and event ingress paths.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::{ActorId, BeadId, DepKind, NamespaceId};

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ValidatedBeadId(BeadId);

impl ValidatedBeadId {
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        BeadId::parse(raw).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_inner(self) -> BeadId {
        self.0
    }
}

impl fmt::Debug for ValidatedBeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ValidatedBeadId({})", self.0)
    }
}

impl fmt::Display for ValidatedBeadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<BeadId> for ValidatedBeadId {
    fn as_ref(&self) -> &BeadId {
        &self.0
    }
}

impl From<ValidatedBeadId> for BeadId {
    fn from(id: ValidatedBeadId) -> BeadId {
        id.0
    }
}

impl From<BeadId> for ValidatedBeadId {
    fn from(id: BeadId) -> ValidatedBeadId {
        ValidatedBeadId(id)
    }
}

impl TryFrom<String> for ValidatedBeadId {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ValidatedBeadId::parse(&value)
    }
}

impl From<ValidatedBeadId> for String {
    fn from(id: ValidatedBeadId) -> String {
        id.0.to_string()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ValidatedNamespaceId(NamespaceId);

impl ValidatedNamespaceId {
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        let trimmed = raw.trim();
        let normalized = trimmed.to_lowercase();
        NamespaceId::parse(normalized).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_inner(self) -> NamespaceId {
        self.0
    }
}

impl fmt::Debug for ValidatedNamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ValidatedNamespaceId({})", self.0)
    }
}

impl fmt::Display for ValidatedNamespaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<NamespaceId> for ValidatedNamespaceId {
    fn as_ref(&self) -> &NamespaceId {
        &self.0
    }
}

impl From<ValidatedNamespaceId> for NamespaceId {
    fn from(id: ValidatedNamespaceId) -> NamespaceId {
        id.0
    }
}

impl From<NamespaceId> for ValidatedNamespaceId {
    fn from(id: NamespaceId) -> ValidatedNamespaceId {
        ValidatedNamespaceId(id)
    }
}

impl TryFrom<String> for ValidatedNamespaceId {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ValidatedNamespaceId::parse(&value)
    }
}

impl From<ValidatedNamespaceId> for String {
    fn from(id: ValidatedNamespaceId) -> String {
        id.0.to_string()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ValidatedDepKind(DepKind);

impl ValidatedDepKind {
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        DepKind::parse(raw).map(Self)
    }

    pub fn as_str(&self) -> &'static str {
        self.0.as_str()
    }

    pub fn into_inner(self) -> DepKind {
        self.0
    }
}

impl fmt::Debug for ValidatedDepKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ValidatedDepKind({})", self.0.as_str())
    }
}

impl fmt::Display for ValidatedDepKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.as_str())
    }
}

impl AsRef<DepKind> for ValidatedDepKind {
    fn as_ref(&self) -> &DepKind {
        &self.0
    }
}

impl From<ValidatedDepKind> for DepKind {
    fn from(kind: ValidatedDepKind) -> DepKind {
        kind.0
    }
}

impl From<DepKind> for ValidatedDepKind {
    fn from(kind: DepKind) -> ValidatedDepKind {
        ValidatedDepKind(kind)
    }
}

impl TryFrom<String> for ValidatedDepKind {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ValidatedDepKind::parse(&value)
    }
}

impl From<ValidatedDepKind> for String {
    fn from(kind: ValidatedDepKind) -> String {
        kind.0.as_str().to_string()
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ValidatedActorId(ActorId);

impl ValidatedActorId {
    pub fn parse(raw: &str) -> Result<Self, CoreError> {
        let trimmed = raw.trim();
        ActorId::new(trimmed.to_string()).map(Self)
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_inner(self) -> ActorId {
        self.0
    }
}

impl fmt::Debug for ValidatedActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ValidatedActorId({})", self.0)
    }
}

impl fmt::Display for ValidatedActorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<ActorId> for ValidatedActorId {
    fn as_ref(&self) -> &ActorId {
        &self.0
    }
}

impl From<ValidatedActorId> for ActorId {
    fn from(id: ValidatedActorId) -> ActorId {
        id.0
    }
}

impl From<ActorId> for ValidatedActorId {
    fn from(id: ActorId) -> ValidatedActorId {
        ValidatedActorId(id)
    }
}

impl TryFrom<String> for ValidatedActorId {
    type Error = CoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        ValidatedActorId::parse(&value)
    }
}

impl From<ValidatedActorId> for String {
    fn from(id: ValidatedActorId) -> String {
        id.0.to_string()
    }
}
