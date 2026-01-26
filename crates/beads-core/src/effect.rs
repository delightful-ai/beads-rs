//! Error classification types for effect tracking and retry semantics.

/// Whether retrying this operation may succeed.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Transience {
    /// Retry will never help without changing inputs/state.
    Permanent,
    /// Retry may help (transient contention/outage).
    Retryable,
    /// Unknown if retry will help.
    Unknown,
}

impl Transience {
    pub fn is_retryable(self) -> bool {
        matches!(self, Transience::Retryable)
    }
}

/// What we know about side effects when an error is returned.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Effect {
    /// Definitely no side effects occurred.
    None,
    /// Side effects definitely occurred (locally or remotely).
    Some,
    /// We don't know if side effects occurred.
    Unknown,
}

crate::enum_str! {
    impl Effect {
        pub fn as_str(&self) -> &'static str;
        fn parse_str(raw: &str) -> Option<Self>;
        variants {
            None => ["none"],
            Some => ["some"],
            Unknown => ["unknown"],
        }
    }
}
