//! Canonical JSON helpers for checkpoint hashing.

pub use crate::core::json_canon::{CanonJsonError, to_canon_json_bytes};

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn canon_json_sorts_keys() {
        let mut map = HashMap::new();
        map.insert("b", 2);
        map.insert("a", 1);

        let bytes = to_canon_json_bytes(&map).expect("canon json");
        assert_eq!(bytes, br#"{"a":1,"b":2}"#);
    }
}
