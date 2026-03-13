//! Query surface aliases used by internal read executors.

pub use beads_surface::query::{Filters, SortField};

pub type QueryResult = beads_api::QueryResult;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_default_matches_all() {
        // Can't easily test without a bead, but at least verify default construction
        let filters = Filters::default();
        assert!(filters.status.is_none());
        assert!(filters.priority.is_none());
        assert!(!filters.unclaimed);
    }
}
