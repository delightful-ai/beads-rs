use std::collections::BTreeSet;

use beads_rs::ErrorCode;

fn parse_error_codes(doc: &str) -> BTreeSet<String> {
    let mut codes = BTreeSet::new();
    for line in doc.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }
        let mut columns = trimmed.trim_matches('|').split('|').map(str::trim);
        let Some(first) = columns.next() else {
            continue;
        };
        let Some(code) = first
            .strip_prefix('`')
            .and_then(|value| value.strip_suffix('`'))
        else {
            continue;
        };
        if code.eq_ignore_ascii_case("code") || code.is_empty() {
            continue;
        }
        codes.insert(code.to_string());
    }
    codes
}
