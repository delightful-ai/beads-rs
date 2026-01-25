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

#[test]
fn realtime_error_codes_are_known() {
    let doc = include_str!("../../../../REALTIME_ERRORS.md");
    let codes = parse_error_codes(doc);
    assert!(
        !codes.is_empty(),
        "no error codes parsed from REALTIME_ERRORS.md"
    );

    let unknown: Vec<String> = codes
        .iter()
        .filter(|code| matches!(ErrorCode::parse(code), ErrorCode::Unknown(_)))
        .cloned()
        .collect();
    assert!(
        unknown.is_empty(),
        "unrecognized error codes in REALTIME_ERRORS.md: {}",
        unknown.join(", ")
    );
}
