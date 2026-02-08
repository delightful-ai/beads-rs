use beads_core::NamespaceId;

pub(crate) fn fmt_issue_ref(namespace: &NamespaceId, id: &str) -> String {
    format!("{}/{}", namespace.as_str(), id)
}

pub(crate) fn fmt_labels(labels: &[String]) -> String {
    let mut out = String::from("[");
    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(label);
    }
    out.push(']');
    out
}
