//! CQL-literal helpers for inline INSERT/SELECT statements.
//!
//! Why inline values instead of parameter bindings: CoreDB's prepared-
//! statement metadata is not Cassandra-compatible enough for the
//! scylla driver to deserialize, and `query_unpaged` with non-empty
//! values transparently prepares first. We work around this by
//! building plain `query_unpaged(_, ())` calls where every value is
//! already serialized into the CQL text. The escapers below are the
//! only safety boundary, so all repository writers must go through
//! them rather than concatenating strings directly.

/// Escape a TEXT value into a CQL string literal: wraps in single
/// quotes and doubles any embedded `'`.
pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
        }
        out.push(c);
    }
    out.push('\'');
    out
}

/// CQL boolean literal.
pub fn fbool(b: bool) -> &'static str {
    if b {
        "true"
    } else {
        "false"
    }
}

/// UUID literal — Cassandra accepts the canonical `xxxxxxxx-xxxx-...`
/// form without quotes.
pub fn fuuid(u: uuid::Uuid) -> String {
    u.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn esc_doubles_quotes() {
        assert_eq!(esc("it's"), "'it''s'");
        assert_eq!(esc(""), "''");
        assert_eq!(esc("plain"), "'plain'");
    }
}
