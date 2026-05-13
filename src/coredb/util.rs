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
/// quotes, doubles any embedded `'`, and collapses ASCII control
/// characters (newline, carriage return, tab, plus the rest of
/// `< 0x20`) into a single space.
///
/// The control-char folding is for CoreDB specifically: its CQL parser
/// is regex-based and rejects literal newlines inside string values
/// with "Invalid INSERT syntax", even though standard Cassandra
/// accepts them. LLM-generated content frequently contains newlines
/// (raw JSON responses, multi-sentence reasoning), so without this
/// sanitization every LLM-mode INSERT fails.
pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        if c == '\'' {
            out.push('\'');
            out.push('\'');
        } else if (c as u32) < 0x20 {
            // Tab, LF, CR, and other C0 control chars → space. Keeps
            // the visible text readable; never introduces a stray
            // quote that could leak out of the literal.
            out.push(' ');
        } else {
            out.push(c);
        }
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

    #[test]
    fn esc_collapses_control_chars() {
        // LLM responses often contain literal newlines / tabs which
        // CoreDB's CQL parser rejects. They should become spaces, and
        // the quote-escape rule still applies in the same string.
        assert_eq!(
            esc("line one\nline two\ttabbed"),
            "'line one line two tabbed'",
        );
        assert_eq!(esc("with 'quotes'\nand newline"), "'with ''quotes'' and newline'");
    }
}
