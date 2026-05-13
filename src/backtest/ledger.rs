//! On-disk JSONL ledger for Decision rows.
//!
//! Why this exists: the CoreDB server (yhc007/coredb) currently
//! advertises every SELECT column as type `Text` in result metadata
//! while sending raw binary payload for non-text columns. The scylla
//! driver can't decode that on either the typed or untyped read path,
//! so `decisions.list_day` against CoreDB is unusable. Until the
//! CoreDB side emits correct type codes, the comparator needs a
//! parallel store it can actually read back.
//!
//! Each call to [`append`] writes one Decision as one JSON object on
//! one line. [`read_day`] streams the file and returns the rows whose
//! `bucket_day_ms` matches. The file is append-only, so concurrent
//! writers race only on the final byte of each line — for the
//! single-process backtest path that's fine.
//!
//! Path resolution: `DECISIONS_LEDGER` env var if set, otherwise
//! `./decisions.jsonl` in the process's cwd. Override the env var when
//! running multiple agent instances against the same workspace.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::coredb::types::{Decision, Millis};

const DEFAULT_PATH: &str = "./decisions.jsonl";

/// Resolved filesystem path for the ledger.
pub fn path() -> PathBuf {
    std::env::var_os("DECISIONS_LEDGER")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_PATH))
}

/// Append one Decision to the ledger. Each row becomes a single JSON
/// line; existing rows are untouched. The file is created on first
/// write.
pub fn append(d: &Decision) -> Result<()> {
    let p = path();
    let mut f = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .with_context(|| format!("open ledger at {}", p.display()))?;
    let line = serde_json::to_string(d).context("serialize decision")?;
    f.write_all(line.as_bytes())?;
    f.write_all(b"\n")?;
    Ok(())
}

/// Read every Decision row from the ledger whose `bucket_day_ms`
/// matches `bucket_day_ms`. Missing file returns an empty list (no
/// decisions written yet is a normal state). Malformed lines are
/// skipped with a warning rather than failing the whole read.
pub fn read_day(bucket_day_ms: Millis) -> Result<Vec<Decision>> {
    let p = path();
    if !Path::new(&p).exists() {
        return Ok(Vec::new());
    }
    let f = File::open(&p).with_context(|| format!("open ledger at {}", p.display()))?;
    let mut out = Vec::new();
    let mut skipped = 0u32;
    for (i, line) in BufReader::new(f).lines().enumerate() {
        let line = match line {
            Ok(s) => s,
            Err(e) => {
                eprintln!("ledger: read error on line {}: {e}", i + 1);
                skipped += 1;
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Decision>(&line) {
            Ok(d) if d.bucket_day_ms == bucket_day_ms => out.push(d),
            Ok(_) => {} // different day, ignore
            Err(e) => {
                eprintln!("ledger: parse error on line {}: {e}", i + 1);
                skipped += 1;
            }
        }
    }
    if skipped > 0 {
        eprintln!("ledger: skipped {skipped} malformed lines");
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn sample(bd: i64) -> Decision {
        Decision {
            bucket_day_ms: bd,
            ts_ms: bd + 1,
            decision_id: Uuid::new_v4(),
            market_slug: "test-market".to_string(),
            side: "YES".to_string(),
            size_usd: 10.0,
            confidence: 0.6,
            edge_bps: 200,
            reasoning: "test reason".to_string(),
            raw_response: "baseline-rule".to_string(),
            entry_price: 0.5,
        }
    }

    #[test]
    fn roundtrip_filters_by_bucket_day() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("decisions.jsonl");
        std::env::set_var("DECISIONS_LEDGER", &p);

        let bd1 = 1_700_000_000_000i64;
        let bd2 = 1_700_086_400_000i64;
        append(&sample(bd1)).unwrap();
        append(&sample(bd1)).unwrap();
        append(&sample(bd2)).unwrap();

        let day1 = read_day(bd1).unwrap();
        assert_eq!(day1.len(), 2);
        let day2 = read_day(bd2).unwrap();
        assert_eq!(day2.len(), 1);

        std::env::remove_var("DECISIONS_LEDGER");
    }

    #[test]
    fn missing_file_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("nonexistent.jsonl");
        std::env::set_var("DECISIONS_LEDGER", &p);
        assert!(read_day(0).unwrap().is_empty());
        std::env::remove_var("DECISIONS_LEDGER");
    }
}
