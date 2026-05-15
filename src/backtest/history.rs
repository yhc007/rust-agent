//! `pnl-history` subcommand. Reads N bucket_day partitions of
//! `strategy_pnl_snapshots` (default 1 = today only) and prints
//! them as a time-sorted series so divergence-over-time is easy
//! to eyeball.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::coredb::strategy_pnl::StrategyPnlRepo;
use crate::coredb::types::{bucket_day, now_ms, Millis, StrategyPnlSnapshot};
use crate::coredb::CoreDb;

const DAY_MS: Millis = 86_400_000;

pub async fn run(
    coredb_uri: &str,
    strategies_filter: Option<&[String]>,
    days: u32,
    json: bool,
) -> Result<()> {
    let days = days.max(1);
    macro_rules! say {
        ($($t:tt)*) => { if !json { println!($($t)*); } };
    }
    say!("📈 pnl-history: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let repo = StrategyPnlRepo::new(db.session()).await?;

    // Compute the list of UTC-day buckets, oldest first. For days=1
    // this collapses to "[today]" so the historical single-day call
    // path is unchanged.
    let today = bucket_day(now_ms());
    let mut buckets: Vec<Millis> = Vec::with_capacity(days as usize);
    for i in 0..days as i64 {
        buckets.push(today - (days as i64 - 1 - i) * DAY_MS);
    }

    let mut snapshots: Vec<StrategyPnlSnapshot> = Vec::new();
    let mut empty_days = 0u32;
    for bd in &buckets {
        match repo.list_day(*bd).await {
            Ok(rows) => {
                if rows.is_empty() {
                    empty_days += 1;
                } else {
                    snapshots.extend(rows);
                }
            }
            Err(e) => {
                // Don't fail the whole window on one bad partition —
                // a corrupt or missing day shouldn't hide N-1 good
                // ones. Surface the failure inline so the operator
                // sees it next to the rest of the output.
                eprintln!("  ! list_day({bd}) failed: {e}");
                empty_days += 1;
            }
        }
    }
    if days > 1 {
        say!(
            "   spanning {} UTC days ({} ms steps): {} → {} (today)",
            days,
            DAY_MS,
            buckets.first().copied().unwrap_or(0),
            today,
        );
        if empty_days > 0 {
            say!("   {empty_days} of {days} days had no snapshots");
        }
    }

    let pre_filter = snapshots.len();
    if let Some(allow) = strategies_filter {
        let set: std::collections::HashSet<&str> = allow.iter().map(String::as_str).collect();
        snapshots.retain(|s| set.contains(s.strategy.as_str()));
        say!(
            "   filter: {} → {} snapshots ({} strategies allowed: {})",
            pre_filter,
            snapshots.len(),
            allow.len(),
            allow.join(", "),
        );
    }
    say!("   {} snapshots total", snapshots.len());

    // Always sort ascending so the JSON path and the text path
    // share identical ordering — JSON consumers can rely on the
    // series being chronological without re-sorting.
    snapshots.sort_by_key(|s| s.ts_ms);

    if json {
        print_json_payload(JsonOut {
            days,
            bucket_days: buckets,
            filter: JsonFilter {
                strategies: strategies_filter.map(|s| s.to_vec()),
            },
            n_total: pre_filter,
            n_after_filter: snapshots.len(),
            empty_days,
            snapshots: snapshots.iter().map(JsonSnapshot::from).collect(),
        });
        return Ok(());
    }

    if snapshots.is_empty() {
        let hint = if strategies_filter.is_some() {
            "   (filter excluded every row — drop --strategies or check the labels match what compare-pnl wrote.)"
        } else {
            "   (no snapshots in this window — run `compare-pnl` first, ideally on a cron, or widen --days.)"
        };
        println!("{hint}");
        return Ok(());
    }

    // (Sort already happened above so the JSON and text paths share
    // ordering — see the `--json` branch.)

    println!();
    // Wider timestamp column when spanning multiple days — fall back
    // to the compact HH:MM:SS form when there's only one day to keep
    // the single-day output unchanged.
    let multi_day = days > 1;
    let ts_header = if multi_day { "ts (UTC)" } else { "ts (UTC)" };
    let ts_width = if multi_day { 19 } else { 10 };
    println!(
        "   {:<width$} {:<8} {:>5} {:>5} {:>5} {:>5} {:>10} {:>10}",
        ts_header, "strategy", "n", "YES", "NO", "PASS", "Σ size", "Σ pnl",
        width = ts_width,
    );
    for s in &snapshots {
        let dt = DateTime::<Utc>::from_timestamp_millis(s.ts_ms)
            .map(|d| {
                if multi_day {
                    d.format("%Y-%m-%d %H:%M:%S").to_string()
                } else {
                    d.format("%H:%M:%S").to_string()
                }
            })
            .unwrap_or_else(|| s.ts_ms.to_string());
        println!(
            "   {:<width$} {:<8} {:>5} {:>5} {:>5} {:>5} {:>10} {:>10}",
            dt,
            s.strategy,
            s.n_decisions,
            s.n_yes,
            s.n_no,
            s.n_pass,
            format!("${:.2}", s.sum_size_usd),
            format!("${:+.2}", s.sum_pnl),
            width = ts_width,
        );
    }

    Ok(())
}

// ---- JSON output --------------------------------------------------

#[derive(Serialize)]
struct JsonOut {
    days: u32,
    /// Each bucket_day_ms in the window, oldest first. `len()` ==
    /// `days` even when individual partitions came back empty so a
    /// downstream consumer can spot gaps.
    bucket_days: Vec<Millis>,
    filter: JsonFilter,
    /// Snapshot count across the window before `--strategies` filter.
    n_total: usize,
    /// Snapshot count after filter. Equal to `n_total` when no
    /// filter was passed.
    n_after_filter: usize,
    /// Number of `bucket_days` that returned zero rows. Useful for
    /// "is my cron actually running?" sanity checks.
    empty_days: u32,
    /// Time-series rows, ts-ascending. Empty when no data fell into
    /// the chosen window — the wrapper still emits with all the
    /// metadata so downstream scripts don't have to special-case.
    snapshots: Vec<JsonSnapshot>,
}

#[derive(Serialize)]
struct JsonFilter {
    strategies: Option<Vec<String>>,
}

#[derive(Serialize)]
struct JsonSnapshot {
    bucket_day_ms: Millis,
    ts_ms: Millis,
    strategy: String,
    n_decisions: i32,
    n_yes: i32,
    n_no: i32,
    n_pass: i32,
    sum_size_usd: f64,
    sum_pnl: f64,
}

impl From<&StrategyPnlSnapshot> for JsonSnapshot {
    fn from(s: &StrategyPnlSnapshot) -> Self {
        Self {
            bucket_day_ms: s.bucket_day_ms,
            ts_ms: s.ts_ms,
            strategy: s.strategy.clone(),
            n_decisions: s.n_decisions,
            n_yes: s.n_yes,
            n_no: s.n_no,
            n_pass: s.n_pass,
            sum_size_usd: s.sum_size_usd,
            sum_pnl: s.sum_pnl,
        }
    }
}

fn print_json_payload(payload: JsonOut) {
    match serde_json::to_string_pretty(&payload) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("pnl-history: serialize JSON failed: {e}"),
    }
}
