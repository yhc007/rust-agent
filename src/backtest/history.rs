//! `pnl-history` subcommand. Reads N bucket_day partitions of
//! `strategy_pnl_snapshots` (default 1 = today only) and prints
//! them as a time-sorted series so divergence-over-time is easy
//! to eyeball.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::coredb::strategy_pnl::StrategyPnlRepo;
use crate::coredb::types::{bucket_day, now_ms, Millis, StrategyPnlSnapshot};
use crate::coredb::CoreDb;

const DAY_MS: Millis = 86_400_000;

pub async fn run(
    coredb_uri: &str,
    strategies_filter: Option<&[String]>,
    days: u32,
) -> Result<()> {
    let days = days.max(1);
    println!("📈 pnl-history: connecting to CoreDB at {coredb_uri}");
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
        println!(
            "   spanning {} UTC days ({} ms steps): {} → {} (today)",
            days,
            DAY_MS,
            buckets.first().copied().unwrap_or(0),
            today,
        );
        if empty_days > 0 {
            println!("   {empty_days} of {days} days had no snapshots");
        }
    }

    let pre_filter = snapshots.len();
    if let Some(allow) = strategies_filter {
        let set: std::collections::HashSet<&str> = allow.iter().map(String::as_str).collect();
        snapshots.retain(|s| set.contains(s.strategy.as_str()));
        println!(
            "   filter: {} → {} snapshots ({} strategies allowed: {})",
            pre_filter,
            snapshots.len(),
            allow.len(),
            allow.join(", "),
        );
    }
    println!("   {} snapshots total", snapshots.len());

    if snapshots.is_empty() {
        let hint = if strategies_filter.is_some() {
            "   (filter excluded every row — drop --strategies or check the labels match what compare-pnl wrote.)"
        } else {
            "   (no snapshots in this window — run `compare-pnl` first, ideally on a cron, or widen --days.)"
        };
        println!("{hint}");
        return Ok(());
    }

    // Sort ascending across the whole window. Within a single day the
    // CQL response order is already roughly ts-ascending, but spanning
    // multiple days requires a global sort so the trend is monotonic.
    snapshots.sort_by_key(|s| s.ts_ms);

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
