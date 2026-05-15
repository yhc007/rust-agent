//! `pnl-history` subcommand. Reads every `strategy_pnl_snapshots` row
//! for today's UTC bucket and prints them as a time series, one
//! snapshot per row, grouped by strategy so divergence over time is
//! easy to eyeball.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::coredb::strategy_pnl::StrategyPnlRepo;
use crate::coredb::types::{bucket_day, now_ms};
use crate::coredb::CoreDb;

pub async fn run(coredb_uri: &str, strategies_filter: Option<&[String]>) -> Result<()> {
    println!("📈 pnl-history: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let repo = StrategyPnlRepo::new(db.session()).await?;

    let bd = bucket_day(now_ms());
    let mut snapshots = repo.list_day(bd).await.context("list snapshots for today")?;
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
    println!(
        "   {} snapshots for bucket_day = {} (UTC ms)",
        snapshots.len(),
        bd
    );

    if snapshots.is_empty() {
        let hint = if strategies_filter.is_some() {
            "   (filter excluded every row — drop --strategies or check the labels match what compare-pnl wrote.)"
        } else {
            "   (no snapshots yet — run `compare-pnl` first, ideally on a cron.)"
        };
        println!("{hint}");
        return Ok(());
    }

    println!();
    println!(
        "   {:<10} {:<8} {:>5} {:>5} {:>5} {:>5} {:>10} {:>10}",
        "ts (UTC)", "strategy", "n", "YES", "NO", "PASS", "Σ size", "Σ pnl"
    );
    for s in &snapshots {
        let dt = DateTime::<Utc>::from_timestamp_millis(s.ts_ms)
            .map(|d| d.format("%H:%M:%S").to_string())
            .unwrap_or_else(|| s.ts_ms.to_string());
        println!(
            "   {:<10} {:<8} {:>5} {:>5} {:>5} {:>5} {:>10} {:>10}",
            dt,
            s.strategy,
            s.n_decisions,
            s.n_yes,
            s.n_no,
            s.n_pass,
            format!("${:.2}", s.sum_size_usd),
            format!("${:+.2}", s.sum_pnl),
        );
    }

    Ok(())
}
