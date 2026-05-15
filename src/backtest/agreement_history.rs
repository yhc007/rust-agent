//! `agreement-history` subcommand. Dumps the pairwise agreement-rate
//! time series from `polymarket_btc.agreement_snapshots`. Pairs with
//! `pnl-history` — same `--days N` and `--strategies` knobs so a
//! cron-driven `compare-pnl` can be replayed two different ways
//! (per-strategy PnL vs. cross-strategy agreement rate).
//!
//! We only print the upper triangle of each snapshot (i.e. ordered
//! pairs where `strategy_a < strategy_b`) — the on-wire storage is
//! symmetric so reading every row would print every pair twice with
//! identical numbers.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use crate::coredb::agreement::AgreementRepo;
use crate::coredb::types::{bucket_day, now_ms, AgreementSnapshot, Millis};
use crate::coredb::CoreDb;

const DAY_MS: Millis = 86_400_000;

pub async fn run(
    coredb_uri: &str,
    strategies_filter: Option<&[String]>,
    days: u32,
) -> Result<()> {
    let days = days.max(1);
    println!("🧩 agreement-history: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let repo = AgreementRepo::new(db.session()).await?;

    let today = bucket_day(now_ms());
    let mut buckets: Vec<Millis> = Vec::with_capacity(days as usize);
    for i in 0..days as i64 {
        buckets.push(today - (days as i64 - 1 - i) * DAY_MS);
    }

    let mut rows: Vec<AgreementSnapshot> = Vec::new();
    let mut empty_days = 0u32;
    for bd in &buckets {
        match repo.list_day(*bd).await {
            Ok(v) => {
                if v.is_empty() {
                    empty_days += 1;
                } else {
                    rows.extend(v);
                }
            }
            Err(e) => {
                eprintln!("  ! list_day({bd}) failed: {e}");
                empty_days += 1;
            }
        }
    }
    if days > 1 {
        println!(
            "   spanning {} UTC days: {} → {} (today)",
            days,
            buckets.first().copied().unwrap_or(0),
            today,
        );
        if empty_days > 0 {
            println!("   {empty_days} of {days} days had no snapshots");
        }
    }

    // Filter to (a < b) ordered pairs so each pair shows up once
    // per ts. Then optionally restrict to operator-chosen
    // strategies — a row qualifies iff *both* endpoints are in the
    // allow-list so the displayed series is fully self-consistent
    // (no half-truths where one side was filtered out).
    let pre_filter = rows.len();
    rows.retain(|r| r.strategy_a < r.strategy_b);
    if let Some(allow) = strategies_filter {
        let set: std::collections::HashSet<&str> = allow.iter().map(String::as_str).collect();
        rows.retain(|r| set.contains(r.strategy_a.as_str()) && set.contains(r.strategy_b.as_str()));
        println!(
            "   filter: {} → {} rows (strategies allowed: {})",
            pre_filter / 2.max(1),
            rows.len(),
            allow.join(", "),
        );
    }
    println!("   {} snapshot rows total", rows.len());

    if rows.is_empty() {
        let hint = if strategies_filter.is_some() {
            "   (filter excluded every row — drop --strategies or check the labels.)"
        } else {
            "   (no agreement snapshots yet — run `compare-pnl` first, ideally on a cron.)"
        };
        println!("{hint}");
        return Ok(());
    }

    println!();
    let multi_day = days > 1;
    let ts_width = if multi_day { 19 } else { 10 };
    println!(
        "   {:<width$} {:<12} {:<12} {:>8} {:>8} {:>8}",
        "ts (UTC)", "strategy_a", "strategy_b", "shared", "matches", "rate",
        width = ts_width,
    );
    for r in &rows {
        let dt = DateTime::<Utc>::from_timestamp_millis(r.ts_ms)
            .map(|d| {
                if multi_day {
                    d.format("%Y-%m-%d %H:%M:%S").to_string()
                } else {
                    d.format("%H:%M:%S").to_string()
                }
            })
            .unwrap_or_else(|| r.ts_ms.to_string());
        println!(
            "   {:<width$} {:<12} {:<12} {:>8} {:>8} {:>7.1}%",
            dt,
            r.strategy_a,
            r.strategy_b,
            r.shared,
            r.matches,
            r.rate() * 100.0,
            width = ts_width,
        );
    }
    Ok(())
}
