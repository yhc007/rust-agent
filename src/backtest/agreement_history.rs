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
use serde::Serialize;

use crate::coredb::agreement::AgreementRepo;
use crate::coredb::types::{bucket_day, now_ms, AgreementSnapshot, Millis};
use crate::coredb::CoreDb;

const DAY_MS: Millis = 86_400_000;

pub async fn run(
    coredb_uri: &str,
    strategies_filter: Option<&[String]>,
    days: u32,
    since_ms: Option<i64>,
    json: bool,
) -> Result<()> {
    let days = days.max(1);
    macro_rules! say {
        ($($t:tt)*) => { if !json { println!($($t)*); } };
    }
    say!("🧩 agreement-history: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let repo = AgreementRepo::new(db.session()).await?;

    let today = bucket_day(now_ms());
    let effective_days: u32 = match since_ms {
        Some(since) => {
            let start_bd = bucket_day(since);
            ((today - start_bd) / DAY_MS).max(0) as u32 + 1
        }
        None => days,
    };
    let mut buckets: Vec<Millis> = Vec::with_capacity(effective_days as usize);
    for i in 0..effective_days as i64 {
        buckets.push(today - (effective_days as i64 - 1 - i) * DAY_MS);
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
    // Honor --since's precise start boundary by post-filtering.
    if let Some(since) = since_ms {
        rows.retain(|r| r.ts_ms >= since);
        say!(
            "   --since {} → {} snapshots from {} bucket_day(s)",
            since,
            rows.len(),
            effective_days,
        );
    } else if effective_days > 1 {
        say!(
            "   spanning {} UTC days: {} → {} (today)",
            effective_days,
            buckets.first().copied().unwrap_or(0),
            today,
        );
        if empty_days > 0 {
            say!(
                "   {empty_days} of {effective_days} days had no snapshots",
            );
        }
    }

    // Filter to (a < b) ordered pairs so each pair shows up once
    // per ts. Then optionally restrict to operator-chosen
    // strategies — a row qualifies iff *both* endpoints are in the
    // allow-list so the displayed series is fully self-consistent
    // (no half-truths where one side was filtered out).
    let pre_filter_pairs = rows.len() / 2.max(1);
    rows.retain(|r| r.strategy_a < r.strategy_b);
    let after_triangle = rows.len();
    if let Some(allow) = strategies_filter {
        let set: std::collections::HashSet<&str> = allow.iter().map(String::as_str).collect();
        rows.retain(|r| set.contains(r.strategy_a.as_str()) && set.contains(r.strategy_b.as_str()));
        say!(
            "   filter: {} → {} rows (strategies allowed: {})",
            pre_filter_pairs,
            rows.len(),
            allow.join(", "),
        );
    }
    say!("   {} snapshot rows total", rows.len());

    // Sort once so JSON + text share ordering.
    rows.sort_by(|a, b| {
        a.ts_ms
            .cmp(&b.ts_ms)
            .then_with(|| a.strategy_a.cmp(&b.strategy_a))
            .then_with(|| a.strategy_b.cmp(&b.strategy_b))
    });

    if json {
        print_json_payload(JsonOut {
            days: effective_days,
            since_ms,
            bucket_days: buckets,
            filter: JsonFilter {
                strategies: strategies_filter.map(|s| s.to_vec()),
            },
            n_total: pre_filter_pairs,
            n_after_filter: rows.len(),
            empty_days,
            snapshots: rows.iter().map(JsonSnapshot::from).collect(),
        });
        return Ok(());
    }

    if rows.is_empty() {
        let hint = if strategies_filter.is_some() {
            "   (filter excluded every row — drop --strategies or check the labels.)"
        } else {
            "   (no agreement snapshots yet — run `compare-pnl` first, ideally on a cron.)"
        };
        println!("{hint}");
        return Ok(());
    }
    let _ = after_triangle; // surfaced via n_total above; text path doesn't need it

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

// ---- JSON output --------------------------------------------------

#[derive(Serialize)]
struct JsonOut {
    days: u32,
    /// Resolved `--since` start in ms-since-epoch, or absent when
    /// `--days` governed the window.
    #[serde(skip_serializing_if = "Option::is_none")]
    since_ms: Option<i64>,
    bucket_days: Vec<Millis>,
    filter: JsonFilter,
    /// Count of distinct (ts, pair) entries in the window *after*
    /// the upper-triangle filter (so each pair shows up once per ts)
    /// but *before* the optional `--strategies` allow-list.
    n_total: usize,
    /// Same count after the allow-list. Equal to `n_total` when no
    /// filter is in play.
    n_after_filter: usize,
    empty_days: u32,
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
    strategy_a: String,
    strategy_b: String,
    shared: i32,
    matches: i32,
    rate: f64,
}

impl From<&AgreementSnapshot> for JsonSnapshot {
    fn from(s: &AgreementSnapshot) -> Self {
        Self {
            bucket_day_ms: s.bucket_day_ms,
            ts_ms: s.ts_ms,
            strategy_a: s.strategy_a.clone(),
            strategy_b: s.strategy_b.clone(),
            shared: s.shared,
            matches: s.matches,
            rate: s.rate(),
        }
    }
}

fn print_json_payload(payload: JsonOut) {
    match serde_json::to_string_pretty(&payload) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("agreement-history: serialize JSON failed: {e}"),
    }
}
