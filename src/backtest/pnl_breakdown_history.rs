//! `pnl-breakdown-history` subcommand. Dumps the per-(strategy,
//! exec) realized PnL series from `polymarket_btc.pnl_breakdown`.
//! Mirrors `pnl-history` / `agreement-history` so operators have a
//! CLI view of the same data the Grafana
//! `agent_pnl_breakdown_realized_usd` panels see.
//!
//! Filters compose:
//!   --days N           widen the window beyond today
//!   --strategies a,b   restrict to a subset of strategy labels
//!   --execs paper,live restrict to paper-only or live-only rows
//!   --json             machine-readable single-object dump

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::coredb::pnl_breakdown::PnlBreakdownRepo;
use crate::coredb::types::{bucket_day, now_ms, Millis, PnlBreakdown};
use crate::coredb::CoreDb;

const DAY_MS: Millis = 86_400_000;

pub async fn run(
    coredb_uri: &str,
    strategies_filter: Option<&[String]>,
    execs_filter: Option<&[String]>,
    days: u32,
    json: bool,
) -> Result<()> {
    let days = days.max(1);
    macro_rules! say {
        ($($t:tt)*) => { if !json { println!($($t)*); } };
    }
    say!("💰 pnl-breakdown-history: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let repo = PnlBreakdownRepo::new(db.session()).await?;

    let today = bucket_day(now_ms());
    let mut buckets: Vec<Millis> = Vec::with_capacity(days as usize);
    for i in 0..days as i64 {
        buckets.push(today - (days as i64 - 1 - i) * DAY_MS);
    }

    let mut rows: Vec<PnlBreakdown> = Vec::new();
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
        say!(
            "   spanning {} UTC days: {} → {} (today)",
            days,
            buckets.first().copied().unwrap_or(0),
            today,
        );
        if empty_days > 0 {
            say!("   {empty_days} of {days} days had no rows");
        }
    }

    let pre_filter = rows.len();
    if let Some(allow) = strategies_filter {
        let set: std::collections::HashSet<&str> = allow.iter().map(String::as_str).collect();
        rows.retain(|r| set.contains(r.strategy.as_str()));
        say!(
            "   --strategies filter: {} → {} rows (allowed: {})",
            pre_filter,
            rows.len(),
            allow.join(", "),
        );
    }
    let pre_exec_filter = rows.len();
    if let Some(allow) = execs_filter {
        let set: std::collections::HashSet<&str> = allow.iter().map(String::as_str).collect();
        rows.retain(|r| set.contains(r.exec.as_str()));
        say!(
            "   --execs filter:      {} → {} rows (allowed: {})",
            pre_exec_filter,
            rows.len(),
            allow.join(", "),
        );
    }
    say!("   {} rows total", rows.len());

    // Sort by (bucket_day, strategy, exec) so the JSON and text
    // outputs share ordering.
    rows.sort_by(|a, b| {
        a.bucket_day_ms
            .cmp(&b.bucket_day_ms)
            .then_with(|| a.strategy.cmp(&b.strategy))
            .then_with(|| a.exec.cmp(&b.exec))
    });

    if json {
        print_json_payload(JsonOut {
            days,
            bucket_days: buckets,
            filter: JsonFilter {
                strategies: strategies_filter.map(|s| s.to_vec()),
                execs: execs_filter.map(|s| s.to_vec()),
            },
            n_total: pre_filter,
            n_after_filter: rows.len(),
            empty_days,
            breakdowns: rows.iter().map(JsonBreakdown::from).collect(),
        });
        return Ok(());
    }

    if rows.is_empty() {
        let hint = if strategies_filter.is_some() || execs_filter.is_some() {
            "   (filter excluded every row — drop --strategies / --execs or check the labels.)"
        } else {
            "   (no breakdowns yet — run `settle-pnl` after markets resolve.)"
        };
        println!("{hint}");
        return Ok(());
    }

    println!();
    let multi_day = days > 1;
    let date_width = if multi_day { 10 } else { 0 };
    if multi_day {
        println!(
            "   {:<width$} {:<10} {:<6} {:>10} {:>10}",
            "date (UTC)",
            "strategy",
            "exec",
            "settled",
            "realized $",
            width = date_width,
        );
    } else {
        println!(
            "   {:<10} {:<6} {:>10} {:>10}",
            "strategy", "exec", "settled", "realized $"
        );
    }
    for r in &rows {
        let date = DateTime::<Utc>::from_timestamp_millis(r.bucket_day_ms)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| r.bucket_day_ms.to_string());
        if multi_day {
            println!(
                "   {:<width$} {:<10} {:<6} {:>10} {:>10}",
                date,
                r.strategy,
                r.exec,
                r.n_settled,
                format!("${:+.2}", r.realized_pnl),
                width = date_width,
            );
        } else {
            println!(
                "   {:<10} {:<6} {:>10} {:>10}",
                r.strategy,
                r.exec,
                r.n_settled,
                format!("${:+.2}", r.realized_pnl),
            );
        }
    }
    Ok(())
}

// ---- JSON output --------------------------------------------------

#[derive(Serialize)]
struct JsonOut {
    days: u32,
    bucket_days: Vec<Millis>,
    filter: JsonFilter,
    n_total: usize,
    n_after_filter: usize,
    empty_days: u32,
    breakdowns: Vec<JsonBreakdown>,
}

#[derive(Serialize)]
struct JsonFilter {
    strategies: Option<Vec<String>>,
    execs: Option<Vec<String>>,
}

#[derive(Serialize)]
struct JsonBreakdown {
    bucket_day_ms: Millis,
    strategy: String,
    exec: String,
    realized_pnl: f64,
    n_settled: i32,
}

impl From<&PnlBreakdown> for JsonBreakdown {
    fn from(b: &PnlBreakdown) -> Self {
        Self {
            bucket_day_ms: b.bucket_day_ms,
            strategy: b.strategy.clone(),
            exec: b.exec.clone(),
            realized_pnl: b.realized_pnl,
            n_settled: b.n_settled,
        }
    }
}

fn print_json_payload(payload: JsonOut) {
    match serde_json::to_string_pretty(&payload) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("pnl-breakdown-history: serialize JSON failed: {e}"),
    }
}
