//! Realized PnL roll-up. Reads every Order written for one UTC day,
//! looks up its market in Polymarket Gamma's resolved-markets feed,
//! and pays out the order at $1/share if its side won, $0 if it lost.
//! Aggregates by strategy (joined on `Decision.raw_response` via
//! `decision_id`), prints a summary, and upserts the day's total
//! realized PnL into `polymarket_btc.pnl_daily`.
//!
//! Math (matches Polymarket's outcome-share economics):
//! - `side` won  → pnl = fill_size * (1 - fill_price)
//! - `side` lost → pnl = -fill_size * fill_price
//!
//! That symmetric form works for both YES and NO because `route_decision`
//! already filled NO orders at `1 - entry_price`, so the YES/NO share
//! count and the cost-basis are already in the right currency. We don't
//! need to special-case the side here.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Serialize;
use uuid::Uuid;

use crate::coredb::decisions::DecisionRepo;
use crate::coredb::orders::OrderRepo;
use crate::coredb::pnl::PnlRepo;
use crate::coredb::pnl_breakdown::PnlBreakdownRepo;
use crate::coredb::types::{bucket_day, now_ms, Millis, PnlBreakdown, PnlDaily};
use crate::coredb::CoreDb;

const DAY_MS: Millis = 86_400_000;

/// Multi-day settle plan. Phase 5 added the window dimension so
/// paper positions written on past days (whose markets have since
/// resolved on Polymarket) actually get settled instead of being
/// permanently stranded in the "today-only" bucket the original
/// flow couldn't reach.
///
/// `days = 1` + `since_ms = None` keeps the historical default
/// (today only), so existing callers — including the daemon's
/// periodic settle tick — are byte-for-byte compatible.
pub struct SettlePlan {
    pub days: u32,
    pub since_ms: Option<i64>,
    pub json: bool,
}

impl SettlePlan {
    pub fn today_only(json: bool) -> Self {
        Self { days: 1, since_ms: None, json }
    }
}

/// Backwards-compatible single-day entry point. Existing callers
/// (the daemon, the standalone `rust-agent settle-pnl` without
/// window flags) keep working unchanged.
pub async fn run(coredb_uri: &str, json: bool) -> Result<()> {
    run_with_plan(coredb_uri, SettlePlan::today_only(json)).await
}

/// Window-aware settle pass. Iterates every bucket_day in the
/// resolved window, settles each day's orders independently
/// (so pnl_daily / pnl_breakdown stay correctly keyed by their
/// own day_ms), and prints both a per-day and a grand-total
/// summary. The resolved-markets feed from Polymarket Gamma is
/// fetched once and shared across days — Gamma returns every
/// recently-closed market regardless of which day it resolved on,
/// so per-day re-fetch would be redundant.
pub async fn run_with_plan(coredb_uri: &str, plan: SettlePlan) -> Result<()> {
    let SettlePlan { days, since_ms, json } = plan;
    macro_rules! say {
        ($($t:tt)*) => { if !json { println!($($t)*); } };
    }
    say!("⚖️  settle-pnl: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let order_repo = OrderRepo::new(db.session()).await?;
    let dec_repo = DecisionRepo::new(db.session()).await?;
    let pnl_repo = PnlRepo::new(db.session()).await?;
    let breakdown_repo = PnlBreakdownRepo::new(db.session()).await?;

    // --since takes precedence; otherwise --days. Same shape as
    // compare-pnl / digest so an operator fluent in one is fluent
    // in all three.
    let today = bucket_day(now_ms());
    let effective_days: u32 = match since_ms {
        Some(since) => {
            let start_bd = bucket_day(since);
            ((today - start_bd) / DAY_MS).max(0) as u32 + 1
        }
        None => days.max(1),
    };
    let mut bucket_days: Vec<Millis> = Vec::with_capacity(effective_days as usize);
    for i in 0..effective_days as i64 {
        bucket_days.push(today - (effective_days as i64 - 1 - i) * DAY_MS);
    }
    say!(
        "   window: {} bucket_day(s), {} → {} (today)",
        effective_days, bucket_days[0], today,
    );

    let http = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("reqwest client")?;
    say!("🌐 settle-pnl: pulling resolved markets from Polymarket Gamma");
    let resolved = fetch_resolved_markets(&http).await?;
    say!("   {} resolved markets visible", resolved.len());

    // Grand totals across the entire window. Per-day numbers stay
    // in `day_summaries` for the operator-facing breakdown.
    let mut grand_realized = 0.0f64;
    let mut grand_settled = 0i32;
    let mut grand_orders = 0usize;
    let mut grand_unresolved = 0u32;
    let mut grand_by_strategy: HashMap<String, StrategyTotal> = HashMap::new();
    let mut day_summaries: Vec<DaySummary> = Vec::with_capacity(effective_days as usize);

    for &bd in &bucket_days {
        let orders = match order_repo.list_day(bd).await {
            Ok(o) => o,
            Err(e) => {
                eprintln!("  ! settle-pnl: orders.list_day({bd}) failed: {e}");
                continue;
            }
        };
        let decisions = match dec_repo.list_day(bd).await {
            Ok(d) => d,
            Err(e) => {
                eprintln!("  ! settle-pnl: decisions.list_day({bd}) failed: {e}");
                continue;
            }
        };
        let strategy_of: HashMap<Uuid, String> = decisions
            .iter()
            .map(|d| (d.decision_id, d.effective_strategy().to_string()))
            .collect();

        let day = settle_one_day(bd, &orders, &strategy_of, &resolved);

        // Per-strategy roll-up into the grand total (across days).
        for (strategy, t) in &day.by_strategy {
            let agg = grand_by_strategy.entry(strategy.clone()).or_default();
            agg.n_orders += t.n_orders;
            agg.n_settled += t.n_settled;
            agg.n_unresolved += t.n_unresolved;
            agg.realized_pnl += t.realized_pnl;
        }
        grand_realized += day.realized_total;
        grand_settled += day.n_settled;
        grand_orders += day.n_orders;
        grand_unresolved += day.n_unresolved;

        // Write pnl_daily + pnl_breakdown rows for this day if
        // anything settled. We persist per-day even in a multi-day
        // run so the daily-resolution time series stays correct;
        // pnl_breakdown is keyed by (bucket_day, strategy, exec)
        // and pnl_daily by day_ms — neither expects aggregation
        // across days.
        if day.n_settled > 0 {
            let row = PnlDaily {
                day_ms: bd,
                realized: day.realized_total,
                unrealized: 0.0,
                n_trades: day.n_settled,
                llm_cost_usd: 0.0,
            };
            if let Err(e) = pnl_repo.upsert(&row).await {
                eprintln!("  ! pnl_daily.upsert({bd}) failed: {e}");
            }
            let mut breakdown_keys: Vec<&(String, String)> =
                day.by_strategy_exec.keys().collect();
            breakdown_keys.sort();
            for key in breakdown_keys {
                let t = &day.by_strategy_exec[key];
                if t.n_settled == 0 {
                    continue;
                }
                let b = PnlBreakdown {
                    bucket_day_ms: bd,
                    strategy: key.0.clone(),
                    exec: key.1.clone(),
                    realized_pnl: t.realized_pnl,
                    n_settled: t.n_settled as i32,
                };
                if let Err(e) = breakdown_repo.upsert(&b).await {
                    eprintln!(
                        "  ! pnl_breakdown upsert failed for ({}, {}, {}): {e}",
                        bd, b.strategy, b.exec,
                    );
                }
            }
        }
        day_summaries.push(day);
    }

    // Print per-day + grand-total summary.
    if !json {
        for ds in &day_summaries {
            println!("\n⚖️  Realized PnL — bucket_day = {} (UTC ms)", ds.bucket_day_ms);
            println!(
                "   {:<10} {:>10} {:>10} {:>11} {:>12}",
                "strategy", "n_orders", "settled", "unresolved", "realized $"
            );
            let mut keys: Vec<&String> = ds.by_strategy.keys().collect();
            keys.sort();
            for k in &keys {
                let t = &ds.by_strategy[*k];
                println!(
                    "   {:<10} {:>10} {:>10} {:>11} {:>12}",
                    k, t.n_orders, t.n_settled, t.n_unresolved,
                    format!("${:+.2}", t.realized_pnl),
                );
            }
            println!(
                "   {:<10} {:>10} {:>10} {:>11} {:>12}",
                "TOTAL", ds.n_orders, ds.n_settled, ds.n_unresolved,
                format!("${:+.2}", ds.realized_total),
            );
        }
        if effective_days > 1 {
            println!("\n📈 Window total ({} days):", effective_days);
            println!(
                "   {:<10} {:>10} {:>10} {:>11} {:>12}",
                "strategy", "n_orders", "settled", "unresolved", "realized $"
            );
            let mut gkeys: Vec<&String> = grand_by_strategy.keys().collect();
            gkeys.sort();
            for k in &gkeys {
                let t = &grand_by_strategy[*k];
                println!(
                    "   {:<10} {:>10} {:>10} {:>11} {:>12}",
                    k, t.n_orders, t.n_settled, t.n_unresolved,
                    format!("${:+.2}", t.realized_pnl),
                );
            }
            println!(
                "   {:<10} {:>10} {:>10} {:>11} {:>12}",
                "TOTAL", grand_orders, grand_settled, grand_unresolved,
                format!("${:+.2}", grand_realized),
            );
        }
    }

    if grand_settled == 0 {
        if json {
            print_json_payload(build_json_payload_multi(
                &bucket_days, &day_summaries,
                grand_orders, grand_settled, grand_unresolved, grand_realized,
                &grand_by_strategy, /* pnl_daily_written = */ false,
            ));
            return Ok(());
        }
        println!(
            "\n   (no orders settled in window — markets still open or none decisive yet.)"
        );
        return Ok(());
    }

    if json {
        print_json_payload(build_json_payload_multi(
            &bucket_days, &day_summaries,
            grand_orders, grand_settled, grand_unresolved, grand_realized,
            &grand_by_strategy, /* pnl_daily_written = */ true,
        ));
    }
    Ok(())
}

/// Per-day settlement result. Used to keep multi-day aggregation
/// readable + to drive the per-day section of the operator output.
struct DaySummary {
    bucket_day_ms: i64,
    n_orders: usize,
    n_settled: i32,
    n_unresolved: u32,
    realized_total: f64,
    by_strategy: HashMap<String, StrategyTotal>,
    by_strategy_exec: HashMap<(String, String), StrategyTotal>,
}

/// Pure settlement function: given one day's orders + the resolved-
/// markets map, produce the per-strategy + per-(strategy, exec)
/// roll-up. No I/O — the caller writes pnl_daily / pnl_breakdown.
fn settle_one_day(
    bucket_day_ms: i64,
    orders: &[crate::coredb::types::Order],
    strategy_of: &HashMap<Uuid, String>,
    resolved: &HashMap<String, bool>,
) -> DaySummary {
    let mut by_strategy: HashMap<String, StrategyTotal> = HashMap::new();
    let mut by_strategy_exec: HashMap<(String, String), StrategyTotal> = HashMap::new();
    let mut realized_total = 0.0;
    let mut n_settled = 0i32;
    let mut n_unresolved = 0u32;

    for o in orders {
        let strategy = strategy_of
            .get(&o.decision_id)
            .cloned()
            .unwrap_or_else(|| "other".to_string());
        let exec = if o.order_id.starts_with("paper-") {
            "paper".to_string()
        } else {
            "live".to_string()
        };
        let agg = by_strategy.entry(strategy.clone()).or_default();
        agg.n_orders += 1;
        let agg_be = by_strategy_exec.entry((strategy, exec)).or_default();
        agg_be.n_orders += 1;

        match resolved.get(&o.market_slug) {
            Some(&yes_won) => {
                let side_won = (o.side == "YES" && yes_won) || (o.side == "NO" && !yes_won);
                let pnl = if side_won {
                    o.fill_size * (1.0 - o.fill_price)
                } else {
                    -o.fill_size * o.fill_price
                };
                agg.realized_pnl += pnl;
                agg.n_settled += 1;
                agg_be.realized_pnl += pnl;
                agg_be.n_settled += 1;
                realized_total += pnl;
                n_settled += 1;
            }
            None => {
                agg.n_unresolved += 1;
                agg_be.n_unresolved += 1;
                n_unresolved += 1;
            }
        }
    }

    DaySummary {
        bucket_day_ms,
        n_orders: orders.len(),
        n_settled,
        n_unresolved,
        realized_total,
        by_strategy,
        by_strategy_exec,
    }
}

#[derive(Default, Debug)]
struct StrategyTotal {
    n_orders: u32,
    n_settled: u32,
    n_unresolved: u32,
    realized_pnl: f64,
}

// ---- JSON output --------------------------------------------------

#[derive(Serialize)]
struct JsonOut {
    /// Latest day in the settled window. Kept for backward
    /// compatibility with single-day callers that grep this field;
    /// the full list of days is in `bucket_days` below.
    bucket_day_ms: i64,
    /// Every bucket_day processed, oldest first. `len() ==
    /// days_spanned`. Single-day calls produce a 1-element array
    /// so downstream tooling can treat the field uniformly.
    bucket_days: Vec<i64>,
    days_spanned: u32,
    /// Grand-total order count across the window.
    n_orders: usize,
    /// Grand-total settled (resolved-and-paid) order count.
    n_settled: i32,
    /// Grand-total still-open order count.
    n_unresolved: u32,
    /// Grand-total realized PnL ($) across every settled order
    /// in the window.
    realized_pnl_total: f64,
    /// Per-strategy roll-up aggregated across the window.
    strategies: Vec<JsonStrategyAggregate>,
    /// Per-day breakdown. Useful for charting daily realized PnL
    /// without recomputing from pnl_daily; mirrors what gets
    /// written to that table in the same loop.
    per_day: Vec<JsonDaySummary>,
    /// True iff at least one bucket_day got a pnl_daily row
    /// written. Same "anything actually settled" structural
    /// signal callers used in single-day mode.
    pnl_daily_written: bool,
}

#[derive(Serialize)]
struct JsonStrategyAggregate {
    strategy: String,
    n_orders: u32,
    n_settled: u32,
    n_unresolved: u32,
    realized_pnl: f64,
}

#[derive(Serialize)]
struct JsonDaySummary {
    bucket_day_ms: i64,
    n_orders: usize,
    n_settled: i32,
    n_unresolved: u32,
    realized_pnl: f64,
}

fn build_json_payload_multi(
    bucket_days: &[Millis],
    day_summaries: &[DaySummary],
    grand_orders: usize,
    grand_settled: i32,
    grand_unresolved: u32,
    grand_realized: f64,
    grand_by_strategy: &HashMap<String, StrategyTotal>,
    pnl_daily_written: bool,
) -> JsonOut {
    let mut keys: Vec<&String> = grand_by_strategy.keys().collect();
    keys.sort();
    JsonOut {
        bucket_day_ms: *bucket_days.last().unwrap_or(&0),
        bucket_days: bucket_days.to_vec(),
        days_spanned: bucket_days.len() as u32,
        n_orders: grand_orders,
        n_settled: grand_settled,
        n_unresolved: grand_unresolved,
        realized_pnl_total: grand_realized,
        strategies: keys
            .iter()
            .map(|k| JsonStrategyAggregate {
                strategy: (*k).clone(),
                n_orders: grand_by_strategy[*k].n_orders,
                n_settled: grand_by_strategy[*k].n_settled,
                n_unresolved: grand_by_strategy[*k].n_unresolved,
                realized_pnl: grand_by_strategy[*k].realized_pnl,
            })
            .collect(),
        per_day: day_summaries
            .iter()
            .map(|d| JsonDaySummary {
                bucket_day_ms: d.bucket_day_ms,
                n_orders: d.n_orders,
                n_settled: d.n_settled,
                n_unresolved: d.n_unresolved,
                realized_pnl: d.realized_total,
            })
            .collect(),
        pnl_daily_written,
    }
}

fn print_json_payload(payload: JsonOut) {
    match serde_json::to_string_pretty(&payload) {
        Ok(s) => println!("{s}"),
        Err(e) => eprintln!("settle-pnl: serialize JSON failed: {e}"),
    }
}

/// Pull every recently-closed market from Polymarket Gamma. Returns a
/// `slug → yes_won` map for markets whose outcome is decisive
/// (`outcomePrices` is `["1","0"]` or `["0","1"]`). Refunded / no-
/// consensus markets (anything not converged to a clean 0/1) are
/// dropped: we'd rather count them as unresolved than silently book
/// them as a loss / win on either side.
async fn fetch_resolved_markets(http: &Client) -> Result<HashMap<String, bool>> {
    // `closed=true` is what Gamma surfaces post-resolution. We don't add
    // `active=*` because resolved markets aren't active by definition,
    // and we lift the limit so we catch every recently-settled market a
    // single page covers (Polymarket pages from newest backwards).
    const URL: &str = "https://gamma-api.polymarket.com/markets?closed=true&limit=500";
    let rows: Vec<serde_json::Value> = http
        .get(URL)
        .send()
        .await
        .context("gamma HTTP")?
        .error_for_status()
        .context("gamma status")?
        .json()
        .await
        .context("gamma JSON")?;
    let mut out = HashMap::new();
    for v in rows {
        let slug = match v.get("slug").and_then(|x| x.as_str()) {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };
        let prices = v
            .get("outcomePrices")
            .and_then(|x| x.as_str())
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .unwrap_or_default();
        if prices.len() < 2 {
            continue;
        }
        let yes: f64 = prices[0].parse().unwrap_or(0.0);
        let no: f64 = prices[1].parse().unwrap_or(0.0);
        if yes >= 0.99 {
            out.insert(slug.to_string(), true);
        } else if no >= 0.99 {
            out.insert(slug.to_string(), false);
        }
        // else: refunded / ambiguous; treat as unresolved.
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coredb::types::Order;

    fn order(side: &str, fill_size: f64, fill_price: f64) -> Order {
        Order {
            bucket_day_ms: 0,
            ts_ms: 0,
            order_id: "test".into(),
            decision_id: Uuid::nil(),
            market_slug: "m".into(),
            side: side.into(),
            size: fill_size,
            price: fill_price,
            status: "filled".into(),
            fill_size,
            fill_price,
        }
    }

    fn pnl(o: &Order, yes_won: bool) -> f64 {
        let side_won = (o.side == "YES" && yes_won) || (o.side == "NO" && !yes_won);
        if side_won {
            o.fill_size * (1.0 - o.fill_price)
        } else {
            -o.fill_size * o.fill_price
        }
    }

    #[test]
    fn yes_wins_when_yes_outcome() {
        // Bought 10 shares of YES at 0.50 ($5 cost). YES wins → 10 - 5 = +$5.
        let o = order("YES", 10.0, 0.5);
        assert!((pnl(&o, true) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn yes_loses_when_no_outcome() {
        // Same buy, NO wins → -$5 cost.
        let o = order("YES", 10.0, 0.5);
        assert!((pnl(&o, false) + 5.0).abs() < 1e-9);
    }

    #[test]
    fn no_wins_when_no_outcome() {
        // Bought 10 NO shares at 0.30 ($3 cost). NO wins → 10 - 3 = +$7.
        let o = order("NO", 10.0, 0.3);
        assert!((pnl(&o, false) - 7.0).abs() < 1e-9);
    }

    #[test]
    fn no_loses_when_yes_outcome() {
        let o = order("NO", 10.0, 0.3);
        assert!((pnl(&o, true) + 3.0).abs() < 1e-9);
    }

    #[test]
    fn break_even_at_one_half() {
        // Bought 2 YES shares at 0.50 ($1 cost). YES wins → 2 - 1 = +1, lose → -1.
        let o = order("YES", 2.0, 0.5);
        assert!((pnl(&o, true) - 1.0).abs() < 1e-9);
        assert!((pnl(&o, false) + 1.0).abs() < 1e-9);
    }
}
