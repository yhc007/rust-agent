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
use crate::coredb::types::{bucket_day, now_ms, PnlDaily};
use crate::coredb::CoreDb;

pub async fn run(coredb_uri: &str, json: bool) -> Result<()> {
    macro_rules! say {
        ($($t:tt)*) => { if !json { println!($($t)*); } };
    }
    say!("⚖️  settle-pnl: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let order_repo = OrderRepo::new(db.session()).await?;
    let dec_repo = DecisionRepo::new(db.session()).await?;
    let pnl_repo = PnlRepo::new(db.session()).await?;

    let bd = bucket_day(now_ms());
    let orders = order_repo.list_day(bd).await.context("read orders")?;
    say!("   {} orders for bucket_day = {} (UTC ms)", orders.len(), bd);
    if orders.is_empty() {
        if json {
            // Stable shape even when there's nothing to settle —
            // mirrors the empty-input behavior of compare-pnl --json.
            print_json_payload(JsonOut {
                bucket_day_ms: bd,
                n_orders: 0,
                n_settled: 0,
                n_unresolved: 0,
                realized_pnl_total: 0.0,
                strategies: Vec::new(),
                pnl_daily_written: false,
            });
            return Ok(());
        }
        println!(
            "   (no orders to settle — run `backtest --execute` first to populate.)"
        );
        return Ok(());
    }

    // Map each Order back to its Decision via decision_id so we know which
    // strategy emitted it. Pre-schema or tool-driven decisions land in
    // "other" — separate from baseline/llm — so they don't silently
    // inflate one bucket.
    let decisions = dec_repo.list_day(bd).await.context("read decisions")?;
    let strategy_of: HashMap<Uuid, String> = decisions
        .iter()
        .map(|d| (d.decision_id, d.effective_strategy().to_string()))
        .collect();

    let http = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("reqwest client")?;
    say!("🌐 settle-pnl: pulling resolved markets from Polymarket Gamma");
    let resolved = fetch_resolved_markets(&http).await?;
    say!("   {} resolved markets visible", resolved.len());

    let mut by_strategy: HashMap<String, StrategyTotal> = HashMap::new();
    let mut realized_total = 0.0;
    let mut n_settled = 0i32;
    let mut n_unresolved = 0u32;
    for o in &orders {
        let strategy = strategy_of
            .get(&o.decision_id)
            .cloned()
            .unwrap_or_else(|| "other".to_string());
        let agg = by_strategy.entry(strategy).or_default();
        agg.n_orders += 1;

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
                realized_total += pnl;
                n_settled += 1;
            }
            None => {
                agg.n_unresolved += 1;
                n_unresolved += 1;
            }
        }
    }

    let mut keys: Vec<_> = by_strategy.keys().cloned().collect();
    keys.sort();
    if !json {
        println!("\n⚖️  Realized PnL — bucket_day = {bd} (UTC ms)");
        println!(
            "   {:<10} {:>10} {:>10} {:>11} {:>12}",
            "strategy", "n_orders", "settled", "unresolved", "realized $"
        );
        for k in &keys {
            let t = &by_strategy[k];
            println!(
                "   {:<10} {:>10} {:>10} {:>11} {:>12}",
                k,
                t.n_orders,
                t.n_settled,
                t.n_unresolved,
                format!("${:+.2}", t.realized_pnl)
            );
        }
        println!(
            "   {:<10} {:>10} {:>10} {:>11} {:>12}",
            "TOTAL",
            orders.len(),
            n_settled,
            n_unresolved,
            format!("${:+.2}", realized_total)
        );
    }

    if n_settled == 0 {
        if json {
            // Emit the payload even with zero settled rows so the
            // operator's `jq` chain doesn't have to special-case
            // "markets still open" — `pnl_daily_written: false`
            // is the structural signal.
            print_json_payload(build_json_payload(
                bd,
                &orders,
                n_settled,
                n_unresolved,
                realized_total,
                &keys,
                &by_strategy,
                /* pnl_daily_written = */ false,
            ));
            return Ok(());
        }
        println!(
            "\n   (no orders settled yet — markets are still open. Re-run after resolution.)"
        );
        return Ok(());
    }

    let row = PnlDaily {
        day_ms: bd,
        realized: realized_total,
        unrealized: 0.0,        // separate concern; populated by compare-pnl-flavoured rollups
        n_trades: n_settled,
        llm_cost_usd: 0.0,      // not tracked yet
    };
    pnl_repo
        .upsert(&row)
        .await
        .context("pnl_daily.upsert")?;
    say!(
        "\n   ✓ upserted polymarket_btc.pnl_daily for day {bd}: realized={:+.2}, n_trades={}",
        realized_total, n_settled
    );

    if json {
        print_json_payload(build_json_payload(
            bd,
            &orders,
            n_settled,
            n_unresolved,
            realized_total,
            &keys,
            &by_strategy,
            /* pnl_daily_written = */ true,
        ));
    }
    Ok(())
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
    bucket_day_ms: i64,
    /// Total orders read from the bucket_day partition.
    n_orders: usize,
    /// Subset whose market was resolved at scrape time.
    n_settled: i32,
    /// Subset whose market is still open (unresolved). Sum of
    /// `n_settled + n_unresolved` is `n_orders` minus any orders
    /// that didn't appear in the resolved feed but also weren't
    /// counted as unresolved (currently none — every order goes
    /// into one bucket).
    n_unresolved: u32,
    realized_pnl_total: f64,
    /// Per-strategy roll-up, sorted by strategy name for stable JSON.
    strategies: Vec<JsonStrategyAggregate>,
    /// Whether settle-pnl wrote a row to `polymarket_btc.pnl_daily`
    /// this run. `false` when zero orders settled — the caller is
    /// expected to retry post-resolution.
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

fn build_json_payload(
    bd: i64,
    orders: &[crate::coredb::types::Order],
    n_settled: i32,
    n_unresolved: u32,
    realized_total: f64,
    keys: &[String],
    by_strategy: &HashMap<String, StrategyTotal>,
    pnl_daily_written: bool,
) -> JsonOut {
    JsonOut {
        bucket_day_ms: bd,
        n_orders: orders.len(),
        n_settled,
        n_unresolved,
        realized_pnl_total: realized_total,
        strategies: keys
            .iter()
            .map(|k| {
                let t = &by_strategy[k];
                JsonStrategyAggregate {
                    strategy: k.clone(),
                    n_orders: t.n_orders,
                    n_settled: t.n_settled,
                    n_unresolved: t.n_unresolved,
                    realized_pnl: t.realized_pnl,
                }
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
