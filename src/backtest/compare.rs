//! Mark-to-market PnL comparison between strategies.
//!
//! Reads every `polymarket_btc.decisions` row for a chosen UTC day,
//! buckets them by strategy (baseline rule vs LLM, based on the row's
//! `raw_response` content), looks up the current YES price for each
//! market live from Polymarket Gamma, and prints a side-by-side PnL
//! summary plus a list of markets where the two strategies disagreed.
//!
//! "PnL" here is a hypothetical paper PnL — no orders are placed, no
//! fees / slippage are modeled. The point is comparative: same market
//! prices, same time window, two strategies → which one would have
//! made more money mark-to-market right now?

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;

use crate::backtest::ledger;
use crate::coredb::types::{bucket_day, now_ms, Decision};

/// Strategy bucket for a single decision row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Strategy {
    Baseline,
    Llm,
}

impl Strategy {
    fn classify(d: &Decision) -> Self {
        if d.raw_response == "baseline-rule" {
            Strategy::Baseline
        } else {
            Strategy::Llm
        }
    }
    fn label(&self) -> &'static str {
        match self {
            Strategy::Baseline => "baseline",
            Strategy::Llm => "llm",
        }
    }
}

/// One marked-to-market decision row.
struct Marked<'a> {
    decision: &'a Decision,
    mark: Option<f64>,
    pnl: Option<f64>,
}

pub async fn run(_coredb_uri: &str) -> Result<()> {
    let bd = bucket_day(now_ms());
    let ledger_path = ledger::path();
    println!(
        "📊 compare-pnl: reading ledger {} for bucket_day = {} (UTC ms)",
        ledger_path.display(),
        bd
    );

    let decisions = ledger::read_day(bd).context("read ledger")?;
    println!("   {} decisions in ledger for today", decisions.len());

    if decisions.is_empty() {
        println!(
            "   (nothing to compare — run `backtest` and `backtest --llm` first, then re-run.)"
        );
        return Ok(());
    }

    // Drop pre-schema rows (entry_price = 0): they predate the column
    // and we can't mark them to market. We also drop pre-schema PASS
    // rows here — including them would inflate one strategy's PASS
    // count and skew side-distribution readings even though they don't
    // affect PnL.
    let usable: Vec<&Decision> = decisions
        .iter()
        .filter(|d| d.entry_price > 0.0)
        .collect();
    let dropped = decisions.len() - usable.len();
    if dropped > 0 {
        println!(
            "   skipped {dropped} pre-schema rows (no entry_price recorded)"
        );
    }
    if usable.is_empty() {
        println!(
            "   (no rows with entry_price — re-run `backtest [--llm]` and try again.)"
        );
        return Ok(());
    }

    let http = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("reqwest client")?;
    println!("🌐 compare-pnl: pulling current YES prices from Polymarket Gamma");
    let marks = fetch_current_yes_prices(&http).await?;
    println!("   {} live yes prices", marks.len());

    // Mark each decision. PASS rows always get a defined PnL of 0; non-PASS
    // rows whose market isn't in the live pull get None and are excluded
    // from per-strategy totals (would otherwise silently zero out).
    let mut by_strategy: HashMap<Strategy, Vec<Marked>> = HashMap::new();
    for d in &usable {
        let mark = marks.get(&d.market_slug).copied();
        let pnl = match d.side.as_str() {
            "PASS" => Some(0.0),
            _ => mark.map(|m| pnl_for(&d.side, d.size_usd, d.entry_price, m)),
        };
        by_strategy
            .entry(Strategy::classify(d))
            .or_default()
            .push(Marked { decision: d, mark, pnl });
    }

    print_summary(&by_strategy);
    print_disagreements(&usable);

    Ok(())
}

/// Marked-to-market PnL for a single decision. Treats Polymarket prices
/// as probabilities in [0,1] and assumes the share economics
/// (1 share pays $1 if its side wins).
///
/// - YES bought at `entry`, marked at `mark`:
///     shares = size / entry,  value = shares * mark
///     pnl = value - size = size * (mark - entry) / entry
/// - NO bought at `1 - entry`, marked at `1 - mark`:
///     shares = size / (1 - entry),  value = shares * (1 - mark)
///     pnl = size * (entry - mark) / (1 - entry)
///
/// Returns 0 for unrecognized sides (defensive) and clamps `entry` /
/// `mark` to (1e-6, 1-1e-6) to avoid divide-by-zero on degenerate
/// markets.
pub fn pnl_for(side: &str, size_usd: f64, entry: f64, mark: f64) -> f64 {
    if size_usd <= 0.0 {
        return 0.0;
    }
    let eps = 1e-6;
    let e = entry.clamp(eps, 1.0 - eps);
    let m = mark.clamp(eps, 1.0 - eps);
    match side {
        "YES" => size_usd * (m - e) / e,
        "NO" => size_usd * (e - m) / (1.0 - e),
        _ => 0.0,
    }
}

fn print_summary(by_strategy: &HashMap<Strategy, Vec<Marked>>) {
    println!("\n🏆 Strategy comparison");
    println!(
        "   {:<10} {:>10} {:>5} {:>5} {:>5} {:>10} {:>10} {:>10}",
        "strategy", "decisions", "YES", "NO", "PASS", "Σ size", "Σ pnl", "avg pnl"
    );
    let mut strategies: Vec<_> = by_strategy.keys().copied().collect();
    strategies.sort_by_key(|s| s.label());
    for s in strategies {
        let rows = &by_strategy[&s];
        let n = rows.len();
        let mut counts = HashMap::<&str, u32>::new();
        let mut sum_size = 0.0;
        let mut sum_pnl = 0.0;
        let mut counted = 0u32;
        for r in rows {
            *counts.entry(r.decision.side.as_str()).or_default() += 1;
            if r.decision.side != "PASS" {
                sum_size += r.decision.size_usd;
            }
            if let Some(p) = r.pnl {
                sum_pnl += p;
                counted += 1;
            }
        }
        let avg = if counted > 0 {
            sum_pnl / counted as f64
        } else {
            0.0
        };
        println!(
            "   {:<10} {:>10} {:>5} {:>5} {:>5} {:>10} {:>10} {:>10}",
            s.label(),
            n,
            counts.get("YES").copied().unwrap_or(0),
            counts.get("NO").copied().unwrap_or(0),
            counts.get("PASS").copied().unwrap_or(0),
            format!("${:.2}", sum_size),
            format!("${:+.2}", sum_pnl),
            format!("${:+.2}", avg),
        );
    }
}

/// Print markets where baseline and LLM picked different sides on the
/// same decision timestamp. Limited to the first 20 so the output
/// stays readable.
fn print_disagreements(all: &[&Decision]) {
    let mut by_market: HashMap<&str, (Option<&Decision>, Option<&Decision>)> = HashMap::new();
    for d in all {
        let slot = by_market.entry(d.market_slug.as_str()).or_default();
        match Strategy::classify(d) {
            Strategy::Baseline => slot.0 = Some(d),
            Strategy::Llm => slot.1 = Some(d),
        }
    }
    let mut disagree: Vec<_> = by_market
        .iter()
        .filter_map(|(slug, (b, l))| match (b, l) {
            (Some(b), Some(l)) if b.side != l.side => Some((slug, b, l)),
            _ => None,
        })
        .collect();
    disagree.sort_by_key(|(slug, _, _)| **slug);

    if disagree.is_empty() {
        println!("\n🤝 No same-day disagreements between strategies");
        return;
    }
    println!(
        "\n🔀 {} markets where strategies disagreed (first 20 shown):",
        disagree.len()
    );
    for (slug, b, l) in disagree.iter().take(20) {
        println!(
            "   {slug}\n       baseline = {:<4} size=${:.2}  conf={:.2}\n       llm      = {:<4} size=${:.2}  conf={:.2}",
            b.side, b.size_usd, b.confidence, l.side, l.size_usd, l.confidence,
        );
    }
}

/// Pull every active BTC market from Polymarket Gamma and return a
/// slug → yes_price map. Matches the same endpoint the ingest path
/// uses so the slug set lines up.
async fn fetch_current_yes_prices(http: &Client) -> Result<HashMap<String, f64>> {
    const URL: &str = "https://gamma-api.polymarket.com/markets\
                       ?active=true&closed=false&order=volume24hr&ascending=false&limit=500";
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
        let slug = v.get("slug").and_then(|x| x.as_str()).unwrap_or("");
        if slug.is_empty() {
            continue;
        }
        let yes_price = v
            .get("outcomePrices")
            .and_then(|x| x.as_str())
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .and_then(|v| v.first().cloned())
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(0.0);
        if yes_price > 0.0 {
            out.insert(slug.to_string(), yes_price);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pass_pnl_is_zero() {
        assert_eq!(pnl_for("PASS", 100.0, 0.5, 0.7), 0.0);
    }

    #[test]
    fn yes_long_gains_when_mark_rises() {
        // Bought $10 of YES at 0.50; market moves to 0.60.
        // shares = 20, value = $12, pnl = +$2.
        let p = pnl_for("YES", 10.0, 0.5, 0.6);
        assert!((p - 2.0).abs() < 1e-9, "{p}");
    }

    #[test]
    fn yes_long_loses_when_mark_falls() {
        let p = pnl_for("YES", 10.0, 0.5, 0.4);
        assert!((p + 2.0).abs() < 1e-9, "{p}");
    }

    #[test]
    fn no_short_gains_when_mark_falls() {
        // Bought $10 of NO at 0.50 (i.e. paid 0.50/share); market YES
        // moves down to 0.40, so NO is now worth 0.60.
        // shares = 20, value = $12, pnl = +$2.
        let p = pnl_for("NO", 10.0, 0.5, 0.4);
        assert!((p - 2.0).abs() < 1e-9, "{p}");
    }

    #[test]
    fn zero_size_is_zero_pnl() {
        assert_eq!(pnl_for("YES", 0.0, 0.5, 0.7), 0.0);
    }

    #[test]
    fn degenerate_entry_does_not_panic() {
        // Entry at 0 or 1 would divide-by-zero without clamping.
        let p = pnl_for("YES", 10.0, 0.0, 0.5);
        assert!(p.is_finite());
        let p = pnl_for("NO", 10.0, 1.0, 0.5);
        assert!(p.is_finite());
    }
}
