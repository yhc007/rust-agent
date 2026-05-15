//! Mark-to-market PnL comparison between strategies.
//!
//! Reads every `polymarket_btc.decisions` row for a chosen UTC day,
//! buckets them by strategy (the explicit `strategy` column, with a
//! legacy `raw_response == "baseline-rule"` fallback for pre-schema
//! rows), looks up the current YES price for each market live from
//! Polymarket Gamma, and prints a side-by-side PnL summary plus a list
//! of markets where strategies disagreed.
//!
//! "PnL" here is a hypothetical paper PnL — no orders are placed, no
//! fees / slippage are modeled. The point is comparative: same market
//! prices, same time window, N strategies → which one would have made
//! more money mark-to-market right now?

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;

use crate::coredb::decisions::DecisionRepo;
use crate::coredb::strategy_pnl::StrategyPnlRepo;
use crate::coredb::types::{bucket_day, now_ms, Decision, StrategyPnlSnapshot};
use crate::coredb::CoreDb;

/// One marked-to-market decision row.
struct Marked<'a> {
    decision: &'a Decision,
    #[allow(dead_code)]
    mark: Option<f64>,
    pnl: Option<f64>,
}

pub async fn run(coredb_uri: &str) -> Result<()> {
    println!("📊 compare-pnl: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let repo = DecisionRepo::new(db.session()).await?;
    let snap_repo = StrategyPnlRepo::new(db.session()).await?;

    let snapshot_ts = now_ms();
    let bd = bucket_day(snapshot_ts);
    let decisions = repo
        .list_day(bd)
        .await
        .context("list decisions for today")?;
    println!(
        "   {} decisions for bucket_day = {} (UTC ms)",
        decisions.len(),
        bd
    );

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
    let mut by_strategy: HashMap<String, Vec<Marked>> = HashMap::new();
    for d in &usable {
        let mark = marks.get(&d.market_slug).copied();
        let pnl = match d.side.as_str() {
            "PASS" => Some(0.0),
            _ => mark.map(|m| pnl_for(&d.side, d.size_usd, d.entry_price, m)),
        };
        by_strategy
            .entry(d.effective_strategy().to_string())
            .or_default()
            .push(Marked { decision: d, mark, pnl });
    }

    print_summary(&by_strategy);
    print_agreement_matrix(&usable);
    print_disagreements(&usable);

    // Persist per-strategy aggregates as time-series snapshots so the
    // history can be reconstructed later (or scraped by a cron-driven
    // pnl-history readout). One row per strategy per invocation; failures
    // are logged but don't bail the run since the human-readable output
    // already landed.
    for (strategy, rows) in &by_strategy {
        let mut n_yes = 0i32;
        let mut n_no = 0i32;
        let mut n_pass = 0i32;
        let mut sum_size = 0.0;
        let mut sum_pnl = 0.0;
        for r in rows {
            match r.decision.side.as_str() {
                "YES" => {
                    n_yes += 1;
                    sum_size += r.decision.size_usd;
                }
                "NO" => {
                    n_no += 1;
                    sum_size += r.decision.size_usd;
                }
                _ => n_pass += 1,
            }
            if let Some(p) = r.pnl {
                sum_pnl += p;
            }
        }
        let snap = StrategyPnlSnapshot {
            bucket_day_ms: bd,
            ts_ms: snapshot_ts,
            strategy: strategy.clone(),
            n_decisions: rows.len() as i32,
            sum_size_usd: sum_size,
            sum_pnl,
            n_yes,
            n_no,
            n_pass,
        };
        if let Err(e) = snap_repo.insert(&snap).await {
            eprintln!("  ! strategy_pnl_snapshots insert failed for {strategy}: {e}");
        }
    }

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

fn print_summary(by_strategy: &HashMap<String, Vec<Marked>>) {
    println!("\n🏆 Strategy comparison");
    println!(
        "   {:<12} {:>10} {:>5} {:>5} {:>5} {:>10} {:>10} {:>10}",
        "strategy", "decisions", "YES", "NO", "PASS", "Σ size", "Σ pnl", "avg pnl"
    );
    let mut strategies: Vec<&String> = by_strategy.keys().collect();
    strategies.sort();
    for s in strategies {
        let rows = &by_strategy[s];
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
            "   {:<12} {:>10} {:>5} {:>5} {:>5} {:>10} {:>10} {:>10}",
            s,
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

/// Compute and print an N×N agreement matrix: for every ordered
/// pair of strategies (A, B), what fraction of markets where both
/// emitted a decision did they pick the same side?
///
/// PASS counts as a side, so two strategies that both PASS the same
/// market agree on it. That matches the intuition for the consensus
/// dashboard ("did both look at this and reach the same conclusion?")
/// even though PASS isn't really an "opinion".
fn print_agreement_matrix(all: &[&Decision]) {
    let matrix = compute_agreement_matrix(all);
    if matrix.strategies.len() < 2 {
        // 0 or 1 strategies → matrix is degenerate; the summary table
        // already covers the single-strategy case.
        return;
    }
    println!("\n🧩 Pairwise agreement (% of shared markets both strategies picked the same side):");
    // Header row.
    print!("   {:<12}", "");
    for s in &matrix.strategies {
        print!(" {:>10}", truncate_label(s, 10));
    }
    println!();
    // Body.
    for (i, row_name) in matrix.strategies.iter().enumerate() {
        print!("   {:<12}", truncate_label(row_name, 12));
        for j in 0..matrix.strategies.len() {
            if i == j {
                print!(" {:>10}", "-");
            } else {
                let cell = &matrix.cells[i][j];
                if cell.shared == 0 {
                    print!(" {:>10}", "n/a");
                } else {
                    let pct = (cell.matches as f64 / cell.shared as f64) * 100.0;
                    print!(" {:>9.1}%", pct);
                }
            }
        }
        println!();
    }
    // Footnote with sample size — important when one pair only has a
    // handful of shared markets and an apparently-high % is noise.
    println!("   shared-market counts:");
    for (i, a) in matrix.strategies.iter().enumerate() {
        for (j, b) in matrix.strategies.iter().enumerate() {
            if j <= i {
                continue;
            }
            let cell = &matrix.cells[i][j];
            println!(
                "     {:<12} ↔ {:<12} {:>4} shared, {:>4} matches",
                a, b, cell.shared, cell.matches,
            );
        }
    }
}

/// One pair's agreement count. `shared` = number of markets where both
/// strategies emitted a decision; `matches` = subset where they picked
/// the same side (incl. both PASS).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AgreementCell {
    pub shared: u32,
    pub matches: u32,
}

/// Full agreement matrix. `strategies` is the sorted list of strategy
/// names; `cells[i][j]` is the i↔j agreement (symmetric, diagonal
/// untouched/default).
#[derive(Debug, Clone, Default)]
pub struct AgreementMatrix {
    pub strategies: Vec<String>,
    pub cells: Vec<Vec<AgreementCell>>,
}

/// Build the matrix from a flat list of decisions. Pure function —
/// extracted from the printer so it can be unit-tested without
/// stdout capture.
fn compute_agreement_matrix(all: &[&Decision]) -> AgreementMatrix {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    // market_slug → strategy → latest side. Later rows for the same
    // (market, strategy) overwrite earlier — matches the dashboard's
    // latest-wins semantics so a periodic daemon doesn't double-count.
    let mut by_market: HashMap<&str, BTreeMap<&str, &str>> = HashMap::new();
    let mut latest_ts: HashMap<(&str, &str), i64> = HashMap::new();
    for d in all {
        let key = (d.market_slug.as_str(), d.effective_strategy());
        let keep = latest_ts.get(&key).map(|t| d.ts_ms >= *t).unwrap_or(true);
        if keep {
            latest_ts.insert(key, d.ts_ms);
            by_market
                .entry(d.market_slug.as_str())
                .or_default()
                .insert(d.effective_strategy(), d.side.as_str());
        }
    }

    let strategies: BTreeSet<String> = all.iter().map(|d| d.effective_strategy().to_string()).collect();
    let strategies: Vec<String> = strategies.into_iter().collect();
    let n = strategies.len();
    let mut cells = vec![vec![AgreementCell::default(); n]; n];
    for picks in by_market.values() {
        for (i, a) in strategies.iter().enumerate() {
            let Some(side_a) = picks.get(a.as_str()) else { continue };
            for (j, b) in strategies.iter().enumerate() {
                if i == j {
                    continue;
                }
                let Some(side_b) = picks.get(b.as_str()) else { continue };
                cells[i][j].shared += 1;
                if side_a == side_b {
                    cells[i][j].matches += 1;
                }
            }
        }
    }
    AgreementMatrix { strategies, cells }
}

fn truncate_label(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max.saturating_sub(1)).collect::<String>() + "…"
    }
}

/// Print markets where strategies picked different sides on the same
/// decision timestamp. With N strategies in play we group by
/// market_slug and flag any market whose (strategy → side) map has
/// more than one distinct side. Limited to the first 20 so the output
/// stays readable.
fn print_disagreements(all: &[&Decision]) {
    // market_slug → (strategy → decision). Latest decision per
    // (market, strategy) wins so a periodic run that emitted multiple
    // entries doesn't double-list.
    let mut by_market: HashMap<&str, HashMap<&str, &Decision>> = HashMap::new();
    for d in all {
        let per_strat = by_market.entry(d.market_slug.as_str()).or_default();
        let strat = d.effective_strategy();
        let keep = match per_strat.get(strat) {
            Some(prev) => d.ts_ms >= prev.ts_ms,
            None => true,
        };
        if keep {
            per_strat.insert(strat, d);
        }
    }
    let mut disagree: Vec<(&&str, Vec<(&&str, &&Decision)>)> = by_market
        .iter()
        .filter_map(|(slug, per_strat)| {
            let distinct: std::collections::HashSet<&str> =
                per_strat.values().map(|d| d.side.as_str()).collect();
            if distinct.len() > 1 {
                let mut pairs: Vec<_> = per_strat.iter().collect();
                pairs.sort_by_key(|(k, _)| **k);
                Some((slug, pairs))
            } else {
                None
            }
        })
        .collect();
    disagree.sort_by_key(|(slug, _)| **slug);

    if disagree.is_empty() {
        println!("\n🤝 No same-day disagreements between strategies");
        return;
    }
    println!(
        "\n🔀 {} markets where strategies disagreed (first 20 shown):",
        disagree.len()
    );
    for (slug, pairs) in disagree.iter().take(20) {
        println!("   {slug}");
        for (strat, d) in pairs {
            println!(
                "       {:<10} = {:<4} size=${:.2}  conf={:.2}",
                strat, d.side, d.size_usd, d.confidence,
            );
        }
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

    fn dec(slug: &str, strategy: &str, side: &str, ts: i64) -> crate::coredb::types::Decision {
        crate::coredb::types::Decision {
            bucket_day_ms: 0,
            ts_ms: ts,
            decision_id: uuid::Uuid::nil(),
            market_slug: slug.into(),
            side: side.into(),
            size_usd: 1.0,
            confidence: 0.0,
            edge_bps: 0,
            reasoning: String::new(),
            raw_response: String::new(),
            entry_price: 0.5,
            strategy: strategy.into(),
        }
    }

    fn refs<'a>(v: &'a [crate::coredb::types::Decision]) -> Vec<&'a crate::coredb::types::Decision> {
        v.iter().collect()
    }

    #[test]
    fn agreement_matrix_two_strategies_perfect_match() {
        let rows = vec![
            dec("m1", "a", "YES", 1),
            dec("m1", "b", "YES", 1),
            dec("m2", "a", "NO", 2),
            dec("m2", "b", "NO", 2),
        ];
        let m = compute_agreement_matrix(&refs(&rows));
        assert_eq!(m.strategies, vec!["a", "b"]);
        assert_eq!(m.cells[0][1].shared, 2);
        assert_eq!(m.cells[0][1].matches, 2);
        // Symmetric.
        assert_eq!(m.cells[1][0], m.cells[0][1]);
    }

    #[test]
    fn agreement_matrix_partial_match() {
        let rows = vec![
            dec("m1", "a", "YES", 1),
            dec("m1", "b", "NO", 1),
            dec("m2", "a", "PASS", 2),
            dec("m2", "b", "PASS", 2),
            dec("m3", "a", "YES", 3),
            dec("m3", "b", "YES", 3),
        ];
        let m = compute_agreement_matrix(&refs(&rows));
        assert_eq!(m.cells[0][1].shared, 3);
        assert_eq!(m.cells[0][1].matches, 2);
    }

    #[test]
    fn agreement_matrix_only_shared_markets_count() {
        // b never weighed in on m2, so it shouldn't dilute the pair.
        let rows = vec![
            dec("m1", "a", "YES", 1),
            dec("m1", "b", "YES", 1),
            dec("m2", "a", "NO", 2),
            // no b on m2
        ];
        let m = compute_agreement_matrix(&refs(&rows));
        assert_eq!(m.cells[0][1].shared, 1);
        assert_eq!(m.cells[0][1].matches, 1);
    }

    #[test]
    fn agreement_matrix_three_strategies_off_diagonal() {
        let rows = vec![
            dec("m1", "a", "YES", 1),
            dec("m1", "b", "YES", 1),
            dec("m1", "c", "NO", 1),
        ];
        let m = compute_agreement_matrix(&refs(&rows));
        assert_eq!(m.strategies, vec!["a", "b", "c"]);
        // a-b matches, a-c and b-c don't.
        assert_eq!(m.cells[0][1].matches, 1);
        assert_eq!(m.cells[0][2].matches, 0);
        assert_eq!(m.cells[1][2].matches, 0);
    }

    #[test]
    fn agreement_matrix_latest_decision_wins() {
        // a flips YES → NO at ts=5; the matrix should use NO and
        // therefore disagree with b's YES.
        let rows = vec![
            dec("m1", "a", "YES", 1),
            dec("m1", "a", "NO", 5),
            dec("m1", "b", "YES", 3),
        ];
        let m = compute_agreement_matrix(&refs(&rows));
        assert_eq!(m.cells[0][1].shared, 1);
        assert_eq!(m.cells[0][1].matches, 0);
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
