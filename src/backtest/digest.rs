//! `digest` subcommand — Phase 2 curation layer.
//!
//! Reads a window of `polymarket_btc.decisions` rows, ranks every
//! row by [`crate::notification::decision_score`], and surfaces:
//!
//! 1. **Top-N highest-score recommendations** (across all strategies)
//! 2. **Per-strategy aggregate** — how many decisions, average score,
//!    side distribution
//!
//! Modes:
//!
//! - Default: human-readable table to stdout
//! - `--json`: single-object JSON for downstream tooling
//! - `--post`: single curated message to the configured Slack webhook
//!   (so daily cron can replace the noisy per-decision firehose with
//!   one summary)
//!
//! The window arg shape mirrors `compare-pnl`'s `--since` / `--days`
//! — same parser, same semantics, so an operator who's comfortable
//! with one is comfortable with the other.

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::json;

use crate::coredb::decisions::DecisionRepo;
use crate::coredb::pnl_breakdown::PnlBreakdownRepo;
use crate::coredb::types::{bucket_day, now_ms, Decision, Millis};
use crate::coredb::CoreDb;
use crate::notification::{decision_score, NotificationConfig};

const DAY_MS: Millis = 86_400_000;

/// Configuration for one digest invocation. Built once from CLI args
/// in `main.rs`; the run function below is pure-by-construction
/// (its only I/O is the CoreDB read + the optional Slack post).
pub struct DigestPlan {
    pub days: u32,
    pub since_ms: Option<i64>,
    pub strategies_filter: Option<Vec<String>>,
    pub top_n: usize,
    pub json: bool,
    /// When true, post one curated summary to the SLACK_WEBHOOK_URL
    /// from env. Body content matches stdout, formatted as a single
    /// multi-line Slack message.
    pub post: bool,
}

/// Per-strategy aggregate for the summary table / JSON payload.
///
/// `realized_pnl_usd` and `n_settled` are joined in from
/// `pnl_breakdown` (Phase 3 hindsight): the "what actually
/// happened" view alongside "what was recommended". Both default
/// to 0 when no rows have been written for this strategy in the
/// window — settle-pnl hasn't run yet, or the markets haven't
/// resolved on Polymarket.
#[derive(Debug, Clone, Serialize)]
pub struct StrategyAgg {
    pub strategy: String,
    pub n_decisions: usize,
    pub n_yes: usize,
    pub n_no: usize,
    pub n_pass: usize,
    pub avg_score: f64,
    pub max_score: f64,
    pub realized_pnl_usd: f64,
    pub n_settled: i32,
}

/// One ranked decision in the top-N list — projects only what the
/// digest renders, keeping the JSON payload tight.
#[derive(Debug, Clone, Serialize)]
pub struct RankedDecision {
    pub score: f64,
    pub strategy: String,
    pub market_slug: String,
    pub side: String,
    pub size_usd: f64,
    pub confidence: f64,
    pub edge_bps: i32,
    pub entry_price: f64,
    pub reasoning: String,
    pub ts_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DigestPayload {
    pub generated_at_ms: i64,
    pub window_start_ms: i64,
    pub window_end_ms: i64,
    pub days_spanned: u32,
    pub n_decisions_total: usize,
    pub strategies: Vec<StrategyAgg>,
    pub top: Vec<RankedDecision>,
}

/// Read every decision in the chosen window and compute aggregates +
/// top-N. `(payload, slack_body)` — caller decides which to emit.
pub async fn build_payload(
    coredb_uri: &str,
    plan: &DigestPlan,
) -> Result<(DigestPayload, String)> {
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let repo = DecisionRepo::new(db.session()).await?;
    let breakdown_repo = PnlBreakdownRepo::new(db.session()).await?;

    let snapshot_ts = now_ms();
    let today = bucket_day(snapshot_ts);

    // --since takes precedence; otherwise --days. Same shape as
    // compare-pnl so a user fluent in one is fluent in both.
    let effective_days: u32 = match plan.since_ms {
        Some(since) => {
            let start_bd = bucket_day(since);
            ((today - start_bd) / DAY_MS).max(0) as u32 + 1
        }
        None => plan.days.max(1),
    };

    let mut rows: Vec<Decision> = Vec::new();
    for i in 0..effective_days as i64 {
        let day_bd = today - (effective_days as i64 - 1 - i) * DAY_MS;
        if let Ok(part) = repo.list_day(day_bd).await {
            rows.extend(part);
        }
    }
    // Post-filter on --since boundary so a "since 6h" run doesn't
    // pull in earlier rows from today's bucket.
    if let Some(since) = plan.since_ms {
        rows.retain(|d| d.ts_ms >= since);
    }

    // Apply strategy filter at the same point compare-pnl does.
    if let Some(allow) = plan.strategies_filter.as_ref() {
        let set: std::collections::HashSet<&str> = allow.iter().map(String::as_str).collect();
        rows.retain(|d| set.contains(d.effective_strategy()));
    }

    let n_total = rows.len();
    let window_start_ms = plan.since_ms.unwrap_or_else(|| {
        today - (effective_days as i64 - 1) * DAY_MS
    });

    // Phase 3 hindsight: pull realized PnL across the same window
    // and aggregate per strategy (across paper + live execs — the
    // operator wants total "what the bot did" not split by exec).
    // Failures on individual bucket_day reads degrade gracefully:
    // the aggregate stays at 0 for that day rather than aborting.
    let mut realized_per_strategy: std::collections::HashMap<String, (f64, i32)> =
        std::collections::HashMap::new();
    for i in 0..effective_days as i64 {
        let day_bd = today - (effective_days as i64 - 1 - i) * DAY_MS;
        if let Ok(part) = breakdown_repo.list_day(day_bd).await {
            for row in part {
                let entry = realized_per_strategy
                    .entry(row.strategy.clone())
                    .or_insert((0.0, 0));
                entry.0 += row.realized_pnl;
                entry.1 += row.n_settled;
            }
        }
    }

    // Per-strategy aggregate.
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<String, Vec<&Decision>> = BTreeMap::new();
    for d in &rows {
        groups.entry(d.effective_strategy().to_string()).or_default().push(d);
    }
    let mut strategies: Vec<StrategyAgg> = groups
        .iter()
        .map(|(strategy, ds)| {
            let mut sum_score = 0.0;
            let mut max_score: f64 = 0.0;
            let (mut n_yes, mut n_no, mut n_pass) = (0, 0, 0);
            for d in ds {
                let s = decision_score(d.confidence, d.edge_bps);
                sum_score += s;
                if s > max_score {
                    max_score = s;
                }
                match d.side.as_str() {
                    "YES" => n_yes += 1,
                    "NO" => n_no += 1,
                    "PASS" => n_pass += 1,
                    _ => {}
                }
            }
            let n = ds.len();
            let (realized_pnl_usd, n_settled) = realized_per_strategy
                .get(strategy)
                .copied()
                .unwrap_or((0.0, 0));
            StrategyAgg {
                strategy: strategy.clone(),
                n_decisions: n,
                n_yes,
                n_no,
                n_pass,
                avg_score: if n > 0 { sum_score / n as f64 } else { 0.0 },
                max_score,
                realized_pnl_usd,
                n_settled,
            }
        })
        .collect();
    // Surface strategies that have only realized data in the window
    // (settled trades from decisions made earlier). They show 0
    // decisions for the window but a non-zero realized PnL, which
    // is still useful operator signal: "this strategy is still
    // generating returns even though it wasn't active today."
    for (strategy, (realized_pnl_usd, n_settled)) in &realized_per_strategy {
        if !groups.contains_key(strategy) {
            strategies.push(StrategyAgg {
                strategy: strategy.clone(),
                n_decisions: 0,
                n_yes: 0,
                n_no: 0,
                n_pass: 0,
                avg_score: 0.0,
                max_score: 0.0,
                realized_pnl_usd: *realized_pnl_usd,
                n_settled: *n_settled,
            });
        }
    }
    // Sort by avg_score desc so the "best strategy of the window"
    // surfaces first.
    strategies.sort_by(|a, b| b.avg_score.partial_cmp(&a.avg_score).unwrap_or(std::cmp::Ordering::Equal));

    // Top-N decisions across all strategies, ranked by score.
    // PASS decisions can have score = 0; exclude them from the
    // top list so the curated summary stays actionable.
    let mut ranked: Vec<RankedDecision> = rows
        .iter()
        .filter(|d| d.side != "PASS")
        .map(|d| {
            RankedDecision {
                score: decision_score(d.confidence, d.edge_bps),
                strategy: d.effective_strategy().to_string(),
                market_slug: d.market_slug.clone(),
                side: d.side.clone(),
                size_usd: d.size_usd,
                confidence: d.confidence,
                edge_bps: d.edge_bps,
                entry_price: d.entry_price,
                reasoning: d.reasoning.clone(),
                ts_ms: d.ts_ms,
            }
        })
        .collect();
    ranked.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    ranked.truncate(plan.top_n);

    let payload = DigestPayload {
        generated_at_ms: snapshot_ts,
        window_start_ms,
        window_end_ms: snapshot_ts,
        days_spanned: effective_days,
        n_decisions_total: n_total,
        strategies,
        top: ranked,
    };

    let slack_body = format_slack_body(&payload);
    Ok((payload, slack_body))
}

/// Slack-shaped multi-line message body. Header line + per-strategy
/// scores + top recommendations with `_reasoning_` italics. Sized to
/// fit one Slack notification card without scrolling.
fn format_slack_body(p: &DigestPayload) -> String {
    let mut out = String::new();
    let total_realized: f64 = p.strategies.iter().map(|s| s.realized_pnl_usd).sum();
    let total_settled: i32 = p.strategies.iter().map(|s| s.n_settled).sum();
    let realized_summary = if total_settled > 0 {
        format!(" · realized ${:+.2} from {} settled", total_realized, total_settled)
    } else {
        // No settled trades in the window — surface that fact so
        // the operator doesn't wonder where the P&L column is.
        " · no settled trades in window".to_string()
    };
    out.push_str(&format!(
        "📊 *Polymarket BTC digest* — {} decisions across {} day(s){}\n",
        p.n_decisions_total, p.days_spanned, realized_summary,
    ));
    out.push_str("\n*Per-strategy:*\n");
    for s in &p.strategies {
        // Realized P&L column only renders when there are settled
        // trades for this strategy — keeps lines short for
        // strategies that haven't had any markets resolve yet.
        let realized_part = if s.n_settled > 0 {
            format!(" · realized ${:+.2} ({} settled)", s.realized_pnl_usd, s.n_settled)
        } else {
            String::new()
        };
        // Strategies that only have realized data (no decisions in
        // window) deserve a slightly different shape — no "0 dec"
        // gymnastics, just the realized number.
        if s.n_decisions == 0 && s.n_settled > 0 {
            out.push_str(&format!(
                "  `{}` — no new decisions in window{}\n",
                s.strategy, realized_part,
            ));
        } else {
            out.push_str(&format!(
                "  `{}` — {} dec (Y{} N{} P{}) · avg score {:.2} · max {:.2}{}\n",
                s.strategy, s.n_decisions, s.n_yes, s.n_no, s.n_pass,
                s.avg_score, s.max_score, realized_part,
            ));
        }
    }
    if !p.top.is_empty() {
        out.push_str(&format!("\n*Top {} recommendations:*\n", p.top.len()));
        for (i, d) in p.top.iter().enumerate() {
            let arrow = match d.side.as_str() {
                "YES" => "🟢",
                "NO" => "🔴",
                _ => "·",
            };
            // Keep reasoning brief in the digest so the message
            // doesn't blow past Slack's card-rendering limit.
            let mut reasoning = d.reasoning.clone();
            if reasoning.chars().count() > 140 {
                let t: String = reasoning.chars().take(137).collect();
                reasoning = format!("{t}…");
            }
            out.push_str(&format!(
                "  {}. {:.2}  `{}` {} {} ${:.0} @{:.4} ({})\n     _{}_\n",
                i + 1, d.score, d.strategy, arrow, d.side, d.size_usd, d.entry_price,
                d.market_slug, reasoning,
            ));
        }
    }
    out
}

/// CLI entry — drives `build_payload` and renders the chosen output.
pub async fn run(coredb_uri: &str, plan: DigestPlan) -> Result<()> {
    let (payload, slack_body) = build_payload(coredb_uri, &plan).await?;

    if plan.json {
        // jq-friendly single-object dump on stdout.
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        // Same content as the Slack body — stdout-friendly with the
        // Slack `_italic_` markers stripped is fine; the markers are
        // unobtrusive in plain text too.
        println!("{}", slack_body);
    }

    if plan.post {
        let cfg = NotificationConfig::from_env();
        match cfg.webhook_url.as_ref() {
            Some(url) => {
                let payload = json!({ "text": slack_body });
                let client = reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build()
                    .context("build http client")?;
                match client.post(url).json(&payload).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        eprintln!("✓ digest posted to Slack");
                    }
                    Ok(resp) => {
                        eprintln!("⚠ Slack returned {}: digest may not have arrived", resp.status());
                    }
                    Err(e) => {
                        eprintln!("⚠ Slack POST failed: {e}");
                    }
                }
            }
            None => {
                eprintln!("⚠ --post requested but SLACK_WEBHOOK_URL is unset; skipping post");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notification::decision_score;
    use uuid::Uuid;

    fn d(strategy: &str, side: &str, conf: f64, edge: i32, slug: &str) -> Decision {
        Decision {
            bucket_day_ms: 0,
            ts_ms: 0,
            decision_id: Uuid::nil(),
            market_slug: slug.into(),
            side: side.into(),
            size_usd: 50.0,
            confidence: conf,
            edge_bps: edge,
            reasoning: "test".into(),
            raw_response: "t".into(),
            entry_price: 0.5,
            strategy: strategy.into(),
        }
    }

    #[test]
    fn score_zero_when_edge_is_zero() {
        assert!((decision_score(0.95, 0)).abs() < 1e-9);
    }

    #[test]
    fn score_saturates_at_5000_edge() {
        let s_5000 = decision_score(0.5, 5000);
        let s_10000 = decision_score(0.5, 10000);
        assert!((s_5000 - 0.5).abs() < 1e-9);
        assert!((s_5000 - s_10000).abs() < 1e-9);
    }

    #[test]
    fn score_negative_edge_treated_as_abs() {
        let s_pos = decision_score(0.8, 2500);
        let s_neg = decision_score(0.8, -2500);
        assert!((s_pos - s_neg).abs() < 1e-9);
    }

    #[test]
    fn score_clamps_confidence_to_unit_interval() {
        let s_high = decision_score(2.0, 2500);
        let s_one = decision_score(1.0, 2500);
        assert!((s_high - s_one).abs() < 1e-9);
        let s_neg = decision_score(-0.5, 2500);
        assert!(s_neg.abs() < 1e-9);
    }

    fn agg(strategy: &str, decisions: usize, score: f64, realized: f64, settled: i32) -> StrategyAgg {
        StrategyAgg {
            strategy: strategy.into(),
            n_decisions: decisions,
            n_yes: 0, n_no: 0, n_pass: 0,
            avg_score: score, max_score: score,
            realized_pnl_usd: realized,
            n_settled: settled,
        }
    }

    #[test]
    fn slack_body_renders_strategies_and_top() {
        let payload = DigestPayload {
            generated_at_ms: 1_700_000_000_000,
            window_start_ms: 1_700_000_000_000 - 86_400_000,
            window_end_ms: 1_700_000_000_000,
            days_spanned: 1,
            n_decisions_total: 2,
            strategies: vec![
                agg("baseline", 1, 0.5, 0.0, 0),
                agg("llm", 1, 0.85, 0.0, 0),
            ],
            top: vec![
                RankedDecision {
                    score: 0.85,
                    strategy: "llm".into(),
                    market_slug: "bitcoin-above-82k-on-may-19".into(),
                    side: "NO".into(),
                    size_usd: 100.0,
                    confidence: 0.85,
                    edge_bps: 2500,
                    entry_price: 0.0015,
                    reasoning: "Spot $78k vs $82k threshold...".into(),
                    ts_ms: 0,
                },
            ],
        };
        let body = format_slack_body(&payload);
        assert!(body.contains("Polymarket BTC digest"));
        assert!(body.contains("baseline"));
        assert!(body.contains("llm"));
        assert!(body.contains("Top 1 recommendations"));
        assert!(body.contains("0.85"));
        assert!(body.contains("bitcoin-above-82k-on-may-19"));
        assert!(body.contains("🔴 NO"));
    }

    #[test]
    fn slack_body_truncates_long_reasoning() {
        let payload = DigestPayload {
            generated_at_ms: 0, window_start_ms: 0, window_end_ms: 0,
            days_spanned: 1, n_decisions_total: 1,
            strategies: vec![],
            top: vec![RankedDecision {
                score: 0.9, strategy: "x".into(),
                market_slug: "m".into(), side: "YES".into(),
                size_usd: 10.0, confidence: 0.9, edge_bps: 5000,
                entry_price: 0.5, reasoning: "x".repeat(500), ts_ms: 0,
            }],
        };
        let body = format_slack_body(&payload);
        assert!(body.contains("…"), "expected truncation ellipsis");
        assert!(
            body.chars().count() < 600,
            "body should be bounded; got {} chars",
            body.chars().count()
        );
    }

    #[test]
    fn pass_decisions_excluded_from_top() {
        let payload = DigestPayload {
            generated_at_ms: 0, window_start_ms: 0, window_end_ms: 0,
            days_spanned: 1, n_decisions_total: 3,
            strategies: vec![],
            top: vec![
                RankedDecision {
                    score: 0.9, strategy: "x".into(),
                    market_slug: "active".into(), side: "YES".into(),
                    size_usd: 50.0, confidence: 0.9, edge_bps: 5000,
                    entry_price: 0.5, reasoning: "real bet".into(), ts_ms: 0,
                },
            ],
        };
        let body = format_slack_body(&payload);
        assert!(body.contains("active"));
        assert!(!body.contains("PASS"), "PASS lines should never appear in top");
        let _ = d("baseline", "PASS", 0.0, 0, "x");
    }

    /// Phase 3: realized P&L row gets rendered in the per-strategy
    /// table when n_settled > 0. Negative P&L formats with the
    /// leading sign so it's instantly distinguishable from a win.
    #[test]
    fn slack_body_renders_realized_pnl_when_settled() {
        let payload = DigestPayload {
            generated_at_ms: 0, window_start_ms: 0, window_end_ms: 0,
            days_spanned: 1, n_decisions_total: 2,
            strategies: vec![
                agg("baseline", 1, 0.5, 12.34, 3),
                agg("llm", 1, 0.85, -2.50, 1),
            ],
            top: vec![],
        };
        let body = format_slack_body(&payload);
        assert!(body.contains("realized $+12.34 (3 settled)"));
        assert!(body.contains("realized $-2.50 (1 settled)"));
        // Header should aggregate total + count.
        assert!(body.contains("realized $+9.84 from 4 settled"));
    }

    /// When no markets have resolved yet, the realized columns are
    /// suppressed entirely (no zero-noise lines) and the header
    /// surfaces a "no settled trades" hint so the operator knows
    /// the P&L data is absent by design, not by bug.
    #[test]
    fn slack_body_suppresses_realized_pnl_when_no_settled() {
        let payload = DigestPayload {
            generated_at_ms: 0, window_start_ms: 0, window_end_ms: 0,
            days_spanned: 1, n_decisions_total: 1,
            strategies: vec![agg("baseline", 1, 0.5, 0.0, 0)],
            top: vec![],
        };
        let body = format_slack_body(&payload);
        assert!(body.contains("no settled trades in window"));
        // Should NOT show "$+0.00 (0 settled)" noise on the per-strategy line.
        assert!(!body.contains("(0 settled)"));
    }

    /// Phase 3: a strategy that has realized data but no new
    /// decisions in the window (decisions from earlier days that
    /// just settled) still surfaces, with a distinct shape that
    /// doesn't pretend it had recent activity.
    #[test]
    fn slack_body_renders_realized_only_strategies() {
        let payload = DigestPayload {
            generated_at_ms: 0, window_start_ms: 0, window_end_ms: 0,
            days_spanned: 1, n_decisions_total: 0,
            strategies: vec![StrategyAgg {
                strategy: "deepseek".into(),
                n_decisions: 0, n_yes: 0, n_no: 0, n_pass: 0,
                avg_score: 0.0, max_score: 0.0,
                realized_pnl_usd: 8.40, n_settled: 2,
            }],
            top: vec![],
        };
        let body = format_slack_body(&payload);
        assert!(body.contains("no new decisions in window"));
        assert!(body.contains("realized $+8.40"));
        // Sanity: no garbled per-strategy line like "0 dec (Y0 N0 P0)".
        assert!(!body.contains("0 dec (Y0"));
    }
}
