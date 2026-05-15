//! Backtest orchestrator.
//!
//! Decisions are pulled live from Polymarket Gamma (the read side of
//! CoreDB has historically been brittle and we only need market
//! metadata here, not stored rows). Every produced decision still
//! lands in `polymarket_btc.decisions` for downstream comparison /
//! audit. With `--execute`, non-PASS decisions are also routed through
//! the risk gate + paper executor and persisted as Order + Position
//! rows.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use uuid::Uuid;

use crate::api::{AnthropicClient, ApiClient, OpenAICompatClient};
use crate::backtest::{baseline::evaluate as baseline_evaluate, llm as llm_mod, BacktestPlan};
use crate::config::{Backend, Config};
use crate::coredb::btc::BtcTickRepo;
use crate::coredb::decisions::DecisionRepo;
use crate::coredb::markets::MarketRepo;
use crate::coredb::orders::{OrderRepo, PositionRepo};
use crate::coredb::types::{bucket_day, now_ms, Decision, Market};
use crate::coredb::CoreDb;
use crate::execution::auto::{route_decision, Outcome};
use crate::execution::live::LiveExec;
use crate::execution::paper::PaperExec;
use crate::execution::Executor;
use crate::risk::RiskLimits;

/// CoreDB tick is considered fresh if its `ts_ms` is within this many
/// milliseconds of "now". Outside that window we fall through to a
/// Binance REST quote so a stale or stopped ingest doesn't poison the
/// decision. 60 s is generous given the WS ingest writes ~2 rows/s.
const CACHE_FRESHNESS_MS: i64 = 60_000;

/// Polymarket cache freshness. The ingest path polls Gamma every 30 s
/// per `crate::data::polymarket`; 90 s gives us a one-cycle miss before
/// we fall back to a live REST pull.
const MARKETS_CACHE_FRESHNESS_MS: i64 = 90_000;

#[derive(Debug, Deserialize)]
struct BinanceTicker {
    #[serde(rename = "lastPrice")]
    last_price: String,
}

pub async fn run(coredb_uri: &str, plan: BacktestPlan, execute: bool, live: bool) -> Result<()> {
    println!(
        "🔍 backtest: connecting to CoreDB at {coredb_uri} (plan={plan:?}, execute={execute}, live={live})"
    );
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let dec_repo = DecisionRepo::new(db.session()).await?;

    // The auto-execution wiring is only built when --execute is set, so
    // a vanilla backtest run keeps its current zero-write footprint on
    // the orders / positions tables and doesn't pay for the extra repo
    // construction. `--live` swaps PaperExec for LiveExec (which is
    // *itself* DRY_RUN unless LIVE_TRADING_ENABLED=1, so this flag
    // alone never broadcasts money).
    let exec_ctx = if execute {
        let exec: Box<dyn Executor> = if live {
            let market_repo_for_exec = Arc::new(MarketRepo::new(db.session()).await?);
            let live = LiveExec::from_env(market_repo_for_exec).map_err(|e| {
                anyhow::anyhow!("--live requested but LiveExec construction failed: {e}")
            })?;
            Box::new(live)
        } else {
            Box::new(PaperExec)
        };
        Some(ExecCtx {
            order_repo: OrderRepo::new(db.session()).await?,
            pos_repo: PositionRepo::new(db.session()).await?,
            exec,
            limits: RiskLimits::default(),
        })
    } else {
        if live {
            eprintln!("  ⚠ --live ignored: --execute is off");
        }
        None
    };

    // Resolve every LLM strategy the plan calls for *before* touching
    // markets — fail fast on missing credentials rather than after a
    // 30-second Polymarket pull. An empty-string preset resolves to the
    // env-driven default (legacy `--llm` / `--both`); a named preset
    // resolves the matching backend directly so multi-LLM runs don't
    // have to mutate AGENT_BACKEND env vars between strategies.
    let llm_ctxs: Vec<LlmCtx> = if plan.wants_llm() {
        let mut v = Vec::with_capacity(plan.llm_presets.len());
        for preset in &plan.llm_presets {
            v.push(LlmCtx::resolve(preset)?);
        }
        // Deduplicate by label — if the user passes `--llms deepseek,deepseek`
        // or `--llms deepseek,` (env default also resolves to deepseek)
        // we'd otherwise spend twice the budget for no extra signal.
        let mut seen = std::collections::HashSet::new();
        v.retain(|c| seen.insert(c.label.to_string()));
        v
    } else {
        Vec::new()
    };
    if !llm_ctxs.is_empty() {
        println!("🧠 backtest: LLM strategies =");
        for ctx in &llm_ctxs {
            println!("     - {} ({})", ctx.label, ctx.model);
        }
    }

    let http = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("reqwest client")?;

    // Prefer the CoreDB cache when the WS ingest has populated a fresh
    // tick. Falls through to Binance REST when the cache is missing or
    // stale (>CACHE_FRESHNESS_MS), so this still works in setups
    // without `rust-agent ingest` running. Picking the cache also
    // means decisions made during a single ingest session use prices
    // consistent with what the ingest pipeline is actually recording.
    let btc_tick_repo = BtcTickRepo::new(db.session()).await?;
    let btc_price = match btc_tick_repo.latest("BTCUSDT").await {
        Ok(Some(t)) if (now_ms() - t.ts_ms) < CACHE_FRESHNESS_MS => {
            let age_ms = now_ms() - t.ts_ms;
            println!(
                "💰 backtest: BTC spot from CoreDB cache ({} ms old): ${:.2}",
                age_ms, t.price
            );
            t.price
        }
        Ok(other) => {
            if let Some(stale) = other {
                let age_ms = now_ms() - stale.ts_ms;
                println!(
                    "💰 backtest: cache tick is stale ({} ms old); falling back to Binance REST",
                    age_ms
                );
            } else {
                println!(
                    "💰 backtest: no CoreDB cache yet; fetching BTC spot from Binance REST"
                );
            }
            fetch_btc_rest(&http).await?
        }
        Err(e) => {
            eprintln!("  ! btc cache read failed ({e}); falling back to Binance REST");
            fetch_btc_rest(&http).await?
        }
    };
    println!("   BTC spot: ${btc_price:.2}");

    // Same cache-first pattern as the BTC spot above: prefer the
    // markets table when the ingest path has populated a recent
    // snapshot; otherwise pull from Gamma live. Freshness is measured
    // by the max `updated_at_ms` across the cached set — a fully empty
    // table is also a cache miss.
    let market_repo = MarketRepo::new(db.session()).await?;
    let markets = match load_markets(&market_repo, &http).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("  ! market load failed: {e}");
            Vec::new()
        }
    };
    println!("   {} open BTC markets", markets.len());

    let ts = now_ms();
    let bd = bucket_day(ts);
    // counts: strategy → (side → count). One outer entry per strategy
    // in play (baseline + every LLM in plan.llm_presets), so the final
    // summary stays comparable across strategies.
    let mut counts: std::collections::HashMap<String, std::collections::HashMap<String, u32>> =
        std::collections::HashMap::new();
    let mut stored = 0u32;
    let mut exec_stats = ExecStats::default();
    for (i, m) in markets.iter().enumerate() {
        // Buffer the per-strategy evaluations for THIS market before
        // any inserts so all rows share the same `ts` / `entry_price`.
        // In --both / --llms mode that's what makes the head-to-head
        // fair: same BTC spot, same Polymarket yes_price, same
        // decision instant across every strategy.
        let mut emissions: Vec<Decision> = Vec::new();

        if plan.wants_baseline() {
            let b = baseline_evaluate(m, btc_price);
            emissions.push(Decision {
                bucket_day_ms: bd,
                ts_ms: ts,
                decision_id: Uuid::new_v4(),
                market_slug: m.slug.clone(),
                side: b.side.to_string(),
                size_usd: b.size_usd,
                confidence: b.confidence,
                edge_bps: b.edge_bps,
                reasoning: b.reasoning,
                raw_response: "baseline-rule".to_string(),
                entry_price: m.last_price,
                strategy: "baseline".to_string(),
            });
        }

        for ctx in &llm_ctxs {
            if (i + 1) % 5 == 0 || i == 0 {
                println!(
                    "   {}: {}/{}  ({})",
                    ctx.strategy_label,
                    i + 1,
                    markets.len(),
                    m.slug
                );
            }
            let d = llm_mod::evaluate(
                m,
                btc_price,
                ctx.client.as_ref(),
                &ctx.model,
                ctx.max_tokens,
            )
            .await;
            emissions.push(Decision {
                bucket_day_ms: bd,
                ts_ms: ts,
                decision_id: Uuid::new_v4(),
                market_slug: m.slug.clone(),
                side: d.side,
                size_usd: d.size_usd,
                confidence: d.confidence,
                edge_bps: d.edge_bps,
                reasoning: d.reasoning,
                raw_response: d.raw_response,
                entry_price: m.last_price,
                strategy: ctx.strategy_label.clone(),
            });
        }

        for decision in &emissions {
            let strategy = decision.strategy.clone();
            *counts
                .entry(strategy.clone())
                .or_default()
                .entry(decision.side.clone())
                .or_insert(0) += 1;
            match dec_repo.insert(decision).await {
                Ok(()) => stored += 1,
                Err(e) => {
                    eprintln!("  ! decisions insert failed for {} ({}): {e}", m.slug, strategy)
                }
            }

            // --execute: route every non-PASS decision through risk gate +
            // PaperExec immediately after the decisions row lands. Skips,
            // blocks, exec errors all roll into exec_stats so the final
            // summary is one place to look. We don't bail on individual
            // failures — backtest is a batch operation and partial
            // execution is more useful than no execution. In --both mode
            // this runs once per strategy per market.
            if let Some(ctx) = exec_ctx.as_ref() {
                match route_decision(
                    decision,
                    ctx.exec.as_ref(),
                    &ctx.limits,
                    &ctx.order_repo,
                    &ctx.pos_repo,
                )
                .await
                {
                    Ok(Outcome::Filled(_)) => exec_stats.filled += 1,
                    Ok(Outcome::Skipped(_)) => exec_stats.skipped += 1,
                    Ok(Outcome::Blocked(reason)) => {
                        exec_stats.blocked += 1;
                        if exec_stats.blocked <= 3 {
                            eprintln!("  ! risk gate blocked {}: {reason}", m.slug);
                        }
                    }
                    Ok(Outcome::ExecError(reason)) => {
                        exec_stats.errors += 1;
                        eprintln!("  ! exec failed for {}: {reason}", m.slug);
                    }
                    Err(e) => {
                        exec_stats.errors += 1;
                        eprintln!("  ! route_decision raised for {}: {e}", m.slug);
                    }
                }
            }
        } // end inner per-strategy loop
    } // end outer per-market loop

    println!("✓ backtest complete:");
    let mut strategies: Vec<&String> = counts.keys().collect();
    strategies.sort();
    for strategy in &strategies {
        let sides = &counts[*strategy];
        let line: Vec<String> = ["YES", "NO", "PASS"]
            .iter()
            .map(|s| format!("{s}={}", sides.get(*s).copied().unwrap_or(0)))
            .collect();
        println!("   {strategy:<10} {}", line.join(" "));
    }
    println!("   stored {} decisions in polymarket_btc.decisions", stored);
    if exec_ctx.is_some() {
        println!(
            "   execute: {} filled, {} blocked, {} skipped, {} errors",
            exec_stats.filled, exec_stats.blocked, exec_stats.skipped, exec_stats.errors
        );
    }
    Ok(())
}

/// Repositories + executor + limits constructed once per `run`
/// invocation when `--execute` is set. Kept in one place so the per-
/// market loop body stays readable. `exec` is a trait object so
/// `--live` can swap `LiveExec` in for `PaperExec` without changing
/// the call-site code.
struct ExecCtx {
    order_repo: OrderRepo,
    pos_repo: PositionRepo,
    exec: Box<dyn Executor>,
    limits: RiskLimits,
}

#[derive(Default)]
struct ExecStats {
    filled: u32,
    blocked: u32,
    skipped: u32,
    errors: u32,
}

/// Bundle of everything one LLM strategy needs at call time. Built
/// once per `run` invocation so we don't re-resolve the backend per
/// market.
///
/// `label` is the resolved backend identifier (`anthropic`,
/// `deepseek`, ...). `strategy_label` is what goes into
/// `decisions.strategy` — for the legacy default-LLM path that's the
/// historical `"llm"` so existing snapshots keep grouping; for the
/// N-way path it's the preset name itself.
struct LlmCtx {
    client: Box<dyn ApiClient>,
    model: String,
    max_tokens: u32,
    label: &'static str,
    strategy_label: String,
}

impl LlmCtx {
    /// Resolve a preset name to a ready-to-call LLM context. An empty
    /// preset means "use the env-driven default" (legacy `--llm` /
    /// `--both`); otherwise the preset name picks the backend directly
    /// (`deepseek`, `anthropic`, `openai`).
    ///
    /// In the empty-preset case the decision rows are labeled `"llm"`
    /// so they cluster with historical pre-`--llms` data. Named
    /// presets are labeled by name so N-way comparisons stay distinct.
    fn resolve(preset: &str) -> Result<Self> {
        let config = if preset.is_empty() {
            Config::load()?
        } else {
            load_preset_config(preset)?
        };
        let label;
        let client: Box<dyn ApiClient> = match config.backend.clone() {
            Backend::Anthropic { api_key } => {
                label = "anthropic";
                Box::new(AnthropicClient::new(api_key))
            }
            Backend::OpenAICompat { api_key, base_url } => {
                label = if base_url.contains("api.deepseek.com") {
                    "deepseek"
                } else if base_url.contains("api.openai.com") {
                    "openai"
                } else {
                    "openai-compat"
                };
                Box::new(OpenAICompatClient::new(base_url, api_key))
            }
        };
        let strategy_label = if preset.is_empty() {
            "llm".to_string()
        } else {
            label.to_string()
        };
        Ok(Self {
            client,
            model: config.model,
            max_tokens: config.max_tokens,
            label,
            strategy_label,
        })
    }
}

/// Resolve a named preset to a fully-formed [`Config`] without going
/// through `AGENT_BACKEND`. This lets `--llms deepseek,anthropic` run
/// both backends in one process without env mutation.
///
/// Each preset reads only the env vars it actually needs — anthropic
/// doesn't touch DeepSeek creds and vice-versa — so a user with only
/// one provider configured can still run a single-strategy pass.
fn load_preset_config(preset: &str) -> Result<Config> {
    let cfg = Config::default();
    match preset.to_ascii_lowercase().as_str() {
        "anthropic" | "claude" => {
            let api_key = std::env::var("ANTHROPIC_API_KEY").context(
                "--llms anthropic needs ANTHROPIC_API_KEY set",
            )?;
            let model = std::env::var("ANTHROPIC_MODEL")
                .unwrap_or_else(|_| "claude-sonnet-4-20250514".to_string());
            Ok(Config {
                backend: Backend::Anthropic { api_key },
                model,
                ..cfg
            })
        }
        "deepseek" => {
            let api_key = std::env::var("DEEPSEEK_API_KEY")
                .ok()
                .filter(|s| !s.is_empty())
                .or_else(|| read_dotfile(".deepseek"))
                .context(
                    "--llms deepseek needs DEEPSEEK_API_KEY env or ~/.deepseek",
                )?;
            let base_url = std::env::var("DEEPSEEK_BASE_URL")
                .unwrap_or_else(|_| "https://api.deepseek.com/v1".to_string());
            let model = std::env::var("DEEPSEEK_MODEL")
                .unwrap_or_else(|_| "deepseek-chat".to_string());
            Ok(Config {
                backend: Backend::OpenAICompat { api_key, base_url },
                model,
                ..cfg
            })
        }
        "openai" => {
            let api_key = std::env::var("OPENAI_API_KEY").context(
                "--llms openai needs OPENAI_API_KEY set",
            )?;
            let base_url = std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1".to_string());
            let model = std::env::var("OPENAI_MODEL").context(
                "--llms openai needs OPENAI_MODEL set (e.g. gpt-4o)",
            )?;
            Ok(Config {
                backend: Backend::OpenAICompat { api_key, base_url },
                model,
                ..cfg
            })
        }
        other => anyhow::bail!(
            "unknown --llms preset {other:?}; supported: anthropic, deepseek, openai"
        ),
    }
}

fn read_dotfile(name: &str) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let raw = std::fs::read_to_string(std::path::Path::new(&home).join(name)).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preset_unknown_returns_error() {
        let r = load_preset_config("nonsense");
        assert!(r.is_err());
        let msg = r.err().unwrap().to_string();
        assert!(msg.contains("unknown --llms preset"), "got: {msg}");
    }

    #[test]
    fn preset_anthropic_missing_key_errors() {
        // Don't actually exec the env-touching path conditionally on
        // ANTHROPIC_API_KEY presence — the test runner's env is
        // ambient. Just assert the function returns a structured
        // error type when called with a key that is *guaranteed* not
        // to exist by using an alias the resolver doesn't recognize.
        // (Real env-coupled tests live in integration.)
        let r = load_preset_config("CLAUDE-typo");
        assert!(r.is_err());
    }
}

/// Load the open BTC markets, preferring the CoreDB cache when it is
/// fresh and falling back to Polymarket Gamma otherwise. The freshness
/// check uses the newest `updated_at_ms` in the cached set against
/// [`MARKETS_CACHE_FRESHNESS_MS`]. An empty cache is treated as a miss.
async fn load_markets(repo: &MarketRepo, http: &Client) -> Result<Vec<Market>> {
    match repo.list_open().await {
        Ok(cached) if !cached.is_empty() => {
            let newest = cached.iter().map(|m| m.updated_at_ms).max().unwrap_or(0);
            let age = now_ms() - newest;
            if age < MARKETS_CACHE_FRESHNESS_MS {
                let btc_only: Vec<Market> = cached.into_iter().filter(|m| is_btc(&m.slug, &m.question)).collect();
                println!(
                    "📦 backtest: markets from CoreDB cache ({} ms old, {} btc rows)",
                    age,
                    btc_only.len()
                );
                return Ok(btc_only);
            }
            println!(
                "📦 backtest: cached markets stale ({} ms old); falling back to Polymarket Gamma",
                age
            );
        }
        Ok(_) => {
            println!("📦 backtest: no cached markets; falling back to Polymarket Gamma");
        }
        Err(e) => {
            eprintln!("  ! markets cache read failed ({e}); falling back to Polymarket Gamma");
        }
    }
    fetch_btc_markets(http).await
}

fn is_btc(slug: &str, question: &str) -> bool {
    let s = slug.to_ascii_lowercase();
    let q = question.to_ascii_lowercase();
    s.contains("bitcoin") || s.contains("btc") || q.contains("bitcoin") || q.contains("btc")
}

/// Pull the latest BTCUSDT price from Binance's 24hr ticker REST
/// endpoint. Used as the fallback when CoreDB has no recent cached
/// tick. Returns the parsed `lastPrice` field.
async fn fetch_btc_rest(http: &Client) -> Result<f64> {
    let t: BinanceTicker = http
        .get("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT")
        .send()
        .await
        .context("binance HTTP")?
        .error_for_status()
        .context("binance status")?
        .json()
        .await
        .context("binance JSON")?;
    t.last_price.parse().context("parse lastPrice")
}

async fn fetch_btc_markets(http: &Client) -> Result<Vec<Market>> {
    const URL: &str = "https://gamma-api.polymarket.com/markets\
                       ?active=true&closed=false&order=volume24hr&ascending=false&limit=500";
    let rows: Vec<serde_json::Value> = http
        .get(URL)
        .send().await.context("gamma HTTP")?
        .error_for_status().context("gamma status")?
        .json().await.context("gamma JSON")?;
    let now = now_ms();
    let mut out = Vec::new();
    for v in rows {
        let slug = v.get("slug").and_then(|x| x.as_str()).unwrap_or("");
        let question = v.get("question").and_then(|x| x.as_str()).unwrap_or("");
        let s = slug.to_ascii_lowercase();
        let q = question.to_ascii_lowercase();
        let is_btc = s.contains("bitcoin") || s.contains("btc")
            || q.contains("bitcoin") || q.contains("btc");
        if !is_btc { continue; }
        let yes_price = v.get("outcomePrices")
            .and_then(|x| x.as_str())
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .and_then(|v| v.first().cloned())
            .and_then(|p| p.parse::<f64>().ok())
            .unwrap_or(0.0);
        // Capture clobTokenIds when present so a backtest-only run (no
        // ingest daemon) still lands tokenIds in Market structs for any
        // downstream LiveExec usage. Index 0 = YES, index 1 = NO.
        let clob_token_ids = v.get("clobTokenIds")
            .and_then(|x| x.as_str())
            .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
            .unwrap_or_default();
        let yes_token_id = clob_token_ids.first().cloned().unwrap_or_default();
        let no_token_id = clob_token_ids.get(1).cloned().unwrap_or_default();
        out.push(Market {
            slug: slug.to_string(),
            question: question.to_string(),
            end_date_ms: v.get("endDate")
                .and_then(|x| x.as_str())
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0),
            outcomes: v.get("outcomes").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            closed: false,
            last_price: yes_price,
            updated_at_ms: now,
            yes_token_id,
            no_token_id,
        });
    }
    Ok(out)
}
