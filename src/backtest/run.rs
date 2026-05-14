//! Backtest orchestrator.
//!
//! CoreDB's SELECT responses currently advertise every column as
//! `Text`, which trips the scylla driver's typed-row deserializer for
//! TIMESTAMP / DOUBLE / BOOLEAN columns. Until that's smoothed over,
//! the read path here goes straight to Polymarket Gamma so the
//! backtest can actually run. CoreDB stays the write target — every
//! produced decision still lands in `polymarket_btc.decisions`.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use uuid::Uuid;

use crate::api::{AnthropicClient, ApiClient, OpenAICompatClient};
use crate::backtest::{baseline::evaluate as baseline_evaluate, llm as llm_mod, BacktestMode};
use crate::config::{Backend, Config};
use crate::coredb::decisions::DecisionRepo;
use crate::coredb::types::{bucket_day, now_ms, Decision, Market};
use crate::coredb::CoreDb;

#[derive(Debug, Deserialize)]
struct BinanceTicker {
    #[serde(rename = "lastPrice")]
    last_price: String,
}

pub async fn run(coredb_uri: &str, mode: BacktestMode) -> Result<()> {
    println!("🔍 backtest: connecting to CoreDB at {coredb_uri} (mode={mode:?})");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let dec_repo = DecisionRepo::new(db.session()).await?;

    // Build the LLM client only when we'll actually use it — Config::load
    // touches the env (or ~/.deepseek), and we don't want to demand any
    // credentials for the baseline-only path.
    let llm_ctx = match mode {
        BacktestMode::Baseline => None,
        BacktestMode::Llm => Some(LlmCtx::from_config(Config::load()?)?),
    };
    if let Some(ctx) = &llm_ctx {
        println!("🧠 backtest: LLM = {} ({})", ctx.label, ctx.model);
    }

    let http = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("reqwest client")?;

    println!("💰 backtest: fetching BTC spot from Binance");
    let t: BinanceTicker = http
        .get("https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT")
        .send().await.context("binance HTTP")?
        .error_for_status().context("binance status")?
        .json().await.context("binance JSON")?;
    let btc_price: f64 = t.last_price.parse().context("parse lastPrice")?;
    println!("   BTC spot: ${btc_price:.2}");

    println!("📦 backtest: pulling open BTC markets from Polymarket Gamma");
    let markets = fetch_btc_markets(&http).await?;
    println!("   {} open BTC markets", markets.len());

    let ts = now_ms();
    let bd = bucket_day(ts);
    let mut counts = std::collections::HashMap::<String, u32>::new();
    let mut stored = 0u32;
    for (i, m) in markets.iter().enumerate() {
        let (side, size_usd, confidence, edge_bps, reasoning, raw_response) =
            match (&llm_ctx, mode) {
                (Some(ctx), BacktestMode::Llm) => {
                    if (i + 1) % 5 == 0 || i == 0 {
                        println!("   llm: {}/{}  ({})", i + 1, markets.len(), m.slug);
                    }
                    let d = llm_mod::evaluate(
                        m,
                        btc_price,
                        ctx.client.as_ref(),
                        &ctx.model,
                        ctx.max_tokens,
                    )
                    .await;
                    (
                        d.side,
                        d.size_usd,
                        d.confidence,
                        d.edge_bps,
                        d.reasoning,
                        d.raw_response,
                    )
                }
                _ => {
                    let b = baseline_evaluate(m, btc_price);
                    (
                        b.side.to_string(),
                        b.size_usd,
                        b.confidence,
                        b.edge_bps,
                        b.reasoning,
                        "baseline-rule".to_string(),
                    )
                }
            };
        *counts.entry(side.clone()).or_insert(0) += 1;
        let decision = Decision {
            bucket_day_ms: bd,
            ts_ms: ts,
            decision_id: Uuid::new_v4(),
            market_slug: m.slug.clone(),
            side,
            size_usd,
            confidence,
            edge_bps,
            reasoning,
            raw_response,
            // YES price at decision time. The mark-to-market PnL comparator
            // needs an entry price; capturing it here avoids a separate
            // price-history table.
            entry_price: m.last_price,
        };
        match dec_repo.insert(&decision).await {
            Ok(()) => stored += 1,
            Err(e) => eprintln!("  ! decisions insert failed for {}: {e}", m.slug),
        }
    }
    println!("✓ backtest complete:");
    for side in ["YES", "NO", "PASS"] {
        println!("   {side:<5} {}", counts.get(side).copied().unwrap_or(0));
    }
    println!("   stored {} decisions in polymarket_btc.decisions", stored);
    Ok(())
}

/// Bundle of everything the LLM path needs at call time. Built once
/// per `run` invocation so we don't re-resolve the backend per market.
struct LlmCtx {
    client: Box<dyn ApiClient>,
    model: String,
    max_tokens: u32,
    label: &'static str,
}

impl LlmCtx {
    fn from_config(config: Config) -> Result<Self> {
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
        Ok(Self {
            client,
            model: config.model,
            max_tokens: config.max_tokens,
            label,
        })
    }
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
        });
    }
    Ok(out)
}
