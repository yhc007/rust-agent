//! Polymarket Gamma poller for Bitcoin-tagged markets.
//!
//! Reads `/markets?tag_id=620&active=true&closed=false&limit=100`
//! every 30s and upserts each row into the `markets` table.
//! `outcomePrices` is a JSON-encoded `["yes_price", "no_price"]`, so we
//! store the parsed YES price into `last_price` for quick reads.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::DateTime;
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use crate::coredb::markets::MarketRepo;
use crate::coredb::types::{now_ms, Market};

// Gamma's tag parameters are inconsistent across versions
// (`tag_id=620` returns nothing, `tag=Bitcoin` mixes in unrelated
// markets that mention "before GTA VI" etc.). Pulling the active set
// sorted by 24h volume and filtering client-side on slug/question is
// noisier on the wire but far more reliable.
const GAMMA_URL: &str =
    "https://gamma-api.polymarket.com/markets\
     ?active=true&closed=false&order=volume24hr&ascending=false&limit=500";

fn is_btc(slug: &str, question: &str) -> bool {
    let s = slug.to_ascii_lowercase();
    let q = question.to_ascii_lowercase();
    let hit = |t: &str| s.contains(t) || q.contains(t);
    hit("bitcoin") || hit("btc")
}

/// Subset of fields we need from Gamma. The full payload has ~40 keys;
/// keeping our struct narrow lets us evolve the API without panicking
/// when Gamma adds new fields.
#[derive(Debug, Deserialize)]
struct GammaMarket {
    slug: String,
    question: String,
    #[serde(default)]
    end_date: Option<String>,
    #[serde(default)]
    outcomes: Option<String>,
    #[serde(default)]
    outcome_prices: Option<String>,
    #[serde(default)]
    closed: bool,
    /// Gamma serialises this as a stringified JSON array of decimal
    /// ERC-1155 token ids, e.g. `["1234…", "9876…"]`. Index 0 = YES,
    /// index 1 = NO. Required for the live executor; paper code can
    /// keep working without it.
    #[serde(default)]
    clob_token_ids: Option<String>,
}

pub async fn run(
    repo: Arc<MarketRepo>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let http = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .context("polymarket reqwest client")?;
    let mut tick = interval(Duration::from_secs(30));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                match poll_once(&http, &repo).await {
                    Ok(n) => info!("polymarket: upserted {n} BTC markets"),
                    Err(e) => warn!("polymarket poll failed: {e}"),
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("polymarket ingest: shutdown signal received");
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn poll_once(http: &Client, repo: &MarketRepo) -> Result<usize> {
    // Gamma sometimes returns the field as snake_case JSON despite
    // Pascal-tagged docs; serde renames keep us forward-compatible.
    let raw: Vec<serde_json::Value> = http
        .get(GAMMA_URL)
        .send()
        .await
        .context("gamma HTTP send")?
        .error_for_status()
        .context("gamma HTTP status")?
        .json()
        .await
        .context("gamma HTTP json")?;

    let now = now_ms();
    let mut n = 0usize;
    for v in raw {
        // Gamma uses camelCase keys (`endDate`, `outcomePrices`).
        // Re-bind through serde_json::from_value so missing fields
        // become None without aborting the whole batch.
        let m = GammaMarket {
            slug: v.get("slug").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            question: v.get("question").and_then(|x| x.as_str()).unwrap_or("").to_string(),
            end_date: v.get("endDate").and_then(|x| x.as_str()).map(str::to_string),
            outcomes: v.get("outcomes").and_then(|x| x.as_str()).map(str::to_string),
            outcome_prices: v
                .get("outcomePrices")
                .and_then(|x| x.as_str())
                .map(str::to_string),
            closed: v.get("closed").and_then(|x| x.as_bool()).unwrap_or(false),
            clob_token_ids: v
                .get("clobTokenIds")
                .and_then(|x| x.as_str())
                .map(str::to_string),
        };
        if m.slug.is_empty() {
            continue;
        }
        if !is_btc(&m.slug, &m.question) {
            continue;
        }
        let (yes_token_id, no_token_id) = parse_clob_token_ids(m.clob_token_ids.as_deref());
        let market = Market {
            slug: m.slug,
            question: m.question,
            end_date_ms: m
                .end_date
                .as_deref()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0),
            outcomes: m.outcomes.unwrap_or_default(),
            closed: m.closed,
            last_price: parse_yes_price(m.outcome_prices.as_deref()),
            updated_at_ms: now,
            yes_token_id,
            no_token_id,
        };
        if let Err(e) = repo.upsert(&market).await {
            warn!("polymarket upsert {} failed: {e}", market.slug);
            continue;
        }
        n += 1;
    }
    Ok(n)
}

fn parse_yes_price(prices_json: Option<&str>) -> f64 {
    let Some(s) = prices_json else { return 0.0 };
    let parsed: Result<Vec<String>, _> = serde_json::from_str(s);
    parsed
        .ok()
        .and_then(|v| v.first().cloned())
        .and_then(|p| p.parse().ok())
        .unwrap_or(0.0)
}

/// Parse Gamma's `clobTokenIds` (a stringified JSON array of decimal
/// token-id strings) into `(yes, no)`. Missing / malformed / wrong-
/// length payloads return `("".into(), "".into())`; LiveExec treats
/// empty strings as "not yet known" and refuses to sign.
fn parse_clob_token_ids(s: Option<&str>) -> (String, String) {
    let Some(s) = s else { return (String::new(), String::new()) };
    let parsed: Result<Vec<String>, _> = serde_json::from_str(s);
    match parsed {
        Ok(v) if v.len() >= 2 => (v[0].clone(), v[1].clone()),
        _ => (String::new(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_clob_token_ids() {
        let s = r#"["1234567890", "9876543210"]"#;
        let (yes, no) = parse_clob_token_ids(Some(s));
        assert_eq!(yes, "1234567890");
        assert_eq!(no, "9876543210");
    }

    #[test]
    fn missing_clob_token_ids_returns_empty() {
        assert_eq!(parse_clob_token_ids(None), (String::new(), String::new()));
        assert_eq!(
            parse_clob_token_ids(Some("not json")),
            (String::new(), String::new())
        );
        // Wrong length (one-element array) → empty.
        assert_eq!(
            parse_clob_token_ids(Some(r#"["only-yes"]"#)),
            (String::new(), String::new())
        );
    }
}
