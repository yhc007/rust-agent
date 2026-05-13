//! Binance REST poller for BTCUSDT bookTicker.
//!
//! We deliberately use REST over WS for v1: the polling cadence
//! (~1s) is short enough for the kind of single-name short-term signals
//! the agent reasons over, and REST avoids the reconnect/sequence-gap
//! plumbing a WS implementation would need. Swap to
//! `tokio-tungstenite` + `wss://stream.binance.com:9443/ws/btcusdt@bookTicker`
//! when sub-100ms latency matters.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use crate::coredb::btc::BtcTickRepo;
use crate::coredb::types::{bucket_hour, now_ms, BtcTick};

const BINANCE_URL: &str = "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT";

/// Binance bookTicker REST shape. `priceChange`/`volume` aren't returned
/// by this endpoint so we read those from `/ticker/24hr` separately when
/// the agent needs them — the ingestion path only needs bid/ask.
#[derive(Debug, Deserialize)]
struct BookTicker {
    #[serde(rename = "symbol")]
    _symbol: String,
    #[serde(rename = "bidPrice")]
    bid_price: String,
    #[serde(rename = "askPrice")]
    ask_price: String,
}

pub async fn run(
    repo: Arc<BtcTickRepo>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let http = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("binance reqwest client")?;
    let mut tick = interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(e) = poll_once(&http, &repo).await {
                    warn!("binance poll failed: {e}");
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("binance ingest: shutdown signal received");
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn poll_once(http: &Client, repo: &BtcTickRepo) -> Result<()> {
    let r: BookTicker = http
        .get(BINANCE_URL)
        .send()
        .await
        .context("binance HTTP send")?
        .error_for_status()
        .context("binance HTTP status")?
        .json()
        .await
        .context("binance HTTP json")?;
    let bid: f64 = r.bid_price.parse().context("bidPrice parse")?;
    let ask: f64 = r.ask_price.parse().context("askPrice parse")?;
    let price = (bid + ask) / 2.0;
    let ts = now_ms();
    let tick = BtcTick {
        bucket_hour_ms: bucket_hour(ts),
        symbol: "BTCUSDT".to_string(),
        ts_ms: ts,
        price,
        volume: 0.0,
        bid,
        ask,
    };
    repo.insert(&tick).await?;
    info!("binance tick {price:.2} bid={bid:.2} ask={ask:.2}");
    Ok(())
}
