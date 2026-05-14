//! Binance bookTicker WebSocket ingest.
//!
//! Subscribes to `wss://stream.binance.com:9443/ws/btcusdt@bookTicker`
//! and streams every top-of-book update into CoreDB. Binance pushes
//! one frame per orderbook change at the top of book (~10-100/s for
//! BTCUSDT), which is more density than CoreDB or the agent loop need;
//! the CoreDB writer is throttled to one row every `WRITE_THROTTLE`
//! so storage cost stays linear in time, not in market activity. The
//! WS read itself stays unthrottled — that's where the latency win
//! lives, and any in-process consumer can pull the freshest snapshot
//! via a future feed handle without paying CoreDB roundtrip per tick.
//!
//! Reconnect: if the WS drops mid-session we reconnect with capped
//! exponential backoff (1s → 2s → … → 30s). Shutdown via the watch
//! channel from `crate::data::ingest` is honoured in both the connect
//! wait and the read loop.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::coredb::btc::BtcTickRepo;
use crate::coredb::types::{bucket_hour, now_ms, BtcTick};

const WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker";
const WRITE_THROTTLE: Duration = Duration::from_millis(500);
const RECONNECT_INITIAL: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// Binance `bookTicker` stream frame. Only the fields we need are
/// deserialised; Binance also sends `u` (updateId) and the qty fields
/// which the ingest path doesn't use.
#[derive(Debug, Deserialize)]
struct BookTickerFrame {
    #[serde(rename = "s")]
    _symbol: String,
    #[serde(rename = "b")]
    bid_price: String,
    #[serde(rename = "a")]
    ask_price: String,
}

pub async fn run(repo: Arc<BtcTickRepo>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    info!("binance ws: connecting to {WS_URL}");
    let mut backoff = RECONNECT_INITIAL;
    loop {
        // Bail before connecting if shutdown is already requested —
        // happens when ingest::run sees Ctrl+C before this task gets
        // its first chance to schedule.
        if *shutdown.borrow() {
            info!("binance ws: shutdown before connect");
            return Ok(());
        }

        match run_one_session(repo.clone(), &mut shutdown).await {
            Ok(()) => {
                // Clean shutdown (signal arrived) — exit the reconnect
                // loop too.
                return Ok(());
            }
            Err(e) => {
                warn!("binance ws: session ended ({e}); reconnecting in {:?}", backoff);
            }
        }

        // Sleep with shutdown awareness so a Ctrl+C during the back-off
        // window doesn't have to wait the full delay.
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("binance ws: shutdown during reconnect backoff");
                    return Ok(());
                }
            }
        }
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

/// Run one WS session until the stream ends, the shutdown signal
/// fires, or a transport-level error occurs. Returns `Ok(())` on
/// graceful shutdown, `Err(_)` for anything the outer reconnect loop
/// should retry.
async fn run_one_session(
    repo: Arc<BtcTickRepo>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let (ws_stream, _resp) = connect_async(WS_URL)
        .await
        .context("connect_async")?;
    let (_write, mut read) = ws_stream.split();
    info!("binance ws: connected");

    let mut last_write = Instant::now() - WRITE_THROTTLE;
    let mut n_frames = 0u64;
    let mut n_writes = 0u64;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(
                        "binance ws: shutdown signal received ({} frames, {} writes)",
                        n_frames, n_writes
                    );
                    return Ok(());
                }
            }
            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(anyhow!("ws read error: {e}")),
                    None => return Err(anyhow!("ws stream ended")),
                };
                match msg {
                    Message::Text(text) => {
                        n_frames += 1;
                        let frame: BookTickerFrame = match serde_json::from_str(&text) {
                            Ok(f) => f,
                            Err(e) => {
                                debug!("binance ws: skip non-bookTicker frame ({e}): {text}");
                                continue;
                            }
                        };
                        // Drop frames between throttle windows — the WS
                        // gives us many more updates than the agent or
                        // CoreDB benefit from. The latest bid/ask is
                        // what matters at decision time, and any
                        // intra-window movement reflects pre-trade jitter
                        // we'd want to smooth anyway.
                        if last_write.elapsed() < WRITE_THROTTLE {
                            continue;
                        }
                        let bid: f64 = match frame.bid_price.parse() {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("binance ws: bid parse failed ({e}): {}", frame.bid_price);
                                continue;
                            }
                        };
                        let ask: f64 = match frame.ask_price.parse() {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("binance ws: ask parse failed ({e}): {}", frame.ask_price);
                                continue;
                            }
                        };
                        let ts = now_ms();
                        let tick = BtcTick {
                            bucket_hour_ms: bucket_hour(ts),
                            symbol: "BTCUSDT".to_string(),
                            ts_ms: ts,
                            price: (bid + ask) / 2.0,
                            volume: 0.0,
                            bid,
                            ask,
                        };
                        if let Err(e) = repo.insert(&tick).await {
                            warn!("binance ws: coredb insert failed: {e}");
                        } else {
                            n_writes += 1;
                            last_write = Instant::now();
                        }
                    }
                    Message::Ping(payload) => {
                        // tungstenite handles pong automatically when the
                        // sink is polled. We don't write back on this
                        // half; the lib pongs on the next write/poll.
                        debug!("binance ws: ping ({} bytes)", payload.len());
                    }
                    Message::Close(frame) => {
                        return Err(anyhow!(
                            "ws closed by peer: {:?}",
                            frame.map(|f| f.reason.to_string())
                        ));
                    }
                    other => {
                        debug!("binance ws: ignoring {:?}", other);
                    }
                }
            }
        }
    }
}
