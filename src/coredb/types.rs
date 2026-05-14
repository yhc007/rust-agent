//! Domain types shared by every CoreDB repository.
//!
//! Timestamps are kept as `i64` milliseconds since unix epoch. CoreDB's
//! TIMESTAMP type maps cleanly to that, and avoiding chrono in the row
//! shape keeps deserialization deps minimal. Conversion helpers live at
//! the bottom of the file.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type Millis = i64;

/// A single BTC price tick from any spot venue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BtcTick {
    pub bucket_hour_ms: Millis,
    pub symbol: String,
    pub ts_ms: Millis,
    pub price: f64,
    pub volume: f64,
    pub bid: f64,
    pub ask: f64,
}

/// Snapshot of a Polymarket orderbook at a moment in time.
/// The `*_bids` / `*_asks` fields are JSON-encoded `[[price, size], ...]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub bucket_hour_ms: Millis,
    pub market_slug: String,
    pub ts_ms: Millis,
    pub yes_bids: String,
    pub yes_asks: String,
    pub no_bids: String,
    pub no_asks: String,
}

/// Polymarket market metadata. `outcomes` is JSON like `["Yes","No"]`.
/// `yes_token_id` / `no_token_id` are decimal-string ERC-1155 token ids
/// from Polymarket's `clobTokenIds` (Gamma `clobTokenIds` is the
/// canonical source; positions[0] = YES, positions[1] = NO). They're
/// strings because the values are 256-bit and don't fit in i64.
/// Pre-schema rows (or markets where Gamma didn't return the field)
/// read back as the empty string and should be treated as "not yet
/// known" by anything depending on them (e.g. LiveExec).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub slug: String,
    pub question: String,
    pub end_date_ms: Millis,
    pub outcomes: String,
    pub closed: bool,
    pub last_price: f64,
    pub updated_at_ms: Millis,
    pub yes_token_id: String,
    pub no_token_id: String,
}

/// LLM decision record. `entry_price` is the market's YES price at the
/// instant the decision was made; persisted so a later PnL comparison
/// can mark-to-market without depending on a separate price timeseries.
/// Decisions written before the schema column existed will read back
/// as `0.0` and should be excluded from comparisons.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub bucket_day_ms: Millis,
    pub ts_ms: Millis,
    pub decision_id: Uuid,
    pub market_slug: String,
    pub side: String,
    pub size_usd: f64,
    pub confidence: f64,
    pub edge_bps: i32,
    pub reasoning: String,
    pub raw_response: String,
    pub entry_price: f64,
}

/// Order lifecycle record. `status` is `pending|filled|canceled|partial`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub bucket_day_ms: Millis,
    pub ts_ms: Millis,
    pub order_id: String,
    pub decision_id: Uuid,
    pub market_slug: String,
    pub side: String,
    pub size: f64,
    pub price: f64,
    pub status: String,
    pub fill_size: f64,
    pub fill_price: f64,
}

/// Current position per market. Overwritten on each update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub market_slug: String,
    pub side: String,
    pub size: f64,
    pub avg_price: f64,
    pub updated_at_ms: Millis,
}

/// One mark-to-market snapshot for a single strategy, captured during a
/// `compare-pnl` run. Rows accumulate over time so the time series can
/// be reconstructed even after the server restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyPnlSnapshot {
    pub bucket_day_ms: Millis,
    pub ts_ms: Millis,
    pub strategy: String, // "baseline" | "llm"
    pub n_decisions: i32,
    pub sum_size_usd: f64,
    pub sum_pnl: f64,
    pub n_yes: i32,
    pub n_no: i32,
    pub n_pass: i32,
}

/// Daily PnL roll-up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlDaily {
    pub day_ms: Millis,
    pub realized: f64,
    pub unrealized: f64,
    pub n_trades: i32,
    pub llm_cost_usd: f64,
}

/// Truncate a unix-ms timestamp to the start of its UTC hour.
pub fn bucket_hour(ts_ms: Millis) -> Millis {
    let h = 3_600_000;
    (ts_ms / h) * h
}

/// Truncate a unix-ms timestamp to the start of its UTC day.
pub fn bucket_day(ts_ms: Millis) -> Millis {
    let d = 86_400_000;
    (ts_ms / d) * d
}

/// Current unix-ms timestamp.
pub fn now_ms() -> Millis {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
