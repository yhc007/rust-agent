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
///
/// `strategy` is the explicit label ("baseline", "deepseek", "anthropic",
/// ...). Rows written before that column existed read back as empty;
/// use [`Decision::effective_strategy`] to get a sensible label that
/// falls back to the legacy `raw_response == "baseline-rule"` inference.
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
    pub strategy: String,
}

impl Decision {
    /// Strategy label for grouping / aggregation. Prefers the explicit
    /// `strategy` column; on pre-schema rows where it's empty, falls
    /// back to the historical `raw_response == "baseline-rule"`
    /// inference so old data keeps comparing correctly.
    pub fn effective_strategy(&self) -> &str {
        if !self.strategy.is_empty() {
            &self.strategy
        } else if self.raw_response == "baseline-rule" {
            "baseline"
        } else {
            "llm"
        }
    }
}

#[cfg(test)]
mod agreement_snapshot_tests {
    use super::*;

    fn a(shared: i32, matches: i32) -> AgreementSnapshot {
        AgreementSnapshot {
            bucket_day_ms: 0,
            ts_ms: 0,
            strategy_a: "a".into(),
            strategy_b: "b".into(),
            shared,
            matches,
        }
    }

    #[test]
    fn rate_zero_when_no_shared_markets() {
        assert_eq!(a(0, 0).rate(), 0.0);
        // matches > shared shouldn't happen in practice, but rate
        // should still return *something* without panicking.
        assert_eq!(a(0, 5).rate(), 0.0);
    }

    #[test]
    fn rate_is_matches_over_shared() {
        assert!((a(10, 7).rate() - 0.7).abs() < 1e-9);
        assert_eq!(a(4, 4).rate(), 1.0);
        assert_eq!(a(4, 0).rate(), 0.0);
    }
}

#[cfg(test)]
mod decision_tests {
    use super::*;

    fn d(strategy: &str, raw: &str) -> Decision {
        Decision {
            bucket_day_ms: 0,
            ts_ms: 0,
            decision_id: Uuid::nil(),
            market_slug: String::new(),
            side: "PASS".into(),
            size_usd: 0.0,
            confidence: 0.0,
            edge_bps: 0,
            reasoning: String::new(),
            raw_response: raw.into(),
            entry_price: 0.0,
            strategy: strategy.into(),
        }
    }

    #[test]
    fn effective_strategy_prefers_explicit_column() {
        assert_eq!(d("deepseek", "baseline-rule").effective_strategy(), "deepseek");
        assert_eq!(d("anthropic", "...llm json...").effective_strategy(), "anthropic");
    }

    #[test]
    fn effective_strategy_legacy_baseline_inference() {
        assert_eq!(d("", "baseline-rule").effective_strategy(), "baseline");
    }

    #[test]
    fn effective_strategy_legacy_llm_inference() {
        assert_eq!(d("", "{\"side\":\"YES\"}").effective_strategy(), "llm");
        assert_eq!(d("", "").effective_strategy(), "llm");
    }
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

/// One ordered-pair agreement cell from a `compare-pnl` matrix run.
/// Stored as N×(N-1) rows per snapshot ts (no diagonal). Symmetric
/// on the wire because the read path doesn't have to know that —
/// each row carries its full (strategy_a, strategy_b) tuple and
/// readers can pivot as needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgreementSnapshot {
    pub bucket_day_ms: Millis,
    pub ts_ms: Millis,
    pub strategy_a: String,
    pub strategy_b: String,
    /// Markets where both strategies emitted a decision (latest per
    /// pair, matching the matrix builder's semantics).
    pub shared: i32,
    /// Subset of `shared` where both picked the same side (PASS
    /// included — see compare-pnl matrix docstring).
    pub matches: i32,
}

impl AgreementSnapshot {
    /// matches / shared as a [0.0, 1.0] rate; 0.0 when shared is 0
    /// (degenerate but the only sensible default for sparkline math).
    pub fn rate(&self) -> f64 {
        if self.shared == 0 {
            0.0
        } else {
            self.matches as f64 / self.shared as f64
        }
    }
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
