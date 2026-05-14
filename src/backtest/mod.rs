//! Single-pass batch decision runner ("backtest" in name only at the
//! moment — a real time-replay backtest needs months of orderbook
//! history we don't have yet).
//!
//! Snapshots every open BTC market in CoreDB and produces one Decision
//! per market using either the deterministic [`baseline`] rule or the
//! [`llm`] path (DeepSeek by default). Both paths land their rows in
//! `polymarket_btc.decisions` so the two strategies can be compared
//! side-by-side on real captured data.

pub mod baseline;
pub mod compare;
pub mod history;
pub mod llm;
pub mod run;
pub mod settle;

/// Which decision engine [`run::run`] should use. Selected by the CLI:
/// default → `Baseline`, `--llm` → `Llm`, `--both` → `Both`.
///
/// `Both` is the timing-honest comparison mode: each market gets two
/// `Decision` rows written back-to-back with the same `entry_price`
/// and `ts_ms`, so a downstream `compare-pnl` or `settle-pnl` is
/// looking at two strategies seeing identical market state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestMode {
    Baseline,
    Llm,
    Both,
}

impl BacktestMode {
    pub fn wants_baseline(self) -> bool {
        matches!(self, BacktestMode::Baseline | BacktestMode::Both)
    }
    pub fn wants_llm(self) -> bool {
        matches!(self, BacktestMode::Llm | BacktestMode::Both)
    }
}
