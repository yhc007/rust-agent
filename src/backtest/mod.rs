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
pub mod ledger;
pub mod llm;
pub mod run;

/// Which decision engine [`run::run`] should use. Selected by the CLI's
/// `--llm` flag (or `--baseline`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BacktestMode {
    Baseline,
    Llm,
}
