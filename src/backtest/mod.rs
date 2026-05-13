//! Single-pass batch decision runner ("backtest" in name only at the
//! moment — a real time-replay backtest needs months of orderbook
//! history we don't have yet).
//!
//! Snapshots every open BTC market in CoreDB, runs a deterministic
//! baseline rule against the current Binance spot price, and records
//! one Decision per market. Future Phase-3.5: swap the baseline for a
//! DeepSeek call and compare PnL.

pub mod baseline;
pub mod run;
