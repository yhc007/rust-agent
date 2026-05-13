//! Data ingestion daemons that feed CoreDB.
//!
//! Two REST pollers run in parallel via tokio::spawn:
//! - `binance` — BTCUSDT best bid/ask every ~1s into `btc_ticks`.
//! - `polymarket` — Bitcoin-tagged markets every ~30s into `markets`.
//!
//! Both share the same `CoreDb` via Arc-wrapped repos. WebSocket-based
//! ingestion can replace REST polling later without touching the
//! repository layer.

pub mod binance;
pub mod ingest;
pub mod polymarket;
