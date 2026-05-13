//! Agent decision repository (inline-CQL flavour).

use std::sync::Arc;

use scylla::Session;

use super::error::CoreDbError;
use super::types::{Decision, Millis};
use super::util::{esc, fuuid};

pub struct DecisionRepo {
    session: Arc<Session>,
}

impl DecisionRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn insert(&self, d: &Decision) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.decisions \
             (bucket_day, ts, decision_id, market_slug, side, size_usd, confidence, \
              edge_bps, reasoning, raw_response, entry_price) \
             VALUES ({bd}, {ts}, {id}, {slug}, {side}, {size}, {conf}, {edge}, {reasoning}, {raw}, {entry})",
            bd = d.bucket_day_ms,
            ts = d.ts_ms,
            id = fuuid(d.decision_id),
            slug = esc(&d.market_slug),
            side = esc(&d.side),
            size = d.size_usd,
            conf = d.confidence,
            edge = d.edge_bps,
            reasoning = esc(&d.reasoning),
            raw = esc(&d.raw_response),
            entry = d.entry_price,
        );
        self.session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("decisions.insert: {e}")))?;
        Ok(())
    }

    /// Read every decision row for one UTC day.
    ///
    /// **Currently unusable against the real CoreDB server**: the protocol
    /// implementation in `yhc007/coredb` advertises every SELECT column
    /// as type `Text` in the result metadata while sending raw binary
    /// payload for non-text columns (TIMESTAMP / DOUBLE / INT / UUID).
    /// The scylla driver rejects this on both typed and untyped paths
    /// (typed: type-check fails; untyped String: invalid UTF-8). Until
    /// the CoreDB side starts emitting correct type codes, callers that
    /// need to compare or replay decisions should use the on-disk JSONL
    /// ledger written by `crate::backtest::ledger`.
    pub async fn list_day(&self, bucket_day_ms: Millis) -> Result<Vec<Decision>, CoreDbError> {
        let _ = (&self.session, bucket_day_ms);
        Err(CoreDbError::Query(
            "decisions.list_day is unsupported by the current CoreDB server \
             (every SELECT column is mis-typed as Text). Read from the \
             JSONL ledger instead."
                .to_string(),
        ))
    }
}
