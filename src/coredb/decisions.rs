//! Agent decision repository (inline-CQL flavour).

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;
use uuid::Uuid;

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

    pub async fn list_day(&self, bucket_day_ms: Millis) -> Result<Vec<Decision>, CoreDbError> {
        let q = format!(
            "SELECT bucket_day, ts, decision_id, market_slug, side, size_usd, confidence, \
                    edge_bps, reasoning, raw_response, entry_price \
             FROM polymarket_btc.decisions WHERE bucket_day = {bucket_day_ms}"
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("decisions.list_day: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("decisions.list_day rows: {e}")))?;
        // Column order in the response follows CoreDB's HashMap iteration
        // (Row.columns is HashMap<String, CassandraValue>), which is not the
        // SELECT-list order. Using a `(Name, Type)` typed-tuple risks a
        // position mismatch, so we read row-by-row and look up each column
        // by name from a Decision-shaped accessor instead.
        let typed = rows
            .rows::<NamedRow>()
            .map_err(|e| CoreDbError::Query(format!("decisions.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let row = row.map_err(|e| CoreDbError::Query(format!("decisions row: {e}")))?;
            out.push(Decision {
                bucket_day_ms: row.bucket_day.map(|t| t.0).unwrap_or(0),
                ts_ms: row.ts.map(|t| t.0).unwrap_or(0),
                decision_id: row.decision_id.unwrap_or_else(Uuid::nil),
                market_slug: row.market_slug.unwrap_or_default(),
                side: row.side.unwrap_or_default(),
                size_usd: row.size_usd.unwrap_or(0.0),
                confidence: row.confidence.unwrap_or(0.0),
                edge_bps: row.edge_bps.unwrap_or(0),
                reasoning: row.reasoning.unwrap_or_default(),
                raw_response: row.raw_response.unwrap_or_default(),
                entry_price: row.entry_price.unwrap_or(0.0),
            });
        }
        Ok(out)
    }
}

/// Name-keyed projection of `polymarket_btc.decisions`. Matched to the
/// response by column name rather than by position so CoreDB's
/// HashMap-iteration column order doesn't have to match the SELECT
/// list. Every field is `Option` because the underlying TEXT columns
/// may be NULL and because pre-schema rows lack `entry_price`.
#[derive(scylla::DeserializeRow)]
struct NamedRow {
    bucket_day: Option<CqlTimestamp>,
    ts: Option<CqlTimestamp>,
    decision_id: Option<Uuid>,
    market_slug: Option<String>,
    side: Option<String>,
    size_usd: Option<f64>,
    confidence: Option<f64>,
    edge_bps: Option<i32>,
    reasoning: Option<String>,
    raw_response: Option<String>,
    entry_price: Option<f64>,
}
