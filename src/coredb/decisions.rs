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
              edge_bps, reasoning, raw_response) \
             VALUES ({bd}, {ts}, {id}, {slug}, {side}, {size}, {conf}, {edge}, {reasoning}, {raw})",
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
                    edge_bps, reasoning, raw_response \
             FROM polymarket_btc.decisions WHERE bucket_day = {bucket_day_ms}"
        );
        let qr = self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("decisions.list_day: {e}")))?;
        let rows = qr.into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("decisions.list_day rows: {e}")))?;
        type Row = (
            CqlTimestamp, CqlTimestamp, Uuid, String, String,
            f64, f64, i32, String, String,
        );
        let typed = rows.rows::<Row>()
            .map_err(|e| CoreDbError::Query(format!("decisions.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let (bd, ts, id, slug, side, size_usd, conf, edge, reasoning, raw) =
                row.map_err(|e| CoreDbError::Query(format!("decisions row: {e}")))?;
            out.push(Decision {
                bucket_day_ms: bd.0,
                ts_ms: ts.0,
                decision_id: id,
                market_slug: slug,
                side,
                size_usd,
                confidence: conf,
                edge_bps: edge,
                reasoning,
                raw_response: raw,
            });
        }
        Ok(out)
    }
}
