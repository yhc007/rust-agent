//! Polymarket market metadata repository (inline-CQL flavour).

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;

use super::error::CoreDbError;
use super::types::Market;
use super::util::{esc, fbool};

pub struct MarketRepo {
    session: Arc<Session>,
}

impl MarketRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn upsert(&self, m: &Market) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.markets \
             (slug, question, end_date, outcomes, closed, last_price, updated_at) \
             VALUES ({slug}, {question}, {end_date}, {outcomes}, {closed}, {last_price}, {updated_at})",
            slug = esc(&m.slug),
            question = esc(&m.question),
            end_date = m.end_date_ms,
            outcomes = esc(&m.outcomes),
            closed = fbool(m.closed),
            last_price = m.last_price,
            updated_at = m.updated_at_ms,
        );
        self.session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("markets.upsert: {e}")))?;
        Ok(())
    }

    pub async fn list_open(&self) -> Result<Vec<Market>, CoreDbError> {
        let q = "SELECT slug, question, end_date, outcomes, closed, last_price, updated_at \
                 FROM polymarket_btc.markets WHERE closed = false ALLOW FILTERING";
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("markets.list_open: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("markets.list_open rows: {e}")))?;
        let typed = rows
            .rows::<(String, String, CqlTimestamp, String, bool, f64, CqlTimestamp)>()
            .map_err(|e| CoreDbError::Query(format!("markets.list_open typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let (slug, question, end_date, outcomes, closed, last_price, updated_at) =
                row.map_err(|e| CoreDbError::Query(format!("markets.list_open row: {e}")))?;
            out.push(Market {
                slug,
                question,
                end_date_ms: end_date.0,
                outcomes,
                closed,
                last_price,
                updated_at_ms: updated_at.0,
            });
        }
        Ok(out)
    }

    pub async fn get(&self, slug: &str) -> Result<Option<Market>, CoreDbError> {
        let q = format!(
            "SELECT slug, question, end_date, outcomes, closed, last_price, updated_at \
             FROM polymarket_btc.markets WHERE slug = {}",
            esc(slug)
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("markets.get: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("markets.get rows: {e}")))?;
        let typed = rows
            .rows::<(String, String, CqlTimestamp, String, bool, f64, CqlTimestamp)>()
            .map_err(|e| CoreDbError::Query(format!("markets.get typed: {e}")))?;
        for row in typed {
            let (slug, question, end_date, outcomes, closed, last_price, updated_at) =
                row.map_err(|e| CoreDbError::Query(format!("markets.get row: {e}")))?;
            return Ok(Some(Market {
                slug,
                question,
                end_date_ms: end_date.0,
                outcomes,
                closed,
                last_price,
                updated_at_ms: updated_at.0,
            }));
        }
        Ok(None)
    }
}
