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
             (slug, question, end_date, outcomes, closed, last_price, updated_at, \
              yes_token_id, no_token_id) \
             VALUES ({slug}, {question}, {end_date}, {outcomes}, {closed}, {last_price}, \
                     {updated_at}, {yes}, {no})",
            slug = esc(&m.slug),
            question = esc(&m.question),
            end_date = m.end_date_ms,
            outcomes = esc(&m.outcomes),
            closed = fbool(m.closed),
            last_price = m.last_price,
            updated_at = m.updated_at_ms,
            yes = esc(&m.yes_token_id),
            no = esc(&m.no_token_id),
        );
        self.session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("markets.upsert: {e}")))?;
        Ok(())
    }

    pub async fn list_open(&self) -> Result<Vec<Market>, CoreDbError> {
        let q = "SELECT slug, question, end_date, outcomes, closed, last_price, updated_at, \
                        yes_token_id, no_token_id \
                 FROM polymarket_btc.markets WHERE closed = false ALLOW FILTERING";
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("markets.list_open: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("markets.list_open rows: {e}")))?;
        // Name-keyed deser, same shim as decisions / btc_ticks / orders.
        let typed = rows
            .rows::<MarketRow>()
            .map_err(|e| CoreDbError::Query(format!("markets.list_open typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("markets.list_open row: {e}")))?;
            out.push(market_from_row(r));
        }
        Ok(out)
    }

    pub async fn get(&self, slug: &str) -> Result<Option<Market>, CoreDbError> {
        let q = format!(
            "SELECT slug, question, end_date, outcomes, closed, last_price, updated_at, \
                    yes_token_id, no_token_id \
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
            .rows::<MarketRow>()
            .map_err(|e| CoreDbError::Query(format!("markets.get typed: {e}")))?;
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("markets.get row: {e}")))?;
            return Ok(Some(market_from_row(r)));
        }
        Ok(None)
    }
}

#[derive(scylla::DeserializeRow)]
struct MarketRow {
    slug: Option<String>,
    question: Option<String>,
    end_date: Option<CqlTimestamp>,
    outcomes: Option<String>,
    closed: Option<bool>,
    last_price: Option<f64>,
    updated_at: Option<CqlTimestamp>,
    yes_token_id: Option<String>,
    no_token_id: Option<String>,
}

fn market_from_row(r: MarketRow) -> Market {
    Market {
        slug: r.slug.unwrap_or_default(),
        question: r.question.unwrap_or_default(),
        end_date_ms: r.end_date.map(|t| t.0).unwrap_or(0),
        outcomes: r.outcomes.unwrap_or_default(),
        closed: r.closed.unwrap_or(false),
        last_price: r.last_price.unwrap_or(0.0),
        updated_at_ms: r.updated_at.map(|t| t.0).unwrap_or(0),
        yes_token_id: r.yes_token_id.unwrap_or_default(),
        no_token_id: r.no_token_id.unwrap_or_default(),
    }
}
