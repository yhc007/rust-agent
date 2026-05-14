//! Order lifecycle + current-position repositories (inline-CQL).

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;
use uuid::Uuid;

use super::error::CoreDbError;
use super::types::{Millis, Order, Position};
use super::util::{esc, fuuid};

pub struct OrderRepo {
    session: Arc<Session>,
}

impl OrderRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn insert(&self, o: &Order) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.orders \
             (bucket_day, ts, order_id, decision_id, market_slug, side, size, price, \
              status, fill_size, fill_price) \
             VALUES ({bd}, {ts}, {oid}, {did}, {slug}, {side}, {size}, {price}, {st}, {fs}, {fp})",
            bd = o.bucket_day_ms,
            ts = o.ts_ms,
            oid = esc(&o.order_id),
            did = fuuid(o.decision_id),
            slug = esc(&o.market_slug),
            side = esc(&o.side),
            size = o.size,
            price = o.price,
            st = esc(&o.status),
            fs = o.fill_size,
            fp = o.fill_price,
        );
        self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("orders.insert: {e}")))?;
        Ok(())
    }

    pub async fn list_day(&self, bucket_day_ms: Millis) -> Result<Vec<Order>, CoreDbError> {
        let q = format!(
            "SELECT bucket_day, ts, order_id, decision_id, market_slug, side, size, price, \
                    status, fill_size, fill_price \
             FROM polymarket_btc.orders WHERE bucket_day = {bucket_day_ms}"
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("orders.list_day: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("orders.list_day rows: {e}")))?;
        // CoreDB returns columns in HashMap-iteration order, not SELECT-list
        // order — see the equivalent shim in decisions.rs. Name-keyed
        // DeserializeRow sidesteps the reshuffle.
        let typed = rows
            .rows::<OrderRow>()
            .map_err(|e| CoreDbError::Query(format!("orders.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("orders row: {e}")))?;
            out.push(Order {
                bucket_day_ms: r.bucket_day.map(|t| t.0).unwrap_or(0),
                ts_ms: r.ts.map(|t| t.0).unwrap_or(0),
                order_id: r.order_id.unwrap_or_default(),
                decision_id: r.decision_id.unwrap_or_else(Uuid::nil),
                market_slug: r.market_slug.unwrap_or_default(),
                side: r.side.unwrap_or_default(),
                size: r.size.unwrap_or(0.0),
                price: r.price.unwrap_or(0.0),
                status: r.status.unwrap_or_default(),
                fill_size: r.fill_size.unwrap_or(0.0),
                fill_price: r.fill_price.unwrap_or(0.0),
            });
        }
        Ok(out)
    }
}

#[derive(scylla::DeserializeRow)]
struct OrderRow {
    bucket_day: Option<CqlTimestamp>,
    ts: Option<CqlTimestamp>,
    order_id: Option<String>,
    decision_id: Option<Uuid>,
    market_slug: Option<String>,
    side: Option<String>,
    size: Option<f64>,
    price: Option<f64>,
    status: Option<String>,
    fill_size: Option<f64>,
    fill_price: Option<f64>,
}

pub struct PositionRepo {
    session: Arc<Session>,
}

impl PositionRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn upsert(&self, p: &Position) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.positions \
             (market_slug, side, size, avg_price, updated_at) \
             VALUES ({slug}, {side}, {size}, {avg}, {upd})",
            slug = esc(&p.market_slug),
            side = esc(&p.side),
            size = p.size,
            avg = p.avg_price,
            upd = p.updated_at_ms,
        );
        self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("positions.upsert: {e}")))?;
        Ok(())
    }

    pub async fn list_all(&self) -> Result<Vec<Position>, CoreDbError> {
        let q = "SELECT market_slug, side, size, avg_price, updated_at \
                 FROM polymarket_btc.positions";
        let qr = self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("positions.list_all: {e}")))?;
        let rows = qr.into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("positions.list_all rows: {e}")))?;
        // Name-keyed: CoreDB returns columns in HashMap iteration order so
        // tuple-position deserialization is unsafe.
        let typed = rows
            .rows::<PositionRow>()
            .map_err(|e| CoreDbError::Query(format!("positions.list_all typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("positions row: {e}")))?;
            out.push(Position {
                market_slug: r.market_slug.unwrap_or_default(),
                side: r.side.unwrap_or_default(),
                size: r.size.unwrap_or(0.0),
                avg_price: r.avg_price.unwrap_or(0.0),
                updated_at_ms: r.updated_at.map(|t| t.0).unwrap_or(0),
            });
        }
        Ok(out)
    }
}

#[derive(scylla::DeserializeRow)]
struct PositionRow {
    market_slug: Option<String>,
    side: Option<String>,
    size: Option<f64>,
    avg_price: Option<f64>,
    updated_at: Option<CqlTimestamp>,
}
