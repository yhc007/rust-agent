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
        let qr = self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("orders.list_day: {e}")))?;
        let rows = qr.into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("orders.list_day rows: {e}")))?;
        type Row = (
            CqlTimestamp, CqlTimestamp, String, Uuid, String, String,
            f64, f64, String, f64, f64,
        );
        let typed = rows.rows::<Row>()
            .map_err(|e| CoreDbError::Query(format!("orders.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let (bd, ts, oid, did, slug, side, size, price, status, fs, fp) =
                row.map_err(|e| CoreDbError::Query(format!("orders row: {e}")))?;
            out.push(Order {
                bucket_day_ms: bd.0,
                ts_ms: ts.0,
                order_id: oid,
                decision_id: did,
                market_slug: slug,
                side,
                size,
                price,
                status,
                fill_size: fs,
                fill_price: fp,
            });
        }
        Ok(out)
    }
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
        let typed = rows
            .rows::<(String, String, f64, f64, CqlTimestamp)>()
            .map_err(|e| CoreDbError::Query(format!("positions.list_all typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let (slug, side, size, avg, upd) =
                row.map_err(|e| CoreDbError::Query(format!("positions row: {e}")))?;
            out.push(Position {
                market_slug: slug,
                side,
                size,
                avg_price: avg,
                updated_at_ms: upd.0,
            });
        }
        Ok(out)
    }
}
