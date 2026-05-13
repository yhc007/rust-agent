//! Polymarket orderbook snapshot repository (inline-CQL).

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;

use super::error::CoreDbError;
use super::types::{Millis, OrderbookSnapshot};
use super::util::esc;

pub struct OrderbookRepo {
    session: Arc<Session>,
}

impl OrderbookRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn insert(&self, s: &OrderbookSnapshot) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.orderbook_snapshots \
             (bucket_hour, market_slug, ts, yes_bids, yes_asks, no_bids, no_asks) \
             VALUES ({bh}, {slug}, {ts}, {yb}, {ya}, {nb}, {na})",
            bh = s.bucket_hour_ms,
            slug = esc(&s.market_slug),
            ts = s.ts_ms,
            yb = esc(&s.yes_bids),
            ya = esc(&s.yes_asks),
            nb = esc(&s.no_bids),
            na = esc(&s.no_asks),
        );
        self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("orderbook.insert: {e}")))?;
        Ok(())
    }

    pub async fn list_hour(
        &self,
        bucket_hour_ms: Millis,
        market_slug: &str,
    ) -> Result<Vec<OrderbookSnapshot>, CoreDbError> {
        let q = format!(
            "SELECT bucket_hour, market_slug, ts, yes_bids, yes_asks, no_bids, no_asks \
             FROM polymarket_btc.orderbook_snapshots \
             WHERE bucket_hour = {} AND market_slug = {}",
            bucket_hour_ms,
            esc(market_slug),
        );
        let qr = self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("orderbook.list_hour: {e}")))?;
        let rows = qr.into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("orderbook.list_hour rows: {e}")))?;
        let typed = rows
            .rows::<(CqlTimestamp, String, CqlTimestamp, String, String, String, String)>()
            .map_err(|e| CoreDbError::Query(format!("orderbook.list_hour typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let (bh, slug, ts, yb, ya, nb, na) =
                row.map_err(|e| CoreDbError::Query(format!("orderbook row: {e}")))?;
            out.push(OrderbookSnapshot {
                bucket_hour_ms: bh.0,
                market_slug: slug,
                ts_ms: ts.0,
                yes_bids: yb,
                yes_asks: ya,
                no_bids: nb,
                no_asks: na,
            });
        }
        Ok(out)
    }
}
