//! BTC tick repository (inline-CQL flavour).

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;

use super::error::CoreDbError;
use super::types::{BtcTick, Millis};
use super::util::esc;

pub struct BtcTickRepo {
    session: Arc<Session>,
}

impl BtcTickRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn insert(&self, t: &BtcTick) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.btc_ticks \
             (bucket_hour, symbol, ts, price, volume, bid, ask) \
             VALUES ({bh}, {sym}, {ts}, {price}, {volume}, {bid}, {ask})",
            bh = t.bucket_hour_ms,
            sym = esc(&t.symbol),
            ts = t.ts_ms,
            price = t.price,
            volume = t.volume,
            bid = t.bid,
            ask = t.ask,
        );
        self.session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("btc_ticks.insert: {e}")))?;
        Ok(())
    }

    pub async fn list_hour(
        &self,
        bucket_hour_ms: Millis,
        symbol: &str,
    ) -> Result<Vec<BtcTick>, CoreDbError> {
        let q = format!(
            "SELECT bucket_hour, symbol, ts, price, volume, bid, ask \
             FROM polymarket_btc.btc_ticks \
             WHERE bucket_hour = {} AND symbol = {}",
            bucket_hour_ms,
            esc(symbol),
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("btc_ticks.list_hour: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("btc_ticks.list_hour rows: {e}")))?;
        let typed = rows
            .rows::<(CqlTimestamp, String, CqlTimestamp, f64, f64, f64, f64)>()
            .map_err(|e| CoreDbError::Query(format!("btc_ticks.list_hour typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let (bh, sym, ts, price, volume, bid, ask) =
                row.map_err(|e| CoreDbError::Query(format!("btc_ticks row: {e}")))?;
            out.push(BtcTick {
                bucket_hour_ms: bh.0,
                symbol: sym,
                ts_ms: ts.0,
                price,
                volume,
                bid,
                ask,
            });
        }
        Ok(out)
    }
}
