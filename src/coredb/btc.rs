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
        if rows.rows_num() == 0 {
            return Ok(Vec::new());
        }
        // Name-keyed deser so HashMap-iteration column order doesn't trip us
        // up (same pattern as decisions / orders / strategy_pnl repos).
        let typed = rows
            .rows::<TickRow>()
            .map_err(|e| CoreDbError::Query(format!("btc_ticks.list_hour typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("btc_ticks row: {e}")))?;
            out.push(BtcTick {
                bucket_hour_ms: r.bucket_hour.map(|t| t.0).unwrap_or(0),
                symbol: r.symbol.unwrap_or_default(),
                ts_ms: r.ts.map(|t| t.0).unwrap_or(0),
                price: r.price.unwrap_or(0.0),
                volume: r.volume.unwrap_or(0.0),
                bid: r.bid.unwrap_or(0.0),
                ask: r.ask.unwrap_or(0.0),
            });
        }
        Ok(out)
    }

    /// Return the freshest tick for `symbol` from CoreDB. Looks in the
    /// current UTC hour partition first and falls back to the previous
    /// hour if the current one is empty (right after an hour boundary
    /// when the WS ingest hasn't fired yet). Returns `Ok(None)` when
    /// neither bucket has a row — caller should treat that as "cache
    /// miss, hit the upstream feed instead".
    pub async fn latest(&self, symbol: &str) -> Result<Option<BtcTick>, CoreDbError> {
        let now = super::types::now_ms();
        let cur = super::types::bucket_hour(now);
        let mut ticks = self.list_hour(cur, symbol).await?;
        if ticks.is_empty() {
            let prev = cur - 3_600_000;
            ticks = self.list_hour(prev, symbol).await?;
        }
        Ok(ticks.into_iter().max_by_key(|t| t.ts_ms))
    }
}

#[derive(scylla::DeserializeRow)]
struct TickRow {
    bucket_hour: Option<CqlTimestamp>,
    symbol: Option<String>,
    ts: Option<CqlTimestamp>,
    price: Option<f64>,
    volume: Option<f64>,
    bid: Option<f64>,
    ask: Option<f64>,
}
