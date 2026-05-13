//! Daily PnL repository (inline-CQL).

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;

use super::error::CoreDbError;
use super::types::{Millis, PnlDaily};

pub struct PnlRepo {
    session: Arc<Session>,
}

impl PnlRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn upsert(&self, p: &PnlDaily) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.pnl_daily \
             (day, realized, unrealized, n_trades, llm_cost_usd) \
             VALUES ({day}, {r}, {u}, {n}, {c})",
            day = p.day_ms,
            r = p.realized,
            u = p.unrealized,
            n = p.n_trades,
            c = p.llm_cost_usd,
        );
        self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("pnl.upsert: {e}")))?;
        Ok(())
    }

    pub async fn range(&self, from_ms: Millis, to_ms: Millis) -> Result<Vec<PnlDaily>, CoreDbError> {
        let q = format!(
            "SELECT day, realized, unrealized, n_trades, llm_cost_usd \
             FROM polymarket_btc.pnl_daily WHERE day >= {from_ms} AND day <= {to_ms} ALLOW FILTERING"
        );
        let qr = self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("pnl.range: {e}")))?;
        let rows = qr.into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("pnl.range rows: {e}")))?;
        let typed = rows
            .rows::<(CqlTimestamp, f64, f64, i32, f64)>()
            .map_err(|e| CoreDbError::Query(format!("pnl.range typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let (day, realized, unrealized, n_trades, llm_cost_usd) =
                row.map_err(|e| CoreDbError::Query(format!("pnl row: {e}")))?;
            out.push(PnlDaily {
                day_ms: day.0,
                realized,
                unrealized,
                n_trades,
                llm_cost_usd,
            });
        }
        Ok(out)
    }
}
