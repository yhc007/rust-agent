//! Per-(strategy, exec) realized PnL breakdown.
//!
//! Sibling to `pnl_daily` but keyed by strategy + paper/live so a
//! Grafana panel can split PnL across both axes. Settle-pnl writes
//! to both tables in the same run: `pnl_daily` stays the
//! single-row daily total; `pnl_breakdown` carries the
//! per-(strategy, exec) decomposition.

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;

use super::error::CoreDbError;
use super::types::{Millis, PnlBreakdown};
use super::util::esc;

pub struct PnlBreakdownRepo {
    session: Arc<Session>,
}

impl PnlBreakdownRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    /// Upsert one row. Same-(bucket_day, strategy, exec) writes
    /// replace; CoreDB does last-write-wins by timestamp.
    pub async fn upsert(&self, b: &PnlBreakdown) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.pnl_breakdown \
             (bucket_day, strategy, exec, realized_pnl, n_settled) \
             VALUES ({day}, {strat}, {exec}, {realized}, {n})",
            day = b.bucket_day_ms,
            strat = esc(&b.strategy),
            exec = esc(&b.exec),
            realized = b.realized_pnl,
            n = b.n_settled,
        );
        self.session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("pnl_breakdown.upsert: {e}")))?;
        Ok(())
    }

    /// Every row for one UTC day's bucket. Sorted by (strategy,
    /// exec) for stable output — CoreDB returns rows in cluster
    /// order which already matches but we sort defensively in case
    /// the storage iterator changes shape.
    pub async fn list_day(
        &self,
        bucket_day_ms: Millis,
    ) -> Result<Vec<PnlBreakdown>, CoreDbError> {
        let q = format!(
            "SELECT bucket_day, strategy, exec, realized_pnl, n_settled \
             FROM polymarket_btc.pnl_breakdown WHERE bucket_day = {bucket_day_ms}"
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("pnl_breakdown.list_day: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("pnl_breakdown.list_day rows: {e}")))?;
        if rows.rows_num() == 0 {
            return Ok(Vec::new());
        }
        let typed = rows
            .rows::<NamedRow>()
            .map_err(|e| CoreDbError::Query(format!("pnl_breakdown.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("pnl_breakdown row: {e}")))?;
            out.push(PnlBreakdown {
                bucket_day_ms: r.bucket_day.map(|t| t.0).unwrap_or(0),
                strategy: r.strategy.unwrap_or_default(),
                exec: r.exec.unwrap_or_default(),
                realized_pnl: r.realized_pnl.unwrap_or(0.0),
                n_settled: r.n_settled.unwrap_or(0),
            });
        }
        out.sort_by(|a, b| a.strategy.cmp(&b.strategy).then_with(|| a.exec.cmp(&b.exec)));
        Ok(out)
    }
}

#[derive(scylla::DeserializeRow)]
struct NamedRow {
    bucket_day: Option<CqlTimestamp>,
    strategy: Option<String>,
    exec: Option<String>,
    realized_pnl: Option<f64>,
    n_settled: Option<i32>,
}
