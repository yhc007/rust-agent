//! Per-strategy mark-to-market PnL snapshot repository.
//!
//! `compare-pnl` writes one row per strategy per invocation; the rows
//! cluster under `bucket_day` so a day's worth of snapshots stays
//! together. Mark-to-market only — these rows are unrealized; settled
//! PnL rolls into `pnl_daily` once markets resolve.

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;

use super::error::CoreDbError;
use super::types::{Millis, StrategyPnlSnapshot};
use super::util::esc;

pub struct StrategyPnlRepo {
    session: Arc<Session>,
}

impl StrategyPnlRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn insert(&self, s: &StrategyPnlSnapshot) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.strategy_pnl_snapshots \
             (bucket_day, ts, strategy, n_decisions, sum_size_usd, sum_pnl, \
              n_yes, n_no, n_pass) \
             VALUES ({bd}, {ts}, {strat}, {n}, {size}, {pnl}, {y}, {no}, {pass})",
            bd = s.bucket_day_ms,
            ts = s.ts_ms,
            strat = esc(&s.strategy),
            n = s.n_decisions,
            size = s.sum_size_usd,
            pnl = s.sum_pnl,
            y = s.n_yes,
            no = s.n_no,
            pass = s.n_pass,
        );
        self.session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("strategy_pnl.insert: {e}")))?;
        Ok(())
    }

    /// Every snapshot row for one UTC day, sorted by `ts` ascending then
    /// strategy. Returns an empty Vec when no snapshots have been written
    /// for that day yet.
    pub async fn list_day(&self, bucket_day_ms: Millis) -> Result<Vec<StrategyPnlSnapshot>, CoreDbError> {
        let q = format!(
            "SELECT bucket_day, ts, strategy, n_decisions, sum_size_usd, sum_pnl, \
                    n_yes, n_no, n_pass \
             FROM polymarket_btc.strategy_pnl_snapshots WHERE bucket_day = {bucket_day_ms}"
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("strategy_pnl.list_day: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("strategy_pnl.list_day rows: {e}")))?;
        if rows.rows_num() == 0 {
            return Ok(Vec::new());
        }
        let typed = rows
            .rows::<NamedRow>()
            .map_err(|e| CoreDbError::Query(format!("strategy_pnl.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("strategy_pnl row: {e}")))?;
            out.push(StrategyPnlSnapshot {
                bucket_day_ms: r.bucket_day.map(|t| t.0).unwrap_or(0),
                ts_ms: r.ts.map(|t| t.0).unwrap_or(0),
                strategy: r.strategy.unwrap_or_default(),
                n_decisions: r.n_decisions.unwrap_or(0),
                sum_size_usd: r.sum_size_usd.unwrap_or(0.0),
                sum_pnl: r.sum_pnl.unwrap_or(0.0),
                n_yes: r.n_yes.unwrap_or(0),
                n_no: r.n_no.unwrap_or(0),
                n_pass: r.n_pass.unwrap_or(0),
            });
        }
        // CoreDB's SELECT delivers rows in storage (skiplist) order, which is
        // already (ts ASC, strategy ASC) thanks to the clustering key. Sort
        // again defensively so future schema changes can't break callers.
        out.sort_by(|a, b| a.ts_ms.cmp(&b.ts_ms).then_with(|| a.strategy.cmp(&b.strategy)));
        Ok(out)
    }
}

/// Name-keyed projection matching the SELECT above. Same shape as the
/// trick used in `decisions.list_day` to dodge CoreDB's HashMap-order
/// column reshuffling.
#[derive(scylla::DeserializeRow)]
struct NamedRow {
    bucket_day: Option<CqlTimestamp>,
    ts: Option<CqlTimestamp>,
    strategy: Option<String>,
    n_decisions: Option<i32>,
    sum_size_usd: Option<f64>,
    sum_pnl: Option<f64>,
    n_yes: Option<i32>,
    n_no: Option<i32>,
    n_pass: Option<i32>,
}
