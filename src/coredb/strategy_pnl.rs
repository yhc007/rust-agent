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

/// Compute the change in `sum_pnl` over the trailing 24h window for
/// one strategy's snapshot series. Returns `None` when no baseline
/// snapshot lands within ±6h of `latest.ts_ms - 24h` — e.g. the
/// daemon has been running for less than ~18h, or snapshots are
/// sparse around that mark — so callers render an empty cell /
/// emit no series rather than booking a delta against an arbitrary
/// older point.
///
/// The reference clock is the latest snapshot's `ts_ms`, not
/// `Utc::now()`: a halted `compare-pnl` then shows the most recent
/// valid 24h delta instead of going blank, which matches the trend
/// sparkline's own latest-row semantics.
///
/// Pure function on the input slice so it's testable without any
/// runtime context. Series is expected ascending by `ts_ms` but the
/// algorithm only cares about `(.last(), .min_by_key)`, so passing
/// an unsorted slice still produces a valid result.
///
/// Slice type is `&[&StrategyPnlSnapshot]` rather than
/// `&[StrategyPnlSnapshot]` because the natural shape on the caller
/// side is "per-strategy grouped refs into a larger Vec" — owned
/// callers can adapt with `iter().collect::<Vec<_>>()`.
pub fn pnl_delta_24h(series: &[&StrategyPnlSnapshot]) -> Option<f64> {
    let latest = series.last()?;
    let target = latest.ts_ms - 24 * 60 * 60 * 1000;
    let tolerance = 6 * 60 * 60 * 1000;
    let baseline = *series
        .iter()
        .min_by_key(|s| (s.ts_ms - target).abs())?;
    if (baseline.ts_ms - target).abs() > tolerance {
        return None;
    }
    if baseline.ts_ms >= latest.ts_ms {
        return None;
    }
    Some(latest.sum_pnl - baseline.sum_pnl)
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
