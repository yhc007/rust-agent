//! Pairwise agreement snapshot repository.
//!
//! Stores one row per ordered strategy pair per `compare-pnl`
//! invocation, partitioned on bucket_day so a day's matrix history
//! clusters together. The `agreement-history` CLI subcommand and any
//! future dashboard column read through `list_day`.
//!
//! Symmetric on the wire — we store both (a, b) and (b, a) so a
//! caller filtering on `strategy_a = X` gets every counterpart in a
//! single partition slice without having to flip the operand order.

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;

use super::error::CoreDbError;
use super::types::{AgreementSnapshot, Millis};
use super::util::esc;

pub struct AgreementRepo {
    session: Arc<Session>,
}

impl AgreementRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn insert(&self, s: &AgreementSnapshot) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.agreement_snapshots \
             (bucket_day, ts, strategy_a, strategy_b, shared, matches) \
             VALUES ({bd}, {ts}, {a}, {b}, {shared}, {matches})",
            bd = s.bucket_day_ms,
            ts = s.ts_ms,
            a = esc(&s.strategy_a),
            b = esc(&s.strategy_b),
            shared = s.shared,
            matches = s.matches,
        );
        self.session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("agreement.insert: {e}")))?;
        Ok(())
    }

    /// All rows for one UTC day, sorted by (ts ASC, strategy_a,
    /// strategy_b) so a downstream consumer gets a stable time series
    /// without re-sorting. Empty Vec when no rows have landed for
    /// the day yet.
    pub async fn list_day(
        &self,
        bucket_day_ms: Millis,
    ) -> Result<Vec<AgreementSnapshot>, CoreDbError> {
        let q = format!(
            "SELECT bucket_day, ts, strategy_a, strategy_b, shared, matches \
             FROM polymarket_btc.agreement_snapshots WHERE bucket_day = {bucket_day_ms}"
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("agreement.list_day: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("agreement.list_day rows: {e}")))?;
        if rows.rows_num() == 0 {
            return Ok(Vec::new());
        }
        let typed = rows
            .rows::<NamedRow>()
            .map_err(|e| CoreDbError::Query(format!("agreement.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("agreement row: {e}")))?;
            out.push(AgreementSnapshot {
                bucket_day_ms: r.bucket_day.map(|t| t.0).unwrap_or(0),
                ts_ms: r.ts.map(|t| t.0).unwrap_or(0),
                strategy_a: r.strategy_a.unwrap_or_default(),
                strategy_b: r.strategy_b.unwrap_or_default(),
                shared: r.shared.unwrap_or(0),
                matches: r.matches.unwrap_or(0),
            });
        }
        out.sort_by(|a, b| {
            a.ts_ms
                .cmp(&b.ts_ms)
                .then_with(|| a.strategy_a.cmp(&b.strategy_a))
                .then_with(|| a.strategy_b.cmp(&b.strategy_b))
        });
        Ok(out)
    }
}

#[derive(scylla::DeserializeRow)]
struct NamedRow {
    bucket_day: Option<CqlTimestamp>,
    ts: Option<CqlTimestamp>,
    strategy_a: Option<String>,
    strategy_b: Option<String>,
    shared: Option<i32>,
    matches: Option<i32>,
}
