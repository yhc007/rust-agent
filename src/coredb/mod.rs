//! CoreDB (Cassandra-compatible) data layer for the polymarket-btc subsystem.
//!
//! `CoreDb::connect(uri).await?` then `db.migrate().await?` to bring up
//! the `polymarket_btc` keyspace and tables. Wraps a `scylla::Session`
//! that other repositories will share via `Arc`.

pub mod agreement;
pub mod btc;
pub mod decisions;
pub mod error;
pub mod markets;
pub mod orderbook;
pub mod orders;
pub mod pnl;
pub mod schema;
pub mod strategy_pnl;
pub mod types;
pub mod util;

use std::sync::Arc;
use std::time::Duration;

use scylla::{Session, SessionBuilder};

pub use error::CoreDbError;

pub struct CoreDb {
    session: Arc<Session>,
}

impl CoreDb {
    /// Connect to a CoreDB node by `host:port`.
    pub async fn connect(uri: impl AsRef<str>) -> Result<Self, CoreDbError> {
        let session = SessionBuilder::new()
            .known_node(uri.as_ref())
            .connection_timeout(Duration::from_secs(5))
            .build()
            .await
            .map_err(|e| CoreDbError::SessionBuild(e.to_string()))?;
        Ok(Self {
            session: Arc::new(session),
        })
    }

    pub fn session(&self) -> Arc<Session> {
        self.session.clone()
    }

    /// Tables created by `migrate`, in the order checks should run.
    /// `positions` (pre-v2, PK = market_slug) is deliberately omitted —
    /// it is no longer created, written, or read by the runtime.
    /// Existing deployments may still have the orphan table on disk;
    /// `DROP TABLE polymarket_btc.positions` via cqlsh removes it.
    pub const TABLES: &'static [&'static str] = &[
        "btc_ticks",
        "orderbook_snapshots",
        "markets",
        "decisions",
        "orders",
        "positions_v2",
        "pnl_daily",
        "strategy_pnl_snapshots",
        "agreement_snapshots",
    ];

    /// Approximate row count for each known table.
    ///
    /// Strategy per table: try server-side `SELECT COUNT(*) FROM ks.t`
    /// first with a generous 5-minute per-query timeout; fall back to
    /// the historical streaming-tally on parser failure for older
    /// CoreDB. On the daemon's continuously-ingesting `btc_ticks`
    /// table both paths can still exceed even a 5-minute window, so
    /// per-table failures return `None` rather than aborting the
    /// whole report — operators can still see the other tables'
    /// counts when `stats` runs against a busy DB.
    pub async fn count_rows(&self) -> Result<Vec<(String, Option<i64>)>, CoreDbError> {
        let mut out = Vec::with_capacity(Self::TABLES.len());
        for t in Self::TABLES {
            out.push(((*t).to_string(), self.count_one(t).await));
        }
        Ok(out)
    }

    /// Best-effort row count for one table. `None` when every path
    /// (server-side aggregate + streaming tally) fails. The error is
    /// logged once at warn-level so an operator inspecting the
    /// daemon's journalctl can see *why* the count is missing
    /// without parsing a hidden CLI flag.
    async fn count_one(&self, table: &str) -> Option<i64> {
        use scylla::statement::query::Query;
        use std::time::Duration;
        use tracing::warn;

        // Path A: server-side COUNT(*). Bounded by however CoreDB
        // implements the aggregate (it currently materializes rows
        // server-side, so on huge tables this is still slow — but a
        // 5-min timeout gives it a real chance instead of the 30s
        // default).
        let agg_text = format!("SELECT COUNT(*) FROM {}.{}", schema::KEYSPACE, table);
        let mut agg_q = Query::new(agg_text);
        agg_q.set_request_timeout(Some(Duration::from_secs(300)));
        match self.session.query_unpaged(agg_q, &[]).await {
            Ok(qr) => {
                if let Ok(rows) = qr.into_rows_result() {
                    if rows.rows_num() >= 1 {
                        // Untyped row iteration so we don't care
                        // which name the column ended up with.
                        if let Ok(typed) = rows.rows::<(i64,)>() {
                            for row in typed.flatten() {
                                let (n,) = row;
                                return Some(n);
                            }
                        }
                    }
                }
                // Query succeeded but result shape was unexpected —
                // fall through.
            }
            Err(e) => {
                warn!("count_rows[{table}]: COUNT(*) failed ({e}); trying streaming");
            }
        }

        // Path B: streaming tally. Same 5-min timeout — gives huge
        // tables a fighting chance even without a working COUNT(*).
        let scan_text = format!("SELECT * FROM {}.{}", schema::KEYSPACE, table);
        let mut scan_q = Query::new(scan_text);
        scan_q.set_request_timeout(Some(Duration::from_secs(300)));
        match self.session.query_unpaged(scan_q, &[]).await {
            Ok(qr) => match qr.into_rows_result() {
                Ok(rows) => Some(rows.rows_num() as i64),
                Err(e) => {
                    warn!("count_rows[{table}]: rows_result failed: {e}");
                    None
                }
            },
            Err(e) => {
                warn!("count_rows[{table}]: streaming SELECT failed: {e}");
                None
            }
        }
    }

    /// Probe each expected table with a `SELECT ... LIMIT 1`. Returns the
    /// list of tables that responded successfully. Use as a post-migrate
    /// smoke check.
    pub async fn verify_tables(&self) -> Result<Vec<String>, CoreDbError> {
        let mut ok = Vec::with_capacity(Self::TABLES.len());
        for t in Self::TABLES {
            let q = format!("SELECT * FROM {}.{} LIMIT 1", schema::KEYSPACE, t);
            self.session
                .query_unpaged(q, &[])
                .await
                .map_err(|e| CoreDbError::Query(format!("table `{}`: {}", t, e)))?;
            ok.push((*t).to_string());
        }
        Ok(ok)
    }

    /// Apply all schema migrations. Idempotent — safe to run on a populated DB.
    ///
    /// CoreDB's `CREATE INDEX` parser does not accept `IF NOT EXISTS`, so we
    /// treat "already exists" errors as success to keep the migration
    /// re-runnable.
    pub async fn migrate(&self) -> Result<(), CoreDbError> {
        for stmt in schema::MIGRATIONS {
            match self.session.query_unpaged(*stmt, &[]).await {
                Ok(_) => {}
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("already exists") {
                        continue;
                    }
                    return Err(CoreDbError::Migration {
                        stmt: stmt.to_string(),
                        message: msg,
                    });
                }
            }
        }
        Ok(())
    }
}
