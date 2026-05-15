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

    /// Approximate row count for each known table. CoreDB's CQL parser
    /// does not accept WHERE-less COUNT plus ALLOW FILTERING in some
    /// versions, so we fall back to streaming rows and tallying client
    /// side. Fine for the few-thousand-row daily volumes the agent
    /// produces; do not call this in a hot path.
    pub async fn count_rows(&self) -> Result<Vec<(String, i64)>, CoreDbError> {
        let mut out = Vec::with_capacity(Self::TABLES.len());
        for t in Self::TABLES {
            let q = format!("SELECT * FROM {}.{}", schema::KEYSPACE, t);
            let qr = self
                .session
                .query_unpaged(q, &[])
                .await
                .map_err(|e| CoreDbError::Query(format!("count `{}`: {}", t, e)))?;
            let rows = qr
                .into_rows_result()
                .map_err(|e| CoreDbError::Query(format!("count rows `{}`: {}", t, e)))?;
            out.push(((*t).to_string(), rows.rows_num() as i64));
        }
        Ok(out)
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
