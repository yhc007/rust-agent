//! Ingestion orchestrator. Spawns binance + polymarket pollers, hands
//! each one its repo handle and a shared shutdown signal, then waits
//! for Ctrl+C.

use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::signal;
use tokio::sync::watch;
use tracing::info;

use crate::coredb::btc::BtcTickRepo;
use crate::coredb::markets::MarketRepo;
use crate::coredb::CoreDb;

use super::{binance, polymarket};

pub async fn run(coredb_uri: &str) -> Result<()> {
    info!("ingest: connecting to CoreDB at {coredb_uri}");
    let db = CoreDb::connect(coredb_uri).await.context("connect coredb")?;
    let session = db.session();
    let btc_repo = Arc::new(BtcTickRepo::new(session.clone()).await?);
    let market_repo = Arc::new(MarketRepo::new(session.clone()).await?);

    let (tx, rx) = watch::channel(false);

    let h_binance = tokio::spawn(binance::run(btc_repo, rx.clone()));
    let h_poly = tokio::spawn(polymarket::run(market_repo, rx.clone()));

    info!("ingest: pollers running — Ctrl+C to stop");
    signal::ctrl_c().await.context("install Ctrl+C handler")?;
    info!("ingest: shutdown signal received");
    let _ = tx.send(true);

    let _ = h_binance.await;
    let _ = h_poly.await;
    info!("ingest: clean exit");
    Ok(())
}
