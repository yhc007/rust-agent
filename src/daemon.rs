//! Single-process autonomous orchestrator.
//!
//! `rust-agent daemon` runs four things side by side under one tokio
//! runtime so the operator doesn't need cron, systemd, or a process
//! supervisor for day-to-day paper trading:
//!
//! 1. Binance WS + Polymarket REST ingest → CoreDB.
//! 2. `backtest --both --execute` on a fixed interval (default 30 min).
//! 3. `compare-pnl` on a fixed interval (default 15 min).
//! 4. `settle-pnl` on a longer interval (default 1 h).
//!
//! All tasks share a single `watch::channel(bool)` shutdown signal so
//! Ctrl+C tears the whole pipeline down cleanly. Each periodic task
//! handles its own errors and keeps going — a single transient
//! Polymarket / Binance / DeepSeek failure doesn't take the daemon
//! down.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::signal;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use crate::backtest::{self, BacktestMode};
use crate::coredb::btc::BtcTickRepo;
use crate::coredb::markets::MarketRepo;
use crate::coredb::CoreDb;
use crate::data::{binance, polymarket, user_channel};
use crate::execution::clob_auth::ApiCreds;

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub coredb_uri: String,
    pub backtest_every_secs: u64,
    pub compare_every_secs: u64,
    pub settle_every_secs: u64,
    /// When false, the periodic backtests stop at decision insert and
    /// don't execute. Default true.
    pub execute: bool,
    /// When true, the periodic backtests use LiveExec instead of
    /// PaperExec. LiveExec stays DRY_RUN unless `LIVE_TRADING_ENABLED=1`
    /// is also exported — two gates by design.
    pub live: bool,
}

impl DaemonConfig {
    pub fn new(coredb_uri: String) -> Self {
        Self {
            coredb_uri,
            backtest_every_secs: 30 * 60,
            compare_every_secs: 15 * 60,
            settle_every_secs: 60 * 60,
            execute: true,
            live: false,
        }
    }
}

pub async fn run(cfg: DaemonConfig) -> Result<()> {
    let DaemonConfig {
        coredb_uri,
        backtest_every_secs,
        compare_every_secs,
        settle_every_secs,
        execute,
        live,
    } = cfg;

    println!("🛰️  daemon: starting");
    println!("    coredb_uri        = {coredb_uri}");
    println!(
        "    backtest every    = {backtest_every_secs}s (mode=both, execute={execute}, live={live})"
    );
    println!("    compare-pnl every = {compare_every_secs}s");
    println!("    settle-pnl every  = {settle_every_secs}s");
    if live {
        println!(
            "    ⚠ --live: backtests route through LiveExec. LIVE_TRADING_ENABLED + \
             POLYMARKET_CLOB_* + on-chain USDC approve still required for real submission."
        );
    }
    println!("    Ctrl+C to stop");

    // One CoreDB connection for the long-running ingest pollers. The
    // periodic batch jobs construct their own connections per
    // invocation — they're brief and this keeps backtest/compare/settle
    // callable as-is without threading a session through.
    let db = CoreDb::connect(&coredb_uri)
        .await
        .with_context(|| format!("daemon: connect coredb at {coredb_uri}"))?;
    let btc_repo = Arc::new(BtcTickRepo::new(db.session()).await?);
    let market_repo = Arc::new(MarketRepo::new(db.session()).await?);

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // 1. Ingest pollers (long-running tasks). These already accept a
    //    watch::Receiver and exit cleanly when it flips to `true`.
    let h_binance = tokio::spawn(binance::run(btc_repo, shutdown_rx.clone()));
    let h_polymarket = tokio::spawn(polymarket::run(market_repo, shutdown_rx.clone()));

    // Polymarket user-channel WS listener — only spawned when CLOB
    // credentials are visible in env. Paper-only deployments skip
    // this cleanly. Log-only for now; the next turn wires it into
    // orders + positions writes.
    let h_user_channel = match load_clob_creds_from_env() {
        Some(creds) => {
            info!("daemon: CLOB creds present; spawning user-channel listener");
            Some(tokio::spawn(user_channel::run(
                std::sync::Arc::new(creds),
                shutdown_rx.clone(),
            )))
        }
        None => {
            info!(
                "daemon: POLYMARKET_CLOB_API_KEY/_SECRET/_PASSPHRASE not all set; \
                 user-channel listener skipped"
            );
            None
        }
    };

    // 2/3/4. Periodic batch jobs. Each loop has the same shape — wait
    // for either the next tick or shutdown, then run the body.
    let h_backtest = tokio::spawn(periodic(
        "backtest",
        Duration::from_secs(backtest_every_secs),
        /* skip_first = */ false,
        shutdown_rx.clone(),
        {
            let uri = coredb_uri.clone();
            move || {
                let uri = uri.clone();
                Box::pin(async move {
                    backtest::run::run(&uri, BacktestMode::Both, execute, live).await
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        },
    ));

    let h_compare = tokio::spawn(periodic(
        "compare-pnl",
        Duration::from_secs(compare_every_secs),
        /* skip_first = */ true,
        shutdown_rx.clone(),
        {
            let uri = coredb_uri.clone();
            move || {
                let uri = uri.clone();
                Box::pin(async move { backtest::compare::run(&uri).await })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        },
    ));

    let h_settle = tokio::spawn(periodic(
        "settle-pnl",
        Duration::from_secs(settle_every_secs),
        /* skip_first = */ true,
        shutdown_rx.clone(),
        {
            let uri = coredb_uri.clone();
            move || {
                let uri = uri.clone();
                Box::pin(async move { backtest::settle::run(&uri).await })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        },
    ));

    info!("daemon: all subtasks spawned; awaiting Ctrl+C");
    signal::ctrl_c().await.context("install Ctrl+C handler")?;
    info!("daemon: Ctrl+C received; broadcasting shutdown");
    let _ = shutdown_tx.send(true);

    // Join everything before returning so the operator sees terminal
    // logs from each subtask. Ignore JoinError — that just means the
    // task panicked, which we want to log but not promote to the
    // caller's exit code.
    let _ = tokio::join!(h_binance, h_polymarket, h_backtest, h_compare, h_settle);
    if let Some(h) = h_user_channel {
        let _ = h.await;
    }
    info!("daemon: clean exit");
    Ok(())
}

/// Load the CLOB credential triple from env. Returns `None` if any of
/// the three vars is missing or empty so callers can quietly skip
/// authenticated paths on paper-only deployments.
fn load_clob_creds_from_env() -> Option<ApiCreds> {
    let api_key = std::env::var("POLYMARKET_CLOB_API_KEY").ok()?;
    let secret = std::env::var("POLYMARKET_CLOB_SECRET").ok()?;
    let passphrase = std::env::var("POLYMARKET_CLOB_PASSPHRASE").ok()?;
    if api_key.is_empty() || secret.is_empty() || passphrase.is_empty() {
        return None;
    }
    Some(ApiCreds {
        api_key,
        secret,
        passphrase,
    })
}

/// One periodic-job loop. Names the task in log lines, optionally
/// skips the immediate first tick (so compare/settle don't try to read
/// before backtest has populated anything), and swallows individual
/// run errors with a warn-level log so a single bad iteration doesn't
/// kill the daemon.
async fn periodic<F>(
    label: &'static str,
    every: Duration,
    skip_first: bool,
    mut shutdown: watch::Receiver<bool>,
    mut body: F,
) where
    F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        + Send
        + 'static,
{
    let mut tick = interval(every);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    if skip_first {
        // tokio's interval's first tick fires immediately by default —
        // for compare-pnl/settle-pnl that's wasteful on a cold daemon.
        tick.tick().await;
    }
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("daemon[{label}]: shutdown");
                    return;
                }
            }
            _ = tick.tick() => {
                info!("daemon[{label}]: tick");
                if let Err(e) = body().await {
                    warn!("daemon[{label}]: iteration failed: {e}");
                }
            }
        }
    }
}
