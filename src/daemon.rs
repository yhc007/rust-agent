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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::signal;
use tokio::sync::{watch, RwLock};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use crate::backtest::{self, BacktestPlan};
use crate::coredb::btc::BtcTickRepo;
use crate::coredb::markets::MarketRepo;
use crate::coredb::orders::{OrderRepo, PositionRepo};
use crate::coredb::types::now_ms;
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
    /// LLM strategy presets to run on every periodic backtest. Empty
    /// = legacy `--both` (baseline + single env-resolved LLM).
    /// Non-empty = N-way run with these presets, with baseline always
    /// included (the daemon's whole point is comparison).
    pub llm_presets: Vec<String>,
    /// When set, the daemon also serves a tiny `/health` JSON
    /// endpoint on `0.0.0.0:<port>`. Intended for external monitors
    /// / reverse proxies / systemd watchdog scripts. `None` keeps
    /// the daemon HTTP-free for setups that don't want a listener.
    pub health_port: Option<u16>,
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
            llm_presets: Vec::new(),
            health_port: None,
        }
    }
}

/// Per-periodic-task heartbeat. Each iteration writes
/// `last_tick_ms`; success updates `last_success_ms`; failure
/// records `last_error` + bumps `consecutive_errors` (reset on the
/// next success). `/health` aggregates these into the response body.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SubtaskHealth {
    pub last_tick_ms: Option<i64>,
    pub last_success_ms: Option<i64>,
    pub last_error_ms: Option<i64>,
    pub last_error: Option<String>,
    pub consecutive_errors: u32,
}

impl SubtaskHealth {
    fn record_tick(&mut self) {
        self.last_tick_ms = Some(now_ms());
    }
    fn record_result(&mut self, result: &Result<()>) {
        let ts = now_ms();
        match result {
            Ok(_) => {
                self.last_success_ms = Some(ts);
                self.consecutive_errors = 0;
                // Don't clear last_error — the operator may want the
                // most recent failure context even after recovery.
            }
            Err(e) => {
                self.last_error_ms = Some(ts);
                // Truncate so a 50KB anyhow chain doesn't blow up the
                // JSON response. Operators chasing the full error
                // still have journalctl.
                self.last_error = Some(truncate_error(&format!("{e:#}"), 512));
                self.consecutive_errors = self.consecutive_errors.saturating_add(1);
            }
        }
    }
}

fn truncate_error(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        s.chars().take(max).collect::<String>() + "…"
    }
}

#[derive(Debug, Default)]
pub struct HealthState {
    pub started_at_ms: i64,
    pub backtest: SubtaskHealth,
    pub compare: SubtaskHealth,
    pub settle: SubtaskHealth,
    pub user_channel_present: bool,
}

#[derive(Serialize)]
struct HealthResponse {
    /// "ok" iff no subtask has more than [`UNHEALTHY_AFTER_ERRORS`]
    /// consecutive errors *and* ingest cache rows are fresher than
    /// [`INGEST_STALE_MS`]. Otherwise "degraded". Surfaced as the
    /// top-level field so a reverse-proxy probe can grep on it.
    status: &'static str,
    started_at_ms: i64,
    now_ms: i64,
    uptime_secs: i64,
    backtest: SubtaskHealth,
    compare: SubtaskHealth,
    settle: SubtaskHealth,
    /// True when the user-channel WS listener was spawned at startup.
    /// False is normal for paper-only deployments and doesn't degrade
    /// the overall status.
    user_channel_present: bool,
    /// Age in ms of the newest `btc_ticks` row. `None` if the table
    /// can't be read at all (CoreDB down, table missing, etc.). The
    /// status check treats `None` as stale.
    ingest_btc_age_ms: Option<i64>,
    /// Age in ms of the newest `markets.updated_at_ms`. Same
    /// semantics as `ingest_btc_age_ms`.
    ingest_polymarket_age_ms: Option<i64>,
}

const UNHEALTHY_AFTER_ERRORS: u32 = 3;
/// Ingest is considered stale once nothing has landed for 5 minutes.
/// Binance bookTicker writes ~2 rows/s normally, Polymarket Gamma
/// poll is every 30 s, so this is a generous slack.
const INGEST_STALE_MS: i64 = 5 * 60 * 1000;

/// Which periodic subtask is reporting a tick. Used by [`periodic`]
/// to write into the correct slot of the shared [`HealthState`].
#[derive(Debug, Clone, Copy)]
enum TaskKind {
    Backtest,
    Compare,
    Settle,
}

pub async fn run(cfg: DaemonConfig) -> Result<()> {
    let DaemonConfig {
        coredb_uri,
        backtest_every_secs,
        compare_every_secs,
        settle_every_secs,
        execute,
        live,
        llm_presets,
        health_port,
    } = cfg;

    println!("🛰️  daemon: starting");
    println!("    coredb_uri        = {coredb_uri}");
    let plan_label = if llm_presets.is_empty() {
        "both (baseline + single LLM)".to_string()
    } else {
        format!("baseline + [{}]", llm_presets.join(","))
    };
    println!(
        "    backtest every    = {backtest_every_secs}s (plan={plan_label}, execute={execute}, live={live})"
    );
    println!("    compare-pnl every = {compare_every_secs}s");
    println!("    settle-pnl every  = {settle_every_secs}s");
    if let Some(port) = health_port {
        println!("    health endpoint   = http://0.0.0.0:{port}/health");
    }
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

    let health = Arc::new(RwLock::new(HealthState {
        started_at_ms: now_ms(),
        ..HealthState::default()
    }));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // 1. Ingest pollers (long-running tasks). These already accept a
    //    watch::Receiver and exit cleanly when it flips to `true`.
    //    Clone the repo handles for the health endpoint before
    //    they move into the spawn — the handler reads them too.
    let btc_repo_for_health = btc_repo.clone();
    let market_repo_for_health = market_repo.clone();
    let h_binance = tokio::spawn(binance::run(btc_repo, shutdown_rx.clone()));
    let h_polymarket = tokio::spawn(polymarket::run(market_repo, shutdown_rx.clone()));

    // Polymarket user-channel WS listener — only spawned when CLOB
    // credentials are visible in env. Paper-only deployments skip
    // this cleanly. The listener writes back to the `orders` table
    // only when APPLY_FILLS=1 is also exported (defense-in-depth:
    // first stand it up observation-only, eyeball the payloads,
    // then opt into mutation).
    // systemd watchdog: if WATCHDOG_USEC is set in env (which it is
    // iff the unit declared WatchdogSec=), spawn a task that pings
    // sd_notify(WATCHDOG=1) at half that interval. If the tokio
    // runtime ever deadlocks, the pings stop and systemd restarts
    // the unit. No-op for non-systemd invocations.
    let h_watchdog = spawn_watchdog(shutdown_rx.clone());

    let h_user_channel = match load_clob_creds_from_env() {
        Some(creds) => {
            health.write().await.user_channel_present = true;
            let apply = matches!(std::env::var("APPLY_FILLS").as_deref(), Ok("1"));
            let (order_repo_for_ws, pos_repo_for_ws) = if apply {
                (
                    Some(Arc::new(OrderRepo::new(db.session()).await?)),
                    Some(Arc::new(PositionRepo::new(db.session()).await?)),
                )
            } else {
                (None, None)
            };
            info!(
                "daemon: CLOB creds present; spawning user-channel listener (apply_fills={apply})"
            );
            Some(tokio::spawn(user_channel::run(
                std::sync::Arc::new(creds),
                order_repo_for_ws,
                pos_repo_for_ws,
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
        TaskKind::Backtest,
        Duration::from_secs(backtest_every_secs),
        /* skip_first = */ false,
        shutdown_rx.clone(),
        health.clone(),
        {
            let uri = coredb_uri.clone();
            let presets = llm_presets.clone();
            move || {
                let uri = uri.clone();
                let presets = presets.clone();
                Box::pin(async move {
                    let plan = if presets.is_empty() {
                        BacktestPlan::both()
                    } else {
                        BacktestPlan::multi(true, presets)
                    };
                    backtest::run::run(&uri, plan, execute, live).await
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        },
    ));

    let h_compare = tokio::spawn(periodic(
        "compare-pnl",
        TaskKind::Compare,
        Duration::from_secs(compare_every_secs),
        /* skip_first = */ true,
        shutdown_rx.clone(),
        health.clone(),
        {
            let uri = coredb_uri.clone();
            move || {
                let uri = uri.clone();
                Box::pin(async move { backtest::compare::run(&uri, None, 1).await })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        },
    ));

    let h_settle = tokio::spawn(periodic(
        "settle-pnl",
        TaskKind::Settle,
        Duration::from_secs(settle_every_secs),
        /* skip_first = */ true,
        shutdown_rx.clone(),
        health.clone(),
        {
            let uri = coredb_uri.clone();
            move || {
                let uri = uri.clone();
                Box::pin(async move { backtest::settle::run(&uri).await })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        },
    ));

    // Optional /health HTTP endpoint. The handler reads `health`
    // (periodic-task heartbeats), plus `btc_repo` / `market_repo`
    // for ingest staleness, so it always reflects the latest state
    // without polling. Bound to 0.0.0.0 so a sidecar / reverse
    // proxy can scrape it without extra config.
    let h_health = if let Some(port) = health_port {
        let addr = SocketAddr::from(([0, 0, 0, 0], port));
        let app_state = HealthAppState {
            health: health.clone(),
            btc_repo: btc_repo_for_health,
            market_repo: market_repo_for_health,
        };
        let app = Router::new()
            .route("/health", get(health_handler))
            .with_state(app_state);
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("daemon: bind health endpoint at {addr}"))?;
        info!("daemon: /health listening on {addr}");
        // axum::serve with graceful shutdown so the daemon's Ctrl+C
        // tears the listener down without orphaning the port.
        let mut health_shutdown = shutdown_rx.clone();
        Some(tokio::spawn(async move {
            let server = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = health_shutdown.changed().await;
                });
            if let Err(e) = server.await {
                warn!("daemon: health server exited: {e}");
            }
        }))
    } else {
        None
    };

    info!("daemon: all subtasks spawned; awaiting Ctrl+C");
    // Tell systemd we're ready. No-op when not under systemd
    // (NOTIFY_SOCKET is unset). Must come *after* listeners are up
    // so a `systemctl --user start --wait` actually waits for the
    // service to be operational, not just for `daemon` to fork.
    if let Err(e) = sd_notify::notify(false, &[sd_notify::NotifyState::Ready]) {
        warn!("daemon: sd_notify(READY=1) failed: {e}");
    }
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
    if let Some(h) = h_health {
        let _ = h.await;
    }
    if let Some(h) = h_watchdog {
        let _ = h.await;
    }
    // STOPPING=1 lets systemd distinguish a clean shutdown from a
    // crash; helps `systemctl --user status` show the right state.
    let _ = sd_notify::notify(false, &[sd_notify::NotifyState::Stopping]);
    info!("daemon: clean exit");
    Ok(())
}

/// Spawn the systemd watchdog pinger if `WATCHDOG_USEC` is in env
/// (i.e. the unit declared `WatchdogSec=`). Returns `None` when not
/// running under systemd / watchdog disabled, so the daemon path is
/// free of any sd-notify dependency outside of unit-managed runs.
///
/// Pings at half the configured watchdog interval — the canonical
/// safety margin from `man sd_watchdog_enabled(3)`. If the tokio
/// runtime ever deadlocks, this task can't run, pings stop, and
/// systemd restarts the unit per its `Restart=` policy.
fn spawn_watchdog(
    mut shutdown: watch::Receiver<bool>,
) -> Option<tokio::task::JoinHandle<()>> {
    let mut usec = 0u64;
    if !sd_notify::watchdog_enabled(false, &mut usec) {
        return None;
    }
    // sd_notify returns microseconds; ping at half that, with a floor
    // of 1 s so a misconfigured 100ms watchdog doesn't pin a CPU.
    let interval = Duration::from_micros(usec / 2).max(Duration::from_secs(1));
    info!(
        "daemon: systemd watchdog enabled (WatchdogSec={}us, pinging every {:?})",
        usec, interval
    );
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!("daemon[watchdog]: shutdown");
                        return;
                    }
                }
                _ = tick.tick() => {
                    if let Err(e) = sd_notify::notify(false, &[sd_notify::NotifyState::Watchdog]) {
                        warn!("daemon[watchdog]: notify failed: {e}");
                    }
                }
            }
        }
    }))
}

#[derive(Clone)]
struct HealthAppState {
    health: Arc<RwLock<HealthState>>,
    btc_repo: Arc<BtcTickRepo>,
    market_repo: Arc<MarketRepo>,
}

async fn health_handler(State(s): State<HealthAppState>) -> Json<HealthResponse> {
    let now = now_ms();
    let snap = s.health.read().await;

    // Ingest freshness: read each repo's latest row. Treat any error
    // (including "table empty") as missing data, which the status
    // line will then flag as degraded.
    let btc_age = match s.btc_repo.latest("BTCUSDT").await {
        Ok(Some(t)) => Some(now.saturating_sub(t.ts_ms)),
        _ => None,
    };
    let polymarket_age = match s.market_repo.list_open().await {
        Ok(rows) if !rows.is_empty() => {
            let newest = rows.iter().map(|m| m.updated_at_ms).max().unwrap_or(0);
            Some(now.saturating_sub(newest))
        }
        _ => None,
    };

    let ingest_ok = match (btc_age, polymarket_age) {
        (Some(b), Some(p)) => b < INGEST_STALE_MS && p < INGEST_STALE_MS,
        _ => false,
    };
    let periodic_ok = snap.backtest.consecutive_errors < UNHEALTHY_AFTER_ERRORS
        && snap.compare.consecutive_errors < UNHEALTHY_AFTER_ERRORS
        && snap.settle.consecutive_errors < UNHEALTHY_AFTER_ERRORS;
    let status = if ingest_ok && periodic_ok { "ok" } else { "degraded" };

    Json(HealthResponse {
        status,
        started_at_ms: snap.started_at_ms,
        now_ms: now,
        uptime_secs: (now - snap.started_at_ms) / 1000,
        backtest: snap.backtest.clone(),
        compare: snap.compare.clone(),
        settle: snap.settle.clone(),
        user_channel_present: snap.user_channel_present,
        ingest_btc_age_ms: btc_age,
        ingest_polymarket_age_ms: polymarket_age,
    })
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
/// before backtest has populated anything), records every tick /
/// success / error into the shared health state, and swallows
/// individual run errors with a warn-level log so a single bad
/// iteration doesn't kill the daemon.
async fn periodic<F>(
    label: &'static str,
    kind: TaskKind,
    every: Duration,
    skip_first: bool,
    mut shutdown: watch::Receiver<bool>,
    health: Arc<RwLock<HealthState>>,
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
                {
                    let mut h = health.write().await;
                    slot_mut(&mut h, kind).record_tick();
                }
                let result = body().await;
                if let Err(ref e) = result {
                    warn!("daemon[{label}]: iteration failed: {e}");
                }
                {
                    let mut h = health.write().await;
                    slot_mut(&mut h, kind).record_result(&result);
                }
            }
        }
    }
}

/// Pick the right [`SubtaskHealth`] field for a given [`TaskKind`].
/// Centralized so the mapping isn't repeated at every call site.
fn slot_mut(h: &mut HealthState, kind: TaskKind) -> &mut SubtaskHealth {
    match kind {
        TaskKind::Backtest => &mut h.backtest,
        TaskKind::Compare => &mut h.compare,
        TaskKind::Settle => &mut h.settle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn record_success_clears_consecutive_errors() {
        let mut h = SubtaskHealth::default();
        h.record_result(&Err(anyhow!("boom")));
        h.record_result(&Err(anyhow!("boom again")));
        assert_eq!(h.consecutive_errors, 2);
        h.record_result(&Ok::<(), anyhow::Error>(()));
        assert_eq!(h.consecutive_errors, 0);
        // last_error stays even after recovery — operators still want
        // the most recent failure context for postmortems.
        assert!(h.last_error.is_some());
        assert!(h.last_success_ms.is_some());
    }

    #[test]
    fn record_error_accumulates_and_truncates_message() {
        let mut h = SubtaskHealth::default();
        let huge_msg = "x".repeat(2000);
        h.record_result(&Err::<(), anyhow::Error>(anyhow!(huge_msg)));
        let msg = h.last_error.as_deref().unwrap();
        // 512-char cap + ellipsis.
        assert!(msg.chars().count() <= 513, "got {} chars", msg.chars().count());
        assert!(msg.ends_with('…'));
    }

    #[test]
    fn slot_mut_routes_each_kind() {
        let mut h = HealthState::default();
        slot_mut(&mut h, TaskKind::Backtest).record_tick();
        slot_mut(&mut h, TaskKind::Compare).record_tick();
        slot_mut(&mut h, TaskKind::Settle).record_tick();
        assert!(h.backtest.last_tick_ms.is_some());
        assert!(h.compare.last_tick_ms.is_some());
        assert!(h.settle.last_tick_ms.is_some());
    }

    /// Watchdog must be a no-op when running outside systemd (no
    /// WATCHDOG_USEC env). The function returns `None` so the daemon
    /// doesn't spawn a pinger task that would just churn errors.
    #[tokio::test]
    async fn watchdog_disabled_returns_none_without_env() {
        // Some test runners propagate systemd vars from the parent;
        // remove them explicitly so this test is hermetic.
        std::env::remove_var("WATCHDOG_USEC");
        std::env::remove_var("WATCHDOG_PID");
        let (_tx, rx) = watch::channel(false);
        let h = spawn_watchdog(rx);
        assert!(h.is_none(), "watchdog should be disabled without WATCHDOG_USEC");
    }
}
