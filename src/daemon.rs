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
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tokio::signal;
use tokio::sync::{watch, RwLock};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

use crate::backtest::{self, BacktestPlan};
use crate::coredb::btc::BtcTickRepo;
use crate::coredb::decisions::DecisionRepo;
use crate::coredb::markets::MarketRepo;
use crate::coredb::orders::{OrderRepo, PositionRepo};
use crate::coredb::pnl::PnlRepo;
use crate::coredb::pnl_breakdown::PnlBreakdownRepo;
use crate::coredb::types::{bucket_day, now_ms};
use crate::coredb::CoreDb;
use crate::data::{binance, polymarket, user_channel};
use crate::execution::clob_auth::ApiCreds;
use crate::risk::RiskLimits;

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

#[derive(Debug, Default, Clone)]
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
    /// Per-cache freshness ages in ms. `None` for a cache that has
    /// never been populated (e.g. on a brand-new daemon before the
    /// first /metrics scrape). Dashboard surfaces these in the chip
    /// detail line so a single stuck cache is visible without
    /// staring at the whole snapshot. NOT factored into `status` —
    /// each cache has its own TTL and "stuck" means different
    /// things for different sources (the pnl caches refresh hourly
    /// when settle-pnl runs; the orders cache refreshes when CQL
    /// writes land).
    cache_ages_ms: HealthCacheAges,
    /// Lifetime restart counts per ingest source. Monotonic across
    /// the daemon's lifetime; resets on process restart. Same
    /// source as the `agent_ingest_restarts_total{source=...}`
    /// counter — exposed on /health too so the dashboard can spot
    /// a flapping worker by comparing consecutive snapshots.
    ingest_restarts: IngestRestartsWire,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct IngestRestartsWire {
    pub binance: u64,
    pub polymarket: u64,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct HealthCacheAges {
    pub decisions: Option<i64>,
    pub orders: Option<i64>,
    pub pnl_daily: Option<i64>,
    pub pnl_breakdown: Option<i64>,
    pub pnl_breakdown_yesterday: Option<i64>,
    pub pnl_breakdown_window: Option<i64>,
    pub positions: Option<i64>,
    pub ingest_probe: Option<i64>,
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

    // 1. Ingest pollers (long-running tasks). Wrap each in a
    //    supervisor that respawns the worker if the corresponding
    //    repo's newest row goes stale past INGEST_STALE_RESTART_S
    //    seconds. Covers stuck WS connections / hung HTTP polls
    //    where the inner reconnect loop never fires because the
    //    OS sees the socket as alive. The supervisor itself
    //    observes the shared shutdown signal so a daemon Ctrl+C
    //    drops both supervisor + worker cleanly.
    //
    //    Clone the repo handles for the health endpoint before
    //    they move into the supervisor — the handler reads them
    //    too.
    let btc_repo_for_health = btc_repo.clone();
    let market_repo_for_health = market_repo.clone();
    let stale_threshold_ms = ingest_stale_restart_ms();
    info!(
        "daemon: ingest watchdog threshold = {}ms (0 disables)",
        stale_threshold_ms,
    );
    let ingest_restarts = Arc::new(IngestRestartCounters::default());
    let h_binance = tokio::spawn({
        let btc_repo = btc_repo.clone();
        let shutdown_rx = shutdown_rx.clone();
        let restart_counter = Arc::clone(&ingest_restarts);
        async move {
            supervised_ingest(
                "binance",
                stale_threshold_ms,
                {
                    let btc_repo = btc_repo.clone();
                    move || {
                        let r = btc_repo.clone();
                        async move {
                            r.latest("BTCUSDT")
                                .await
                                .ok()
                                .flatten()
                                .map(|t| (now_ms() - t.ts_ms).max(0))
                        }
                    }
                },
                {
                    let btc_repo = btc_repo.clone();
                    move |sub_shutdown| {
                        let r = btc_repo.clone();
                        tokio::spawn(async move { binance::run(r, sub_shutdown).await })
                    }
                },
                shutdown_rx,
                Some(Arc::clone(&restart_counter.binance)),
            )
            .await;
            Ok::<(), anyhow::Error>(())
        }
    });
    let h_polymarket = tokio::spawn({
        let market_repo = market_repo.clone();
        let shutdown_rx = shutdown_rx.clone();
        let restart_counter = Arc::clone(&ingest_restarts);
        async move {
            supervised_ingest(
                "polymarket",
                stale_threshold_ms,
                {
                    let market_repo = market_repo.clone();
                    move || {
                        let r = market_repo.clone();
                        async move {
                            r.list_open()
                                .await
                                .ok()
                                .and_then(|rows| {
                                    rows.iter().map(|m| m.updated_at_ms).max()
                                })
                                .map(|t| (now_ms() - t).max(0))
                        }
                    }
                },
                {
                    let market_repo = market_repo.clone();
                    move |sub_shutdown| {
                        let r = market_repo.clone();
                        tokio::spawn(async move { polymarket::run(r, sub_shutdown).await })
                    }
                },
                shutdown_rx,
                Some(Arc::clone(&restart_counter.polymarket)),
            )
            .await;
            Ok::<(), anyhow::Error>(())
        }
    });

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
                Box::pin(async move { backtest::compare::run(&uri, None, 1, false).await })
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
                Box::pin(async move { backtest::settle::run(&uri, false).await })
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
        // DecisionRepo for the per-scrape decisions-today count. Built
        // once at startup so each /metrics hit just runs the query.
        // Wrapped in Arc so HealthAppState's Clone stays cheap.
        let decision_repo = DecisionRepo::new(db.session()).await.ok().map(Arc::new);
        let order_repo = OrderRepo::new(db.session()).await.ok().map(Arc::new);
        let pnl_repo = PnlRepo::new(db.session()).await.ok().map(Arc::new);
        let pnl_breakdown_repo = PnlBreakdownRepo::new(db.session())
            .await
            .ok()
            .map(Arc::new);
        let position_repo_for_metrics = PositionRepo::new(db.session()).await.ok().map(Arc::new);
        let app_state = HealthAppState {
            health: health.clone(),
            btc_repo: btc_repo_for_health,
            market_repo: market_repo_for_health,
            decision_repo,
            decisions_cache: Arc::new(RwLock::new(None)),
            order_repo,
            orders_cache: Arc::new(RwLock::new(None)),
            pnl_repo,
            pnl_daily_cache: Arc::new(RwLock::new(None)),
            pnl_breakdown_repo,
            pnl_breakdown_cache: Arc::new(RwLock::new(None)),
            pnl_breakdown_yesterday_cache: Arc::new(RwLock::new(None)),
            pnl_breakdown_window_cache: Arc::new(RwLock::new(None)),
            position_repo_for_metrics,
            positions_cache: Arc::new(RwLock::new(None)),
            ingest_cache: Arc::new(RwLock::new(None)),
            ingest_restarts: Arc::clone(&ingest_restarts),
        };
        let app = Router::new()
            .route("/health", get(health_handler))
            .route("/metrics", get(metrics_handler))
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

/// Default TTL on the cached decisions-per-(strategy,side) tally
/// that `/metrics` emits. Prometheus default scrape is every 15s,
/// so 10s keeps the daemon's CoreDB hit rate at most ~1/10s under
/// whatever scrape concurrency. Tuning higher trades freshness for
/// load; tuning lower hits CoreDB more aggressively for marginal
/// benefit since the underlying data only ticks on backtest runs
/// (every 30 min in the default daemon config). Operator-tunable
/// via `METRICS_DECISIONS_CACHE_TTL_S` env. The orders cache
/// shares this TTL — same data-churn cadence.
pub const METRICS_DECISIONS_CACHE_TTL_MS_DEFAULT: i64 = 10_000;

/// Read `METRICS_DECISIONS_CACHE_TTL_S` env (seconds), fall back
/// to the const default when unset or malformed.
pub fn metrics_decisions_cache_ttl_ms() -> i64 {
    std::env::var("METRICS_DECISIONS_CACHE_TTL_S")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(METRICS_DECISIONS_CACHE_TTL_MS_DEFAULT)
}

/// Default TTL on the cached ingest-staleness probes. Shorter than
/// the decisions cache because the underlying data ticks every
/// ~500ms (Binance WS) and every 30s (Polymarket Gamma) — a 5s
/// ceiling preserves real staleness signal while still saving
/// CoreDB the per-scrape btc.latest + markets.list_open round-
/// trips that /health and /metrics each used to issue
/// independently. Operator-tunable via `INGEST_CACHE_TTL_S`.
pub const INGEST_CACHE_TTL_MS_DEFAULT: i64 = 5_000;

pub fn ingest_cache_ttl_ms() -> i64 {
    std::env::var("INGEST_CACHE_TTL_S")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(INGEST_CACHE_TTL_MS_DEFAULT)
}

/// Default threshold for the ingest watchdog. When the newest btc
/// tick (or polymarket market) is older than this, the supervisor
/// aborts the running worker and respawns a fresh one — covers
/// stuck WS connections where the underlying reconnect loop never
/// fires because the OS sees the socket as alive. Operator can
/// override via `INGEST_STALE_RESTART_S` env (seconds). Set to 0 to
/// disable auto-restart entirely.
const DEFAULT_INGEST_STALE_RESTART_MS: i64 = 5 * 60 * 1000;

fn ingest_stale_restart_ms() -> i64 {
    std::env::var("INGEST_STALE_RESTART_S")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(DEFAULT_INGEST_STALE_RESTART_MS)
}

#[derive(Debug, Clone)]
struct DecisionsCacheEntry {
    fetched_at_ms: i64,
    counts: std::collections::BTreeMap<(String, String), u32>,
}

/// Cached today's-orders tally keyed by (strategy, side, exec,
/// status). Same stale-while-revalidate pattern + 10s TTL as
/// decisions, since orders churn at the same backtest-tick cadence
/// (~30 min) and a 10s freshness ceiling is plenty.
#[derive(Debug, Clone)]
struct OrdersCacheEntry {
    fetched_at_ms: i64,
    counts: std::collections::BTreeMap<(String, String, String, String), u32>,
}

/// Cached today's `pnl_daily` row. Realized PnL only changes when
/// `settle-pnl` runs and markets resolve — both hours-scale events
/// — so a 60s TTL is plenty of freshness without hammering CoreDB.
/// The row is small (single-row partition lookup) but the cache
/// keeps scrape latency bounded.
#[derive(Debug, Clone)]
struct PnlDailyCacheEntry {
    fetched_at_ms: i64,
    /// `None` when no row exists for today yet — settle-pnl hasn't
    /// run, or no orders have resolved.
    row: Option<crate::coredb::types::PnlDaily>,
}

/// Default TTL for the pnl_daily cache. Longer than the decisions
/// cache because realized PnL only ticks when settle-pnl runs
/// (default 1h cadence in the daemon). Same TTL is used for the
/// pnl_breakdown today / yesterday / window caches — all written
/// by the same settle-pnl call. Operator-tunable via
/// `METRICS_PNL_DAILY_CACHE_TTL_S` env.
pub const METRICS_PNL_DAILY_CACHE_TTL_MS_DEFAULT: i64 = 60_000;

pub fn metrics_pnl_daily_cache_ttl_ms() -> i64 {
    std::env::var("METRICS_PNL_DAILY_CACHE_TTL_S")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(METRICS_PNL_DAILY_CACHE_TTL_MS_DEFAULT)
}

/// Width of the rolling pnl_breakdown window metric, in days.
/// Hard-coded — exposed to Grafana as the literal label `days="7"`
/// in `agent_pnl_breakdown_window_*` so the panel query is
/// trivially `sum by (strategy)(agent_pnl_breakdown_window_realized_usd)`.
/// One UTC bucket_day per offset, anchored on `now`.
const PNL_BREAKDOWN_WINDOW_DAYS: i64 = 7;

/// Cached today's pnl_breakdown rows (per strategy × exec).
/// Same TTL as pnl_daily — both are written by the same
/// settle-pnl call.
#[derive(Debug, Clone)]
struct PnlBreakdownCacheEntry {
    fetched_at_ms: i64,
    rows: Vec<crate::coredb::types::PnlBreakdown>,
}

/// Cached aggregate over the last N=`PNL_BREAKDOWN_WINDOW_DAYS`
/// UTC days. The rows here are already summed per (strategy, exec)
/// across the window, so the renderer emits one series per pair
/// without touching CoreDB on the hot path. `anchor_bucket_day_ms`
/// is today's bucket at refresh time — when `bucket_day(now)` no
/// longer matches it (i.e. the day rolled), the cache is invalidated
/// even if the TTL hasn't expired.
#[derive(Debug, Clone)]
struct PnlBreakdownWindowCacheEntry {
    fetched_at_ms: i64,
    anchor_bucket_day_ms: i64,
    rows: Vec<crate::coredb::types::PnlBreakdown>,
}

/// Cached open-positions snapshot. Positions only change on fill
/// events (paper fills land synchronously; live fills land
/// through the user-channel WS listener), so a 10s TTL is plenty
/// of freshness and saves CoreDB the per-scrape list_all call.
#[derive(Debug, Clone)]
struct PositionsCacheEntry {
    fetched_at_ms: i64,
    rows: Vec<crate::coredb::types::Position>,
}

/// Cached pair of ingest-staleness ages. Stored as a unit (not
/// per-source) so a transient one-source outage shows up in the
/// next refresh as `None` rather than being masked by per-source
/// stale-while-revalidate — operators want the gap to fire, not
/// the last-known-good age to keep painting.
#[derive(Debug, Clone)]
struct IngestProbeCache {
    fetched_at_ms: i64,
    btc_age_ms: Option<i64>,
    polymarket_age_ms: Option<i64>,
}

#[derive(Clone)]
struct HealthAppState {
    health: Arc<RwLock<HealthState>>,
    btc_repo: Arc<BtcTickRepo>,
    market_repo: Arc<MarketRepo>,
    /// Used by `/metrics` to count today's decisions per (strategy,
    /// side). Optional so a future deployment that wants to disable
    /// the per-scrape decisions read (e.g. for cost) can leave this
    /// `None` without breaking the rest of the metrics output.
    decision_repo: Option<Arc<DecisionRepo>>,
    /// Stale-while-revalidate cache for the decisions tally. Saves
    /// CoreDB a `list_day` per scrape under high-frequency
    /// scraping. Refresh failures keep the previous entry around
    /// so transient CoreDB blips don't blank the Grafana panel.
    decisions_cache: Arc<RwLock<Option<DecisionsCacheEntry>>>,
    /// Used by `/metrics` for the agent_orders_today gauge. Needs
    /// the decision_repo too (above) to join orders back to their
    /// originating strategy via decision_id.
    order_repo: Option<Arc<OrderRepo>>,
    orders_cache: Arc<RwLock<Option<OrdersCacheEntry>>>,
    /// Used by `/metrics` for the agent_pnl_daily_realized_usd
    /// gauge. settle-pnl writes here; the metrics handler reads.
    pnl_repo: Option<Arc<PnlRepo>>,
    pnl_daily_cache: Arc<RwLock<Option<PnlDailyCacheEntry>>>,
    /// Sibling to pnl_repo: per-(strategy, exec) realized PnL.
    /// Also written by settle-pnl, also cached with the 60s pnl TTL.
    pnl_breakdown_repo: Option<Arc<PnlBreakdownRepo>>,
    pnl_breakdown_cache: Arc<RwLock<Option<PnlBreakdownCacheEntry>>>,
    /// Yesterday's pnl_breakdown — fixed once settle-pnl has done
    /// its post-midnight pass. Surfaced via
    /// agent_pnl_breakdown_yesterday_* so a Grafana panel can plot
    /// "yesterday's final" as a baseline next to today's running
    /// realized PnL. Same 60s TTL since the underlying data only
    /// changes when settle-pnl runs.
    pnl_breakdown_yesterday_cache: Arc<RwLock<Option<PnlBreakdownCacheEntry>>>,
    /// Rolling N-day window over pnl_breakdown — same repo as the
    /// today/yesterday caches above, but each refresh sums the last
    /// PNL_BREAKDOWN_WINDOW_DAYS UTC days into a single rowset per
    /// (strategy, exec). Cached so the metrics handler doesn't fan
    /// out 7 list_day calls per scrape; invalidated on day-roll so
    /// the window stays anchored to "today".
    pnl_breakdown_window_cache: Arc<RwLock<Option<PnlBreakdownWindowCacheEntry>>>,
    /// Open positions read from positions_v2 for the
    /// agent_open_positions_* gauges. 10s scrape cache.
    position_repo_for_metrics: Option<Arc<PositionRepo>>,
    positions_cache: Arc<RwLock<Option<PositionsCacheEntry>>>,
    /// Shared cache for the two ingest-staleness probes that
    /// `/health` and `/metrics` both need. Refresh on TTL expiry
    /// runs both probes once and stores whatever comes back, so a
    /// concurrent scrape on the *other* endpoint reuses the work.
    ingest_cache: Arc<RwLock<Option<IngestProbeCache>>>,
    /// Lifetime restart count per ingest source, incremented by the
    /// `supervised_ingest` task each time it respawns its worker.
    /// Surfaced via `agent_ingest_restarts_total{source=...}` so a
    /// flapping worker is visible from Grafana before the next
    /// /health poll. Counter semantics: monotonic, reset on
    /// process restart.
    ingest_restarts: Arc<IngestRestartCounters>,
}

/// Per-source restart bookkeeping for the ingest supervisor. Holds
/// a lifetime count (monotonic; resets on process restart) and the
/// timestamp of the most recent restart (`0` when none has
/// happened yet). The latter feeds the daemon's "recent restart"
/// health downgrade — any restart in the last
/// [`RECENT_RESTART_MS`] window flips `/health.status` to
/// "degraded" so reverse-proxy probes surface a flapping worker
/// without comparing consecutive counts themselves.
#[derive(Debug, Default)]
pub struct RestartTracker {
    pub count: std::sync::atomic::AtomicU64,
    pub last_at_ms: std::sync::atomic::AtomicI64,
}

/// Per-source atomic counters for the ingest supervisor. Holds two
/// hot fields — one per ingest source — instead of a HashMap so
/// the supervisor's increment path is two atomic ops without lock
/// contention. Stored as Arc<RestartTracker> so the supervisor can
/// hold a strong reference for the lifetime of the spawned task
/// while the metrics + health handlers still read through the
/// parent Arc.
#[derive(Debug, Default)]
pub struct IngestRestartCounters {
    pub binance: Arc<RestartTracker>,
    pub polymarket: Arc<RestartTracker>,
}

/// Window during which a restart event keeps daemon status
/// degraded (default). 5 min is long enough that a single
/// transient flap stays visible to a Prometheus scrape but short
/// enough that a recovered worker reports green on the next
/// sustained healthy stretch. Operator-tunable via the
/// `RECENT_RESTART_S` env var.
pub const RECENT_RESTART_MS_DEFAULT: i64 = 5 * 60 * 1000;

/// Read `RECENT_RESTART_S` env (seconds), fall back to
/// [`RECENT_RESTART_MS_DEFAULT`] when unset or malformed. Set 0
/// to disable the recent-restart contribution to status (the
/// counters still increment; just don't downgrade).
pub fn recent_restart_ms() -> i64 {
    std::env::var("RECENT_RESTART_S")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(RECENT_RESTART_MS_DEFAULT)
}

async fn health_handler(State(s): State<HealthAppState>) -> Json<HealthResponse> {
    let now = now_ms();
    let inputs = gather_health_inputs(&s, now).await;
    Json(compute_health_response(&inputs))
}

/// Pre-gathered inputs the pure `compute_health_response` function
/// needs. Same gather/render split idea as `metrics_handler` —
/// keeps cache locks acquired only during the async refresh, and
/// makes the status / serialization logic testable in isolation
/// without scylla sessions.
#[derive(Debug, Clone)]
pub struct HealthInputs {
    pub now_ms: i64,
    pub health: HealthState,
    pub btc_age_ms: Option<i64>,
    pub polymarket_age_ms: Option<i64>,
    pub cache_ages: HealthCacheAges,
    pub ingest_restarts: IngestRestartsWire,
    /// Timestamp (ms since epoch) of the most recent supervisor
    /// restart, across both ingest sources. `None` when no
    /// restart has happened in this process's lifetime. Fed into
    /// the status check so a recent flap downgrades to "degraded"
    /// for `recent_restart_ms` after the event.
    pub last_restart_at_ms: Option<i64>,
    /// Operator-tuned threshold above which any cache age
    /// downgrades status to "degraded". Resolved once at gather
    /// time (from `STALE_CACHE_HEALTH_S` env, with the const
    /// default) so `compute_health_response` stays a pure
    /// function of its input.
    pub stale_cache_health_ms: i64,
    /// Operator-tuned window during which a recent ingest restart
    /// keeps status "degraded". Resolved from `RECENT_RESTART_S`
    /// env at gather time, same purity rationale as above.
    pub recent_restart_ms: i64,
}

async fn gather_health_inputs(s: &HealthAppState, now: i64) -> HealthInputs {
    let health = s.health.read().await.clone();
    let (btc_age_ms, polymarket_age_ms) = ingest_ages_cached(s, now).await;
    // Per-cache freshness — read each cache's `fetched_at_ms` under
    // a brief read lock and compute age. Order matters only for the
    // read-lock contention story (we acquire each lock sequentially
    // and release before moving on); the slot order in the struct
    // is alphabetical for stable JSON output.
    let cache_ages = HealthCacheAges {
        decisions: s
            .decisions_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
        orders: s
            .orders_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
        pnl_daily: s
            .pnl_daily_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
        pnl_breakdown: s
            .pnl_breakdown_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
        pnl_breakdown_yesterday: s
            .pnl_breakdown_yesterday_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
        pnl_breakdown_window: s
            .pnl_breakdown_window_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
        positions: s
            .positions_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
        ingest_probe: s
            .ingest_cache
            .read()
            .await
            .as_ref()
            .map(|c| (now - c.fetched_at_ms).max(0)),
    };
    let ingest_restarts = IngestRestartsWire {
        binance: s
            .ingest_restarts
            .binance
            .count
            .load(std::sync::atomic::Ordering::Relaxed),
        polymarket: s
            .ingest_restarts
            .polymarket
            .count
            .load(std::sync::atomic::Ordering::Relaxed),
    };
    // Most-recent restart across both sources. 0 means "no restart
    // ever in this process's lifetime" — map to None so the status
    // check doesn't accidentally fire on a fresh daemon.
    let last_b = s
        .ingest_restarts
        .binance
        .last_at_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    let last_p = s
        .ingest_restarts
        .polymarket
        .last_at_ms
        .load(std::sync::atomic::Ordering::Relaxed);
    let last_restart_at_ms = match (last_b, last_p) {
        (0, 0) => None,
        (b, 0) => Some(b),
        (0, p) => Some(p),
        (b, p) => Some(b.max(p)),
    };
    HealthInputs {
        now_ms: now,
        health,
        btc_age_ms,
        polymarket_age_ms,
        cache_ages,
        ingest_restarts,
        last_restart_at_ms,
        stale_cache_health_ms: stale_cache_health_ms(),
        recent_restart_ms: recent_restart_ms(),
    }
}

/// Pure /health response builder. Status is "ok" iff every periodic
/// subtask has `< UNHEALTHY_AFTER_ERRORS` consecutive errors AND
/// both ingest sources have an age below `INGEST_STALE_MS`. Any
/// `None` ingest age (probe failed entirely) counts as stale —
/// operators want the bad state to fire, not be masked by a
/// missing measurement. A cache that has been sitting past the
/// configured `stale_cache_health_ms` also downgrades to
/// "degraded" so reverse-proxy probes grep'ing `status` surface a
/// stuck cache without consulting `cache_ages_ms` directly.
///
/// 10-minute default chosen to mirror the dashboard's
/// stalest_cache_hint threshold so the chip + status field stay
/// in sync without operator coordination.
pub const STALE_CACHE_HEALTH_MS_DEFAULT: i64 = 10 * 60 * 1000;

/// Read `STALE_CACHE_HEALTH_S` env (seconds), fall back to
/// [`STALE_CACHE_HEALTH_MS_DEFAULT`] when unset or malformed.
/// Operators set 0 to disable the cache-staleness contribution
/// to status entirely (caches still report their ages on
/// /health.cache_ages_ms; just none of them downgrades status).
pub fn stale_cache_health_ms() -> i64 {
    std::env::var("STALE_CACHE_HEALTH_S")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(STALE_CACHE_HEALTH_MS_DEFAULT)
}

/// Returns true when ANY cache age exceeds `threshold_ms`. A
/// `None` slot means "never populated" — that's expected on a
/// fresh daemon and does NOT count as stale (operator already
/// sees this via the dashboard's empty-row state). Same convention
/// as the dashboard's `stalest_cache_hint` helper.
fn any_cache_stale(ages: &HealthCacheAges, threshold_ms: i64) -> bool {
    [
        ages.decisions,
        ages.orders,
        ages.pnl_daily,
        ages.pnl_breakdown,
        ages.pnl_breakdown_yesterday,
        ages.pnl_breakdown_window,
        ages.positions,
        ages.ingest_probe,
    ]
    .into_iter()
    .filter_map(|a| a)
    .any(|a| a > threshold_ms)
}

pub fn compute_health_response(inp: &HealthInputs) -> HealthResponse {
    let ingest_ok = match (inp.btc_age_ms, inp.polymarket_age_ms) {
        (Some(b), Some(p)) => b < INGEST_STALE_MS && p < INGEST_STALE_MS,
        _ => false,
    };
    let periodic_ok = inp.health.backtest.consecutive_errors < UNHEALTHY_AFTER_ERRORS
        && inp.health.compare.consecutive_errors < UNHEALTHY_AFTER_ERRORS
        && inp.health.settle.consecutive_errors < UNHEALTHY_AFTER_ERRORS;
    // Cache staleness disabled when threshold == 0 (operator opt-out).
    let cache_ok =
        inp.stale_cache_health_ms <= 0 || !any_cache_stale(&inp.cache_ages, inp.stale_cache_health_ms);
    // Recent ingest restart: any supervisor restart within the
    // configured window keeps daemon "degraded" so a reverse-
    // proxy probe surfaces a flapping worker without having to
    // diff the counters itself. Clock skew tolerated by clamping
    // the diff to non-negative; a far-future `last_restart_at_ms`
    // (would mean a corrupt timestamp) is treated as "ok" since
    // reading "from the future" makes less operational sense than
    // reading "old enough to be safe". Threshold <= 0 disables.
    let restart_ok = if inp.recent_restart_ms <= 0 {
        true
    } else {
        match inp.last_restart_at_ms {
            None => true,
            Some(t) => {
                let age = inp.now_ms.saturating_sub(t);
                age < 0 || age > inp.recent_restart_ms
            }
        }
    };
    let status = if ingest_ok && periodic_ok && cache_ok && restart_ok {
        "ok"
    } else {
        "degraded"
    };
    HealthResponse {
        status,
        started_at_ms: inp.health.started_at_ms,
        now_ms: inp.now_ms,
        uptime_secs: (inp.now_ms - inp.health.started_at_ms) / 1000,
        backtest: inp.health.backtest.clone(),
        compare: inp.health.compare.clone(),
        settle: inp.health.settle.clone(),
        user_channel_present: inp.health.user_channel_present,
        ingest_btc_age_ms: inp.btc_age_ms,
        ingest_polymarket_age_ms: inp.polymarket_age_ms,
        cache_ages_ms: inp.cache_ages.clone(),
        ingest_restarts: inp.ingest_restarts.clone(),
    }
}

/// Pre-gathered data the pure `render_metrics` renderer needs to
/// build a Prometheus exposition body. The async `metrics_handler`
/// is responsible for populating this from the live caches +
/// repos; the renderer just emits formatted lines so it can be
/// driven from a unit test without a real CoreDB.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub now_ms: i64,
    pub health: HealthState,
    pub btc_age_ms: Option<i64>,
    pub polymarket_age_ms: Option<i64>,
    pub decisions: Option<DecisionsCacheEntry>,
    pub orders: Option<OrdersCacheEntry>,
    pub pnl_daily: Option<PnlDailyCacheEntry>,
    pub pnl_breakdown: Option<PnlBreakdownCacheEntry>,
    pub pnl_breakdown_yesterday: Option<PnlBreakdownCacheEntry>,
    pub pnl_breakdown_window: Option<PnlBreakdownWindowCacheEntry>,
    pub positions: Option<PositionsCacheEntry>,
    /// Age (ms) of the shared ingest_probe cache. Stored separately
    /// from the per-cache slots above because the ingest probe
    /// itself doesn't surface its rows here — only its
    /// fetched_at_ms. Used by the agent_cache_age_seconds gauge
    /// family so operators see "ingest_probe" alongside the rest.
    pub ingest_probe_age_ms: Option<i64>,
    /// Lifetime restart counts per ingest source. Read at gather
    /// time so render_metrics has a snapshot, not a live atomic
    /// reference. Counter semantics → emitted as a Prometheus
    /// `counter` (suffix `_total`).
    pub ingest_restarts_binance: u64,
    pub ingest_restarts_polymarket: u64,
    pub risk: RiskLimits,
    /// Operator-facing comment lines to prepend at the top of the
    /// output (e.g. "# decisions_today refresh failed; serving
    /// cached data: ..."). Each entry is one line — no trailing
    /// newline; the renderer adds it. Prometheus parsers ignore
    /// `#`-prefixed lines.
    pub notes: Vec<String>,
}

// Need Clone for MetricsSnapshot; RiskLimits doesn't currently
// implement it. PathBuf + f64 are both Clone — add the derive.
impl Clone for RiskLimits {
    fn clone(&self) -> Self {
        RiskLimits {
            max_order_usd: self.max_order_usd,
            kill_switch_path: self.kill_switch_path.clone(),
        }
    }
}

/// Pure renderer: takes pre-gathered cache snapshots + health state
/// and returns the Prometheus exposition body. No async, no I/O,
/// no lock acquisition — driveable from any test without a CoreDB
/// session.
///
/// Behavioural contract (matched against the prior inline-handler
/// version): same metric family names, same label keys, same
/// "missing series = no data yet" convention, same headline-scalar
/// derivations. Operator-facing `# refresh failed` comments are
/// emitted at the top via `MetricsSnapshot.notes` so the renderer
/// itself is a straight function of its input.
pub fn render_metrics(s: &MetricsSnapshot) -> String {
    let now = s.now_ms;
    let snap = &s.health;
    let mut out = String::with_capacity(2048);

    for note in &s.notes {
        out.push_str(note);
        if !note.ends_with('\n') {
            out.push('\n');
        }
    }

    let uptime_secs = (now - snap.started_at_ms).max(0) / 1000;
    out.push_str("# HELP agent_uptime_seconds Process uptime in seconds.\n");
    out.push_str("# TYPE agent_uptime_seconds gauge\n");
    out.push_str(&format!("agent_uptime_seconds {uptime_secs}\n"));

    out.push_str(
        "# HELP agent_subtask_consecutive_errors Consecutive failed iterations per periodic subtask.\n",
    );
    out.push_str("# TYPE agent_subtask_consecutive_errors gauge\n");
    for (label, sub) in [
        ("backtest", &snap.backtest),
        ("compare", &snap.compare),
        ("settle", &snap.settle),
    ] {
        out.push_str(&format!(
            "agent_subtask_consecutive_errors{{task=\"{label}\"}} {}\n",
            sub.consecutive_errors,
        ));
    }

    out.push_str(
        "# HELP agent_subtask_last_tick_age_seconds Seconds since the subtask's last tick.\n",
    );
    out.push_str("# TYPE agent_subtask_last_tick_age_seconds gauge\n");
    for (label, sub) in [
        ("backtest", &snap.backtest),
        ("compare", &snap.compare),
        ("settle", &snap.settle),
    ] {
        if let Some(t) = sub.last_tick_ms {
            let age = (now - t).max(0) / 1000;
            out.push_str(&format!(
                "agent_subtask_last_tick_age_seconds{{task=\"{label}\"}} {age}\n",
            ));
        }
    }

    out.push_str(
        "# HELP agent_subtask_last_success_age_seconds Seconds since the subtask's last successful iteration.\n",
    );
    out.push_str("# TYPE agent_subtask_last_success_age_seconds gauge\n");
    for (label, sub) in [
        ("backtest", &snap.backtest),
        ("compare", &snap.compare),
        ("settle", &snap.settle),
    ] {
        if let Some(t) = sub.last_success_ms {
            let age = (now - t).max(0) / 1000;
            out.push_str(&format!(
                "agent_subtask_last_success_age_seconds{{task=\"{label}\"}} {age}\n",
            ));
        }
    }

    out.push_str(
        "# HELP agent_ingest_age_seconds Seconds since the newest ingest row for each source.\n",
    );
    out.push_str("# TYPE agent_ingest_age_seconds gauge\n");
    if let Some(age_ms) = s.btc_age_ms {
        out.push_str(&format!(
            "agent_ingest_age_seconds{{source=\"btc\"}} {}\n",
            age_ms.max(0) / 1000,
        ));
    }
    if let Some(age_ms) = s.polymarket_age_ms {
        out.push_str(&format!(
            "agent_ingest_age_seconds{{source=\"polymarket\"}} {}\n",
            age_ms.max(0) / 1000,
        ));
    }

    // Ingest watchdog restart counters. Monotonic across the
    // process lifetime — Prometheus `rate()` over a window
    // shows flapping workers. Counter (NOT gauge) so a Grafana
    // panel using `increase()` works as expected.
    out.push_str(
        "# HELP agent_ingest_restarts_total Times the ingest supervisor respawned its worker due to data staleness. Monotonic, resets on process restart.\n",
    );
    out.push_str("# TYPE agent_ingest_restarts_total counter\n");
    out.push_str(&format!(
        "agent_ingest_restarts_total{{source=\"binance\"}} {}\n",
        s.ingest_restarts_binance,
    ));
    out.push_str(&format!(
        "agent_ingest_restarts_total{{source=\"polymarket\"}} {}\n",
        s.ingest_restarts_polymarket,
    ));

    out.push_str(
        "# HELP agent_user_channel_present 1 when the Polymarket user-channel WS listener was spawned at startup.\n",
    );
    out.push_str("# TYPE agent_user_channel_present gauge\n");
    out.push_str(&format!(
        "agent_user_channel_present {}\n",
        if snap.user_channel_present { 1 } else { 0 },
    ));

    // Unified per-slot cache freshness gauge. Mirrors the
    // `cache_ages_ms` sub-object on /health so Grafana can chart
    // every cache's TTL behavior on one panel. A slot whose cache
    // was never populated (None) emits no series — same "missing =
    // no data yet" convention as the rest of the metrics output.
    let cache_age_slots: [(&str, Option<i64>); 8] = [
        (
            "decisions",
            s.decisions.as_ref().map(|c| (now - c.fetched_at_ms).max(0)),
        ),
        (
            "orders",
            s.orders.as_ref().map(|c| (now - c.fetched_at_ms).max(0)),
        ),
        (
            "pnl_daily",
            s.pnl_daily.as_ref().map(|c| (now - c.fetched_at_ms).max(0)),
        ),
        (
            "pnl_breakdown",
            s.pnl_breakdown.as_ref().map(|c| (now - c.fetched_at_ms).max(0)),
        ),
        (
            "pnl_breakdown_yesterday",
            s.pnl_breakdown_yesterday
                .as_ref()
                .map(|c| (now - c.fetched_at_ms).max(0)),
        ),
        (
            "pnl_breakdown_window",
            s.pnl_breakdown_window
                .as_ref()
                .map(|c| (now - c.fetched_at_ms).max(0)),
        ),
        (
            "positions",
            s.positions.as_ref().map(|c| (now - c.fetched_at_ms).max(0)),
        ),
        ("ingest_probe", s.ingest_probe_age_ms),
    ];
    let any_cache_populated = cache_age_slots.iter().any(|(_, age)| age.is_some());
    if any_cache_populated {
        out.push_str(
            "# HELP agent_cache_age_seconds Age (s) of each per-cache slot since its last refresh. One series per slot in /health's cache_ages_ms.\n",
        );
        out.push_str("# TYPE agent_cache_age_seconds gauge\n");
        for (slot, age) in cache_age_slots.iter() {
            if let Some(a) = age {
                out.push_str(&format!(
                    "agent_cache_age_seconds{{slot=\"{slot}\"}} {}\n",
                    a / 1000,
                ));
            }
        }
    }

    if let Some(cache) = s.decisions.as_ref() {
        let cache_age_secs = (now - cache.fetched_at_ms).max(0) / 1000;
        out.push_str(
            "# HELP agent_decisions_today_cache_age_seconds Age of the cached decisions tally in seconds. Bounded by METRICS_DECISIONS_CACHE_TTL_S env (default 10s).\n",
        );
        out.push_str("# TYPE agent_decisions_today_cache_age_seconds gauge\n");
        out.push_str(&format!(
            "agent_decisions_today_cache_age_seconds {cache_age_secs}\n"
        ));

        out.push_str(
            "# HELP agent_decisions_today Count of polymarket_btc.decisions rows written for today's UTC bucket, by strategy/side.\n",
        );
        out.push_str("# TYPE agent_decisions_today gauge\n");
        for ((strategy, side), n) in &cache.counts {
            out.push_str(&format!(
                "agent_decisions_today{{strategy=\"{}\",side=\"{}\"}} {}\n",
                escape_label(strategy),
                escape_label(side),
                n,
            ));
        }
        let total: u32 = cache.counts.values().sum();
        out.push_str(
            "# HELP agent_decisions_today_total Total decisions written today across every (strategy, side).\n",
        );
        out.push_str("# TYPE agent_decisions_today_total gauge\n");
        out.push_str(&format!("agent_decisions_today_total {total}\n"));
    }

    if let Some(cache) = s.orders.as_ref() {
        out.push_str(
            "# HELP agent_orders_today Count of polymarket_btc.orders rows for today's UTC bucket, by strategy/side/exec/status.\n",
        );
        out.push_str("# TYPE agent_orders_today gauge\n");
        for ((strategy, side, exec, status), n) in &cache.counts {
            out.push_str(&format!(
                "agent_orders_today{{strategy=\"{}\",side=\"{}\",exec=\"{}\",status=\"{}\"}} {}\n",
                escape_label(strategy),
                escape_label(side),
                escape_label(exec),
                escape_label(status),
                n,
            ));
        }
        let total: u32 = cache.counts.values().sum();
        out.push_str(
            "# HELP agent_orders_today_total Total orders written today across every (strategy, side, exec, status).\n",
        );
        out.push_str("# TYPE agent_orders_today_total gauge\n");
        out.push_str(&format!("agent_orders_today_total {total}\n"));
    }

    if let Some(cache) = s.pnl_daily.as_ref() {
        if let Some(row) = cache.row.as_ref() {
            out.push_str(
                "# HELP agent_pnl_daily_realized_usd Realized PnL in USD for today's UTC bucket. Written by settle-pnl; read-only here.\n",
            );
            out.push_str("# TYPE agent_pnl_daily_realized_usd gauge\n");
            out.push_str(&format!(
                "agent_pnl_daily_realized_usd {}\n",
                row.realized
            ));
            out.push_str(
                "# HELP agent_pnl_daily_trades_count Number of settled trades counted into today's realized PnL.\n",
            );
            out.push_str("# TYPE agent_pnl_daily_trades_count gauge\n");
            out.push_str(&format!(
                "agent_pnl_daily_trades_count {}\n",
                row.n_trades
            ));
        }
    }

    out.push_str(
        "# HELP agent_risk_max_order_usd Current RISK_MAX_ORDER_USD limit ($) in force.\n",
    );
    out.push_str("# TYPE agent_risk_max_order_usd gauge\n");
    out.push_str(&format!(
        "agent_risk_max_order_usd {}\n",
        s.risk.max_order_usd
    ));
    out.push_str(
        "# HELP agent_risk_kill_switch_active 1 when the RISK_KILL_PATH file exists (all orders blocked); 0 otherwise.\n",
    );
    out.push_str("# TYPE agent_risk_kill_switch_active gauge\n");
    out.push_str(&format!(
        "agent_risk_kill_switch_active {}\n",
        if s.risk.kill_switch_path.exists() { 1 } else { 0 },
    ));

    if let Some(cache) = s.pnl_breakdown.as_ref() {
        if !cache.rows.is_empty() {
            out.push_str(
                "# HELP agent_pnl_breakdown_realized_usd Realized PnL in USD broken down by strategy + exec. Written by settle-pnl; read-only here.\n",
            );
            out.push_str("# TYPE agent_pnl_breakdown_realized_usd gauge\n");
            for row in &cache.rows {
                out.push_str(&format!(
                    "agent_pnl_breakdown_realized_usd{{strategy=\"{}\",exec=\"{}\"}} {}\n",
                    escape_label(&row.strategy),
                    escape_label(&row.exec),
                    row.realized_pnl,
                ));
            }
            out.push_str(
                "# HELP agent_pnl_breakdown_trades_count Number of settled trades in this strategy/exec bucket.\n",
            );
            out.push_str("# TYPE agent_pnl_breakdown_trades_count gauge\n");
            for row in &cache.rows {
                out.push_str(&format!(
                    "agent_pnl_breakdown_trades_count{{strategy=\"{}\",exec=\"{}\"}} {}\n",
                    escape_label(&row.strategy),
                    escape_label(&row.exec),
                    row.n_settled,
                ));
            }
        }
    }

    // Yesterday's pnl_breakdown — fixed once settle-pnl has done
    // its post-midnight pass. Lets Grafana panel "yesterday final
    // realized PnL by strategy" as a baseline alongside today's
    // running total. Empty when no settle ran yesterday (early in
    // a daemon's life or a missed cron).
    if let Some(cache) = s.pnl_breakdown_yesterday.as_ref() {
        if !cache.rows.is_empty() {
            out.push_str(
                "# HELP agent_pnl_breakdown_yesterday_realized_usd Yesterday's final realized PnL in USD per (strategy, exec). Useful as a baseline next to agent_pnl_breakdown_realized_usd.\n",
            );
            out.push_str("# TYPE agent_pnl_breakdown_yesterday_realized_usd gauge\n");
            for row in &cache.rows {
                out.push_str(&format!(
                    "agent_pnl_breakdown_yesterday_realized_usd{{strategy=\"{}\",exec=\"{}\"}} {}\n",
                    escape_label(&row.strategy),
                    escape_label(&row.exec),
                    row.realized_pnl,
                ));
            }
            out.push_str(
                "# HELP agent_pnl_breakdown_yesterday_trades_count Number of settled trades counted into yesterday's realized PnL bucket.\n",
            );
            out.push_str("# TYPE agent_pnl_breakdown_yesterday_trades_count gauge\n");
            for row in &cache.rows {
                out.push_str(&format!(
                    "agent_pnl_breakdown_yesterday_trades_count{{strategy=\"{}\",exec=\"{}\"}} {}\n",
                    escape_label(&row.strategy),
                    escape_label(&row.exec),
                    row.n_settled,
                ));
            }
        }
    }

    // Rolling N-day window. Same per-(strategy, exec) shape as the
    // today/yesterday gauges above, with an extra `days` label so a
    // single Grafana panel can keep all three time horizons side by
    // side. The constant is baked into the label rather than read
    // from the snapshot because all rows share the same window —
    // any operator who wants a different horizon should add a
    // sibling metric (cheaper than turning this one into a histogram).
    if let Some(cache) = s.pnl_breakdown_window.as_ref() {
        if !cache.rows.is_empty() {
            out.push_str(
                "# HELP agent_pnl_breakdown_window_realized_usd Realized PnL in USD summed over the last `days` UTC days per (strategy, exec). Anchored on today's UTC bucket.\n",
            );
            out.push_str("# TYPE agent_pnl_breakdown_window_realized_usd gauge\n");
            for row in &cache.rows {
                out.push_str(&format!(
                    "agent_pnl_breakdown_window_realized_usd{{strategy=\"{}\",exec=\"{}\",days=\"{}\"}} {}\n",
                    escape_label(&row.strategy),
                    escape_label(&row.exec),
                    PNL_BREAKDOWN_WINDOW_DAYS,
                    row.realized_pnl,
                ));
            }
            out.push_str(
                "# HELP agent_pnl_breakdown_window_trades_count Number of settled trades summed over the last `days` UTC days per (strategy, exec).\n",
            );
            out.push_str("# TYPE agent_pnl_breakdown_window_trades_count gauge\n");
            for row in &cache.rows {
                out.push_str(&format!(
                    "agent_pnl_breakdown_window_trades_count{{strategy=\"{}\",exec=\"{}\",days=\"{}\"}} {}\n",
                    escape_label(&row.strategy),
                    escape_label(&row.exec),
                    PNL_BREAKDOWN_WINDOW_DAYS,
                    row.n_settled,
                ));
            }
        }
    }

    if let Some(cache) = s.positions.as_ref() {
        if !cache.rows.is_empty() {
            out.push_str(
                "# HELP agent_open_positions_size Open position size (shares) per (market, side) from positions_v2.\n",
            );
            out.push_str("# TYPE agent_open_positions_size gauge\n");
            for p in &cache.rows {
                out.push_str(&format!(
                    "agent_open_positions_size{{market=\"{}\",side=\"{}\"}} {}\n",
                    escape_label(&p.market_slug),
                    escape_label(&p.side),
                    p.size,
                ));
            }
            out.push_str(
                "# HELP agent_open_positions_avg_price Volume-weighted average fill price per (market, side) in [0,1] Polymarket outcome units.\n",
            );
            out.push_str("# TYPE agent_open_positions_avg_price gauge\n");
            for p in &cache.rows {
                out.push_str(&format!(
                    "agent_open_positions_avg_price{{market=\"{}\",side=\"{}\"}} {}\n",
                    escape_label(&p.market_slug),
                    escape_label(&p.side),
                    p.avg_price,
                ));
            }
            out.push_str(
                "# HELP agent_open_positions_notional_usd Notional value of the open position in USD — pre-computed `size * avg_price` so single-query panels don't need label-matching.\n",
            );
            out.push_str("# TYPE agent_open_positions_notional_usd gauge\n");
            for p in &cache.rows {
                out.push_str(&format!(
                    "agent_open_positions_notional_usd{{market=\"{}\",side=\"{}\"}} {}\n",
                    escape_label(&p.market_slug),
                    escape_label(&p.side),
                    p.size * p.avg_price,
                ));
            }
            let nonzero = cache.rows.iter().filter(|p| p.size > 0.0).count();
            let total_notional: f64 = cache.rows.iter().map(|p| p.size * p.avg_price).sum();
            out.push_str(
                "# HELP agent_open_positions_count Number of non-empty open positions (size > 0).\n",
            );
            out.push_str("# TYPE agent_open_positions_count gauge\n");
            out.push_str(&format!("agent_open_positions_count {}\n", nonzero));
            out.push_str(
                "# HELP agent_open_positions_total_notional_usd Sum of notional ($) across every open position.\n",
            );
            out.push_str("# TYPE agent_open_positions_total_notional_usd gauge\n");
            out.push_str(&format!(
                "agent_open_positions_total_notional_usd {}\n",
                total_notional
            ));
        }
    }

    out
}

/// Prometheus text-exposition handler. Surfaces the same HealthState
/// `/health` does, but in line-oriented gauges keyed by the standard
/// `agent_*` namespace so an existing Prometheus scrape config can
/// pick it up without a JSON-to-metrics translator. No external
/// prometheus crate dependency — the exposition format is plain
/// text and we render it directly.
///
/// Returns `text/plain; version=0.0.4; charset=utf-8` per the
/// Prometheus convention so scrapers content-negotiate correctly.
async fn metrics_handler(State(s): State<HealthAppState>) -> impl IntoResponse {
    let now = now_ms();
    let snapshot = gather_metrics_snapshot(&s, now).await;
    let body = render_metrics(&snapshot);

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    (StatusCode::OK, headers, body)
}

/// Refresh every cache that backs `/metrics` and snapshot the
/// resulting state into a [`MetricsSnapshot`] that the pure
/// [`render_metrics`] can consume. Holds the write locks only
/// while each cache is being refreshed; clones the entries out
/// so the snapshot can be rendered after every lock is dropped.
async fn gather_metrics_snapshot(s: &HealthAppState, now: i64) -> MetricsSnapshot {
    let mut notes: Vec<String> = Vec::new();

    // Health subtask state — small struct, copy is cheap.
    let health = s.health.read().await.clone();

    let (btc_age_ms, polymarket_age_ms) = ingest_ages_cached(s, now).await;

    // Decisions cache refresh.
    let decisions: Option<DecisionsCacheEntry> = if let Some(dec_repo) = &s.decision_repo {
        let mut cache_guard = s.decisions_cache.write().await;
        let need_refresh = cache_guard
            .as_ref()
            .map(|c| now - c.fetched_at_ms > metrics_decisions_cache_ttl_ms())
            .unwrap_or(true);
        if need_refresh {
            match dec_repo.list_day(bucket_day(now)).await {
                Ok(rows) => {
                    use std::collections::BTreeMap;
                    let mut counts: BTreeMap<(String, String), u32> = BTreeMap::new();
                    for d in &rows {
                        *counts
                            .entry((d.effective_strategy().to_string(), d.side.clone()))
                            .or_insert(0) += 1;
                    }
                    *cache_guard = Some(DecisionsCacheEntry {
                        fetched_at_ms: now,
                        counts,
                    });
                }
                Err(e) => {
                    if cache_guard.is_none() {
                        notes.push(format!(
                            "# decisions_today read failed (no cached fallback): {e}"
                        ));
                    } else {
                        notes.push(format!(
                            "# decisions_today refresh failed; serving cached data: {e}"
                        ));
                    }
                }
            }
        }
        cache_guard.clone()
    } else {
        None
    };

    // Orders cache refresh.
    let orders: Option<OrdersCacheEntry> = if let (Some(order_repo), Some(decision_repo)) =
        (s.order_repo.as_ref(), s.decision_repo.as_ref())
    {
        let mut cache_guard = s.orders_cache.write().await;
        let need_refresh = cache_guard
            .as_ref()
            .map(|c| now - c.fetched_at_ms > metrics_decisions_cache_ttl_ms())
            .unwrap_or(true);
        if need_refresh {
            let bd = bucket_day(now);
            let strategy_of: std::collections::HashMap<uuid::Uuid, String> =
                match decision_repo.list_day(bd).await {
                    Ok(rows) => rows
                        .iter()
                        .map(|d| (d.decision_id, d.effective_strategy().to_string()))
                        .collect(),
                    Err(_) => std::collections::HashMap::new(),
                };
            match order_repo.list_day(bd).await {
                Ok(rows) => {
                    use std::collections::BTreeMap;
                    let mut counts: BTreeMap<(String, String, String, String), u32> =
                        BTreeMap::new();
                    for o in &rows {
                        let strategy = strategy_of
                            .get(&o.decision_id)
                            .cloned()
                            .unwrap_or_else(|| "other".to_string());
                        let exec = if o.order_id.starts_with("paper-") {
                            "paper".to_string()
                        } else {
                            "live".to_string()
                        };
                        *counts
                            .entry((strategy, o.side.clone(), exec, o.status.clone()))
                            .or_insert(0) += 1;
                    }
                    *cache_guard = Some(OrdersCacheEntry {
                        fetched_at_ms: now,
                        counts,
                    });
                }
                Err(e) => {
                    if cache_guard.is_none() {
                        notes.push(format!(
                            "# orders_today read failed (no cached fallback): {e}"
                        ));
                    } else {
                        notes.push(format!(
                            "# orders_today refresh failed; serving cached data: {e}"
                        ));
                    }
                }
            }
        }
        cache_guard.clone()
    } else {
        None
    };

    // pnl_daily cache refresh.
    let pnl_daily: Option<PnlDailyCacheEntry> = if let Some(pnl_repo) = s.pnl_repo.as_ref() {
        let mut cache_guard = s.pnl_daily_cache.write().await;
        let need_refresh = cache_guard
            .as_ref()
            .map(|c| now - c.fetched_at_ms > metrics_pnl_daily_cache_ttl_ms())
            .unwrap_or(true);
        if need_refresh {
            let bd = bucket_day(now);
            match pnl_repo.range(bd, bd).await {
                Ok(rows) => {
                    *cache_guard = Some(PnlDailyCacheEntry {
                        fetched_at_ms: now,
                        row: rows.into_iter().next(),
                    });
                }
                Err(e) => {
                    if cache_guard.is_none() {
                        notes.push(format!(
                            "# pnl_daily read failed (no cached fallback): {e}"
                        ));
                    } else {
                        notes.push(format!(
                            "# pnl_daily refresh failed; serving cached data: {e}"
                        ));
                    }
                }
            }
        }
        cache_guard.clone()
    } else {
        None
    };

    // pnl_breakdown cache refresh.
    let pnl_breakdown: Option<PnlBreakdownCacheEntry> =
        if let Some(breakdown_repo) = s.pnl_breakdown_repo.as_ref() {
            let mut cache_guard = s.pnl_breakdown_cache.write().await;
            let need_refresh = cache_guard
                .as_ref()
                .map(|c| now - c.fetched_at_ms > metrics_pnl_daily_cache_ttl_ms())
                .unwrap_or(true);
            if need_refresh {
                match breakdown_repo.list_day(bucket_day(now)).await {
                    Ok(rows) => {
                        *cache_guard = Some(PnlBreakdownCacheEntry {
                            fetched_at_ms: now,
                            rows,
                        });
                    }
                    Err(e) => {
                        if cache_guard.is_none() {
                            notes.push(format!(
                                "# pnl_breakdown read failed (no cached fallback): {e}"
                            ));
                        } else {
                            notes.push(format!(
                                "# pnl_breakdown refresh failed; serving cached data: {e}"
                            ));
                        }
                    }
                }
            }
            cache_guard.clone()
        } else {
            None
        };

    // Yesterday's pnl_breakdown — same repo, same TTL, just a
    // different bucket_day. Stored in its own cache slot so the
    // today/yesterday refreshes are independent.
    let pnl_breakdown_yesterday: Option<PnlBreakdownCacheEntry> =
        if let Some(breakdown_repo) = s.pnl_breakdown_repo.as_ref() {
            let mut cache_guard = s.pnl_breakdown_yesterday_cache.write().await;
            // Also invalidate the cache if the day rolled — without
            // this check a daemon that's been up across midnight
            // would keep returning the day-before-yesterday's data
            // until the TTL expired.
            let cached_day = cache_guard.as_ref().and_then(|c| c.rows.first()).map(|r| r.bucket_day_ms);
            let yesterday_bd = bucket_day(now) - 86_400_000;
            let day_rolled = cached_day.map(|d| d != yesterday_bd).unwrap_or(false);
            let need_refresh = day_rolled
                || cache_guard
                    .as_ref()
                    .map(|c| now - c.fetched_at_ms > metrics_pnl_daily_cache_ttl_ms())
                    .unwrap_or(true);
            if need_refresh {
                match breakdown_repo.list_day(yesterday_bd).await {
                    Ok(rows) => {
                        *cache_guard = Some(PnlBreakdownCacheEntry {
                            fetched_at_ms: now,
                            rows,
                        });
                    }
                    Err(e) => {
                        if cache_guard.is_none() {
                            notes.push(format!(
                                "# pnl_breakdown_yesterday read failed (no cached fallback): {e}"
                            ));
                        } else {
                            notes.push(format!(
                                "# pnl_breakdown_yesterday refresh failed; serving cached data: {e}"
                            ));
                        }
                    }
                }
            }
            cache_guard.clone()
        } else {
            None
        };

    // Rolling N-day window over pnl_breakdown. One refresh fans
    // out PNL_BREAKDOWN_WINDOW_DAYS list_day calls; the cache holds
    // the already-aggregated rows so the metrics renderer doesn't
    // re-do the sum on the hot path. Invalidated on day-roll so the
    // window keeps anchoring on "today" across midnight.
    let pnl_breakdown_window: Option<PnlBreakdownWindowCacheEntry> =
        if let Some(breakdown_repo) = s.pnl_breakdown_repo.as_ref() {
            let mut cache_guard = s.pnl_breakdown_window_cache.write().await;
            let today_bd = bucket_day(now);
            let cached_anchor = cache_guard.as_ref().map(|c| c.anchor_bucket_day_ms);
            let day_rolled = cached_anchor.map(|d| d != today_bd).unwrap_or(false);
            let need_refresh = day_rolled
                || cache_guard
                    .as_ref()
                    .map(|c| now - c.fetched_at_ms > metrics_pnl_daily_cache_ttl_ms())
                    .unwrap_or(true);
            if need_refresh {
                // Fan out the N day reads and aggregate by (strategy,
                // exec). One failed day doesn't poison the whole
                // window — it's recorded as a note and the surviving
                // days still feed the cache.
                let day_ms = 86_400_000_i64;
                let mut agg: std::collections::BTreeMap<(String, String), (f64, i32)> =
                    std::collections::BTreeMap::new();
                let mut any_err = false;
                for i in 0..PNL_BREAKDOWN_WINDOW_DAYS {
                    let bd = today_bd - i * day_ms;
                    match breakdown_repo.list_day(bd).await {
                        Ok(rows) => {
                            for r in rows {
                                let key = (r.strategy.clone(), r.exec.clone());
                                let entry = agg.entry(key).or_insert((0.0, 0));
                                entry.0 += r.realized_pnl;
                                entry.1 += r.n_settled;
                            }
                        }
                        Err(e) => {
                            any_err = true;
                            notes.push(format!(
                                "# pnl_breakdown_window day {bd} read failed: {e}"
                            ));
                        }
                    }
                }
                // Only update cache when *something* came back — if
                // every read failed, fall back to the previously
                // cached aggregate rather than blanking the panel.
                if !(any_err && agg.is_empty() && cache_guard.is_some()) {
                    let rows: Vec<crate::coredb::types::PnlBreakdown> = agg
                        .into_iter()
                        .map(|((strategy, exec), (pnl, n))| crate::coredb::types::PnlBreakdown {
                            bucket_day_ms: today_bd,
                            strategy,
                            exec,
                            realized_pnl: pnl,
                            n_settled: n,
                        })
                        .collect();
                    *cache_guard = Some(PnlBreakdownWindowCacheEntry {
                        fetched_at_ms: now,
                        anchor_bucket_day_ms: today_bd,
                        rows,
                    });
                }
            }
            cache_guard.clone()
        } else {
            None
        };

    // Positions cache refresh.
    let positions: Option<PositionsCacheEntry> =
        if let Some(position_repo) = s.position_repo_for_metrics.as_ref() {
            let mut cache_guard = s.positions_cache.write().await;
            let need_refresh = cache_guard
                .as_ref()
                .map(|c| now - c.fetched_at_ms > metrics_decisions_cache_ttl_ms())
                .unwrap_or(true);
            if need_refresh {
                match position_repo.list_all().await {
                    Ok(rows) => {
                        *cache_guard = Some(PositionsCacheEntry {
                            fetched_at_ms: now,
                            rows,
                        });
                    }
                    Err(e) => {
                        if cache_guard.is_none() {
                            notes.push(format!(
                                "# open_positions read failed (no cached fallback): {e}"
                            ));
                        } else {
                            notes.push(format!(
                                "# open_positions refresh failed; serving cached data: {e}"
                            ));
                        }
                    }
                }
            }
            cache_guard.clone()
        } else {
            None
        };

    // Ingest probe cache age: read fetched_at_ms separately so the
    // agent_cache_age_seconds{slot="ingest_probe"} gauge has the
    // same shape as the other slots. The probe rows themselves
    // (btc_age_ms / polymarket_age_ms) are already in scope above.
    let ingest_probe_age_ms = s
        .ingest_cache
        .read()
        .await
        .as_ref()
        .map(|c| (now - c.fetched_at_ms).max(0));

    // Restart counts — atomic snapshot. Cheap; no caching needed.
    let ingest_restarts_binance = s
        .ingest_restarts
        .binance
        .count
        .load(std::sync::atomic::Ordering::Relaxed);
    let ingest_restarts_polymarket = s
        .ingest_restarts
        .polymarket
        .count
        .load(std::sync::atomic::Ordering::Relaxed);

    MetricsSnapshot {
        now_ms: now,
        health,
        btc_age_ms,
        polymarket_age_ms,
        decisions,
        orders,
        pnl_daily,
        pnl_breakdown,
        pnl_breakdown_yesterday,
        pnl_breakdown_window,
        positions,
        ingest_probe_age_ms,
        ingest_restarts_binance,
        ingest_restarts_polymarket,
        risk: RiskLimits::default(),
        notes,
    }
}


/// Shared ingest-staleness cache lookup.
///
/// Both `/health` and `/metrics` hit `btc_repo.latest` +
/// `market_repo.list_open` to derive the ingest-age signal. Without
/// caching, two concurrent scrapes round-trip CoreDB four times for
/// data that ticks every ~500ms (Binance WS) and 30s (Polymarket
/// REST) anyway. The cache reduces that to at most one btc + one
/// markets read per `INGEST_CACHE_TTL_S` env window across both
/// endpoints (default 5s; see `ingest_cache_ttl_ms`).
///
/// Stored as a unit (no per-source stale-while-revalidate): if a
/// source goes down we want the next scrape's gauge to drop the
/// series, not paint the last-known-good age forever.
async fn ingest_ages_cached(
    s: &HealthAppState,
    now: i64,
) -> (Option<i64>, Option<i64>) {
    let mut guard = s.ingest_cache.write().await;
    let need_refresh = guard
        .as_ref()
        .map(|c| now - c.fetched_at_ms > ingest_cache_ttl_ms())
        .unwrap_or(true);

    if need_refresh {
        let btc_age_ms = match s.btc_repo.latest("BTCUSDT").await {
            Ok(Some(t)) => Some(now.saturating_sub(t.ts_ms)),
            _ => None,
        };
        let polymarket_age_ms = match s.market_repo.list_open().await {
            Ok(rows) if !rows.is_empty() => {
                let newest = rows.iter().map(|m| m.updated_at_ms).max().unwrap_or(0);
                Some(now.saturating_sub(newest))
            }
            _ => None,
        };
        *guard = Some(IngestProbeCache {
            fetched_at_ms: now,
            btc_age_ms,
            polymarket_age_ms,
        });
    }

    // SAFETY: we either just refreshed or had an existing cache; both
    // branches leave `guard` populated. `expect` over `unwrap` so a
    // future refactor that drops the cache-set on refresh fails loud.
    let cache = guard.as_ref().expect("ingest_cache populated above");
    // Adjust the returned ages by the cache age so the values stay
    // monotonic-ish across scrapes within a TTL window — without this
    // a scrape 4s after refresh would report the same age the refresh
    // captured 4s ago, which would look like a frozen clock to
    // Grafana. The cap defends against a clock skew flipping the
    // age negative.
    let age_offset = (now - cache.fetched_at_ms).max(0);
    (
        cache.btc_age_ms.map(|a| a.saturating_add(age_offset)),
        cache.polymarket_age_ms.map(|a| a.saturating_add(age_offset)),
    )
}

/// Escape a Prometheus label-value: backslash, double-quote, and
/// newline get a leading backslash per the exposition spec. Our
/// label values come from strategy / side labels which are nearly
/// always alphanumeric, but defending against future operator-
/// supplied strategy names is cheap.
fn escape_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
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

/// Supervise an ingest worker: spawn it, probe its data
/// freshness every 60s, abort + respawn if the newest row is older
/// than `stale_threshold_ms`. A threshold of 0 disables auto-
/// restart entirely (the worker still runs; we just don't watch
/// it).
///
/// `probe` reads the latest row age; `spawn` takes a per-worker
/// shutdown receiver and returns a JoinHandle for the spawned
/// task. The supervisor owns one watch::Sender per worker so it
/// can issue a graceful shutdown before resorting to abort.
///
/// On global shutdown the supervisor signals the worker via its
/// per-worker channel, awaits it briefly, and returns.
async fn supervised_ingest<P, PFut, S>(
    label: &'static str,
    stale_threshold_ms: i64,
    mut probe: P,
    mut spawn: S,
    mut shutdown: watch::Receiver<bool>,
    restart_tracker: Option<Arc<RestartTracker>>,
) where
    P: FnMut() -> PFut + Send + 'static,
    PFut: std::future::Future<Output = Option<i64>> + Send,
    S: FnMut(watch::Receiver<bool>) -> tokio::task::JoinHandle<Result<()>> + Send + 'static,
{
    // Per-worker shutdown channel. Re-created on each restart so an
    // earlier "shutdown then respawn" can't leak the prior signal.
    let (mut worker_tx, worker_rx) = watch::channel(false);
    let mut handle = spawn(worker_rx);
    let mut tick = interval(Duration::from_secs(60));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Skip the immediate first tick: workers need time to make
    // their first WS / HTTP probe before we evaluate freshness.
    tick.tick().await;

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("ingest supervisor {label}: daemon shutdown — stopping worker");
                    let _ = worker_tx.send(true);
                    let _ = handle.await;
                    return;
                }
            }
            _ = tick.tick() => {
                if stale_threshold_ms <= 0 {
                    continue;
                }
                if let Some(age_ms) = probe().await {
                    if age_ms > stale_threshold_ms {
                        warn!(
                            "ingest supervisor {label}: stale ({}ms > {}ms threshold) — restarting worker",
                            age_ms, stale_threshold_ms,
                        );
                        // Graceful shutdown first, then abort if it
                        // doesn't drain promptly. Abort is safe —
                        // both workers drop their socket / HTTP
                        // client on Drop.
                        let _ = worker_tx.send(true);
                        let abort_deadline = tokio::time::sleep(Duration::from_secs(5));
                        tokio::pin!(abort_deadline);
                        tokio::select! {
                            _ = &mut handle => {}
                            _ = &mut abort_deadline => {
                                warn!("ingest supervisor {label}: worker did not drain in 5s — aborting");
                                handle.abort();
                                let _ = (&mut handle).await;
                            }
                        }
                        // Fresh channel + fresh task. The old `tx`
                        // is dropped at end of scope; readers of
                        // the new `rx` start clean.
                        let (new_tx, new_rx) = watch::channel(false);
                        worker_tx = new_tx;
                        handle = spawn(new_rx);
                        if let Some(t) = &restart_tracker {
                            t.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            t.last_at_ms
                                .store(now_ms(), std::sync::atomic::Ordering::Relaxed);
                        }
                        info!("ingest supervisor {label}: worker respawned");
                    }
                }
            }
        }
    }
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
    #[test]
    fn escape_label_passes_alphanumeric_through() {
        assert_eq!(escape_label("baseline"), "baseline");
        assert_eq!(escape_label("deepseek-chat"), "deepseek-chat");
    }

    #[test]
    fn escape_label_escapes_special_chars() {
        assert_eq!(escape_label("a\\b"), "a\\\\b");
        assert_eq!(escape_label("a\"b"), "a\\\"b");
        assert_eq!(escape_label("a\nb"), "a\\nb");
    }

    /// Builder for the pure `compute_health_response` test cases.
    /// Defaults to a "fresh + happy" state — every test tweaks the
    /// fields relevant to the branch it's exercising.
    fn happy_inputs() -> super::HealthInputs {
        super::HealthInputs {
            now_ms: 1_700_000_010_000,
            health: super::HealthState {
                started_at_ms: 1_700_000_000_000,
                backtest: super::SubtaskHealth::default(),
                compare: super::SubtaskHealth::default(),
                settle: super::SubtaskHealth::default(),
                user_channel_present: false,
            },
            btc_age_ms: Some(500),
            polymarket_age_ms: Some(12_000),
            cache_ages: super::HealthCacheAges::default(),
            ingest_restarts: super::IngestRestartsWire::default(),
            last_restart_at_ms: None,
            stale_cache_health_ms: super::STALE_CACHE_HEALTH_MS_DEFAULT,
            recent_restart_ms: super::RECENT_RESTART_MS_DEFAULT,
        }
    }

    #[test]
    fn health_ok_when_fresh_ingest_and_no_subtask_errors() {
        let r = super::compute_health_response(&happy_inputs());
        assert_eq!(r.status, "ok");
        assert_eq!(r.uptime_secs, 10);
    }

    #[test]
    fn health_degraded_when_ingest_missing() {
        let mut inp = happy_inputs();
        inp.btc_age_ms = None;
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "degraded");
    }

    #[test]
    fn health_degraded_when_ingest_stale() {
        let mut inp = happy_inputs();
        // Past INGEST_STALE_MS (5 min = 300_000 ms).
        inp.polymarket_age_ms = Some(600_000);
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "degraded");
    }

    #[test]
    fn health_degraded_when_subtask_over_error_threshold() {
        let mut inp = happy_inputs();
        // UNHEALTHY_AFTER_ERRORS = 3; >= 3 is degraded.
        inp.health.backtest.consecutive_errors = 5;
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "degraded");
    }

    #[test]
    fn health_ok_at_subtask_error_threshold_minus_one() {
        let mut inp = happy_inputs();
        inp.health.backtest.consecutive_errors = super::UNHEALTHY_AFTER_ERRORS - 1;
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    /// `cache_ages_ms` round-trips from inputs → response → JSON. The
    /// dashboard reads the JSON shape directly, so pin both that the
    /// field is present (non-null when input has it set) and that
    /// the missing-cache case serializes as `null` so the chip
    /// renderer can tell "cache never populated" from "cache
    /// freshly refreshed at 0ms".
    /// A cache slot whose age exceeds [`STALE_CACHE_HEALTH_MS`]
    /// (10 min) flips status from "ok" to "degraded" — that
    /// surfaces to reverse-proxy probes grep'ing the status field
    /// without them having to inspect `cache_ages_ms` directly.
    /// Mirrors the dashboard's stalest_cache_hint threshold so the
    /// chip + status field stay in sync.
    #[test]
    fn health_degraded_when_any_cache_stale_past_threshold() {
        let mut inp = happy_inputs();
        // 11 min — just past the 10-min threshold.
        inp.cache_ages.pnl_breakdown = Some(11 * 60 * 1000);
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "degraded");
    }

    #[test]
    fn health_ok_when_all_caches_under_threshold() {
        let mut inp = happy_inputs();
        // 9 min — under the 10-min threshold.
        inp.cache_ages.pnl_breakdown = Some(9 * 60 * 1000);
        inp.cache_ages.orders = Some(30_000);
        inp.cache_ages.decisions = Some(0);
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    #[test]
    fn health_ok_ignores_unpopulated_cache_slots() {
        // All-None ages = "never refreshed yet" = fresh daemon,
        // not stale. Status must stay "ok".
        let mut inp = happy_inputs();
        inp.cache_ages = super::HealthCacheAges::default();
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    /// A restart within the last [`RECENT_RESTART_MS`] window
    /// keeps the daemon status at "degraded" even if every other
    /// signal (ingest, periodic, caches) is green. Reverse-proxy
    /// probes grep'ing the status field see the incident without
    /// having to diff the counters themselves.
    #[test]
    fn health_degraded_when_restart_within_recent_window() {
        let mut inp = happy_inputs();
        // 2 min ago — inside the 5-min window.
        inp.last_restart_at_ms = Some(inp.now_ms - 2 * 60 * 1000);
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "degraded");
    }

    #[test]
    fn health_ok_when_restart_older_than_recent_window() {
        let mut inp = happy_inputs();
        // 10 min ago — past the 5-min window.
        inp.last_restart_at_ms = Some(inp.now_ms - 10 * 60 * 1000);
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    #[test]
    fn health_ok_when_no_restart_has_ever_happened() {
        let mut inp = happy_inputs();
        inp.last_restart_at_ms = None;
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    #[test]
    fn health_ok_with_far_future_restart_timestamp() {
        // Clock skew safety: a `last_restart_at_ms` from "the
        // future" (e.g. clock went backwards on a previous boot)
        // would naively produce a negative age. We treat that as
        // ok — reading "from the future" makes less operational
        // sense than reading "old enough to be safe".
        let mut inp = happy_inputs();
        inp.last_restart_at_ms = Some(inp.now_ms + 10 * 60 * 1000);
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    #[test]
    fn health_response_carries_cache_ages_per_slot() {
        let mut inp = happy_inputs();
        inp.cache_ages = super::HealthCacheAges {
            decisions: Some(1_234),
            orders: Some(2_345),
            pnl_daily: None,
            pnl_breakdown: Some(34_000),
            pnl_breakdown_yesterday: None,
            pnl_breakdown_window: Some(60_000),
            positions: Some(800),
            ingest_probe: Some(100),
        };
        let r = super::compute_health_response(&inp);
        assert_eq!(r.cache_ages_ms.decisions, Some(1_234));
        assert_eq!(r.cache_ages_ms.orders, Some(2_345));
        assert_eq!(r.cache_ages_ms.pnl_daily, None);
        assert_eq!(r.cache_ages_ms.pnl_breakdown, Some(34_000));
        assert_eq!(r.cache_ages_ms.pnl_breakdown_yesterday, None);
        assert_eq!(r.cache_ages_ms.pnl_breakdown_window, Some(60_000));
        assert_eq!(r.cache_ages_ms.positions, Some(800));
        assert_eq!(r.cache_ages_ms.ingest_probe, Some(100));

        // JSON shape pin: dashboard depends on the exact field
        // names + None → null mapping. Build the JSON via the
        // existing Serialize impl rather than asserting it
        // text-equally because field order may vary.
        let json = serde_json::to_value(&r).unwrap();
        let ages = &json["cache_ages_ms"];
        assert_eq!(ages["decisions"], 1234);
        assert_eq!(ages["pnl_daily"], serde_json::Value::Null);
        assert_eq!(ages["pnl_breakdown_window"], 60_000);
        assert_eq!(ages["ingest_probe"], 100);
    }

    /// Status does NOT factor in cache_ages that are within the
    /// freshness threshold. A populated-but-fresh cache must keep
    /// the daemon "ok" — the response still surfaces the ages, but
    /// they don't down-grade status until one crosses
    /// STALE_CACHE_HEALTH_MS. See
    /// `health_degraded_when_any_cache_stale_past_threshold` for
    /// the inverse direction.
    #[test]
    fn health_ok_with_fresh_cache_ages_populated() {
        let mut inp = happy_inputs();
        inp.cache_ages.decisions = Some(1_234);
        inp.cache_ages.orders = Some(5_678);
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
        assert_eq!(r.cache_ages_ms.decisions, Some(1_234));
        assert_eq!(r.cache_ages_ms.orders, Some(5_678));
    }

    /// Drive `render_metrics` with a fully-populated synthetic
    /// MetricsSnapshot — exercises every conditional branch
    /// (every cache populated, every subtask field set) so the
    /// output contains every metric family the daemon can emit.
    /// Stronger than the source-grep companion: catches "branch
    /// silently stopped emitting" at runtime, not just textual
    /// rename.
    #[test]
    fn render_metrics_emits_every_family_with_populated_snapshot() {
        use crate::coredb::types::{PnlBreakdown, PnlDaily, Position};
        use std::collections::BTreeMap;
        use std::path::PathBuf;

        let mut decisions_counts = BTreeMap::new();
        decisions_counts.insert(("baseline".to_string(), "YES".to_string()), 3);
        let mut orders_counts = BTreeMap::new();
        orders_counts.insert(
            ("baseline".into(), "YES".into(), "paper".into(), "filled".into()),
            2,
        );

        let snap = super::MetricsSnapshot {
            now_ms: 1_700_000_010_000,
            health: super::HealthState {
                started_at_ms: 1_700_000_000_000,
                backtest: super::SubtaskHealth {
                    last_tick_ms: Some(1_700_000_005_000),
                    last_success_ms: Some(1_700_000_005_000),
                    last_error_ms: None,
                    last_error: None,
                    consecutive_errors: 0,
                },
                compare: super::SubtaskHealth::default(),
                settle: super::SubtaskHealth::default(),
                user_channel_present: true,
            },
            btc_age_ms: Some(500),
            polymarket_age_ms: Some(12_000),
            decisions: Some(super::DecisionsCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                counts: decisions_counts,
            }),
            orders: Some(super::OrdersCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                counts: orders_counts,
            }),
            pnl_daily: Some(super::PnlDailyCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                row: Some(PnlDaily {
                    day_ms: 1_700_000_000_000,
                    realized: 12.34,
                    unrealized: 0.0,
                    n_trades: 5,
                    llm_cost_usd: 0.0,
                }),
            }),
            pnl_breakdown: Some(super::PnlBreakdownCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                rows: vec![PnlBreakdown {
                    bucket_day_ms: 1_700_000_000_000,
                    strategy: "baseline".into(),
                    exec: "paper".into(),
                    realized_pnl: 12.34,
                    n_settled: 5,
                }],
            }),
            pnl_breakdown_yesterday: Some(super::PnlBreakdownCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                rows: vec![PnlBreakdown {
                    bucket_day_ms: 1_699_913_600_000,
                    strategy: "baseline".into(),
                    exec: "paper".into(),
                    realized_pnl: 100.0,
                    n_settled: 20,
                }],
            }),
            pnl_breakdown_window: Some(super::PnlBreakdownWindowCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                anchor_bucket_day_ms: 1_700_000_000_000,
                rows: vec![PnlBreakdown {
                    bucket_day_ms: 1_700_000_000_000,
                    strategy: "baseline".into(),
                    exec: "paper".into(),
                    realized_pnl: 250.75,
                    n_settled: 42,
                }],
            }),
            positions: Some(super::PositionsCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                rows: vec![Position {
                    market_slug: "m".into(),
                    side: "YES".into(),
                    size: 10.0,
                    avg_price: 0.5,
                    updated_at_ms: 1_700_000_005_000,
                }],
            }),
            ingest_probe_age_ms: Some(1_500),
            ingest_restarts_binance: 3,
            ingest_restarts_polymarket: 1,
            risk: super::RiskLimits {
                max_order_usd: 25.0,
                kill_switch_path: PathBuf::from("/tmp/__definitely_nonexistent__"),
            },
            notes: vec!["# heads up, this is a test note".to_string()],
        };

        let out = super::render_metrics(&snap);

        // Every metric family from EXPECTED_FAMILIES must appear as
        // an actual `agent_<name>` sample line, not just a TYPE
        // header. Stronger guarantee than the grep test below.
        const EXPECTED_FAMILIES: &[&str] = &[
            "agent_cache_age_seconds",
            "agent_decisions_today",
            "agent_decisions_today_cache_age_seconds",
            "agent_decisions_today_total",
            "agent_ingest_age_seconds",
            "agent_ingest_restarts_total",
            "agent_open_positions_avg_price",
            "agent_open_positions_count",
            "agent_open_positions_notional_usd",
            "agent_open_positions_size",
            "agent_open_positions_total_notional_usd",
            "agent_orders_today",
            "agent_orders_today_total",
            "agent_pnl_breakdown_realized_usd",
            "agent_pnl_breakdown_trades_count",
            "agent_pnl_breakdown_window_realized_usd",
            "agent_pnl_breakdown_window_trades_count",
            "agent_pnl_breakdown_yesterday_realized_usd",
            "agent_pnl_breakdown_yesterday_trades_count",
            "agent_pnl_daily_realized_usd",
            "agent_pnl_daily_trades_count",
            "agent_risk_kill_switch_active",
            "agent_risk_max_order_usd",
            "agent_subtask_consecutive_errors",
            "agent_subtask_last_success_age_seconds",
            "agent_subtask_last_tick_age_seconds",
            "agent_uptime_seconds",
            "agent_user_channel_present",
        ];
        for family in EXPECTED_FAMILIES {
            // Either the bare name (for scalars) or the `{` suffix
            // (for labelled gauges) — both forms count as "this
            // family produced a sample". Anchor with `\n` so we
            // don't match prefix overlap (`foo_total` vs `foo`).
            let bare = format!("\n{family} ");
            let labelled = format!("\n{family}{{");
            assert!(
                out.contains(&bare) || out.contains(&labelled),
                "render_metrics did not emit a sample line for `{family}`",
            );
        }

        // Notes prepended at the top.
        assert!(
            out.starts_with("# heads up, this is a test note"),
            "render_metrics dropped the notes preamble",
        );
    }

    /// Pin the exact label shape of the rolling-window pnl_breakdown
    /// metric — `days="7"` is part of the Grafana dashboard query
    /// (`sum by (strategy)(agent_pnl_breakdown_window_realized_usd{days="7"})`),
    /// so a refactor that drops or renames that label silently
    /// breaks downstream panels. Catches the regression at
    /// `cargo test` time.
    #[test]
    fn render_metrics_pnl_breakdown_window_includes_days_label() {
        use crate::coredb::types::PnlBreakdown;
        use std::path::PathBuf;

        let snap = super::MetricsSnapshot {
            now_ms: 1_700_000_010_000,
            health: super::HealthState {
                started_at_ms: 1_700_000_000_000,
                backtest: super::SubtaskHealth::default(),
                compare: super::SubtaskHealth::default(),
                settle: super::SubtaskHealth::default(),
                user_channel_present: false,
            },
            btc_age_ms: None,
            polymarket_age_ms: None,
            decisions: None,
            orders: None,
            pnl_daily: None,
            pnl_breakdown: None,
            pnl_breakdown_yesterday: None,
            pnl_breakdown_window: Some(super::PnlBreakdownWindowCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                anchor_bucket_day_ms: 1_700_000_000_000,
                rows: vec![PnlBreakdown {
                    bucket_day_ms: 1_700_000_000_000,
                    strategy: "deepseek".into(),
                    exec: "live".into(),
                    realized_pnl: -3.5,
                    n_settled: 7,
                }],
            }),
            positions: None,
            ingest_probe_age_ms: None,
            ingest_restarts_binance: 0,
            ingest_restarts_polymarket: 0,
            risk: super::RiskLimits {
                max_order_usd: 50.0,
                kill_switch_path: PathBuf::from("/tmp/__definitely_nonexistent__"),
            },
            notes: Vec::new(),
        };

        let out = super::render_metrics(&snap);
        let expected_realized = format!(
            "agent_pnl_breakdown_window_realized_usd{{strategy=\"deepseek\",exec=\"live\",days=\"{}\"}} -3.5",
            super::PNL_BREAKDOWN_WINDOW_DAYS,
        );
        let expected_count = format!(
            "agent_pnl_breakdown_window_trades_count{{strategy=\"deepseek\",exec=\"live\",days=\"{}\"}} 7",
            super::PNL_BREAKDOWN_WINDOW_DAYS,
        );
        assert!(
            out.contains(&expected_realized),
            "window realized line missing or mis-labelled. Got:\n{out}",
        );
        assert!(
            out.contains(&expected_count),
            "window count line missing or mis-labelled. Got:\n{out}",
        );
    }

    /// `agent_cache_age_seconds{slot="..."}` emits one series per
    /// populated cache. Pin the exact label name + units (the
    /// metric is in *seconds*, not ms — fetched_at_ms is divided
    /// by 1000 on the way out). Tested against a partially-
    /// populated snapshot: decisions + ingest_probe present, the
    /// rest missing.
    #[test]
    fn render_metrics_cache_age_seconds_emits_per_slot() {
        use crate::coredb::types::PnlDaily;
        use std::collections::BTreeMap;
        use std::path::PathBuf;

        let mut decisions_counts = BTreeMap::new();
        decisions_counts.insert(("baseline".to_string(), "YES".to_string()), 1);
        let snap = super::MetricsSnapshot {
            now_ms: 1_700_000_010_000,
            health: super::HealthState {
                started_at_ms: 1_700_000_000_000,
                backtest: super::SubtaskHealth::default(),
                compare: super::SubtaskHealth::default(),
                settle: super::SubtaskHealth::default(),
                user_channel_present: false,
            },
            btc_age_ms: None,
            polymarket_age_ms: None,
            decisions: Some(super::DecisionsCacheEntry {
                // 7s old: 1_700_000_010_000 - 1_700_000_003_000.
                fetched_at_ms: 1_700_000_003_000,
                counts: decisions_counts,
            }),
            orders: None,
            pnl_daily: Some(super::PnlDailyCacheEntry {
                // 17s old.
                fetched_at_ms: 1_699_999_993_000,
                row: Some(PnlDaily {
                    day_ms: 1_700_000_000_000,
                    realized: 0.0,
                    unrealized: 0.0,
                    n_trades: 0,
                    llm_cost_usd: 0.0,
                }),
            }),
            pnl_breakdown: None,
            pnl_breakdown_yesterday: None,
            pnl_breakdown_window: None,
            positions: None,
            ingest_probe_age_ms: Some(2_500),
            ingest_restarts_binance: 0,
            ingest_restarts_polymarket: 0,
            risk: super::RiskLimits {
                max_order_usd: 50.0,
                kill_switch_path: PathBuf::from("/tmp/__definitely_nonexistent__"),
            },
            notes: Vec::new(),
        };

        let out = super::render_metrics(&snap);
        // Populated slots: exact label + value (ms → s).
        assert!(
            out.contains("agent_cache_age_seconds{slot=\"decisions\"} 7"),
            "decisions slot missing or wrong age. Got:\n{out}"
        );
        assert!(
            out.contains("agent_cache_age_seconds{slot=\"pnl_daily\"} 17"),
            "pnl_daily slot missing or wrong age. Got:\n{out}"
        );
        assert!(
            out.contains("agent_cache_age_seconds{slot=\"ingest_probe\"} 2"),
            "ingest_probe slot missing or wrong age (2500ms→2s). Got:\n{out}"
        );
        // Missing slots: no series emitted ("missing = no data yet"
        // contract). Specifically check orders since the rest of
        // the suite already pins this for the other slots.
        assert!(
            !out.contains("agent_cache_age_seconds{slot=\"orders\"}"),
            "orders slot should not emit when cache is None"
        );
        assert!(
            !out.contains("agent_cache_age_seconds{slot=\"positions\"}"),
            "positions slot should not emit when cache is None"
        );
    }

    /// `agent_ingest_restarts_total` is a *counter*, not a gauge,
    /// with one series per ingest source. Pin the exact label
    /// shape + TYPE line so a Grafana panel using `rate()` keeps
    /// working when the rust-agent binary churns.
    #[test]
    fn render_metrics_ingest_restarts_emits_counter_per_source() {
        use std::path::PathBuf;
        let snap = super::MetricsSnapshot {
            now_ms: 1_700_000_010_000,
            health: super::HealthState {
                started_at_ms: 1_700_000_000_000,
                backtest: super::SubtaskHealth::default(),
                compare: super::SubtaskHealth::default(),
                settle: super::SubtaskHealth::default(),
                user_channel_present: false,
            },
            btc_age_ms: None,
            polymarket_age_ms: None,
            decisions: None,
            orders: None,
            pnl_daily: None,
            pnl_breakdown: None,
            pnl_breakdown_yesterday: None,
            pnl_breakdown_window: None,
            positions: None,
            ingest_probe_age_ms: None,
            ingest_restarts_binance: 7,
            ingest_restarts_polymarket: 2,
            risk: super::RiskLimits {
                max_order_usd: 50.0,
                kill_switch_path: PathBuf::from("/tmp/__definitely_nonexistent__"),
            },
            notes: Vec::new(),
        };
        let out = super::render_metrics(&snap);
        assert!(
            out.contains("# TYPE agent_ingest_restarts_total counter\n"),
            "expected counter TYPE line. Got:\n{out}",
        );
        assert!(
            out.contains("agent_ingest_restarts_total{source=\"binance\"} 7\n"),
            "expected binance series with count=7. Got:\n{out}",
        );
        assert!(
            out.contains("agent_ingest_restarts_total{source=\"polymarket\"} 2\n"),
            "expected polymarket series with count=2. Got:\n{out}",
        );
    }

    /// All-None caches → no agent_cache_age_seconds series at all.
    /// Pin the TYPE line too — if the renderer ever changes to
    /// emit an empty family header, downstream tooling that grep's
    /// for the TYPE line would silently start treating the
    /// non-existent series as zero.
    #[test]
    fn render_metrics_cache_age_seconds_empty_emits_no_family() {
        use std::path::PathBuf;
        let snap = super::MetricsSnapshot {
            now_ms: 1_700_000_010_000,
            health: super::HealthState {
                started_at_ms: 1_700_000_000_000,
                backtest: super::SubtaskHealth::default(),
                compare: super::SubtaskHealth::default(),
                settle: super::SubtaskHealth::default(),
                user_channel_present: false,
            },
            btc_age_ms: None,
            polymarket_age_ms: None,
            decisions: None,
            orders: None,
            pnl_daily: None,
            pnl_breakdown: None,
            pnl_breakdown_yesterday: None,
            pnl_breakdown_window: None,
            positions: None,
            ingest_probe_age_ms: None,
            ingest_restarts_binance: 0,
            ingest_restarts_polymarket: 0,
            risk: super::RiskLimits {
                max_order_usd: 50.0,
                kill_switch_path: PathBuf::from("/tmp/__definitely_nonexistent__"),
            },
            notes: Vec::new(),
        };
        let out = super::render_metrics(&snap);
        assert!(
            !out.contains("agent_cache_age_seconds"),
            "no cache_age family should emit when every slot is None"
        );
    }

    /// Empty rowset → no series. Same contract as the today /
    /// yesterday gauges: "missing = no data yet", not a zero-row
    /// emission that Grafana would chart as a constant 0.
    #[test]
    fn render_metrics_pnl_breakdown_window_empty_emits_no_series() {
        use std::path::PathBuf;

        let snap = super::MetricsSnapshot {
            now_ms: 1_700_000_010_000,
            health: super::HealthState {
                started_at_ms: 1_700_000_000_000,
                backtest: super::SubtaskHealth::default(),
                compare: super::SubtaskHealth::default(),
                settle: super::SubtaskHealth::default(),
                user_channel_present: false,
            },
            btc_age_ms: None,
            polymarket_age_ms: None,
            decisions: None,
            orders: None,
            pnl_daily: None,
            pnl_breakdown: None,
            pnl_breakdown_yesterday: None,
            pnl_breakdown_window: Some(super::PnlBreakdownWindowCacheEntry {
                fetched_at_ms: 1_700_000_009_000,
                anchor_bucket_day_ms: 1_700_000_000_000,
                rows: Vec::new(),
            }),
            positions: None,
            ingest_probe_age_ms: None,
            ingest_restarts_binance: 0,
            ingest_restarts_polymarket: 0,
            risk: super::RiskLimits {
                max_order_usd: 50.0,
                kill_switch_path: PathBuf::from("/tmp/__definitely_nonexistent__"),
            },
            notes: Vec::new(),
        };

        let out = super::render_metrics(&snap);
        assert!(
            !out.contains("agent_pnl_breakdown_window_realized_usd"),
            "empty window cache should not emit realized series"
        );
        assert!(
            !out.contains("agent_pnl_breakdown_window_trades_count"),
            "empty window cache should not emit count series"
        );
    }

    /// Pin the set of metric family names the daemon's /metrics
    /// endpoint emits. Catches accidental rename / removal at
    /// `cargo test` time instead of waiting for a Grafana panel
    /// to silently break.
    ///
    /// This is a textual grep over `daemon.rs` rather than a real
    /// handler render — the handler depends on live CoreDB repos
    /// and building scylla sessions for a unit test is heavier
    /// than the safety this test buys. Adding a new metric:
    /// extend `EXPECTED_FAMILIES` below.
    #[test]
    fn metrics_handler_emits_expected_families() {
        // Every metric family the handler `out.push_str`s a
        // `# TYPE <name> <type>` line for. Sorted for diff
        // readability when adding new entries.
        const EXPECTED_FAMILIES: &[&str] = &[
            "agent_cache_age_seconds",
            "agent_decisions_today",
            "agent_decisions_today_cache_age_seconds",
            "agent_decisions_today_total",
            "agent_ingest_age_seconds",
            "agent_ingest_restarts_total",
            "agent_open_positions_avg_price",
            "agent_open_positions_count",
            "agent_open_positions_notional_usd",
            "agent_open_positions_size",
            "agent_open_positions_total_notional_usd",
            "agent_orders_today",
            "agent_orders_today_total",
            "agent_pnl_breakdown_realized_usd",
            "agent_pnl_breakdown_trades_count",
            "agent_pnl_breakdown_window_realized_usd",
            "agent_pnl_breakdown_window_trades_count",
            "agent_pnl_breakdown_yesterday_realized_usd",
            "agent_pnl_breakdown_yesterday_trades_count",
            "agent_pnl_daily_realized_usd",
            "agent_pnl_daily_trades_count",
            "agent_risk_kill_switch_active",
            "agent_risk_max_order_usd",
            "agent_subtask_consecutive_errors",
            "agent_subtask_last_success_age_seconds",
            "agent_subtask_last_tick_age_seconds",
            "agent_uptime_seconds",
            "agent_user_channel_present",
        ];

        // Read the source of this same file. include_str! pins the
        // path at compile time, so a refactor that splits daemon.rs
        // (e.g. into a daemon/ module dir) would need to update
        // the path here too — that's the *only* time this test
        // should fail without an intentional metric change.
        let src = include_str!("daemon.rs");
        for family in EXPECTED_FAMILIES {
            // Each family declares either a `gauge` or a `counter`
            // TYPE — accept either so adding a counter doesn't
            // force renaming the test logic. The narrower form of
            // this assertion would silently drop counters.
            let is_gauge = src.contains(&format!("# TYPE {family} gauge"));
            let is_counter = src.contains(&format!("# TYPE {family} counter"));
            assert!(
                is_gauge || is_counter,
                "metric family `{family}` no longer emits a `# TYPE` line — \
                 either it was renamed (update EXPECTED_FAMILIES) or removed \
                 (audit downstream Grafana panels first)",
            );
        }

        // Inverse check: every `# TYPE agent_…` line in the source
        // is in the expected list. Catches "new metric added but
        // test not updated" — preserves the snapshot's
        // completeness over time.
        let mut emitted: Vec<&str> = src
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                let prefix = "out.push_str(\"# TYPE ";
                let idx = trimmed.find(prefix)?;
                let rest = &trimmed[idx + prefix.len()..];
                rest.split(' ').next()
            })
            .filter(|name| name.starts_with("agent_"))
            .collect();
            emitted.sort();
            emitted.dedup();
        let expected: std::collections::HashSet<&str> =
            EXPECTED_FAMILIES.iter().copied().collect();
        for name in &emitted {
            assert!(
                expected.contains(name),
                "new metric family `{name}` emitted but missing from \
                 EXPECTED_FAMILIES — please add it (and update CLAUDE.md)",
            );
        }
    }

    /// Env-parsing for the per-cache metrics TTLs. Default,
    /// explicit override, and malformed value all round-trip
    /// through the same accessors gather_metrics_snapshot uses.
    #[test]
    fn metrics_decisions_cache_ttl_ms_env_parsing() {
        std::env::remove_var("METRICS_DECISIONS_CACHE_TTL_S");
        assert_eq!(
            super::metrics_decisions_cache_ttl_ms(),
            super::METRICS_DECISIONS_CACHE_TTL_MS_DEFAULT,
        );
        std::env::set_var("METRICS_DECISIONS_CACHE_TTL_S", "30");
        assert_eq!(super::metrics_decisions_cache_ttl_ms(), 30_000);
        std::env::set_var("METRICS_DECISIONS_CACHE_TTL_S", "bogus");
        assert_eq!(
            super::metrics_decisions_cache_ttl_ms(),
            super::METRICS_DECISIONS_CACHE_TTL_MS_DEFAULT,
        );
        std::env::remove_var("METRICS_DECISIONS_CACHE_TTL_S");
    }

    #[test]
    fn metrics_pnl_daily_cache_ttl_ms_env_parsing() {
        std::env::remove_var("METRICS_PNL_DAILY_CACHE_TTL_S");
        assert_eq!(
            super::metrics_pnl_daily_cache_ttl_ms(),
            super::METRICS_PNL_DAILY_CACHE_TTL_MS_DEFAULT,
        );
        std::env::set_var("METRICS_PNL_DAILY_CACHE_TTL_S", "180");
        assert_eq!(super::metrics_pnl_daily_cache_ttl_ms(), 180_000);
        std::env::set_var("METRICS_PNL_DAILY_CACHE_TTL_S", "bogus");
        assert_eq!(
            super::metrics_pnl_daily_cache_ttl_ms(),
            super::METRICS_PNL_DAILY_CACHE_TTL_MS_DEFAULT,
        );
        std::env::remove_var("METRICS_PNL_DAILY_CACHE_TTL_S");
    }

    #[test]
    fn ingest_cache_ttl_ms_env_parsing() {
        std::env::remove_var("INGEST_CACHE_TTL_S");
        assert_eq!(
            super::ingest_cache_ttl_ms(),
            super::INGEST_CACHE_TTL_MS_DEFAULT,
        );
        std::env::set_var("INGEST_CACHE_TTL_S", "2");
        assert_eq!(super::ingest_cache_ttl_ms(), 2_000);
        std::env::set_var("INGEST_CACHE_TTL_S", "bogus");
        assert_eq!(
            super::ingest_cache_ttl_ms(),
            super::INGEST_CACHE_TTL_MS_DEFAULT,
        );
        std::env::remove_var("INGEST_CACHE_TTL_S");
    }

    /// Env-parsing for the cache-staleness threshold. Default,
    /// explicit override, "0 = disabled", and malformed value all
    /// round-trip through the same accessor compute_health_response
    /// uses indirectly via gather_health_inputs.
    #[test]
    fn stale_cache_health_ms_env_parsing() {
        std::env::remove_var("STALE_CACHE_HEALTH_S");
        assert_eq!(
            super::stale_cache_health_ms(),
            super::STALE_CACHE_HEALTH_MS_DEFAULT,
        );
        std::env::set_var("STALE_CACHE_HEALTH_S", "60");
        assert_eq!(super::stale_cache_health_ms(), 60_000);
        std::env::set_var("STALE_CACHE_HEALTH_S", "0");
        assert_eq!(super::stale_cache_health_ms(), 0);
        std::env::set_var("STALE_CACHE_HEALTH_S", "bogus");
        assert_eq!(
            super::stale_cache_health_ms(),
            super::STALE_CACHE_HEALTH_MS_DEFAULT,
        );
        std::env::remove_var("STALE_CACHE_HEALTH_S");
    }

    /// Same parsing contract for the recent-restart window.
    #[test]
    fn recent_restart_ms_env_parsing() {
        std::env::remove_var("RECENT_RESTART_S");
        assert_eq!(
            super::recent_restart_ms(),
            super::RECENT_RESTART_MS_DEFAULT,
        );
        std::env::set_var("RECENT_RESTART_S", "30");
        assert_eq!(super::recent_restart_ms(), 30_000);
        std::env::set_var("RECENT_RESTART_S", "0");
        assert_eq!(super::recent_restart_ms(), 0);
        std::env::set_var("RECENT_RESTART_S", "bogus");
        assert_eq!(
            super::recent_restart_ms(),
            super::RECENT_RESTART_MS_DEFAULT,
        );
        std::env::remove_var("RECENT_RESTART_S");
    }

    /// `stale_cache_health_ms = 0` opts out: every cache stays
    /// "fresh enough" for status purposes regardless of age. The
    /// cache_ages_ms response field still reports the underlying
    /// ages so operators can see them.
    #[test]
    fn health_ok_when_cache_threshold_disabled() {
        let mut inp = happy_inputs();
        inp.stale_cache_health_ms = 0;
        inp.cache_ages.pnl_breakdown = Some(60 * 60 * 1000); // 1h
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    /// `recent_restart_ms = 0` opts out: a fresh restart doesn't
    /// downgrade status. Counters still increment via the metrics
    /// pipeline; this just suppresses the auto-downgrade.
    #[test]
    fn health_ok_when_recent_restart_window_disabled() {
        let mut inp = happy_inputs();
        inp.recent_restart_ms = 0;
        inp.last_restart_at_ms = Some(inp.now_ms - 1_000); // 1s ago
        let r = super::compute_health_response(&inp);
        assert_eq!(r.status, "ok");
    }

    /// Custom thresholds work end-to-end: a 60s window with a 90s-
    /// old restart is "ok"; the same restart against a 120s window
    /// would have flipped to "degraded".
    #[test]
    fn health_respects_custom_recent_restart_window() {
        let mut inp = happy_inputs();
        inp.recent_restart_ms = 60_000;
        inp.last_restart_at_ms = Some(inp.now_ms - 90_000); // 90s ago
        assert_eq!(super::compute_health_response(&inp).status, "ok");

        inp.recent_restart_ms = 120_000;
        assert_eq!(super::compute_health_response(&inp).status, "degraded");
    }

    /// Env-parsing for the ingest watchdog threshold. Default,
    /// explicit override, "0 = disabled", and malformed value all
    /// round-trip through the same accessor the daemon uses.
    #[test]
    fn ingest_stale_restart_ms_env_parsing() {
        // Hermetic: clear before each branch.
        std::env::remove_var("INGEST_STALE_RESTART_S");
        assert_eq!(
            super::ingest_stale_restart_ms(),
            super::DEFAULT_INGEST_STALE_RESTART_MS,
        );

        std::env::set_var("INGEST_STALE_RESTART_S", "120");
        assert_eq!(super::ingest_stale_restart_ms(), 120_000);

        // 0 = disabled; the supervisor checks `<= 0` and skips.
        std::env::set_var("INGEST_STALE_RESTART_S", "0");
        assert_eq!(super::ingest_stale_restart_ms(), 0);

        // Malformed string → fall back to default.
        std::env::set_var("INGEST_STALE_RESTART_S", "not-a-number");
        assert_eq!(
            super::ingest_stale_restart_ms(),
            super::DEFAULT_INGEST_STALE_RESTART_MS,
        );

        std::env::remove_var("INGEST_STALE_RESTART_S");
    }

    /// Supervisor respawns the worker when the probe reports
    /// staleness above threshold. The probe returns 1_000_000ms
    /// (way over the 5-min default); the spawn closure counts
    /// calls. After two probe ticks (≈ first 60s + restart) the
    /// counter must be ≥ 2.
    ///
    /// Uses a 50ms tick window via overriding TOKIO_TEST sleeps —
    /// actually no, the supervisor's interval is hardcoded to
    /// 60s. To make this fast we use `tokio::time::pause()` to
    /// mock the clock and advance manually.
    #[tokio::test(start_paused = true)]
    async fn supervised_ingest_respawns_when_probe_reports_stale() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let spawn_calls = Arc::new(AtomicUsize::new(0));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let spawn_calls_for_spawn = Arc::clone(&spawn_calls);
        let spawn_fn = move |_rx: watch::Receiver<bool>| {
            spawn_calls_for_spawn.fetch_add(1, Ordering::SeqCst);
            // Worker that exits immediately — supervisor will
            // notice the handle drained but doesn't react to that
            // (only to staleness). The next probe tick fires the
            // restart.
            tokio::spawn(async move { Ok(()) })
        };

        // Probe always reports massive staleness.
        let probe_fn = || async { Some(1_000_000_i64) };

        // Also wire a restart tracker so we can assert it
        // increments on each restart, not just rely on
        // spawn-counting.
        let restart_tracker = Arc::new(super::RestartTracker::default());
        let supervisor = tokio::spawn(super::supervised_ingest(
            "test",
            5_000, // 5s threshold (way under the 1Ms reported)
            probe_fn,
            spawn_fn,
            shutdown_rx,
            Some(Arc::clone(&restart_tracker)),
        ));

        // Advance the clock past two probe ticks (60s interval +
        // a bit). Spawn count should grow: initial spawn at start,
        // plus a restart after each stale tick. Be generous with
        // sleep — tokio's paused clock advances synchronously so
        // multiple ticks fire in one .advance call.
        tokio::time::sleep(std::time::Duration::from_secs(125)).await;

        let n = spawn_calls.load(Ordering::SeqCst);
        assert!(
            n >= 2,
            "expected supervisor to respawn at least once after staleness; got {n} spawn calls",
        );
        // The restart counter tracks ONLY restarts, not the initial
        // spawn — so it should be n-1 or more. The initial spawn
        // isn't a "restart", confirming the counter has the
        // monotonic-after-first-spawn semantics the metric needs.
        let restarts = restart_tracker
            .count
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            restarts >= 1,
            "expected restart_tracker to fire at least once; got {restarts}",
        );
        assert!(
            restarts <= n as u64,
            "restart_tracker.count ({restarts}) exceeded total spawn count ({n})",
        );
        // last_at_ms must have been written too (non-zero after at
        // least one restart). The exact value depends on the
        // mocked clock; non-zero is the contract.
        let last_at = restart_tracker
            .last_at_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            last_at > 0,
            "expected restart_tracker.last_at_ms to be set after restarts; got {last_at}",
        );

        let _ = shutdown_tx.send(true);
        let _ = supervisor.await;
    }

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
