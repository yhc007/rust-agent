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
use crate::coredb::types::{bucket_day, now_ms};
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
        let app_state = HealthAppState {
            health: health.clone(),
            btc_repo: btc_repo_for_health,
            market_repo: market_repo_for_health,
            decision_repo,
            decisions_cache: Arc::new(RwLock::new(None)),
            order_repo,
            orders_cache: Arc::new(RwLock::new(None)),
            ingest_cache: Arc::new(RwLock::new(None)),
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

/// TTL on the cached decisions-per-(strategy,side) tally that
/// `/metrics` emits. Prometheus default scrape is every 15s, so
/// 10s keeps the daemon's CoreDB hit rate at most ~1/10s under
/// whatever scrape concurrency. Tuning higher trades freshness for
/// load; tuning lower hits CoreDB more aggressively for marginal
/// benefit since the underlying data only ticks on backtest runs
/// (every 30 min in the default daemon config).
const METRICS_DECISIONS_CACHE_TTL_MS: i64 = 10_000;

/// TTL on the cached ingest-staleness probes. Shorter than the
/// decisions cache because the underlying data ticks every ~500ms
/// (Binance WS) and every 30s (Polymarket Gamma) — a 5s ceiling
/// preserves real staleness signal while still saving CoreDB the
/// per-scrape btc.latest + markets.list_open round-trips that
/// /health and /metrics each used to issue independently.
const INGEST_CACHE_TTL_MS: i64 = 5_000;

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
    /// Shared cache for the two ingest-staleness probes that
    /// `/health` and `/metrics` both need. Refresh on TTL expiry
    /// runs both probes once and stores whatever comes back, so a
    /// concurrent scrape on the *other* endpoint reuses the work.
    ingest_cache: Arc<RwLock<Option<IngestProbeCache>>>,
}

async fn health_handler(State(s): State<HealthAppState>) -> Json<HealthResponse> {
    let now = now_ms();
    let snap = s.health.read().await;

    let (btc_age, polymarket_age) = ingest_ages_cached(&s, now).await;

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
    let snap = s.health.read().await;

    let (btc_age, polymarket_age) = ingest_ages_cached(&s, now).await;

    let mut out = String::with_capacity(2048);
    let uptime_secs = (now - snap.started_at_ms).max(0) / 1000;

    // Helper: emit one HELP/TYPE/sample triplet. Prometheus
    // discourages duplicate HELP/TYPE lines, so families with
    // multiple labeled samples emit the header once via push_help.
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

    // Age-since-last-tick / age-since-last-success in seconds. NaN
    // when the subtask hasn't ticked yet — Prometheus accepts NaN as
    // a valid sample value but emits a warning in some scrapers, so
    // skip the sample entirely instead. A missing series is the
    // clearer signal here.
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
    if let Some(age_ms) = btc_age {
        out.push_str(&format!(
            "agent_ingest_age_seconds{{source=\"btc\"}} {}\n",
            age_ms.max(0) / 1000,
        ));
    }
    if let Some(age_ms) = polymarket_age {
        out.push_str(&format!(
            "agent_ingest_age_seconds{{source=\"polymarket\"}} {}\n",
            age_ms.max(0) / 1000,
        ));
    }

    out.push_str(
        "# HELP agent_user_channel_present 1 when the Polymarket user-channel WS listener was spawned at startup.\n",
    );
    out.push_str("# TYPE agent_user_channel_present gauge\n");
    out.push_str(&format!(
        "agent_user_channel_present {}\n",
        if snap.user_channel_present { 1 } else { 0 },
    ));

    // Today's decision count per (strategy, side). Gauge, not counter,
    // because the value resets at UTC midnight when the bucket_day
    // partition rolls — Prometheus `increase()` over a sub-day window
    // works perfectly on a gauge, and tagging this `_total` would
    // mislead `rate()`-using dashboards. Skipped silently when the
    // repo failed to construct at startup.
    //
    // Stale-while-revalidate: we hold the write lock through the
    // optional refresh, then either emit the freshly fetched data or
    // fall back to the previous cached entry on transient failures.
    // The cache age itself is exposed as `agent_decisions_today_cache_age_seconds`
    // so operators can verify the TTL is doing what they expect.
    if let Some(dec_repo) = &s.decision_repo {
        let mut cache_guard = s.decisions_cache.write().await;
        let need_refresh = cache_guard
            .as_ref()
            .map(|c| now - c.fetched_at_ms > METRICS_DECISIONS_CACHE_TTL_MS)
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
                        out.push_str(&format!(
                            "# decisions_today read failed (no cached fallback): {e}\n"
                        ));
                    } else {
                        out.push_str(&format!(
                            "# decisions_today refresh failed; serving cached data: {e}\n"
                        ));
                    }
                }
            }
        }

        if let Some(cache) = cache_guard.as_ref() {
            let cache_age_secs = (now - cache.fetched_at_ms).max(0) / 1000;
            out.push_str(
                "# HELP agent_decisions_today_cache_age_seconds Age of the cached decisions tally in seconds. Bounded by METRICS_DECISIONS_CACHE_TTL_MS.\n",
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
        }
    }

    // Today's order count per (strategy, side, exec, status).
    // exec="paper" iff `order_id` starts with "paper-" (PaperExec's
    // synthetic id format); else "live" — covers CLOB-issued IDs
    // and any future executor that doesn't share the paper- prefix.
    // status comes straight from the orders.status column ("filled"
    // / "pending" / "partial" / "canceled" today).
    //
    // strategy is joined back via decisions.decision_id, same as
    // settle-pnl does; orders that don't match a decision (legacy
    // rows or tool-driven inserts) fall into "other".
    //
    // Shares the 10s cache TTL with decisions — orders churn at
    // the same backtest cadence so freshness invariant matches.
    if let (Some(order_repo), Some(decision_repo)) =
        (s.order_repo.as_ref(), s.decision_repo.as_ref())
    {
        let mut cache_guard = s.orders_cache.write().await;
        let need_refresh = cache_guard
            .as_ref()
            .map(|c| now - c.fetched_at_ms > METRICS_DECISIONS_CACHE_TTL_MS)
            .unwrap_or(true);

        if need_refresh {
            let bd = bucket_day(now);
            // Decisions read can fail independently — if it does, we
            // fall back to "other" for every order's strategy.
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
                        out.push_str(&format!(
                            "# orders_today read failed (no cached fallback): {e}\n"
                        ));
                    } else {
                        out.push_str(&format!(
                            "# orders_today refresh failed; serving cached data: {e}\n"
                        ));
                    }
                }
            }
        }

        if let Some(cache) = cache_guard.as_ref() {
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
        }
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    (StatusCode::OK, headers, out)
}

/// Shared ingest-staleness cache lookup.
///
/// Both `/health` and `/metrics` hit `btc_repo.latest` +
/// `market_repo.list_open` to derive the ingest-age signal. Without
/// caching, two concurrent scrapes round-trip CoreDB four times for
/// data that ticks every ~500ms (Binance WS) and 30s (Polymarket
/// REST) anyway. The cache reduces that to at most one btc + one
/// markets read per `INGEST_CACHE_TTL_MS` window across both
/// endpoints.
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
        .map(|c| now - c.fetched_at_ms > INGEST_CACHE_TTL_MS)
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
