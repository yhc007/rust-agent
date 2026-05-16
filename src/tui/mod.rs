//! Live operator dashboard. Periodically reads CoreDB and renders:
//!
//! - Top: BTC spot + last refresh age + run-time counter.
//! - Strategy PnL: latest `strategy_pnl_snapshots` row per strategy.
//! - Open positions table.
//! - Recent decisions table (last 15, newest first).
//!
//! Hotkeys: `q` / Esc to quit, `r` to force a refresh.

use std::io::{self, Stdout};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use ratatui::Terminal;

use crate::coredb::agreement::AgreementRepo;
use crate::coredb::btc::BtcTickRepo;
use crate::coredb::decisions::DecisionRepo;
use crate::coredb::orders::PositionRepo;
use crate::coredb::pnl_breakdown::PnlBreakdownRepo;
use crate::coredb::strategy_pnl::StrategyPnlRepo;
use crate::coredb::types::{
    bucket_day, now_ms, AgreementSnapshot, BtcTick, Decision, Millis, PnlBreakdown, Position,
    StrategyPnlSnapshot,
};
use crate::coredb::CoreDb;

const REFRESH_EVERY: Duration = Duration::from_secs(5);
const RECENT_DECISIONS: usize = 15;

/// TTL on persisted dashboard state. After this many milliseconds
/// without a refresh, the saved `prev_ranks` + sort key are
/// treated as stale and the dashboard starts fresh. 2 hours
/// balances "operator restarted within their session" (state
/// usefully restored) against "stale ranks from 2 days ago
/// produce a meaningless Δ column". Tuneable via
/// `DASHBOARD_STATE_TTL_S` env (seconds).
const DASHBOARD_STATE_TTL_MS_DEFAULT: i64 = 2 * 60 * 60 * 1000;

/// Maximum number of points rendered in the strategy-pnl trend
/// sparkline. Older snapshots still inform the min/max of the column
/// shape via the slice we take, but only the last `SPARK_WIDTH` chars
/// are drawn so the table layout stays predictable across days of
/// accumulation. One unicode block per snapshot — column width is the
/// same number of cells.
const SPARK_WIDTH: usize = 24;

/// Width of the narrower agreement-rate sparkline that lives in the
/// strategy-pnl panel. Smaller than SPARK_WIDTH because the panel is
/// already wide and the agreement series is bounded [0,1] — the value
/// is more about smoothness than amplitude, so fewer samples still
/// convey the trend.
const AGREE_SPARK_WIDTH: usize = 14;

pub async fn run(
    coredb_uri: &str,
    health_url: Option<String>,
    pnl_days: u32,
) -> Result<()> {
    let pnl_days = pnl_days.max(1);
    let db = CoreDb::connect(coredb_uri)
        .await
        .with_context(|| format!("connect coredb at {coredb_uri}"))?;
    let btc_repo = BtcTickRepo::new(db.session()).await?;
    let dec_repo = DecisionRepo::new(db.session()).await?;
    let pos_repo = PositionRepo::new(db.session()).await?;
    let pnl_repo = StrategyPnlRepo::new(db.session()).await?;
    let agree_repo = AgreementRepo::new(db.session()).await?;
    let breakdown_repo = PnlBreakdownRepo::new(db.session()).await?;

    // Short-timeout HTTP client for the daemon's /health endpoint.
    // Built once and reused across refreshes — keep-alive matters
    // because the dashboard hits the endpoint every REFRESH_EVERY.
    // Constructed only when --health-url is set so paper-only setups
    // don't pay the connection cost.
    let health_client = health_url.as_ref().map(|_| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("reqwest client")
    });

    enable_raw_mode().context("enable raw_mode")?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("EnterAlternateScreen")?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend).context("Terminal::new")?;

    // Guard so terminal state always gets restored even on panic / error.
    let started_at = Instant::now();
    let res = main_loop(
        &mut terminal,
        started_at,
        &btc_repo,
        &dec_repo,
        &pos_repo,
        &pnl_repo,
        &agree_repo,
        &breakdown_repo,
        health_url.as_deref(),
        health_client.as_ref(),
        pnl_days,
    )
    .await;

    // Restore terminal before propagating any error from main_loop.
    disable_raw_mode().ok();
    execute!(terminal.backend_mut(), LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();
    res
}

async fn main_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    started_at: Instant,
    btc_repo: &BtcTickRepo,
    dec_repo: &DecisionRepo,
    pos_repo: &PositionRepo,
    pnl_repo: &StrategyPnlRepo,
    agree_repo: &AgreementRepo,
    breakdown_repo: &PnlBreakdownRepo,
    health_url: Option<&str>,
    health_client: Option<&reqwest::Client>,
    pnl_days: u32,
) -> Result<()> {
    // Restart counts seen on the most recent /health probe. Stashed
    // here (not in the Snapshot) because it must survive the
    // assignment that rebuilds `snapshot` on each refresh — the
    // whole point is comparing across consecutive snapshots.
    let mut prev_ingest_restarts: Option<(u64, u64)> = None;
    // Per-strategy rank from the previous render's comparison
    // panel. Feeds the Δ column so the operator sees momentum
    // ("X moved up since last refresh") without diffing snapshots
    // mentally. Restored from disk when a recent dashboard run
    // left a state file behind — a quick restart preserves
    // momentum continuity.
    let state_path = dashboard_state_path();
    let state_ttl_ms = dashboard_state_ttl_ms();
    let persisted = load_persisted_dashboard_state(&state_path, now_ms(), state_ttl_ms);
    let mut prev_ranks: std::collections::HashMap<String, usize> = persisted
        .as_ref()
        .map(|p| p.ranks.clone())
        .unwrap_or_default();
    let mut comparison_sort: ComparisonSort = persisted
        .as_ref()
        .map(|p| p.sort)
        .unwrap_or(ComparisonSort::Window7dDesc);
    // Restore the focused-strategy filter too so a restart
    // doesn't drop the operator back to "show every strategy."
    // Note: if the saved strategy is no longer in the current
    // snapshot, the cycle_strategy fallback handling lets the
    // operator press +/- once to land back at None — better
    // than silently clobbering their choice here.
    let restored_filter: Option<String> = persisted
        .as_ref()
        .and_then(|p| p.strategy_filter.clone());
    let mut snapshot = fetch_snapshot(
        btc_repo, dec_repo, pos_repo, pnl_repo, agree_repo, breakdown_repo, health_url,
        health_client, pnl_days, prev_ingest_restarts,
    )
    .await;
    if let Some(chip) = &snapshot.health {
        prev_ingest_restarts = chip.ingest_restarts;
    }
    let mut last_refresh = Instant::now();
    // Strategy currently focused in the decisions panel. `None` = show
    // every strategy. Set by digit-key hotkeys; cleared by `0` or `c`.
    // Survives refreshes so the operator's selection doesn't reset on
    // every auto-tick — and now survives a dashboard restart too,
    // sourced from `restored_filter` above. Stale filter check: if
    // the restored strategy isn't in the live snapshot's
    // strategies-in-view, drop it. Otherwise the operator would
    // see a phantom-focused state (no row highlights, "vs <name>"
    // header pointing at a strategy that doesn't exist anymore)
    // until they press `0` to clear. The check only runs once at
    // startup; mid-session filter changes are unaffected.
    let mut strategy_filter: Option<String> =
        validate_restored_filter(restored_filter, &strategies_in_view(&snapshot));

    loop {
        let uptime = started_at.elapsed();
        let snap_age = last_refresh.elapsed();
        // Cache the sorted strategy list so hotkey-1..9 lookup and the
        // footer "[1] baseline ..." render are consistent within a
        // single frame.
        let strategies = strategies_in_view(&snapshot);
        terminal.draw(|f| {
            draw(
                f,
                &snapshot,
                snap_age,
                uptime,
                &strategies,
                strategy_filter.as_deref(),
                &prev_ranks,
                comparison_sort,
            )
        })?;
        // Refresh prev_ranks to the ordering this frame just
        // rendered, so the next frame's Δ column shows momentum
        // relative to "the rendering the operator just saw." Uses
        // the same sort key the panel just rendered with so the
        // ranks line up; switching sort mid-session changes the
        // baseline naturally.
        let next_rows = build_strategy_comparison_rows_sorted(
            &snapshot,
            strategy_filter.as_deref(),
            comparison_sort,
        );
        prev_ranks = next_rows
            .iter()
            .enumerate()
            .map(|(i, r)| (r.strategy.clone(), i + 1))
            .collect();
        // Persist after each refresh so a quick restart picks up
        // a meaningful baseline for the Δ column. Best-effort —
        // I/O failures are debug-logged and don't break the
        // render loop.
        save_persisted_dashboard_state(
            &state_path,
            &PersistedDashboardState {
                ranks: prev_ranks.clone(),
                sort: comparison_sort,
                saved_at_ms: now_ms(),
                strategy_filter: strategy_filter.clone(),
            },
        );

        // Drain pending input with a small budget so the auto-refresh
        // tick is still responsive. We poll for `min(REFRESH_EVERY -
        // snap_age, 200ms)` and break to re-render on any event.
        let until_refresh = REFRESH_EVERY.saturating_sub(snap_age);
        let poll_budget = until_refresh.min(Duration::from_millis(200));
        if event::poll(poll_budget).context("event poll")? {
            match event::read().context("event read")? {
                Event::Key(k) if k.kind == KeyEventKind::Press => match k.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                    KeyCode::Char('r') => {
                        snapshot = fetch_snapshot(
                            btc_repo, dec_repo, pos_repo, pnl_repo, agree_repo, breakdown_repo,
                            health_url, health_client, pnl_days, prev_ingest_restarts,
                        )
                        .await;
                        if let Some(chip) = &snapshot.health {
                            if let Some(r) = chip.ingest_restarts {
                                prev_ingest_restarts = Some(r);
                            }
                        }
                        last_refresh = Instant::now();
                    }
                    // Digit hotkeys: focus the Nth strategy currently
                    // visible in the cached list. `0` clears the filter.
                    // Out-of-range digits are ignored so a misclick
                    // doesn't blank the panel.
                    KeyCode::Char('0') | KeyCode::Char('c') => strategy_filter = None,
                    KeyCode::Char(ch @ '1'..='9') => {
                        let idx = (ch as u8 - b'1') as usize;
                        if let Some(name) = strategies.get(idx) {
                            strategy_filter = Some(name.clone());
                        }
                    }
                    // +/- cycle through the filter states as a single
                    // cycle:  None → strategies[0] → strategies[1] → …
                    //  → strategies[N-1] → None → …
                    // Useful when N exceeds the 9-digit hotkey range.
                    // No-op when the strategy list is empty.
                    KeyCode::Char('+') | KeyCode::Char('=') => {
                        strategy_filter = cycle_strategy(&strategies, strategy_filter.as_deref(), 1);
                    }
                    KeyCode::Char('-') | KeyCode::Char('_') => {
                        strategy_filter = cycle_strategy(&strategies, strategy_filter.as_deref(), -1);
                    }
                    // Cycle the comparison panel's sort key. No
                    // refresh required; the next terminal.draw
                    // pick the new order. Operator preferences
                    // stay session-scoped (resets on dashboard
                    // restart, same as the strategy filter).
                    KeyCode::Char('s') | KeyCode::Char('S') => {
                        comparison_sort = comparison_sort.cycle();
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        // Auto-refresh on the cadence.
        if last_refresh.elapsed() >= REFRESH_EVERY {
            snapshot = fetch_snapshot(
                btc_repo, dec_repo, pos_repo, pnl_repo, agree_repo, breakdown_repo, health_url,
                health_client, pnl_days, prev_ingest_restarts,
            )
            .await;
            if let Some(chip) = &snapshot.health {
                if let Some(r) = chip.ingest_restarts {
                    prev_ingest_restarts = Some(r);
                }
            }
            last_refresh = Instant::now();
        }
    }
}

/// One frame's worth of CoreDB data. Held as a single value so the
/// render path doesn't have to worry about half-loaded state — every
/// repo read failure rolls into the same `errors` Vec and gets
/// surfaced in the footer instead of crashing the UI.
#[derive(Default)]
struct Snapshot {
    btc: Option<BtcTick>,
    positions: Vec<Position>,
    decisions: Vec<Decision>,
    snapshots: Vec<StrategyPnlSnapshot>,
    agreements: Vec<AgreementSnapshot>,
    consensus: Vec<MarketConsensus>,
    /// Yesterday's final realized PnL per (strategy, exec). Footer of
    /// the strategy-pnl panel reads this to show end-of-day totals
    /// alongside today's running numbers. One row per (strategy, exec)
    /// — both paper and live get summed per strategy in the footer.
    pnl_breakdown_yesterday: Vec<PnlBreakdown>,
    /// Rolling 7-day pnl_breakdown rows (one per (strategy, exec) per
    /// bucket_day) concatenated across the last 7 UTC days. The
    /// "7d total:" footer line aggregates these per strategy.
    /// Mirrors the agent_pnl_breakdown_window_* /metrics gauge.
    pnl_breakdown_window: Vec<PnlBreakdown>,
    /// Result of the most recent /health probe. `None` when the
    /// dashboard wasn't started with --health-url. Otherwise carries
    /// a parsed result *or* an "unreachable" marker so the header
    /// chip can distinguish "configured but failing" from "off".
    health: Option<HealthChip>,
    errors: Vec<String>,
}

/// Reduced view of the daemon /health response — just enough to
/// render the header chip and (optionally) one tooltip line below it.
#[derive(Debug, Clone)]
struct HealthChip {
    status: HealthStatus,
    /// `Some(seconds)` when /health replied with a usable status
    /// payload; `None` when the probe failed (connection refused,
    /// timeout, bad JSON, ...).
    daemon_uptime_secs: Option<i64>,
    /// Free-form one-liner used in the secondary header row. Captures
    /// either the worst subtask's error or the unreachable reason.
    detail: Option<String>,
    /// Restart counts seen on this probe. Carried on the chip so
    /// the main_loop can stash them and compare on the next
    /// refresh — that's how "binance restarted 3x" surfaces in
    /// the chip detail line.
    ///
    /// `None` when the probe failed (no payload). `Some((0, 0))`
    /// on a fresh daemon where nothing has ever restarted.
    ingest_restarts: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HealthStatus {
    Ok,
    Degraded,
    Unreachable,
}

impl HealthStatus {
    fn label(self) -> &'static str {
        match self {
            HealthStatus::Ok => " ok ",
            HealthStatus::Degraded => " degraded ",
            HealthStatus::Unreachable => " unreachable ",
        }
    }
    fn color(self) -> Color {
        match self {
            HealthStatus::Ok => Color::Green,
            HealthStatus::Degraded => Color::Yellow,
            HealthStatus::Unreachable => Color::Red,
        }
    }
}

/// Subset of the daemon's HealthResponse we parse for chip rendering.
/// Stays decoupled from `daemon::HealthResponse` on purpose: the wire
/// format is the integration contract; the dashboard binds to that,
/// not to internal Rust types that might churn.
#[derive(Debug, serde::Deserialize)]
struct HealthWire {
    status: String,
    uptime_secs: i64,
    #[serde(default)]
    backtest: HealthSubtaskWire,
    #[serde(default)]
    compare: HealthSubtaskWire,
    #[serde(default)]
    settle: HealthSubtaskWire,
    #[serde(default)]
    ingest_btc_age_ms: Option<i64>,
    #[serde(default)]
    ingest_polymarket_age_ms: Option<i64>,
    /// Per-cache freshness ages (ms) keyed by cache slot. Surfaced
    /// in the chip detail line when a cache is way past its TTL so
    /// the operator can pinpoint which sub-system is stuck without
    /// reading a `/metrics` page. Missing on pre-2026-05-16 daemons;
    /// `#[serde(default)]` keeps backward compat.
    #[serde(default)]
    cache_ages_ms: HealthCacheAgesWire,
    /// Per-source lifetime restart count. The dashboard compares
    /// these across refreshes to surface flapping workers in the
    /// chip detail line. Missing on pre-2026-05-16 daemons; default
    /// to zeros so old responses parse cleanly.
    #[serde(default)]
    ingest_restarts: IngestRestartsWire,
}

#[derive(Debug, Default, Clone, Copy, serde::Deserialize)]
struct IngestRestartsWire {
    #[serde(default)]
    binance: u64,
    #[serde(default)]
    polymarket: u64,
}

#[derive(Debug, Default, serde::Deserialize)]
struct HealthCacheAgesWire {
    #[serde(default)]
    decisions: Option<i64>,
    #[serde(default)]
    orders: Option<i64>,
    #[serde(default)]
    pnl_daily: Option<i64>,
    #[serde(default)]
    pnl_breakdown: Option<i64>,
    #[serde(default)]
    pnl_breakdown_yesterday: Option<i64>,
    #[serde(default)]
    pnl_breakdown_window: Option<i64>,
    #[serde(default)]
    positions: Option<i64>,
    #[serde(default)]
    ingest_probe: Option<i64>,
}

#[derive(Debug, Default, serde::Deserialize)]
struct HealthSubtaskWire {
    #[serde(default)]
    consecutive_errors: u32,
    #[serde(default)]
    last_error: Option<String>,
}

/// Per-market view of how the active strategies voted today. The
/// sparkline / decisions panels surface single rows; this view answers
/// "do my N strategies agree on this market right now?" which only
/// makes sense when ≥2 strategies have evaluated the same market.
#[derive(Debug, Clone)]
struct MarketConsensus {
    market_slug: String,
    /// strategy → latest non-PASS side on this market today. PASS
    /// entries are not stored here; absence = either no vote or only
    /// a PASS vote so far.
    active_picks: std::collections::BTreeMap<String, String>,
    /// Number of strategies that emitted a PASS row on this market.
    /// Together with `active_picks.len()` this tells the operator how
    /// many strategies have looked at the market at all.
    pass_strategies: u32,
    /// Sum of size_usd across `active_picks`. Sort key for the panel.
    sum_size_usd: f64,
    /// Newest ts_ms across all rows seen for this market.
    last_ts_ms: i64,
    agreement: AgreementKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AgreementKind {
    /// ≥2 active strategies and every active pick is the same side.
    AllAgree,
    /// ≥2 active strategies and they don't all match.
    Split,
    /// Exactly 1 active strategy; the rest passed or haven't voted.
    Solo,
    /// No active strategy — every vote was PASS.
    AllPass,
}

impl AgreementKind {
    fn short_label(self) -> &'static str {
        match self {
            AgreementKind::AllAgree => "✓ all",
            AgreementKind::Split => "✗ split",
            AgreementKind::Solo => "solo",
            AgreementKind::AllPass => "all pass",
        }
    }
    fn color(self) -> Color {
        match self {
            AgreementKind::AllAgree => Color::Green,
            AgreementKind::Split => Color::Yellow,
            AgreementKind::Solo => Color::Cyan,
            AgreementKind::AllPass => Color::DarkGray,
        }
    }
}

async fn fetch_snapshot(
    btc: &BtcTickRepo,
    dec: &DecisionRepo,
    pos: &PositionRepo,
    pnl: &StrategyPnlRepo,
    agree: &AgreementRepo,
    breakdown: &PnlBreakdownRepo,
    health_url: Option<&str>,
    health_client: Option<&reqwest::Client>,
    pnl_days: u32,
    prev_ingest_restarts: Option<(u64, u64)>,
) -> Snapshot {
    let mut s = Snapshot::default();
    match btc.latest("BTCUSDT").await {
        Ok(t) => s.btc = t,
        Err(e) => s.errors.push(format!("btc.latest: {e}")),
    }
    let bd = bucket_day(now_ms());
    match dec.list_day(bd).await {
        Ok(mut v) => {
            // Newest first; the dashboard only displays the head of
            // this list anyway.
            v.sort_by(|a, b| b.ts_ms.cmp(&a.ts_ms));
            s.decisions = v;
        }
        Err(e) => s.errors.push(format!("decisions.list_day: {e}")),
    }
    match pos.list_all().await {
        Ok(mut v) => {
            // Most recently updated first.
            v.sort_by(|a, b| b.updated_at_ms.cmp(&a.updated_at_ms));
            s.positions = v;
        }
        Err(e) => s.errors.push(format!("positions.list_all: {e}")),
    }
    // Read N day-partitions for the strategy-pnl sparkline. Default
    // 1 keeps the historical "today only" behavior; a higher value
    // surfaces real multi-day trends in the trend column. A failure
    // on one day is logged into `errors` but doesn't bail the rest
    // of the window.
    const DAY_MS: Millis = 86_400_000;
    let days = pnl_days.max(1);
    for i in 0..days as i64 {
        let day_bd = bd - (days as i64 - 1 - i) * DAY_MS;
        match pnl.list_day(day_bd).await {
            Ok(v) => s.snapshots.extend(v),
            Err(e) => s.errors.push(format!("strategy_pnl.list_day({day_bd}): {e}")),
        }
        // Agreement snapshots share the same window — same partition
        // shape (`bucket_day`), populated by the same compare-pnl
        // run, so reading them together keeps the panel's per-row
        // trend self-consistent with its pnl trend.
        match agree.list_day(day_bd).await {
            Ok(v) => s.agreements.extend(v),
            Err(e) => s.errors.push(format!("agreement.list_day({day_bd}): {e}")),
        }
    }
    s.consensus = build_consensus(&s.decisions);
    // Yesterday's pnl_breakdown — one extra CoreDB read so the
    // strategy-pnl panel footer can show end-of-day final realized PnL
    // by strategy alongside today's running totals. Empty rowset is
    // expected on a fresh deployment; we surface that as "no settled
    // trades yet" in the render path rather than as an error.
    let yesterday_bd = bd - 86_400_000;
    match breakdown.list_day(yesterday_bd).await {
        Ok(v) => s.pnl_breakdown_yesterday = v,
        Err(e) => s.errors.push(format!("pnl_breakdown.list_day({yesterday_bd}): {e}")),
    }
    // 7-day rolling pnl_breakdown for the "7d total:" footer line.
    // One list_day per day across the last 7 UTC bucket_days. A
    // failed read on any single day is recorded as an error but
    // doesn't poison the rest of the window — the footer renders
    // whatever days returned successfully. Yesterday (already read
    // above) is included again here so the aggregate is complete;
    // CoreDB-side caching makes the duplicate cheap.
    const WINDOW_DAYS: i64 = 7;
    const DAY_MS_WIN: Millis = 86_400_000;
    for i in 0..WINDOW_DAYS {
        let day_bd = bd - i * DAY_MS_WIN;
        match breakdown.list_day(day_bd).await {
            Ok(v) => s.pnl_breakdown_window.extend(v),
            Err(e) => s
                .errors
                .push(format!("pnl_breakdown.list_day({day_bd}) [7d window]: {e}")),
        }
    }
    if let (Some(url), Some(client)) = (health_url, health_client) {
        s.health = Some(fetch_health(client, url, prev_ingest_restarts).await);
    }
    s
}

/// Hit the daemon /health endpoint once and reduce the response to a
/// [`HealthChip`]. Any non-2xx, timeout, connection error, or JSON
/// parse error collapses into an Unreachable chip with the reason in
/// `detail` — the operator should be able to tell *why* the dashboard
/// can't reach the daemon without dropping to a terminal.
async fn fetch_health(
    client: &reqwest::Client,
    url: &str,
    prev_restarts: Option<(u64, u64)>,
) -> HealthChip {
    let resp = match client.get(url).send().await {
        Ok(r) => r,
        Err(e) => {
            return HealthChip {
                status: HealthStatus::Unreachable,
                daemon_uptime_secs: None,
                detail: Some(format!("GET failed: {}", short_err(&e.to_string()))),
                ingest_restarts: None,
            };
        }
    };
    if !resp.status().is_success() {
        return HealthChip {
            status: HealthStatus::Unreachable,
            daemon_uptime_secs: None,
            detail: Some(format!("HTTP {}", resp.status().as_u16())),
            ingest_restarts: None,
        };
    }
    let body: HealthWire = match resp.json().await {
        Ok(b) => b,
        Err(e) => {
            return HealthChip {
                status: HealthStatus::Unreachable,
                daemon_uptime_secs: None,
                detail: Some(format!("bad JSON: {}", short_err(&e.to_string()))),
                ingest_restarts: None,
            };
        }
    };
    let status = match body.status.as_str() {
        "ok" => HealthStatus::Ok,
        _ => HealthStatus::Degraded,
    };
    let mut detail = if status == HealthStatus::Ok {
        // Healthy path: include a compact ingest-age summary instead
        // of the worst-error line so the dashboard surfaces actionable
        // info even when nothing is broken yet.
        match (body.ingest_btc_age_ms, body.ingest_polymarket_age_ms) {
            (Some(b), Some(p)) => Some(format!("btc {}ms / poly {}ms", b, p)),
            _ => None,
        }
    } else {
        // Degraded path: pick the worst subtask's error message — the
        // one most likely to explain why status flipped. Falls back to
        // stale ingest if every subtask is fine.
        worst_subtask_error(&body).or_else(|| {
            match (body.ingest_btc_age_ms, body.ingest_polymarket_age_ms) {
                (None, _) => Some("btc ingest unreachable".into()),
                (_, None) => Some("polymarket ingest unreachable".into()),
                (Some(b), Some(p)) => Some(format!("ingest stale: btc {}ms / poly {}ms", b, p)),
            }
        })
    };
    // Append a stalest-cache hint when any cache has been sitting
    // past its TTL for a while. Highlights stuck sub-systems without
    // making the operator open /metrics. STALE_CACHE_HINT_MS is
    // intentionally generous — settle-pnl runs hourly so the pnl
    // caches legitimately sit at 5+ minutes between refreshes.
    if let Some(hint) = stalest_cache_hint(&body.cache_ages_ms) {
        let combined = match detail {
            Some(existing) => format!("{existing} · {hint}"),
            None => hint,
        };
        detail = Some(combined);
    }
    // Restart-rate hint: when the daemon's per-source restart
    // count has ticked up since our last successful probe, surface
    // it so the operator sees a flapping worker even when the
    // staleness probe is transiently healthy (the supervisor's
    // restart unsticks the worker → next /health shows fresh
    // ingest → without this hint, the dashboard never flags the
    // event). Only fires when we have a `prev` to compare against
    // — the first poll after dashboard startup just stashes a
    // baseline.
    let current_restarts = (
        body.ingest_restarts.binance,
        body.ingest_restarts.polymarket,
    );
    let mut status = status;
    let delta_hint = restart_delta_hint(prev_restarts, current_restarts);
    if let Some(hint) = delta_hint.as_ref() {
        let combined = match detail {
            Some(existing) => format!("{existing} · {hint}"),
            None => hint.clone(),
        };
        detail = Some(combined);
    }
    // Restart events are dashboard-detected (the daemon is
    // stateless across probes). Downgrade the chip status locally
    // so the operator's glance-level signal — green vs yellow —
    // reflects the incident the same way the detail line does.
    // Doesn't affect the daemon's /health.status field (that's
    // still grep'd by reverse proxies based on the daemon's own
    // view).
    status = chip_status_with_local_downgrade(status, delta_hint.is_some());
    HealthChip {
        status,
        daemon_uptime_secs: Some(body.uptime_secs),
        detail,
        ingest_restarts: Some(current_restarts),
    }
}

/// Apply a dashboard-local downgrade rule on top of the daemon's
/// reported status. Currently: if any local signal (only the
/// restart-delta hint for now) fired AND the daemon's status was
/// `Ok`, bump to `Degraded` so the chip color matches the detail
/// line's hint. Leaves Degraded / Unreachable untouched — the
/// daemon already flagged the problem; downgrading further would
/// just hide the original signal.
fn chip_status_with_local_downgrade(
    daemon_status: HealthStatus,
    local_signal_fired: bool,
) -> HealthStatus {
    if local_signal_fired && daemon_status == HealthStatus::Ok {
        HealthStatus::Degraded
    } else {
        daemon_status
    }
}

/// Render a chip hint like "binance restarted 2×" when the daemon's
/// per-source restart count has increased since the previous probe.
/// `prev == None` means "first poll" — no baseline yet, no hint.
/// Both sources can fire in the same string ("binance 1× · poly 3×")
/// so the operator sees a multi-source incident at a glance.
fn restart_delta_hint(
    prev: Option<(u64, u64)>,
    current: (u64, u64),
) -> Option<String> {
    let (prev_b, prev_p) = prev?;
    let d_b = current.0.saturating_sub(prev_b);
    let d_p = current.1.saturating_sub(prev_p);
    if d_b == 0 && d_p == 0 {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    if d_b > 0 {
        parts.push(format!("binance restarted {d_b}×"));
    }
    if d_p > 0 {
        parts.push(format!("polymarket restarted {d_p}×"));
    }
    Some(parts.join(" · "))
}

/// Threshold above which a cache is flagged as "stale" in the chip
/// detail. settle-pnl runs hourly, so the pnl caches commonly sit at
/// minutes-old between refreshes — 10 min is the rough operator-pain
/// threshold ("we should look at this") without being so tight that
/// every legitimate quiet period triggers a warning.
const STALE_CACHE_HINT_MS: i64 = 10 * 60 * 1000;

/// Render a chip hint like "stale: pnl_breakdown 1234s" when at
/// least one cache slot's age is above [`STALE_CACHE_HINT_MS`].
/// Returns `None` when every cache is fresh or unpopulated (None
/// ages are treated as "never refreshed" rather than "stale forever"
/// — the operator already sees that signal via the dashboard's
/// empty-row state).
///
/// Pure helper so the threshold logic + label formatting are
/// unit-testable without spinning up a /health server.
fn stalest_cache_hint(ages: &HealthCacheAgesWire) -> Option<String> {
    let slots: [(&str, Option<i64>); 8] = [
        ("decisions", ages.decisions),
        ("orders", ages.orders),
        ("pnl_daily", ages.pnl_daily),
        ("pnl_breakdown", ages.pnl_breakdown),
        ("pnl_breakdown_yesterday", ages.pnl_breakdown_yesterday),
        ("pnl_breakdown_window", ages.pnl_breakdown_window),
        ("positions", ages.positions),
        ("ingest_probe", ages.ingest_probe),
    ];
    let (name, age) = slots
        .iter()
        .filter_map(|(n, a)| a.map(|v| (*n, v)))
        .filter(|(_, a)| *a > STALE_CACHE_HINT_MS)
        .max_by_key(|(_, a)| *a)?;
    Some(format!("stale: {name} {}s", age / 1000))
}

fn worst_subtask_error(body: &HealthWire) -> Option<String> {
    let candidates = [
        ("backtest", &body.backtest),
        ("compare", &body.compare),
        ("settle", &body.settle),
    ];
    let worst = candidates
        .iter()
        .filter(|(_, s)| s.consecutive_errors > 0)
        .max_by_key(|(_, s)| s.consecutive_errors)?;
    let (label, sub) = worst;
    let msg = sub.last_error.as_deref().unwrap_or("(no message)");
    Some(format!(
        "{}: {}× — {}",
        label,
        sub.consecutive_errors,
        short_err(msg)
    ))
}

fn short_err(s: &str) -> String {
    const MAX: usize = 80;
    if s.chars().count() <= MAX {
        s.to_string()
    } else {
        s.chars().take(MAX).collect::<String>() + "…"
    }
}

/// Dashboard state persisted across restarts. Just enough to keep
/// the Δ rank-change column and the operator's last-active sort
/// key meaningful after a restart — the actual data on the panels
/// is always re-fetched from CoreDB.
///
/// `saved_at_ms` drives a 2-hour TTL: dashboards that have been
/// dark for longer than that get a fresh start (a Δ comparing
/// today's ranks against ranks from 2 days ago is more confusing
/// than informative).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedDashboardState {
    pub ranks: std::collections::HashMap<String, usize>,
    pub sort: ComparisonSort,
    pub saved_at_ms: i64,
    /// Active strategy filter ("focused strategy") at the time
    /// the state was saved. `#[serde(default)]` so existing
    /// state files written before this field was added still
    /// parse cleanly. `None` = no filter (the dashboard's
    /// default state, equivalent to pressing `0` or `c`).
    #[serde(default)]
    pub strategy_filter: Option<String>,
}

/// Resolve the persistence path: `$DASHBOARD_STATE_PATH` if set,
/// else `~/.cache/rust-agent/dashboard_state.json`. The directory
/// is created lazily by `save_persisted_dashboard_state` — read
/// returns `None` cleanly when the path doesn't exist yet.
fn dashboard_state_path() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("DASHBOARD_STATE_PATH") {
        return std::path::PathBuf::from(p);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    std::path::PathBuf::from(home)
        .join(".cache")
        .join("rust-agent")
        .join("dashboard_state.json")
}

fn dashboard_state_ttl_ms() -> i64 {
    std::env::var("DASHBOARD_STATE_TTL_S")
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(DASHBOARD_STATE_TTL_MS_DEFAULT)
}

/// Read the persisted dashboard state from disk. Returns `None`
/// when (a) the file doesn't exist, (b) the file fails to parse,
/// or (c) the saved state is older than the TTL window. None of
/// these are operator-visible errors — the dashboard just
/// starts fresh.
fn load_persisted_dashboard_state(
    path: &std::path::Path,
    now_ms: i64,
    ttl_ms: i64,
) -> Option<PersistedDashboardState> {
    let bytes = std::fs::read(path).ok()?;
    let state: PersistedDashboardState = serde_json::from_slice(&bytes).ok()?;
    if (now_ms - state.saved_at_ms).abs() > ttl_ms {
        return None;
    }
    Some(state)
}

/// Best-effort write — creates the parent directory if missing,
/// serializes the state, swallows I/O errors with a tracing
/// warning. Called after every dashboard refresh; failures must
/// not break rendering.
fn save_persisted_dashboard_state(
    path: &std::path::Path,
    state: &PersistedDashboardState,
) {
    if let Some(parent) = path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            tracing::debug!("dashboard state: mkdir {} failed: {e}", parent.display());
            return;
        }
    }
    match serde_json::to_vec(state) {
        Ok(bytes) => {
            if let Err(e) = std::fs::write(path, bytes) {
                tracing::debug!(
                    "dashboard state: write {} failed: {e}",
                    path.display()
                );
            }
        }
        Err(e) => {
            tracing::debug!("dashboard state: serialize failed: {e}");
        }
    }
}

/// Truncate `s` to at most `max` characters, appending "…" when
/// trimmed so the operator can tell the line was clipped vs.
/// genuinely fit. `max == 0` returns an empty string;
/// `max == 1` returns just "…" for any non-empty input.
///
/// Used to keep the header chip's second line readable on narrow
/// terminals: the detail stack (worst-subtask · stale-cache ·
/// restart-delta · ingest-age summary) can easily exceed 100
/// chars; without a budget the line wraps and the panel layout
/// breaks. Earlier-added hints (most important first) survive
/// the trim.
fn truncate_with_ellipsis(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    let len = s.chars().count();
    if len <= max {
        return s.to_string();
    }
    // Reserve one char for the ellipsis itself.
    let take = max.saturating_sub(1);
    let mut out: String = s.chars().take(take).collect();
    out.push('…');
    out
}

/// Human-readable description of the time span covered by a snapshot
/// rowset. Used in the strategy-pnl panel title so the operator
/// knows whether the sparkline reflects today only or a wider
/// historical window without checking which flag they passed.
///
/// Returns "today's series" for sub-day spans (the typical case
/// with default `--pnl-days 1`), "Nd Nh series" for hour-resolution
/// spans, and "Nd series" for whole-day spans. Empty input falls
/// through to "today's series" — the title is then paired with the
/// "no snapshots yet" body anyway.
/// Sorted, deduped list of strategy labels we currently have data
/// for. Drives the digit-hotkey mapping: position 0 = key `1`,
/// position 1 = key `2`, ... so the footer's `[1] baseline ...` hint
/// stays in lockstep with what the keys actually do. Prefers
/// strategy-pnl snapshots over decisions for stability — a tick that
/// drops a noisy strategy from decisions shouldn't reshuffle hotkey
/// indexes mid-session.
fn strategies_in_view(snapshot: &Snapshot) -> Vec<String> {
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for s in &snapshot.snapshots {
        seen.insert(s.strategy.clone());
    }
    for d in &snapshot.decisions {
        seen.insert(d.effective_strategy().to_string());
    }
    seen.into_iter().collect()
}

/// Order the consensus rows for rendering. When no filter is
/// active, returns references in the same order `build_consensus`
/// produced (split → all-agree → solo → all-pass, by size, by ts).
///
/// When `filter` is `Some(name)`, partitions into two tiers:
///   - tier 0: markets that `active_picks` contains `name` for, OR
///             that have `name` listed among their PASS strategies
///             (i.e. the filtered strategy weighed in either way)
///   - tier 1: everything else
///
/// We can't easily distinguish "this strategy passed on this market"
/// from "this strategy hasn't been here yet" because `MarketConsensus`
/// only stores a `pass_strategies` count, not the per-strategy list.
/// So tier 0 only catches active picks. Solo / AllPass markets
/// without the filter's strategy fall into tier 1.
///
/// Within each tier the existing pre-sort order is preserved (stable
/// sort) so the secondary keys (agreement, size, ts) still apply.
fn filter_aware_consensus<'a>(
    rows: &'a [MarketConsensus],
    filter: Option<&str>,
) -> Vec<&'a MarketConsensus> {
    let mut out: Vec<&'a MarketConsensus> = rows.iter().collect();
    if let Some(name) = filter {
        out.sort_by_key(|c| {
            if c.active_picks.contains_key(name) {
                0
            } else {
                1
            }
        });
    }
    out
}

/// Cycle the strategy filter forward (`step = 1`) or backward
/// (`step = -1`) through the cached strategy list. `None` is part
/// of the cycle (meaning "show all"), so the order is:
///   None → strategies[0] → strategies[1] → … → strategies[N-1] → None
///
/// Returns `None` when `strategies` is empty so the operator's
/// keystroke is a no-op against a freshly-launched DB with no data.
/// Returning early in that case also avoids modulus-by-zero math.
///
/// A current filter that's no longer in the list (the strategy
/// At dashboard startup, sanity-check a restored
/// `strategy_filter` against the first frame's live strategies.
/// Returns `None` when the saved strategy is absent (e.g.
/// snapshot data shifted while the dashboard was down, or the
/// operator's deployment changed its LLM presets). Otherwise
/// passes the filter through unchanged.
///
/// Mid-session filter changes are unaffected — this only
/// covers the "restore from disk" path, where a phantom focus
/// state would otherwise live until the operator pressed `0`.
fn validate_restored_filter(
    restored: Option<String>,
    strategies: &[String],
) -> Option<String> {
    match restored {
        Some(name) if strategies.iter().any(|s| s == &name) => Some(name),
        _ => None,
    }
}

/// dropped out between refreshes) is treated as if `None` was
/// selected — keeps the UI predictable when the underlying data
/// shifts.
fn cycle_strategy(
    strategies: &[String],
    current: Option<&str>,
    step: i32,
) -> Option<String> {
    if strategies.is_empty() {
        return None;
    }
    // Map None to position N (one past the end); the cycle has
    // length N+1 with None as the "wrap" slot.
    let len = strategies.len();
    let cur_pos: usize = match current {
        Some(name) => strategies
            .iter()
            .position(|s| s == name)
            .unwrap_or(len),
        None => len,
    };
    // Modulo arithmetic in the (N+1)-cycle. Cast to i64 first to
    // avoid usize underflow on `step = -1` when `cur_pos = 0`.
    let n_plus_1 = (len + 1) as i64;
    let next = (cur_pos as i64 + step as i64).rem_euclid(n_plus_1);
    if next as usize == len {
        None
    } else {
        Some(strategies[next as usize].clone())
    }
}

/// For each strategy, build a ts-sorted series of mean pairwise
/// agreement rates against every other strategy at that ts.
///
/// Why mean: agreement is a per-pair quantity, but the strategy-pnl
/// panel renders one row per strategy. The natural per-strategy
/// aggregation is "average of every pair I participate in" — it
/// answers "how aligned is this strategy with the room?" in a single
/// scalar without losing the pair-wise data (which still lives in
/// `agreement_snapshots` for the `agreement-history` command).
///
/// Output is `strategy → Vec<rate>`, ts-sorted. Empty input → empty
/// output. Skipped pairs (`shared == 0`) contribute 0.0 via
/// `AgreementSnapshot::rate` — these are degenerate-but-existing
/// snapshot rows and including them keeps the timestamp denominator
/// consistent across strategies.
fn per_strategy_agree_series(
    agreements: &[AgreementSnapshot],
) -> std::collections::HashMap<String, Vec<f64>> {
    per_strategy_agree_series_filtered(agreements, None)
}

/// Filter-aware variant. When `filter == Some(name)`:
///   - For rows whose `strategy_a == name`: full mean across every
///     pair the filter participates in (matches the unfiltered
///     series — this is the filter row's own column).
///   - For rows whose `strategy_a != name`: include only the pair
///     where `strategy_b == name`, so the column shows that row's
///     strategy's pair-rate against the focused strategy specifically.
///   - For strategies that have no pair with the filter: empty
///     series (renders as a blank sparkline, signalling "no signal
///     to show against the focused strategy").
///
/// Lets the operator see "how aligned is each strategy *with the
/// focused one*?" right in the strategy-pnl panel without dropping
/// into agreement-history.
fn per_strategy_agree_series_filtered(
    agreements: &[AgreementSnapshot],
    filter: Option<&str>,
) -> std::collections::HashMap<String, Vec<f64>> {
    use std::collections::HashMap;
    // (strategy, ts) → Vec<rate>. Each entry captures every
    // pair-rate this strategy participated in at this ts.
    let mut per_strat_ts: HashMap<(String, i64), Vec<f64>> = HashMap::new();
    for a in agreements {
        let include = match filter {
            None => true,
            Some(f) => {
                // The filter row keeps its mean-of-all-pairs signal;
                // every other row sees only its pair *with* the
                // filter.
                a.strategy_a == f || a.strategy_b == f
            }
        };
        if !include {
            continue;
        }
        per_strat_ts
            .entry((a.strategy_a.clone(), a.ts_ms))
            .or_default()
            .push(a.rate());
    }
    // Collapse the inner Vec to a mean, then re-key by strategy.
    let mut per_strat: HashMap<String, Vec<(i64, f64)>> = HashMap::new();
    for ((strat, ts), rates) in per_strat_ts {
        let mean = if rates.is_empty() {
            0.0
        } else {
            rates.iter().sum::<f64>() / rates.len() as f64
        };
        per_strat.entry(strat).or_default().push((ts, mean));
    }
    // Sort each series by ts ascending, then drop the timestamp —
    // the sparkline renderer doesn't need it.
    let mut out: HashMap<String, Vec<f64>> = HashMap::new();
    for (strat, mut series) in per_strat {
        series.sort_by_key(|(ts, _)| *ts);
        out.insert(strat, series.into_iter().map(|(_, r)| r).collect());
    }
    out
}

fn ts_span_label(snapshots: &[StrategyPnlSnapshot]) -> String {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for s in snapshots {
        if s.ts_ms < min {
            min = s.ts_ms;
        }
        if s.ts_ms > max {
            max = s.ts_ms;
        }
    }
    if min == i64::MAX || max == i64::MIN {
        return "today's series".to_string();
    }
    let span_ms = (max - min).max(0);
    let day_ms = 86_400_000_i64;
    let hour_ms = 3_600_000_i64;
    if span_ms < day_ms {
        "today's series".to_string()
    } else {
        let days = span_ms / day_ms;
        let hours = (span_ms % day_ms) / hour_ms;
        if hours == 0 {
            format!("{days}d series")
        } else {
            format!("{days}d {hours}h series")
        }
    }
}

/// Group today's decisions per market and classify cross-strategy
/// agreement. Latest decision per (market, strategy) wins so a
/// periodic daemon that emitted multiple rows doesn't double-vote.
fn build_consensus(decisions: &[Decision]) -> Vec<MarketConsensus> {
    use std::collections::BTreeMap;
    // market_slug → (strategy → latest decision)
    let mut by_market: std::collections::HashMap<&str, BTreeMap<String, &Decision>> =
        std::collections::HashMap::new();
    for d in decisions {
        let strat = d.effective_strategy().to_string();
        let entry = by_market.entry(d.market_slug.as_str()).or_default();
        let keep = match entry.get(&strat) {
            Some(prev) => d.ts_ms >= prev.ts_ms,
            None => true,
        };
        if keep {
            entry.insert(strat, d);
        }
    }

    let mut out: Vec<MarketConsensus> = by_market
        .into_iter()
        .map(|(slug, picks)| {
            let mut active = BTreeMap::new();
            let mut pass = 0u32;
            let mut sum = 0.0;
            let mut last_ts = 0i64;
            for (strat, d) in &picks {
                if d.ts_ms > last_ts {
                    last_ts = d.ts_ms;
                }
                match d.side.as_str() {
                    "PASS" => pass += 1,
                    _ => {
                        active.insert(strat.clone(), d.side.clone());
                        sum += d.size_usd;
                    }
                }
            }
            let agreement = classify_agreement(&active, pass);
            MarketConsensus {
                market_slug: slug.to_string(),
                active_picks: active,
                pass_strategies: pass,
                sum_size_usd: sum,
                last_ts_ms: last_ts,
                agreement,
            }
        })
        .collect();

    // Sort: splits first (they're the actionable disagreements),
    // then by total active size descending, then newest first as a
    // tiebreak.
    out.sort_by(|a, b| {
        let key = |c: &MarketConsensus| match c.agreement {
            AgreementKind::Split => 0,
            AgreementKind::AllAgree => 1,
            AgreementKind::Solo => 2,
            AgreementKind::AllPass => 3,
        };
        key(a)
            .cmp(&key(b))
            .then_with(|| b.sum_size_usd.partial_cmp(&a.sum_size_usd).unwrap_or(std::cmp::Ordering::Equal))
            .then_with(|| b.last_ts_ms.cmp(&a.last_ts_ms))
    });
    out
}

fn classify_agreement(
    active: &std::collections::BTreeMap<String, String>,
    pass_count: u32,
) -> AgreementKind {
    use std::collections::HashSet;
    match active.len() {
        0 => {
            if pass_count == 0 {
                // No data — shouldn't happen because by_market wouldn't have
                // an entry, but defend against the empty case anyway.
                AgreementKind::AllPass
            } else {
                AgreementKind::AllPass
            }
        }
        1 => AgreementKind::Solo,
        _ => {
            let distinct: HashSet<&str> = active.values().map(|s| s.as_str()).collect();
            if distinct.len() == 1 {
                AgreementKind::AllAgree
            } else {
                AgreementKind::Split
            }
        }
    }
}

fn draw(
    f: &mut ratatui::Frame,
    s: &Snapshot,
    snap_age: Duration,
    uptime: Duration,
    strategies: &[String],
    strategy_filter: Option<&str>,
    prev_ranks: &std::collections::HashMap<String, usize>,
    sort: ComparisonSort,
) {
    // Footer needs an extra line to surface the strategy hotkeys, so
    // grow it to 4 rows when at least one strategy is available.
    let footer_height = if strategies.is_empty() { 3 } else { 4 };
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            // 4-row header so both content lines (BTC + chip, then
            // snap-age + chip detail) survive the surrounding
            // borders. Length(3) — the old value — gave only one
            // content line inside the box, hiding the meta line
            // and rendering the chip's `detail` field invisible.
            Constraint::Length(4),                  // header
            // 6 = bordered table (1 top + 1 header + 3 data + 1
            // bottom). Previously included two footer chip lines
            // for "Yesterday final:" and "7d total:" — both are
            // now redundant with the dedicated columns in the
            // strategy comparison panel below, so the rendered
            // panel is just the trend table.
            Constraint::Length(6),                  // strategy pnl
            // 7 = bordered comparison table (1 top border + 1
            // header + up to 4 data rows + 1 bottom border). One
            // row per strategy: rank# / today / yesterday / 7d /
            // agreement-rate. Consolidates the per-strategy
            // numbers that were previously spread across the
            // strategy-pnl trend sparkline + the two footer chip
            // lines. 4 data rows fits typical 2-4 strategy LLM
            // configs without needing a scroll.
            Constraint::Length(7),                  // strategy comparison
            Constraint::Min(5),                     // market consensus
            Constraint::Min(5),                     // positions
            Constraint::Min(7),                     // recent decisions
            Constraint::Length(footer_height),      // footer
        ])
        .split(f.area());

    draw_header(f, chunks[0], s, snap_age, uptime);
    draw_strategy_pnl(f, chunks[1], s, strategy_filter);
    draw_strategy_comparison(f, chunks[2], s, strategy_filter, prev_ranks, sort);
    draw_consensus(f, chunks[3], s, strategy_filter);
    draw_positions(f, chunks[4], s);
    draw_decisions(f, chunks[5], s, strategy_filter);
    draw_footer(f, chunks[6], s, strategies, strategy_filter);
}

fn draw_consensus(
    f: &mut ratatui::Frame,
    area: Rect,
    s: &Snapshot,
    strategy_filter: Option<&str>,
) {
    let header = Row::new(["market_slug", "agree", "picks", "Σ size"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    // Filter-aware view: when a strategy filter is active, prioritize
    // markets that *involve* the focused strategy so they cluster at
    // the top of the panel. Within each priority tier the existing
    // (agreement kind → size → ts) ordering still applies.
    //
    // Why a per-frame sort instead of baking it into build_consensus:
    // the operator can change the filter mid-session via the digit /
    // +/- hotkeys, which only re-render — they don't re-fetch. A
    // draw-time resort means the rearrange tracks the hotkey
    // immediately without a full snapshot refresh.
    let visible: Vec<&MarketConsensus> = filter_aware_consensus(&s.consensus, strategy_filter);
    let rows: Vec<Row> = visible
        .into_iter()
        .take(usize::from(area.height.saturating_sub(3)))
        .map(|c| {
            // "baseline=YES  llm=NO  anthropic=YES (+1 pass)" rendered
            // as a Vec<Span> instead of a flat String so we can paint
            // the filtered strategy's chip with the same cyan
            // background as the strategy-pnl row + footer hotkey
            // chip. PASS counts surface even though they aren't in
            // active_picks because that's the difference between
            // "solo opinion vs. nobody else looked" and "solo
            // opinion vs. everyone else explicitly stayed out".
            let mut spans: Vec<Span> = Vec::new();
            let highlight = Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD);
            let mut first = true;
            for (strat, side) in &c.active_picks {
                if !first {
                    spans.push(Span::raw("  "));
                }
                first = false;
                let label = format!("{strat}={side}");
                if strategy_filter == Some(strat.as_str()) {
                    spans.push(Span::styled(label, highlight));
                } else {
                    spans.push(Span::raw(label));
                }
            }
            if c.pass_strategies > 0 {
                if !first {
                    spans.push(Span::raw("  "));
                }
                spans.push(Span::raw(format!("(+{} PASS)", c.pass_strategies)));
            }
            let agree_cell = Span::styled(
                c.agreement.short_label(),
                Style::default().fg(c.agreement.color()).add_modifier(Modifier::BOLD),
            );
            Row::new(vec![
                Cell::from(c.market_slug.clone()),
                Cell::from(agree_cell),
                Cell::from(Line::from(spans)),
                Cell::from(format!("${:.2}", c.sum_size_usd)),
            ])
        })
        .collect();
    let widths = [
        Constraint::Min(25),
        Constraint::Length(8),
        Constraint::Min(40),
        Constraint::Length(10),
    ];
    let title = if s.consensus.is_empty() {
        " market consensus (no decisions today yet) ".to_string()
    } else {
        // Quick at-a-glance roll-up alongside the panel title.
        let mut all_agree = 0u32;
        let mut split = 0u32;
        let mut solo = 0u32;
        let mut pass = 0u32;
        for c in &s.consensus {
            match c.agreement {
                AgreementKind::AllAgree => all_agree += 1,
                AgreementKind::Split => split += 1,
                AgreementKind::Solo => solo += 1,
                AgreementKind::AllPass => pass += 1,
            }
        }
        format!(
            " market consensus ({} markets — ✓{} all, ✗{} split, {} solo, {} pass) ",
            s.consensus.len(),
            all_agree,
            split,
            solo,
            pass,
        )
    };
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, area);
}

fn draw_header(
    f: &mut ratatui::Frame,
    area: Rect,
    s: &Snapshot,
    snap_age: Duration,
    uptime: Duration,
) {
    let btc_text = match &s.btc {
        Some(t) => {
            let age_ms = now_ms() - t.ts_ms;
            format!(
                "BTC ${:>10.2}   bid/ask ${:.2} / ${:.2}   tick age {:>5} ms",
                t.price, t.bid, t.ask, age_ms
            )
        }
        None => "BTC: (no cached tick)".to_string(),
    };
    // First line: BTC stats + (optional) colored daemon health chip.
    // The chip lives on the right side of the same line so the
    // operator's eye lands on it in the same glance as BTC spot.
    let mut first_line: Vec<Span> = vec![
        Span::styled(btc_text, Style::default().add_modifier(Modifier::BOLD)),
    ];
    if let Some(chip) = &s.health {
        first_line.push(Span::raw("   "));
        first_line.push(Span::styled(
            format!("daemon:{}", chip.status.label()),
            Style::default()
                .fg(Color::Black)
                .bg(chip.status.color())
                .add_modifier(Modifier::BOLD),
        ));
        if let Some(uptime) = chip.daemon_uptime_secs {
            first_line.push(Span::raw(format!(
                " up {}",
                fmt_dur(Duration::from_secs(uptime.max(0) as u64))
            )));
        }
    }

    // Second line: the existing local-snapshot / uptime meta, plus
    // the health-chip's detail string when present so a "degraded"
    // chip is actionable without dropping to journalctl.
    let mut meta = format!(
        "snap {:>4} ms ago   uptime {}",
        snap_age.as_millis(),
        fmt_dur(uptime)
    );
    if let Some(chip) = &s.health {
        if let Some(detail) = &chip.detail {
            meta.push_str("   ");
            meta.push_str(detail);
        }
    }
    // Truncate to the inner content width (area minus 2 border
    // chars) so a long detail-hint chain doesn't wrap onto the
    // next row and break the panel layout. Ratatui clips
    // automatically, but the clip happens silently; the explicit
    // "…" tells the operator content was dropped.
    let inner_width = area.width.saturating_sub(2) as usize;
    let meta = truncate_with_ellipsis(&meta, inner_width);
    let para = Paragraph::new(vec![Line::from(first_line), Line::from(meta)])
        .block(Block::default().borders(Borders::ALL).title(" rust-agent dashboard "));
    f.render_widget(para, area);
}

fn draw_strategy_pnl(
    f: &mut ratatui::Frame,
    area: Rect,
    s: &Snapshot,
    strategy_filter: Option<&str>,
) {
    // The bordered trend table fills the full panel area. Prior
    // versions of this function carried two footer chip lines
    // ("Yesterday final:" / "7d total:") below the table, but the
    // strategy comparison panel now surfaces both signals as
    // dedicated per-strategy columns — duplicating them in
    // chip-form just adds visual noise without adding info.
    let table_area = area;
    // Group snapshots per strategy so we can both:
    //   (a) pick the latest row for the headline columns, and
    //   (b) reconstruct the time-ordered sum_pnl series to feed the
    //       trend sparkline. Day-bucketed `list_day` already filters
    //       to today, so the series fits the dashboard's intended
    //       "what happened today" framing.
    let mut by_strategy: std::collections::HashMap<String, Vec<&StrategyPnlSnapshot>> =
        std::collections::HashMap::new();
    for snap in &s.snapshots {
        by_strategy.entry(snap.strategy.clone()).or_default().push(snap);
    }
    for v in by_strategy.values_mut() {
        v.sort_by_key(|x| x.ts_ms);
    }
    let mut keys: Vec<_> = by_strategy.keys().cloned().collect();
    keys.sort();

    // Per-strategy mean pairwise agreement series — sourced from the
    // same time window as the pnl snapshots above. Computed once
    // outside the row loop so each strategy's lookup is O(1).
    //
    // When a strategy filter is active, the series is narrowed to
    // pair-rates *involving the focused strategy*. That turns the
    // `agree` column into "how aligned is each strategy with the
    // focused one over time?" — much more directly readable than
    // the unfiltered mean once the operator has picked someone to
    // benchmark against. The filter's own row keeps its mean-of-
    // all-pairs signal so it stays interpretable.
    let agree_series = per_strategy_agree_series_filtered(&s.agreements, strategy_filter);

    // Last column label tracks the active filter: when no filter is
    // set, the agree sparkline is mean-across-all-pairs and "agree"
    // is the right label. With a filter active the series narrows
    // (see per_strategy_agree_series_filtered) so "vs <name>" tells
    // the operator what the column actually measures without
    // re-reading the docstring. Truncates if the strategy name is
    // too long to fit alongside "vs " in AGREE_SPARK_WIDTH chars.
    let agree_header: String = match strategy_filter {
        None => "agree".to_string(),
        Some(name) => {
            let prefix = "vs ";
            let max_name = AGREE_SPARK_WIDTH.saturating_sub(prefix.chars().count());
            let truncated: String = name.chars().take(max_name).collect();
            format!("{prefix}{truncated}")
        }
    };
    let header = Row::new(vec![
        Cell::from("strategy"),
        Cell::from("ts (UTC)"),
        Cell::from("decisions"),
        Cell::from("YES"),
        Cell::from("NO"),
        Cell::from("PASS"),
        Cell::from("Σ size"),
        Cell::from("Σ pnl"),
        Cell::from("pnl trend"),
        Cell::from(agree_header),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));
    let mut rows: Vec<Row> = Vec::new();
    for k in &keys {
        let series = &by_strategy[k];
        // Safe: keys only contains strategies that pushed at least one
        // snapshot into `by_strategy`, so the Vec is non-empty.
        let latest = *series.last().unwrap();
        let pnl_style = if latest.sum_pnl >= 0.0 {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::Red)
        };
        let ts = DateTime::<Utc>::from_timestamp_millis(latest.ts_ms)
            .map(|d| d.format("%H:%M:%S").to_string())
            .unwrap_or_else(|| latest.ts_ms.to_string());
        let pnls: Vec<f64> = series
            .iter()
            .rev()
            .take(SPARK_WIDTH)
            .map(|snap| snap.sum_pnl)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        // Agreement sparkline: take last AGREE_SPARK_WIDTH samples
        // (ts-ascending) from this strategy's series. Empty Vec for
        // single-strategy runs (nothing to compare against) renders
        // as a blank cell — sparkline() returns "" for empty input.
        let agrees: Vec<f64> = agree_series
            .get(k)
            .map(|v| {
                v.iter()
                    .rev()
                    .take(AGREE_SPARK_WIDTH)
                    .copied()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect()
            })
            .unwrap_or_default();
        // Color the agreement sparkline cyan to visually separate it
        // from the green/red pnl sparkline next to it.
        let agree_style = Style::default().fg(Color::Cyan);
        // When the operator has a strategy filter active, the matching
        // row gets a cyan-background `▶` gutter in the strategy column
        // (and the cell is bolded). Matches the cyan highlight on the
        // active hotkey chip in the footer so the operator can see
        // their cycle position across both panels at a glance.
        let is_active = strategy_filter == Some(latest.strategy.as_str());
        let strategy_cell = if is_active {
            // No space before the strategy name — keeps the 10-char
            // column width unchanged for strategies up to 9 chars.
            // The triangle marker plus cyan background is enough
            // visual contrast even without extra padding.
            Cell::from(Span::styled(
                format!("▶{}", latest.strategy),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ))
        } else {
            Cell::from(latest.strategy.clone())
        };
        rows.push(Row::new(vec![
            strategy_cell,
            Cell::from(ts),
            Cell::from(latest.n_decisions.to_string()),
            Cell::from(latest.n_yes.to_string()),
            Cell::from(latest.n_no.to_string()),
            Cell::from(latest.n_pass.to_string()),
            Cell::from(format!("${:.2}", latest.sum_size_usd)),
            Cell::from(Span::styled(format!("${:+.2}", latest.sum_pnl), pnl_style)),
            Cell::from(Span::styled(sparkline(&pnls), pnl_style)),
            Cell::from(Span::styled(sparkline(&agrees), agree_style)),
        ]));
    }
    // Compute the actual ts span of the snapshots we have so the
    // title reflects the real window — operators running with
    // --pnl-days 7 see "trend ~ 7 days" rather than a stale
    // "today's series" label, and an empty window still gets the
    // unambiguous "no snapshots yet" hint.
    let title: String = if rows.is_empty() {
        " strategy pnl (no snapshots yet — run `compare-pnl` or daemon) ".to_string()
    } else {
        let span_label = ts_span_label(&s.snapshots);
        format!(" strategy pnl (latest snapshot per strategy, trend ~ {span_label}) ")
    };
    let widths = [
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(5),
        Constraint::Length(5),
        Constraint::Length(5),
        Constraint::Length(12),
        Constraint::Length(12),
        Constraint::Length(SPARK_WIDTH as u16),
        Constraint::Length(AGREE_SPARK_WIDTH as u16),
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, table_area);
}

/// One-row-per-strategy table consolidating the per-strategy
/// numbers that were previously spread across the strategy-pnl
/// trend sparkline + the two footer chip lines. Columns:
///   - strategy (with ▶ marker if filter is active)
///   - today's Σ pnl (latest strategy_pnl_snapshot)
///   - yesterday final (sum from pnl_breakdown_yesterday)
///   - 7d window (sum from pnl_breakdown_window)
///   - agree rate (mean across pairs or vs filter when active)
///
/// Same per-strategy aggregation rules as the footer renderers
/// (paper + live summed within a strategy; alphabetical order for
/// stable rendering).
fn draw_strategy_comparison(
    f: &mut ratatui::Frame,
    area: Rect,
    s: &Snapshot,
    strategy_filter: Option<&str>,
    prev_ranks: &std::collections::HashMap<String, usize>,
    sort: ComparisonSort,
) {
    let rows_data = build_strategy_comparison_rows_sorted(s, strategy_filter, sort);
    let agree_header: String = match strategy_filter {
        None => "agree (mean)".to_string(),
        Some(name) => format!("vs {name}"),
    };
    let header = Row::new(vec![
        Cell::from("Δ"),
        Cell::from("#"),
        Cell::from("strategy"),
        Cell::from("today Σpnl"),
        Cell::from("today gap"),
        Cell::from("yesterday"),
        Cell::from("7d total"),
        Cell::from("7d gap"),
        Cell::from(agree_header),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD));

    // Leader's 7d total drives the `7d gap` spread column. After
    // sort, rows_data[0] is the 7d leader (largest window_pnl with
    // alphabetical tiebreak). `None` when there are no rows yet —
    // the spread column is unused in that case.
    let leader_pnl: Option<f64> = rows_data.first().map(|r| r.window_pnl);
    // Today's leader: max today_pnl across all rows. This may be
    // a different strategy than the 7d leader (rank-1 row), so a
    // mid-table row might still show "(leader)" in the today
    // column. Tied today_pnl values resolve by `partial_cmp` first
    // match wins — alphabetical iteration order from
    // build_strategy_comparison_rows means an alphabetical
    // tiebreak naturally.
    let today_leader_pnl: Option<f64> = rows_data
        .iter()
        .map(|r| r.today_pnl)
        .reduce(|a, b| if a >= b { a } else { b });
    let today_leader_strategy: Option<String> = today_leader_pnl.and_then(|lp| {
        rows_data
            .iter()
            .find(|r| (r.today_pnl - lp).abs() < f64::EPSILON)
            .map(|r| r.strategy.clone())
    });

    let mut rows: Vec<Row> = Vec::new();
    for (i, r) in rows_data.iter().enumerate() {
        // 1-based rank: rows are already sorted 7d-desc with
        // alphabetical tiebreak, so position == rank. Explicit
        // number makes "who's winning?" unambiguous even when
        // scrolling clips lower rows mid-list. Top-3 ranks get a
        // gold/silver/bronze accent so the eye lands there first;
        // 4+ stays neutral.
        let rank = i + 1;
        let rank_color = match rank {
            1 => Color::Yellow, // gold
            2 => Color::Gray,   // silver — closest ratatui has to it
            3 => Color::LightRed, // bronze-ish
            _ => Color::DarkGray,
        };
        let rank_cell = Cell::from(Span::styled(
            format!("#{rank}"),
            Style::default().fg(rank_color).add_modifier(Modifier::BOLD),
        ));
        // Rank-change indicator: compare against the rank we saw
        // for this strategy on the previous refresh. Empty
        // (single space) when there's no prior baseline — a fresh
        // strategy joining mid-session shouldn't show "↑" against
        // a phantom worst rank. The empty cell stays width-stable
        // so the column doesn't shift between renders.
        let (delta_glyph, delta_color) = match prev_ranks.get(&r.strategy).copied() {
            None => (" ", Color::DarkGray), // unseen previously
            Some(prev) if prev == rank => ("=", Color::DarkGray),
            Some(prev) if prev > rank => ("↑", Color::Green), // moved up
            Some(_) => ("↓", Color::Red),                       // moved down
        };
        let delta_cell = Cell::from(Span::styled(
            delta_glyph.to_string(),
            Style::default().fg(delta_color).add_modifier(Modifier::BOLD),
        ));
        let is_active = strategy_filter == Some(r.strategy.as_str());
        let strategy_cell = if is_active {
            Cell::from(Span::styled(
                format!("▶{}", r.strategy),
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ))
        } else {
            Cell::from(r.strategy.clone())
        };
        let cell_money = |v: f64| -> Cell<'static> {
            let color = if v >= 0.0 { Color::Green } else { Color::Red };
            Cell::from(Span::styled(
                format!("${:+.2}", v),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ))
        };
        let agree_cell = match r.agree_rate {
            Some(rate) => Cell::from(Span::styled(
                format!("{:.0}%", rate * 100.0),
                Style::default().fg(Color::Cyan),
            )),
            None => Cell::from(Span::styled(
                "—",
                Style::default().fg(Color::DarkGray),
            )),
        };
        // `7d gap`: gap to the #1's 7d total. Leader row reads
        // "(leader)" in gold; tied non-leader rows read "(tied)"
        // in cyan; behind rows read "$-N.NN" in red (the value is
        // always ≤ 0 by the desc-sort invariant). When there are
        // no rows yet (empty universe) the cell is unused — the
        // for-loop didn't fire.
        let spread_cell = match leader_pnl {
            Some(_) if i == 0 => Cell::from(Span::styled(
                "(leader)",
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
            )),
            Some(lp) => {
                let spread = r.window_pnl - lp;
                if spread.abs() < f64::EPSILON {
                    Cell::from(Span::styled(
                        "(tied)",
                        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                    ))
                } else {
                    Cell::from(Span::styled(
                        format!("${:+.2}", spread),
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    ))
                }
            }
            None => Cell::from(""),
        };
        // `today gap`: parallel spread for today_pnl. Today's
        // leader may be a different strategy than the 7d leader,
        // so this cell can read "(leader)" on a row that's
        // mid-table by 7d rank. Same render branches as
        // `7d gap` above.
        let today_gap_cell = match today_leader_pnl {
            Some(lp) => {
                let is_today_leader = today_leader_strategy
                    .as_deref()
                    .map(|s| s == r.strategy.as_str())
                    .unwrap_or(false);
                if is_today_leader {
                    Cell::from(Span::styled(
                        "(leader)",
                        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
                    ))
                } else {
                    let spread = r.today_pnl - lp;
                    if spread.abs() < f64::EPSILON {
                        Cell::from(Span::styled(
                            "(tied)",
                            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                        ))
                    } else {
                        Cell::from(Span::styled(
                            format!("${:+.2}", spread),
                            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                        ))
                    }
                }
            }
            None => Cell::from(""),
        };
        rows.push(Row::new(vec![
            delta_cell,
            rank_cell,
            strategy_cell,
            cell_money(r.today_pnl),
            today_gap_cell,
            cell_money(r.yesterday_pnl),
            cell_money(r.window_pnl),
            spread_cell,
            agree_cell,
        ]));
    }

    let title = if rows.is_empty() {
        " strategy comparison (no data yet) ".to_string()
    } else {
        format!(" strategy comparison • sorted by: {} ", sort.label())
    };
    let widths = [
        Constraint::Length(2),  // Δ rank change
        Constraint::Length(3),  // rank "#NN"
        Constraint::Length(12), // strategy
        Constraint::Length(12), // today
        Constraint::Length(12), // today gap
        Constraint::Length(12), // yesterday
        Constraint::Length(12), // 7d total
        Constraint::Length(12), // 7d gap
        Constraint::Length(15), // agree
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, area);
}

/// Per-strategy aggregated row that `draw_strategy_comparison`
/// renders. Pure function output so the table-shape contract
/// (one row per strategy, sums folded across (paper, live) and
/// across days, agreement averaged over the configured filter)
/// can be unit-tested without touching ratatui.
#[derive(Debug, Clone, PartialEq)]
struct StrategyComparisonRow {
    strategy: String,
    today_pnl: f64,
    yesterday_pnl: f64,
    window_pnl: f64,
    /// `None` when no agreement signal is available for this
    /// strategy (e.g. a single-strategy run where nothing to
    /// compare against, OR a strategy that has no rows in the
    /// agreement window).
    agree_rate: Option<f64>,
}

/// Hotkey-driven sort key for the strategy-comparison table.
/// Cycled by the `s` hotkey: default → today-desc → yesterday-desc
/// → name-asc → back to default. Default mirrors the historical
/// ranking the panel shipped with so dashboards behave unchanged
/// for operators who never press `s`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum ComparisonSort {
    Window7dDesc,
    TodayDesc,
    YesterdayDesc,
    StrategyAsc,
}

impl ComparisonSort {
    /// Step to the next sort key in the cycle. Used by the `s`
    /// hotkey in main_loop.
    fn cycle(self) -> Self {
        match self {
            ComparisonSort::Window7dDesc => ComparisonSort::TodayDesc,
            ComparisonSort::TodayDesc => ComparisonSort::YesterdayDesc,
            ComparisonSort::YesterdayDesc => ComparisonSort::StrategyAsc,
            ComparisonSort::StrategyAsc => ComparisonSort::Window7dDesc,
        }
    }

    /// Short label used in the comparison panel title so the
    /// operator can see which axis is sorted-by at a glance.
    /// Uses "desc" / "asc" rather than ↑/↓ glyphs so the title
    /// doesn't collide with the Δ column's arrow indicators.
    fn label(self) -> &'static str {
        match self {
            ComparisonSort::Window7dDesc => "7d desc",
            ComparisonSort::TodayDesc => "today desc",
            ComparisonSort::YesterdayDesc => "yesterday desc",
            ComparisonSort::StrategyAsc => "name asc",
        }
    }
}

fn build_strategy_comparison_rows(
    s: &Snapshot,
    strategy_filter: Option<&str>,
) -> Vec<StrategyComparisonRow> {
    build_strategy_comparison_rows_sorted(s, strategy_filter, ComparisonSort::Window7dDesc)
}

fn build_strategy_comparison_rows_sorted(
    s: &Snapshot,
    strategy_filter: Option<&str>,
    sort: ComparisonSort,
) -> Vec<StrategyComparisonRow> {
    use std::collections::BTreeMap;

    // Universe of strategies: union of every source's keys.
    // Some strategies have today's snapshot but no settled
    // trades yesterday (or vice versa); the row still shows up
    // with $0.00 for the missing column.
    let mut universe: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    for snap in &s.snapshots {
        universe.insert(snap.strategy.clone());
    }
    for r in &s.pnl_breakdown_yesterday {
        universe.insert(r.strategy.clone());
    }
    for r in &s.pnl_breakdown_window {
        universe.insert(r.strategy.clone());
    }

    // Today's pnl: pick the latest strategy_pnl_snapshot per
    // strategy. Matches the strategy-pnl panel's "latest row"
    // semantics.
    let mut today: BTreeMap<String, (i64, f64)> = BTreeMap::new();
    for snap in &s.snapshots {
        let e = today.entry(snap.strategy.clone()).or_insert((i64::MIN, 0.0));
        if snap.ts_ms > e.0 {
            *e = (snap.ts_ms, snap.sum_pnl);
        }
    }

    let mut yesterday: BTreeMap<String, f64> = BTreeMap::new();
    for r in &s.pnl_breakdown_yesterday {
        *yesterday.entry(r.strategy.clone()).or_insert(0.0) += r.realized_pnl;
    }

    let mut window: BTreeMap<String, f64> = BTreeMap::new();
    for r in &s.pnl_breakdown_window {
        *window.entry(r.strategy.clone()).or_insert(0.0) += r.realized_pnl;
    }

    // Agreement: mean across the configured filter (see
    // per_strategy_agree_series_filtered — uses None = all pairs;
    // Some(name) = pair-with-name).
    let series = per_strategy_agree_series_filtered(&s.agreements, strategy_filter);
    let mean_agree = |strat: &str| -> Option<f64> {
        series.get(strat).and_then(|v| {
            if v.is_empty() {
                None
            } else {
                Some(v.iter().sum::<f64>() / v.len() as f64)
            }
        })
    };

    let mut out: Vec<StrategyComparisonRow> = Vec::with_capacity(universe.len());
    for strategy in universe {
        out.push(StrategyComparisonRow {
            today_pnl: today.get(&strategy).map(|(_, p)| *p).unwrap_or(0.0),
            yesterday_pnl: yesterday.get(&strategy).copied().unwrap_or(0.0),
            window_pnl: window.get(&strategy).copied().unwrap_or(0.0),
            agree_rate: mean_agree(&strategy),
            strategy,
        });
    }
    // Rank order: dictated by the operator's sort key (default
    // Window7dDesc puts the winning strategy on top). Alphabetical
    // tiebreak keeps the rendering deterministic when two
    // strategies happen to be tied (e.g. both at $0.00 on a fresh
    // deployment). NaN pnls — shouldn't happen in practice, but be
    // defensive — sort last so they don't poison the top of the
    // list via Equal-fallback in partial_cmp.
    use std::cmp::Ordering;
    let by_strategy = |a: &StrategyComparisonRow, b: &StrategyComparisonRow| -> Ordering {
        a.strategy.cmp(&b.strategy)
    };
    let cmp_desc = |x: f64, y: f64| -> Ordering {
        y.partial_cmp(&x).unwrap_or(Ordering::Equal)
    };
    match sort {
        ComparisonSort::Window7dDesc => out.sort_by(|a, b| {
            cmp_desc(a.window_pnl, b.window_pnl).then_with(|| by_strategy(a, b))
        }),
        ComparisonSort::TodayDesc => out.sort_by(|a, b| {
            cmp_desc(a.today_pnl, b.today_pnl).then_with(|| by_strategy(a, b))
        }),
        ComparisonSort::YesterdayDesc => out.sort_by(|a, b| {
            cmp_desc(a.yesterday_pnl, b.yesterday_pnl).then_with(|| by_strategy(a, b))
        }),
        ComparisonSort::StrategyAsc => out.sort_by(by_strategy),
    }
    out
}

/// Render `values` as a unicode block sparkline. The output is exactly
/// `values.len()` characters wide so callers can size the table column
/// to match by slicing the input first. A constant series collapses
/// to all "▄" (mid-block) rather than producing a misleading rising
/// or falling ramp, and `NaN` values are folded to the current min so
/// they never spike the column.
fn sparkline(values: &[f64]) -> String {
    if values.is_empty() {
        return String::new();
    }
    const BARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for &v in values {
        if v.is_nan() {
            continue;
        }
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    if !min.is_finite() || !max.is_finite() {
        // Entirely NaN — degenerate but render *something* so the
        // column still occupies its fixed width.
        return BARS[3].to_string().repeat(values.len());
    }
    let span = max - min;
    let mut out = String::with_capacity(values.len() * 3);
    for &v in values {
        let normalized = if span <= f64::EPSILON || !v.is_finite() {
            0.5_f64
        } else {
            ((v - min) / span).clamp(0.0, 1.0)
        };
        let idx = ((normalized * (BARS.len() as f64 - 1.0)).round() as usize).min(BARS.len() - 1);
        out.push(BARS[idx]);
    }
    out
}

fn draw_positions(f: &mut ratatui::Frame, area: Rect, s: &Snapshot) {
    let header = Row::new(["market_slug", "side", "size", "avg_price", "updated"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = s
        .positions
        .iter()
        .take(usize::from(area.height.saturating_sub(3)))
        .map(|p| {
            let ts = DateTime::<Utc>::from_timestamp_millis(p.updated_at_ms)
                .map(|d| d.format("%H:%M:%S").to_string())
                .unwrap_or_default();
            let side_style = match p.side.as_str() {
                "YES" => Style::default().fg(Color::Green),
                "NO" => Style::default().fg(Color::Red),
                _ => Style::default(),
            };
            Row::new(vec![
                Cell::from(p.market_slug.clone()),
                Cell::from(Span::styled(p.side.clone(), side_style)),
                Cell::from(format!("{:.2}", p.size)),
                Cell::from(format!("${:.4}", p.avg_price)),
                Cell::from(ts),
            ])
        })
        .collect();
    let widths = [
        Constraint::Min(30),
        Constraint::Length(5),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(10),
    ];
    let title = format!(" positions ({} open) ", s.positions.len());
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, area);
}

fn draw_decisions(f: &mut ratatui::Frame, area: Rect, s: &Snapshot, strategy_filter: Option<&str>) {
    let header = Row::new(["ts (UTC)", "strategy", "market_slug", "side", "size", "conf"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    // Filter pre-take so the panel shows the latest N *for the
    // selected strategy*, not the latest N overall with most rows
    // hidden — a filter of an unused strategy would otherwise show
    // an empty table even when newer decisions exist.
    let filtered: Vec<&Decision> = s
        .decisions
        .iter()
        .filter(|d| match strategy_filter {
            Some(f) => d.effective_strategy() == f,
            None => true,
        })
        .collect();
    let total_for_strategy = filtered.len();
    let rows: Vec<Row> = filtered
        .iter()
        .take(RECENT_DECISIONS)
        .map(|d| {
            let ts = DateTime::<Utc>::from_timestamp_millis(d.ts_ms)
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_default();
            let strategy = d.effective_strategy();
            let side_style = match d.side.as_str() {
                "YES" => Style::default().fg(Color::Green),
                "NO" => Style::default().fg(Color::Red),
                _ => Style::default().fg(Color::DarkGray),
            };
            Row::new(vec![
                Cell::from(ts),
                Cell::from(strategy.to_string()),
                Cell::from(d.market_slug.clone()),
                Cell::from(Span::styled(d.side.clone(), side_style)),
                Cell::from(format!("${:.2}", d.size_usd)),
                Cell::from(format!("{:.2}", d.confidence)),
            ])
        })
        .collect();
    let widths = [
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Min(30),
        Constraint::Length(5),
        Constraint::Length(8),
        Constraint::Length(6),
    ];
    let title = match strategy_filter {
        Some(f) => format!(
            " decisions today — {} only ({} total, showing latest {}) ",
            f,
            total_for_strategy,
            total_for_strategy.min(RECENT_DECISIONS)
        ),
        None => format!(
            " decisions today ({} total, showing latest {}) ",
            s.decisions.len(),
            s.decisions.len().min(RECENT_DECISIONS)
        ),
    };
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, area);
}

fn draw_footer(
    f: &mut ratatui::Frame,
    area: Rect,
    s: &Snapshot,
    strategies: &[String],
    strategy_filter: Option<&str>,
) {
    let mut lines = vec![Line::from(
        " [q]/Esc quit   [r] refresh now   [s] cycle sort   (auto-refresh every 5 s) ",
    )];

    // Strategy hotkeys line. Each visible strategy gets its digit
    // shortcut so the operator can read "[1] baseline  [2] deepseek"
    // and press the matching key. The active filter (if any) is
    // bolded + colored so it's obvious which one is in effect.
    if !strategies.is_empty() {
        let mut spans: Vec<Span> = vec![Span::raw(" filter: ")];
        for (i, name) in strategies.iter().enumerate().take(9) {
            let key = (b'1' + i as u8) as char;
            let label = format!(" [{key}] {name} ");
            let span = if strategy_filter == Some(name.as_str()) {
                Span::styled(
                    label,
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                Span::raw(label)
            };
            spans.push(span);
        }
        let clear_label = " [0/c] all ";
        spans.push(if strategy_filter.is_none() {
            Span::styled(
                clear_label,
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::raw(clear_label)
        });
        // Compact cycle-key hint at the line end — useful when N
        // strategies > 9 and the digit hotkeys can't reach all of
        // them. No highlighting since these are stateless keys.
        spans.push(Span::raw("  [+/-] cycle"));
        lines.push(Line::from(spans));
    }

    for e in s.errors.iter().take(2) {
        lines.push(Line::from(Span::styled(
            format!(" ! {e}"),
            Style::default().fg(Color::Red),
        )));
    }
    f.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn fmt_dur(d: Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}h{m:02}m{s:02}s")
    } else if m > 0 {
        format!("{m}m{s:02}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::{build_consensus, sparkline, AgreementKind};
    use crate::coredb::types::{Decision, PnlBreakdown};
    use ratatui::style::Color;
    use uuid::Uuid;

    fn dec(slug: &str, strategy: &str, side: &str, size: f64, ts: i64) -> Decision {
        Decision {
            bucket_day_ms: 0,
            ts_ms: ts,
            decision_id: Uuid::nil(),
            market_slug: slug.into(),
            side: side.into(),
            size_usd: size,
            confidence: 0.0,
            edge_bps: 0,
            reasoning: String::new(),
            raw_response: String::new(),
            entry_price: 0.5,
            strategy: strategy.into(),
        }
    }

    #[test]
    fn sparkline_empty() {
        assert_eq!(sparkline(&[]), "");
    }

    #[test]
    fn sparkline_width_matches_input() {
        for n in 1..=10 {
            let v: Vec<f64> = (0..n).map(|i| i as f64).collect();
            assert_eq!(sparkline(&v).chars().count(), n);
        }
    }

    #[test]
    fn sparkline_constant_series_is_uniform() {
        let s = sparkline(&[5.0, 5.0, 5.0, 5.0]);
        let chars: Vec<char> = s.chars().collect();
        assert_eq!(chars.len(), 4);
        assert!(chars.iter().all(|c| *c == chars[0]));
    }

    #[test]
    fn sparkline_ramp_is_monotonic() {
        let s = sparkline(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]);
        let chars: Vec<char> = s.chars().collect();
        // First char should be the smallest block, last the largest;
        // a strictly increasing input maps to a non-decreasing string
        // of unicode block heights.
        assert_eq!(chars.first(), Some(&'▁'));
        assert_eq!(chars.last(), Some(&'█'));
        for w in chars.windows(2) {
            assert!(w[0] <= w[1], "expected non-decreasing, got {chars:?}");
        }
    }

    #[test]
    fn sparkline_descending_is_reverse_monotonic() {
        let s = sparkline(&[7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 0.0]);
        let chars: Vec<char> = s.chars().collect();
        for w in chars.windows(2) {
            assert!(w[0] >= w[1], "expected non-increasing, got {chars:?}");
        }
    }

    #[test]
    fn sparkline_handles_negatives() {
        // Mix of negative and positive PnL values — the column should
        // still anchor min→'▁' and max→'█'.
        let s = sparkline(&[-10.0, -5.0, 0.0, 5.0, 10.0]);
        let chars: Vec<char> = s.chars().collect();
        assert_eq!(chars.first(), Some(&'▁'));
        assert_eq!(chars.last(), Some(&'█'));
    }

    #[test]
    fn sparkline_nan_does_not_panic() {
        let s = sparkline(&[1.0, f64::NAN, 2.0, 3.0]);
        assert_eq!(s.chars().count(), 4);
    }

    #[test]
    fn sparkline_all_nan_renders_filler() {
        let s = sparkline(&[f64::NAN, f64::NAN, f64::NAN]);
        assert_eq!(s.chars().count(), 3);
    }

    #[test]
    fn consensus_all_agree_two_strategies() {
        let rows = vec![
            dec("btc-100k", "baseline", "YES", 10.0, 1),
            dec("btc-100k", "deepseek", "YES", 5.0, 2),
        ];
        let c = build_consensus(&rows);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].agreement, AgreementKind::AllAgree);
        assert_eq!(c[0].active_picks.len(), 2);
        assert!((c[0].sum_size_usd - 15.0).abs() < 1e-9);
    }

    #[test]
    fn consensus_split_when_sides_differ() {
        let rows = vec![
            dec("btc-100k", "baseline", "YES", 10.0, 1),
            dec("btc-100k", "deepseek", "NO", 5.0, 2),
        ];
        let c = build_consensus(&rows);
        assert_eq!(c[0].agreement, AgreementKind::Split);
    }

    #[test]
    fn consensus_solo_when_one_active_one_pass() {
        let rows = vec![
            dec("btc-100k", "baseline", "YES", 10.0, 1),
            dec("btc-100k", "deepseek", "PASS", 0.0, 2),
        ];
        let c = build_consensus(&rows);
        assert_eq!(c[0].agreement, AgreementKind::Solo);
        assert_eq!(c[0].active_picks.len(), 1);
        assert_eq!(c[0].pass_strategies, 1);
    }

    #[test]
    fn consensus_all_pass_when_no_strategy_takes_a_side() {
        let rows = vec![
            dec("btc-100k", "baseline", "PASS", 0.0, 1),
            dec("btc-100k", "deepseek", "PASS", 0.0, 2),
        ];
        let c = build_consensus(&rows);
        assert_eq!(c[0].agreement, AgreementKind::AllPass);
        assert!(c[0].active_picks.is_empty());
        assert_eq!(c[0].pass_strategies, 2);
    }

    #[test]
    fn consensus_latest_per_strategy_wins() {
        // baseline emits YES then NO; latest (NO) should win and the
        // market should be flagged as a split vs deepseek=YES.
        let rows = vec![
            dec("btc-100k", "baseline", "YES", 10.0, 1),
            dec("btc-100k", "baseline", "NO", 12.0, 5),
            dec("btc-100k", "deepseek", "YES", 5.0, 3),
        ];
        let c = build_consensus(&rows);
        assert_eq!(c[0].agreement, AgreementKind::Split);
        assert_eq!(c[0].active_picks.get("baseline").map(String::as_str), Some("NO"));
        // size should reflect the latest baseline row, not the first.
        assert!((c[0].sum_size_usd - 17.0).abs() < 1e-9);
    }

    #[test]
    fn health_worst_subtask_picks_max_consecutive_errors() {
        let body = super::HealthWire {
            status: "degraded".into(),
            uptime_secs: 100,
            backtest: super::HealthSubtaskWire { consecutive_errors: 1, last_error: Some("a".into()) },
            compare: super::HealthSubtaskWire { consecutive_errors: 5, last_error: Some("worst".into()) },
            settle: super::HealthSubtaskWire { consecutive_errors: 3, last_error: Some("c".into()) },
            ingest_btc_age_ms: Some(100),
            ingest_polymarket_age_ms: Some(200),
            cache_ages_ms: super::HealthCacheAgesWire::default(),
            ingest_restarts: super::IngestRestartsWire::default(),
        };
        let msg = super::worst_subtask_error(&body).unwrap();
        assert!(msg.starts_with("compare: 5×"), "got: {msg}");
        assert!(msg.contains("worst"), "got: {msg}");
    }

    #[test]
    fn health_worst_returns_none_when_all_healthy() {
        let body = super::HealthWire {
            status: "ok".into(),
            uptime_secs: 0,
            backtest: super::HealthSubtaskWire::default(),
            compare: super::HealthSubtaskWire::default(),
            settle: super::HealthSubtaskWire::default(),
            ingest_btc_age_ms: Some(100),
            ingest_polymarket_age_ms: Some(200),
            cache_ages_ms: super::HealthCacheAgesWire::default(),
            ingest_restarts: super::IngestRestartsWire::default(),
        };
        assert!(super::worst_subtask_error(&body).is_none());
    }

    #[test]
    fn restart_delta_hint_none_on_first_poll() {
        // prev=None means we have no baseline yet — first poll
        // after dashboard startup just stashes the baseline.
        assert!(super::restart_delta_hint(None, (0, 0)).is_none());
        assert!(super::restart_delta_hint(None, (7, 3)).is_none());
    }

    #[test]
    fn restart_delta_hint_none_when_no_delta() {
        // Same counts on consecutive probes → no hint.
        assert!(super::restart_delta_hint(Some((5, 2)), (5, 2)).is_none());
    }

    #[test]
    fn restart_delta_hint_reports_binance_only_delta() {
        let h = super::restart_delta_hint(Some((1, 7)), (3, 7)).unwrap();
        assert!(
            h.contains("binance restarted 2×"),
            "expected binance-only delta, got: {h}",
        );
        assert!(
            !h.contains("polymarket"),
            "polymarket shouldn't show when delta is 0, got: {h}",
        );
    }

    #[test]
    fn restart_delta_hint_reports_both_sources_with_separator() {
        let h = super::restart_delta_hint(Some((0, 0)), (2, 3)).unwrap();
        assert!(
            h.contains("binance restarted 2×"),
            "binance chip missing, got: {h}"
        );
        assert!(
            h.contains("polymarket restarted 3×"),
            "polymarket chip missing, got: {h}"
        );
        assert!(h.contains(" · "), "expected separator, got: {h}");
    }

    #[test]
    fn chip_local_downgrade_promotes_ok_to_degraded_when_signal_fires() {
        assert_eq!(
            super::chip_status_with_local_downgrade(super::HealthStatus::Ok, true),
            super::HealthStatus::Degraded,
        );
    }

    #[test]
    fn chip_local_downgrade_is_noop_when_signal_quiet() {
        assert_eq!(
            super::chip_status_with_local_downgrade(super::HealthStatus::Ok, false),
            super::HealthStatus::Ok,
        );
    }

    #[test]
    fn chip_local_downgrade_does_not_recolor_already_degraded() {
        // Don't promote Degraded → some-other state. Daemon
        // already flagged the problem; clobbering its status
        // would lose information.
        assert_eq!(
            super::chip_status_with_local_downgrade(super::HealthStatus::Degraded, true),
            super::HealthStatus::Degraded,
        );
        assert_eq!(
            super::chip_status_with_local_downgrade(super::HealthStatus::Unreachable, true),
            super::HealthStatus::Unreachable,
        );
    }

    #[test]
    fn restart_delta_hint_safe_when_counters_reset_below_prev() {
        // Process restart drops the counters to 0. saturating_sub
        // makes us emit no hint rather than panicking on negative
        // delta or reporting a giant pretend-delta.
        assert!(super::restart_delta_hint(Some((10, 5)), (0, 0)).is_none());
    }

    #[test]
    fn stalest_cache_hint_returns_none_when_all_fresh() {
        let ages = super::HealthCacheAgesWire {
            decisions: Some(1_000),
            orders: Some(500),
            pnl_daily: Some(60_000),
            pnl_breakdown: Some(30_000),
            pnl_breakdown_yesterday: Some(120_000),
            pnl_breakdown_window: Some(45_000),
            positions: Some(2_000),
            ingest_probe: Some(100),
        };
        assert!(super::stalest_cache_hint(&ages).is_none());
    }

    #[test]
    fn stalest_cache_hint_picks_max_age_over_threshold() {
        // Threshold is 10 min = 600_000 ms. Two slots over (pnl_breakdown_window
        // at 12 min, pnl_daily at 11 min); pnl_breakdown_window wins.
        let ages = super::HealthCacheAgesWire {
            decisions: Some(1_000),
            orders: None,
            pnl_daily: Some(11 * 60 * 1000),
            pnl_breakdown: None,
            pnl_breakdown_yesterday: Some(60_000),
            pnl_breakdown_window: Some(12 * 60 * 1000),
            positions: None,
            ingest_probe: Some(100),
        };
        let hint = super::stalest_cache_hint(&ages).unwrap();
        assert!(
            hint.starts_with("stale: pnl_breakdown_window"),
            "expected window to be the stalest, got: {hint}",
        );
        assert!(hint.contains("720s"), "expected age in seconds, got: {hint}");
    }

    #[test]
    fn stalest_cache_hint_ignores_none_slots() {
        // All None → "never refreshed" → no hint (operator already
        // sees this via empty-row dashboard state).
        let ages = super::HealthCacheAgesWire::default();
        assert!(super::stalest_cache_hint(&ages).is_none());
    }

    /// Persistence round-trip: write state to a temp path, read
    /// it back, confirm ranks + sort are preserved.
    #[test]
    fn dashboard_state_round_trips_to_disk() {
        let mut tmp = std::env::temp_dir();
        tmp.push(format!(
            "rust-agent-dashboard-state-test-{}.json",
            std::process::id(),
        ));
        let _ = std::fs::remove_file(&tmp);

        let mut ranks = std::collections::HashMap::new();
        ranks.insert("baseline".to_string(), 1);
        ranks.insert("deepseek".to_string(), 2);
        let saved = super::PersistedDashboardState {
            ranks: ranks.clone(),
            sort: super::ComparisonSort::TodayDesc,
            saved_at_ms: 1_700_000_000_000,
            strategy_filter: Some("baseline".to_string()),
        };
        super::save_persisted_dashboard_state(&tmp, &saved);

        let loaded = super::load_persisted_dashboard_state(
            &tmp,
            1_700_000_000_000, // same "now" → diff = 0 → within TTL
            super::DASHBOARD_STATE_TTL_MS_DEFAULT,
        )
        .expect("expected to round-trip");
        assert_eq!(loaded.ranks, ranks);
        assert_eq!(loaded.sort, super::ComparisonSort::TodayDesc);
        assert_eq!(loaded.saved_at_ms, 1_700_000_000_000);
        assert_eq!(
            loaded.strategy_filter.as_deref(),
            Some("baseline"),
            "expected strategy_filter to round-trip",
        );

        let _ = std::fs::remove_file(&tmp);
    }

    /// Backwards-compat: a state file written before the
    /// `strategy_filter` field existed (or with the field as
    /// `null`) still parses cleanly. `#[serde(default)]` keeps
    /// existing rollouts forward-compatible.
    #[test]
    fn dashboard_state_load_tolerates_missing_strategy_filter_field() {
        let mut tmp = std::env::temp_dir();
        tmp.push(format!(
            "rust-agent-state-compat-{}.json",
            std::process::id(),
        ));
        // Hand-written JSON without the `strategy_filter` field
        // mirrors a pre-2026-05 state file on disk.
        let pre_filter_json = serde_json::json!({
            "ranks": { "baseline": 1 },
            "sort": "Window7dDesc",
            "saved_at_ms": 1_700_000_000_000_i64,
        });
        std::fs::write(&tmp, pre_filter_json.to_string()).unwrap();
        let loaded = super::load_persisted_dashboard_state(
            &tmp,
            1_700_000_000_000,
            super::DASHBOARD_STATE_TTL_MS_DEFAULT,
        )
        .expect("expected pre-filter file to parse");
        assert_eq!(loaded.strategy_filter, None, "missing field → None");
        let _ = std::fs::remove_file(&tmp);
    }

    /// State older than the TTL is discarded — operator gets a
    /// fresh start rather than misleading Δ indicators against
    /// ranks from days ago.
    #[test]
    fn dashboard_state_discarded_when_older_than_ttl() {
        let mut tmp = std::env::temp_dir();
        tmp.push(format!(
            "rust-agent-dashboard-state-ttl-test-{}.json",
            std::process::id(),
        ));
        let _ = std::fs::remove_file(&tmp);

        let saved = super::PersistedDashboardState {
            ranks: Default::default(),
            sort: super::ComparisonSort::Window7dDesc,
            saved_at_ms: 1_700_000_000_000,
            strategy_filter: None,
        };
        super::save_persisted_dashboard_state(&tmp, &saved);

        // 3 hours later — past the 2h default TTL → None.
        let now = 1_700_000_000_000 + 3 * 60 * 60 * 1000;
        let loaded = super::load_persisted_dashboard_state(
            &tmp,
            now,
            super::DASHBOARD_STATE_TTL_MS_DEFAULT,
        );
        assert!(loaded.is_none(), "expected stale state to be discarded");

        let _ = std::fs::remove_file(&tmp);
    }

    /// Missing file → None (not an error). A fresh dashboard
    /// startup is the common case for this branch.
    #[test]
    fn dashboard_state_load_missing_file_returns_none() {
        let nonexistent = std::env::temp_dir().join(format!(
            "rust-agent-definitely-does-not-exist-{}.json",
            std::process::id(),
        ));
        let _ = std::fs::remove_file(&nonexistent);
        let loaded = super::load_persisted_dashboard_state(
            &nonexistent,
            1_700_000_000_000,
            super::DASHBOARD_STATE_TTL_MS_DEFAULT,
        );
        assert!(loaded.is_none());
    }

    /// Corrupt file → None. A garbage state file shouldn't crash
    /// the dashboard — operator just gets a fresh start.
    #[test]
    fn dashboard_state_load_corrupt_file_returns_none() {
        let mut tmp = std::env::temp_dir();
        tmp.push(format!(
            "rust-agent-corrupt-state-{}.json",
            std::process::id(),
        ));
        std::fs::write(&tmp, b"not json at all").unwrap();
        let loaded = super::load_persisted_dashboard_state(
            &tmp,
            1_700_000_000_000,
            super::DASHBOARD_STATE_TTL_MS_DEFAULT,
        );
        assert!(loaded.is_none());
        let _ = std::fs::remove_file(&tmp);
    }

    /// Env-parsing for the TTL knob.
    #[test]
    fn dashboard_state_ttl_env_parsing() {
        std::env::remove_var("DASHBOARD_STATE_TTL_S");
        assert_eq!(
            super::dashboard_state_ttl_ms(),
            super::DASHBOARD_STATE_TTL_MS_DEFAULT,
        );
        std::env::set_var("DASHBOARD_STATE_TTL_S", "60");
        assert_eq!(super::dashboard_state_ttl_ms(), 60_000);
        std::env::set_var("DASHBOARD_STATE_TTL_S", "bogus");
        assert_eq!(
            super::dashboard_state_ttl_ms(),
            super::DASHBOARD_STATE_TTL_MS_DEFAULT,
        );
        std::env::remove_var("DASHBOARD_STATE_TTL_S");
    }

    /// Path resolver respects DASHBOARD_STATE_PATH override.
    #[test]
    fn dashboard_state_path_respects_env_override() {
        std::env::set_var("DASHBOARD_STATE_PATH", "/tmp/my-custom-state.json");
        let path = super::dashboard_state_path();
        assert_eq!(path.to_str(), Some("/tmp/my-custom-state.json"));
        std::env::remove_var("DASHBOARD_STATE_PATH");
        // Default falls back to $HOME/.cache/rust-agent/...
        let default_path = super::dashboard_state_path();
        let s = default_path.to_string_lossy();
        assert!(
            s.contains(".cache/rust-agent/dashboard_state.json"),
            "default path looks wrong: {s}",
        );
    }

    #[test]
    fn short_err_truncates_long_strings() {
        let long = "x".repeat(200);
        let s = super::short_err(&long);
        // 80-char cap + ellipsis.
        assert!(s.chars().count() <= 81, "got {} chars", s.chars().count());
        assert!(s.ends_with('…'));
    }

    #[test]
    fn short_err_keeps_short_strings_intact() {
        assert_eq!(super::short_err("hello"), "hello");
    }

    #[test]
    fn truncate_with_ellipsis_passthrough_when_under_budget() {
        assert_eq!(super::truncate_with_ellipsis("hello", 10), "hello");
    }

    #[test]
    fn truncate_with_ellipsis_passthrough_at_exact_budget() {
        // No clipping when budget == length.
        assert_eq!(super::truncate_with_ellipsis("hello", 5), "hello");
    }

    #[test]
    fn truncate_with_ellipsis_clips_with_indicator() {
        // 5 budget, 10-char input: keep 4 chars + "…".
        let s = "abcdefghij";
        let out = super::truncate_with_ellipsis(s, 5);
        assert_eq!(out, "abcd…");
        assert_eq!(out.chars().count(), 5);
    }

    #[test]
    fn truncate_with_ellipsis_zero_budget_returns_empty() {
        // Operator deliberately allocated no space — render
        // nothing, not even "…".
        assert_eq!(super::truncate_with_ellipsis("anything", 0), "");
    }

    #[test]
    fn truncate_with_ellipsis_one_budget_returns_ellipsis() {
        // Edge: budget = 1 → just emit "…" rather than the first
        // char. The ellipsis is the more informative single char
        // because it announces "something was here".
        assert_eq!(super::truncate_with_ellipsis("anything", 1), "…");
    }

    #[test]
    fn truncate_with_ellipsis_handles_unicode_correctly() {
        // Counts CHARS not bytes; "▶" is one char (3 bytes UTF-8).
        // Budget 3 on a 5-char input: 2 chars + "…".
        let s = "▶abcd";
        let out = super::truncate_with_ellipsis(s, 3);
        assert_eq!(out, "▶a…");
        assert_eq!(out.chars().count(), 3);
    }

    fn snap(ts_ms: i64) -> crate::coredb::types::StrategyPnlSnapshot {
        crate::coredb::types::StrategyPnlSnapshot {
            bucket_day_ms: 0,
            ts_ms,
            strategy: "x".into(),
            n_decisions: 0,
            sum_size_usd: 0.0,
            sum_pnl: 0.0,
            n_yes: 0,
            n_no: 0,
            n_pass: 0,
        }
    }

    #[test]
    fn span_label_empty_input() {
        assert_eq!(super::ts_span_label(&[]), "today's series");
    }

    #[test]
    fn span_label_subday_collapses_to_today() {
        // 1 hour span — still treated as today's series.
        let rows = vec![snap(0), snap(3_600_000)];
        assert_eq!(super::ts_span_label(&rows), "today's series");
    }

    #[test]
    fn span_label_whole_days_only() {
        let day_ms = 86_400_000_i64;
        let rows = vec![snap(0), snap(3 * day_ms)];
        assert_eq!(super::ts_span_label(&rows), "3d series");
    }

    #[test]
    fn strategies_in_view_merges_decisions_and_snapshots() {
        let mut s = super::Snapshot::default();
        s.snapshots.push(crate::coredb::types::StrategyPnlSnapshot {
            bucket_day_ms: 0,
            ts_ms: 1,
            strategy: "baseline".into(),
            n_decisions: 0,
            sum_size_usd: 0.0,
            sum_pnl: 0.0,
            n_yes: 0,
            n_no: 0,
            n_pass: 0,
        });
        s.decisions.push(dec("m", "deepseek", "YES", 1.0, 1));
        s.decisions.push(dec("m", "baseline", "NO", 1.0, 2)); // duplicate of snapshot's
        let v = super::strategies_in_view(&s);
        // BTreeSet → sorted, deduped.
        assert_eq!(v, vec!["baseline", "deepseek"]);
    }

    #[test]
    fn strategies_in_view_empty_when_no_data() {
        let s = super::Snapshot::default();
        assert!(super::strategies_in_view(&s).is_empty());
    }

    fn ag(a: &str, b: &str, ts: i64, shared: i32, matches: i32) -> crate::coredb::types::AgreementSnapshot {
        crate::coredb::types::AgreementSnapshot {
            bucket_day_ms: 0,
            ts_ms: ts,
            strategy_a: a.into(),
            strategy_b: b.into(),
            shared,
            matches,
        }
    }

    #[test]
    fn per_strategy_agree_series_empty_input() {
        let out = super::per_strategy_agree_series(&[]);
        assert!(out.is_empty());
    }

    #[test]
    fn per_strategy_agree_series_means_pair_rates_at_each_ts() {
        // 3-strategy snapshot at ts=10: baseline vs deepseek 50%,
        // baseline vs llm 100%, deepseek vs llm 0%.
        // baseline's mean = (0.5 + 1.0) / 2 = 0.75
        // deepseek's mean = (0.5 + 0.0) / 2 = 0.25
        // llm's mean      = (1.0 + 0.0) / 2 = 0.5
        let rows = vec![
            ag("baseline", "deepseek", 10, 10, 5),
            ag("deepseek", "baseline", 10, 10, 5),
            ag("baseline", "llm", 10, 10, 10),
            ag("llm", "baseline", 10, 10, 10),
            ag("deepseek", "llm", 10, 10, 0),
            ag("llm", "deepseek", 10, 10, 0),
        ];
        let out = super::per_strategy_agree_series(&rows);
        assert!((out["baseline"][0] - 0.75).abs() < 1e-9);
        assert!((out["deepseek"][0] - 0.25).abs() < 1e-9);
        assert!((out["llm"][0] - 0.5).abs() < 1e-9);
    }

    #[test]
    fn per_strategy_agree_series_filtered_narrows_other_rows_to_filter_pair() {
        // 3-strategy snapshot at ts=10. Pair rates:
        //   a↔b = 50%, a↔c = 100%, b↔c = 0%.
        // Filter = "a":
        //   - row "a": mean across (a↔b, a↔c) = (0.5 + 1.0) / 2 = 0.75
        //   - row "b": only (b↔a) = 0.5  (NOT averaged with b↔c)
        //   - row "c": only (c↔a) = 1.0  (NOT averaged with c↔b)
        let rows = vec![
            ag("a", "b", 10, 10, 5),
            ag("b", "a", 10, 10, 5),
            ag("a", "c", 10, 10, 10),
            ag("c", "a", 10, 10, 10),
            ag("b", "c", 10, 10, 0),
            ag("c", "b", 10, 10, 0),
        ];
        let out = super::per_strategy_agree_series_filtered(&rows, Some("a"));
        // a's row: 0.75 (mean of all pairs it's in).
        assert!((out["a"][0] - 0.75).abs() < 1e-9, "a got {:?}", out["a"]);
        // b's row: 0.5 (only b↔a, the b↔c=0.0 is excluded).
        assert!((out["b"][0] - 0.5).abs() < 1e-9, "b got {:?}", out["b"]);
        // c's row: 1.0 (only c↔a, the c↔b=0.0 is excluded).
        assert!((out["c"][0] - 1.0).abs() < 1e-9, "c got {:?}", out["c"]);
    }

    #[test]
    fn per_strategy_agree_series_filtered_no_filter_matches_unfiltered() {
        let rows = vec![
            ag("a", "b", 10, 10, 5),
            ag("b", "a", 10, 10, 5),
            ag("a", "c", 10, 10, 10),
        ];
        let plain = super::per_strategy_agree_series(&rows);
        let filtered_none = super::per_strategy_agree_series_filtered(&rows, None);
        assert_eq!(plain, filtered_none);
    }

    #[test]
    fn per_strategy_agree_series_filtered_unrelated_strategy_drops_out() {
        // d never paired with the filter "a" → no entry in output.
        let rows = vec![
            ag("a", "b", 10, 10, 5),
            ag("b", "a", 10, 10, 5),
            ag("d", "c", 10, 10, 5),
            ag("c", "d", 10, 10, 5),
        ];
        let out = super::per_strategy_agree_series_filtered(&rows, Some("a"));
        assert!(out.contains_key("a"));
        assert!(out.contains_key("b"));
        assert!(!out.contains_key("d"), "d has no pair with a → no series");
        assert!(!out.contains_key("c"), "c has no pair with a → no series");
    }

    #[test]
    fn per_strategy_agree_series_ts_sorted_ascending() {
        // Insert ts out of order; the output series must be ascending.
        let rows = vec![
            ag("baseline", "deepseek", 30, 10, 6),
            ag("baseline", "deepseek", 10, 10, 2),
            ag("baseline", "deepseek", 20, 10, 4),
        ];
        let out = super::per_strategy_agree_series(&rows);
        let series = &out["baseline"];
        assert_eq!(series.len(), 3);
        // Rates: 0.2 → 0.4 → 0.6 strictly increasing iff ts-sorted.
        for w in series.windows(2) {
            assert!(w[0] < w[1], "expected ascending, got {series:?}");
        }
    }

    #[test]
    fn strategies_in_view_inferred_label_for_legacy_decisions() {
        // Legacy row: strategy column empty, raw_response says baseline-rule.
        let mut s = super::Snapshot::default();
        let mut d = dec("m", "", "PASS", 0.0, 1);
        d.raw_response = "baseline-rule".to_string();
        s.decisions.push(d);
        let v = super::strategies_in_view(&s);
        assert_eq!(v, vec!["baseline"]);
    }

    fn cycle(strategies: &[&str], current: Option<&str>, step: i32) -> Option<String> {
        let s: Vec<String> = strategies.iter().map(|x| (*x).to_string()).collect();
        super::cycle_strategy(&s, current, step)
    }

    // ---- Snapshot tests ---------------------------------------------------
    //
    // Each test calls `render_test_dashboard(filter)` to get a string dump
    // of the full dashboard rendered with a deterministic synthetic
    // Snapshot, then asserts the narrow slice of invariants relevant to
    // one panel. Splitting by panel means a regression's failing test
    // name points at the broken panel — `panel_consensus_renders_picks`
    // failing is unambiguously a consensus issue, not a footer-line drift
    // that got rolled into one all-in-one assertion.

    /// Render the full dashboard with a fixed synthetic Snapshot and
    /// return the visible character buffer as a newline-separated
    /// string. Sized 140×35 so every column fits without truncation.
    /// PnL values are picked so one strategy is positive and one
    /// negative — exercises both coloring branches without making
    /// the test brittle to amplitude.
    ///
    /// `health` lets a caller inject a [`HealthChip`] into the
    /// rendered Snapshot to exercise the header's chip + detail-line
    /// rendering. `None` means "don't include a /health chip".
    fn render_test_dashboard_with(
        filter: Option<&str>,
        health: Option<super::HealthChip>,
    ) -> String {
        render_test_dashboard_full(filter, health, Vec::new(), Vec::new())
    }

    /// Full test renderer that also accepts a `pnl_breakdown_yesterday`
    /// and `pnl_breakdown_window` fixture so the new footer lines can
    /// be exercised end-to-end via the standard `draw()` entry point.
    /// The thinner shims keep the existing test call sites stable.
    fn render_test_dashboard_full(
        filter: Option<&str>,
        health: Option<super::HealthChip>,
        pnl_breakdown_yesterday: Vec<PnlBreakdown>,
        pnl_breakdown_window: Vec<PnlBreakdown>,
    ) -> String {
        use crate::coredb::types::{BtcTick, StrategyPnlSnapshot};
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use std::time::Duration;

        let mut s = super::Snapshot::default();
        s.btc = Some(BtcTick {
            bucket_hour_ms: 0,
            symbol: "BTCUSDT".into(),
            ts_ms: 1700000000000,
            price: 79123.45,
            volume: 0.0,
            bid: 79123.40,
            ask: 79123.50,
        });
        s.snapshots = vec![
            StrategyPnlSnapshot {
                bucket_day_ms: 0,
                ts_ms: 1700000000000,
                strategy: "baseline".into(),
                n_decisions: 100,
                sum_size_usd: 500.0,
                sum_pnl: 12.34,
                n_yes: 30,
                n_no: 30,
                n_pass: 40,
            },
            StrategyPnlSnapshot {
                bucket_day_ms: 0,
                ts_ms: 1700000000000,
                strategy: "deepseek".into(),
                n_decisions: 50,
                sum_size_usd: 250.0,
                sum_pnl: -5.67,
                n_yes: 10,
                n_no: 30,
                n_pass: 10,
            },
        ];
        s.health = health;
        s.pnl_breakdown_yesterday = pnl_breakdown_yesterday;
        s.pnl_breakdown_window = pnl_breakdown_window;

        let strategies = super::strategies_in_view(&s);
        // 50 rows tall: 4 (header) + 8 (strategy pnl) + 7
        // (comparison) + 5 + 5 + 7 (3 Min panels) + 4 (footer) = 40
        // wanted; the extra 10 rows give the Min chunks room to
        // expand rather than getting squeezed (and shrinking the
        // adjacent Length chunks with them, which silently clipped
        // the comparison panel's bottom rows back when this was 35).
        let backend = TestBackend::new(140, 50);
        let mut terminal = Terminal::new(backend).unwrap();
        // Default to "no previous ranks" — first frame shows blank
        // Δ column. Tests that exercise rank momentum use
        // `render_test_dashboard_with_prev_ranks` below.
        let prev_ranks: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        terminal
            .draw(|f| {
                super::draw(
                    f,
                    &s,
                    Duration::from_millis(0),
                    Duration::from_secs(42),
                    &strategies,
                    filter,
                    &prev_ranks,
                    super::ComparisonSort::Window7dDesc,
                )
            })
            .unwrap();
        render_buffer(terminal.backend().buffer())
    }

    /// Backwards-compat shim — `health` defaults to `None`.
    fn render_test_dashboard(filter: Option<&str>) -> String {
        render_test_dashboard_with(filter, None)
    }

    /// Render the full dashboard onto a custom-width TestBackend so
    /// truncation/clipping behavior on narrow terminals can be
    /// pinned. Same fixture as `render_test_dashboard_with` for
    /// every other field; only `health.detail` is parameterized to
    /// the test's needs.
    fn render_test_dashboard_at_width(
        width: u16,
        height: u16,
        chip: super::HealthChip,
    ) -> String {
        use crate::coredb::types::BtcTick;
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use std::time::Duration;
        let mut s = super::Snapshot::default();
        s.btc = Some(BtcTick {
            bucket_hour_ms: 0,
            symbol: "BTCUSDT".into(),
            ts_ms: 1700000000000,
            price: 79123.45,
            volume: 0.0,
            bid: 79123.40,
            ask: 79123.50,
        });
        s.health = Some(chip);
        let strategies = super::strategies_in_view(&s);
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        let prev_ranks: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        terminal
            .draw(|f| {
                super::draw(
                    f,
                    &s,
                    Duration::from_millis(0),
                    Duration::from_secs(42),
                    &strategies,
                    None,
                    &prev_ranks,
                    super::ComparisonSort::Window7dDesc,
                )
            })
            .unwrap();
        render_buffer(terminal.backend().buffer())
    }

    /// On a 60-col terminal a maxed-out chip detail (worst-subtask
    /// + stale-cache + restart-delta + ingest-age summary) must
    /// truncate with "…" rather than wrap onto the next line. The
    /// "…" character is the explicit signal that content was
    /// clipped; without truncation ratatui silently clips and the
    /// operator can't tell.
    #[test]
    fn header_chip_detail_truncates_on_narrow_terminal() {
        let chip = super::HealthChip {
            status: super::HealthStatus::Degraded,
            daemon_uptime_secs: Some(3_600),
            detail: Some(
                "backtest: 5× — connection refused to 127.0.0.1:9042 · \
                 stale: pnl_breakdown_window 720s · binance restarted 3× · \
                 polymarket restarted 2× · btc 412ms / poly 1234ms"
                    .into(),
            ),
            ingest_restarts: Some((0, 0)),
        };
        // Height 35 matches the production dashboard's typical
        // terminal — gives all layout chunks room without testing
        // any "shrinkage" edge cases that aren't this commit's
        // concern.
        let dump = render_test_dashboard_at_width(60, 35, chip);
        // Header is the top 4 rows of the dashboard; restrict to
        // those to avoid false positives from other panels.
        let header: String = dump.lines().take(4).collect::<Vec<_>>().join("\n");
        assert!(
            header.contains('…'),
            "expected ellipsis in clipped header detail; got:\n{header}",
        );
    }

    /// Wide terminal (140 cols) keeps the same detail intact —
    /// confirms truncation isn't over-eager.
    #[test]
    fn header_chip_detail_intact_on_wide_terminal() {
        let chip = super::HealthChip {
            status: super::HealthStatus::Degraded,
            daemon_uptime_secs: Some(3_600),
            detail: Some("backtest: 2× — connection refused".into()),
            ingest_restarts: Some((0, 0)),
        };
        let dump = render_test_dashboard_at_width(140, 35, chip);
        let header: String = dump.lines().take(4).collect::<Vec<_>>().join("\n");
        assert!(
            header.contains("backtest: 2× — connection refused"),
            "expected full detail when budget is generous; got:\n{header}",
        );
        assert!(
            !header.contains('…'),
            "should NOT clip when content fits; got:\n{header}",
        );
    }

    #[test]
    fn panel_header_renders_dashboard_title() {
        let dump = render_test_dashboard(None);
        assert!(dump.contains("rust-agent dashboard"), "header title missing");
        // BTC price formatting — pinned at .2 precision per draw_header.
        assert!(
            dump.contains("$  79123.45"),
            "BTC price missing or mis-formatted; got dump:\n{dump}"
        );
    }

    #[test]
    fn panel_strategy_pnl_has_all_column_headers() {
        let dump = render_test_dashboard(None);
        assert!(dump.contains("strategy pnl"), "strategy-pnl title missing");
        for label in [
            "strategy", "ts (UTC)", "decisions", "YES", "NO", "PASS",
            "Σ size", "Σ pnl", "pnl trend", "agree",
        ] {
            assert!(
                dump.contains(label),
                "strategy-pnl column header `{label}` missing"
            );
        }
    }

    #[test]
    fn panel_strategy_pnl_filter_changes_header_and_marker() {
        let dump = render_test_dashboard(Some("deepseek"));
        // Agree column header switches when filter is active.
        assert!(
            dump.contains("vs deepseek"),
            "expected `vs deepseek` header under active filter"
        );
        // Focused row gets the ▶ marker.
        assert!(
            dump.contains("▶deepseek"),
            "expected ▶deepseek row marker under active filter"
        );
        // The unfiltered strategy-pnl panel must NOT have
        // `vs <strategy-name>` in its `agree` column header
        // (sanity-check on the dynamic-header logic in the
        // opposite direction). Note that the comparison panel
        // unconditionally has a `vs leader` column header — so
        // we look for the specific `vs deepseek` form rather than
        // any "vs " substring.
        let plain_dump = render_test_dashboard(None);
        assert!(
            !plain_dump.contains("vs deepseek"),
            "`vs deepseek` leaked into no-filter render"
        );
    }

    /// Pure builder for the comparison panel. Universe = union
    /// of every source's strategies; missing values default to
    /// $0.00. Today picks the latest snapshot per strategy;
    /// yesterday + 7d window sum across (paper, live, day) tuples.
    #[test]
    fn build_strategy_comparison_unions_sources_and_aggregates() {
        use crate::coredb::types::StrategyPnlSnapshot;
        let mut s = super::Snapshot::default();
        s.snapshots = vec![
            // baseline: two ts → pick the latest.
            StrategyPnlSnapshot {
                bucket_day_ms: 0,
                ts_ms: 1,
                strategy: "baseline".into(),
                n_decisions: 0,
                sum_size_usd: 0.0,
                sum_pnl: 1.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
            StrategyPnlSnapshot {
                bucket_day_ms: 0,
                ts_ms: 5,
                strategy: "baseline".into(),
                n_decisions: 0,
                sum_size_usd: 0.0,
                sum_pnl: 12.34,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
            // deepseek: only in snapshots (no settled trades).
            StrategyPnlSnapshot {
                bucket_day_ms: 0,
                ts_ms: 1,
                strategy: "deepseek".into(),
                n_decisions: 0,
                sum_size_usd: 0.0,
                sum_pnl: -5.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
        ];
        s.pnl_breakdown_yesterday = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: 1.5,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "live".into(),
                realized_pnl: 3.0,
                n_settled: 1,
            },
        ];
        s.pnl_breakdown_window = vec![
            // anthropic: only in window (no snapshot, no yesterday).
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "anthropic".into(),
                exec: "paper".into(),
                realized_pnl: 10.0,
                n_settled: 2,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
        ];

        let rows = super::build_strategy_comparison_rows(&s, None);
        let by_name: std::collections::HashMap<_, _> =
            rows.iter().map(|r| (r.strategy.as_str(), r)).collect();

        let baseline = by_name.get("baseline").unwrap();
        assert!((baseline.today_pnl - 12.34).abs() < 1e-9, "today picks latest ts");
        assert!((baseline.yesterday_pnl - 4.5).abs() < 1e-9, "yesterday sums paper+live");
        assert!((baseline.window_pnl - 5.0).abs() < 1e-9, "7d total from window");

        let deepseek = by_name.get("deepseek").unwrap();
        assert!((deepseek.today_pnl - (-5.0)).abs() < 1e-9);
        assert_eq!(deepseek.yesterday_pnl, 0.0, "no yesterday rows → 0");
        assert_eq!(deepseek.window_pnl, 0.0, "no window rows → 0");

        let anthropic = by_name.get("anthropic").unwrap();
        assert_eq!(anthropic.today_pnl, 0.0, "no snapshot → 0");
        assert_eq!(anthropic.yesterday_pnl, 0.0);
        assert!((anthropic.window_pnl - 10.0).abs() < 1e-9);

        // Universe is unioned. Order is 7d-desc with alphabetical
        // tiebreak — in this fixture: anthropic (10.0) > baseline
        // (5.0) > deepseek (0.0). Happens to coincide with
        // alphabetical here; see `build_strategy_comparison_sorts_
        // by_window_pnl_desc` for a fixture where the two
        // orderings differ.
        let names: Vec<_> = rows.iter().map(|r| r.strategy.as_str()).collect();
        assert_eq!(names, vec!["anthropic", "baseline", "deepseek"]);
    }

    /// Sort key cycles through 4 states and returns to the start.
    /// Locks the cycle order so a refactor that reshuffles the
    /// match arm doesn't silently break the hotkey UX.
    #[test]
    fn comparison_sort_cycles_through_all_four() {
        let s0 = super::ComparisonSort::Window7dDesc;
        let s1 = s0.cycle();
        let s2 = s1.cycle();
        let s3 = s2.cycle();
        let s4 = s3.cycle();
        assert_eq!(s1, super::ComparisonSort::TodayDesc);
        assert_eq!(s2, super::ComparisonSort::YesterdayDesc);
        assert_eq!(s3, super::ComparisonSort::StrategyAsc);
        assert_eq!(s4, super::ComparisonSort::Window7dDesc, "cycle should wrap");
    }

    /// Each sort key produces the expected ordering on a fixture
    /// where every column has a distinct ranking, so misrouting
    /// a sort key surfaces as a different head-of-list strategy.
    #[test]
    fn build_strategy_comparison_sorts_by_each_key() {
        use crate::coredb::types::StrategyPnlSnapshot;
        let mut s = super::Snapshot::default();
        // today:     baseline=+50 > deepseek=+10 > anthropic=-5
        // yesterday: anthropic=+20 > deepseek=+5 > baseline=-3
        // 7d:        deepseek=+100 > anthropic=+50 > baseline=-10
        // name asc:  anthropic, baseline, deepseek
        s.snapshots = vec![
            StrategyPnlSnapshot {
                bucket_day_ms: 0, ts_ms: 1, strategy: "baseline".into(),
                n_decisions: 0, sum_size_usd: 0.0, sum_pnl: 50.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
            StrategyPnlSnapshot {
                bucket_day_ms: 0, ts_ms: 1, strategy: "deepseek".into(),
                n_decisions: 0, sum_size_usd: 0.0, sum_pnl: 10.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
            StrategyPnlSnapshot {
                bucket_day_ms: 0, ts_ms: 1, strategy: "anthropic".into(),
                n_decisions: 0, sum_size_usd: 0.0, sum_pnl: -5.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
        ];
        s.pnl_breakdown_yesterday = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "anthropic".into(),
                exec: "paper".into(), realized_pnl: 20.0, n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "deepseek".into(),
                exec: "paper".into(), realized_pnl: 5.0, n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "baseline".into(),
                exec: "paper".into(), realized_pnl: -3.0, n_settled: 1,
            },
        ];
        s.pnl_breakdown_window = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "deepseek".into(),
                exec: "paper".into(), realized_pnl: 100.0, n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "anthropic".into(),
                exec: "paper".into(), realized_pnl: 50.0, n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "baseline".into(),
                exec: "paper".into(), realized_pnl: -10.0, n_settled: 1,
            },
        ];

        let head =
            |sort: super::ComparisonSort| -> Vec<String> {
                super::build_strategy_comparison_rows_sorted(&s, None, sort)
                    .into_iter()
                    .map(|r| r.strategy)
                    .collect()
            };
        assert_eq!(
            head(super::ComparisonSort::Window7dDesc),
            vec!["deepseek", "anthropic", "baseline"],
            "7d-desc",
        );
        assert_eq!(
            head(super::ComparisonSort::TodayDesc),
            vec!["baseline", "deepseek", "anthropic"],
            "today-desc",
        );
        assert_eq!(
            head(super::ComparisonSort::YesterdayDesc),
            vec!["anthropic", "deepseek", "baseline"],
            "yesterday-desc",
        );
        assert_eq!(
            head(super::ComparisonSort::StrategyAsc),
            vec!["anthropic", "baseline", "deepseek"],
            "strategy-asc",
        );
    }

    /// Title carries the active sort label so the operator can
    /// see at a glance which key the panel is ordered by.
    /// Default (Window7dDesc) → "7d↓" in title.
    #[test]
    fn panel_strategy_comparison_title_shows_sort_label() {
        let window = vec![super::PnlBreakdown {
            bucket_day_ms: 0, strategy: "baseline".into(),
            exec: "paper".into(), realized_pnl: 5.0, n_settled: 1,
        }];
        let dump = render_test_dashboard_full(None, None, Vec::new(), window);
        assert!(
            dump.contains("sorted by: 7d desc"),
            "expected default sort label in title:\n{dump}",
        );
    }

    /// Rank order on the comparison panel is "winning strategy
    /// first" — sort by 7d PnL descending, with alphabetical
    /// tiebreak for determinism on ties. Pin this with a fixture
    /// where alphabetical ordering would diverge from 7d-desc.
    #[test]
    fn build_strategy_comparison_sorts_by_window_pnl_desc() {
        let mut s = super::Snapshot::default();
        // 7d ranks: deepseek=$+100 → anthropic=$+50 → baseline=$-10
        // Alphabetical would give: anthropic, baseline, deepseek
        // — three distinct orderings depending on the sort key.
        s.pnl_breakdown_window = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: -10.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: 100.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "anthropic".into(),
                exec: "paper".into(),
                realized_pnl: 50.0,
                n_settled: 1,
            },
        ];
        let rows = super::build_strategy_comparison_rows(&s, None);
        let names: Vec<_> = rows.iter().map(|r| r.strategy.as_str()).collect();
        assert_eq!(
            names,
            vec!["deepseek", "anthropic", "baseline"],
            "expected 7d-desc ranking; got {names:?}",
        );
    }

    /// Tiebreak on equal 7d PnL: alphabetical. A fresh deployment
    /// where every strategy is at $0.00 must still render
    /// deterministically across frames (no churn from non-stable
    /// f64 ordering).
    #[test]
    fn build_strategy_comparison_alphabetical_tiebreak_on_equal_window_pnl() {
        let mut s = super::Snapshot::default();
        // Same 7d PnL for all three; alphabetical wins the tie.
        s.pnl_breakdown_window = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "anthropic".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
        ];
        let rows = super::build_strategy_comparison_rows(&s, None);
        let names: Vec<_> = rows.iter().map(|r| r.strategy.as_str()).collect();
        assert_eq!(
            names,
            vec!["anthropic", "baseline", "deepseek"],
            "expected alphabetical tiebreak; got {names:?}",
        );
    }

    /// End-to-end snapshot test: the panel renders with its title
    /// and the per-strategy chips show today + yesterday + 7d.
    #[test]
    fn panel_strategy_comparison_renders_with_strategy_chips() {
        // Use the populated yesterday + window fixture from the
        // existing footer test so the rows have meaningful values.
        let yesterday = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: 1.5,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: -2.25,
                n_settled: 1,
            },
        ];
        let window = yesterday.clone();
        let dump = render_test_dashboard_full(None, None, yesterday, window);
        assert!(
            dump.contains("strategy comparison"),
            "expected comparison panel title in dump",
        );
        // Header columns visible.
        for label in ["today Σpnl", "yesterday", "7d total", "agree (mean)"] {
            assert!(
                dump.contains(label),
                "comparison column `{label}` missing"
            );
        }
        // Per-strategy rows: each strategy from the fixture appears
        // somewhere in the rendered dashboard. Specifically the
        // yesterday's `baseline $+1.50` shows up in the comparison
        // panel's `yesterday` column (formatted as `$+1.50`).
        assert!(
            dump.contains("$+1.50"),
            "expected baseline yesterday value $+1.50 in dump"
        );
    }

    /// Rank prefix on each row matches the sort position. The
    /// comparison panel is 7d-desc-sorted, so the top row gets
    /// "#1", the next "#2", etc. Pin the indicator so a refactor
    /// that drops the rank cell or swaps in a different format
    /// (e.g. "1." vs "#1") fails loudly.
    #[test]
    fn panel_strategy_comparison_shows_rank_prefix() {
        let window = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: 100.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "anthropic".into(),
                exec: "paper".into(),
                realized_pnl: 50.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: -10.0,
                n_settled: 1,
            },
        ];
        let dump = render_test_dashboard_full(None, None, Vec::new(), window);
        // Header indicator (just "#") and the three ranks.
        assert!(dump.contains("#1"), "expected #1 rank in dump:\n{dump}");
        assert!(dump.contains("#2"), "expected #2 rank in dump:\n{dump}");
        assert!(dump.contains("#3"), "expected #3 rank in dump:\n{dump}");
    }

    /// Helper that renders the dashboard with explicit
    /// `prev_ranks` so rank-change indicator behavior can be
    /// exercised without simulating two full refreshes.
    fn render_test_dashboard_with_prev_ranks(
        pnl_breakdown_window: Vec<super::PnlBreakdown>,
        prev_ranks: std::collections::HashMap<String, usize>,
    ) -> String {
        use crate::coredb::types::BtcTick;
        use ratatui::backend::TestBackend;
        use ratatui::Terminal;
        use std::time::Duration;
        let mut s = super::Snapshot::default();
        s.btc = Some(BtcTick {
            bucket_hour_ms: 0,
            symbol: "BTCUSDT".into(),
            ts_ms: 1700000000000,
            price: 79123.45,
            volume: 0.0,
            bid: 79123.40,
            ask: 79123.50,
        });
        s.pnl_breakdown_window = pnl_breakdown_window;
        let strategies = super::strategies_in_view(&s);
        let backend = TestBackend::new(140, 50);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                super::draw(
                    f,
                    &s,
                    Duration::from_millis(0),
                    Duration::from_secs(42),
                    &strategies,
                    None,
                    &prev_ranks,
                    super::ComparisonSort::Window7dDesc,
                )
            })
            .unwrap();
        render_buffer(terminal.backend().buffer())
    }

    /// Rank-change indicator: per-strategy comparison of current
    /// rank vs `prev_ranks` produces ↑ / ↓ / = / blank. Pin each
    /// branch with a single-frame fixture.
    #[test]
    fn panel_strategy_comparison_rank_change_indicator() {
        // Current ranks (by window_pnl desc):
        //   #1 deepseek ($100)
        //   #2 anthropic ($50)
        //   #3 baseline (-$10)
        let window = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: 100.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "anthropic".into(),
                exec: "paper".into(),
                realized_pnl: 50.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: -10.0,
                n_settled: 1,
            },
        ];
        // Previous frame:
        //   #1 anthropic (now #2 → moved DOWN, "↓")
        //   #2 deepseek  (now #1 → moved UP,   "↑")
        //   #3 baseline  (now #3 → "=")
        //   (newcomer: would have blank but all 3 are in prev)
        let mut prev: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        prev.insert("anthropic".into(), 1);
        prev.insert("deepseek".into(), 2);
        prev.insert("baseline".into(), 3);

        let dump = render_test_dashboard_with_prev_ranks(window.clone(), prev);
        assert!(dump.contains('↑'), "expected ↑ for deepseek (#2→#1):\n{dump}");
        assert!(dump.contains('↓'), "expected ↓ for anthropic (#1→#2):\n{dump}");
        assert!(dump.contains('='), "expected = for baseline (#3→#3):\n{dump}");

        // Empty prev_ranks → no indicators emitted at all (a fresh
        // dashboard start has no baseline to compare against).
        let dump_empty = render_test_dashboard_with_prev_ranks(
            window.clone(),
            std::collections::HashMap::new(),
        );
        assert!(
            !dump_empty.contains('↑') && !dump_empty.contains('↓'),
            "fresh dashboard (no prev) should emit no Δ arrows:\n{dump_empty}"
        );
    }

    /// Δ column header is just the symbol "Δ" — pin so the
    /// header layout doesn't drift silently.
    #[test]
    fn panel_strategy_comparison_delta_column_header_present() {
        let window = vec![super::PnlBreakdown {
            bucket_day_ms: 0,
            strategy: "baseline".into(),
            exec: "paper".into(),
            realized_pnl: 1.0,
            n_settled: 1,
        }];
        let dump = render_test_dashboard_with_prev_ranks(
            window,
            std::collections::HashMap::new(),
        );
        assert!(dump.contains('Δ'), "Δ column header missing in dump:\n{dump}");
    }

    /// Spread column: leader row reads "(leader)" in gold; tied
    /// non-leader rows read "(tied)" in cyan; non-tied rows read
    /// "$-N.NN" in red. Pin all three branches with a fixture
    /// that exercises each.
    #[test]
    fn panel_strategy_comparison_spread_column_branches() {
        let window = vec![
            // deepseek leads at +100 → rank 1, "(leader)"
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: 100.0,
                n_settled: 1,
            },
            // anthropic at +50 → rank 2, spread = -50 → "$-50.00"
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "anthropic".into(),
                exec: "paper".into(),
                realized_pnl: 50.0,
                n_settled: 1,
            },
            // baseline at -10 → rank 3, spread = -110 → "$-110.00"
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: -10.0,
                n_settled: 1,
            },
        ];
        let dump = render_test_dashboard_full(None, None, Vec::new(), window);
        assert!(dump.contains("(leader)"), "leader marker missing:\n{dump}");
        assert!(dump.contains("$-50.00"), "rank-2 spread missing:\n{dump}");
        assert!(dump.contains("$-110.00"), "rank-3 spread missing:\n{dump}");
        // Header label visible.
        assert!(dump.contains("7d gap"), "7d gap header missing:\n{dump}");
    }

    /// `today gap` column independently leaderboards by today's
    /// Σpnl, not the 7d total. A strategy that's mid-table by 7d
    /// rank can still show "(leader)" in this column if it had
    /// the best session today. Pin both columns coexisting on the
    /// same row set.
    #[test]
    fn panel_strategy_comparison_today_gap_column_branches() {
        // Fixture: snapshots give today_pnl per strategy; window
        // gives 7d. Today's order differs from 7d's:
        //   today:  baseline=+30 > anthropic=+10 > deepseek=-5
        //   7d:     deepseek=+100 > anthropic=+50 > baseline=-10
        use crate::coredb::types::StrategyPnlSnapshot;
        let mut s = super::Snapshot::default();
        s.snapshots = vec![
            StrategyPnlSnapshot {
                bucket_day_ms: 0, ts_ms: 1, strategy: "baseline".into(),
                n_decisions: 0, sum_size_usd: 0.0, sum_pnl: 30.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
            StrategyPnlSnapshot {
                bucket_day_ms: 0, ts_ms: 1, strategy: "anthropic".into(),
                n_decisions: 0, sum_size_usd: 0.0, sum_pnl: 10.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
            StrategyPnlSnapshot {
                bucket_day_ms: 0, ts_ms: 1, strategy: "deepseek".into(),
                n_decisions: 0, sum_size_usd: 0.0, sum_pnl: -5.0,
                n_yes: 0, n_no: 0, n_pass: 0,
            },
        ];
        s.pnl_breakdown_window = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "deepseek".into(),
                exec: "paper".into(), realized_pnl: 100.0, n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "anthropic".into(),
                exec: "paper".into(), realized_pnl: 50.0, n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0, strategy: "baseline".into(),
                exec: "paper".into(), realized_pnl: -10.0, n_settled: 1,
            },
        ];
        let strategies = super::strategies_in_view(&s);
        let prev_ranks: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        let backend = ratatui::backend::TestBackend::new(140, 50);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                super::draw(
                    f,
                    &s,
                    std::time::Duration::from_millis(0),
                    std::time::Duration::from_secs(42),
                    &strategies,
                    None,
                    &prev_ranks,
                    super::ComparisonSort::Window7dDesc,
                )
            })
            .unwrap();
        let dump = render_buffer(terminal.backend().buffer());

        // today gap header is rendered.
        assert!(dump.contains("today gap"), "today gap header missing:\n{dump}");
        // Each strategy is renderable AND distinct from 7d gap
        // direction. baseline is BOTH 7d-last AND today-first;
        // we expect to see "$-110.00" (its 7d gap) AND "(leader)"
        // somewhere on the same row.
        //
        // anthropic: today $-20 vs leader baseline; 7d $-50 vs leader deepseek.
        //   So row has "$-20.00" (today gap) AND "$-50.00" (7d gap).
        // deepseek:  today $-35 vs leader baseline; 7d (leader).
        // baseline:  today (leader); 7d $-110 vs leader deepseek.
        assert!(
            dump.contains("$-20.00"),
            "anthropic's today-gap $-20.00 missing:\n{dump}"
        );
        assert!(
            dump.contains("$-35.00"),
            "deepseek's today-gap $-35.00 missing:\n{dump}"
        );
        assert!(
            dump.contains("$-110.00"),
            "baseline's 7d-gap $-110.00 missing:\n{dump}"
        );
        // Both leader markers in the same dump — they're on
        // different rows because today's leader (baseline) ≠ 7d
        // leader (deepseek). Substring count ≥ 2.
        let leader_count = dump.matches("(leader)").count();
        assert!(
            leader_count >= 2,
            "expected ≥2 (leader) markers (one per column), got {leader_count}:\n{dump}"
        );
    }

    /// Tied non-leader rows read "(tied)" instead of "$+0.00" —
    /// "(tied)" is more informative for the operator since
    /// "$+0.00" could ambiguously mean "exactly $0 ahead" or
    /// "no data".
    #[test]
    fn panel_strategy_comparison_spread_tied_rows() {
        let window = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "anthropic".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
        ];
        let dump = render_test_dashboard_full(None, None, Vec::new(), window);
        assert!(dump.contains("(leader)"), "leader marker missing");
        assert!(dump.contains("(tied)"), "tied marker missing");
        // The leader is anthropic (alphabetical tiebreak), and
        // the other two are tied with it. No "$-" or "$+" in the
        // spread column for these rows.
        let spread_block: String = dump
            .lines()
            .filter(|l| {
                l.contains("anthropic")
                    || l.contains("baseline")
                    || l.contains("deepseek")
            })
            .collect::<Vec<_>>()
            .join("\n");
        // Other panels (decisions, etc.) might contain "$+5.00"
        // for unrelated reasons. Just confirm the leader marker +
        // tied marker both appear in the comparison rows.
        assert!(
            spread_block.contains("(leader)") && spread_block.contains("(tied)"),
            "leader+tied combo missing in comparison rows:\n{spread_block}",
        );
    }

    /// Filter highlight: when a strategy_filter is active, that
    /// strategy's row gets the `▶<name>` prefix in the comparison
    /// table, AND the agree-rate column header switches to
    /// "vs <name>".
    #[test]
    fn panel_strategy_comparison_filter_marker_and_header() {
        let yesterday = vec![super::PnlBreakdown {
            bucket_day_ms: 0,
            strategy: "deepseek".into(),
            exec: "paper".into(),
            realized_pnl: 1.0,
            n_settled: 1,
        }];
        let window = yesterday.clone();
        let dump = render_test_dashboard_full(
            Some("deepseek"),
            None,
            yesterday,
            window,
        );
        assert!(
            dump.contains("vs deepseek"),
            "expected vs-deepseek header in comparison panel under filter",
        );
        assert!(
            dump.contains("▶deepseek"),
            "expected ▶deepseek row marker in comparison panel",
        );
    }

    #[test]
    fn panel_consensus_title_present() {
        let dump = render_test_dashboard(None);
        assert!(
            dump.contains("market consensus"),
            "consensus panel title missing"
        );
    }

    #[test]
    fn panel_positions_title_present() {
        let dump = render_test_dashboard(None);
        assert!(
            dump.contains("positions"),
            "positions panel title missing"
        );
    }

    #[test]
    fn panel_decisions_title_present() {
        let dump = render_test_dashboard(None);
        assert!(
            dump.contains("decisions today"),
            "decisions panel title missing"
        );
    }

    #[test]
    fn panel_header_chip_ok_renders_status_and_uptime() {
        // Healthy daemon: status label + daemon uptime "up Xs" both
        // visible. `detail` carries the ingest-age summary on the
        // green path — render it as the second header line.
        let chip = super::HealthChip {
            status: super::HealthStatus::Ok,
            daemon_uptime_secs: Some(125),
            detail: Some("btc 412ms / poly 1234ms".into()),
            ingest_restarts: Some((0, 0)),
        };
        let dump = render_test_dashboard_with(None, Some(chip));
        assert!(dump.contains("daemon:"), "chip prefix missing");
        assert!(dump.contains("ok"), "ok status label missing");
        // Uptime fmt_dur(125s) = "2m05s".
        assert!(dump.contains("up 2m05s"), "uptime label missing");
        assert!(
            dump.contains("btc 412ms / poly 1234ms"),
            "ingest-age detail missing"
        );
    }

    #[test]
    fn panel_header_chip_degraded_surfaces_detail_message() {
        // Degraded daemon: detail carries the worst subtask error
        // instead of ingest ages. Operator needs to see "why?" in
        // the header without journalctl.
        let chip = super::HealthChip {
            status: super::HealthStatus::Degraded,
            daemon_uptime_secs: Some(3600),
            detail: Some("backtest: 5× — connection refused".into()),
            ingest_restarts: Some((0, 0)),
        };
        let dump = render_test_dashboard_with(None, Some(chip));
        assert!(dump.contains("degraded"), "degraded status label missing");
        // Uptime fmt_dur(3600s) = "1h00m00s".
        assert!(dump.contains("up 1h00m00s"), "uptime label missing");
        assert!(
            dump.contains("backtest: 5× — connection refused"),
            "degraded detail not surfaced"
        );
    }

    #[test]
    fn panel_header_chip_unreachable_shows_reason() {
        // Unreachable: /health probe failed. daemon_uptime_secs is
        // None on this branch (we couldn't read it). Detail
        // captures the underlying GET / parse error.
        let chip = super::HealthChip {
            status: super::HealthStatus::Unreachable,
            daemon_uptime_secs: None,
            detail: Some("GET failed: connection refused".into()),
            ingest_restarts: None,
        };
        let dump = render_test_dashboard_with(None, Some(chip));
        assert!(dump.contains("unreachable"), "unreachable label missing");
        // No uptime suffix when daemon_uptime_secs is None — must not
        // render a stale "up 0s".
        assert!(!dump.contains(" up "), "uptime accidentally rendered without data");
        assert!(
            dump.contains("GET failed: connection refused"),
            "unreachable detail missing"
        );
    }

    #[test]
    fn panel_footer_renders_hotkey_chips() {
        let dump = render_test_dashboard(None);
        // Hardcoded hint line — survives any reorder of subsequent
        // rendered lines because we only check substrings.
        assert!(dump.contains("[q]/Esc quit"), "quit chip missing");
        assert!(dump.contains("[r] refresh"), "refresh chip missing");
        assert!(dump.contains("filter:"), "filter prefix missing");
        assert!(dump.contains("[+/-] cycle"), "cycle hint missing");
        // Digit chips for the two strategies present in the snapshot.
        assert!(dump.contains("[1] baseline"));
        assert!(dump.contains("[2] deepseek"));
        assert!(dump.contains("[0/c] all"));
    }

    /// Render a ratatui [`Buffer`] to a newline-separated string by
    /// concatenating cell symbols row by row. Style information is
    /// dropped — assertion-level snapshot testing only needs the
    /// visible character layout.
    fn render_buffer(buffer: &ratatui::buffer::Buffer) -> String {
        let width = buffer.area.width as usize;
        let mut out = String::new();
        for row in buffer.content.chunks(width) {
            for cell in row {
                out.push_str(cell.symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn filter_aware_consensus_passthrough_without_filter() {
        // No filter → identity order.
        let rows = vec![
            dec("m-a", "baseline", "YES", 1.0, 1),
            dec("m-a", "deepseek", "YES", 1.0, 1),
            dec("m-b", "baseline", "YES", 1.0, 2),
            dec("m-b", "deepseek", "NO", 1.0, 2),
        ];
        let consensus = super::build_consensus(&rows);
        let out = super::filter_aware_consensus(&consensus, None);
        let slugs: Vec<&str> = out.iter().map(|c| c.market_slug.as_str()).collect();
        let original: Vec<&str> = consensus.iter().map(|c| c.market_slug.as_str()).collect();
        assert_eq!(slugs, original);
    }

    #[test]
    fn filter_aware_consensus_promotes_focused_strategy() {
        // Build a 3-market table:
        //   m-only-baseline: only baseline weighed in
        //   m-only-deepseek: only deepseek weighed in
        //   m-both:          baseline + deepseek both weighed in
        // Filtering to "deepseek" should put m-only-deepseek and
        // m-both in tier 0, m-only-baseline in tier 1.
        let rows = vec![
            dec("m-only-baseline", "baseline", "YES", 1.0, 1),
            dec("m-only-deepseek", "deepseek", "YES", 1.0, 2),
            dec("m-both", "baseline", "YES", 1.0, 3),
            dec("m-both", "deepseek", "NO", 1.0, 3),
        ];
        let consensus = super::build_consensus(&rows);
        let out = super::filter_aware_consensus(&consensus, Some("deepseek"));
        // Tier 0 first: m-only-deepseek + m-both (order between them
        // preserved from pre-sort: stable sort).
        let tier0: std::collections::HashSet<&str> =
            out.iter().take(2).map(|c| c.market_slug.as_str()).collect();
        assert!(tier0.contains("m-only-deepseek"));
        assert!(tier0.contains("m-both"));
        // Tier 1 last: m-only-baseline.
        assert_eq!(out[2].market_slug, "m-only-baseline");
    }

    /// Restored filter matching a live strategy passes through.
    /// Restored filter for a strategy no longer present (data
    /// shifted during downtime) drops to None — otherwise the
    /// dashboard would show a phantom-focus state.
    #[test]
    fn validate_restored_filter_keeps_present_strategy() {
        let strats = vec!["baseline".to_string(), "deepseek".to_string()];
        assert_eq!(
            super::validate_restored_filter(Some("baseline".into()), &strats),
            Some("baseline".to_string()),
        );
    }

    #[test]
    fn validate_restored_filter_drops_absent_strategy() {
        let strats = vec!["baseline".to_string(), "deepseek".to_string()];
        assert_eq!(
            super::validate_restored_filter(Some("anthropic".into()), &strats),
            None,
            "absent strategy should drop",
        );
    }

    #[test]
    fn validate_restored_filter_none_stays_none() {
        let strats = vec!["baseline".to_string()];
        assert_eq!(super::validate_restored_filter(None, &strats), None);
    }

    #[test]
    fn validate_restored_filter_empty_strategies_drops_everything() {
        // No strategies visible (fresh deployment, no data yet)
        // → any restored filter is stale by definition.
        assert_eq!(
            super::validate_restored_filter(Some("baseline".into()), &[]),
            None,
        );
    }

    #[test]
    fn cycle_strategy_empty_list_is_no_op() {
        assert_eq!(cycle(&[], None, 1), None);
        assert_eq!(cycle(&[], Some("baseline"), 1), None);
        assert_eq!(cycle(&[], None, -1), None);
    }

    #[test]
    fn cycle_strategy_forward_from_none_picks_first() {
        assert_eq!(cycle(&["a", "b", "c"], None, 1).as_deref(), Some("a"));
    }

    #[test]
    fn cycle_strategy_forward_wraps_to_none_after_last() {
        assert_eq!(cycle(&["a", "b"], Some("a"), 1).as_deref(), Some("b"));
        // After the last strategy, +1 wraps back to None.
        assert_eq!(cycle(&["a", "b"], Some("b"), 1), None);
        // And from None we go to the first again.
        assert_eq!(cycle(&["a", "b"], None, 1).as_deref(), Some("a"));
    }

    #[test]
    fn cycle_strategy_backward_from_none_picks_last() {
        assert_eq!(cycle(&["a", "b", "c"], None, -1).as_deref(), Some("c"));
    }

    #[test]
    fn cycle_strategy_backward_wraps() {
        // From first → -1 → None.
        assert_eq!(cycle(&["a", "b"], Some("a"), -1), None);
        // None → -1 → last.
        assert_eq!(cycle(&["a", "b"], None, -1).as_deref(), Some("b"));
    }

    #[test]
    fn cycle_strategy_stale_filter_treated_as_none() {
        // Filter selected a strategy that no longer appears in the
        // list (e.g. snapshot data shifted). +1 should jump to the
        // first strategy — not panic on missing index lookup.
        assert_eq!(cycle(&["a", "b"], Some("gone"), 1).as_deref(), Some("a"));
        assert_eq!(cycle(&["a", "b"], Some("gone"), -1).as_deref(), Some("b"));
    }

    #[test]
    fn span_label_days_and_hours() {
        let day_ms = 86_400_000_i64;
        let hour_ms = 3_600_000_i64;
        let rows = vec![snap(0), snap(2 * day_ms + 5 * hour_ms)];
        assert_eq!(super::ts_span_label(&rows), "2d 5h series");
    }

    #[test]
    fn health_status_label_and_color() {
        // Just lock in the mapping — if someone reorders the enum
        // we want the chip colors to stay consistent.
        assert_eq!(super::HealthStatus::Ok.color(), Color::Green);
        assert_eq!(super::HealthStatus::Degraded.color(), Color::Yellow);
        assert_eq!(super::HealthStatus::Unreachable.color(), Color::Red);
    }

    #[test]
    fn consensus_sort_puts_splits_first() {
        let rows = vec![
            // Market A: all agree (sum size $20)
            dec("market-a", "baseline", "YES", 10.0, 1),
            dec("market-a", "deepseek", "YES", 10.0, 2),
            // Market B: split (sum size $5)
            dec("market-b", "baseline", "YES", 3.0, 3),
            dec("market-b", "deepseek", "NO", 2.0, 4),
            // Market C: solo
            dec("market-c", "baseline", "YES", 4.0, 5),
        ];
        let c = build_consensus(&rows);
        // Splits sort first regardless of size; then all-agree by size
        // desc; then solo.
        assert_eq!(c[0].market_slug, "market-b");
        assert_eq!(c[1].market_slug, "market-a");
        assert_eq!(c[2].market_slug, "market-c");
    }
}
