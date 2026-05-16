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
    let mut snapshot = fetch_snapshot(
        btc_repo, dec_repo, pos_repo, pnl_repo, agree_repo, breakdown_repo, health_url,
        health_client, pnl_days,
    )
    .await;
    let mut last_refresh = Instant::now();
    // Strategy currently focused in the decisions panel. `None` = show
    // every strategy. Set by digit-key hotkeys; cleared by `0` or `c`.
    // Survives refreshes so the operator's selection doesn't reset on
    // every auto-tick.
    let mut strategy_filter: Option<String> = None;

    loop {
        let uptime = started_at.elapsed();
        let snap_age = last_refresh.elapsed();
        // Cache the sorted strategy list so hotkey-1..9 lookup and the
        // footer "[1] baseline ..." render are consistent within a
        // single frame.
        let strategies = strategies_in_view(&snapshot);
        terminal.draw(|f| draw(f, &snapshot, snap_age, uptime, &strategies, strategy_filter.as_deref()))?;

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
                            health_url, health_client, pnl_days,
                        )
                        .await;
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
                    _ => {}
                },
                _ => {}
            }
        }

        // Auto-refresh on the cadence.
        if last_refresh.elapsed() >= REFRESH_EVERY {
            snapshot = fetch_snapshot(
                btc_repo, dec_repo, pos_repo, pnl_repo, agree_repo, breakdown_repo, health_url,
                health_client, pnl_days,
            )
            .await;
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
    if let (Some(url), Some(client)) = (health_url, health_client) {
        s.health = Some(fetch_health(client, url).await);
    }
    s
}

/// Hit the daemon /health endpoint once and reduce the response to a
/// [`HealthChip`]. Any non-2xx, timeout, connection error, or JSON
/// parse error collapses into an Unreachable chip with the reason in
/// `detail` — the operator should be able to tell *why* the dashboard
/// can't reach the daemon without dropping to a terminal.
async fn fetch_health(client: &reqwest::Client, url: &str) -> HealthChip {
    let resp = match client.get(url).send().await {
        Ok(r) => r,
        Err(e) => {
            return HealthChip {
                status: HealthStatus::Unreachable,
                daemon_uptime_secs: None,
                detail: Some(format!("GET failed: {}", short_err(&e.to_string()))),
            };
        }
    };
    if !resp.status().is_success() {
        return HealthChip {
            status: HealthStatus::Unreachable,
            daemon_uptime_secs: None,
            detail: Some(format!("HTTP {}", resp.status().as_u16())),
        };
    }
    let body: HealthWire = match resp.json().await {
        Ok(b) => b,
        Err(e) => {
            return HealthChip {
                status: HealthStatus::Unreachable,
                daemon_uptime_secs: None,
                detail: Some(format!("bad JSON: {}", short_err(&e.to_string()))),
            };
        }
    };
    let status = match body.status.as_str() {
        "ok" => HealthStatus::Ok,
        _ => HealthStatus::Degraded,
    };
    let detail = if status == HealthStatus::Ok {
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
    HealthChip {
        status,
        daemon_uptime_secs: Some(body.uptime_secs),
        detail,
    }
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
            // 7 = bordered table (6) + 1-row footer below it that
            // surfaces yesterday's final realized PnL by strategy.
            Constraint::Length(7),                  // strategy pnl + yesterday footer
            Constraint::Min(5),                     // market consensus
            Constraint::Min(5),                     // positions
            Constraint::Min(7),                     // recent decisions
            Constraint::Length(footer_height),      // footer
        ])
        .split(f.area());

    draw_header(f, chunks[0], s, snap_age, uptime);
    draw_strategy_pnl(f, chunks[1], s, strategy_filter);
    draw_consensus(f, chunks[2], s, strategy_filter);
    draw_positions(f, chunks[3], s);
    draw_decisions(f, chunks[4], s, strategy_filter);
    draw_footer(f, chunks[5], s, strategies, strategy_filter);
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
    // Split the panel into a bordered table (top) and a single-row
    // yesterday-PnL footer (bottom). Footer sits *outside* the table's
    // box so the table's row count isn't affected by its presence.
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(1)])
        .split(area);
    let table_area = split[0];
    let footer_area = split[1];
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

    // One-line yesterday-PnL footer just under the table. Strategy is
    // the grouping axis (paper + live summed within a strategy)
    // because operators care about per-strategy edge — paper vs live
    // splits live in /metrics + pnl-breakdown-history when they need
    // them. Empty rowset renders as a hint line rather than an error.
    let footer_line = render_yesterday_pnl_footer(&s.pnl_breakdown_yesterday, strategy_filter);
    f.render_widget(Paragraph::new(footer_line), footer_area);
}

/// Build the one-line "Yesterday final: …" footer that lives just under
/// the strategy-pnl table. Pulled out of the draw function so it's
/// unit-testable without touching ratatui.
///
/// Rendering rules:
///   - No rows → "Yesterday final: (no settled trades yet)".
///   - With rows → sum realized_pnl per strategy (across paper+live)
///     and emit "Yesterday final: a +$X.XX  b -$Y.YY". Strategy order
///     is alphabetical for stable rendering across frames.
///   - The active filter, if any, gets a cyan highlight on its chip so
///     the operator's eye lands on it the same way it lands on the row
///     marker in the table above.
fn render_yesterday_pnl_footer<'a>(
    rows: &'a [PnlBreakdown],
    strategy_filter: Option<&str>,
) -> Line<'a> {
    if rows.is_empty() {
        return Line::from(vec![
            Span::styled(
                "Yesterday final: ",
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                "(no settled trades yet)",
                Style::default().fg(Color::DarkGray),
            ),
        ]);
    }
    // Aggregate paper + live within a strategy: operators want
    // per-strategy bottom-line, not a paper-vs-live split (the
    // pnl-breakdown-history report already covers that axis).
    let mut by_strategy: std::collections::BTreeMap<String, f64> =
        std::collections::BTreeMap::new();
    for r in rows {
        *by_strategy.entry(r.strategy.clone()).or_insert(0.0) += r.realized_pnl;
    }
    let mut spans: Vec<Span<'a>> = vec![Span::styled(
        "Yesterday final: ",
        Style::default().add_modifier(Modifier::BOLD),
    )];
    let mut first = true;
    for (strategy, total) in by_strategy {
        if !first {
            spans.push(Span::raw("  "));
        }
        first = false;
        let color = if total >= 0.0 { Color::Green } else { Color::Red };
        let label = format!("{strategy} ${:+.2}", total);
        let style = if strategy_filter == Some(strategy.as_str()) {
            Style::default()
                .fg(Color::Black)
                .bg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(color).add_modifier(Modifier::BOLD)
        };
        spans.push(Span::styled(label, style));
    }
    Line::from(spans)
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
        " [q]/Esc quit   [r] refresh now   (auto-refresh every 5 s) ",
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
        };
        assert!(super::worst_subtask_error(&body).is_none());
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
        render_test_dashboard_full(filter, health, Vec::new())
    }

    /// Full test renderer that also accepts a `pnl_breakdown_yesterday`
    /// fixture so the new footer line can be exercised end-to-end via
    /// the standard `draw()` entry point. The two thinner shims keep the
    /// existing test call sites stable.
    fn render_test_dashboard_full(
        filter: Option<&str>,
        health: Option<super::HealthChip>,
        pnl_breakdown_yesterday: Vec<PnlBreakdown>,
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

        let strategies = super::strategies_in_view(&s);
        let backend = TestBackend::new(140, 35);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| {
                super::draw(
                    f,
                    &s,
                    Duration::from_millis(0),
                    Duration::from_secs(42),
                    &strategies,
                    filter,
                )
            })
            .unwrap();
        render_buffer(terminal.backend().buffer())
    }

    /// Backwards-compat shim — `health` defaults to `None`.
    fn render_test_dashboard(filter: Option<&str>) -> String {
        render_test_dashboard_with(filter, None)
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
        // The unfiltered case must NOT have "vs " in it (sanity-
        // check on the dynamic-header logic in the opposite
        // direction). The consensus panel uses plain "agree" as a
        // column name in both cases, so we don't try to assert
        // its absence — only check that the filtered prefix
        // doesn't leak into the no-filter render.
        let plain_dump = render_test_dashboard(None);
        assert!(
            !plain_dump.contains("vs "),
            "`vs ` leaked into no-filter render"
        );
    }

    #[test]
    fn panel_strategy_pnl_footer_empty_shows_hint() {
        // No yesterday rows → hint line surfaces inside the dashboard,
        // not an error. Confirms the empty-rowset branch.
        let dump = render_test_dashboard(None);
        assert!(
            dump.contains("Yesterday final:"),
            "footer prefix missing in default render"
        );
        assert!(
            dump.contains("(no settled trades yet)"),
            "expected empty-rowset hint to render"
        );
    }

    #[test]
    fn panel_strategy_pnl_footer_renders_per_strategy_totals() {
        // Populated yesterday — paper+live within a strategy are
        // summed (1.50 + 3.00 = 4.50 for baseline; -2.25 alone for
        // deepseek). The rendered line includes both per-strategy
        // chips. Strategy chips are formatted as "name $+X.XX" /
        // "name $-X.XX" so a `+`-prefixed positive baseline is what
        // the test pins.
        let yesterday = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "paper".into(),
                realized_pnl: 1.50,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "baseline".into(),
                exec: "live".into(),
                realized_pnl: 3.00,
                n_settled: 2,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "deepseek".into(),
                exec: "paper".into(),
                realized_pnl: -2.25,
                n_settled: 1,
            },
        ];
        let dump = render_test_dashboard_full(None, None, yesterday);
        assert!(
            dump.contains("Yesterday final:"),
            "footer prefix missing in populated render"
        );
        assert!(
            dump.contains("baseline $+4.50"),
            "expected baseline paper+live aggregate \"baseline $+4.50\", got dump:\n{dump}"
        );
        assert!(
            dump.contains("deepseek $-2.25"),
            "expected deepseek total \"deepseek $-2.25\", got dump:\n{dump}"
        );
        // The empty hint must not leak through when data is present.
        assert!(
            !dump.contains("no settled trades yet"),
            "empty hint leaked into populated render"
        );
    }

    #[test]
    fn yesterday_footer_pure_empty_branch() {
        // Unit-test the pure helper directly so we don't have to spin
        // up a TestBackend just to exercise the empty branch.
        let line = super::render_yesterday_pnl_footer(&[], None);
        let flat: String = line
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect::<String>();
        assert!(flat.starts_with("Yesterday final:"), "got: {flat}");
        assert!(
            flat.contains("(no settled trades yet)"),
            "expected empty hint, got: {flat}"
        );
    }

    #[test]
    fn yesterday_footer_pure_aggregates_paper_plus_live() {
        let rows = vec![
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "alpha".into(),
                exec: "paper".into(),
                realized_pnl: 5.0,
                n_settled: 1,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "alpha".into(),
                exec: "live".into(),
                realized_pnl: 7.5,
                n_settled: 2,
            },
            super::PnlBreakdown {
                bucket_day_ms: 0,
                strategy: "beta".into(),
                exec: "paper".into(),
                realized_pnl: -1.25,
                n_settled: 1,
            },
        ];
        let line = super::render_yesterday_pnl_footer(&rows, None);
        let flat: String = line
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect::<String>();
        // BTreeMap iteration → alphabetical: alpha before beta.
        let alpha_pos = flat.find("alpha $+12.50").unwrap_or_else(|| {
            panic!("alpha aggregate missing — got: {flat}")
        });
        let beta_pos = flat.find("beta $-1.25").unwrap_or_else(|| {
            panic!("beta aggregate missing — got: {flat}")
        });
        assert!(
            alpha_pos < beta_pos,
            "strategies should render alphabetically — got: {flat}"
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
