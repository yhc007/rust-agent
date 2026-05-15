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

use crate::coredb::btc::BtcTickRepo;
use crate::coredb::decisions::DecisionRepo;
use crate::coredb::orders::PositionRepo;
use crate::coredb::strategy_pnl::StrategyPnlRepo;
use crate::coredb::types::{bucket_day, now_ms, BtcTick, Decision, Position, StrategyPnlSnapshot};
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

pub async fn run(coredb_uri: &str) -> Result<()> {
    let db = CoreDb::connect(coredb_uri)
        .await
        .with_context(|| format!("connect coredb at {coredb_uri}"))?;
    let btc_repo = BtcTickRepo::new(db.session()).await?;
    let dec_repo = DecisionRepo::new(db.session()).await?;
    let pos_repo = PositionRepo::new(db.session()).await?;
    let pnl_repo = StrategyPnlRepo::new(db.session()).await?;

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
) -> Result<()> {
    let mut snapshot = fetch_snapshot(btc_repo, dec_repo, pos_repo, pnl_repo).await;
    let mut last_refresh = Instant::now();

    loop {
        let uptime = started_at.elapsed();
        let snap_age = last_refresh.elapsed();
        terminal.draw(|f| draw(f, &snapshot, snap_age, uptime))?;

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
                        snapshot = fetch_snapshot(btc_repo, dec_repo, pos_repo, pnl_repo).await;
                        last_refresh = Instant::now();
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        // Auto-refresh on the cadence.
        if last_refresh.elapsed() >= REFRESH_EVERY {
            snapshot = fetch_snapshot(btc_repo, dec_repo, pos_repo, pnl_repo).await;
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
    errors: Vec<String>,
}

async fn fetch_snapshot(
    btc: &BtcTickRepo,
    dec: &DecisionRepo,
    pos: &PositionRepo,
    pnl: &StrategyPnlRepo,
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
    match pnl.list_day(bd).await {
        Ok(v) => s.snapshots = v,
        Err(e) => s.errors.push(format!("strategy_pnl.list_day: {e}")),
    }
    s
}

fn draw(
    f: &mut ratatui::Frame,
    s: &Snapshot,
    snap_age: Duration,
    uptime: Duration,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),   // header
            Constraint::Length(6),   // strategy pnl
            Constraint::Min(6),      // positions
            Constraint::Min(8),      // recent decisions
            Constraint::Length(3),   // footer
        ])
        .split(f.area());

    draw_header(f, chunks[0], s, snap_age, uptime);
    draw_strategy_pnl(f, chunks[1], s);
    draw_positions(f, chunks[2], s);
    draw_decisions(f, chunks[3], s);
    draw_footer(f, chunks[4], s);
}

fn draw_header(
    f: &mut ratatui::Frame,
    area: Rect,
    s: &Snapshot,
    snap_age: Duration,
    uptime: Duration,
) {
    let btc_line = match &s.btc {
        Some(t) => {
            let age_ms = now_ms() - t.ts_ms;
            format!(
                "BTC ${:>10.2}   bid/ask ${:.2} / ${:.2}   tick age {:>5} ms",
                t.price, t.bid, t.ask, age_ms
            )
        }
        None => "BTC: (no cached tick)".to_string(),
    };
    let meta = format!(
        "snap {:>4} ms ago   uptime {}",
        snap_age.as_millis(),
        fmt_dur(uptime)
    );
    let para = Paragraph::new(vec![
        Line::from(Span::styled(btc_line, Style::default().add_modifier(Modifier::BOLD))),
        Line::from(meta),
    ])
    .block(Block::default().borders(Borders::ALL).title(" rust-agent dashboard "));
    f.render_widget(para, area);
}

fn draw_strategy_pnl(f: &mut ratatui::Frame, area: Rect, s: &Snapshot) {
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

    let header = Row::new([
        "strategy", "ts (UTC)", "decisions", "YES", "NO", "PASS", "Σ size", "Σ pnl", "trend",
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
        rows.push(Row::new(vec![
            Cell::from(latest.strategy.clone()),
            Cell::from(ts),
            Cell::from(latest.n_decisions.to_string()),
            Cell::from(latest.n_yes.to_string()),
            Cell::from(latest.n_no.to_string()),
            Cell::from(latest.n_pass.to_string()),
            Cell::from(format!("${:.2}", latest.sum_size_usd)),
            Cell::from(Span::styled(format!("${:+.2}", latest.sum_pnl), pnl_style)),
            Cell::from(Span::styled(sparkline(&pnls), pnl_style)),
        ]));
    }
    let title = if rows.is_empty() {
        " strategy pnl (no snapshots yet — run `compare-pnl` or daemon) "
    } else {
        " strategy pnl (latest snapshot per strategy, trend = today's series) "
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
    ];
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, area);
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

fn draw_decisions(f: &mut ratatui::Frame, area: Rect, s: &Snapshot) {
    let header = Row::new(["ts (UTC)", "strategy", "market_slug", "side", "size", "conf"])
        .style(Style::default().add_modifier(Modifier::BOLD));
    let rows: Vec<Row> = s
        .decisions
        .iter()
        .take(RECENT_DECISIONS)
        .map(|d| {
            let ts = DateTime::<Utc>::from_timestamp_millis(d.ts_ms)
                .map(|t| t.format("%H:%M:%S").to_string())
                .unwrap_or_default();
            let strategy = if d.raw_response == "baseline-rule" {
                "baseline"
            } else {
                "llm"
            };
            let side_style = match d.side.as_str() {
                "YES" => Style::default().fg(Color::Green),
                "NO" => Style::default().fg(Color::Red),
                _ => Style::default().fg(Color::DarkGray),
            };
            Row::new(vec![
                Cell::from(ts),
                Cell::from(strategy),
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
    let title = format!(
        " decisions today ({} total, showing latest {}) ",
        s.decisions.len(),
        s.decisions.len().min(RECENT_DECISIONS)
    );
    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title));
    f.render_widget(table, area);
}

fn draw_footer(f: &mut ratatui::Frame, area: Rect, s: &Snapshot) {
    let mut lines = vec![Line::from(
        " [q]/Esc quit   [r] refresh now   (auto-refresh every 5 s) ",
    )];
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
    use super::sparkline;

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
}
