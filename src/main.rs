//! Rust Agent CLI
//! 
//! A terminal-based AI agent powered by Claude.

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

mod api;
mod backtest;
mod config;
mod coredb;
mod daemon;
mod data;
mod engine;
mod execution;
mod risk;
mod tools;
mod memory;
mod tui;
mod web;

use engine::QueryEngine;
use config::{Backend, Config};

#[derive(Parser)]
#[command(name = "rust-agent")]
#[command(author = "Paul Yu")]
#[command(version = "0.1.0")]
#[command(about = "🦀 Rust-based AI Agent", long_about = None)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    
    /// Model to use. Empty / unset → fall back to whatever
    /// `Config::load` picked for the active backend (default
    /// `claude-sonnet-4-20250514` on Anthropic, `OPENAI_MODEL` on
    /// the OpenAI-compat backend). Setting this here previously
    /// hard-coded an Anthropic id that the OpenAI path then sent to
    /// vLLM, which rejected the request with 404.
    #[arg(short, long, default_value = "")]
    model: String,
    
    /// Run a single prompt and exit
    #[arg(short, long)]
    prompt: Option<String>,
    
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start interactive chat
    Chat {
        /// Initial prompt
        #[arg(short, long)]
        prompt: Option<String>,
    },
    /// Run a single task
    Run {
        /// Task description
        task: String,
    },
    /// List available tools
    Tools,
    /// Show configuration
    Config,
    /// Serve the agent over HTTP — opens a browser-friendly SSE
    /// endpoint at /api/agent/run and a single-page UI at /.
    Serve {
        /// Port to bind. Defaults to 8090.
        #[arg(short = 'P', long, default_value = "8090")]
        port: u16,
        /// Bind address. Defaults to 127.0.0.1; use 0.0.0.0 to expose.
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
    },
    /// Apply CoreDB schema migrations for the polymarket_btc keyspace.
    Migrate {
        /// CoreDB native-protocol endpoint (host:port). Defaults to 127.0.0.1:9042.
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
    },
    /// Run Binance + Polymarket ingestion daemons that populate CoreDB.
    /// Stays in the foreground; Ctrl+C for a clean shutdown.
    Ingest {
        /// CoreDB endpoint. Defaults to 127.0.0.1:9042.
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
    },
    /// Print row counts for each polymarket_btc table.
    Stats {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
    },
    /// Run a decision pass over every open BTC market in CoreDB,
    /// recording one decision per market. Default uses the deterministic
    /// baseline rule; `--llm` switches to the configured LLM (DeepSeek
    /// by default — see `Config::load`); `--both` runs both strategies
    /// against the same market state and emits two decision rows per
    /// market, which makes downstream PnL comparisons timing-honest.
    /// With `--execute`, every non-PASS decision is also routed through
    /// the risk gate + paper executor, writing Order + Position rows.
    Backtest {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
        /// Use the LLM-driven decision path instead of the baseline rule.
        #[arg(long)]
        llm: bool,
        /// Run BOTH baseline and LLM strategies per market (overrides --llm).
        #[arg(long)]
        both: bool,
        /// Run N LLM strategies side-by-side on the same market state.
        /// Comma-separated preset names — supported: `anthropic`,
        /// `deepseek`, `openai`. Overrides `--llm`; combine with
        /// `--both` to also include the baseline rule.
        ///
        /// Each preset reads its own env vars: anthropic →
        /// ANTHROPIC_API_KEY (+ optional ANTHROPIC_MODEL), deepseek →
        /// DEEPSEEK_API_KEY / ~/.deepseek (+ optional DEEPSEEK_MODEL),
        /// openai → OPENAI_API_KEY + OPENAI_MODEL.
        #[arg(long, value_delimiter = ',')]
        llms: Vec<String>,
        /// Auto-execute non-PASS decisions via risk gate + executor.
        #[arg(long)]
        execute: bool,
        /// Use LiveExec instead of PaperExec. LiveExec is *itself* DRY_RUN
        /// unless LIVE_TRADING_ENABLED=1 — this flag alone does not
        /// broadcast orders. Both gates must be set, plus valid
        /// POLYMARKET_CLOB_* creds, plus a USDC approve on-chain.
        #[arg(long)]
        live: bool,
    },
    /// Compare baseline vs LLM strategy PnL on today's decisions, marked
    /// to the current Polymarket YES prices. Reads `polymarket_btc.decisions`
    /// and prints aggregates + per-market disagreements. Also persists a
    /// per-strategy snapshot into `polymarket_btc.strategy_pnl_snapshots`
    /// so the run can be replayed as a time series via `pnl-history`.
    ComparePnl {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
        /// Restrict the comparison to a comma-separated subset of
        /// strategy labels (e.g. `--strategies baseline,deepseek`).
        /// Empty = include every strategy that appears in the
        /// chosen window. Matches `Decision::effective_strategy()`,
        /// so the same labels you see in the dashboard / consensus
        /// panel work here.
        #[arg(long, value_delimiter = ',')]
        strategies: Vec<String>,
        /// Number of UTC days back from "today" to include in the
        /// comparison (1 = today only, the historical default).
        /// Multi-day runs are analysis-only — they skip the
        /// strategy_pnl_snapshots persistence to avoid corrupting
        /// the daily-resolution time series.
        #[arg(long, default_value_t = 1)]
        days: u32,
    },
    /// Dump strategy PnL snapshots from CoreDB as a time series.
    /// Default scans today only; use `--days N` to widen the window
    /// to the last N UTC days. Intended consumer of the cron-driven
    /// `compare-pnl` writes.
    PnlHistory {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
        /// Restrict the dump to a comma-separated subset of strategy
        /// labels (e.g. `--strategies baseline,deepseek`). Empty =
        /// include every strategy that appears in today's snapshots.
        /// Mirrors the `compare-pnl --strategies` flag for symmetry.
        #[arg(long, value_delimiter = ',')]
        strategies: Vec<String>,
        /// Number of UTC days back from "today" to include (1 = today
        /// only, the historical default). Each day is a separate
        /// `strategy_pnl_snapshots` partition read; failures on one
        /// day don't abort the rest.
        #[arg(long, default_value_t = 1)]
        days: u32,
    },
    /// Settle today's orders against Polymarket's resolved markets.
    /// Computes realized PnL per order, aggregates per strategy, and
    /// upserts the day's total into `polymarket_btc.pnl_daily`.
    SettlePnl {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
    },
    /// Dump every row in `polymarket_btc.positions_v2`, newest first.
    /// The headless analogue of the dashboard's positions panel.
    Positions {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
    },
    /// Connect to Polymarket's user-channel WebSocket and log
    /// incoming fill/order notifications. Requires
    /// POLYMARKET_CLOB_API_KEY / _SECRET / _PASSPHRASE. Set
    /// APPLY_FILLS=1 to also write trade events back into the
    /// `orders` table (default off — observation only).
    UserChannel {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
    },
    /// Live operator dashboard. Polls CoreDB every 5 s and shows
    /// strategy PnL, open positions, and recent decisions. `q` to quit.
    Dashboard {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
        /// Optional daemon /health URL to probe each refresh. When
        /// set, the header gains a colored chip (ok/degraded/
        /// unreachable) plus a one-line detail summary. Typical
        /// value: `http://127.0.0.1:9099/health`.
        #[arg(long)]
        health_url: Option<String>,
        /// Number of UTC days back from "today" to feed the strategy-
        /// PnL trend sparkline (1 = today only, the historical default).
        /// Wider windows surface multi-day trends — useful once the
        /// daemon has been running through several UTC days.
        #[arg(long, default_value_t = 1)]
        pnl_days: u32,
    },
    /// Run the Polymarket CLOB L1 handshake against
    /// `POST https://clob.polymarket.com/auth/api-key` using
    /// `POLYMARKET_PRIVATE_KEY` and print the issued API key /
    /// secret / passphrase. Run once per wallet; persist the output.
    ClobAuth {},
    /// USDC `approve` the Polymarket CTF Exchange as a spender. One-
    /// time per wallet. Without this, even a successful `POST /order`
    /// is rejected at fill. Default DRY_RUN — pass `--send` to
    /// actually broadcast the transaction.
    UsdcApprove {
        /// Polygon mainnet RPC URL (HTTP). Falls back to
        /// `POLYGON_RPC_URL` env when not passed.
        #[arg(long)]
        rpc_url: Option<String>,
        /// USDC base units (6 decimals). Default: `U256::MAX` (unlimited).
        #[arg(long)]
        amount: Option<String>,
        /// Override the spender address (default: CTF Exchange).
        #[arg(long)]
        spender: Option<String>,
        /// Override the USDC token address (default: PoS-bridged USDC.e).
        #[arg(long)]
        usdc: Option<String>,
        /// Actually broadcast the approve transaction. Without this
        /// flag the helper only reads + prints the intended call.
        #[arg(long)]
        send: bool,
    },
    /// Run ingest + periodic backtest/compare-pnl/settle-pnl under one
    /// process. The "do everything" mode — replaces a cron stack for
    /// day-to-day paper trading. Ctrl+C tears the whole pipeline down.
    Daemon {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
        /// Seconds between backtest --both --execute runs.
        #[arg(long, default_value_t = 1800)]
        backtest_every: u64,
        /// Seconds between compare-pnl runs.
        #[arg(long, default_value_t = 900)]
        compare_every: u64,
        /// Seconds between settle-pnl runs.
        #[arg(long, default_value_t = 3600)]
        settle_every: u64,
        /// Suppress paper-execute on the periodic backtests (decisions
        /// only). Off by default — paper trading is the point.
        #[arg(long)]
        no_execute: bool,
        /// Use LiveExec instead of PaperExec for the periodic
        /// backtests. Same two-gate posture as `backtest --live`:
        /// LiveExec stays DRY_RUN unless LIVE_TRADING_ENABLED=1.
        #[arg(long)]
        live: bool,
        /// Comma-separated LLM preset names (`anthropic,deepseek,openai`)
        /// to run on every periodic backtest. Empty = single
        /// env-resolved LLM (legacy `--both`). Each preset must have
        /// its own credentials set (see `backtest --llms` docs).
        #[arg(long, value_delimiter = ',')]
        llms: Vec<String>,
        /// When set, the daemon exposes a tiny `/health` JSON
        /// endpoint on `0.0.0.0:<port>`. Intended for external
        /// monitors / reverse proxies / systemd watchdog scripts.
        /// Default off (no listener).
        #[arg(long)]
        health_port: Option<u16>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Setup logging
    let filter = if cli.verbose {
        EnvFilter::new("debug")
    } else {
        EnvFilter::new("info")
    };
    
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .init();

    // Config is loaded lazily inside the arms that need it — the coredb /
    // ingest / backtest / stats / tools subcommands have no LLM dependency
    // and should not require ANTHROPIC_API_KEY (or the active backend's key)
    // just to read the keyspace.

    match cli.command {
        Some(Commands::Chat { prompt }) => {
            run_chat(Config::load()?, cli.model, prompt).await?;
        }
        Some(Commands::Run { task }) => {
            run_task(Config::load()?, cli.model, task).await?;
        }
        Some(Commands::Tools) => {
            list_tools();
        }
        Some(Commands::Config) => {
            show_config(&Config::load()?);
        }
        Some(Commands::Serve { port, host }) => {
            run_serve(Config::load()?, cli.model, host, port).await?;
        }
        Some(Commands::Migrate { coredb_uri }) => {
            run_migrate(coredb_uri).await?;
        }
        Some(Commands::Ingest { coredb_uri }) => {
            data::ingest::run(&coredb_uri).await?;
        }
        Some(Commands::Stats { coredb_uri }) => {
            run_stats(coredb_uri).await?;
        }
        Some(Commands::Backtest { coredb_uri, llm, both, llms, execute, live }) => {
            let plan = if !llms.is_empty() {
                // `--llms` is the N-way path. `--both` here means
                // "also run baseline alongside the listed LLMs"; the
                // legacy single-LLM `--llm` flag is ignored because
                // `--llms` is strictly more expressive.
                backtest::BacktestPlan::multi(both, llms)
            } else if both {
                backtest::BacktestPlan::both()
            } else if llm {
                backtest::BacktestPlan::default_llm()
            } else {
                backtest::BacktestPlan::baseline_only()
            };
            backtest::run::run(&coredb_uri, plan, execute, live).await?;
        }
        Some(Commands::ComparePnl { coredb_uri, strategies, days }) => {
            let filter = if strategies.is_empty() { None } else { Some(strategies) };
            backtest::compare::run(&coredb_uri, filter.as_deref(), days).await?;
        }
        Some(Commands::PnlHistory { coredb_uri, strategies, days }) => {
            let filter = if strategies.is_empty() { None } else { Some(strategies) };
            backtest::history::run(&coredb_uri, filter.as_deref(), days).await?;
        }
        Some(Commands::SettlePnl { coredb_uri }) => {
            backtest::settle::run(&coredb_uri).await?;
        }
        Some(Commands::Positions { coredb_uri }) => {
            run_positions(&coredb_uri).await?;
        }
        Some(Commands::UserChannel { coredb_uri }) => {
            run_user_channel(coredb_uri).await?;
        }
        Some(Commands::Dashboard { coredb_uri, health_url, pnl_days }) => {
            tui::run(&coredb_uri, health_url, pnl_days).await?;
        }
        Some(Commands::ClobAuth {}) => {
            run_clob_auth().await?;
        }
        Some(Commands::UsdcApprove {
            rpc_url,
            amount,
            spender,
            usdc,
            send,
        }) => {
            run_usdc_approve(rpc_url, amount, spender, usdc, send).await?;
        }
        Some(Commands::Daemon {
            coredb_uri,
            backtest_every,
            compare_every,
            settle_every,
            no_execute,
            live,
            llms,
            health_port,
        }) => {
            let mut cfg = daemon::DaemonConfig::new(coredb_uri);
            cfg.backtest_every_secs = backtest_every;
            cfg.compare_every_secs = compare_every;
            cfg.settle_every_secs = settle_every;
            cfg.execute = !no_execute;
            cfg.live = live;
            cfg.llm_presets = llms;
            cfg.health_port = health_port;
            daemon::run(cfg).await?;
        }
        None => {
            // Default: interactive chat
            let config = Config::load()?;
            if let Some(prompt) = cli.prompt {
                run_task(config, cli.model, prompt).await?;
            } else {
                run_chat(config, cli.model, None).await?;
            }
        }
    }
    
    Ok(())
}

/// One-time setup: ask Polymarket's CLOB for an API key for the wallet
/// loaded from `POLYMARKET_PRIVATE_KEY` and print the issued
/// credentials. The handshake is an L1 (wallet signature) call; once
/// you have the creds you persist them yourself and feed them into
/// later L2 (HMAC) authenticated calls. Polymarket only mints the
/// secret once per key, so save the output before re-running.
async fn run_clob_auth() -> Result<()> {
    use alloy::signers::local::PrivateKeySigner;
    let raw = std::env::var("POLYMARKET_PRIVATE_KEY")
        .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY not set"))?;
    let stripped = raw.trim().trim_start_matches("0x");
    if stripped.len() != 64 {
        anyhow::bail!(
            "POLYMARKET_PRIVATE_KEY must be 32 bytes (64 hex chars); got {}",
            stripped.len()
        );
    }
    let bytes = hex::decode(stripped)?;
    let signer = PrivateKeySigner::from_slice(&bytes)?;
    println!("🔑 wallet: {}", signer.address());
    println!(
        "🌐 POST {}/auth/api-key (L1 EIP-712 ClobAuth)",
        execution::clob_auth::CLOB_BASE_URL
    );
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;
    let creds = execution::clob_auth::request_api_creds(&http, &signer).await?;
    println!();
    println!("✓ credentials issued — save these:");
    println!("    api_key:    {}", creds.api_key);
    println!("    secret:     {}", creds.secret);
    println!("    passphrase: {}", creds.passphrase);
    println!();
    println!("Suggested:");
    println!("    export POLYMARKET_CLOB_API_KEY={}", creds.api_key);
    println!("    export POLYMARKET_CLOB_SECRET={}", creds.secret);
    println!("    export POLYMARKET_CLOB_PASSPHRASE={}", creds.passphrase);
    Ok(())
}

/// Standalone wrapper around `data::user_channel::run`. Loads CLOB
/// credentials from the env vars `rust-agent clob-auth` prints, sets
/// up an ad-hoc shutdown channel wired to Ctrl+C, and runs until the
/// user interrupts.
async fn run_user_channel(coredb_uri: String) -> Result<()> {
    use std::sync::Arc;
    use tokio::signal;
    use tokio::sync::watch;
    let api_key = std::env::var("POLYMARKET_CLOB_API_KEY")
        .map_err(|_| anyhow::anyhow!("POLYMARKET_CLOB_API_KEY not set"))?;
    let secret = std::env::var("POLYMARKET_CLOB_SECRET")
        .map_err(|_| anyhow::anyhow!("POLYMARKET_CLOB_SECRET not set"))?;
    let passphrase = std::env::var("POLYMARKET_CLOB_PASSPHRASE")
        .map_err(|_| anyhow::anyhow!("POLYMARKET_CLOB_PASSPHRASE not set"))?;
    let creds = Arc::new(execution::clob_auth::ApiCreds {
        api_key,
        secret,
        passphrase,
    });
    // APPLY_FILLS=1 → connect to CoreDB and let the listener write
    // trade events back into the orders table. Default off; the
    // listener observes-only.
    let apply = matches!(std::env::var("APPLY_FILLS").as_deref(), Ok("1"));
    let (order_repo, pos_repo) = if apply {
        let db = coredb::CoreDb::connect(&coredb_uri).await?;
        let o = coredb::orders::OrderRepo::new(db.session()).await?;
        let p = coredb::orders::PositionRepo::new(db.session()).await?;
        (Some(Arc::new(o)), Some(Arc::new(p)))
    } else {
        (None, None)
    };
    let (tx, rx) = watch::channel(false);
    let listener = tokio::spawn(data::user_channel::run(creds, order_repo, pos_repo, rx));
    println!(
        "🔔 user-channel: subscribed (apply_fills={apply}). Ctrl+C to stop."
    );
    signal::ctrl_c().await?;
    let _ = tx.send(true);
    let _ = listener.await;
    println!("✓ user-channel: clean exit");
    Ok(())
}

async fn run_positions(coredb_uri: &str) -> Result<()> {
    use chrono::{DateTime, Utc};
    use coredb::orders::PositionRepo;
    use coredb::CoreDb;

    let db = CoreDb::connect(coredb_uri).await?;
    let repo = PositionRepo::new(db.session()).await?;
    let mut positions = repo.list_all().await?;
    // Newest first; everything else is alphabetical for stable
    // diffability across consecutive dumps.
    positions.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then_with(|| a.market_slug.cmp(&b.market_slug))
    });

    println!("📜 positions ({} open)", positions.len());
    if positions.is_empty() {
        return Ok(());
    }
    println!(
        "   {:<40} {:<5} {:>10} {:>10} {:>10}",
        "market_slug", "side", "size", "avg_price", "updated"
    );
    for p in &positions {
        let ts = DateTime::<Utc>::from_timestamp_millis(p.updated_at_ms)
            .map(|d| d.format("%H:%M:%S").to_string())
            .unwrap_or_default();
        println!(
            "   {:<40} {:<5} {:>10.2} {:>10.4} {:>10}",
            truncate(&p.market_slug, 40),
            p.side,
            p.size,
            p.avg_price,
            ts
        );
    }
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('…');
        out
    }
}

async fn run_usdc_approve(
    rpc_url: Option<String>,
    amount: Option<String>,
    spender: Option<String>,
    usdc: Option<String>,
    send: bool,
) -> Result<()> {
    use std::str::FromStr;
    use alloy::primitives::{Address, U256};
    use execution::usdc_approve::{
        run as approve_run, ApproveConfig, POLYGON_USDC_E, POLYMARKET_CTF_EXCHANGE,
    };

    let rpc_url = rpc_url
        .or_else(|| std::env::var("POLYGON_RPC_URL").ok())
        .ok_or_else(|| {
            anyhow::anyhow!("--rpc-url not passed and POLYGON_RPC_URL env not set")
        })?;
    let usdc_address = match usdc {
        Some(s) => Address::from_str(&s)?,
        None => Address::from_str(POLYGON_USDC_E)?,
    };
    let spender_address = match spender {
        Some(s) => Address::from_str(&s)?,
        None => Address::from_str(POLYMARKET_CTF_EXCHANGE)?,
    };
    let amount_value = match amount {
        Some(s) if s.eq_ignore_ascii_case("max") => U256::MAX,
        Some(s) => U256::from_str_radix(&s, 10)
            .map_err(|e| anyhow::anyhow!("parse --amount: {e}"))?,
        None => U256::MAX,
    };
    approve_run(ApproveConfig {
        rpc_url,
        usdc_address,
        spender: spender_address,
        amount: amount_value,
        send,
    })
    .await
}

async fn run_chat(config: Config, model: String, initial_prompt: Option<String>) -> Result<()> {
    println!("🦀 Rust Agent v0.1.0");
    let resolved = if model.is_empty() { config.model.clone() } else { model.clone() };
    println!("Model: {}", resolved);
    println!("Type 'exit' or Ctrl+C to quit\n");

    let mut engine = QueryEngine::new(config, model)?;
    
    // Handle initial prompt if provided
    if let Some(prompt) = initial_prompt {
        engine.process_input(&prompt).await?;
    }
    
    // Interactive loop
    loop {
        print!("> ");
        use std::io::Write;
        std::io::stdout().flush()?;
        
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        let input = input.trim();
        
        if input.is_empty() {
            continue;
        }
        
        if input == "exit" || input == "quit" {
            println!("Goodbye! 👋");
            break;
        }
        
        if let Err(e) = engine.process_input(input).await {
            eprintln!("Error: {}", e);
        }
        
        println!();
    }
    
    Ok(())
}

async fn run_task(config: Config, model: String, task: String) -> Result<()> {
    let mut engine = QueryEngine::new(config, model)?;
    engine.process_input(&task).await?;
    Ok(())
}

async fn run_stats(coredb_uri: String) -> Result<()> {
    let db = coredb::CoreDb::connect(&coredb_uri).await?;
    let counts = db.count_rows().await?;
    println!("📊 CoreDB polymarket_btc row counts:");
    for (t, n) in &counts {
        println!("   {t:<20} {n}");
    }
    let total: i64 = counts.iter().map(|(_, n)| n).sum();
    println!("   {:<20} {}", "Σ", total);
    Ok(())
}

async fn run_migrate(coredb_uri: String) -> Result<()> {
    println!("🗄️  Connecting to CoreDB at {coredb_uri} ...");
    let db = coredb::CoreDb::connect(&coredb_uri).await?;
    println!("📐 Applying polymarket_btc schema migrations ...");
    db.migrate().await?;
    println!("✓ Migrations applied. Keyspace `polymarket_btc` is ready.");
    println!("🔎 Verifying tables ...");
    let tables = db.verify_tables().await?;
    for t in &tables {
        println!("   ✓ polymarket_btc.{t}");
    }
    println!("✓ {} tables present.", tables.len());
    Ok(())
}

async fn run_serve(config: Config, model: String, host: String, port: u16) -> Result<()> {
    let addr: std::net::SocketAddr = format!("{host}:{port}").parse()?;
    let resolved_model = if model.is_empty() {
        config.model.clone()
    } else {
        model.clone()
    };
    println!("🦀 rust-agent serve");
    println!("  backend: {}", config.backend.label());
    println!("  model:   {}", resolved_model);
    println!("  open:    http://{addr}/");
    println!();
    let app = web::build_router(config, model);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn list_tools() {
    println!("📦 Available Tools:\n");
    println!("  Local:");
    println!("    bash               - Execute shell commands");
    println!("    file_read          - Read file contents");
    println!("    file_write         - Write to files");
    println!("    file_edit          - Edit files with search/replace");
    println!("    grep               - Search file contents");
    println!("    glob               - Find files by pattern");
    println!();
    println!("  pdf-kg (registered when PDFKG_BACKEND_URL is set or default :8088 reachable):");
    println!("    pdfkg_list_jobs    - Enumerate indexed PDFs");
    println!("    pdfkg_search       - Retrieve top-k graph nodes (no LLM)");
    println!("    pdfkg_ask          - End-to-end RAG with multimodal answer");
    println!("    pdfkg_get_page     - Read one page's chunks + image refs");
    println!("    pdfkg_get_image    - Image metadata + bytes_url");
    println!("    pdfkg_get_subgraph - Ego-subgraph traversal");
}

fn show_config(config: &Config) {
    println!("⚙️  Configuration:\n");
    println!("  Backend: {}", config.backend.label());
    match &config.backend {
        Backend::Anthropic { api_key } => {
            println!("  API Key: {}", masked(api_key));
        }
        Backend::OpenAICompat { api_key, base_url } => {
            println!("  Base URL: {base_url}");
            println!("  API Key: {}", masked(api_key));
        }
    }
    println!("  Model: {}", config.model);
    println!("  Max Tokens: {}", config.max_tokens);
    println!("  Tool Timeout: {:?}", config.tool_timeout);
}

fn masked(key: &str) -> String {
    if key.len() < 12 {
        // Short keys (e.g. "dummy") are clearly placeholders — show as-is.
        return key.to_string();
    }
    format!("{}...{}", &key[..8], &key[key.len() - 4..])
}
