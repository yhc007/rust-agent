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
    /// by default — see `Config::load`).
    Backtest {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
        /// Use the LLM-driven decision path instead of the baseline rule.
        #[arg(long)]
        llm: bool,
    },
    /// Compare baseline vs LLM strategy PnL on today's decisions, marked
    /// to the current Polymarket YES prices. Reads `polymarket_btc.decisions`
    /// and prints aggregates + per-market disagreements.
    ComparePnl {
        #[arg(long, default_value = "127.0.0.1:9042")]
        coredb_uri: String,
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
        Some(Commands::Backtest { coredb_uri, llm }) => {
            let mode = if llm {
                backtest::BacktestMode::Llm
            } else {
                backtest::BacktestMode::Baseline
            };
            backtest::run::run(&coredb_uri, mode).await?;
        }
        Some(Commands::ComparePnl { coredb_uri }) => {
            backtest::compare::run(&coredb_uri).await?;
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
