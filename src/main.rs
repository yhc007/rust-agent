//! Rust Agent CLI
//! 
//! A terminal-based AI agent powered by Claude.

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{fmt, EnvFilter};

mod api;
mod config;
mod engine;
mod tools;
mod memory;
mod tui;

use engine::QueryEngine;
use config::Config;

#[derive(Parser)]
#[command(name = "rust-agent")]
#[command(author = "Paul Yu")]
#[command(version = "0.1.0")]
#[command(about = "🦀 Rust-based AI Agent", long_about = None)]
struct Cli {
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    
    /// Model to use
    #[arg(short, long, default_value = "claude-sonnet-4-20250514")]
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
    
    // Load config
    let config = Config::load()?;
    
    // Handle commands
    match cli.command {
        Some(Commands::Chat { prompt }) => {
            run_chat(config, cli.model, prompt).await?;
        }
        Some(Commands::Run { task }) => {
            run_task(config, cli.model, task).await?;
        }
        Some(Commands::Tools) => {
            list_tools();
        }
        Some(Commands::Config) => {
            show_config(&config);
        }
        None => {
            // Default: interactive chat
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
    println!("Model: {}", model);
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

fn list_tools() {
    println!("📦 Available Tools:\n");
    println!("  bash        - Execute shell commands");
    println!("  file_read   - Read file contents");
    println!("  file_write  - Write to files");
    println!("  file_edit   - Edit files with search/replace");
    println!("  grep        - Search file contents");
    println!("  glob        - Find files by pattern");
}

fn show_config(config: &Config) {
    println!("⚙️  Configuration:\n");
    println!("  API Key: {}...{}", 
        &config.api_key[..8], 
        &config.api_key[config.api_key.len()-4..]);
    println!("  Max Tokens: {}", config.max_tokens);
    println!("  Tool Timeout: {:?}", config.tool_timeout);
}
