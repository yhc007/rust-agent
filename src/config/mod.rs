//! Configuration module

use std::time::Duration;
use anyhow::{Result, Context};

/// Agent configuration
#[derive(Debug, Clone)]
pub struct Config {
    /// Anthropic API key
    pub api_key: String,
    /// Default model
    pub model: String,
    /// Max tokens for response
    pub max_tokens: u32,
    /// Max turns in conversation
    pub max_turns: u32,
    /// Tool execution timeout
    pub tool_timeout: Duration,
    /// Token budget for context
    pub token_budget: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            model: "claude-sonnet-4-20250514".to_string(),
            max_tokens: 16384,
            max_turns: 200,
            tool_timeout: Duration::from_secs(120),
            token_budget: 100000,
        }
    }
}

impl Config {
    /// Load configuration from environment
    pub fn load() -> Result<Self> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .context("ANTHROPIC_API_KEY environment variable not set")?;
        
        let max_tokens = std::env::var("RUST_AGENT_MAX_TOKENS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16384);
        
        let tool_timeout_secs = std::env::var("RUST_AGENT_TOOL_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120);
        
        Ok(Self {
            api_key,
            max_tokens,
            tool_timeout: Duration::from_secs(tool_timeout_secs),
            ..Default::default()
        })
    }
}
