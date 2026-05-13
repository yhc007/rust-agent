//! Configuration module
//!
//! Selects which LLM backend the QueryEngine talks to:
//! - `AGENT_BACKEND=anthropic` (default) → `ANTHROPIC_API_KEY`
//! - `AGENT_BACKEND=openai`              → `OPENAI_API_KEY` + `OPENAI_BASE_URL` + `OPENAI_MODEL`
//! - `AGENT_BACKEND=deepseek`            → `DEEPSEEK_API_KEY` (or `OPENAI_API_KEY` as fallback);
//!                                         base_url defaults to `https://api.deepseek.com/v1`
//!                                         and model to `deepseek-chat`. `deepseek-reasoner` is
//!                                         also fine for text-only runs but its tool-use story
//!                                         is incomplete — keep `deepseek-chat` for the agent loop.
//!
//! Why a single Config carries every flavor: keeps `Config::load`
//! the one place that touches `std::env`, and keeps `main.rs` from
//! caring which backend is in play. The engine builds its
//! `Box<dyn ApiClient>` from this enum.

use std::time::Duration;

use anyhow::{Context, Result};

/// Which LLM backend the engine talks to. Anthropic is the
/// historical default; OpenAI-compat was added to support vLLM
/// (Nemotron etc.) and any other server speaking the same wire
/// format.
#[derive(Debug, Clone)]
pub enum Backend {
    Anthropic {
        api_key: String,
    },
    /// Generic OpenAI-compatible endpoint. The `base_url` is the part
    /// before `/chat/completions` (e.g. `http://localhost:8000/v1`).
    /// `api_key` may be a dummy value when the server doesn't
    /// authenticate — vLLM accepts any non-empty bearer.
    OpenAICompat {
        api_key: String,
        base_url: String,
    },
}

impl Backend {
    /// Stable label for logs and the `tools` CLI subcommand.
    pub fn label(&self) -> &'static str {
        match self {
            Backend::Anthropic { .. } => "anthropic",
            Backend::OpenAICompat { base_url, .. } if base_url.contains("api.openai.com") => {
                "openai"
            }
            Backend::OpenAICompat { base_url, .. } if base_url.contains("api.deepseek.com") => {
                "deepseek"
            }
            Backend::OpenAICompat { .. } => "openai-compat",
        }
    }
}

/// Agent configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub backend: Backend,
    pub model: String,
    pub max_tokens: u32,
    pub max_turns: u32,
    pub tool_timeout: Duration,
    pub token_budget: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            backend: Backend::Anthropic {
                api_key: String::new(),
            },
            model: default_model_for_anthropic(),
            max_tokens: 16384,
            max_turns: 200,
            tool_timeout: Duration::from_secs(120),
            token_budget: 100000,
        }
    }
}

fn default_model_for_anthropic() -> String {
    "claude-sonnet-4-20250514".to_string()
}

/// Read an API key from a dotfile under `$HOME`. Returns `None` when
/// the file is missing, unreadable, or empty. We trim whitespace so a
/// trailing newline (the typical `echo "$KEY" > ~/.deepseek` shape)
/// doesn't get sent as part of the key.
fn read_key_file(filename: &str) -> Option<String> {
    let home = std::env::var("HOME").ok()?;
    let path = std::path::Path::new(&home).join(filename);
    let raw = std::fs::read_to_string(path).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Resolve a DeepSeek API key from the supported sources in priority
/// order: `DEEPSEEK_API_KEY` env var, then `~/.deepseek`.
fn deepseek_key() -> Option<String> {
    std::env::var("DEEPSEEK_API_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| read_key_file(".deepseek"))
}

/// Pick a backend name when `AGENT_BACKEND` is unset. Preserves the old
/// "anthropic by default" behavior whenever `ANTHROPIC_API_KEY` is in
/// the env, so existing setups don't silently flip backends. Falls
/// through to deepseek when only a DeepSeek key is reachable — natural
/// for the polymarket-btc project, which is deepseek-first.
fn detect_default_backend() -> &'static str {
    if std::env::var("ANTHROPIC_API_KEY")
        .ok()
        .filter(|s| !s.is_empty())
        .is_some()
    {
        return "anthropic";
    }
    if deepseek_key().is_some() {
        return "deepseek";
    }
    // No key in sight — keep the historical default so the resulting
    // error message ("ANTHROPIC_API_KEY not set") matches what users
    // who haven't configured anything used to see.
    "anthropic"
}

impl Config {
    /// Load configuration from environment. Backend selection
    /// happens here so the rest of the codebase doesn't have to
    /// re-read env vars.
    pub fn load() -> Result<Self> {
        let backend_name = std::env::var("AGENT_BACKEND")
            .unwrap_or_else(|_| detect_default_backend().to_string());
        let (backend, default_model) = match backend_name.to_ascii_lowercase().as_str() {
            "anthropic" => {
                let api_key = std::env::var("ANTHROPIC_API_KEY")
                    .context("ANTHROPIC_API_KEY environment variable not set")?;
                (
                    Backend::Anthropic { api_key },
                    default_model_for_anthropic(),
                )
            }
            "openai" | "openai_compat" | "openai-compat" | "vllm" => {
                let api_key = std::env::var("OPENAI_API_KEY").context(
                    "OPENAI_API_KEY not set (use 'dummy' if your server doesn't authenticate)",
                )?;
                let base_url = std::env::var("OPENAI_BASE_URL").context(
                    "OPENAI_BASE_URL not set — example: http://localhost:8000/v1",
                )?;
                // Auto-detection (`GET /v1/models`) was considered but
                // rejected: Config::load runs inside #[tokio::main]
                // already, and reqwest::blocking panics when its
                // runtime is dropped inside an async context. A pure
                // std TcpStream probe would work but adds tedious
                // manual HTTP parsing for a single call. Requiring
                // OPENAI_MODEL is simpler and more explicit; we
                // surface a clear error when it's missing.
                let model = std::env::var("OPENAI_MODEL").context(
                    "OPENAI_MODEL not set — pick a model id from your server's /v1/models",
                )?;
                (Backend::OpenAICompat { api_key, base_url }, model)
            }
            "deepseek" => {
                // DeepSeek speaks the OpenAI chat-completions wire format,
                // so we route through the same OpenAICompat client. Only
                // the env-var lookup and defaults differ.
                let api_key = deepseek_key()
                    .or_else(|| std::env::var("OPENAI_API_KEY").ok())
                    .context(
                        "DEEPSEEK_API_KEY not set (also tried ~/.deepseek and OPENAI_API_KEY)",
                    )?;
                let base_url = std::env::var("OPENAI_BASE_URL")
                    .unwrap_or_else(|_| "https://api.deepseek.com/v1".to_string());
                let model = std::env::var("OPENAI_MODEL")
                    .or_else(|_| std::env::var("DEEPSEEK_MODEL"))
                    .unwrap_or_else(|_| "deepseek-chat".to_string());
                (Backend::OpenAICompat { api_key, base_url }, model)
            }
            other => {
                anyhow::bail!(
                    "AGENT_BACKEND={other:?} not recognized; use 'anthropic', 'openai', or 'deepseek'"
                );
            }
        };

        let model = std::env::var("RUST_AGENT_MODEL").unwrap_or(default_model);
        let max_tokens = std::env::var("RUST_AGENT_MAX_TOKENS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(16384);
        let tool_timeout_secs = std::env::var("RUST_AGENT_TOOL_TIMEOUT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120);

        Ok(Self {
            backend,
            model,
            max_tokens,
            tool_timeout: Duration::from_secs(tool_timeout_secs),
            ..Default::default()
        })
    }
}

