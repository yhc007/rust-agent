//! Thin HTTP client for the pdf-kg backend's `/api/tools/*` surface.
//!
//! The remote tool catalog and invocation contract are documented in
//! `docs/tools-and-mcp.md` of the pdf-kg repo. We talk REST instead of
//! MCP here because (a) it's simpler — no JSON-RPC envelope and no
//! capability handshake to manage, and (b) rust-agent's `Tool` trait
//! already shapes our locally-registered surface; MCP would be a
//! second indirection on top.

use std::time::Duration;

use reqwest::Client;
use serde_json::Value;

const DEFAULT_BACKEND: &str = "http://localhost:8088";
/// Tool calls can include a synthesizer round trip (`ask_pdf`) which
/// on CPU vLLM can take 30–60s. The agent loop itself doesn't enforce
/// a per-tool timeout, so we cap here.
const DEFAULT_TIMEOUT_SECS: u64 = 180;

/// Cheap to clone — wraps a `reqwest::Client` and a base URL.
#[derive(Debug, Clone)]
pub struct PdfKgClient {
    base: String,
    http: Client,
}

impl PdfKgClient {
    /// Build a client pointing at `base` (no trailing slash). For most
    /// callers [`from_env`] is the right entry point.
    pub fn new(base: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .build()
            .expect("reqwest client");
        Self {
            base: base.into().trim_end_matches('/').to_string(),
            http,
        }
    }

    /// Resolve the backend URL from `PDFKG_BACKEND_URL`, falling back
    /// to `http://localhost:8088` for the local dev setup. Returns
    /// `None` only when the caller explicitly disables integration
    /// via `PDFKG_BACKEND_URL=disabled` — useful for environments
    /// where pdf-kg isn't deployed and we don't want failing tools
    /// cluttering the catalog.
    pub fn from_env() -> Option<Self> {
        match std::env::var("PDFKG_BACKEND_URL") {
            Ok(v) if v.eq_ignore_ascii_case("disabled") || v.is_empty() => None,
            Ok(v) => Some(Self::new(v)),
            Err(_) => Some(Self::new(DEFAULT_BACKEND)),
        }
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// Invoke `POST /api/tools/{name}/invoke` with `arguments` as the
    /// body. Returns the tool's raw JSON response on success.
    ///
    /// Error shape from pdf-kg (HTTP 4xx/5xx):
    /// ```json
    /// {"error":"bad_input"|"tool_not_found"|"resource_not_found"|"internal",
    ///  "message":"..."}
    /// ```
    /// We surface that message verbatim so the agent's LLM can read
    /// what went wrong and decide whether to retry.
    pub async fn invoke(
        &self,
        tool: &str,
        arguments: Value,
    ) -> Result<Value, PdfKgError> {
        let url = format!("{}/api/tools/{tool}/invoke", self.base);
        let resp = self
            .http
            .post(&url)
            .json(&arguments)
            .send()
            .await
            .map_err(|e| PdfKgError::Transport(e.to_string()))?;
        let status = resp.status();
        if status.is_success() {
            resp.json::<Value>()
                .await
                .map_err(|e| PdfKgError::Decode(e.to_string()))
        } else {
            // Try to decode the structured error; fall back to raw
            // text when the backend returned something unexpected.
            let body = resp.text().await.unwrap_or_default();
            let parsed: Option<Value> = serde_json::from_str(&body).ok();
            let message = parsed
                .as_ref()
                .and_then(|v| v.get("message"))
                .and_then(|m| m.as_str())
                .map(str::to_string)
                .unwrap_or_else(|| body.clone());
            let kind = parsed
                .as_ref()
                .and_then(|v| v.get("error"))
                .and_then(|e| e.as_str())
                .unwrap_or("http_error");
            Err(PdfKgError::Remote {
                status: status.as_u16(),
                kind: kind.to_string(),
                message,
            })
        }
    }
}

/// Errors surfaced by [`PdfKgClient::invoke`]. The agent's `Tool`
/// implementations translate these into the appropriate
/// `ToolResult`/`ToolError` variant.
#[derive(Debug, thiserror::Error)]
pub enum PdfKgError {
    #[error("transport: {0}")]
    Transport(String),

    #[error("decode response: {0}")]
    Decode(String),

    #[error("pdf-kg {kind} ({status}): {message}")]
    Remote {
        status: u16,
        kind: String,
        message: String,
    },
}

impl PdfKgError {
    /// Whether this error should be reported to the LLM as a soft
    /// failure (model can retry with different args) or hard
    /// failure (system/config — bail the loop).
    pub fn is_soft(&self) -> bool {
        match self {
            // Bad input: model can adjust args.
            PdfKgError::Remote { kind, .. } if kind == "bad_input" => true,
            // Resource missing: model can try a different job_id /
            // image_id / page_no.
            PdfKgError::Remote { kind, .. } if kind == "resource_not_found" => true,
            // Transport / decode / 5xx: the agent shouldn't ask the
            // model to fix our infrastructure. Hard fail.
            _ => false,
        }
    }
}
