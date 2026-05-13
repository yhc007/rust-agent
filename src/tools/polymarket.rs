//! Polymarket read-only tools.
//!
//! Each tool fetches fresh data from Polymarket Gamma / CLOB rather
//! than reading CoreDB, so the agent sees the latest state on every
//! call regardless of ingestion lag. CoreDB-backed views (historical
//! window queries, backtest replay) live elsewhere.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::tools::registry::{Tool, ToolContext, ToolError, ToolResult};

const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
const CLOB_BASE: &str = "https://clob.polymarket.com";

fn is_btc(slug: &str, question: &str) -> bool {
    let s = slug.to_ascii_lowercase();
    let q = question.to_ascii_lowercase();
    s.contains("bitcoin") || s.contains("btc") || q.contains("bitcoin") || q.contains("btc")
}

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("reqwest client")
}

// ===========================================================================
// polymarket_list_btc_markets
// ===========================================================================

#[derive(Deserialize, Default)]
struct ListInput {
    #[serde(default = "default_limit")]
    limit: u32,
}
fn default_limit() -> u32 {
    20
}

pub struct ListBtcMarketsTool {
    http: Client,
}

impl ListBtcMarketsTool {
    pub fn new() -> Self {
        Self { http: http_client() }
    }
}

#[async_trait]
impl Tool for ListBtcMarketsTool {
    fn name(&self) -> &str {
        "polymarket_list_btc_markets"
    }

    fn description(&self) -> &str {
        "List open Polymarket markets whose slug or question references Bitcoin/BTC. \
         Sorted by 24-hour USDC volume descending. Use this to discover which short-term \
         BTC markets are tradable right now. Returns slug, question, outcomes, last YES \
         price, end_date, 24h volume, and liquidity for each market. \
         Use polymarket_get_market for full details on a specific slug."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "description": "How many BTC markets to return (default 20)."
                }
            }
        })
    }

    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: ListInput = if input.is_null() {
            ListInput::default()
        } else {
            serde_json::from_value(input).map_err(|e| ToolError::InvalidInput(e.to_string()))?
        };
        let url = format!(
            "{GAMMA_BASE}/markets?active=true&closed=false\
             &order=volume24hr&ascending=false&limit=500"
        );
        let rows: Vec<Value> = self.http.get(url).send().await
            .and_then(|r| r.error_for_status())
            .map_err(|e| ToolError::ExecutionFailed(format!("gamma fetch: {e}")))?
            .json().await
            .map_err(|e| ToolError::ExecutionFailed(format!("gamma decode: {e}")))?;

        let mut out: Vec<Value> = Vec::new();
        for v in rows {
            let slug = v.get("slug").and_then(|x| x.as_str()).unwrap_or("");
            let question = v.get("question").and_then(|x| x.as_str()).unwrap_or("");
            if !is_btc(slug, question) { continue; }
            let yes_price = v.get("outcomePrices")
                .and_then(|x| x.as_str())
                .and_then(|s| serde_json::from_str::<Vec<String>>(s).ok())
                .and_then(|v| v.first().cloned())
                .and_then(|p| p.parse::<f64>().ok())
                .unwrap_or(0.0);
            out.push(json!({
                "slug": slug,
                "question": question,
                "outcomes": v.get("outcomes"),
                "yes_price": yes_price,
                "end_date": v.get("endDate"),
                "volume_24h": v.get("volume24hr"),
                "liquidity": v.get("liquidityNum"),
            }));
            if out.len() >= input.limit as usize { break; }
        }

        let body = serde_json::to_string_pretty(&out)
            .map_err(|e| ToolError::ExecutionFailed(format!("serialize: {e}")))?;
        Ok(ToolResult::success(body))
    }
}

// ===========================================================================
// polymarket_get_market
// ===========================================================================

#[derive(Deserialize)]
struct GetMarketInput {
    slug: String,
}

pub struct GetMarketTool {
    http: Client,
}

impl GetMarketTool {
    pub fn new() -> Self {
        Self { http: http_client() }
    }
}

#[async_trait]
impl Tool for GetMarketTool {
    fn name(&self) -> &str {
        "polymarket_get_market"
    }

    fn description(&self) -> &str {
        "Fetch full Polymarket Gamma metadata for one market by slug — outcomes, \
         outcomePrices, condition_id, clobTokenIds, end_date, liquidity, volume, \
         orderbook-enabled flag, etc. Use polymarket_list_btc_markets first to \
         discover slugs."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "slug": {"type": "string", "description": "Polymarket market slug"}
            },
            "required": ["slug"]
        })
    }

    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: GetMarketInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        let url = format!("{GAMMA_BASE}/markets?slug={}", urlencode(&input.slug));
        let rows: Vec<Value> = self.http.get(url).send().await
            .and_then(|r| r.error_for_status())
            .map_err(|e| ToolError::ExecutionFailed(format!("gamma fetch: {e}")))?
            .json().await
            .map_err(|e| ToolError::ExecutionFailed(format!("gamma decode: {e}")))?;
        let Some(m) = rows.into_iter().next() else {
            return Ok(ToolResult::error(format!("no market with slug `{}`", input.slug)));
        };
        let body = serde_json::to_string_pretty(&m)
            .map_err(|e| ToolError::ExecutionFailed(format!("serialize: {e}")))?;
        Ok(ToolResult::success(body))
    }
}

// ===========================================================================
// polymarket_get_orderbook
// ===========================================================================

#[derive(Deserialize)]
struct GetOrderbookInput {
    token_id: String,
}

pub struct GetOrderbookTool {
    http: Client,
}

impl GetOrderbookTool {
    pub fn new() -> Self {
        Self { http: http_client() }
    }
}

#[async_trait]
impl Tool for GetOrderbookTool {
    fn name(&self) -> &str {
        "polymarket_get_orderbook"
    }

    fn description(&self) -> &str {
        "Fetch the CLOB orderbook for ONE outcome of a Polymarket market. The \
         outcome is identified by its `token_id`, which is the per-outcome string \
         id from `clobTokenIds` on the market object. Returns bid/ask ladders. \
         Use this when you need depth, mid-price, or imbalance — list_markets only \
         gives you last_price."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "token_id": {
                    "type": "string",
                    "description": "ERC-1155 token id from Polymarket clobTokenIds"
                }
            },
            "required": ["token_id"]
        })
    }

    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: GetOrderbookInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        let url = format!("{CLOB_BASE}/book?token_id={}", urlencode(&input.token_id));
        let resp: Value = self.http.get(url).send().await
            .and_then(|r| r.error_for_status())
            .map_err(|e| ToolError::ExecutionFailed(format!("clob fetch: {e}")))?
            .json().await
            .map_err(|e| ToolError::ExecutionFailed(format!("clob decode: {e}")))?;
        let body = serde_json::to_string_pretty(&resp)
            .map_err(|e| ToolError::ExecutionFailed(format!("serialize: {e}")))?;
        Ok(ToolResult::success(body))
    }
}

// ===========================================================================
// helpers
// ===========================================================================

fn urlencode(s: &str) -> String {
    // Minimal: encode only the characters that break the path/query.
    // For slugs Polymarket already produces URL-safe text, but quote
    // anything weird just in case.
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '~' {
            out.push(c);
        } else {
            for b in c.to_string().bytes() {
                out.push_str(&format!("%{:02X}", b));
            }
        }
    }
    out
}

pub fn register(registry: &mut crate::tools::ToolRegistry) {
    registry.register(Box::new(ListBtcMarketsTool::new()));
    registry.register(Box::new(GetMarketTool::new()));
    registry.register(Box::new(GetOrderbookTool::new()));
}
