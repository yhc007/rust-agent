//! BTC spot-quote tools (Binance public REST).

use std::time::Duration;

use async_trait::async_trait;
use reqwest::Client;
use serde_json::{json, Value};

use crate::tools::registry::{Tool, ToolContext, ToolError, ToolResult};

const BINANCE_BASE: &str = "https://api.binance.com/api/v3";

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("reqwest client")
}

pub struct BtcGetQuoteTool {
    http: Client,
}

impl BtcGetQuoteTool {
    pub fn new() -> Self {
        Self { http: http_client() }
    }
}

#[async_trait]
impl Tool for BtcGetQuoteTool {
    fn name(&self) -> &str {
        "btc_get_quote"
    }

    fn description(&self) -> &str {
        "Fetch the current BTC spot quote from Binance: last price, best bid/ask, \
         24-hour price change percent, 24-hour volume in BTC. Use this to ground \
         any Polymarket BTC-price prediction in the actual spot market. The number \
         the market is asking 'will BTC be above X by date Y' has to be compared \
         against this."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "symbol": {
                    "type": "string",
                    "description": "Binance spot symbol (default BTCUSDT)."
                }
            }
        })
    }

    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let symbol = input.get("symbol")
            .and_then(|x| x.as_str())
            .unwrap_or("BTCUSDT")
            .to_uppercase();
        let url = format!("{BINANCE_BASE}/ticker/24hr?symbol={symbol}");
        let r: Value = self.http.get(url).send().await
            .and_then(|r| r.error_for_status())
            .map_err(|e| ToolError::ExecutionFailed(format!("binance fetch: {e}")))?
            .json().await
            .map_err(|e| ToolError::ExecutionFailed(format!("binance decode: {e}")))?;
        // Pick the fields the model actually needs.
        let view = json!({
            "symbol": r.get("symbol"),
            "last_price": r.get("lastPrice"),
            "bid": r.get("bidPrice"),
            "ask": r.get("askPrice"),
            "price_change_pct_24h": r.get("priceChangePercent"),
            "volume_24h_btc": r.get("volume"),
            "high_24h": r.get("highPrice"),
            "low_24h": r.get("lowPrice"),
        });
        let body = serde_json::to_string_pretty(&view)
            .map_err(|e| ToolError::ExecutionFailed(format!("serialize: {e}")))?;
        Ok(ToolResult::success(body))
    }
}

pub fn register(registry: &mut crate::tools::ToolRegistry) {
    registry.register(Box::new(BtcGetQuoteTool::new()));
}
