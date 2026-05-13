//! `record_decision` tool — the agent persists its trade decision JSON
//! into CoreDB's `decisions` table. Risk gate and execution tools read
//! from that table downstream.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::coredb::decisions::DecisionRepo;
use crate::coredb::types::{bucket_day, now_ms, Decision};
use crate::coredb::CoreDb;
use crate::tools::registry::{Tool, ToolContext, ToolError, ToolResult};

#[derive(Deserialize)]
struct RecordDecisionInput {
    market_slug: String,
    /// "YES", "NO", or "PASS".
    side: String,
    #[serde(default)]
    size_usd: f64,
    #[serde(default)]
    confidence: f64,
    #[serde(default)]
    edge_bps: i32,
    #[serde(default)]
    reasoning: String,
    /// Market's YES price at decision time. Optional; needed for the
    /// `compare-pnl` mark-to-market readout to include this row.
    #[serde(default)]
    entry_price: f64,
}

pub struct RecordDecisionTool {
    coredb_uri: String,
}

impl RecordDecisionTool {
    pub fn new() -> Self {
        Self {
            coredb_uri: std::env::var("COREDB_URI")
                .unwrap_or_else(|_| "127.0.0.1:9042".to_string()),
        }
    }
}

#[async_trait]
impl Tool for RecordDecisionTool {
    fn name(&self) -> &str {
        "record_decision"
    }

    fn description(&self) -> &str {
        "Persist a trade decision into CoreDB's `decisions` audit table. \
         Call this AFTER you have inspected the market and BTC quote, and \
         only when you're emitting a final actionable view. `side` must \
         be one of YES / NO / PASS. PASS still gets recorded so the audit \
         log captures every market you considered. Returns the generated \
         decision_id (UUID) — keep it if you later call place_order."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "market_slug":  {"type": "string"},
                "side":         {"type": "string", "enum": ["YES", "NO", "PASS"]},
                "size_usd":     {"type": "number", "minimum": 0,
                                 "description": "Desired notional; 0 for PASS"},
                "confidence":   {"type": "number", "minimum": 0, "maximum": 1},
                "edge_bps":     {"type": "integer",
                                 "description": "Estimated edge over market price, in bps"},
                "reasoning":    {"type": "string",
                                 "description": "One-paragraph rationale"},
                "entry_price":  {"type": "number", "minimum": 0, "maximum": 1,
                                 "description": "YES price observed at decision time; lets compare-pnl mark this row to market"}
            },
            "required": ["market_slug", "side"]
        })
    }

    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let raw_response = input.to_string();
        let input: RecordDecisionInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        if !matches!(input.side.as_str(), "YES" | "NO" | "PASS") {
            return Err(ToolError::InvalidInput(format!(
                "side must be YES|NO|PASS, got `{}`",
                input.side
            )));
        }

        let db = CoreDb::connect(&self.coredb_uri).await
            .map_err(|e| ToolError::ExecutionFailed(format!("coredb connect: {e}")))?;
        let repo = DecisionRepo::new(db.session()).await
            .map_err(|e| ToolError::ExecutionFailed(format!("decisions repo: {e}")))?;

        let ts = now_ms();
        let decision = Decision {
            bucket_day_ms: bucket_day(ts),
            ts_ms: ts,
            decision_id: Uuid::new_v4(),
            market_slug: input.market_slug.clone(),
            side: input.side.clone(),
            size_usd: input.size_usd,
            confidence: input.confidence,
            edge_bps: input.edge_bps,
            reasoning: input.reasoning,
            raw_response,
            entry_price: input.entry_price,
        };
        repo.insert(&decision).await
            .map_err(|e| ToolError::ExecutionFailed(format!("decisions insert: {e}")))?;

        let body = json!({
            "decision_id": decision.decision_id.to_string(),
            "market_slug": decision.market_slug,
            "side":        decision.side,
            "size_usd":    decision.size_usd,
            "stored_at":   decision.ts_ms,
        });
        Ok(ToolResult::success(body.to_string()))
    }
}

pub fn register(registry: &mut crate::tools::ToolRegistry) {
    registry.register(Box::new(RecordDecisionTool::new()));
}
