//! `place_order` tool — agent-callable. Runs through the Risk Gate,
//! then the paper executor, and records the resulting order + position
//! in CoreDB.

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::coredb::orders::{OrderRepo, PositionRepo};
use crate::coredb::types::{bucket_day, now_ms, Order, Position};
use crate::coredb::CoreDb;
use crate::execution::paper::PaperExec;
use crate::execution::{Executor, PlaceOrderRequest};
use crate::risk::{evaluate, OrderRequest, RiskLimits, RiskVerdict};
use crate::tools::registry::{Tool, ToolContext, ToolError, ToolResult};

#[derive(Deserialize)]
struct PlaceOrderInput {
    market_slug: String,
    side: String,
    size_usd: f64,
    price: f64,
    #[serde(default)]
    decision_id: Option<String>,
}

pub struct PlaceOrderTool {
    coredb_uri: String,
    exec: PaperExec,
    limits: RiskLimits,
}

impl PlaceOrderTool {
    pub fn new() -> Self {
        Self {
            coredb_uri: std::env::var("COREDB_URI")
                .unwrap_or_else(|_| "127.0.0.1:9042".to_string()),
            exec: PaperExec,
            limits: RiskLimits::default(),
        }
    }
}

#[async_trait]
impl Tool for PlaceOrderTool {
    fn name(&self) -> &str {
        "place_order"
    }

    fn description(&self) -> &str {
        "Place a Polymarket paper-trading order. The order is first \
         checked by the deterministic Risk Gate (single-order size cap, \
         kill switch, side/price sanity). If accepted, a paper fill is \
         simulated at the supplied price, and both the order and an \
         updated position are written into CoreDB. Use AFTER \
         record_decision; pass the decision_id back here for the audit \
         trail. Rejected orders return is_error=true with the reason."
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "market_slug": {"type": "string"},
                "side":        {"type": "string", "enum": ["YES", "NO"]},
                "size_usd":    {"type": "number", "minimum": 0,
                                "description": "Notional USD; subject to RISK_MAX_ORDER_USD"},
                "price":       {"type": "number", "minimum": 0, "maximum": 1,
                                "description": "Expected fill price (Polymarket outcome 0-1)"},
                "decision_id": {"type": "string",
                                "description": "Optional UUID from record_decision"}
            },
            "required": ["market_slug", "side", "size_usd", "price"]
        })
    }

    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: PlaceOrderInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        let decision_id = match input.decision_id.as_deref() {
            Some(s) => Uuid::parse_str(s)
                .map_err(|e| ToolError::InvalidInput(format!("decision_id: {e}")))?,
            None => Uuid::nil(),
        };

        let req = OrderRequest {
            market_slug: input.market_slug.clone(),
            side: input.side.clone(),
            size_usd: input.size_usd,
            price: input.price,
        };
        if let RiskVerdict::Block(reason) = evaluate(&req, &self.limits) {
            return Ok(ToolResult::error(format!("risk gate blocked: {reason}")));
        }

        let fill = self
            .exec
            .place_order(PlaceOrderRequest {
                market_slug: req.market_slug.clone(),
                side: req.side.clone(),
                size_usd: req.size_usd,
                price: req.price,
            })
            .await
            .map_err(|e| ToolError::ExecutionFailed(format!("paper exec: {e}")))?;

        let db = CoreDb::connect(&self.coredb_uri).await
            .map_err(|e| ToolError::ExecutionFailed(format!("coredb connect: {e}")))?;
        let order_repo = OrderRepo::new(db.session()).await
            .map_err(|e| ToolError::ExecutionFailed(format!("order repo: {e}")))?;
        let pos_repo = PositionRepo::new(db.session()).await
            .map_err(|e| ToolError::ExecutionFailed(format!("position repo: {e}")))?;

        let ts = now_ms();
        let order = Order {
            bucket_day_ms: bucket_day(ts),
            ts_ms: ts,
            order_id: fill.order_id.clone(),
            decision_id,
            market_slug: req.market_slug.clone(),
            side: req.side.clone(),
            size: fill.fill_size,
            price: req.price,
            status: fill.status.clone(),
            fill_size: fill.fill_size,
            fill_price: fill.fill_price,
        };
        order_repo.insert(&order).await
            .map_err(|e| ToolError::ExecutionFailed(format!("orders insert: {e}")))?;

        // Naive position tracking: overwrite. A proper averaging
        // implementation needs a read+write inside an LWT, which we
        // can't do today (CoreDB SELECT type-deserialization is
        // brittle). This is good enough for paper trading audit.
        let pos = Position {
            market_slug: req.market_slug.clone(),
            side: req.side.clone(),
            size: fill.fill_size,
            avg_price: fill.fill_price,
            updated_at_ms: ts,
        };
        pos_repo.upsert(&pos).await
            .map_err(|e| ToolError::ExecutionFailed(format!("positions upsert: {e}")))?;

        let body = json!({
            "order_id":    fill.order_id,
            "status":      fill.status,
            "fill_size":   fill.fill_size,
            "fill_price":  fill.fill_price,
            "market_slug": req.market_slug,
            "side":        req.side,
        });
        Ok(ToolResult::success(body.to_string()))
    }
}

pub fn register(registry: &mut crate::tools::ToolRegistry) {
    registry.register(Box::new(PlaceOrderTool::new()));
}
