//! Order execution layer.
//!
//! `PaperExec` is the default — fills any allowed order at the supplied
//! `price`. `LiveExec` (Phase 5 skeleton) takes the same trait but signs
//! and submits to Polymarket's CLOB. Same Executor trait so the rest of
//! the codebase doesn't change when you flip paper → live.

pub mod auto;
pub mod clob_auth;
pub mod live;
pub mod paper;
pub mod usdc_approve;

use async_trait::async_trait;
use thiserror::Error;

pub struct PlaceOrderRequest {
    pub market_slug: String,
    pub side: String,
    pub size_usd: f64,
    pub price: f64,
}

#[derive(Debug, Clone)]
pub struct FillResult {
    pub order_id: String,
    pub fill_size: f64,
    pub fill_price: f64,
    pub status: String, // "filled" | "rejected" | "pending"
}

#[derive(Debug, Error)]
pub enum ExecError {
    #[error("paper exec: {0}")]
    Paper(String),

    #[error("live exec: {0}")]
    Live(String),
}

#[async_trait]
pub trait Executor: Send + Sync {
    fn label(&self) -> &'static str;
    async fn place_order(&self, req: PlaceOrderRequest) -> Result<FillResult, ExecError>;

    /// Whether the caller (`execution::auto::route_decision`) should
    /// skip its optimistic `positions_v2::apply_fill` because this
    /// executor's fills will be reported separately via the
    /// user-channel WS. Paper executors return `false` (no WS, route
    /// is the only place writing positions); live executors return
    /// `true` so the same fill doesn't get counted twice — once
    /// optimistically from the POST /order response and a second time
    /// from the WS trade event.
    fn defers_positions_to_ws(&self) -> bool {
        false
    }
}
