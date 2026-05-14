//! Order execution layer.
//!
//! `PaperExec` is the default — fills any allowed order at the supplied
//! `price`. `LiveExec` (Phase 5 skeleton) takes the same trait but signs
//! and submits to Polymarket's CLOB. Same Executor trait so the rest of
//! the codebase doesn't change when you flip paper → live.

pub mod auto;
pub mod live;
pub mod paper;

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
}
