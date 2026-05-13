//! Paper executor. Generates a fake fill at the supplied price.
//!
//! Translates `size_usd` → outcome-token quantity by dividing by the
//! price. Polymarket outcome prices live in [0, 1] (probability), so
//! `size_token = size_usd / price`.

use async_trait::async_trait;
use uuid::Uuid;

use super::{ExecError, Executor, FillResult, PlaceOrderRequest};

pub struct PaperExec;

#[async_trait]
impl Executor for PaperExec {
    fn label(&self) -> &'static str {
        "paper"
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> Result<FillResult, ExecError> {
        if req.price <= 0.0 {
            return Err(ExecError::Paper(
                "price must be > 0 to compute paper fill size".into(),
            ));
        }
        let size_token = req.size_usd / req.price;
        Ok(FillResult {
            order_id: format!("paper-{}", Uuid::new_v4()),
            fill_size: size_token,
            fill_price: req.price,
            status: "filled".to_string(),
        })
    }
}
