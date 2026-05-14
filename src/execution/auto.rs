//! Programmatic decision → order routing.
//!
//! `route_decision` is the non-tool counterpart of
//! [`crate::tools::place_order::PlaceOrderTool`]: same risk-gate +
//! paper-executor + CoreDB persistence pipeline, but callable directly
//! from Rust (i.e. from `backtest::run` when `--execute` is set), so we
//! don't have to spin up an LLM-shaped Tool just to translate a
//! Decision into an Order. The tool stays for agent-driven flows.
//!
//! PASS decisions are skipped here, not blocked — they never reach the
//! risk gate. Real risk rejections come back as
//! [`Outcome::Blocked`] with the underlying gate reason so the caller
//! can summarise.

use anyhow::Result;

use crate::coredb::orders::{OrderRepo, PositionRepo};
use crate::coredb::types::{bucket_day, now_ms, Decision, Order, Position};
use crate::execution::{Executor, PlaceOrderRequest};
use crate::risk::{evaluate, OrderRequest, RiskLimits, RiskVerdict};

#[derive(Debug)]
pub enum Outcome {
    /// Decision routed all the way through; `Order` row + position
    /// upsert are already persisted in CoreDB.
    Filled(Order),
    /// PASS / zero-size / unrecognised side — never went to the gate.
    Skipped(&'static str),
    /// Risk gate rejected the order. No persistence occurred.
    Blocked(String),
    /// Paper executor or the database write failed after risk passed.
    /// The router does not retry; the caller decides what to do (we
    /// log and continue in `backtest::run`).
    ExecError(String),
}

/// Route one Decision through the executor stack. The price used for
/// the fill is `decision.entry_price` for YES, `1 - entry_price` for
/// NO, because Polymarket sells NO shares at `1 - yes_price`. We do
/// NOT re-query Polymarket here — staying consistent with the price
/// the decision was made at is what makes paper PnL comparable across
/// strategies. Live executors may want to re-quote internally before
/// submitting.
///
/// `exec` is a trait object so the same routing pipeline drives both
/// paper trading (`PaperExec`) and live trading (`LiveExec`) — the
/// risk gate + kill switch fire identically in both modes.
pub async fn route_decision(
    decision: &Decision,
    exec: &dyn Executor,
    limits: &RiskLimits,
    order_repo: &OrderRepo,
    pos_repo: &PositionRepo,
) -> Result<Outcome> {
    match decision.side.as_str() {
        "PASS" => return Ok(Outcome::Skipped("PASS")),
        "YES" | "NO" => {}
        _ => return Ok(Outcome::Skipped("unrecognized side")),
    }
    if decision.size_usd <= 0.0 {
        return Ok(Outcome::Skipped("zero size"));
    }
    if decision.entry_price <= 0.0 || decision.entry_price >= 1.0 {
        return Ok(Outcome::Skipped("degenerate entry price"));
    }

    let price_for_side = if decision.side == "YES" {
        decision.entry_price
    } else {
        1.0 - decision.entry_price
    };

    let req = OrderRequest {
        market_slug: decision.market_slug.clone(),
        side: decision.side.clone(),
        size_usd: decision.size_usd,
        price: price_for_side,
    };
    if let RiskVerdict::Block(reason) = evaluate(&req, limits) {
        return Ok(Outcome::Blocked(reason));
    }

    let fill = match exec
        .place_order(PlaceOrderRequest {
            market_slug: req.market_slug.clone(),
            side: req.side.clone(),
            size_usd: req.size_usd,
            price: req.price,
        })
        .await
    {
        Ok(f) => f,
        Err(e) => return Ok(Outcome::ExecError(format!("paper exec: {e}"))),
    };

    let ts = now_ms();
    let order = Order {
        bucket_day_ms: bucket_day(ts),
        ts_ms: ts,
        order_id: fill.order_id.clone(),
        decision_id: decision.decision_id,
        market_slug: req.market_slug.clone(),
        side: req.side.clone(),
        size: fill.fill_size,
        price: req.price,
        status: fill.status.clone(),
        fill_size: fill.fill_size,
        fill_price: fill.fill_price,
    };
    if let Err(e) = order_repo.insert(&order).await {
        return Ok(Outcome::ExecError(format!("orders.insert: {e}")));
    }

    // Naive position tracking: each fill overwrites the previous row
    // for this market — same approach as PlaceOrderTool. A proper
    // average-price implementation needs a read-modify-write inside an
    // LWT, which CoreDB's protocol layer can't reliably round-trip
    // today. The decisions / orders tables retain the audit trail.
    let pos = Position {
        market_slug: req.market_slug.clone(),
        side: req.side.clone(),
        size: fill.fill_size,
        avg_price: fill.fill_price,
        updated_at_ms: ts,
    };
    if let Err(e) = pos_repo.upsert(&pos).await {
        return Ok(Outcome::ExecError(format!("positions.upsert: {e}")));
    }

    Ok(Outcome::Filled(order))
}
