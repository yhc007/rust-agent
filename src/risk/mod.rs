//! Deterministic risk gate. Sits between any `place_order` request and
//! the executor. The LLM is never trusted to enforce these limits —
//! they exist precisely because the LLM might get them wrong.

pub mod gate;
pub use gate::{evaluate, OrderRequest, RiskLimits, RiskVerdict};
