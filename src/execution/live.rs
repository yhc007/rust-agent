//! Live Polymarket CLOB executor — skeleton.
//!
//! Implementing real live trading requires three pieces this stub does
//! NOT yet provide:
//!
//! 1. **EIP-712 signer.** Polymarket CLOB orders are EIP-712 typed-data
//!    blobs signed with the trader's Polygon wallet. The signer needs
//!    the wallet private key (from env / vault / age-encrypted file),
//!    plus the order's typed-data schema (domain, types, message). The
//!    `alloy` crate's `alloy-signer-local` is the recommended path.
//!
//! 2. **CLOB authentication.** Polymarket distinguishes L1 (wallet
//!    signature) auth used to create an API key, and L2 (API key)
//!    auth on subsequent calls. The L1 flow goes through
//!    `POST /auth/api-key` once at startup; L2 then attaches three
//!    headers (POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP) on every
//!    request.
//!
//! 3. **Order submission.** `POST /order` with the signed order body,
//!    plus follow-up subscriptions on the user channel WS for fill
//!    notifications.
//!
//! Until those are filled in, instantiating `LiveExec` returns an
//! error so a misconfigured deployment fails loud at startup rather
//! than silently misrouting to paper. The trait impl below is here so
//! the rest of the codebase can already reference `LiveExec` and the
//! eventual switchover is a one-file change.

use async_trait::async_trait;

use super::{ExecError, Executor, FillResult, PlaceOrderRequest};

pub struct LiveExec {
    _api_base: String,
    _wallet_address: String,
}

impl LiveExec {
    /// Construct a LiveExec. Currently always errors; see module docs.
    pub fn from_env() -> Result<Self, ExecError> {
        Err(ExecError::Live(
            "LiveExec is a stub: EIP-712 signer + CLOB auth flow + \
             POST /order are not implemented yet. Use PaperExec, or \
             implement the three TODOs in execution/live.rs."
                .into(),
        ))
    }
}

#[async_trait]
impl Executor for LiveExec {
    fn label(&self) -> &'static str {
        "live"
    }

    async fn place_order(&self, _req: PlaceOrderRequest) -> Result<FillResult, ExecError> {
        Err(ExecError::Live(
            "LiveExec.place_order not implemented".into(),
        ))
    }
}
