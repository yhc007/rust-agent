//! Polymarket CLOB live executor — sign-only skeleton.
//!
//! What this commit does:
//!
//! - Loads a Polygon wallet private key from `POLYMARKET_PRIVATE_KEY`
//!   (hex, with or without `0x` prefix).
//! - Builds the EIP-712 typed-data `Order` Polymarket's CTF Exchange
//!   contract expects, with `domain.verifyingContract` pinned at the
//!   mainnet exchange address (`0x4bFb…D982E`) and `chainId = 137`.
//! - Signs the order hash with the loaded wallet.
//! - **Returns an `ExecError::Live("DRY_RUN: …")` immediately after
//!   signing.** No HTTP request to Polymarket is made; no allowance
//!   is set; no money can move. The logged payload is the exact bytes
//!   that would go on the wire.
//!
//! What this commit deliberately does NOT do (each is its own follow-
//! up turn so the operator can review what's about to happen):
//!
//! 1. `POST /order` to `https://clob.polymarket.com` — the dry-run
//!    output is intended to be reviewed first.
//! 2. L1/L2 CLOB auth handshake (POLY_ADDRESS / POLY_SIGNATURE /
//!    POLY_TIMESTAMP headers) and the `POST /auth/api-key` step.
//! 3. USDC `approve` flow for the CTF Exchange spender. Without that
//!    on-chain step, even a successful POST would be rejected at fill.
//! 4. Outcome `tokenId` resolution. Polymarket's order references an
//!    ERC-1155 token id, not the slug + side our agent carries.
//!    `route_decision` calls `LiveExec` with `(slug, "YES"|"NO", size_usd,
//!    price)`, but the wire needs `tokenId`. Today we substitute
//!    `U256::ZERO` and log the gap loudly — fixing it requires the
//!    Polymarket ingest pipeline to capture `clobTokenIds` and the
//!    markets table to carry both YES and NO token ids.
//!
//! The whole point of stopping after step "sign + log" is to let the
//! operator (a) confirm the wallet address derived from the private
//! key matches the funded address, (b) eyeball the typed-data payload
//! against Polymarket's published schema before flipping the actual
//! POST on, and (c) keep the rest of the codebase compiling against a
//! real `Executor` impl without any chance of money movement.
//!
//! Risk gate + kill switch still run BEFORE `place_order` because the
//! call path goes through `execution::auto::route_decision` (or the
//! `place_order` agent tool). That posture stays in live mode — never
//! delete the gate.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use alloy::sol;
use alloy::sol_types::{eip712_domain, SolStruct};
use async_trait::async_trait;
use tracing::{info, warn};

use super::{ExecError, Executor, FillResult, PlaceOrderRequest};

sol! {
    /// Polymarket CTF Exchange `Order` struct. Field layout mirrors the
    /// on-chain contract; deviating from this exact ordering / naming
    /// changes the EIP-712 typeHash and silently invalidates every
    /// signature.
    #[derive(Debug)]
    struct Order {
        uint256 salt;
        address maker;
        address signer;
        address taker;
        uint256 tokenId;
        uint256 makerAmount;
        uint256 takerAmount;
        uint256 expiration;
        uint256 nonce;
        uint256 feeRateBps;
        uint8 side;
        uint8 signatureType;
    }
}

/// Polymarket mainnet CTF Exchange. Live trading depends on this exact
/// verifyingContract value matching what Polymarket publishes; a typo
/// here means your signed orders are valid in the abstract but not for
/// Polymarket's specific contract.
const POLYMARKET_CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
const POLYGON_CHAIN_ID: u64 = 137;
/// USDC and Polymarket outcome tokens both use 6 decimal places.
const TOKEN_DECIMALS: u32 = 6;

pub struct LiveExec {
    signer: PrivateKeySigner,
    wallet_address: Address,
}

impl LiveExec {
    /// Load the wallet from `POLYMARKET_PRIVATE_KEY`. Returns
    /// `ExecError::Live(...)` (which the rest of the executor stack
    /// already knows how to surface) when the env var is missing or
    /// the hex doesn't parse to a 32-byte secp256k1 key. The wallet
    /// address derived from the key is logged at info level so the
    /// operator can sanity-check it against the funded Polygon
    /// address before any signing.
    pub fn from_env() -> Result<Self, ExecError> {
        let raw = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| ExecError::Live("POLYMARKET_PRIVATE_KEY not set".into()))?;
        let stripped = raw.trim().trim_start_matches("0x");
        if stripped.len() != 64 {
            return Err(ExecError::Live(format!(
                "POLYMARKET_PRIVATE_KEY must be 32 bytes (64 hex chars); got {}",
                stripped.len()
            )));
        }
        let bytes = hex::decode(stripped)
            .map_err(|e| ExecError::Live(format!("POLYMARKET_PRIVATE_KEY not hex: {e}")))?;
        let signer = PrivateKeySigner::from_slice(&bytes)
            .map_err(|e| ExecError::Live(format!("private-key load: {e}")))?;
        let wallet_address = signer.address();
        info!(
            "live exec: wallet {} loaded (chain_id=137, exchange={POLYMARKET_CTF_EXCHANGE})",
            wallet_address
        );
        Ok(Self {
            signer,
            wallet_address,
        })
    }

    /// Build the typed-data Order from a generic place-order request.
    /// `tokenId` is a placeholder zero — see the module docs and the
    /// run-time warn-log below; resolving outcome → tokenId is a
    /// pre-requisite for actual submission.
    fn build_order(&self, req: &PlaceOrderRequest) -> Order {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        // Cheap salt: ms-since-epoch fits in U256 trivially and avoids a
        // rand dep. For real submission a CSPRNG salt is conventional.
        let salt = U256::from(now_secs);
        // size_usd → USDC base units (6 decimals).
        let usdc_amount = to_base_units(req.size_usd);
        // Outcome shares received = size_usd / price; price ∈ [0,1] is
        // the per-share price in USDC. clamp price away from zero to
        // avoid an infinite share count if the caller forgets the risk
        // gate's price>0 check.
        let share_price = req.price.max(1e-6);
        let token_amount = to_base_units(req.size_usd / share_price);

        Order {
            salt,
            maker: self.wallet_address,
            signer: self.wallet_address,
            taker: Address::ZERO,
            tokenId: U256::ZERO,        // <-- TODO: market.clobTokenIds[side]
            makerAmount: usdc_amount,
            takerAmount: token_amount,
            expiration: U256::ZERO,     // 0 = good-til-cancel
            nonce: U256::from(now_secs),
            feeRateBps: U256::ZERO,
            side: 0,                    // 0 = BUY (we never short)
            signatureType: 0,           // 0 = EOA
        }
    }
}

#[async_trait]
impl Executor for LiveExec {
    fn label(&self) -> &'static str {
        "live"
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> Result<FillResult, ExecError> {
        let order = self.build_order(&req);

        // EIP-712 domain — verifyingContract here is what makes the
        // signature usable against Polymarket's contract specifically.
        let exchange = Address::from_str(POLYMARKET_CTF_EXCHANGE)
            .map_err(|e| ExecError::Live(format!("bad exchange address const: {e}")))?;
        let domain = eip712_domain! {
            name: "Polymarket CTF Exchange",
            version: "1",
            chain_id: POLYGON_CHAIN_ID,
            verifying_contract: exchange,
        };

        let hash = order.eip712_signing_hash(&domain);
        let signature = self
            .signer
            .sign_hash_sync(&hash)
            .map_err(|e| ExecError::Live(format!("sign: {e}")))?;

        // We intentionally do NOT submit yet — see module docs. Log the
        // full signed payload so the operator can review it against
        // Polymarket's published schema before any HTTP call lands.
        info!(
            "live exec [DRY_RUN]: market={} side={} size_usd={:.4} price={:.4}",
            req.market_slug, req.side, req.size_usd, req.price
        );
        info!("live exec [DRY_RUN]: order = {:?}", order);
        info!(
            "live exec [DRY_RUN]: eip712_hash = 0x{}",
            hex::encode(hash.as_slice())
        );
        info!(
            "live exec [DRY_RUN]: signature = 0x{}",
            hex::encode(signature.as_bytes())
        );
        if order.tokenId == U256::ZERO {
            warn!(
                "live exec [DRY_RUN]: tokenId is ZERO — placeholder. Resolve via \
                 market.clobTokenIds[side] before flipping live submission on."
            );
        }

        Err(ExecError::Live(
            "DRY_RUN: order signed but NOT submitted. \
             Live submission requires (a) outcome tokenId resolution, \
             (b) CLOB L1/L2 auth, (c) USDC approve to the CTF Exchange. \
             See src/execution/live.rs module docs."
                .into(),
        ))
    }
}

/// Convert a USD/share float into 6-decimal base units (USDC's
/// granularity, also what Polymarket outcome tokens use). Rounds to
/// nearest base unit; saturates rather than panicking on overflow.
fn to_base_units(amount: f64) -> U256 {
    if !amount.is_finite() || amount <= 0.0 {
        return U256::ZERO;
    }
    let scaled = (amount * 10f64.powi(TOKEN_DECIMALS as i32)).round();
    if scaled >= u128::MAX as f64 {
        return U256::from(u128::MAX);
    }
    U256::from(scaled as u128)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_base_units_handles_dollars() {
        // $10 → 10_000_000 in USDC base units.
        assert_eq!(to_base_units(10.0), U256::from(10_000_000u64));
    }

    #[test]
    fn to_base_units_handles_subdollar() {
        // $0.50 → 500_000.
        assert_eq!(to_base_units(0.5), U256::from(500_000u64));
    }

    #[test]
    fn to_base_units_zero_and_negative_clamp() {
        assert_eq!(to_base_units(0.0), U256::ZERO);
        assert_eq!(to_base_units(-1.0), U256::ZERO);
        assert_eq!(to_base_units(f64::NAN), U256::ZERO);
    }

    fn expect_live_err(r: Result<LiveExec, ExecError>) -> String {
        match r {
            Ok(_) => panic!("expected ExecError::Live, got Ok"),
            Err(ExecError::Live(msg)) => msg,
            Err(other) => panic!("wrong error variant: {other:?}"),
        }
    }

    #[test]
    fn from_env_rejects_missing_key() {
        let original = std::env::var("POLYMARKET_PRIVATE_KEY").ok();
        std::env::remove_var("POLYMARKET_PRIVATE_KEY");
        let msg = expect_live_err(LiveExec::from_env());
        assert!(msg.contains("POLYMARKET_PRIVATE_KEY"), "{msg}");
        if let Some(v) = original {
            std::env::set_var("POLYMARKET_PRIVATE_KEY", v);
        }
    }

    #[test]
    fn from_env_rejects_wrong_length() {
        std::env::set_var("POLYMARKET_PRIVATE_KEY", "0xdeadbeef");
        let msg = expect_live_err(LiveExec::from_env());
        assert!(msg.contains("32 bytes"), "{msg}");
        std::env::remove_var("POLYMARKET_PRIVATE_KEY");
    }

    #[test]
    fn signing_smoke() {
        // Build a real signer from a deterministic test key (NOT a real
        // wallet — this is the test vector from Ethereum book conventions:
        // `0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d`,
        // address 0x70997970C51812dc3A010C7d01b50e0d17dc79C8).
        let key = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
        std::env::set_var("POLYMARKET_PRIVATE_KEY", key);
        let exec = LiveExec::from_env().expect("load test key");
        let req = PlaceOrderRequest {
            market_slug: "test".into(),
            side: "YES".into(),
            size_usd: 10.0,
            price: 0.5,
        };
        let order = exec.build_order(&req);
        // $10 at $0.50/share → makerAmount 10 USDC = 10_000_000, takerAmount 20 shares = 20_000_000.
        assert_eq!(order.makerAmount, U256::from(10_000_000u64));
        assert_eq!(order.takerAmount, U256::from(20_000_000u64));
        assert_eq!(order.side, 0);
        // Sanity: maker == signer == wallet derived from the test key.
        assert_eq!(order.maker, exec.wallet_address);
        assert_eq!(order.signer, exec.wallet_address);
        assert_eq!(
            order.maker,
            Address::from_str("0x70997970C51812dc3A010C7d01b50e0d17dc79C8").unwrap()
        );
        std::env::remove_var("POLYMARKET_PRIVATE_KEY");
    }
}
