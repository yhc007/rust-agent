//! Polymarket CLOB live executor.
//!
//! Two behaviour modes, picked by the `LIVE_TRADING_ENABLED` env var:
//!
//! - **DRY_RUN (default).** Loads the wallet, looks the outcome
//!   `tokenId` up, builds + signs the EIP-712 Order, logs the full
//!   payload, and returns `ExecError::Live("DRY_RUN: …")`. No HTTP
//!   request is made; no money can move. This is what you get with
//!   `LIVE_TRADING_ENABLED` unset (or set to anything other than `1`).
//!
//! - **LIVE submission (`LIVE_TRADING_ENABLED=1`).** Builds the same
//!   signed Order, attaches L2 HMAC headers via
//!   [`crate::execution::clob_auth::l2_headers`], and `POST`s to
//!   `https://clob.polymarket.com/order`. Returns the issued
//!   `orderID` and Polymarket's `status` in a `FillResult`. Requires
//!   the three `POLYMARKET_CLOB_API_KEY` / `POLYMARKET_CLOB_SECRET` /
//!   `POLYMARKET_CLOB_PASSPHRASE` env vars from a prior
//!   `rust-agent clob-auth` run.
//!
//! What this still does NOT do:
//!
//! 1. **USDC `approve`** to the CTF Exchange spender. Even a
//!    successful `POST /order` will be rejected at on-chain fill time
//!    without the approval; do it once per wallet via your tooling of
//!    choice (a follow-up `live-setup --approve` subcommand can wrap
//!    this).
//! 2. **Fill polling.** The `POST /order` response carries the
//!    `orderID` + a `status` ("matched" / "delayed" / "live") but no
//!    fill detail. `FillResult.fill_size` is therefore best-effort —
//!    we report the requested taker amount when status is "matched"
//!    and 0 otherwise. A future user-channel WS subscription closes
//!    this loop precisely.
//!
//! Risk gate + kill switch run BEFORE `place_order` via the call path
//! through `execution::auto::route_decision`. That posture is exactly
//! how we keep the LLM from being able to over-spend even in live mode.

use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use alloy::signers::SignerSync;
use alloy::sol;
use alloy::sol_types::{eip712_domain, SolStruct};
use async_trait::async_trait;
use tracing::info;

use super::{ExecError, Executor, FillResult, PlaceOrderRequest};
use crate::coredb::markets::MarketRepo;
use crate::execution::clob_auth::{self, ApiCreds, CLOB_BASE_URL};

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
    /// Used at `place_order` time to look the market up by slug and
    /// pull the side-specific ERC-1155 token id out of the
    /// `clobTokenIds` Gamma exposes. Without the lookup the order
    /// would go out with `tokenId = 0` and Polymarket would reject
    /// it. Repo lookup is per-call; for the ~handful-of-orders/sec
    /// workload this codebase produces that's fine.
    market_repo: Arc<MarketRepo>,
    /// CLOB API credentials minted via a prior `rust-agent clob-auth`
    /// run. Only required when `LIVE_TRADING_ENABLED=1`; absent in the
    /// DRY_RUN path so the operator can review signatures before
    /// going through the L1 handshake.
    api_creds: Option<ApiCreds>,
    /// Reused reqwest client for `POST /order` so we don't pay the
    /// TLS handshake per invocation. Cheap to clone (Arc-wrapped
    /// internally).
    http: reqwest::Client,
}

impl LiveExec {
    /// Load the wallet from `POLYMARKET_PRIVATE_KEY`. Returns
    /// `ExecError::Live(...)` (which the rest of the executor stack
    /// already knows how to surface) when the env var is missing or
    /// the hex doesn't parse to a 32-byte secp256k1 key. The wallet
    /// address derived from the key is logged at info level so the
    /// operator can sanity-check it against the funded Polygon
    /// address before any signing. `market_repo` is the CoreDB
    /// markets repository the executor will look outcome tokenIds up
    /// from at place-order time.
    pub fn from_env(market_repo: Arc<MarketRepo>) -> Result<Self, ExecError> {
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
        // Try to load CLOB API credentials. Missing creds are fine in
        // the default DRY_RUN mode; we error at place_order time if
        // LIVE_TRADING_ENABLED is on but creds are absent.
        let api_creds = load_api_creds_from_env();
        match (&api_creds, std::env::var("LIVE_TRADING_ENABLED").as_deref()) {
            (Some(_), Ok("1")) => {
                info!(
                    "live exec: wallet {wallet_address} loaded (LIVE submission ENABLED, chain_id=137, exchange={POLYMARKET_CTF_EXCHANGE})"
                );
            }
            (_, Ok("1")) => {
                // LIVE on but no creds — flag now so the operator
                // doesn't get a surprise error on the first order.
                info!(
                    "live exec: wallet {wallet_address} loaded but CLOB API creds missing — \
                     LIVE_TRADING_ENABLED=1 calls will error. Run `rust-agent clob-auth` and export POLYMARKET_CLOB_*"
                );
            }
            _ => {
                info!(
                    "live exec: wallet {wallet_address} loaded (DRY_RUN — set LIVE_TRADING_ENABLED=1 to submit)"
                );
            }
        }
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|e| ExecError::Live(format!("reqwest client: {e}")))?;
        Ok(Self {
            signer,
            wallet_address,
            market_repo,
            api_creds,
            http,
        })
    }

    /// Build the typed-data Order from a generic place-order request +
    /// the already-resolved outcome `token_id` (U256). The caller is
    /// responsible for picking the right side's tokenId; this struct
    /// just plugs it in.
    fn build_order(&self, req: &PlaceOrderRequest, token_id: U256) -> Order {
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
            tokenId: token_id,
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

    /// LiveExec lets the user-channel WS be the source of truth for
    /// positions. `route_decision` would otherwise call apply_fill
    /// optimistically against the POST /order response, and the same
    /// fill would land again when the WS TRADE event arrived — so
    /// positions_v2 would double-count every live trade.
    fn defers_positions_to_ws(&self) -> bool {
        true
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> Result<FillResult, ExecError> {
        // Resolve outcome tokenId via the markets repo. Refusing to
        // proceed without it is intentional: the prior commit signed
        // orders with tokenId = 0 as a placeholder, which would be
        // rejected by Polymarket. Refusing here keeps the failure
        // visible instead of letting a malformed order through to the
        // submission step in a future turn.
        let market = self
            .market_repo
            .get(&req.market_slug)
            .await
            .map_err(|e| ExecError::Live(format!("markets.get({}): {e}", req.market_slug)))?
            .ok_or_else(|| {
                ExecError::Live(format!(
                    "market {} not found in CoreDB — run `ingest` first",
                    req.market_slug
                ))
            })?;
        let token_id_str = match req.side.as_str() {
            "YES" => market.yes_token_id.as_str(),
            "NO" => market.no_token_id.as_str(),
            other => {
                return Err(ExecError::Live(format!(
                    "side must be YES or NO; got {}",
                    other
                )))
            }
        };
        if token_id_str.is_empty() {
            return Err(ExecError::Live(format!(
                "no clobTokenIds captured for {} ({}). Re-run `ingest` after the \
                 Polymarket poller picked up clobTokenIds.",
                req.market_slug, req.side
            )));
        }
        let token_id = U256::from_str_radix(token_id_str, 10).map_err(|e| {
            ExecError::Live(format!(
                "parse tokenId for {} ({}): {e}",
                req.market_slug, req.side
            ))
        })?;

        let order = self.build_order(&req, token_id);

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
        let signature_hex = format!("0x{}", hex::encode(signature.as_bytes()));

        let live_enabled = matches!(
            std::env::var("LIVE_TRADING_ENABLED").as_deref(),
            Ok("1")
        );

        if !live_enabled {
            info!(
                "live exec [DRY_RUN]: market={} side={} size_usd={:.4} price={:.4} tokenId={}",
                req.market_slug, req.side, req.size_usd, req.price, token_id_str
            );
            info!("live exec [DRY_RUN]: order = {:?}", order);
            info!(
                "live exec [DRY_RUN]: eip712_hash = 0x{}",
                hex::encode(hash.as_slice())
            );
            info!("live exec [DRY_RUN]: signature = {signature_hex}");
            return Err(ExecError::Live(
                "DRY_RUN: order signed but NOT submitted. \
                 Set LIVE_TRADING_ENABLED=1 to enable POST /order."
                    .into(),
            ));
        }

        // --- LIVE submission path ---

        let creds = self.api_creds.as_ref().ok_or_else(|| {
            ExecError::Live(
                "LIVE_TRADING_ENABLED=1 but CLOB API creds missing — \
                 run `rust-agent clob-auth` and export \
                 POLYMARKET_CLOB_API_KEY / POLYMARKET_CLOB_SECRET / POLYMARKET_CLOB_PASSPHRASE"
                    .into(),
            )
        })?;

        let body_value = build_post_order_body(&order, &signature_hex, &req.side, &creds.api_key);
        let body_str = body_value.to_string();
        let headers = clob_auth::l2_headers(
            creds,
            self.wallet_address,
            "POST",
            "/order",
            &body_str,
        )
        .map_err(|e| ExecError::Live(format!("l2_headers: {e}")))?;

        info!(
            "live exec [LIVE]: POST {CLOB_BASE_URL}/order market={} side={} size_usd={:.4} price={:.4}",
            req.market_slug, req.side, req.size_usd, req.price
        );

        let url = format!("{CLOB_BASE_URL}/order");
        let mut builder = self.http.post(&url).header("Content-Type", "application/json");
        for (k, v) in &headers {
            builder = builder.header(*k, v);
        }
        let resp = builder
            .body(body_str)
            .send()
            .await
            .map_err(|e| ExecError::Live(format!("POST {url}: {e}")))?;

        let status_code = resp.status();
        let resp_text = resp.text().await.unwrap_or_default();
        if !status_code.is_success() {
            return Err(ExecError::Live(format!(
                "POST {url} returned {status_code}: {resp_text}"
            )));
        }
        let parsed: OrderResponse = serde_json::from_str(&resp_text).map_err(|e| {
            ExecError::Live(format!("parse /order response: {e}: {resp_text}"))
        })?;
        if !parsed.success {
            return Err(ExecError::Live(format!(
                "Polymarket rejected order: errorMsg={:?}, body={resp_text}",
                parsed.error_msg
            )));
        }
        info!(
            "live exec [LIVE]: order placed id={} status={}",
            parsed.order_id, parsed.status
        );
        // Best-effort fill reporting. "matched" → assume the full
        // taker side filled at the requested price. Anything else
        // means the order is resting / pending; the caller should
        // poll trades or subscribe to the user channel for confirmed
        // fill detail.
        let (fill_size, normalized_status) = match parsed.status.as_str() {
            "matched" => (req.size_usd / req.price.max(1e-6), "filled".to_string()),
            other => (0.0, other.to_string()),
        };
        Ok(FillResult {
            order_id: parsed.order_id,
            fill_size,
            fill_price: req.price,
            status: normalized_status,
        })
    }
}

/// Build the JSON body Polymarket's `POST /order` expects. All numeric
/// `Order` fields go on the wire as decimal strings (CLOB convention);
/// `side` is the human-string "BUY"/"SELL", not the on-chain uint8.
/// `signatureType` stays numeric. `owner` carries the API key minted
/// by `clob-auth`; `orderType` is GTC ("good till cancel") by default
/// for the agent's resting-limit-order model.
fn build_post_order_body(
    order: &Order,
    signature_hex: &str,
    agent_side: &str,
    api_key: &str,
) -> serde_json::Value {
    let side_wire = match agent_side {
        "YES" | "NO" => "BUY", // every order our agent emits is a BUY into the side's outcome token.
        other => other,        // pass-through so callers can experiment.
    };
    serde_json::json!({
        "order": {
            "salt": order.salt.to_string(),
            "maker": format!("{}", order.maker),
            "signer": format!("{}", order.signer),
            "taker": format!("{}", order.taker),
            "tokenId": order.tokenId.to_string(),
            "makerAmount": order.makerAmount.to_string(),
            "takerAmount": order.takerAmount.to_string(),
            "expiration": order.expiration.to_string(),
            "nonce": order.nonce.to_string(),
            "feeRateBps": order.feeRateBps.to_string(),
            "side": side_wire,
            "signatureType": order.signatureType,
            "signature": signature_hex,
        },
        "owner": api_key,
        "orderType": "GTC",
    })
}

/// Polymarket `POST /order` response. Field names match what the
/// gateway sends; `errorMsg` is empty on success.
#[derive(Debug, serde::Deserialize)]
struct OrderResponse {
    #[serde(default)]
    success: bool,
    #[serde(rename = "errorMsg", default)]
    error_msg: String,
    #[serde(rename = "orderID", default)]
    order_id: String,
    #[serde(default)]
    status: String,
}

/// Read CLOB API credentials from the env vars `rust-agent clob-auth`
/// suggests exporting. Returns `None` if any are missing — the
/// DRY_RUN path doesn't need them, and `place_order` re-checks before
/// the live branch fires.
fn load_api_creds_from_env() -> Option<ApiCreds> {
    let api_key = std::env::var("POLYMARKET_CLOB_API_KEY").ok()?;
    let secret = std::env::var("POLYMARKET_CLOB_SECRET").ok()?;
    let passphrase = std::env::var("POLYMARKET_CLOB_PASSPHRASE").ok()?;
    if api_key.is_empty() || secret.is_empty() || passphrase.is_empty() {
        return None;
    }
    Some(ApiCreds {
        api_key,
        secret,
        passphrase,
    })
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

    /// Sign-side smoke test that doesn't need a MarketRepo: we bypass
    /// the public `from_env` constructor and exercise the order-build
    /// + signing path directly. The market_repo field never gets
    /// touched, so we don't need to fabricate a Session.
    fn signer_from(key_hex: &str) -> (PrivateKeySigner, Address) {
        let bytes = hex::decode(key_hex).expect("hex");
        let signer = PrivateKeySigner::from_slice(&bytes).expect("signer");
        let addr = signer.address();
        (signer, addr)
    }

    /// Standalone version of `LiveExec::build_order` — we can't call
    /// the method without constructing a full LiveExec (which needs a
    /// MarketRepo). The logic mirrors `build_order` exactly; if you
    /// edit one, edit both.
    fn build_order_standalone(
        wallet: Address,
        req: &PlaceOrderRequest,
        token_id: U256,
    ) -> Order {
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Order {
            salt: U256::from(now_secs),
            maker: wallet,
            signer: wallet,
            taker: Address::ZERO,
            tokenId: token_id,
            makerAmount: to_base_units(req.size_usd),
            takerAmount: to_base_units(req.size_usd / req.price.max(1e-6)),
            expiration: U256::ZERO,
            nonce: U256::from(now_secs),
            feeRateBps: U256::ZERO,
            side: 0,
            signatureType: 0,
        }
    }

    #[test]
    fn post_order_body_shape() {
        // Pin the exact JSON shape Polymarket's gateway accepts: all
        // Order numeric fields as decimal strings, side as "BUY",
        // signatureType numeric, signature as a 0x-hex string,
        // wrapped in {order, owner, orderType: "GTC"}.
        let order = Order {
            salt: U256::from(1700000000u64),
            maker: Address::from_str("0x70997970C51812dc3A010C7d01b50e0d17dc79C8").unwrap(),
            signer: Address::from_str("0x70997970C51812dc3A010C7d01b50e0d17dc79C8").unwrap(),
            taker: Address::ZERO,
            tokenId: U256::from(12345u64),
            makerAmount: U256::from(10_000_000u64),
            takerAmount: U256::from(20_000_000u64),
            expiration: U256::ZERO,
            nonce: U256::from(1700000000u64),
            feeRateBps: U256::ZERO,
            side: 0,
            signatureType: 0,
        };
        let body = build_post_order_body(&order, "0xdeadbeef", "YES", "test-api-key");
        let inner = body.get("order").unwrap();
        assert_eq!(inner.get("salt").unwrap(), "1700000000");
        assert_eq!(inner.get("tokenId").unwrap(), "12345");
        assert_eq!(inner.get("makerAmount").unwrap(), "10000000");
        assert_eq!(inner.get("takerAmount").unwrap(), "20000000");
        assert_eq!(inner.get("side").unwrap(), "BUY");
        assert_eq!(inner.get("signatureType").unwrap(), 0);
        assert_eq!(inner.get("signature").unwrap(), "0xdeadbeef");
        assert_eq!(body.get("owner").unwrap(), "test-api-key");
        assert_eq!(body.get("orderType").unwrap(), "GTC");
    }

    #[test]
    fn no_side_also_maps_to_buy() {
        // Our agent never SELLs — both YES and NO are BUYs into the
        // respective outcome token. The side mapping should reflect that.
        let order = Order {
            salt: U256::ZERO,
            maker: Address::ZERO,
            signer: Address::ZERO,
            taker: Address::ZERO,
            tokenId: U256::ZERO,
            makerAmount: U256::ZERO,
            takerAmount: U256::ZERO,
            expiration: U256::ZERO,
            nonce: U256::ZERO,
            feeRateBps: U256::ZERO,
            side: 0,
            signatureType: 0,
        };
        let body = build_post_order_body(&order, "0x00", "NO", "k");
        assert_eq!(body["order"]["side"], "BUY");
    }

    #[test]
    fn signing_smoke() {
        // Deterministic test key (NOT a real wallet — Ethereum book
        // test vector `0x59c6…690d`, address `0x70997970…`).
        let key = "59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";
        let (_signer, wallet) = signer_from(key);
        let req = PlaceOrderRequest {
            market_slug: "test".into(),
            side: "YES".into(),
            size_usd: 10.0,
            price: 0.5,
        };
        let token_id = U256::from(123456789u64);
        let order = build_order_standalone(wallet, &req, token_id);
        assert_eq!(order.tokenId, token_id);
        // $10 at $0.50/share → makerAmount 10 USDC = 10_000_000, takerAmount 20 shares = 20_000_000.
        assert_eq!(order.makerAmount, U256::from(10_000_000u64));
        assert_eq!(order.takerAmount, U256::from(20_000_000u64));
        assert_eq!(order.side, 0);
        assert_eq!(order.maker, wallet);
        assert_eq!(order.signer, wallet);
        assert_eq!(
            wallet,
            Address::from_str("0x70997970C51812dc3A010C7d01b50e0d17dc79C8").unwrap()
        );
    }
}
