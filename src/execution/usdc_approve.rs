//! One-time USDC `approve(spender, amount)` helper for the Polymarket
//! CTF Exchange.
//!
//! Polymarket fills come from the contract pulling USDC out of your
//! wallet at settle time. That pull only works if the wallet has
//! previously authorised the CTF Exchange address as a spender via an
//! ERC-20 `approve`. Even a successful `POST /order` with a valid
//! EIP-712 signature will be rejected at fill if the allowance is
//! zero — this helper closes that gap.
//!
//! Defaults are conservative: invoking the subcommand without
//! `--send` prints the current allowance and the call that *would*
//! be made, then exits with a non-zero status if the allowance is
//! already at or above the requested amount (so re-running on an
//! already-approved wallet is a no-op rather than a quiet
//! double-spend). `--send` actually broadcasts the tx; the helper
//! asserts the RPC's reported `chain_id == 137` first so you can't
//! point this at the wrong network by accident.

use anyhow::{anyhow, bail, Context, Result};
use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
        function decimals() external view returns (uint8);
    }
}

/// Polygon mainnet PoS-bridged USDC.e — the token Polymarket's CTF
/// Exchange was deployed against. Native USDC (`0x3c49…3359`) is a
/// separate token contract and won't satisfy the exchange's USDC
/// allowance check. Operator can override via `--usdc <ADDR>` if
/// Polymarket migrates.
pub const POLYGON_USDC_E: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
/// Same CTF Exchange address `LiveExec` signs against.
pub const POLYMARKET_CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";
const POLYGON_CHAIN_ID: u64 = 137;

pub struct ApproveConfig {
    pub rpc_url: String,
    pub usdc_address: Address,
    pub spender: Address,
    /// USDC base units (6 decimals). `U256::MAX` for unlimited.
    pub amount: U256,
    /// When `false`, the helper only reads the current allowance,
    /// prints the intended call, and exits — no transaction broadcast.
    pub send: bool,
}

pub async fn run(cfg: ApproveConfig) -> Result<()> {
    let signer = load_signer_from_env()?;
    let wallet_address = signer.address();
    let wallet = EthereumWallet::from(signer);

    println!("🔑 wallet: {wallet_address}");
    println!("🌐 rpc:    {}", cfg.rpc_url);
    println!("💵 usdc:   {}", cfg.usdc_address);
    println!("🔓 spender:{}", cfg.spender);

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .on_http(
            cfg.rpc_url
                .parse()
                .with_context(|| format!("parse rpc_url {}", cfg.rpc_url))?,
        );

    // Confirm we're on Polygon before doing anything else. Cheap call,
    // and it's the single biggest "wrong network" footgun.
    let chain_id = provider
        .get_chain_id()
        .await
        .context("get_chain_id (probe rpc)")?;
    if chain_id != POLYGON_CHAIN_ID {
        bail!(
            "rpc reports chain_id={chain_id}; expected {POLYGON_CHAIN_ID} (Polygon mainnet). \
             Refusing to send transaction on the wrong network."
        );
    }
    println!("✓ chain_id confirmed {chain_id} (Polygon)");

    let usdc = IERC20::new(cfg.usdc_address, &provider);

    let current = usdc
        .allowance(wallet_address, cfg.spender)
        .call()
        .await
        .context("read current allowance")?
        ._0;
    println!("📜 current allowance: {} (base units)", current);

    if current >= cfg.amount {
        println!(
            "✓ already approved at or above target amount; nothing to do. \
             Pass --send with a higher --amount to top up."
        );
        return Ok(());
    }

    let want = if cfg.amount == U256::MAX {
        "MAX".to_string()
    } else {
        cfg.amount.to_string()
    };
    println!("📝 would call USDC.approve({}, {want})", cfg.spender);

    if !cfg.send {
        println!(
            "DRY_RUN: pass --send to broadcast. Re-running without --send is safe — \
             this command reads on-chain state but does not write."
        );
        return Ok(());
    }

    println!("🚀 broadcasting…");
    let pending = usdc
        .approve(cfg.spender, cfg.amount)
        .send()
        .await
        .context("broadcast approve tx")?;
    let tx_hash = *pending.tx_hash();
    println!("   tx_hash = {tx_hash:#x}");
    let receipt = pending
        .get_receipt()
        .await
        .context("await tx receipt")?;
    println!(
        "✓ included in block {} (gas used {})",
        receipt.block_number.unwrap_or_default(),
        receipt.gas_used
    );

    // Read back to confirm the new allowance landed.
    let confirmed = usdc
        .allowance(wallet_address, cfg.spender)
        .call()
        .await
        .context("confirm allowance after tx")?
        ._0;
    println!("📜 new allowance:    {} (base units)", confirmed);
    if confirmed < cfg.amount {
        return Err(anyhow!(
            "tx included but allowance is {confirmed}, expected at least {} — \
             token contract may have rejected the approve",
            cfg.amount
        ));
    }
    Ok(())
}

fn load_signer_from_env() -> Result<PrivateKeySigner> {
    let raw = std::env::var("POLYMARKET_PRIVATE_KEY")
        .map_err(|_| anyhow!("POLYMARKET_PRIVATE_KEY not set"))?;
    let stripped = raw.trim().trim_start_matches("0x");
    if stripped.len() != 64 {
        bail!(
            "POLYMARKET_PRIVATE_KEY must be 32 bytes (64 hex chars); got {}",
            stripped.len()
        );
    }
    let bytes = hex::decode(stripped).context("POLYMARKET_PRIVATE_KEY decode")?;
    PrivateKeySigner::from_slice(&bytes).context("PrivateKeySigner::from_slice")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn constants_parse() {
        // If either of these addresses fails to parse, every approve
        // call below would silently target the wrong contract. Catch
        // the typo here.
        assert!(Address::from_str(POLYGON_USDC_E).is_ok());
        assert!(Address::from_str(POLYMARKET_CTF_EXCHANGE).is_ok());
    }
}
