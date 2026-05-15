//! Polymarket CLOB user-channel WebSocket listener.
//!
//! Once an order is on the book (or matched immediately), Polymarket
//! emits fill / order-state events on a per-user WebSocket. The REST
//! `POST /order` response only carries `orderID + status` — not the
//! fill detail (size, price, txn hash). This module is the listener
//! that closes that loop.
//!
//! **This commit is log-only.** Incoming messages are parsed and
//! logged at INFO level; nothing yet writes back to the `orders` /
//! `positions_v2` tables. The shape of Polymarket's user-channel
//! payloads is best verified against a live feed before we start
//! mutating local state, so the DB-update path stays out of this
//! turn. Add it in a follow-up once a few hours of live messages
//! have been eyeballed.
//!
//! Auth: subscribe message carries the L1-issued `apiKey` / `secret`
//! / `passphrase` triple. We reuse [`crate::execution::clob_auth::ApiCreds`]
//! so the same credentials sourced for `LiveExec` cover this listener.
//!
//! Reconnect: capped exponential backoff (1 s → 30 s), shutdown-aware
//! sleeps so a Ctrl+C during the back-off window exits in <1 s. Same
//! shape as `crate::data::binance`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::watch;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use crate::coredb::orders::OrderRepo;
use crate::coredb::types::{bucket_day, now_ms};
use crate::execution::clob_auth::ApiCreds;

const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const RECONNECT_INITIAL: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);

/// One trade or order-state notification from the user channel. Field
/// shape from Polymarket's documented event payloads. Unknown event
/// types are logged with the raw text and skipped.
#[derive(Debug, Deserialize)]
struct UserEvent {
    #[serde(rename = "event_type", default)]
    event_type: String,
    #[serde(default)]
    id: String,
    /// `asset_id` is Polymarket's term for the ERC-1155 outcome
    /// `tokenId` (the same one `LiveExec` plugs into the signed Order).
    #[serde(default)]
    asset_id: String,
    /// Polymarket sometimes uses `market` (condition_id) on these
    /// payloads instead of `market_slug`. Carry it through so the
    /// follow-up DB-write turn can map back to the slug via
    /// `clobTokenIds` lookup.
    #[serde(default)]
    market: String,
    #[serde(default)]
    side: String,
    /// Numeric fields come stringified on the wire — match the same
    /// convention `POST /order` uses.
    #[serde(default)]
    size: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    status: String,
    /// `taker_order_id` is Polymarket's id for the order that filled.
    /// On TRADE events for our orders this is the same `orderID` the
    /// `POST /order` response returned, so we can map straight back
    /// to the row in `polymarket_btc.orders`.
    #[serde(default)]
    taker_order_id: String,
}

/// Connect, subscribe, and loop forever (until shutdown). All errors
/// trigger a reconnect with capped exponential backoff. The
/// `Arc<ApiCreds>` is held so the same connection can re-send the
/// auth message after a drop.
///
/// When `order_repo` is `Some`, incoming TRADE events with a
/// matching `taker_order_id` in today's bucket cause an in-place
/// upsert of the orders row (fill_size + fill_price + status). When
/// `None`, the listener is observation-only. Daemon controls this
/// via the `APPLY_FILLS=1` env gate.
pub async fn run(
    creds: Arc<ApiCreds>,
    order_repo: Option<Arc<OrderRepo>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    info!(
        "user-channel: connecting to {WS_URL} (apply_fills={})",
        order_repo.is_some()
    );
    let mut backoff = RECONNECT_INITIAL;
    let mut seen_trade_ids: HashSet<String> = HashSet::new();
    loop {
        if *shutdown.borrow() {
            info!("user-channel: shutdown before connect");
            return Ok(());
        }

        match run_one_session(&creds, order_repo.as_deref(), &mut seen_trade_ids, &mut shutdown).await {
            Ok(()) => {
                info!("user-channel: clean exit");
                return Ok(());
            }
            Err(e) => {
                warn!("user-channel: session ended ({e}); reconnecting in {:?}", backoff);
            }
        }

        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("user-channel: shutdown during reconnect backoff");
                    return Ok(());
                }
            }
        }
        backoff = (backoff * 2).min(RECONNECT_MAX);
    }
}

async fn run_one_session(
    creds: &ApiCreds,
    order_repo: Option<&OrderRepo>,
    seen_trade_ids: &mut HashSet<String>,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<()> {
    let (ws_stream, _resp) = connect_async(WS_URL)
        .await
        .context("connect_async user-channel")?;
    let (mut write, mut read) = ws_stream.split();
    info!("user-channel: connected");

    // Subscribe message format from Polymarket's JS / py clob-client.
    // `type: "user"` is the channel discriminator; auth is the L1-issued
    // creds. Polymarket's gateway accepts the secret in its base64url
    // form (we don't decode here — the gateway is what matches against
    // the user-side stored value).
    let sub = serde_json::json!({
        "auth": {
            "apiKey":     creds.api_key,
            "secret":     creds.secret,
            "passphrase": creds.passphrase,
        },
        "type": "user",
    });
    write
        .send(Message::Text(sub.to_string()))
        .await
        .context("send subscribe")?;
    info!("user-channel: subscribed (auth + type=user)");

    let mut n_text = 0u64;
    let mut n_events = 0u64;
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(
                        "user-channel: shutdown signal received ({} text frames, {} parsed events)",
                        n_text, n_events
                    );
                    return Ok(());
                }
            }
            msg = read.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(anyhow!("ws read error: {e}")),
                    None => return Err(anyhow!("ws stream ended")),
                };
                match msg {
                    Message::Text(text) => {
                        n_text += 1;
                        // Polymarket sends a one-off ack (`{"event_type":"connected"}`)
                        // first, then real events. We log either way at
                        // INFO so the operator can spot the handshake.
                        match serde_json::from_str::<UserEvent>(&text) {
                            Ok(ev) => {
                                n_events += 1;
                                info!(
                                    "user-channel: event_type={} id={} market={} asset_id={} side={} size={} price={} status={} taker_order_id={}",
                                    ev.event_type, ev.id, ev.market, ev.asset_id,
                                    ev.side, ev.size, ev.price, ev.status, ev.taker_order_id
                                );
                                if let Some(repo) = order_repo {
                                    if let Err(e) =
                                        maybe_apply_trade(repo, &ev, seen_trade_ids).await
                                    {
                                        warn!("user-channel: apply trade failed: {e}");
                                    }
                                }
                            }
                            Err(e) => {
                                debug!("user-channel: unparsed text frame ({e}): {text}");
                                // Also log the raw payload at INFO so we
                                // can collect samples during the first
                                // few hours of live observation, before
                                // the DB-write follow-up turn locks the
                                // parse schema.
                                info!("user-channel: raw frame = {text}");
                            }
                        }
                    }
                    Message::Ping(p) => {
                        debug!("user-channel: ping ({} bytes)", p.len());
                    }
                    Message::Close(frame) => {
                        return Err(anyhow!(
                            "ws closed by peer: {:?}",
                            frame.map(|f| f.reason.to_string())
                        ));
                    }
                    other => {
                        debug!("user-channel: ignoring {:?}", other);
                    }
                }
            }
        }
    }
}

/// Decide whether to materialise the WS event into the `orders`
/// table. Filters non-trade events, dedupes via the in-memory
/// `seen_trade_ids` set (Polymarket re-sends each trade as it walks
/// MATCHED → MINED → CONFIRMED, and we only want one apply), and
/// requires a `taker_order_id` to map back to our row. Numeric fields
/// arrive stringified on the wire; we parse with `0.0` as the
/// fallback so a bad payload produces a `false` from
/// `apply_ws_fill` rather than corrupting an existing row.
async fn maybe_apply_trade(
    repo: &OrderRepo,
    ev: &UserEvent,
    seen_trade_ids: &mut HashSet<String>,
) -> Result<()> {
    if !ev.event_type.eq_ignore_ascii_case("trade") {
        return Ok(());
    }
    if ev.id.is_empty() {
        debug!("user-channel: trade event missing id; skip dedup + apply");
        return Ok(());
    }
    if !seen_trade_ids.insert(ev.id.clone()) {
        debug!(
            "user-channel: trade {} already applied (status follow-up); skip",
            ev.id
        );
        return Ok(());
    }
    if ev.taker_order_id.is_empty() {
        debug!(
            "user-channel: trade {} has no taker_order_id; cannot map back to orders row",
            ev.id
        );
        return Ok(());
    }
    let fill_size: f64 = ev.size.parse().unwrap_or(0.0);
    let fill_price: f64 = ev.price.parse().unwrap_or(0.0);
    let bd = bucket_day(now_ms());
    let updated = repo
        .apply_ws_fill(bd, &ev.taker_order_id, fill_size, fill_price, &ev.status)
        .await
        .context("apply_ws_fill")?;
    if updated {
        info!(
            "user-channel: APPLIED trade {} → orders.order_id={} fill_size={fill_size} fill_price={fill_price} status={}",
            ev.id, ev.taker_order_id, ev.status
        );
    } else {
        debug!(
            "user-channel: trade {} (taker_order_id={}) — no matching row in today's bucket",
            ev.id, ev.taker_order_id
        );
    }
    Ok(())
}
