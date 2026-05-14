//! CQL DDL for the `polymarket_btc` keyspace.
//!
//! Constraints baked in by the CoreDB CQL parser:
//! - Composite partition keys `((a, b), c)` are not supported, so the
//!   first column of `PRIMARY KEY (...)` is the partition key and the
//!   rest are clustering columns.
//! - `WITH CLUSTERING ORDER BY` is not supported; reads order by the
//!   clustering keys' natural order. Application code must reverse on
//!   read if it wants newest-first.
//! - `CREATE INDEX IF NOT EXISTS` is not parsed; the migrate function
//!   swallows "already exists" errors so re-running is still safe.
//! - `DATE` type is not supported; daily buckets use `TIMESTAMP`.

pub const KEYSPACE: &str = "polymarket_btc";

pub const MIGRATIONS: &[&str] = &[
    // ---- Keyspace ----
    "CREATE KEYSPACE IF NOT EXISTS polymarket_btc \
     WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",

    // ---- BTC price ticks (Binance et al.). partition = bucket_hour. ----
    "CREATE TABLE IF NOT EXISTS polymarket_btc.btc_ticks ( \
        bucket_hour TIMESTAMP, \
        symbol      TEXT, \
        ts          TIMESTAMP, \
        price       DOUBLE, \
        volume      DOUBLE, \
        bid         DOUBLE, \
        ask         DOUBLE, \
        PRIMARY KEY (bucket_hour, symbol, ts) \
     )",

    // ---- Polymarket orderbook snapshots. partition = bucket_hour. ----
    "CREATE TABLE IF NOT EXISTS polymarket_btc.orderbook_snapshots ( \
        bucket_hour TIMESTAMP, \
        market_slug TEXT, \
        ts          TIMESTAMP, \
        yes_bids    TEXT, \
        yes_asks    TEXT, \
        no_bids     TEXT, \
        no_asks     TEXT, \
        PRIMARY KEY (bucket_hour, market_slug, ts) \
     )",

    // ---- Polymarket market metadata. ----
    "CREATE TABLE IF NOT EXISTS polymarket_btc.markets ( \
        slug          TEXT PRIMARY KEY, \
        question      TEXT, \
        end_date      TIMESTAMP, \
        outcomes      TEXT, \
        closed        BOOLEAN, \
        last_price    DOUBLE, \
        updated_at    TIMESTAMP, \
        yes_token_id  TEXT, \
        no_token_id   TEXT \
     )",
    "CREATE INDEX idx_markets_open ON polymarket_btc.markets (closed)",
    // For schemas provisioned before clobTokenIds capture. CoreDB swallows
    // "Column already exists" via the migrate() shim so reruns are safe.
    "ALTER TABLE polymarket_btc.markets ADD yes_token_id TEXT",
    "ALTER TABLE polymarket_btc.markets ADD no_token_id TEXT",

    // ---- Agent decisions (LLM outputs). partition = bucket_day.
    // entry_price = YES price at decision time, needed by `compare-pnl`
    // to mark each decision to current market.
    "CREATE TABLE IF NOT EXISTS polymarket_btc.decisions ( \
        bucket_day   TIMESTAMP, \
        ts           TIMESTAMP, \
        decision_id  UUID, \
        market_slug  TEXT, \
        side         TEXT, \
        size_usd     DOUBLE, \
        confidence   DOUBLE, \
        edge_bps     INT, \
        reasoning    TEXT, \
        raw_response TEXT, \
        entry_price  DOUBLE, \
        PRIMARY KEY (bucket_day, ts, decision_id) \
     )",
    "CREATE INDEX idx_decisions_market ON polymarket_btc.decisions (market_slug)",
    // For databases provisioned before entry_price was part of CREATE TABLE.
    // CoreDB returns "Column 'X' already exists" on duplicate, which migrate()
    // swallows, so re-running is safe on either old or new schema state.
    "ALTER TABLE polymarket_btc.decisions ADD entry_price DOUBLE",

    // ---- Orders / fills. partition = bucket_day. ----
    "CREATE TABLE IF NOT EXISTS polymarket_btc.orders ( \
        bucket_day  TIMESTAMP, \
        ts          TIMESTAMP, \
        order_id    TEXT, \
        decision_id UUID, \
        market_slug TEXT, \
        side        TEXT, \
        size        DOUBLE, \
        price       DOUBLE, \
        status      TEXT, \
        fill_size   DOUBLE, \
        fill_price  DOUBLE, \
        PRIMARY KEY (bucket_day, ts, order_id) \
     )",
    "CREATE INDEX idx_orders_market ON polymarket_btc.orders (market_slug)",
    "CREATE INDEX idx_orders_status ON polymarket_btc.orders (status)",

    // ---- Current positions (overwritten via LWT). ----
    // v1: kept for backward-compat with rows that already landed before
    // the v2 schema; the runtime no longer writes here.
    "CREATE TABLE IF NOT EXISTS polymarket_btc.positions ( \
        market_slug TEXT PRIMARY KEY, \
        side        TEXT, \
        size        DOUBLE, \
        avg_price   DOUBLE, \
        updated_at  TIMESTAMP \
     )",
    // v2: PK is (market_slug, side) so YES and NO positions on the
    // same market are tracked independently. `apply_fill` does a
    // read-modify-write to maintain volume-weighted average price +
    // cumulative size across fills.
    "CREATE TABLE IF NOT EXISTS polymarket_btc.positions_v2 ( \
        market_slug TEXT, \
        side        TEXT, \
        size        DOUBLE, \
        avg_price   DOUBLE, \
        updated_at  TIMESTAMP, \
        PRIMARY KEY (market_slug, side) \
     )",

    // ---- Daily PnL summary. CoreDB has no DATE type so day uses TIMESTAMP. ----
    "CREATE TABLE IF NOT EXISTS polymarket_btc.pnl_daily ( \
        day          TIMESTAMP PRIMARY KEY, \
        realized     DOUBLE, \
        unrealized   DOUBLE, \
        n_trades     INT, \
        llm_cost_usd DOUBLE \
     )",

    // ---- Per-strategy PnL snapshots. One row per (compare-pnl run, strategy).
    // partition = bucket_day so a day's worth of snapshots clusters together.
    // Mark-to-market only; resolution-based realized PnL goes into pnl_daily.
    "CREATE TABLE IF NOT EXISTS polymarket_btc.strategy_pnl_snapshots ( \
        bucket_day   TIMESTAMP, \
        ts           TIMESTAMP, \
        strategy     TEXT, \
        n_decisions  INT, \
        sum_size_usd DOUBLE, \
        sum_pnl      DOUBLE, \
        n_yes        INT, \
        n_no         INT, \
        n_pass       INT, \
        PRIMARY KEY (bucket_day, ts, strategy) \
     )",
];
