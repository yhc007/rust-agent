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
        slug        TEXT PRIMARY KEY, \
        question    TEXT, \
        end_date    TIMESTAMP, \
        outcomes    TEXT, \
        closed      BOOLEAN, \
        last_price  DOUBLE, \
        updated_at  TIMESTAMP \
     )",
    "CREATE INDEX idx_markets_open ON polymarket_btc.markets (closed)",

    // ---- Agent decisions (LLM outputs). partition = bucket_day. ----
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
        PRIMARY KEY (bucket_day, ts, decision_id) \
     )",
    "CREATE INDEX idx_decisions_market ON polymarket_btc.decisions (market_slug)",

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
    "CREATE TABLE IF NOT EXISTS polymarket_btc.positions ( \
        market_slug TEXT PRIMARY KEY, \
        side        TEXT, \
        size        DOUBLE, \
        avg_price   DOUBLE, \
        updated_at  TIMESTAMP \
     )",

    // ---- Daily PnL summary. CoreDB has no DATE type so day uses TIMESTAMP. ----
    "CREATE TABLE IF NOT EXISTS polymarket_btc.pnl_daily ( \
        day          TIMESTAMP PRIMARY KEY, \
        realized     DOUBLE, \
        unrealized   DOUBLE, \
        n_trades     INT, \
        llm_cost_usd DOUBLE \
     )",
];
