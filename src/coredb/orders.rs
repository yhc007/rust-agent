//! Order lifecycle + current-position repositories (inline-CQL).

use std::sync::Arc;

use scylla::frame::value::CqlTimestamp;
use scylla::Session;
use uuid::Uuid;

use super::error::CoreDbError;
use super::types::{Millis, Order, Position};
use super::util::{esc, fuuid};

pub struct OrderRepo {
    session: Arc<Session>,
}

impl OrderRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    pub async fn insert(&self, o: &Order) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.orders \
             (bucket_day, ts, order_id, decision_id, market_slug, side, size, price, \
              status, fill_size, fill_price) \
             VALUES ({bd}, {ts}, {oid}, {did}, {slug}, {side}, {size}, {price}, {st}, {fs}, {fp})",
            bd = o.bucket_day_ms,
            ts = o.ts_ms,
            oid = esc(&o.order_id),
            did = fuuid(o.decision_id),
            slug = esc(&o.market_slug),
            side = esc(&o.side),
            size = o.size,
            price = o.price,
            st = esc(&o.status),
            fs = o.fill_size,
            fp = o.fill_price,
        );
        self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("orders.insert: {e}")))?;
        Ok(())
    }

    pub async fn list_day(&self, bucket_day_ms: Millis) -> Result<Vec<Order>, CoreDbError> {
        let q = format!(
            "SELECT bucket_day, ts, order_id, decision_id, market_slug, side, size, price, \
                    status, fill_size, fill_price \
             FROM polymarket_btc.orders WHERE bucket_day = {bucket_day_ms}"
        );
        let qr = self
            .session
            .query_unpaged(q, ())
            .await
            .map_err(|e| CoreDbError::Query(format!("orders.list_day: {e}")))?;
        let rows = qr
            .into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("orders.list_day rows: {e}")))?;
        if rows.rows_num() == 0 {
            return Ok(Vec::new());
        }
        // CoreDB returns columns in HashMap-iteration order, not SELECT-list
        // order — see the equivalent shim in decisions.rs. Name-keyed
        // DeserializeRow sidesteps the reshuffle.
        let typed = rows
            .rows::<OrderRow>()
            .map_err(|e| CoreDbError::Query(format!("orders.list_day typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("orders row: {e}")))?;
            out.push(Order {
                bucket_day_ms: r.bucket_day.map(|t| t.0).unwrap_or(0),
                ts_ms: r.ts.map(|t| t.0).unwrap_or(0),
                order_id: r.order_id.unwrap_or_default(),
                decision_id: r.decision_id.unwrap_or_else(Uuid::nil),
                market_slug: r.market_slug.unwrap_or_default(),
                side: r.side.unwrap_or_default(),
                size: r.size.unwrap_or(0.0),
                price: r.price.unwrap_or(0.0),
                status: r.status.unwrap_or_default(),
                fill_size: r.fill_size.unwrap_or(0.0),
                fill_price: r.fill_price.unwrap_or(0.0),
            });
        }
        Ok(out)
    }
}

#[derive(scylla::DeserializeRow)]
struct OrderRow {
    bucket_day: Option<CqlTimestamp>,
    ts: Option<CqlTimestamp>,
    order_id: Option<String>,
    decision_id: Option<Uuid>,
    market_slug: Option<String>,
    side: Option<String>,
    size: Option<f64>,
    price: Option<f64>,
    status: Option<String>,
    fill_size: Option<f64>,
    fill_price: Option<f64>,
}

pub struct PositionRepo {
    session: Arc<Session>,
}

impl PositionRepo {
    pub async fn new(session: Arc<Session>) -> Result<Self, CoreDbError> {
        Ok(Self { session })
    }

    /// Raw overwrite: replaces whatever's stored under `(market_slug,
    /// side)` with the supplied row. Useful for tests / one-shot
    /// scripts; runtime fill handling should go through
    /// [`Self::apply_fill`] so the average price actually accumulates.
    pub async fn upsert(&self, p: &Position) -> Result<(), CoreDbError> {
        let q = format!(
            "INSERT INTO polymarket_btc.positions_v2 \
             (market_slug, side, size, avg_price, updated_at) \
             VALUES ({slug}, {side}, {size}, {avg}, {upd})",
            slug = esc(&p.market_slug),
            side = esc(&p.side),
            size = p.size,
            avg = p.avg_price,
            upd = p.updated_at_ms,
        );
        self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("positions.upsert: {e}")))?;
        Ok(())
    }

    /// Apply a fresh fill on top of any existing position for the
    /// `(market_slug, side)` pair. Reads the current row, combines via
    /// [`combine_fills`] (volume-weighted average price, cumulative
    /// size), writes back. This is not atomic — two concurrent fills
    /// on the same (slug, side) can race — but for the agent's
    /// few-fills-per-minute paper workload that's fine, and live
    /// orderflow doesn't get more than one in-flight order per market
    /// per backtest tick.
    pub async fn apply_fill(&self, fill: &Position) -> Result<(), CoreDbError> {
        let prior = self.get(&fill.market_slug, &fill.side).await?;
        let next = match prior {
            Some(existing) => combine_fills(&existing, fill),
            None => fill.clone(),
        };
        self.upsert(&next).await
    }

    /// Read a single position by `(market_slug, side)`. Returns `None`
    /// when no row exists yet. Used by `apply_fill` for the RMW; also
    /// handy from external callers.
    pub async fn get(&self, market_slug: &str, side: &str) -> Result<Option<Position>, CoreDbError> {
        let q = format!(
            "SELECT market_slug, side, size, avg_price, updated_at \
             FROM polymarket_btc.positions_v2 \
             WHERE market_slug = {slug} AND side = {side}",
            slug = esc(market_slug),
            side = esc(side),
        );
        let qr = self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("positions.get: {e}")))?;
        let rows = qr.into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("positions.get rows: {e}")))?;
        // CoreDB returns zero-column metadata on empty result sets,
        // which scylla's typed-row check rejects. Short-circuit before
        // we ever ask for typed rows.
        if rows.rows_num() == 0 {
            return Ok(None);
        }
        let typed = rows
            .rows::<PositionRow>()
            .map_err(|e| CoreDbError::Query(format!("positions.get typed: {e}")))?;
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("positions row: {e}")))?;
            return Ok(Some(Position {
                market_slug: r.market_slug.unwrap_or_default(),
                side: r.side.unwrap_or_default(),
                size: r.size.unwrap_or(0.0),
                avg_price: r.avg_price.unwrap_or(0.0),
                updated_at_ms: r.updated_at.map(|t| t.0).unwrap_or(0),
            }));
        }
        Ok(None)
    }

    pub async fn list_all(&self) -> Result<Vec<Position>, CoreDbError> {
        let q = "SELECT market_slug, side, size, avg_price, updated_at \
                 FROM polymarket_btc.positions_v2";
        let qr = self.session.query_unpaged(q, ()).await
            .map_err(|e| CoreDbError::Query(format!("positions.list_all: {e}")))?;
        let rows = qr.into_rows_result()
            .map_err(|e| CoreDbError::Query(format!("positions.list_all rows: {e}")))?;
        // Empty rowset short-circuit: CoreDB advertises 0 columns when
        // no rows exist, which scylla's typed-row check rejects. See
        // `get` for the same pattern.
        if rows.rows_num() == 0 {
            return Ok(Vec::new());
        }
        // Name-keyed: CoreDB returns columns in HashMap iteration order so
        // tuple-position deserialization is unsafe.
        let typed = rows
            .rows::<PositionRow>()
            .map_err(|e| CoreDbError::Query(format!("positions.list_all typed: {e}")))?;
        let mut out = Vec::new();
        for row in typed {
            let r = row.map_err(|e| CoreDbError::Query(format!("positions row: {e}")))?;
            out.push(Position {
                market_slug: r.market_slug.unwrap_or_default(),
                side: r.side.unwrap_or_default(),
                size: r.size.unwrap_or(0.0),
                avg_price: r.avg_price.unwrap_or(0.0),
                updated_at_ms: r.updated_at.map(|t| t.0).unwrap_or(0),
            });
        }
        Ok(out)
    }
}

#[derive(scylla::DeserializeRow)]
struct PositionRow {
    market_slug: Option<String>,
    side: Option<String>,
    size: Option<f64>,
    avg_price: Option<f64>,
    updated_at: Option<CqlTimestamp>,
}

/// Volume-weighted combination of a prior position and a new fill.
/// New row inherits the slug/side from the prior (caller is
/// responsible for matching on those before calling) and stamps
/// `updated_at_ms` from the new fill.
///
///     size_new   = old.size + fill.size
///     avg_new    = (old.size*old.avg + fill.size*fill.avg) / size_new
///
/// Defensive corners:
/// - When the prior row has zero size (or the fill does), the other
///   side carries through unchanged.
/// - When the total size sums to zero (both empty, or floating-point
///   underflow), avg_price falls back to whichever side had a price.
pub fn combine_fills(prior: &Position, fill: &Position) -> Position {
    let total = prior.size + fill.size;
    let avg = if total > 0.0 {
        (prior.size * prior.avg_price + fill.size * fill.avg_price) / total
    } else if fill.avg_price > 0.0 {
        fill.avg_price
    } else {
        prior.avg_price
    };
    Position {
        market_slug: prior.market_slug.clone(),
        side: prior.side.clone(),
        size: total,
        avg_price: avg,
        updated_at_ms: fill.updated_at_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(side: &str, size: f64, avg: f64, ts: i64) -> Position {
        Position {
            market_slug: "m".into(),
            side: side.into(),
            size,
            avg_price: avg,
            updated_at_ms: ts,
        }
    }

    #[test]
    fn first_fill_passes_through() {
        let prior = pos("YES", 0.0, 0.0, 0);
        let fill = pos("YES", 100.0, 0.4, 1);
        let out = combine_fills(&prior, &fill);
        assert_eq!(out.size, 100.0);
        assert!((out.avg_price - 0.4).abs() < 1e-12);
        assert_eq!(out.updated_at_ms, 1);
    }

    #[test]
    fn vwap_accumulates_correctly() {
        // 100 shares at 0.4 then 100 shares at 0.6 → 200 shares at 0.5.
        let prior = pos("YES", 100.0, 0.4, 1);
        let fill = pos("YES", 100.0, 0.6, 2);
        let out = combine_fills(&prior, &fill);
        assert_eq!(out.size, 200.0);
        assert!((out.avg_price - 0.5).abs() < 1e-12);
        assert_eq!(out.updated_at_ms, 2);
    }

    #[test]
    fn unequal_sizes_weight_correctly() {
        // 30 at 0.2, then 70 at 0.9 → 100 at 0.69.
        let prior = pos("NO", 30.0, 0.2, 1);
        let fill = pos("NO", 70.0, 0.9, 2);
        let out = combine_fills(&prior, &fill);
        assert_eq!(out.size, 100.0);
        assert!((out.avg_price - 0.69).abs() < 1e-12, "{}", out.avg_price);
    }

    #[test]
    fn slug_and_side_carry_over() {
        let prior = pos("YES", 10.0, 0.3, 1);
        let fill = pos("YES", 5.0, 0.5, 2);
        let out = combine_fills(&prior, &fill);
        assert_eq!(out.market_slug, "m");
        assert_eq!(out.side, "YES");
    }

    #[test]
    fn both_empty_does_not_nan() {
        let prior = pos("YES", 0.0, 0.0, 0);
        let fill = pos("YES", 0.0, 0.0, 1);
        let out = combine_fills(&prior, &fill);
        assert_eq!(out.size, 0.0);
        assert_eq!(out.avg_price, 0.0);
    }
}
