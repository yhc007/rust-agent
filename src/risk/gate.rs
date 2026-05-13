//! Risk-gate rules — checked on every order regardless of source.
//!
//! Knobs (all env-overridable):
//! - `RISK_MAX_ORDER_USD`   single order notional limit (default $50)
//! - `RISK_KILL_PATH`       if this path exists, every order is blocked
//!                          (default `./KILL` — drop the file to stop)
//!
//! Side must be YES or NO; PASS is for decisions only and never reaches
//! this code path.

use std::path::PathBuf;

pub struct RiskLimits {
    pub max_order_usd: f64,
    pub kill_switch_path: PathBuf,
}

impl Default for RiskLimits {
    fn default() -> Self {
        let max_order_usd = std::env::var("RISK_MAX_ORDER_USD")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(50.0);
        let kill_switch_path = PathBuf::from(
            std::env::var("RISK_KILL_PATH").unwrap_or_else(|_| "./KILL".to_string()),
        );
        Self {
            max_order_usd,
            kill_switch_path,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub market_slug: String,
    pub side: String,
    pub size_usd: f64,
    pub price: f64,
}

#[derive(Debug)]
pub enum RiskVerdict {
    Allow,
    Block(String),
}

pub fn evaluate(req: &OrderRequest, limits: &RiskLimits) -> RiskVerdict {
    if limits.kill_switch_path.exists() {
        return RiskVerdict::Block(format!(
            "kill switch active: {}",
            limits.kill_switch_path.display()
        ));
    }
    if !matches!(req.side.as_str(), "YES" | "NO") {
        return RiskVerdict::Block(format!(
            "side must be YES or NO; got `{}`",
            req.side
        ));
    }
    if req.size_usd <= 0.0 {
        return RiskVerdict::Block("size_usd must be > 0".into());
    }
    if req.size_usd > limits.max_order_usd {
        return RiskVerdict::Block(format!(
            "size_usd ${:.2} exceeds RISK_MAX_ORDER_USD ${:.2}",
            req.size_usd, limits.max_order_usd
        ));
    }
    if !(0.0..=1.0).contains(&req.price) {
        return RiskVerdict::Block(format!(
            "price must be in [0,1] (Polymarket outcome); got {}",
            req.price
        ));
    }
    RiskVerdict::Allow
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_req() -> OrderRequest {
        OrderRequest {
            market_slug: "x".into(),
            side: "YES".into(),
            size_usd: 10.0,
            price: 0.5,
        }
    }

    #[test]
    fn allows_in_band_order() {
        let l = RiskLimits {
            max_order_usd: 50.0,
            kill_switch_path: "/tmp/__nonexistent_kill__".into(),
        };
        assert!(matches!(evaluate(&ok_req(), &l), RiskVerdict::Allow));
    }

    #[test]
    fn blocks_oversize_order() {
        let l = RiskLimits {
            max_order_usd: 5.0,
            kill_switch_path: "/tmp/__nonexistent_kill__".into(),
        };
        assert!(matches!(evaluate(&ok_req(), &l), RiskVerdict::Block(_)));
    }

    #[test]
    fn blocks_bad_side() {
        let l = RiskLimits::default();
        let mut r = ok_req();
        r.side = "MAYBE".into();
        assert!(matches!(evaluate(&r, &l), RiskVerdict::Block(_)));
    }
}
