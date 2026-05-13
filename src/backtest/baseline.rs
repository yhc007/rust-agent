//! Deterministic baseline: compare BTC spot vs each market's question.
//!
//! The rule is intentionally crude — it exists so that the agent's
//! DeepSeek-driven decisions can be evaluated against *something*. The
//! key insight we encode: when a market asks "BTC above $X by date D"
//! and the spot price is already comfortably above $X, the YES side is
//! mispriced if its implied probability is much below ~0.95. Inverse
//! for "comfortably below".
//!
//! Threshold extraction is regex-based and best-effort. Markets whose
//! question we can't parse a USD threshold from are marked PASS.

use regex::Regex;

use crate::coredb::types::Market;

#[derive(Debug, Clone)]
pub struct BaselineDecision {
    pub side: &'static str, // "YES" | "NO" | "PASS"
    pub size_usd: f64,
    pub confidence: f64,
    pub edge_bps: i32,
    pub reasoning: String,
}

pub fn evaluate(market: &Market, btc_price: f64) -> BaselineDecision {
    let Some(threshold) = extract_usd_threshold(&market.question) else {
        return BaselineDecision {
            side: "PASS",
            size_usd: 0.0,
            confidence: 0.0,
            edge_bps: 0,
            reasoning: "no parseable USD threshold in question".to_string(),
        };
    };

    let yes_price = if market.last_price > 0.0 {
        market.last_price.clamp(0.0, 1.0)
    } else {
        return BaselineDecision {
            side: "PASS",
            size_usd: 0.0,
            confidence: 0.0,
            edge_bps: 0,
            reasoning: "no last price on market".to_string(),
        };
    };

    let above = btc_price >= threshold;
    let gap = (btc_price - threshold).abs() / threshold;

    // Implied prob estimate: comfortably (>5%) above → 0.95, below → 0.05.
    // Within 5% on either side → 0.5 (coin-flip).
    let implied_prob = if gap < 0.05 {
        0.5
    } else if above {
        0.95
    } else {
        0.05
    };
    let edge = implied_prob - yes_price;

    let (side, size_usd, reason) = if edge > 0.05 {
        (
            "YES",
            (edge * 100.0).min(50.0),
            format!(
                "spot {btc_price:.0} {} threshold {threshold:.0}; \
                 implied_prob {implied_prob:.2} vs yes_price {yes_price:.3}",
                if above { ">=" } else { "<" }
            ),
        )
    } else if edge < -0.05 {
        (
            "NO",
            ((-edge) * 100.0).min(50.0),
            format!(
                "spot {btc_price:.0} {} threshold {threshold:.0}; \
                 implied_prob {implied_prob:.2} vs yes_price {yes_price:.3}",
                if above { ">=" } else { "<" }
            ),
        )
    } else {
        (
            "PASS",
            0.0,
            format!(
                "edge {:.3} within ±0.05 band — no clear mispricing",
                edge
            ),
        )
    };

    BaselineDecision {
        side,
        size_usd,
        confidence: edge.abs().min(1.0),
        edge_bps: (edge * 10_000.0) as i32,
        reasoning: reason,
    }
}

/// Pull a USD figure from market questions like "BTC above $150,000 by
/// June 30" or "Will Bitcoin hit 78k on May 13". Returns the parsed
/// dollar value or None.
pub fn extract_usd_threshold(q: &str) -> Option<f64> {
    // Patterns we accept:
    //   $150,000   →  150000
    //   $78k       →  78000
    //   150k       →  150000
    //   $1.2m      →  1200000
    let re = Regex::new(
        r"(?ix)
        \$?\s*
        (?P<num>[0-9]+(?:[\.,][0-9]+)*)
        \s*
        (?P<suf>[kKmM])?
        ",
    )
    .ok()?;
    let mut best: Option<f64> = None;
    for cap in re.captures_iter(q) {
        let raw = cap.name("num")?.as_str().replace(',', "");
        let mut n: f64 = raw.parse().ok()?;
        match cap.name("suf").map(|m| m.as_str()) {
            Some("k") | Some("K") => n *= 1_000.0,
            Some("m") | Some("M") => n *= 1_000_000.0,
            _ => {}
        }
        // Filter out obvious non-price numbers (years like 2026,
        // small ints like "by 5"). BTC questions cluster in the
        // 1e4–1e6 range.
        if (10_000.0..=10_000_000.0).contains(&n) {
            best = Some(n);
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pulls_dollar_with_k_suffix() {
        assert_eq!(extract_usd_threshold("BTC > 78k on May 13"), Some(78_000.0));
    }
    #[test]
    fn pulls_dollar_with_commas() {
        assert_eq!(
            extract_usd_threshold("Bitcoin hit $150,000 by 2026"),
            Some(150_000.0)
        );
    }
    #[test]
    fn ignores_year_numbers() {
        // 2026 should be filtered out — not in 10k..10m band.
        assert_eq!(extract_usd_threshold("Bitcoin in 2026"), None);
    }
}
