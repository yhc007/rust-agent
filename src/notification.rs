//! Best-effort outbound notifications for high-signal trading decisions.
//!
//! Phase 1 of the "recommendation solution" evolution path: every
//! `Decision` that passes the configured confidence + edge filters
//! gets a Slack webhook ping right after `route_decision` returns,
//! so an operator can see what the bot is doing (and what it WOULD
//! do in live mode) without keeping the dashboard open.
//!
//! Design:
//!
//! - **Opt-in**: `SLACK_WEBHOOK_URL` env unset → entirely disabled,
//!   no HTTP calls, no overhead. Existing paper / live flows are
//!   byte-for-byte identical when notifications are off.
//! - **Fire-and-forget**: a webhook failure (network, 4xx, timeout)
//!   never bubbles up. Trading must not break because Slack is down.
//! - **Filtered**: PASS decisions and below-threshold confidence /
//!   edge get dropped at the source so the channel doesn't drown
//!   in low-signal noise.
//! - **Decision-shaped**: the message uses the `Decision`'s own
//!   fields (strategy, side, size, entry_price, edge_bps, confidence,
//!   reasoning) — no separate prediction layer, the LLM's own
//!   `reasoning` string is what shows up.
//!
//! The current sink is Slack-compatible (POST `{ "text": "..." }`).
//! Discord and Mattermost incoming webhooks accept the same shape;
//! Telegram needs a thin adapter that we'll add when actually needed.

use serde_json::json;

use crate::coredb::types::Decision;

/// Operator-tunable filter thresholds + the sink URL. Built once at
/// task startup via [`NotificationConfig::from_env`] and passed by
/// reference to every notify call — no per-call env reads on the
/// hot path.
#[derive(Debug, Clone)]
pub struct NotificationConfig {
    /// Webhook endpoint. `None` ⇒ feature disabled.
    pub webhook_url: Option<String>,
    /// Minimum decision confidence (0.0–1.0) to surface. Default 0.7
    /// — comfortably above the 0.5-anchored baseline rule's typical
    /// output, so LLM strategies that express stronger conviction
    /// rise to the top.
    pub min_confidence: f64,
    /// Minimum |edge_bps| to surface. 500 bps = 5 % implied-prob
    /// gap, a reasonable filter for "this is meaningfully different
    /// from market pricing".
    pub min_edge_bps: i32,
    /// Include PASS decisions. Off by default — PASS is not
    /// actionable and would drown the channel in non-bets.
    pub include_pass: bool,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            webhook_url: None,
            min_confidence: 0.7,
            min_edge_bps: 500,
            include_pass: false,
        }
    }
}

impl NotificationConfig {
    /// Read every knob from env. Missing / empty / malformed values
    /// fall back to defaults — same forgiving pattern as the other
    /// env-tunable knobs in this codebase.
    pub fn from_env() -> Self {
        let webhook_url = std::env::var("SLACK_WEBHOOK_URL")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let min_confidence = std::env::var("NOTIFY_MIN_CONFIDENCE")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.7);
        let min_edge_bps = std::env::var("NOTIFY_MIN_EDGE_BPS")
            .ok()
            .and_then(|s| s.parse::<i32>().ok())
            .unwrap_or(500);
        let include_pass = std::env::var("NOTIFY_INCLUDE_PASS")
            .ok()
            .map(|s| matches!(s.trim(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        Self {
            webhook_url,
            min_confidence,
            min_edge_bps,
            include_pass,
        }
    }

    /// True iff at least one webhook is configured AND any decision
    /// would pass the filter. Callers use this to skip building the
    /// payload entirely when the feature is off.
    pub fn enabled(&self) -> bool {
        self.webhook_url.is_some()
    }

    /// Returns true iff this Decision is "interesting enough" to
    /// surface. Pure function — no env access, no I/O, fully
    /// unit-testable.
    pub fn passes_filter(&self, d: &Decision) -> bool {
        if d.side == "PASS" && !self.include_pass {
            return false;
        }
        if d.confidence < self.min_confidence {
            return false;
        }
        if d.edge_bps.abs() < self.min_edge_bps {
            return false;
        }
        true
    }
}

/// Compute a normalized 0..1 score for ranking decisions. Pure
/// function combining the two signals each strategy already emits:
/// `confidence` (raw 0..1) and `|edge_bps|` saturated at 5000 bps
/// (50 %) — anything beyond that is so large that further differences
/// don't change the ranking decision. Phase 2's `digest` subcommand
/// sorts by this score; the per-decision Slack notification shows it
/// inline so an operator scrolling the channel can spot the
/// strongest signal at a glance.
///
/// The score is intentionally agnostic to side / strategy / market —
/// it ranks by "how notable is this opinion", not by expected dollar
/// return. PnL realization comes from later phases (digest v2 will
/// join with `pnl_daily` for hindsight calibration).
pub fn decision_score(confidence: f64, edge_bps: i32) -> f64 {
    let edge_norm = (edge_bps.abs() as f64).min(5000.0) / 5000.0;
    (confidence.max(0.0).min(1.0)) * edge_norm
}

/// Format the Slack message body for one decision. Pure function so
/// the formatting can be tested without an HTTP mock.
///
/// `outcome_label` is the post-routing status — "paper-filled",
/// "live-filled", "risk-blocked: limit $50 exceeded", etc. Surfacing
/// it lets the operator see both the recommendation AND whether the
/// bot acted on it without opening the dashboard.
pub fn format_message(decision: &Decision, outcome_label: &str) -> String {
    let arrow = match decision.side.as_str() {
        "YES" => "🟢 YES",
        "NO" => "🔴 NO",
        "PASS" => "⚪ PASS",
        _ => "❓",
    };
    // Reasoning can be long for LLM strategies. Truncate at ~240
    // chars to keep the Slack card scannable.
    let mut reasoning = decision.reasoning.clone();
    if reasoning.chars().count() > 240 {
        let truncated: String = reasoning.chars().take(237).collect();
        reasoning = format!("{truncated}…");
    }
    let score = decision_score(decision.confidence, decision.edge_bps);
    format!(
        "*{}* @ `{}`\n{}  ${:.2}  @{:.4} entry\nscore {:.2} · conf {:.2} · edge {:+} bps · {}\n_{}_",
        decision.strategy,
        decision.market_slug,
        arrow,
        decision.size_usd,
        decision.entry_price,
        score,
        decision.confidence,
        decision.edge_bps,
        outcome_label,
        reasoning,
    )
}

/// Fire one webhook ping if (a) the sink is configured and (b) the
/// decision passes the filter. Never errors out — a Slack outage or
/// a 4xx response should not break the trading pipeline. The 3 s
/// timeout caps how long a stuck webhook can delay the caller.
pub async fn notify_decision(
    decision: &Decision,
    outcome_label: &str,
    cfg: &NotificationConfig,
) {
    let Some(url) = cfg.webhook_url.as_ref() else { return };
    if !cfg.passes_filter(decision) {
        return;
    }
    let payload = json!({ "text": format_message(decision, outcome_label) });
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
    {
        Ok(c) => c,
        Err(_) => return,
    };
    let _ = client.post(url).json(&payload).send().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn d(side: &str, conf: f64, edge_bps: i32) -> Decision {
        Decision {
            bucket_day_ms: 0,
            ts_ms: 0,
            decision_id: Uuid::nil(),
            market_slug: "test-market".into(),
            side: side.into(),
            size_usd: 10.0,
            confidence: conf,
            edge_bps,
            reasoning: "test reasoning".into(),
            raw_response: "test".into(),
            entry_price: 0.5,
            strategy: "baseline".into(),
        }
    }

    #[test]
    fn default_config_has_no_webhook_and_disabled() {
        let c = NotificationConfig::default();
        assert!(!c.enabled());
        assert_eq!(c.min_confidence, 0.7);
        assert_eq!(c.min_edge_bps, 500);
        assert!(!c.include_pass);
    }

    #[test]
    fn from_env_reads_webhook_url() {
        std::env::set_var("SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/X/Y/Z");
        let c = NotificationConfig::from_env();
        assert!(c.enabled());
        std::env::remove_var("SLACK_WEBHOOK_URL");
    }

    #[test]
    fn from_env_empty_url_is_disabled() {
        std::env::set_var("SLACK_WEBHOOK_URL", "");
        let c = NotificationConfig::from_env();
        assert!(!c.enabled());
        std::env::remove_var("SLACK_WEBHOOK_URL");
    }

    #[test]
    fn filter_drops_pass_by_default() {
        let c = NotificationConfig::default();
        assert!(!c.passes_filter(&d("PASS", 1.0, 9999)));
    }

    #[test]
    fn filter_honors_include_pass() {
        let mut c = NotificationConfig::default();
        c.include_pass = true;
        assert!(c.passes_filter(&d("PASS", 0.95, 800)));
    }

    #[test]
    fn filter_drops_low_confidence() {
        let c = NotificationConfig::default();
        // Below default 0.7 threshold.
        assert!(!c.passes_filter(&d("YES", 0.5, 9999)));
        // At threshold ⇒ passes.
        assert!(c.passes_filter(&d("YES", 0.7, 9999)));
    }

    #[test]
    fn filter_drops_low_edge_either_sign() {
        let c = NotificationConfig::default();
        // |edge| below 500 default.
        assert!(!c.passes_filter(&d("YES", 0.9, 100)));
        assert!(!c.passes_filter(&d("NO", 0.9, -100)));
        // At threshold ⇒ passes (abs value).
        assert!(c.passes_filter(&d("NO", 0.9, -500)));
    }

    #[test]
    fn message_contains_key_fields() {
        let msg = format_message(&d("YES", 0.85, 1234), "paper-filled");
        assert!(msg.contains("baseline"));
        assert!(msg.contains("test-market"));
        assert!(msg.contains("YES"));
        assert!(msg.contains("0.85"));
        assert!(msg.contains("+1234"));
        assert!(msg.contains("paper-filled"));
        assert!(msg.contains("test reasoning"));
    }

    #[test]
    fn message_truncates_long_reasoning() {
        let mut dec = d("YES", 0.85, 1234);
        dec.reasoning = "x".repeat(500);
        let msg = format_message(&dec, "live-filled");
        // The reasoning portion (between `_` markers) is bounded
        // at ~240 chars. We check that the long input got squashed.
        assert!(
            msg.chars().count() < 500,
            "message should be truncated; got {} chars",
            msg.chars().count()
        );
        assert!(msg.contains("…"), "expected truncation ellipsis");
    }

    #[test]
    fn notify_with_no_webhook_is_noop() {
        // The function returns without panic when the webhook is
        // missing. We can't observe "no HTTP call happened" cheaply
        // without a mock, but we can at least confirm it doesn't
        // panic / block.
        let cfg = NotificationConfig::default();
        let dec = d("YES", 0.9, 1000);
        // Drive via a quick-and-dirty tokio runtime.
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(notify_decision(&dec, "paper-filled", &cfg));
    }
}
