//! LLM-driven decision path. Asks DeepSeek (or whatever backend is
//! configured) to pick YES / NO / PASS for one market.
//!
//! Same output shape as [`crate::backtest::baseline::BaselineDecision`]
//! so [`crate::backtest::run`] can swap between the two by mode without
//! touching the storage step. We deliberately keep the call sequential
//! (one market per request) for v1 — at ~33 markets per backtest it's
//! still under a minute, and the per-request prompt is short enough
//! that parallelism would mostly buy noise.
//!
//! Failure modes are folded into a PASS decision rather than bubbled,
//! so a single API hiccup doesn't bail the whole batch.

use anyhow::Result;
use serde::Deserialize;

use crate::api::{
    ApiClient, ContentBlock, CreateMessageRequest, Message, MessageRole,
};
use crate::coredb::types::Market;

/// Same shape as [`crate::backtest::baseline::BaselineDecision`] plus
/// the raw LLM response we want to persist in
/// `polymarket_btc.decisions.raw_response`.
#[derive(Debug, Clone)]
pub struct LlmDecision {
    pub side: String, // "YES" | "NO" | "PASS"
    pub size_usd: f64,
    pub confidence: f64,
    pub edge_bps: i32,
    pub reasoning: String,
    pub raw_response: String,
}

/// LLM-side JSON shape. Mirrors the natural decision fields; we accept
/// `side` case-insensitively and clamp `size_usd` / `confidence` to
/// safe ranges downstream.
#[derive(Debug, Deserialize)]
struct DecisionPayload {
    side: String,
    #[serde(default)]
    size_usd: f64,
    #[serde(default)]
    confidence: f64,
    #[serde(default)]
    edge_bps: i32,
    #[serde(default)]
    reasoning: String,
}

const SYSTEM_PROMPT: &str = r#"You are a quantitative trading assistant evaluating short-duration Polymarket BTC markets.

For each market you are given:
- The market question (e.g. "Will Bitcoin be above $80,000 on May 15?")
- The market's current YES price in [0.0, 1.0] — the market's implied probability of the YES outcome.
- The current BTC spot price in USD.
- The market's end date (UTC ms epoch).

Output STRICTLY a JSON object with these fields and nothing else (no prose, no markdown, no code fences):

{
  "side": "YES" | "NO" | "PASS",
  "size_usd": number,        // 0.0 to 100.0 (paper trading cap)
  "confidence": number,      // 0.0 to 1.0
  "edge_bps": integer,       // signed; positive = YES is mispriced low
  "reasoning": string        // <= 200 chars, factual not narrative
}

Pick PASS unless you see a real mispricing. PASS with size_usd=0 when:
- The question can't be cleanly interpreted as a BTC price threshold + deadline.
- Spot is within a small band of the threshold (low signal).
- The YES price already reflects the obvious answer (e.g. spot >> threshold and yes_price > 0.93).

For YES/NO: scale size_usd by your confidence and edge magnitude; never exceed 100.

Respond with only the JSON object."#;

/// Run one LLM evaluation. Returns a decision that's always safe to
/// store — on any API / parsing failure we fall back to a PASS
/// containing the raw response (or error) in `reasoning` so the row
/// remains useful for postmortem.
pub async fn evaluate(
    market: &Market,
    btc_price: f64,
    client: &dyn ApiClient,
    model: &str,
    max_tokens: u32,
) -> LlmDecision {
    let user_prompt = format!(
        "market_question: {question}\n\
         yes_price: {yes_price:.4}\n\
         btc_spot_usd: {btc_price:.2}\n\
         end_date_ms: {end_date}\n",
        question = market.question,
        yes_price = market.last_price.clamp(0.0, 1.0),
        btc_price = btc_price,
        end_date = market.end_date_ms,
    );

    let request = CreateMessageRequest {
        model: model.to_string(),
        max_tokens,
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text { text: user_prompt }],
        }],
        system: Some(SYSTEM_PROMPT.to_string()),
        tools: None,
        stream: None,
    };

    let response = match client.create_message(request).await {
        Ok(r) => r,
        Err(e) => {
            return pass_with_reason(
                format!("llm api error: {e}"),
                format!("error: {e}"),
            );
        }
    };

    // Collect any text the model returned. DeepSeek almost always
    // single-blocks the JSON; we still concatenate defensively in case
    // a future backend chunks it.
    let raw: String = response
        .content
        .iter()
        .filter_map(|b| match b {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    if raw.trim().is_empty() {
        return pass_with_reason("llm returned no text".to_string(), String::new());
    }

    let json_slice = match extract_json_object(&raw) {
        Some(s) => s,
        None => {
            return pass_with_reason(
                format!("no JSON object found in llm response: {}", truncate(&raw, 160)),
                raw,
            );
        }
    };

    let payload: DecisionPayload = match serde_json::from_str(json_slice) {
        Ok(p) => p,
        Err(e) => {
            return pass_with_reason(
                format!("llm JSON parse failed: {e}; slice={}", truncate(json_slice, 160)),
                raw,
            );
        }
    };

    let side = normalize_side(&payload.side);
    let size_usd = if side == "PASS" { 0.0 } else { payload.size_usd.clamp(0.0, 100.0) };
    let confidence = payload.confidence.clamp(0.0, 1.0);

    LlmDecision {
        side: side.to_string(),
        size_usd,
        confidence,
        edge_bps: payload.edge_bps,
        reasoning: truncate(&payload.reasoning, 800),
        raw_response: raw,
    }
}

fn pass_with_reason(reason: String, raw_response: String) -> LlmDecision {
    LlmDecision {
        side: "PASS".to_string(),
        size_usd: 0.0,
        confidence: 0.0,
        edge_bps: 0,
        reasoning: reason,
        raw_response,
    }
}

fn normalize_side(s: &str) -> &'static str {
    match s.trim().to_ascii_uppercase().as_str() {
        "YES" => "YES",
        "NO" => "NO",
        _ => "PASS",
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push_str("...");
    out
}

/// Pull the first balanced `{...}` object out of `text`. Used because
/// models sometimes wrap JSON in code fences or prefix it with a stray
/// "Here's the decision:" — we want to survive that without forcing
/// the backend to expose `response_format: json_object`.
fn extract_json_object(text: &str) -> Option<&str> {
    let start = text.find('{')?;
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut in_str = false;
    let mut escape = false;
    for i in start..bytes.len() {
        let b = bytes[i];
        if in_str {
            if escape {
                escape = false;
            } else if b == b'\\' {
                escape = true;
            } else if b == b'"' {
                in_str = false;
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[start..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_bare_json() {
        let s = r#"{"side":"YES","size_usd":10}"#;
        assert_eq!(extract_json_object(s), Some(s));
    }

    #[test]
    fn extracts_from_code_fence() {
        let s = "```json\n{\"side\":\"NO\",\"size_usd\":5}\n```";
        let extracted = extract_json_object(s).unwrap();
        assert!(extracted.starts_with('{'));
        assert!(extracted.ends_with('}'));
        let p: DecisionPayload = serde_json::from_str(extracted).unwrap();
        assert_eq!(p.side, "NO");
        assert_eq!(p.size_usd, 5.0);
    }

    #[test]
    fn handles_nested_braces() {
        let s = r#"prefix {"side":"YES","reasoning":"a {b} c","size_usd":1} suffix"#;
        let extracted = extract_json_object(s).unwrap();
        let p: DecisionPayload = serde_json::from_str(extracted).unwrap();
        assert_eq!(p.side, "YES");
        assert_eq!(p.reasoning, "a {b} c");
    }

    #[test]
    fn handles_escaped_quotes_inside_string() {
        let s = r#"{"side":"NO","reasoning":"he said \"hi\"","size_usd":2}"#;
        let extracted = extract_json_object(s).unwrap();
        let p: DecisionPayload = serde_json::from_str(extracted).unwrap();
        assert_eq!(p.reasoning, "he said \"hi\"");
    }

    #[test]
    fn missing_object_returns_none() {
        assert!(extract_json_object("no braces here").is_none());
    }

    #[test]
    fn normalize_side_is_case_insensitive() {
        assert_eq!(normalize_side("yes"), "YES");
        assert_eq!(normalize_side(" No  "), "NO");
        assert_eq!(normalize_side("maybe"), "PASS");
    }
}
