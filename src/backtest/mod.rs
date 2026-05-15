//! Single-pass batch decision runner ("backtest" in name only at the
//! moment — a real time-replay backtest needs months of orderbook
//! history we don't have yet).
//!
//! Snapshots every open BTC market in CoreDB and produces one Decision
//! per market using either the deterministic [`baseline`] rule or one
//! or more LLM strategies (DeepSeek by default; N-way comparison via
//! `--llms <preset1>,<preset2>`). Every produced row lands in
//! `polymarket_btc.decisions` with an explicit `strategy` label so
//! downstream `compare-pnl` / `settle-pnl` can group it correctly.

pub mod baseline;
pub mod compare;
pub mod history;
pub mod llm;
pub mod run;
pub mod settle;

/// What [`run::run`] should execute against each open market.
///
/// All paths land their rows in the same `polymarket_btc.decisions`
/// table and share the same `ts_ms` / `entry_price` within a single
/// market iteration, so the rows are timing-honest for downstream
/// comparison even when multiple strategies are in play.
///
/// CLI mapping:
/// - default → `baseline_only()`
/// - `--llm` → `default_llm()` (single LLM, no baseline)
/// - `--both` → `both()` (baseline + single default LLM)
/// - `--llms <preset1>,<preset2>` → `multi(false, ...)` (no baseline by default)
/// - `--llms ... --both` → `multi(true, ...)` (baseline + N LLMs)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BacktestPlan {
    /// Run the deterministic baseline rule per market.
    pub include_baseline: bool,
    /// Each entry is a strategy preset name resolved by
    /// [`run::resolve_llm_presets`]. Empty Vec means "no LLM, only
    /// baseline". A single empty-string entry is treated as a request
    /// for the legacy default-LLM path (config from env, label "llm").
    pub llm_presets: Vec<String>,
}

impl BacktestPlan {
    pub fn baseline_only() -> Self {
        Self { include_baseline: true, llm_presets: Vec::new() }
    }
    /// Legacy `--llm`: single LLM, no baseline. The LLM is resolved
    /// from env exactly as before, and its decisions are labeled
    /// `"llm"` so they stay grouped with historical rows.
    pub fn default_llm() -> Self {
        Self { include_baseline: false, llm_presets: vec![String::new()] }
    }
    /// Legacy `--both`: baseline + the single env-resolved LLM.
    pub fn both() -> Self {
        Self { include_baseline: true, llm_presets: vec![String::new()] }
    }
    /// N-way: caller specifies which presets to run, and whether to
    /// also include the baseline.
    pub fn multi(include_baseline: bool, presets: Vec<String>) -> Self {
        Self { include_baseline, llm_presets: presets }
    }

    pub fn wants_baseline(&self) -> bool {
        self.include_baseline
    }
    pub fn wants_llm(&self) -> bool {
        !self.llm_presets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baseline_only_excludes_llm() {
        let p = BacktestPlan::baseline_only();
        assert!(p.wants_baseline());
        assert!(!p.wants_llm());
    }

    #[test]
    fn default_llm_excludes_baseline() {
        let p = BacktestPlan::default_llm();
        assert!(!p.wants_baseline());
        assert!(p.wants_llm());
        assert_eq!(p.llm_presets.len(), 1);
        assert!(p.llm_presets[0].is_empty(), "empty-string preset = env default");
    }

    #[test]
    fn both_includes_baseline_and_default_llm() {
        let p = BacktestPlan::both();
        assert!(p.wants_baseline());
        assert!(p.wants_llm());
        assert_eq!(p.llm_presets, vec![String::new()]);
    }

    #[test]
    fn multi_carries_explicit_presets() {
        let p = BacktestPlan::multi(false, vec!["deepseek".into(), "anthropic".into()]);
        assert!(!p.wants_baseline());
        assert_eq!(p.llm_presets, vec!["deepseek", "anthropic"]);
    }

    #[test]
    fn multi_can_include_baseline() {
        let p = BacktestPlan::multi(true, vec!["openai".into()]);
        assert!(p.wants_baseline());
        assert!(p.wants_llm());
    }
}

