//! Backend-agnostic API client trait.
//!
//! The QueryEngine drives the conversation in Anthropic-shaped types
//! (Message / ContentBlock / ToolUse / ToolResultBlock). Each backend
//! impl translates to and from its own wire format inside
//! [`ApiClient::create_message`]. This means:
//!
//! - The engine stays one code path regardless of which model/server
//!   is on the other end.
//! - Adding a new backend is one new impl, no touching the loop.
//! - Conversion costs are paid once per request, not per message.
//!
//! Why Anthropic-shape as the canonical type? Two reasons:
//! 1. It encodes tool_use / tool_result as first-class content
//!    blocks, which the engine already relies on for the
//!    "no-more-tool-uses → done" loop-exit condition.
//! 2. It's the type our existing AnthropicClient already produces;
//!    keeping it canonical means zero changes to that impl.
//!
//! When OpenAI's wire format is richer in some dimension (e.g.
//! `logprobs`), we currently drop that field — the engine doesn't
//! consume it. Add an accessor on this trait only when there's a
//! second consumer that needs it.

use anyhow::Result;
use async_trait::async_trait;

use super::{CreateMessageRequest, CreateMessageResponse};

/// One round-trip with the underlying LLM endpoint. Implementors:
///
/// - `AnthropicClient` — native Anthropic messages API.
/// - `OpenAICompatClient` — OpenAI chat-completions API and anything
///   that speaks the same wire format (vLLM, Azure OpenAI,
///   OpenRouter, …). Requires the server to support `tools` /
///   `tool_calls`; for vLLM that means launching with
///   `--enable-auto-tool-choice --tool-call-parser <name>`.
#[async_trait]
pub trait ApiClient: Send + Sync {
    /// Stable identifier for logs/diagnostics. e.g. `"anthropic"`,
    /// `"openai-compat"`, `"vllm:hermes"`.
    fn label(&self) -> &'static str;

    /// Send one request, get one response. The request is in
    /// Anthropic shape; this impl is responsible for translating to
    /// its wire format and back. Tool definitions in the request
    /// already carry JSON Schema — backends that wrap differently
    /// (OpenAI's `function` envelope) handle that internally.
    async fn create_message(
        &self,
        request: CreateMessageRequest,
    ) -> Result<CreateMessageResponse>;
}
