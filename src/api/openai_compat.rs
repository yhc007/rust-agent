//! OpenAI chat-completions client.
//!
//! Used to drive any server that speaks the OpenAI wire format
//! (vLLM, Azure OpenAI, OpenRouter, LM Studio, LocalAI, …). The agent
//! loop sees Anthropic-shaped messages; this module translates both
//! ways so callers don't have to know which backend they're talking
//! to.
//!
//! Tool-use translation lives in [`to_openai_messages`] (Anthropic →
//! OpenAI) and [`from_openai_response`] (OpenAI → Anthropic). Read
//! both before changing either — they're inverses of each other and
//! a one-sided change will desync tool_use ↔ tool_call ids.
//!
//! Requirements on the server:
//! - Must accept the `tools` parameter on `/chat/completions`.
//! - Must emit `tool_calls` blocks when the model wants to invoke
//!   one. For vLLM that means launching with
//!   `--enable-auto-tool-choice --tool-call-parser <name>` (try
//!   `hermes` first for Nemotron-Nano-v2; fall back to `pythonic`).
//!
//! Failure modes the caller should know about:
//! - **400 "auto tool choice requires --enable-auto-tool-choice"** —
//!   server isn't tool-aware. Restart with the right flag.
//! - **Empty `tool_calls` despite "tools" being passed** — model
//!   chose not to call a tool. Loop exits naturally (no tool_use
//!   blocks → done).
//! - **Malformed `arguments` JSON** — we surface the parse error;
//!   typically means the tool-call-parser doesn't match the model's
//!   output style. Try a different parser.

use std::time::Duration;

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::client::ApiClient;
use super::{
    ContentBlock, CreateMessageRequest, CreateMessageResponse, Message, MessageRole, ToolDefinition,
    ToolUse, Usage,
};

/// Default timeout for one chat completion. vLLM on CPU is slow when
/// the model has to think + emit a long `tool_calls` block.
const REQUEST_TIMEOUT_SECS: u64 = 300;

/// HTTP client for OpenAI-compatible endpoints. Cheap to clone.
pub struct OpenAICompatClient {
    base_url: String,
    api_key: String,
    http: Client,
    /// `"openai-compat"` for vLLM/Azure/LocalAI, `"openai"` when
    /// pointed at api.openai.com. Exposed via `label()` for logs.
    label: &'static str,
}

impl OpenAICompatClient {
    /// Build a client. `base_url` should be the part BEFORE
    /// `/chat/completions` (e.g. `http://localhost:8000/v1`).
    /// `api_key` may be a dummy value when the server doesn't
    /// authenticate (vLLM accepts any non-empty key).
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let label = if base_url.contains("api.openai.com") {
            "openai"
        } else {
            "openai-compat"
        };
        let http = Client::builder()
            .timeout(Duration::from_secs(REQUEST_TIMEOUT_SECS))
            .build()
            .expect("reqwest client");
        Self {
            base_url,
            api_key: api_key.into(),
            http,
            label,
        }
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }
}

#[async_trait]
impl ApiClient for OpenAICompatClient {
    fn label(&self) -> &'static str {
        self.label
    }

    async fn create_message(
        &self,
        request: CreateMessageRequest,
    ) -> Result<CreateMessageResponse> {
        let body = to_openai_request(&request);
        let resp = self
            .http
            .post(format!("{}/chat/completions", self.base_url))
            .header("content-type", "application/json")
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .context("openai-compat HTTP send")?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("openai-compat {status}: {text}");
        }
        let raw: OpenAIChatResponse = resp
            .json()
            .await
            .context("decode openai-compat response")?;
        from_openai_response(raw, &request.model)
    }
}

// ===========================================================================
// Anthropic → OpenAI request translation
// ===========================================================================

#[derive(Serialize)]
struct OpenAIRequest<'a> {
    model: &'a str,
    messages: Vec<OpenAIMessage>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<OpenAIToolDef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_choice: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<bool>,
}

#[derive(Serialize)]
struct OpenAIToolDef {
    #[serde(rename = "type")]
    kind: &'static str, // "function"
    function: OpenAIFunctionDef,
}

#[derive(Serialize)]
struct OpenAIFunctionDef {
    name: String,
    description: String,
    parameters: Value,
}

#[derive(Serialize, Debug)]
struct OpenAIMessage {
    role: &'static str, // "system" | "user" | "assistant" | "tool"
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    tool_calls: Vec<OpenAIToolCall>,
    /// Only set on `role: "tool"` messages. Matches the assistant's
    /// previous tool_call.id so the model can correlate.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

#[derive(Serialize, Debug)]
struct OpenAIToolCall {
    id: String,
    #[serde(rename = "type")]
    kind: &'static str, // "function"
    function: OpenAIToolCallFunction,
}

#[derive(Serialize, Debug)]
struct OpenAIToolCallFunction {
    name: String,
    /// OpenAI ships arguments as a JSON-encoded STRING, not a parsed
    /// object. The Anthropic input field is a Value — we
    /// `to_string()` it here.
    arguments: String,
}

fn to_openai_request(req: &CreateMessageRequest) -> Value {
    let messages = to_openai_messages(req.system.as_deref(), &req.messages);
    let tools = req.tools.as_ref().map(|defs| {
        defs.iter().map(tool_def_to_openai).collect::<Vec<_>>()
    });
    let body = OpenAIRequest {
        model: &req.model,
        messages,
        max_tokens: req.max_tokens,
        tools,
        tool_choice: req.tools.as_ref().map(|_| "auto"),
        stream: req.stream,
    };
    serde_json::to_value(body).expect("openai request serialize")
}

fn tool_def_to_openai(d: &ToolDefinition) -> OpenAIToolDef {
    OpenAIToolDef {
        kind: "function",
        function: OpenAIFunctionDef {
            name: d.name.clone(),
            description: d.description.clone(),
            parameters: d.input_schema.clone(),
        },
    }
}

/// Flatten Anthropic-shaped messages into OpenAI shape. The big
/// asymmetry: Anthropic uses a single `user` message containing
/// multiple `ToolResult` blocks to return tool outputs; OpenAI
/// requires one `role: "tool"` message *per* tool_call_id. We split
/// here.
fn to_openai_messages(
    system: Option<&str>,
    messages: &[Message],
) -> Vec<OpenAIMessage> {
    let mut out = Vec::with_capacity(messages.len() + 1);
    if let Some(sys) = system {
        out.push(OpenAIMessage {
            role: "system",
            content: Some(sys.to_string()),
            tool_calls: Vec::new(),
            tool_call_id: None,
        });
    }
    for msg in messages {
        match msg.role {
            MessageRole::User => {
                let mut text = String::new();
                let mut tool_results: Vec<(String, String)> = Vec::new();
                for block in &msg.content {
                    match block {
                        ContentBlock::Text { text: t } => {
                            if !text.is_empty() {
                                text.push('\n');
                            }
                            text.push_str(t);
                        }
                        ContentBlock::ToolResult(tr) => {
                            // OpenAI has no `is_error` field on tool
                            // messages — fold it into the content so
                            // the model still sees the failure.
                            let content = if tr.is_error {
                                format!("[error] {}", tr.content)
                            } else {
                                tr.content.clone()
                            };
                            tool_results.push((tr.tool_use_id.clone(), content));
                        }
                        ContentBlock::ToolUse(_) => {
                            // Shouldn't appear on user messages, but
                            // be defensive — skip rather than panic.
                            tracing::warn!("user message contained ToolUse block; dropped");
                        }
                    }
                }
                // OpenAI requires one tool-role message per
                // tool_call_id. Emit those first (matching the
                // assistant turn that produced the tool_calls), then
                // the optional text fragment last.
                for (id, content) in tool_results {
                    out.push(OpenAIMessage {
                        role: "tool",
                        content: Some(content),
                        tool_calls: Vec::new(),
                        tool_call_id: Some(id),
                    });
                }
                if !text.is_empty() {
                    out.push(OpenAIMessage {
                        role: "user",
                        content: Some(text),
                        tool_calls: Vec::new(),
                        tool_call_id: None,
                    });
                }
            }
            MessageRole::Assistant => {
                let mut text = String::new();
                let mut calls: Vec<OpenAIToolCall> = Vec::new();
                for block in &msg.content {
                    match block {
                        ContentBlock::Text { text: t } => {
                            if !text.is_empty() {
                                text.push('\n');
                            }
                            text.push_str(t);
                        }
                        ContentBlock::ToolUse(tu) => {
                            calls.push(OpenAIToolCall {
                                id: tu.id.clone(),
                                kind: "function",
                                function: OpenAIToolCallFunction {
                                    name: tu.name.clone(),
                                    arguments: tu.input.to_string(),
                                },
                            });
                        }
                        ContentBlock::ToolResult(_) => {
                            tracing::warn!(
                                "assistant message contained ToolResult block; dropped"
                            );
                        }
                    }
                }
                out.push(OpenAIMessage {
                    role: "assistant",
                    content: if text.is_empty() { None } else { Some(text) },
                    tool_calls: calls,
                    tool_call_id: None,
                });
            }
        }
    }
    out
}

// ===========================================================================
// OpenAI → Anthropic response translation
// ===========================================================================

#[derive(Debug, Deserialize)]
struct OpenAIChatResponse {
    id: String,
    choices: Vec<OpenAIChoice>,
    #[serde(default)]
    usage: Option<OpenAIUsage>,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenAIChoice {
    #[serde(default)]
    finish_reason: Option<String>,
    message: OpenAIResponseMessage,
}

#[derive(Debug, Deserialize)]
struct OpenAIResponseMessage {
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    tool_calls: Vec<OpenAIToolCallResponse>,
}

#[derive(Debug, Deserialize)]
struct OpenAIToolCallResponse {
    id: String,
    #[serde(rename = "type", default)]
    #[allow(dead_code)]
    kind: Option<String>,
    function: OpenAIToolCallFunctionResponse,
}

#[derive(Debug, Deserialize)]
struct OpenAIToolCallFunctionResponse {
    name: String,
    /// JSON-encoded STRING — we parse to Value below.
    arguments: String,
}

#[derive(Debug, Deserialize)]
struct OpenAIUsage {
    #[serde(default)]
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
}

fn from_openai_response(
    raw: OpenAIChatResponse,
    fallback_model: &str,
) -> Result<CreateMessageResponse> {
    let choice = raw
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("openai-compat: response had no choices"))?;
    let mut content_blocks: Vec<ContentBlock> = Vec::new();
    if let Some(text) = choice.message.content {
        if !text.is_empty() {
            content_blocks.push(ContentBlock::Text { text });
        }
    }
    for call in choice.message.tool_calls {
        // arguments comes as a JSON-encoded string. If the model
        // didn't emit valid JSON, the parser the server picked is
        // wrong for this model — surface it loudly.
        let input: Value = serde_json::from_str(&call.function.arguments)
            .with_context(|| {
                format!(
                    "tool_call arguments not valid JSON (tool-call-parser mismatch?): {}",
                    call.function.arguments
                )
            })?;
        content_blocks.push(ContentBlock::ToolUse(ToolUse {
            id: call.id,
            name: call.function.name,
            input,
        }));
    }
    let usage = raw
        .usage
        .map(|u| Usage {
            input_tokens: u.prompt_tokens,
            output_tokens: u.completion_tokens,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        })
        .unwrap_or(Usage {
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        });
    Ok(CreateMessageResponse {
        id: raw.id,
        content: content_blocks,
        model: raw.model.unwrap_or_else(|| fallback_model.to_string()),
        stop_reason: choice.finish_reason,
        usage,
    })
}

// helper so tests can build a tools list without going through
// `serde_json::Value` macros for the parameters.
#[cfg(test)]
fn tool_def(name: &str, description: &str, schema: Value) -> ToolDefinition {
    ToolDefinition {
        name: name.to_string(),
        description: description.to_string(),
        input_schema: schema,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::ToolResultBlock;

    #[test]
    fn tool_def_wraps_in_function_envelope() {
        let d = tool_def(
            "lookup",
            "find facts",
            json!({"type":"object","properties":{},"required":[]}),
        );
        let serialized = serde_json::to_value(tool_def_to_openai(&d)).unwrap();
        assert_eq!(serialized["type"], "function");
        assert_eq!(serialized["function"]["name"], "lookup");
        assert!(serialized["function"]["parameters"].is_object());
    }

    #[test]
    fn tool_use_round_trips_anthropic_to_openai() {
        // Build an assistant message with a single tool_use.
        let msg = Message {
            role: MessageRole::Assistant,
            content: vec![ContentBlock::ToolUse(ToolUse {
                id: "call_abc".into(),
                name: "lookup".into(),
                input: json!({"q": "France"}),
            })],
        };
        let openai = to_openai_messages(None, std::slice::from_ref(&msg));
        assert_eq!(openai.len(), 1);
        assert_eq!(openai[0].role, "assistant");
        assert_eq!(openai[0].tool_calls.len(), 1);
        let call = &openai[0].tool_calls[0];
        assert_eq!(call.id, "call_abc");
        assert_eq!(call.function.name, "lookup");
        // arguments is a STRING here — that's the OpenAI wire format.
        let parsed: Value = serde_json::from_str(&call.function.arguments).unwrap();
        assert_eq!(parsed, json!({"q": "France"}));
    }

    #[test]
    fn tool_result_splits_into_per_id_tool_messages() {
        // Anthropic sends multiple tool_results in one user message;
        // OpenAI requires one role=tool message per tool_call_id.
        let msg = Message {
            role: MessageRole::User,
            content: vec![
                ContentBlock::ToolResult(ToolResultBlock {
                    tool_use_id: "call_a".into(),
                    content: "ok a".into(),
                    is_error: false,
                }),
                ContentBlock::ToolResult(ToolResultBlock {
                    tool_use_id: "call_b".into(),
                    content: "boom".into(),
                    is_error: true,
                }),
                ContentBlock::Text {
                    text: "trailing user note".into(),
                },
            ],
        };
        let openai = to_openai_messages(None, std::slice::from_ref(&msg));
        // 2 tool messages + 1 user text = 3
        assert_eq!(openai.len(), 3);
        assert_eq!(openai[0].role, "tool");
        assert_eq!(openai[0].tool_call_id.as_deref(), Some("call_a"));
        assert_eq!(openai[0].content.as_deref(), Some("ok a"));
        assert_eq!(openai[1].role, "tool");
        assert_eq!(openai[1].tool_call_id.as_deref(), Some("call_b"));
        // is_error folded into content as a prefix.
        assert!(openai[1].content.as_deref().unwrap().starts_with("[error]"));
        assert_eq!(openai[2].role, "user");
    }

    #[test]
    fn openai_response_with_tool_call_becomes_anthropic_tool_use() {
        let raw = OpenAIChatResponse {
            id: "resp_1".into(),
            model: Some("test-model".into()),
            usage: Some(OpenAIUsage {
                prompt_tokens: 10,
                completion_tokens: 5,
            }),
            choices: vec![OpenAIChoice {
                finish_reason: Some("tool_calls".into()),
                message: OpenAIResponseMessage {
                    content: Some("Looking it up".into()),
                    tool_calls: vec![OpenAIToolCallResponse {
                        id: "call_xyz".into(),
                        kind: Some("function".into()),
                        function: OpenAIToolCallFunctionResponse {
                            name: "lookup".into(),
                            arguments: r#"{"q":"France"}"#.into(),
                        },
                    }],
                },
            }],
        };
        let resp = from_openai_response(raw, "fallback").unwrap();
        assert_eq!(resp.model, "test-model");
        assert_eq!(resp.content.len(), 2);
        match &resp.content[0] {
            ContentBlock::Text { text } => assert_eq!(text, "Looking it up"),
            other => panic!("expected text first, got {other:?}"),
        }
        match &resp.content[1] {
            ContentBlock::ToolUse(tu) => {
                assert_eq!(tu.id, "call_xyz");
                assert_eq!(tu.name, "lookup");
                assert_eq!(tu.input, json!({"q": "France"}));
            }
            other => panic!("expected tool_use, got {other:?}"),
        }
        assert_eq!(resp.usage.input_tokens, 10);
        assert_eq!(resp.usage.output_tokens, 5);
    }

    #[test]
    fn malformed_arguments_string_surfaces_clear_error() {
        let raw = OpenAIChatResponse {
            id: "x".into(),
            model: None,
            usage: None,
            choices: vec![OpenAIChoice {
                finish_reason: None,
                message: OpenAIResponseMessage {
                    content: None,
                    tool_calls: vec![OpenAIToolCallResponse {
                        id: "c".into(),
                        kind: None,
                        function: OpenAIToolCallFunctionResponse {
                            name: "n".into(),
                            arguments: "not json at all".into(),
                        },
                    }],
                },
            }],
        };
        let err = from_openai_response(raw, "fallback").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("not valid JSON") || msg.contains("tool-call-parser mismatch"),
            "error message should hint at parser mismatch, got: {msg}"
        );
    }
}
