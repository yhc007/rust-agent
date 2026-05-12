//! Query Engine — the main agent loop.
//!
//! Two entry points share the same internal state machine:
//!
//! - [`QueryEngine::process_input`] — CLI-friendly. Prints text /
//!   tool previews to stdout, returns when the loop exits.
//! - [`QueryEngine::process_input_streamed`] — emits structured
//!   [`AgentEvent`]s on an mpsc channel as they happen. Used by the
//!   web SSE endpoint so a browser can render the chain live.
//!
//! Both go through the same private [`Self::run_loop`] which drives
//! the conversation + tool execution; only the event sink differs.

use anyhow::Result;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::api::{
    ApiClient, AnthropicClient, ContentBlock, CreateMessageRequest, Message, MessageRole,
    OpenAICompatClient, ToolDefinition, ToolResultBlock, ToolUse,
};
use crate::config::{Backend, Config};
use crate::tools::{create_default_registry, tool_to_definition, ToolContext, ToolRegistry};

const SYSTEM_PROMPT: &str = r#"You are a helpful AI assistant with access to tools for interacting with the local system and the indexed pdf-kg knowledge graph.

**LANGUAGE — STRICT:** ALWAYS reply to the user in Korean (한국어).
This applies even when the user's prompt is in English. Tool names
(pdfkg_search, bash, etc.) and structured tool arguments stay in
English/JSON as required, but any *prose* you emit — reasoning
preludes between tool calls, the final answer, error explanations
— MUST be Korean. If a tool returns Chinese or English content,
translate it into Korean before quoting in your prose; cite page
numbers verbatim ((p.5) etc.).

Tools:
- bash — shell commands
- file_read — examine file contents
- file_write — create or modify files
- grep — search file contents
- pdfkg_list_jobs / pdfkg_search / pdfkg_ask / pdfkg_get_page /
  pdfkg_get_image / pdfkg_get_subgraph — PDF retrieval + Q&A

Be concise but thorough. When you make changes, verify they worked.
"#;

/// One observable step in the agent loop. Serializable so the web
/// SSE endpoint can stringify each event into a `data:` line.
///
/// Lifecycle: `Start` (once) → zero or more
/// `Text` / `ToolUse` / `ToolResult` → `Done` (once, on success) or
/// `Error` (once, on failure). Consumers can rely on at most one
/// terminal event.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentEvent {
    /// First event in the stream — useful for UIs that want to clear
    /// previous run state before the new chain starts.
    Start,
    /// Plain assistant text. The model may emit multiple of these
    /// across multiple turns; render in order.
    Text { content: String },
    /// Assistant decided to call a tool. `id` correlates with the
    /// subsequent `ToolResult.tool_use_id`.
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    /// Tool finished executing. `is_error: true` means the model
    /// should adapt on the next turn rather than the loop bailing.
    ToolResult {
        tool_use_id: String,
        content: String,
        is_error: bool,
    },
    /// Loop exited cleanly (no more tool_use blocks in the latest
    /// assistant response). Terminal.
    Done,
    /// Transport / API / I/O failure that broke the loop. Terminal.
    Error { message: String },
}

/// Query Engine — manages the conversation and tool execution
pub struct QueryEngine {
    /// Boxed so the engine doesn't care whether it's talking to
    /// Anthropic or an OpenAI-compat endpoint (vLLM etc.).
    client: Box<dyn ApiClient>,
    tools: ToolRegistry,
    messages: Vec<Message>,
    model: String,
    max_tokens: u32,
    ctx: ToolContext,
}

impl QueryEngine {
    /// Create a new query engine. `model` override (often supplied
    /// via `--model` on the CLI) wins over `config.model`, which is
    /// itself overridable via `RUST_AGENT_MODEL`. When both are
    /// empty/default we fall through to `config.model` so each
    /// backend's defaults work.
    pub fn new(config: Config, model: String) -> Result<Self> {
        let resolved_model = if model.is_empty() {
            config.model.clone()
        } else {
            model
        };
        let client: Box<dyn ApiClient> = match config.backend {
            Backend::Anthropic { api_key } => Box::new(AnthropicClient::new(api_key)),
            Backend::OpenAICompat { api_key, base_url } => {
                Box::new(OpenAICompatClient::new(base_url, api_key))
            }
        };
        tracing::info!(backend = client.label(), model = %resolved_model, "agent engine ready");
        let tools = create_default_registry();

        Ok(Self {
            client,
            tools,
            messages: Vec::new(),
            model: resolved_model,
            max_tokens: config.max_tokens,
            ctx: ToolContext::default(),
        })
    }

    /// CLI entry point — runs the agent loop and prints text + tool
    /// previews to stdout. Internally just consumes the event stream
    /// of [`Self::process_input_streamed`] and renders each event,
    /// so it stays in sync with the web path automatically.
    pub async fn process_input(&mut self, input: &str) -> Result<()> {
        use std::io::Write;
        let (tx, mut rx) = mpsc::unbounded_channel::<AgentEvent>();
        // Drain the events on the same task — keeps stdout output
        // ordered with the loop's progress. (A background task would
        // race the parent returning.)
        let input = input.to_string();
        let loop_fut = self.run_loop(input, tx);
        // Use a join-style pattern: poll the loop and the receiver
        // together so events print as they fire.
        tokio::pin!(loop_fut);
        loop {
            tokio::select! {
                biased;
                Some(ev) = rx.recv() => print_event(&ev)?,
                res = &mut loop_fut => {
                    // Drain any trailing events before returning.
                    while let Ok(ev) = rx.try_recv() {
                        print_event(&ev)?;
                    }
                    return res;
                }
            }
        }
        // Unreachable
        fn print_event(ev: &AgentEvent) -> Result<()> {
            match ev {
                AgentEvent::Text { content } => {
                    println!("{content}");
                }
                AgentEvent::ToolUse { name, input, .. } => {
                    print!("\n[Tool: {name}] ");
                    std::io::stdout().flush().ok();
                    if let Some(cmd) = input.get("command").and_then(|v| v.as_str()) {
                        println!("{cmd}");
                    } else if let Some(path) = input.get("path").and_then(|v| v.as_str()) {
                        println!("{path}");
                    } else {
                        println!("{}", serde_json::to_string_pretty(input)?);
                    }
                }
                AgentEvent::ToolResult { content, .. } => {
                    let preview: String =
                        content.lines().take(5).collect::<Vec<_>>().join("\n");
                    if !preview.is_empty() {
                        println!("→ {preview}");
                        if content.lines().count() > 5 {
                            println!(
                                "  ... ({} more lines)",
                                content.lines().count() - 5
                            );
                        }
                    }
                }
                AgentEvent::Start | AgentEvent::Done => {}
                AgentEvent::Error { message } => {
                    eprintln!("Error: {message}");
                }
            }
            Ok(())
        }
    }

    /// Web / SSE entry point — emits structured [`AgentEvent`]s as
    /// the agent loop progresses. Caller owns the receiving half of
    /// `tx` and decides how to render them. The returned future
    /// resolves when the loop ends (cleanly or with an error event
    /// already sent), so dropping the receiver after that point is
    /// safe.
    pub async fn process_input_streamed(
        &mut self,
        input: &str,
        tx: mpsc::UnboundedSender<AgentEvent>,
    ) -> Result<()> {
        self.run_loop(input.to_string(), tx).await
    }

    /// Internal loop driver. Sends events on `tx`. Errors bubble
    /// out via the `Result` AND get an `AgentEvent::Error` so SSE
    /// consumers see the failure even if they're not awaiting the
    /// future directly.
    async fn run_loop(
        &mut self,
        input: String,
        tx: mpsc::UnboundedSender<AgentEvent>,
    ) -> Result<()> {
        let _ = tx.send(AgentEvent::Start);

        // Add user message
        self.messages.push(Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text { text: input }],
        });

        let res = self.iterate(&tx).await;
        match &res {
            Ok(_) => {
                let _ = tx.send(AgentEvent::Done);
            }
            Err(e) => {
                let _ = tx.send(AgentEvent::Error {
                    message: e.to_string(),
                });
            }
        }
        res
    }

    async fn iterate(&mut self, tx: &mpsc::UnboundedSender<AgentEvent>) -> Result<()> {
        loop {
            let response = self.send_message().await?;

            // Collect tool uses from response
            let tool_uses: Vec<ToolUse> = response
                .content
                .iter()
                .filter_map(|block| {
                    if let ContentBlock::ToolUse(tu) = block {
                        Some(tu.clone())
                    } else {
                        None
                    }
                })
                .collect();

            // Emit text content
            for block in &response.content {
                if let ContentBlock::Text { text } = block {
                    let _ = tx.send(AgentEvent::Text {
                        content: text.clone(),
                    });
                }
            }

            // Add assistant message
            self.messages.push(Message {
                role: MessageRole::Assistant,
                content: response.content,
            });

            // If no tool uses, we're done
            if tool_uses.is_empty() {
                return Ok(());
            }

            // Execute tools and collect results
            let mut tool_results = Vec::new();
            for tool_use in &tool_uses {
                let _ = tx.send(AgentEvent::ToolUse {
                    id: tool_use.id.clone(),
                    name: tool_use.name.clone(),
                    input: tool_use.input.clone(),
                });

                let result = self
                    .tools
                    .execute(&tool_use.name, tool_use.input.clone(), &self.ctx)
                    .await;
                let (content, is_error) = match result {
                    Ok(r) => (r.output, r.is_error),
                    Err(e) => (format!("Error: {}", e), true),
                };

                let _ = tx.send(AgentEvent::ToolResult {
                    tool_use_id: tool_use.id.clone(),
                    content: content.clone(),
                    is_error,
                });

                tool_results.push(ToolResultBlock {
                    tool_use_id: tool_use.id.clone(),
                    content,
                    is_error,
                });
            }

            // Add tool results as user message
            self.messages.push(Message {
                role: MessageRole::User,
                content: tool_results
                    .into_iter()
                    .map(ContentBlock::ToolResult)
                    .collect(),
            });
        }
    }

    /// Send the current conversation to the underlying API.
    async fn send_message(&self) -> Result<crate::api::CreateMessageResponse> {
        let tool_defs: Vec<ToolDefinition> = self
            .tools
            .all()
            .iter()
            .map(|t| tool_to_definition(t.as_ref()))
            .collect();

        let request = CreateMessageRequest {
            model: self.model.clone(),
            max_tokens: self.max_tokens,
            messages: self.messages.clone(),
            system: Some(SYSTEM_PROMPT.to_string()),
            tools: Some(tool_defs),
            stream: None,
        };

        self.client.create_message(request).await
    }
}
