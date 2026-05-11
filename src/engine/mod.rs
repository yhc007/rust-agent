//! Query Engine - the main agent loop

use anyhow::Result;
use std::io::Write;

use crate::api::{
    ApiClient, AnthropicClient, ContentBlock, CreateMessageRequest, Message, MessageRole,
    OpenAICompatClient, ToolDefinition, ToolResultBlock, ToolUse,
};
use crate::config::{Backend, Config};
use crate::tools::{create_default_registry, tool_to_definition, ToolContext, ToolRegistry};

const SYSTEM_PROMPT: &str = r#"You are a helpful AI assistant with access to tools for interacting with the local system.

When using tools:
- Use the bash tool for shell commands
- Use file_read to examine file contents
- Use file_write to create or modify files
- Use grep to search for patterns in files

Be concise but thorough. When you make changes, verify they worked.
"#;

/// Query Engine - manages the conversation and tool execution
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
    
    /// Process user input
    pub async fn process_input(&mut self, input: &str) -> Result<()> {
        // Add user message
        self.messages.push(Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text { text: input.to_string() }],
        });
        
        // Agent loop - continue until no more tool calls
        loop {
            let response = self.send_message().await?;
            
            // Collect tool uses from response
            let tool_uses: Vec<ToolUse> = response.content.iter()
                .filter_map(|block| {
                    if let ContentBlock::ToolUse(tu) = block {
                        Some(tu.clone())
                    } else {
                        None
                    }
                })
                .collect();
            
            // Print text content
            for block in &response.content {
                if let ContentBlock::Text { text } = block {
                    println!("{}", text);
                }
            }
            
            // Add assistant message
            self.messages.push(Message {
                role: MessageRole::Assistant,
                content: response.content,
            });
            
            // If no tool uses, we're done
            if tool_uses.is_empty() {
                break;
            }
            
            // Execute tools and collect results
            let mut tool_results = Vec::new();
            
            for tool_use in &tool_uses {
                print!("\n[Tool: {}] ", tool_use.name);
                std::io::stdout().flush()?;
                
                // Print a preview of the input
                if let Some(cmd) = tool_use.input.get("command").and_then(|v| v.as_str()) {
                    println!("{}", cmd);
                } else if let Some(path) = tool_use.input.get("path").and_then(|v| v.as_str()) {
                    println!("{}", path);
                } else {
                    println!("{}", serde_json::to_string_pretty(&tool_use.input)?);
                }
                
                // Execute the tool
                let result = self.tools.execute(
                    &tool_use.name,
                    tool_use.input.clone(),
                    &self.ctx,
                ).await;
                
                let (content, is_error) = match result {
                    Ok(r) => (r.output, r.is_error),
                    Err(e) => (format!("Error: {}", e), true),
                };
                
                // Print result preview
                let preview: String = content.lines().take(5).collect::<Vec<_>>().join("\n");
                if !preview.is_empty() {
                    println!("→ {}", preview);
                    if content.lines().count() > 5 {
                        println!("  ... ({} more lines)", content.lines().count() - 5);
                    }
                }
                
                tool_results.push(ToolResultBlock {
                    tool_use_id: tool_use.id.clone(),
                    content,
                    is_error,
                });
            }
            
            // Add tool results as user message
            self.messages.push(Message {
                role: MessageRole::User,
                content: tool_results.into_iter()
                    .map(ContentBlock::ToolResult)
                    .collect(),
            });
        }
        
        Ok(())
    }
    
    /// Send message to Claude API
    async fn send_message(&self) -> Result<crate::api::CreateMessageResponse> {
        // Build tool definitions
        let tool_defs: Vec<ToolDefinition> = self.tools.all()
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
