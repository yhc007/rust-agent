//! Anthropic Claude API client

use anyhow::{Result, Context};
use futures::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const API_URL: &str = "https://api.anthropic.com/v1/messages";
const API_VERSION: &str = "2023-06-01";

/// Anthropic API client
pub struct AnthropicClient {
    client: Client,
    api_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: MessageRole,
    pub content: Vec<ContentBlock>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    User,
    Assistant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse(ToolUse),
    #[serde(rename = "tool_result")]
    ToolResult(ToolResultBlock),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolUse {
    pub id: String,
    pub name: String,
    pub input: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResultBlock {
    pub tool_use_id: String,
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CreateMessageRequest {
    pub model: String,
    pub max_tokens: u32,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateMessageResponse {
    pub id: String,
    pub content: Vec<ContentBlock>,
    pub model: String,
    pub stop_reason: Option<String>,
    pub usage: Usage,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    #[serde(default)]
    pub cache_creation_input_tokens: u32,
    #[serde(default)]
    pub cache_read_input_tokens: u32,
}

/// Stream event from SSE
#[derive(Debug, Clone)]
pub enum StreamEvent {
    ContentBlockStart { index: u32, content_block: ContentBlock },
    ContentBlockDelta { index: u32, delta: ContentDelta },
    ContentBlockStop { index: u32 },
    MessageStart { message: PartialMessage },
    MessageDelta { delta: MessageDeltaData, usage: Usage },
    MessageStop,
    Ping,
    Error { error: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct ContentDelta {
    #[serde(rename = "type")]
    pub delta_type: String,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub partial_json: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PartialMessage {
    pub id: String,
    pub model: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessageDeltaData {
    pub stop_reason: Option<String>,
}

impl AnthropicClient {
    pub fn new(api_key: String) -> Self {
        Self {
            client: Client::new(),
            api_key,
        }
    }
    
    /// Create a message (non-streaming)
    pub async fn create_message(&self, request: CreateMessageRequest) -> Result<CreateMessageResponse> {
        let response = self.client
            .post(API_URL)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION)
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await
            .context("Failed to send request")?;
        
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("API error {}: {}", status, text);
        }
        
        response.json().await.context("Failed to parse response")
    }
    
    /// Create a message with streaming
    pub async fn create_message_stream(
        &self, 
        mut request: CreateMessageRequest
    ) -> Result<mpsc::Receiver<StreamEvent>> {
        request.stream = Some(true);
        
        let response = self.client
            .post(API_URL)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", API_VERSION)
            .header("content-type", "application/json")
            .json(&request)
            .send()
            .await
            .context("Failed to send request")?;
        
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("API error {}: {}", status, text);
        }
        
        let (tx, rx) = mpsc::channel(100);
        
        tokio::spawn(async move {
            let mut stream = response.bytes_stream();
            let mut buffer = String::new();
            
            while let Some(chunk) = stream.next().await {
                if let Ok(bytes) = chunk {
                    buffer.push_str(&String::from_utf8_lossy(&bytes));
                    
                    // Parse SSE events
                    while let Some(event) = Self::parse_sse_event(&mut buffer) {
                        if tx.send(event).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        
        Ok(rx)
    }
    
    fn parse_sse_event(buffer: &mut String) -> Option<StreamEvent> {
        // Find complete event (ends with double newline)
        if let Some(end_pos) = buffer.find("\n\n") {
            let event_str = buffer[..end_pos].to_string();
            *buffer = buffer[end_pos + 2..].to_string();
            
            // Parse event type and data
            let mut event_type = String::new();
            let mut data = String::new();
            
            for line in event_str.lines() {
                if let Some(rest) = line.strip_prefix("event: ") {
                    event_type = rest.to_string();
                } else if let Some(rest) = line.strip_prefix("data: ") {
                    data = rest.to_string();
                }
            }
            
            // Convert to StreamEvent based on type
            match event_type.as_str() {
                "message_start" => {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&data) {
                        if let Some(msg) = parsed.get("message") {
                            if let Ok(message) = serde_json::from_value(msg.clone()) {
                                return Some(StreamEvent::MessageStart { message });
                            }
                        }
                    }
                }
                "content_block_start" => {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&data) {
                        let index = parsed.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                        if let Some(cb) = parsed.get("content_block") {
                            if let Ok(content_block) = serde_json::from_value(cb.clone()) {
                                return Some(StreamEvent::ContentBlockStart { index, content_block });
                            }
                        }
                    }
                }
                "content_block_delta" => {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&data) {
                        let index = parsed.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                        if let Some(d) = parsed.get("delta") {
                            if let Ok(delta) = serde_json::from_value(d.clone()) {
                                return Some(StreamEvent::ContentBlockDelta { index, delta });
                            }
                        }
                    }
                }
                "content_block_stop" => {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&data) {
                        let index = parsed.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                        return Some(StreamEvent::ContentBlockStop { index });
                    }
                }
                "message_delta" => {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&data) {
                        if let (Some(d), Some(u)) = (parsed.get("delta"), parsed.get("usage")) {
                            if let (Ok(delta), Ok(usage)) = (
                                serde_json::from_value(d.clone()),
                                serde_json::from_value(u.clone()),
                            ) {
                                return Some(StreamEvent::MessageDelta { delta, usage });
                            }
                        }
                    }
                }
                "message_stop" => {
                    return Some(StreamEvent::MessageStop);
                }
                "ping" => {
                    return Some(StreamEvent::Ping);
                }
                "error" => {
                    return Some(StreamEvent::Error { error: data });
                }
                _ => {}
            }
            
            None
        } else {
            None
        }
    }
}
