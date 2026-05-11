//! API clients module

mod anthropic;
mod client;
pub mod openai_compat;

pub use anthropic::{
    AnthropicClient,
    Message,
    MessageRole,
    ContentBlock,
    ToolUse,
    ToolResultBlock,
    ToolDefinition,
    CreateMessageRequest,
    CreateMessageResponse,
    StreamEvent,
    Usage,
};
pub use client::ApiClient;
pub use openai_compat::OpenAICompatClient;
