//! Tool registry and trait definitions

use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// Tool execution result
#[derive(Debug, Clone)]
pub struct ToolResult {
    pub output: String,
    pub is_error: bool,
}

impl ToolResult {
    pub fn success(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            is_error: false,
        }
    }
    
    pub fn error(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
            is_error: true,
        }
    }
}

/// Tool execution error
#[derive(Debug, Error)]
pub enum ToolError {
    #[error("Tool not found: {0}")]
    NotFound(String),
    
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
    
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    
    #[error("Timeout")]
    Timeout,
}

/// Context passed to tool execution
#[derive(Debug, Clone)]
pub struct ToolContext {
    /// Current working directory
    pub cwd: String,
    /// Session ID
    pub session_id: String,
}

impl Default for ToolContext {
    fn default() -> Self {
        Self {
            cwd: std::env::current_dir()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_else(|_| ".".to_string()),
            session_id: uuid::Uuid::new_v4().to_string(),
        }
    }
}

/// Tool trait - all tools implement this
#[async_trait]
pub trait Tool: Send + Sync {
    /// Tool name (e.g., "bash", "file_read")
    fn name(&self) -> &str;
    
    /// Tool description for the model
    fn description(&self) -> &str;
    
    /// JSON schema for tool input
    fn input_schema(&self) -> Value;
    
    /// Execute the tool
    async fn execute(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError>;
}

/// Tool registry - manages available tools
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: HashMap::new(),
        }
    }
    
    /// Register a tool
    pub fn register(&mut self, tool: Box<dyn Tool>) {
        let name = tool.name().to_string();
        self.tools.insert(name, Arc::from(tool));
    }
    
    /// Get a tool by name
    pub fn get(&self, name: &str) -> Option<Arc<dyn Tool>> {
        self.tools.get(name).cloned()
    }
    
    /// Get all registered tools
    pub fn all(&self) -> Vec<Arc<dyn Tool>> {
        self.tools.values().cloned().collect()
    }
    
    /// Execute a tool by name
    pub async fn execute(
        &self,
        name: &str,
        input: Value,
        ctx: &ToolContext,
    ) -> Result<ToolResult, ToolError> {
        let tool = self.tools.get(name)
            .ok_or_else(|| ToolError::NotFound(name.to_string()))?;
        
        tool.execute(input, ctx).await
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}
