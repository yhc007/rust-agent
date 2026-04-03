//! File tools - read and write files

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path;
use tokio::fs;

use super::registry::{Tool, ToolContext, ToolError, ToolResult};

/// File read tool
pub struct FileReadTool {
    max_lines: usize,
    max_bytes: usize,
}

#[derive(Debug, Deserialize)]
struct FileReadInput {
    path: String,
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

impl FileReadTool {
    pub fn new() -> Self {
        Self {
            max_lines: 2000,
            max_bytes: 256 * 1024, // 256KB
        }
    }
}

impl Default for FileReadTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for FileReadTool {
    fn name(&self) -> &str {
        "file_read"
    }
    
    fn description(&self) -> &str {
        "Read the contents of a file. Supports text files. Output is truncated to 2000 lines or 256KB. \
        Use offset/limit for large files."
    }
    
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file to read"
                },
                "offset": {
                    "type": "integer",
                    "description": "Line number to start reading from (1-indexed)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of lines to read"
                }
            },
            "required": ["path"]
        })
    }
    
    async fn execute(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: FileReadInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        
        let path = if Path::new(&input.path).is_absolute() {
            input.path.clone()
        } else {
            format!("{}/{}", ctx.cwd, input.path)
        };
        
        // Check if file exists
        if !Path::new(&path).exists() {
            return Err(ToolError::ExecutionFailed(format!(
                "File not found: {}", input.path
            )));
        }
        
        // Read file
        let content = fs::read_to_string(&path)
            .await
            .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
        
        // Apply offset and limit
        let lines: Vec<&str> = content.lines().collect();
        let total_lines = lines.len();
        
        let offset = input.offset.unwrap_or(1).saturating_sub(1);
        let limit = input.limit.unwrap_or(self.max_lines).min(self.max_lines);
        
        let selected: Vec<&str> = lines.into_iter()
            .skip(offset)
            .take(limit)
            .collect();
        
        let mut result = selected.join("\n");
        
        // Truncate if too large
        if result.len() > self.max_bytes {
            result = result[..self.max_bytes].to_string();
            result.push_str("\n... (truncated)");
        }
        
        // Add line info
        let shown = selected.len();
        if offset > 0 || shown < total_lines {
            result = format!(
                "Showing lines {}-{} of {}\n\n{}",
                offset + 1,
                offset + shown,
                total_lines,
                result
            );
        }
        
        Ok(ToolResult::success(result))
    }
}

/// File write tool
pub struct FileWriteTool;

#[derive(Debug, Deserialize)]
struct FileWriteInput {
    path: String,
    content: String,
}

impl FileWriteTool {
    pub fn new() -> Self {
        Self
    }
}

impl Default for FileWriteTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for FileWriteTool {
    fn name(&self) -> &str {
        "file_write"
    }
    
    fn description(&self) -> &str {
        "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. \
        Automatically creates parent directories."
    }
    
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file to write"
                },
                "content": {
                    "type": "string",
                    "description": "Content to write to the file"
                }
            },
            "required": ["path", "content"]
        })
    }
    
    async fn execute(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: FileWriteInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        
        let path = if Path::new(&input.path).is_absolute() {
            input.path.clone()
        } else {
            format!("{}/{}", ctx.cwd, input.path)
        };
        
        let path = Path::new(&path);
        
        // Create parent directories
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
        }
        
        // Write file
        fs::write(&path, &input.content)
            .await
            .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
        
        let bytes = input.content.len();
        let lines = input.content.lines().count();
        
        Ok(ToolResult::success(format!(
            "Wrote {} bytes ({} lines) to {}",
            bytes, lines, input.path
        )))
    }
}
