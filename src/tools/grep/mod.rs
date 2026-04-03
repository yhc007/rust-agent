//! Grep tool - search file contents

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::Path;
use tokio::process::Command;

use super::registry::{Tool, ToolContext, ToolError, ToolResult};

/// Grep tool for searching file contents
pub struct GrepTool {
    max_results: usize,
}

#[derive(Debug, Deserialize)]
struct GrepInput {
    pattern: String,
    path: String,
    #[serde(default)]
    glob: Option<String>,
    #[serde(default)]
    case_insensitive: Option<bool>,
}

impl GrepTool {
    pub fn new() -> Self {
        Self {
            max_results: 100,
        }
    }
}

impl Default for GrepTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for GrepTool {
    fn name(&self) -> &str {
        "grep"
    }
    
    fn description(&self) -> &str {
        "Search for a pattern in files. Returns matching lines with file paths and line numbers. \
        Use glob to filter file types (e.g., \"*.rs\" for Rust files)."
    }
    
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "pattern": {
                    "type": "string",
                    "description": "The pattern to search for (regex supported)"
                },
                "path": {
                    "type": "string",
                    "description": "Directory or file path to search in"
                },
                "glob": {
                    "type": "string",
                    "description": "File pattern to match (e.g., \"*.rs\", \"*.ts\")"
                },
                "case_insensitive": {
                    "type": "boolean",
                    "description": "Whether to ignore case (default: false)"
                }
            },
            "required": ["pattern", "path"]
        })
    }
    
    async fn execute(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: GrepInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        
        let path = if Path::new(&input.path).is_absolute() {
            input.path.clone()
        } else {
            format!("{}/{}", ctx.cwd, input.path)
        };
        
        // Build grep command
        let mut cmd = Command::new("grep");
        cmd.arg("-rn"); // recursive, line numbers
        
        if input.case_insensitive.unwrap_or(false) {
            cmd.arg("-i");
        }
        
        // Add glob pattern if specified
        if let Some(glob) = &input.glob {
            cmd.arg("--include").arg(glob);
        }
        
        cmd.arg(&input.pattern)
            .arg(&path)
            .current_dir(&ctx.cwd);
        
        let output = cmd.output()
            .await
            .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
        
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        
        // Handle results
        if !output.status.success() {
            // Exit code 1 means no matches (not an error)
            if output.status.code() == Some(1) && stderr.is_empty() {
                return Ok(ToolResult::success("No matches found"));
            }
            
            if !stderr.is_empty() {
                return Ok(ToolResult::error(stderr.to_string()));
            }
        }
        
        // Limit results
        let lines: Vec<&str> = stdout.lines().take(self.max_results).collect();
        let total = stdout.lines().count();
        
        let mut result = lines.join("\n");
        
        if total > self.max_results {
            result.push_str(&format!("\n\n... and {} more matches", total - self.max_results));
        }
        
        if result.is_empty() {
            result = "No matches found".to_string();
        }
        
        Ok(ToolResult::success(result))
    }
}
