//! Bash tool - execute shell commands

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use std::process::Stdio;
use tokio::process::Command;
use tokio::time::{timeout, Duration};

use super::registry::{Tool, ToolContext, ToolError, ToolResult};

/// Bash tool for executing shell commands
pub struct BashTool {
    timeout: Duration,
}

#[derive(Debug, Deserialize)]
struct BashInput {
    command: String,
    #[serde(default)]
    timeout: Option<u64>,
}

impl BashTool {
    pub fn new() -> Self {
        Self {
            timeout: Duration::from_secs(120), // 2 minutes default
        }
    }
    
    /// Check for dangerous commands (basic security)
    fn is_dangerous_command(&self, command: &str) -> Option<&'static str> {
        let lower = command.to_lowercase();
        
        // Dangerous patterns
        if lower.contains("rm -rf /") || lower.contains("rm -rf /*") {
            return Some("Recursive delete of root directory");
        }
        
        if lower.contains("| sh") || lower.contains("| bash") {
            if lower.contains("curl") || lower.contains("wget") {
                return Some("Piping remote content to shell");
            }
        }
        
        if lower.contains("> /etc/") || lower.contains(">> /etc/") {
            return Some("Writing to system configuration");
        }
        
        if lower.contains(":(){ :|:& };:") {
            return Some("Fork bomb detected");
        }
        
        None
    }
}

impl Default for BashTool {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Tool for BashTool {
    fn name(&self) -> &str {
        "bash"
    }
    
    fn description(&self) -> &str {
        "Execute a shell command. Use for running CLI commands, scripts, and system operations. \
        Commands run in the current working directory with the user's environment."
    }
    
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The shell command to execute"
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in seconds (default: 120)"
                }
            },
            "required": ["command"]
        })
    }
    
    async fn execute(&self, input: Value, ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let input: BashInput = serde_json::from_value(input)
            .map_err(|e| ToolError::InvalidInput(e.to_string()))?;
        
        // Security check
        if let Some(reason) = self.is_dangerous_command(&input.command) {
            return Err(ToolError::PermissionDenied(format!(
                "Command blocked: {}", reason
            )));
        }
        
        let cmd_timeout = input.timeout
            .map(Duration::from_secs)
            .unwrap_or(self.timeout);
        
        // Execute command
        let result = timeout(cmd_timeout, async {
            let output = Command::new("sh")
                .arg("-c")
                .arg(&input.command)
                .current_dir(&ctx.cwd)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output()
                .await
                .map_err(|e| ToolError::ExecutionFailed(e.to_string()))?;
            
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            
            let mut result = String::new();
            
            if !stdout.is_empty() {
                result.push_str(&stdout);
            }
            
            if !stderr.is_empty() {
                if !result.is_empty() {
                    result.push('\n');
                }
                result.push_str("stderr:\n");
                result.push_str(&stderr);
            }
            
            if result.is_empty() {
                result = "(no output)".to_string();
            }
            
            if output.status.success() {
                Ok(ToolResult::success(result))
            } else {
                let code = output.status.code().unwrap_or(-1);
                Ok(ToolResult::error(format!(
                    "Command exited with code {}\n{}", code, result
                )))
            }
        }).await;
        
        match result {
            Ok(r) => r,
            Err(_) => Err(ToolError::Timeout),
        }
    }
}
