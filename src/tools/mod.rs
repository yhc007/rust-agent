//! Tool system module

mod registry;
pub mod bash;
pub mod file;
pub mod grep;
pub mod pdfkg;

pub use registry::{Tool, ToolRegistry, ToolResult, ToolError, ToolContext};
use serde_json::Value;

use crate::api::ToolDefinition;

/// Convert a Tool to an API ToolDefinition
pub fn tool_to_definition<T: Tool + ?Sized>(tool: &T) -> ToolDefinition {
    ToolDefinition {
        name: tool.name().to_string(),
        description: tool.description().to_string(),
        input_schema: tool.input_schema(),
    }
}

/// Create default tool registry with built-in tools
pub fn create_default_registry() -> ToolRegistry {
    let mut registry = ToolRegistry::new();

    // Register built-in tools
    registry.register(Box::new(bash::BashTool::new()));
    registry.register(Box::new(file::FileReadTool::new()));
    registry.register(Box::new(file::FileWriteTool::new()));
    registry.register(Box::new(grep::GrepTool::new()));

    // Optional: pdf-kg integration. Adds 6 tools (pdfkg_*) when the
    // PDFKG_BACKEND_URL env var points at a reachable pdf-kg backend,
    // skipped silently otherwise. See tools/pdfkg/mod.rs.
    pdfkg::register_from_env(&mut registry);

    registry
}
