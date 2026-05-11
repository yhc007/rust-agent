//! pdf-kg integration: expose the remote PDF knowledge-graph backend
//! as a family of [`crate::tools::Tool`]s.
//!
//! The agent talks plain HTTP to pdf-kg's `/api/tools/*` REST surface
//! — see `docs/tools-and-mcp.md` in the pdf-kg repo. We choose REST
//! over MCP because the local agent already has a richer Tool trait;
//! the MCP wire layer would be a redundant indirection.
//!
//! Tools registered (prefix `pdfkg_` so they don't collide with local
//! filesystem tools):
//! - `pdfkg_list_jobs`    discover indexed PDFs
//! - `pdfkg_search`       retrieval only, no LLM
//! - `pdfkg_ask`          end-to-end RAG with multimodal answer
//! - `pdfkg_get_page`     read one page's chunks + image refs
//! - `pdfkg_get_image`    figure metadata + bytes_url (NOT inlined)
//! - `pdfkg_get_subgraph` ego-subgraph traversal
//!
//! Endpoint resolution: `PDFKG_BACKEND_URL` env var, default
//! `http://localhost:8088`. Set `PDFKG_BACKEND_URL=disabled` to skip
//! registration entirely (useful when pdf-kg isn't running and you
//! don't want dead tools in the agent's catalog).

pub mod client;
pub mod tools;

pub use client::{PdfKgClient, PdfKgError};

use std::sync::Arc;

use super::ToolRegistry;

/// Register all six pdf-kg tools against the supplied registry,
/// sharing a single HTTP client across them. Idempotent — calling
/// twice replaces the previous registrations (the registry is a
/// HashMap keyed by tool name).
pub fn register(registry: &mut ToolRegistry, client: Arc<PdfKgClient>) {
    registry.register(Box::new(tools::ListJobsTool { client: client.clone() }));
    registry.register(Box::new(tools::SearchPdfTool { client: client.clone() }));
    registry.register(Box::new(tools::AskPdfTool { client: client.clone() }));
    registry.register(Box::new(tools::GetPageTool { client: client.clone() }));
    registry.register(Box::new(tools::GetImageTool { client: client.clone() }));
    registry.register(Box::new(tools::GetSubgraphTool { client }));
}

/// Convenience: try [`PdfKgClient::from_env`] and register if a
/// client comes back. Returns `true` when tools were added. Used by
/// `create_default_registry()` so a missing/disabled backend simply
/// leaves the local-only tool surface unchanged.
pub fn register_from_env(registry: &mut ToolRegistry) -> bool {
    match PdfKgClient::from_env() {
        Some(client) => {
            let arc = Arc::new(client);
            tracing::info!(base = %arc.base(), "registering pdf-kg tools");
            register(registry, arc);
            true
        }
        None => {
            tracing::debug!("pdf-kg integration disabled via PDFKG_BACKEND_URL");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tools::Tool;

    #[test]
    fn register_adds_six_pdfkg_tools() {
        let mut registry = ToolRegistry::new();
        let client = Arc::new(PdfKgClient::new("http://localhost:8088"));
        register(&mut registry, client);

        let names: std::collections::BTreeSet<String> = registry
            .all()
            .iter()
            .map(|t| t.name().to_string())
            .collect();
        for expected in [
            "pdfkg_list_jobs",
            "pdfkg_search",
            "pdfkg_ask",
            "pdfkg_get_page",
            "pdfkg_get_image",
            "pdfkg_get_subgraph",
        ] {
            assert!(
                names.contains(expected),
                "missing tool {expected}: registry has {names:?}"
            );
        }
        assert_eq!(names.len(), 6, "expected exactly 6 pdfkg tools, got {names:?}");
    }

    #[test]
    fn each_tool_has_object_input_schema_with_required_array() {
        // Mirrors the validation the Anthropic API does on
        // ToolDefinition schemas — easier to catch a missing
        // `required` array here than to debug a 400 from the API.
        let mut registry = ToolRegistry::new();
        let client = Arc::new(PdfKgClient::new("http://localhost:8088"));
        register(&mut registry, client);
        for tool in registry.all() {
            let schema = tool.input_schema();
            assert_eq!(
                schema["type"], "object",
                "{}: input_schema is not an object schema: {schema}",
                tool.name()
            );
            assert!(
                schema.get("required").is_some(),
                "{}: input_schema missing `required` array",
                tool.name()
            );
        }
    }

    #[test]
    fn from_env_disabled_returns_none() {
        // Lock the env var locally so a stray test harness setting
        // doesn't bleed through. We restore on exit.
        let prev = std::env::var("PDFKG_BACKEND_URL").ok();
        std::env::set_var("PDFKG_BACKEND_URL", "disabled");
        assert!(PdfKgClient::from_env().is_none());
        match prev {
            Some(v) => std::env::set_var("PDFKG_BACKEND_URL", v),
            None => std::env::remove_var("PDFKG_BACKEND_URL"),
        }
    }

    #[test]
    fn from_env_default_falls_back_to_localhost() {
        let prev = std::env::var("PDFKG_BACKEND_URL").ok();
        std::env::remove_var("PDFKG_BACKEND_URL");
        let client = PdfKgClient::from_env().expect("default should produce a client");
        assert_eq!(client.base(), "http://localhost:8088");
        if let Some(v) = prev {
            std::env::set_var("PDFKG_BACKEND_URL", v);
        }
    }
}
