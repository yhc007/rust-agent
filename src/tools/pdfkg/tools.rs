//! Six `Tool` impls that talk to the pdf-kg backend over HTTP.
//!
//! Each is a thin adapter: forward `input` to the matching pdf-kg
//! endpoint, render the JSON response as the `ToolResult.output`
//! string the LLM consumes. We pretty-print the JSON so the model
//! sees structured data clearly — flat string mashing made it confuse
//! field boundaries on multi-doc results in early tests.
//!
//! The `description` field on each tool is what the model reads when
//! it picks which tool to call. They emphasize WHEN to choose this
//! tool vs. its siblings — same pattern as `BashTool` vs.
//! `FileReadTool`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};

use super::client::{PdfKgClient, PdfKgError};
use crate::tools::registry::{Tool, ToolContext, ToolError, ToolResult};

/// Convert a `PdfKgError` to the appropriate `Result<ToolResult,
/// ToolError>` variant. Soft errors (bad input, missing resource)
/// come back as `ToolResult::error` so the LLM can adapt; hard errors
/// (transport, 5xx) propagate as `ToolError::ExecutionFailed` to bail
/// the agent loop.
fn map_error(e: PdfKgError) -> Result<ToolResult, ToolError> {
    if e.is_soft() {
        Ok(ToolResult::error(e.to_string()))
    } else {
        Err(ToolError::ExecutionFailed(e.to_string()))
    }
}

fn render(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

async fn invoke(
    client: &PdfKgClient,
    name: &str,
    args: Value,
) -> Result<ToolResult, ToolError> {
    match client.invoke(name, args).await {
        Ok(v) => Ok(ToolResult::success(render(&v))),
        Err(e) => map_error(e),
    }
}

// ---------------------------------------------------------------------------
// list_jobs
// ---------------------------------------------------------------------------

pub struct ListJobsTool {
    pub client: Arc<PdfKgClient>,
}

#[async_trait]
impl Tool for ListJobsTool {
    fn name(&self) -> &str {
        "pdfkg_list_jobs"
    }
    fn description(&self) -> &str {
        "List every PDF indexed into the pdf-kg knowledge graph. \
        Returns each job's id, source filename, state (running/done/error), \
        and graph stats (node + edge counts). Use this BEFORE pdfkg_ask or \
        pdfkg_search when you don't know which job_ids are available — \
        their `job_ids` parameter accepts ids from this catalog."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {},
            "required": [],
        })
    }
    async fn execute(&self, _input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        invoke(&self.client, "list_jobs", json!({})).await
    }
}

// ---------------------------------------------------------------------------
// pdfkg_search — retrieval only, no LLM
// ---------------------------------------------------------------------------

pub struct SearchPdfTool {
    pub client: Arc<PdfKgClient>,
}

#[async_trait]
impl Tool for SearchPdfTool {
    fn name(&self) -> &str {
        "pdfkg_search"
    }
    fn description(&self) -> &str {
        "Retrieve the top-k most relevant nodes in the pdf-kg graph for a \
        natural-language query WITHOUT calling any synthesis LLM. Returns \
        ranked hits: text chunks and image captions, each tagged with \
        job_id, node_id, score, page_no, and kind. \
        Use when you want fine-grained control over downstream steps \
        (read specific chunks via pdfkg_get_page, fetch a figure via \
        pdfkg_get_image, traverse the graph via pdfkg_get_subgraph). \
        For one-shot question answering, prefer pdfkg_ask — it runs the \
        same retrieval and feeds it to a vision-capable LLM with image \
        attachments. Queries may be Korean or English; bilingual image \
        captions in the index bridge cross-lingual matching."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural-language query. Korean or English."
                },
                "k": {
                    "type": "integer",
                    "description": "Number of hits to return. Default 5; cap 50.",
                },
                "job_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Restrict retrieval to these jobs. Omit to search every indexed PDF."
                },
                "hops": {
                    "type": "integer",
                    "description": "Ego-subgraph radius around each hit. Higher = more context. Default 1."
                }
            },
            "required": ["query"],
        })
    }
    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        invoke(&self.client, "search_pdf", input).await
    }
}

// ---------------------------------------------------------------------------
// pdfkg_ask — full RAG with multimodal LLM
// ---------------------------------------------------------------------------

pub struct AskPdfTool {
    pub client: Arc<PdfKgClient>,
}

#[async_trait]
impl Tool for AskPdfTool {
    fn name(&self) -> &str {
        "pdfkg_ask"
    }
    fn description(&self) -> &str {
        "End-to-end question answering over the pdf-kg corpus. Retrieves \
        relevant chunks + figures, attaches images to a vision-capable \
        LLM, and returns a grounded answer with page citations \
        (`(p.N)` for single-doc, `(filename.pdf p.N)` for multi-doc). \
        Use this for natural-language questions where you want the \
        backend to do the heavy lifting. Use pdfkg_search + \
        pdfkg_get_page instead when you need raw retrieval results or \
        want to compose your own multi-step reasoning. \
        Slower than the retrieval tools — typically 5–30s on CPU vLLM."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "User's natural-language question."
                },
                "k": {
                    "type": "integer",
                    "description": "Retrieval depth before synthesis. Default 5."
                },
                "job_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Restrict to these jobs. Omit to query every indexed PDF."
                },
                "hops": {
                    "type": "integer",
                    "description": "Ego-subgraph radius. Default 1."
                }
            },
            "required": ["query"],
        })
    }
    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        invoke(&self.client, "ask_pdf", input).await
    }
}

// ---------------------------------------------------------------------------
// pdfkg_get_page
// ---------------------------------------------------------------------------

pub struct GetPageTool {
    pub client: Arc<PdfKgClient>,
}

#[async_trait]
impl Tool for GetPageTool {
    fn name(&self) -> &str {
        "pdfkg_get_page"
    }
    fn description(&self) -> &str {
        "Return all text chunks and image refs that live on a specific \
        physical page of an indexed PDF. Use after pdfkg_search or \
        pdfkg_ask when you want to read the surrounding context of a \
        cited page, or to discover the image_ids you can then pass to \
        pdfkg_get_image. `page_no` is the 0-based PDF index; the \
        response's `printed_page_no` carries the label printed on the \
        page itself (footer/header) when available."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "page_no": {
                    "type": "integer",
                    "description": "0-based PDF page index. Page 1 of the PDF = page_no 0."
                }
            },
            "required": ["job_id", "page_no"],
        })
    }
    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        invoke(&self.client, "get_page", input).await
    }
}

// ---------------------------------------------------------------------------
// pdfkg_get_image
//
// We DO NOT inline base64 PNG bytes into the agent's text context by
// default — a 50 KB figure → ~70 KB base64 string, which would burn
// thousands of tokens per image. Instead the tool returns metadata
// plus the URL where the bytes live; the agent can fetch them via
// `pdfkg_get_image_bytes` (separate explicit-opt-in tool) or via a
// vision-aware downstream step.
// ---------------------------------------------------------------------------

pub struct GetImageTool {
    pub client: Arc<PdfKgClient>,
}

#[async_trait]
impl Tool for GetImageTool {
    fn name(&self) -> &str {
        "pdfkg_get_image"
    }
    fn description(&self) -> &str {
        "Look up metadata for a figure extracted from a PDF — its caption \
        (bilingual when domain labels match), page_no, dimensions, mime, \
        and a `bytes_url` pointing at the raw PNG. Does NOT inline the \
        image data; for the actual bytes, the agent should fetch \
        `bytes_url` directly (or use a vision-capable downstream tool \
        that consumes URLs). Use after pdfkg_search / pdfkg_get_page to \
        learn what a hit's figure depicts."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "image_id": {
                    "type": "string",
                    "description": "Node id of the Image node, e.g. \"p5_i198\"."
                }
            },
            "required": ["job_id", "image_id"],
        })
    }
    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        let job_id = input.get("job_id").and_then(Value::as_str).map(str::to_string);
        let image_id = input.get("image_id").and_then(Value::as_str).map(str::to_string);
        match self.client.invoke("get_image", input).await {
            Ok(mut v) => {
                // Strip the heavy base64 field — the LLM doesn't need
                // the bytes in its context window. Re-add a URL hint
                // pointing at pdf-kg's raw-bytes endpoint instead.
                if let Some(obj) = v.as_object_mut() {
                    obj.remove("base64");
                    if let (Some(jid), Some(iid)) = (job_id.as_deref(), image_id.as_deref()) {
                        obj.insert(
                            "bytes_url".into(),
                            Value::String(format!(
                                "{}/api/images/{}/{}",
                                self.client.base(),
                                jid,
                                iid
                            )),
                        );
                    }
                }
                Ok(ToolResult::success(render(&v)))
            }
            Err(e) => map_error(e),
        }
    }
}

// ---------------------------------------------------------------------------
// pdfkg_get_subgraph
// ---------------------------------------------------------------------------

pub struct GetSubgraphTool {
    pub client: Arc<PdfKgClient>,
}

#[async_trait]
impl Tool for GetSubgraphTool {
    fn name(&self) -> &str {
        "pdfkg_get_subgraph"
    }
    fn description(&self) -> &str {
        "Return the ego-subgraph (nodes + edges within `hops` of \
        `node_id`) for one job. Use this to explore connections — find \
        every ImageCaption describing the same Image, every chunk \
        co-located on the same page as a hit, or chunks linked via \
        SIMILAR_TO. hops=1 typically gives DESCRIBES + CO_LOCATED + \
        HAS_PAGE neighbors; hops=2 reaches one similarity-cluster away."
    }
    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "node_id": {
                    "type": "string",
                    "description": "Center node id, from a pdfkg_search hit."
                },
                "hops": {
                    "type": "integer",
                    "description": "Radius. 0 returns just the center. Default 1; cap 3."
                }
            },
            "required": ["job_id", "node_id"],
        })
    }
    async fn execute(&self, input: Value, _ctx: &ToolContext) -> Result<ToolResult, ToolError> {
        invoke(&self.client, "get_subgraph", input).await
    }
}
