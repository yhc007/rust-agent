//! End-to-end smoke test: rust-agent's pdfkg tools call a real
//! pdf-kg backend and round-trip JSON cleanly.
//!
//! Auto-skip when the backend isn't reachable so this test doesn't
//! break CI on machines without a running pdf-kg. We probe with a
//! 2-second TCP-level check before invoking any tool.

use std::sync::Arc;

use rust_agent::tools::pdfkg::{client::PdfKgClient, tools as pdfkg_tools};
use rust_agent::tools::{Tool, ToolContext};
use serde_json::{json, Value};

const BASE: &str = "http://localhost:8088";

async fn backend_reachable() -> bool {
    // Quick health probe — 2s timeout. Returns false if pdf-kg isn't
    // listening or returned anything other than 200 on /api/health.
    // Async because every caller is already inside a tokio runtime;
    // building a nested runtime would panic.
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
    {
        Ok(c) => c,
        Err(_) => return false,
    };
    client
        .get(format!("{BASE}/api/health"))
        .send()
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

fn client() -> Arc<PdfKgClient> {
    Arc::new(PdfKgClient::new(BASE))
}

#[tokio::test]
async fn list_jobs_returns_json_with_job_summaries() {
    if !backend_reachable().await {
        eprintln!("skip: pdf-kg not reachable at {BASE}");
        return;
    }
    let tool = pdfkg_tools::ListJobsTool { client: client() };
    let ctx = ToolContext::default();
    let result = tool.execute(json!({}), &ctx).await.expect("list_jobs ok");
    assert!(!result.is_error, "list_jobs reported soft failure: {}", result.output);
    let parsed: Value = serde_json::from_str(&result.output).expect("output is valid JSON");
    assert!(parsed.is_array(), "expected array, got: {parsed}");
    // If there are jobs, each entry should have the contract fields.
    if let Some(first) = parsed.as_array().and_then(|a| a.first()) {
        assert!(first.get("job_id").is_some());
        assert!(first.get("source").is_some());
        assert!(first.get("state").is_some());
    }
}

#[tokio::test]
async fn search_pdf_returns_hits_array() {
    if !backend_reachable().await {
        eprintln!("skip: pdf-kg not reachable at {BASE}");
        return;
    }
    let tool = pdfkg_tools::SearchPdfTool { client: client() };
    let ctx = ToolContext::default();
    let result = tool
        .execute(json!({ "query": "안전", "k": 3 }), &ctx)
        .await
        .expect("search_pdf ok");
    let parsed: Value = serde_json::from_str(&result.output).expect("valid JSON");
    let hits = parsed.get("hits").and_then(|h| h.as_array());
    assert!(hits.is_some(), "expected 'hits' array, got: {parsed}");
}

#[tokio::test]
async fn search_pdf_with_missing_query_returns_soft_error() {
    if !backend_reachable().await {
        eprintln!("skip: pdf-kg not reachable at {BASE}");
        return;
    }
    let tool = pdfkg_tools::SearchPdfTool { client: client() };
    let ctx = ToolContext::default();
    // bad input — backend returns 400 / bad_input. PdfKgError::is_soft
    // is true for that kind, so the tool surface should report it as
    // a soft failure that the LLM can recover from.
    let result = tool.execute(json!({}), &ctx).await.expect("no transport error");
    assert!(result.is_error, "expected soft error for missing query, got: {}", result.output);
    assert!(
        result.output.contains("bad_input") || result.output.contains("query"),
        "soft error should mention the issue: {}", result.output
    );
}

#[tokio::test]
async fn get_image_strips_base64_but_provides_bytes_url() {
    if !backend_reachable().await {
        eprintln!("skip: pdf-kg not reachable at {BASE}");
        return;
    }
    // Find any image we can ask about.
    let list_tool = pdfkg_tools::ListJobsTool { client: client() };
    let ctx = ToolContext::default();
    let jobs_out = list_tool.execute(json!({}), &ctx).await.expect("list_jobs");
    let jobs: Vec<Value> = serde_json::from_str(&jobs_out.output).expect("jobs json");
    let Some(job) = jobs.first() else {
        eprintln!("skip: no indexed jobs to probe");
        return;
    };
    let job_id = job["job_id"].as_str().expect("job_id string");

    // Use search to surface an ImageCaption hit, then map to image_id.
    let search_tool = pdfkg_tools::SearchPdfTool { client: client() };
    let search_out = search_tool
        .execute(json!({"query": "사진", "k": 10, "job_ids": [job_id]}), &ctx)
        .await
        .expect("search ok");
    let search: Value = serde_json::from_str(&search_out.output).expect("search json");
    let image_id = search["hits"]
        .as_array()
        .and_then(|hits| {
            hits.iter().find_map(|h| {
                let kind = h.get("kind").and_then(Value::as_str)?;
                let node_id = h.get("node_id").and_then(Value::as_str)?;
                if kind == "ImageCaption" {
                    Some(node_id.trim_end_matches("_caption").to_string())
                } else if kind == "Image" {
                    Some(node_id.to_string())
                } else {
                    None
                }
            })
        });
    let Some(image_id) = image_id else {
        eprintln!("skip: no image hits in test corpus");
        return;
    };

    let img_tool = pdfkg_tools::GetImageTool { client: client() };
    let img_out = img_tool
        .execute(json!({"job_id": job_id, "image_id": image_id}), &ctx)
        .await
        .expect("get_image ok");
    let parsed: Value = serde_json::from_str(&img_out.output).expect("valid JSON");

    // Contract: base64 must NOT leak into the LLM context, but a
    // bytes_url must point at pdf-kg's raw-bytes endpoint.
    assert!(
        parsed.get("base64").is_none(),
        "base64 leaked into LLM context: {parsed}"
    );
    let url = parsed.get("bytes_url").and_then(Value::as_str);
    assert!(url.is_some(), "missing bytes_url: {parsed}");
    let url = url.unwrap();
    assert!(url.starts_with(BASE), "bytes_url has wrong base: {url}");
    assert!(url.contains("/api/images/"), "bytes_url has wrong path: {url}");
}
