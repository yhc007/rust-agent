//! 임시 검증 스크립트 — agent loop가 Anthropic API에 보낼
//! ToolDefinition[]가 정확히 어떻게 생겼는지 확인.
use rust_agent::tools::{create_default_registry, tool_to_definition};

fn main() {
    let registry = create_default_registry();
    let defs: Vec<_> = registry.all().iter().map(|t| tool_to_definition(t.as_ref())).collect();
    println!("=== {} tools registered ===\n", defs.len());
    for d in &defs {
        println!("┌─ {}", d.name);
        let desc_first_line = d.description.lines().next().unwrap_or("").trim();
        let cont = if d.description.lines().count() > 1 { " …" } else { "" };
        println!("│  {desc_first_line}{cont}");
        let req: Vec<String> = d.input_schema["required"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let opt_props: Vec<String> = d.input_schema["properties"].as_object()
            .map(|m| m.keys().filter(|k| !req.contains(k)).cloned().collect())
            .unwrap_or_default();
        println!("│  required: {req:?}");
        if !opt_props.is_empty() {
            println!("│  optional: {opt_props:?}");
        }
        println!("└─");
    }
    let pdfkg_count = defs.iter().filter(|d| d.name.starts_with("pdfkg_")).count();
    println!("\nlocal tools: {}, pdfkg tools: {}", defs.len() - pdfkg_count, pdfkg_count);
}
