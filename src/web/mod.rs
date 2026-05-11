//! Embedded web server for the agent-stream UI.
//!
//! - `GET  /`               serves the single-page HTML at
//!                          [`INDEX_HTML`].
//! - `POST /api/agent/run`  body `{prompt: "..."}` → SSE stream of
//!                          [`AgentEvent`] JSON payloads, one per
//!                          `data:` line. The browser consumes via
//!                          fetch + ReadableStream (not
//!                          EventSource, because EventSource is
//!                          GET-only).
//!
//! Each POST spawns a fresh [`QueryEngine`], so requests are
//! independent — no shared conversation state across browser
//! sessions. Conversation history within a single run does
//! accumulate inside the engine, as the CLI path always has.

use std::convert::Infallible;
use std::sync::Arc;

use anyhow::Context;
use axum::extract::State;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream::{Stream, StreamExt};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::config::Config;
use crate::engine::{AgentEvent, QueryEngine};

const INDEX_HTML: &str = include_str!("index.html");

/// Shared state plucked from the CLI invocation. Cloned per request
/// so each agent run starts from the same backend / model defaults.
#[derive(Clone)]
struct AppState {
    config: Arc<Config>,
    model_override: String,
}

#[derive(Debug, Deserialize)]
struct RunRequest {
    prompt: String,
}

/// Build the router. Caller wraps in `serve(addr, router)`.
pub fn build_router(config: Config, model_override: String) -> Router {
    let state = AppState {
        config: Arc::new(config),
        model_override,
    };
    Router::new()
        .route("/", get(index))
        .route("/api/agent/run", post(agent_run))
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

async fn index() -> Response {
    let mut resp = INDEX_HTML.into_response();
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    resp
}

async fn agent_run(
    State(state): State<AppState>,
    Json(req): Json<RunRequest>,
) -> Response {
    if req.prompt.trim().is_empty() {
        return (StatusCode::BAD_REQUEST, "prompt must not be empty").into_response();
    }

    let (tx, rx) = mpsc::unbounded_channel::<AgentEvent>();

    // Build a fresh engine on a blocking-friendly tokio task. The
    // engine itself is async — we just want it on its own task so
    // the response future returns immediately and the SSE stream
    // pipes events as they're produced.
    let config = (*state.config).clone();
    let model_override = state.model_override.clone();
    let prompt = req.prompt;
    tokio::spawn(async move {
        match QueryEngine::new(config, model_override).context("build engine") {
            Ok(mut engine) => {
                if let Err(e) = engine.process_input_streamed(&prompt, tx.clone()).await {
                    // The engine emits its own Error event on the
                    // channel before this branch fires, so logging
                    // is enough here.
                    tracing::warn!(error = %e, "agent loop returned error");
                }
            }
            Err(e) => {
                let _ = tx.send(AgentEvent::Error {
                    message: format!("engine init failed: {e}"),
                });
            }
        }
    });

    let stream = event_stream(rx);
    Sse::new(stream).keep_alive(KeepAlive::default()).into_response()
}

fn event_stream(
    rx: mpsc::UnboundedReceiver<AgentEvent>,
) -> impl Stream<Item = Result<Event, Infallible>> {
    UnboundedReceiverStream::new(rx).map(|ev| {
        let json = serde_json::to_string(&ev)
            .unwrap_or_else(|_| r#"{"type":"error","message":"event serialize"}"#.to_string());
        Ok(Event::default().data(json))
    })
}
