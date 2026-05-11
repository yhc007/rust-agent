//! Rust Agent Core Engine
//! 
//! A high-performance AI agent implementation in Rust.

pub mod api;
pub mod config;
pub mod engine;
pub mod tools;
pub mod memory;
pub mod tui;
pub mod web;

pub use engine::QueryEngine;
pub use tools::{Tool, ToolRegistry, ToolResult};
