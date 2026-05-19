//! Rust Agent Core Engine
//! 
//! A high-performance AI agent implementation in Rust.

pub mod api;
pub mod backtest;
pub mod config;
pub mod coredb;
pub mod daemon;
pub mod data;
pub mod engine;
pub mod execution;
pub mod risk;
pub mod tools;
pub mod memory;
pub mod notification;
pub mod tui;
pub mod web;

pub use engine::QueryEngine;
pub use tools::{Tool, ToolRegistry, ToolResult};
