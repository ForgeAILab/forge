#![forbid(unsafe_code)]

//! Repository-scoped, chat-first Forge application.
//!
//! The executable wiring lives in this crate so the startup path, controller,
//! and renderer can be exercised without an HTTP server or a physical terminal.

pub mod app;
pub mod backend;
pub mod cli;
pub mod controller;
pub mod data_dir;
pub mod keymap;
pub mod repository;
pub mod runtime_backend;
pub mod runtime_lock;
pub mod startup;
pub mod terminal;
pub mod tracing;
pub mod tui;
pub mod ui_bridge;
pub mod view;
