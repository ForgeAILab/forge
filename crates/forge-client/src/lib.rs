#![forbid(unsafe_code)]

pub mod agent;
pub mod analytics;
pub mod auth;
pub mod client;
pub mod daemon;
pub mod daemon_config;
#[doc(hidden)]
pub mod daemon_fs;
pub mod daemon_link;
mod daemon_outbox;
pub mod daemon_persistence;
pub mod daemon_runtime;
pub mod daemon_workspace;
pub mod embedded;
pub mod mcp;
pub mod memory;
pub mod output;
mod password_prompt;
pub mod project;
pub mod provider_login;
pub mod repo;
pub mod run;
pub mod task;

#[derive(Clone, clap::ValueEnum)]
pub enum OutputFormat {
    Json,
    Table,
}
