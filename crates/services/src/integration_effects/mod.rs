//! Persistence-free workspace effects. Inputs are frozen by the Task-step
//! consumer; returned facts confer no workflow, review or queue authority.
//! The module has no repository, event publisher or Task-service capability.
//!
//! Effects are phased where necessary: candidate evidence precedes local Git,
//! while remote admission precedes each exchange and receipt retention follows
//! typed validation. Recorders own those seams, including retries and recovery.
use crate::{
    merge_service::MergeOutcome,
    workspace_backend::{RunResult, RunSpec},
};

pub mod check;
pub mod merge;
pub mod rebase;
pub mod rpc;
mod types;
pub use types::*;

#[cfg(test)]
mod tests;
