//! Daemon workspace protocol test modules.

use super::*;

use helpers::*;
pub(super) use helpers::{attempt_merge_params, attempt_request, merge_params, retain_intent};

#[path = "protocol/helpers.rs"]
mod helpers;
#[path = "protocol/inspection.rs"]
mod inspection;
#[path = "protocol/integration_attempts.rs"]
mod integration_attempts;
#[path = "protocol/owner_operations.rs"]
mod owner_operations;
#[path = "protocol/reconciliation.rs"]
mod reconciliation;
#[path = "protocol/workspace_runs.rs"]
mod workspace_runs;
