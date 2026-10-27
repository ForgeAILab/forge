//! The step check (`lead_check`): an optional gate in the working phase of the
//! inherited subtask workflow, between implementation and `done`. Its role is
//! held by the root's lead. Without a lead the gate is skipped on entry; with
//! one, the lead runs a read-only execution on the subtask and its result
//! block accepts the step or returns it to the same worker thread.
use std::collections::VecDeque;

use super::*;

#[path = "lead_check/helpers.rs"]
mod helpers;
use helpers::*;
#[path = "lead_check/agent_lifecycle.rs"]
mod agent_lifecycle;
#[path = "lead_check/budgets.rs"]
mod budgets;
#[path = "lead_check/lifecycle.rs"]
mod lifecycle;
#[path = "lead_check/prompts_and_recovery.rs"]
mod prompts_and_recovery;
#[path = "lead_check/verdicts.rs"]
mod verdicts;
