//! Stage 4 of the root lead: the lead's operations. Every call here goes
//! through the lead-operation seam, so the database's lead fact and the one
//! command boundary are both in the path. In the end-to-end tests the lead's run is
//! scripted in the test executor: the run issues its operations and
//! completes, and the dispatcher does the rest.
use super::root_lead::{
    executions, fixture_with, park, passes, reload, root_with_child, until, Lead,
};
use super::root_replan::{log, root_with_failed_review, root_with_parked_child, spent};
use super::*;
use forge_agent_host::AgentHostError;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

#[path = "lead_operations/helpers.rs"]
mod helpers;
use helpers::*;
#[path = "lead_operations/crash_replay.rs"]
mod crash_replay;
#[path = "lead_operations/question_lifecycle.rs"]
mod question_lifecycle;
#[path = "lead_operations/questions.rs"]
mod questions;
#[path = "lead_operations/subtask_recovery.rs"]
mod subtask_recovery;
#[path = "lead_operations/subtask_updates.rs"]
mod subtask_updates;
