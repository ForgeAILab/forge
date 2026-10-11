#![forbid(unsafe_code)]

pub use command::{run_workspace_command, workspace_command};
pub use outcome::{ReviewError, ReviewOutcome, StepResult};
pub use workspace::{read_git_diff, CommandLimits, CommandOutput, ReviewWorkspace};

pub mod auditor;
mod command;
pub mod contract;
pub mod follow_up;
mod outcome;
mod workspace;

pub mod check_spec;

#[cfg(test)]
mod contract_driven_tests;
