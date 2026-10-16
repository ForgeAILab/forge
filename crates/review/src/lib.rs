#![forbid(unsafe_code)]

pub use command::{run_workspace_command, workspace_command};
pub use outcome::{ReviewError, ReviewOutcome, StepResult};
pub use workspace::{read_git_diff, CommandLimits, CommandOutput, ReviewWorkspace};

mod command;
pub mod contract;
pub mod follow_up;
mod outcome;
mod workspace;

pub mod check_spec;
pub mod verify;

#[cfg(test)]
mod contract_driven_tests;
