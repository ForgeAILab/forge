#![forbid(unsafe_code)]

pub use command::{run_workspace_command, workspace_command};
pub use runner::{
    read_git_diff, ReviewError, ReviewOutcome, ReviewRequest, ReviewRunner, StepResult,
};
pub use workspace::{CommandLimits, CommandOutput, ReviewWorkspace};

pub mod auditor;
mod command;
pub mod contract;
pub mod follow_up;
mod runner;
mod workspace;
