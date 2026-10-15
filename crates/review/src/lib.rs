#![forbid(unsafe_code)]

pub use command::{run_workspace_command, workspace_command};
pub use outcome::{ReviewError, ReviewOutcome, StepResult};
pub use workspace::{read_git_diff, CommandLimits, CommandOutput, ReviewWorkspace};

mod command;
pub mod contract;
pub mod findings;
pub mod follow_up;
pub use findings::finding_ledger;
mod outcome;
mod workspace;

pub mod check_spec;

#[cfg(test)]
mod contract_driven_tests;
