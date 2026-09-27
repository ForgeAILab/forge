mod common;
mod dispatch;
mod gates;
mod lifecycle;
mod merge;
mod review;
mod subtasks;

pub(crate) use common::review_ci_steps;
pub use dispatch::{DispatchExecutor, DispatchFixAgent, DispatchRoleAgent, NotifyRoleHolder};
pub use gates::{
    AutoCascadeOnUnassignedRole, CheckRetryBudget, DependencyGate, RequireCleanWorktree,
    RequirePlanChecklistComplete, RequireUpstreamRolesCompleted,
};
pub use lifecycle::{
    AutoCascadeOnCompletion, CleanupWorkspaceNow, PublishTaskBlocked, RunBeforeWorkHooks,
    ScheduleWorkspaceCleanup,
};
pub use merge::{AutoCascadeOnMergeResult, CheckMergeFixBudget, RunMerge};
pub use review::{AutoCascadeOnReviewPass, AutoCascadeOnUnconfiguredReview, RunCiSteps};
pub use subtasks::{
    CancelPendingSubtasks, PropagateDoneToSubtasks, SatisfyDependents, SubtaskSequenceComplete,
};

#[cfg(test)]
mod tests;
