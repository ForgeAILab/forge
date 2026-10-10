pub(crate) mod carry;
mod common;
pub(crate) use common::{review_ci_steps, system_comment, with_comment_key};
mod dispatch;
mod gates;
mod lifecycle;
mod merge;
mod review;
mod subtasks;

pub use carry::CarryReviewAuthority;
pub use dispatch::{DispatchExecutor, DispatchFixAgent, DispatchRoleAgent, NotifyRoleHolder};
pub use gates::{
    AutoCascadeOnUnassignedRole, CheckRetryBudget, DependencyGate, RequireCleanWorktree,
    RequirePlanChecklistComplete, RequireUpstreamRolesCompleted,
};
pub use lifecycle::{
    AutoCascadeOnCompletion, CleanupWorkspaceNow, PublishTaskBlocked, RunBeforeWorkHooks,
    ScheduleWorkspaceCleanup,
};
#[cfg(test)]
pub(crate) use merge::test_faults as merge_test_faults;
pub(crate) use merge::{conflict_handoff_result, merge_failure_result, record_integration_failure};
pub use merge::{
    AutoCascadeOnMergeResult, CheckMergeFixBudget, RequireConflictMarkersResolved, RunMerge,
};
pub use review::{
    run_ci_steps_in_step, AutoCascadeOnReviewPass, AutoCascadeOnUnconfiguredReview, RunCiSteps,
};
pub use subtasks::{
    CancelPendingSubtasks, PropagateDoneToSubtasks, SatisfyDependents, SubtaskSequenceComplete,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use merge::target_moved_result;
