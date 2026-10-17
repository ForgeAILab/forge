pub(crate) mod carry;
mod common;
pub(crate) use common::{review_ci_steps, system_comment, task_is_read_only, with_comment_key};
mod dispatch;
mod gates;
mod lifecycle;
mod merge;
pub(crate) mod no_change;
mod review;
mod subtasks;
mod verify;

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
pub(crate) use merge::guard_test_faults;
#[cfg(test)]
pub(crate) use merge::test_faults as merge_test_faults;
pub(crate) use merge::{conflict_handoff_result, merge_failure_result, record_integration_failure};
pub use merge::{CheckMergeFixBudget, RequireConflictMarkersResolved, RunMerge};
#[doc(hidden)]
pub use review::halt as review_entry_halt;
pub use review::{
    run_ci_steps_in_step, set_entry_check_wall_seconds_for_test, AutoCascadeOnReviewPass,
    AutoCascadeOnUnconfiguredReview, RunCiSteps,
};
pub(crate) use review::{
    ENTRY_CHECK_CONFIG_EFFECT, ENTRY_CHECK_REREQUESTS_EFFECT, ENTRY_CHECK_REREQUEST_LIMIT,
};
pub use subtasks::{
    CancelPendingSubtasks, PropagateDoneToSubtasks, SatisfyDependents, SubtaskSequenceComplete,
};

#[cfg(test)]
mod tests;
