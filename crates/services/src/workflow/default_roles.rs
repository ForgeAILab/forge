pub const PLANNER: &str = "planner";
pub const CODER: &str = "coder";
pub const WORKER: &str = "worker";
pub const REVIEWER: &str = "reviewer";
/// The review runner's conformance pass runs as its own child execution.
/// It is the reviewer role in every respect that matters to authorization:
/// same role assignment, same read-only worktree, same reviewer identity.
pub const AUDITOR: &str = "auditor";
pub const ASSIGNEE: &str = "assignee";
pub const INTERACTIVE: &str = "interactive";
pub const MERGE_FIXER: &str = "merge_fixer";
pub const SYSTEM: &str = "system";
