//! Task compaction and Agent Chat context-mode tests.

use super::*;

#[test]
fn task_turns_use_structural_compaction_instead_of_lcm() {
    let task_scope = CanonicalScope {
        scope_type: CanonicalScopeType::Task,
        scope_id: "task-1".to_owned(),
        workspace_access: WorkspaceAccess::TaskRead,
    };
    let reviewer = turn_context_mode(&task_scope, Some("reviewer"));
    assert!(!reviewer.persistent_session);
    assert!(!reviewer.lcm);
    assert!(reviewer.structural_compaction);

    for role in ["planner", "worker"] {
        let mode = turn_context_mode(&task_scope, Some(role));
        assert!(mode.persistent_session, "{role} keeps protected continuity");
        assert!(!mode.lcm, "{role} must not use Task-scoped LCM");
        assert!(
            mode.structural_compaction,
            "{role} uses deterministic structural compaction"
        );
    }

    let policy = task_structural_compactor(100_000).policy().clone();
    assert_eq!(policy.high_watermark, 85_000);
    assert_eq!(policy.low_watermark, 70_000);
}

#[test]
fn agent_chat_turns_keep_lcm() {
    let chat_scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: "chat-1".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    let mode = turn_context_mode(&chat_scope, None);
    assert!(mode.persistent_session);
    assert!(mode.lcm);
    assert!(!mode.structural_compaction);
}
