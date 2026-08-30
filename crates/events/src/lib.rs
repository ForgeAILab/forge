#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

pub const PROJECT_HOOK_RUN_CHANGED_EVENT: &str = "project_hook.run_changed";
pub const OPERATIONS_STATUS_CHANGED_EVENT: &str = "operations.status_changed";
pub const TASK_MOVED_EVENT: &str = "task.moved";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForgeEvent {
    pub event_type: String,
    pub entity_id: String,
    pub timestamp: String,
    #[serde(flatten)]
    pub context: EventContext,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoleAssignmentSnapshot {
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EventContext {
    TaskCreated {
        project_id: String,
        title: String,
    },
    TaskStatusChanged {
        project_id: String,
        old_status: String,
        new_status: String,
    },
    TaskMoved(api_types::TaskMovedEventPayload),
    TaskAutoTransitioned {
        task_id: String,
        from: String,
        to: String,
        reason: String,
    },
    TaskAssigned {
        project_id: String,
        agent_id: String,
        execution_id: String,
    },
    TaskRoleReassigned {
        task_id: String,
        role_name: String,
        previous_assignment: Option<RoleAssignmentSnapshot>,
        new_assignment: Option<RoleAssignmentSnapshot>,
        triggered_cancellation: bool,
        reset_workspace: bool,
        reset_worktree: bool,
        transitioned_to_todo: bool,
    },
    TaskBlocked {
        project_id: String,
        reason: String,
        kind: Option<api_types::FailureKind>,
        source: Option<String>,
        execution_id: Option<String>,
    },
    TaskUnblocked {
        project_id: String,
        previous_reason: Option<String>,
    },
    /// The Project Agent stopped responding to open incidents because its wake
    /// budget is spent. Autonomy has halted with work still outstanding, which
    /// is a user-visible stall rather than routine suppression: nothing will
    /// move until the window rolls over or the user says something.
    ProjectAutonomyStalled {
        project_id: String,
        open_incidents: i64,
        reason: String,
    },
    TaskFailed {
        project_id: String,
        reason: String,
        kind: Option<api_types::FailureKind>,
        execution_id: Option<String>,
    },
    TaskRestarted {
        project_id: String,
        previous_reason: Option<String>,
        new_execution_id: Option<String>,
    },
    TaskCancelled {
        project_id: String,
    },
    TaskRecovered {
        project_id: String,
        reason: String,
    },
    RecoveryApplied {
        project_id: String,
        task_id: String,
        action: String,
        state: Option<String>,
        transition_log_id: Option<String>,
    },
    TaskUpdated {
        project_id: String,
    },
    TaskDeleted {
        project_id: String,
    },
    TaskDependencySatisfied {
        task_id: String,
        depends_on_id: String,
        timestamp: String,
    },
    ExecutionStarted {
        task_id: String,
        agent_id: Option<String>,
    },
    ExecutionLog {
        task_id: String,
        log: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        logs: Option<Vec<serde_json::Value>>,
    },
    ExecutionCompleted {
        task_id: String,
    },
    ExecutionFailed {
        task_id: String,
        error: String,
    },
    ExecutionCancelled {
        task_id: String,
        reason: String,
    },
    TaskExecutionRetry {
        task_id: String,
        execution_id: String,
        attempt: u32,
        delay_seconds: u64,
        next_dispatch_at: String,
    },
    ExecutionStalled {
        task_id: String,
        execution_id: String,
        stale_before: String,
    },
    ExecutionDaemonDisconnected {
        task_id: String,
        execution_id: String,
        daemon_id: String,
    },
    ReconciliationEvent {
        task_id: Option<String>,
        execution_id: Option<String>,
        reason: String,
    },
    TaskExecutionCancelled {
        task_id: String,
        execution_id: String,
        reason: String,
    },
    AgentStatusChanged {
        old_status: String,
        new_status: String,
    },
    AgentCreated {
        name: String,
    },
    AgentArchived {},
    AgentPaused {},
    AgentResumed {},
    ProfileCreated {
        name: String,
    },
    ProfileUpdated {},
    ProfileDeleted {},
    AgentTimeout {
        last_heartbeat: String,
    },
    WorkspaceCreated {
        task_id: String,
        path: String,
    },
    WorkspaceExecutionWaiting {
        workspace_id: String,
        task_id: String,
    },
    WorkspaceCleaned {
        workspace_id: String,
        task_id: String,
        status: String,
    },
    TaskTerminalSessionChanged {
        task_id: String,
        session_id: String,
        workspace_id: String,
        kind: String,
        status: String,
        reason: Option<String>,
    },
    TaskSubtaskSequenceStarted {
        task_id: String,
    },
    TaskSubtaskSequencePaused {
        task_id: String,
        subtask_id: String,
        reason: String,
    },
    TaskSubtaskSequenceResumed {
        task_id: String,
        subtask_id: String,
    },
    TaskSubtaskCommitRecorded {
        task_id: String,
        subtask_id: String,
        result_type: String,
        commit_sha: Option<String>,
    },
    ReviewStarted {
        task_id: String,
        attempt_number: i64,
    },
    ReviewDecided {
        task_id: String,
        status: String,
    },
    ReviewPassed {
        task_id: String,
        review_id: String,
        attempt_number: i64,
    },
    ReviewFailed {
        task_id: String,
        review_id: String,
        attempt_number: i64,
        failed_step_index: usize,
    },
    ReviewApproved {
        task_id: String,
        review_id: String,
    },
    ReviewRejected {
        task_id: String,
        review_id: String,
        reason: String,
    },
    MergeStarted {
        task_id: String,
    },
    MergeSucceeded {
        task_id: String,
    },
    MergeFailed {
        task_id: String,
        reason: String,
    },
    CommentCreated {
        task_id: String,
        comment_id: String,
        author_type: String,
        author_name: String,
    },
    TaskMediaUploaded {
        task_id: String,
        media_id: String,
        content_type: String,
        byte_size: i64,
        filename: String,
    },
    TaskMediaDeleted {
        task_id: String,
        media_id: String,
    },
    FollowUpDispatched {
        task_id: String,
        parent_execution_id: String,
        execution_id: String,
        trigger: String,
    },
    TaskRoleAgentDispatched {
        task_id: String,
        role: String,
        agent_id: String,
        state: String,
        parent_execution_id: Option<String>,
        prompt_system: String,
        prompt_user: String,
    },
    TaskAwaitingHuman {
        task_id: String,
        role: String,
        assignee_id: String,
        state: String,
    },
    ProjectCreated {
        name: String,
    },
    ProjectUpdated {},
    ProjectDeleted {},
    ProjectPaused {
        paused_at: String,
    },
    ProjectResumed {},
    DaemonRegistered {},
    DaemonConnected {},
    DaemonReportReceived {
        detected_clis_count: usize,
    },
    DaemonOffline {},
    TransitionEffectFailed {
        task_id: String,
        from_state: String,
        to_state: String,
        action: String,
        error: String,
    },
    TransitionGuardRejected {
        task_id: String,
        from_state: String,
        to_state: String,
        guard_name: String,
        reason: String,
    },
    TransitionCascadeDepthExceeded {
        task_id: String,
        state: String,
        depth: u8,
    },
    TaskRoleNotified {
        task_id: String,
        role: String,
        notified_agent_id: Option<String>,
        notified_user_handle: Option<String>,
        state: String,
        reason: String,
    },
    NotificationCreated {
        notification_id: String,
        project_id: String,
        task_id: Option<String>,
        event_type: String,
        title: String,
    },
    OperationsStatusChanged {
        trigger: String,
    },
    ProjectHookRunChanged {
        project_id: String,
        run_id: String,
        rule_id: String,
        trigger_type: String,
        dedupe_key: String,
        status: String,
        source_task_id: Option<String>,
        automation_task_id: Option<String>,
        execution_id: Option<String>,
        agent_id: Option<String>,
        reason: Option<String>,
    },
    DomainEventCommitted {
        sequence: i64,
        entity_type: String,
        scope_type: String,
        scope_id: String,
    },
    AgentChatMessageAdmitted {
        chat_id: String,
        message_id: String,
        author_type: String,
    },
    AgentChatTurnProgress {
        chat_id: String,
        turn_job_id: String,
        delta: String,
    },
    AgentChatResponseCompleted {
        chat_id: String,
        turn_job_id: String,
        message_id: String,
    },
    AgentChatUpdated {
        chat_id: String,
        status: String,
    },
    ExternalSyncCompleted {
        integration_id: String,
        imported_count: u32,
        skipped_count: u32,
    },
    ExternalSyncFailed {
        integration_id: String,
        error: String,
    },
    Empty {},
}

#[derive(Debug)]
pub struct EventBus {
    sender: broadcast::Sender<ForgeEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    pub fn with_default_capacity() -> Self {
        Self::new(1024)
    }

    pub fn publish(&self, event: ForgeEvent) -> usize {
        // Returns the number of receivers that received the event.
        // If no subscribers, this returns 0 (not an error).
        self.sender.send(event).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ForgeEvent> {
        self.sender.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::with_default_capacity()
    }
}

/// Helper to create a timestamp for events.
pub fn event_timestamp() -> String {
    chrono::Utc::now().to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_and_receive_event() {
        let bus = EventBus::with_default_capacity();
        let mut rx = bus.subscribe();

        let event = ForgeEvent {
            event_type: "task.created".to_string(),
            entity_id: "task-123".to_string(),
            timestamp: event_timestamp(),
            context: EventContext::TaskCreated {
                project_id: "proj-1".to_string(),
                title: "Test task".to_string(),
            },
        };

        bus.publish(event.clone());

        let received = rx.recv().await.unwrap();
        assert_eq!(received.event_type, "task.created");
        assert_eq!(received.entity_id, "task-123");
    }

    #[tokio::test]
    async fn multiple_subscribers_receive_events() {
        let bus = EventBus::with_default_capacity();
        let mut rx1 = bus.subscribe();
        let mut rx2 = bus.subscribe();

        let event = ForgeEvent {
            event_type: "task.status_changed".to_string(),
            entity_id: "task-456".to_string(),
            timestamp: event_timestamp(),
            context: EventContext::TaskStatusChanged {
                project_id: "proj-1".to_string(),
                old_status: "todo".to_string(),
                new_status: "in_progress".to_string(),
            },
        };

        let count = bus.publish(event);
        assert_eq!(count, 2);

        let r1 = rx1.recv().await.unwrap();
        let r2 = rx2.recv().await.unwrap();
        assert_eq!(r1.entity_id, r2.entity_id);
    }

    #[tokio::test]
    async fn lagged_subscriber_gets_error() {
        let bus = EventBus::new(2); // tiny capacity
        let mut rx = bus.subscribe();

        // Publish 3 events into a capacity-2 channel
        for i in 0..3 {
            bus.publish(ForgeEvent {
                event_type: "task.updated".to_string(),
                entity_id: format!("task-{i}"),
                timestamp: event_timestamp(),
                context: EventContext::TaskUpdated {
                    project_id: format!("proj-{i}"),
                },
            });
        }

        // The first recv should return Lagged error
        let result = rx.recv().await;
        match result {
            Err(broadcast::error::RecvError::Lagged(_)) => {} // expected
            other => panic!("Expected Lagged error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn no_subscribers_publish_succeeds() {
        let bus = EventBus::with_default_capacity();
        // No subscribers — publish should still work (returns 0)
        let count = bus.publish(ForgeEvent {
            event_type: "agent.created".to_string(),
            entity_id: "agent-1".to_string(),
            timestamp: event_timestamp(),
            context: EventContext::AgentCreated {
                name: "test".to_string(),
            },
        });
        assert_eq!(count, 0);
    }
}
