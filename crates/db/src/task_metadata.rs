use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::Task;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordered_sequence_started: Option<bool>,
    #[serde(default, flatten)]
    pub extra: Map<String, Value>,
}

impl TaskMetadata {
    pub fn parse(raw: Option<&str>) -> Result<Self, serde_json::Error> {
        raw.map(serde_json::from_str)
            .transpose()
            .map(Option::unwrap_or_default)
    }

    pub fn to_json(&self) -> Option<String> {
        if self.ordered_sequence_started.is_none() && self.extra.is_empty() {
            return None;
        }
        Some(serde_json::to_string(self).expect("task metadata serialization is infallible"))
    }
}

impl Task {
    pub fn metadata(&self) -> Result<TaskMetadata, serde_json::Error> {
        TaskMetadata::parse(self.metadata_json.as_deref())
    }

    /// The `status` field of the task's entry barrier, when one is recorded.
    ///
    /// The workflow engine persists a transition into a state that has blocking
    /// `before_enter` hooks behind an entry barrier: `running` while the hooks
    /// execute, `blocked` when one of them failed and the task needs attention.
    pub fn entry_barrier_status(&self) -> Option<String> {
        let raw = self.entry_barrier_json.as_deref()?;
        let barrier: Value = serde_json::from_str(raw).ok()?;
        barrier.get("status")?.as_str().map(str::to_owned)
    }

    /// True while the task's current state is still running its blocking
    /// `before_enter` hooks. The status change is already visible, but the
    /// transition has not settled: the engine clears the barrier (bumping the
    /// task version) once the hooks finish.
    pub fn entry_barrier_is_running(&self) -> bool {
        self.entry_barrier_status().as_deref() == Some("running")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_metadata_parse_preserves_known_and_extra_fields() {
        let metadata = TaskMetadata::parse(Some(r#"{"ordered_sequence_started":true,"custom":7}"#))
            .expect("metadata parses");

        assert_eq!(metadata.ordered_sequence_started, Some(true));
        assert_eq!(metadata.extra.get("custom"), Some(&Value::from(7)));
        assert_eq!(
            metadata.to_json().as_deref(),
            Some(r#"{"ordered_sequence_started":true,"custom":7}"#)
        );
    }

    #[test]
    fn task_metadata_to_json_omits_empty_metadata() {
        assert_eq!(TaskMetadata::default().to_json(), None);
    }

    fn task_with_entry_barrier(entry_barrier_json: Option<&str>) -> Task {
        Task {
            id: "task".to_owned(),
            project_id: "project".to_owned(),
            repo_id: None,
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "task".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "review".to_owned(),
            is_automation: false,
            priority: 0,
            board_position: 0.0,
            subtask_order: None,
            task_state_config: None,
            merge_config: None,
            metadata_json: None,
            plan: None,
            error_annotation: None,
            blocked_json: None,
            failed_json: None,
            entry_barrier_json: entry_barrier_json.map(str::to_owned),
            review_passed_at: None,
            archived_at: None,
            deleted_at: None,
            version: 1,
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
        }
    }

    #[test]
    fn entry_barrier_is_running_only_for_a_running_barrier() {
        assert!(task_with_entry_barrier(Some(
            r#"{"state":"review","status":"running","started_at":"2026-01-01T00:00:00Z"}"#
        ))
        .entry_barrier_is_running());
        assert!(!task_with_entry_barrier(Some(
            r#"{"state":"review","status":"blocked","blocking_reason":"ci"}"#
        ))
        .entry_barrier_is_running());
        assert!(!task_with_entry_barrier(None).entry_barrier_is_running());
        assert!(!task_with_entry_barrier(Some("not json")).entry_barrier_is_running());
        assert_eq!(
            task_with_entry_barrier(Some(r#"{"status":"blocked"}"#))
                .entry_barrier_status()
                .as_deref(),
            Some("blocked")
        );
    }
}
