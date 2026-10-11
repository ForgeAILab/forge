use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::Task;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TaskMetadata {
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
        if self.extra.is_empty() {
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
    /// Blocking `before_enter` failures remain `blocked` until recovery.
    /// Running hook ownership is recorded by the leased task-step queue.
    pub fn entry_barrier_status(&self) -> Option<String> {
        let raw = self.entry_barrier_json.as_deref()?;
        let barrier: Value = serde_json::from_str(raw).ok()?;
        barrier.get("status")?.as_str().map(str::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_metadata_parse_preserves_extra_fields() {
        let metadata = TaskMetadata::parse(Some(r#"{"custom":7}"#)).expect("metadata parses");

        assert_eq!(metadata.extra.get("custom"), Some(&Value::from(7)));
        assert_eq!(metadata.to_json().as_deref(), Some(r#"{"custom":7}"#));
    }

    #[test]
    fn task_metadata_to_json_omits_empty_metadata() {
        assert_eq!(TaskMetadata::default().to_json(), None);
    }
}
