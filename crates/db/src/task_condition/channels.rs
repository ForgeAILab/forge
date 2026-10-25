//! Storage values for the converted condition writers. Historical rows still
//! use the mapper's lenient decoder; these closed shapes are for new writes.
use api_types::FailureKind;
use serde::{Deserialize, Deserializer, Serialize};

/// The tag is closed on new writes. Serialization retains the original
/// writer's field order rather than serde's default tag-first ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorAnnotation {
    WorkspaceResetRequired(WorkspaceResetAnnotation),
    ExecutorFailed(ExecutorFailureAnnotation),
    WorkflowLoop(StepFailureAnnotation),
    CascadeFailed(StepFailureAnnotation),
    WorkspaceError(StepFailureAnnotation),
    MergeConflict(StepFailureAnnotation),
    CiFailed(StepFailureAnnotation),
    WithStepFailure {
        annotation: Box<ErrorAnnotation>,
        overlay: StepFailureOverlay,
    },
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AnnotationWire {
    WorkspaceResetRequired(WorkspaceResetAnnotation),
    ExecutorFailed(ExecutorFailureAnnotation),
    WorkflowLoop(StepFailureAnnotation),
    CascadeFailed(StepFailureAnnotation),
    WorkspaceError(StepFailureAnnotation),
    MergeConflict(StepFailureAnnotation),
    CiFailed(StepFailureAnnotation),
}

impl<'de> Deserialize<'de> for ErrorAnnotation {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        let mut fields = serde_json::Map::<String, serde_json::Value>::deserialize(decoder)?;
        let overlay = if let Some(error) = fields.remove("task_step_error") {
            let id = fields
                .get("task_step_id")
                .cloned()
                .ok_or_else(|| serde::de::Error::missing_field("task_step_id"))?;
            let overlay = StepFailureOverlay {
                task_step_error: serde_json::from_value(error).map_err(serde::de::Error::custom)?,
                task_step_id: serde_json::from_value(id).map_err(serde::de::Error::custom)?,
            };
            if matches!(
                fields.get("type").and_then(serde_json::Value::as_str),
                Some("workspace_reset_required" | "executor_failed")
            ) {
                fields.remove("task_step_id");
            }
            Some(overlay)
        } else {
            None
        };
        let wire: AnnotationWire =
            serde_json::from_value(fields.into()).map_err(serde::de::Error::custom)?;
        let annotation = match wire {
            AnnotationWire::WorkspaceResetRequired(value) => Self::WorkspaceResetRequired(value),
            AnnotationWire::ExecutorFailed(value) => Self::ExecutorFailed(value),
            AnnotationWire::WorkflowLoop(value) => Self::WorkflowLoop(value),
            AnnotationWire::CascadeFailed(value) => Self::CascadeFailed(value),
            AnnotationWire::WorkspaceError(value) => Self::WorkspaceError(value),
            AnnotationWire::MergeConflict(value) => Self::MergeConflict(value),
            AnnotationWire::CiFailed(value) => Self::CiFailed(value),
        };
        Ok(match overlay {
            Some(overlay) => Self::WithStepFailure {
                annotation: Box::new(annotation),
                overlay,
            },
            None => annotation,
        })
    }
}

impl Serialize for ErrorAnnotation {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        if let Self::WithStepFailure {
            annotation,
            overlay,
        } = self
        {
            let mut fields = serde_json::to_value(annotation).map_err(serde::ser::Error::custom)?;
            let overlay = serde_json::to_value(overlay).map_err(serde::ser::Error::custom)?;
            fields
                .as_object_mut()
                .expect("annotation is an object")
                .extend(overlay.as_object().expect("overlay is an object").clone());
            return fields.serialize(serializer);
        }
        let mut fields = serializer.serialize_map(None)?;
        match self {
            Self::WithStepFailure { .. } => unreachable!(),
            Self::WorkspaceResetRequired(value) => {
                fields.serialize_entry("blocked_at", &value.blocked_at)?;
                fields.serialize_entry("blocked_by", &value.blocked_by)?;
                fields.serialize_entry("blocking_reason", &value.blocking_reason)?;
                fields.serialize_entry("message", &value.message)?;
                fields.serialize_entry("type", "workspace_reset_required")?;
            }
            Self::ExecutorFailed(value) => {
                fields.serialize_entry("type", "executor_failed")?;
                fields.serialize_entry("blocking_reason", &value.blocking_reason)?;
                fields.serialize_entry("blocked_by", &value.blocked_by)?;
                fields.serialize_entry("blocked_at", &value.blocked_at)?;
                fields.serialize_entry("blocked_execution_id", &value.blocked_execution_id)?;
                fields.serialize_entry("artifact", &value.artifact)?;
                fields.serialize_entry("message", &value.message)?;
            }
            Self::WorkflowLoop(value)
            | Self::CascadeFailed(value)
            | Self::WorkspaceError(value)
            | Self::MergeConflict(value)
            | Self::CiFailed(value) => {
                if let Some(reason) = &value.blocking_reason {
                    fields.serialize_entry("blocking_reason", reason)?;
                }
                fields.serialize_entry("detected_at", &value.detected_at)?;
                fields.serialize_entry("message", &value.message)?;
                fields.serialize_entry("state", &value.state)?;
                fields.serialize_entry("task_step_id", &value.task_step_id)?;
                let tag = match self {
                    Self::WorkflowLoop(_) => "workflow_loop",
                    Self::CascadeFailed(_) => "cascade_failed",
                    Self::WorkspaceError(_) => "workspace_error",
                    Self::MergeConflict(_) => "merge_conflict",
                    Self::CiFailed(_) => "ci_failed",
                    _ => unreachable!(),
                };
                fields.serialize_entry("type", tag)?;
            }
        }
        fields.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceResetAnnotation {
    pub blocked_at: String,
    pub blocked_by: String,
    pub blocking_reason: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotationArtifact {
    pub kind: String,
    pub id: Option<String>,
    pub log_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutorFailureAnnotation {
    pub blocking_reason: String,
    pub blocked_by: Option<String>,
    pub blocked_at: Option<String>,
    pub blocked_execution_id: Option<String>,
    pub artifact: Option<AnnotationArtifact>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepFailureAnnotation {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocking_reason: Option<Option<String>>,
    pub detected_at: String,
    pub message: Option<String>,
    pub state: String,
    pub task_step_id: String,
}

/// The queue historically parses an annotation into Value before adding these
/// fields. Its sorted rendering is intentional, even for a struct-built input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepFailureOverlay {
    pub task_step_error: Option<String>,
    pub task_step_id: String,
}

impl ErrorAnnotation {
    pub fn with_step_failure(&self, overlay: StepFailureOverlay) -> serde_json::Result<String> {
        serde_json::to_string(&Self::WithStepFailure {
            annotation: Box::new(self.clone()),
            overlay,
        })
    }
}

/// Alphabetical fields reproduce json!-built interruption bytes. Missing kind
/// and time are meaningful (the workflow-loop writer has neither).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Interruption {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocked_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub created_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub details: Option<Option<InterruptionDetails>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub execution_id: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "strict_kind",
        skip_serializing_if = "Option::is_none"
    )]
    pub kind: Option<Option<FailureKind>>,
    pub reason: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub source: Option<Option<String>>,
}

// Outer None is absent; Some(None) retains an explicit JSON null.
fn present<'de, D, T>(decoder: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(decoder).map(Some)
}

fn strict_kind<'de, D: Deserializer<'de>>(
    decoder: D,
) -> Result<Option<Option<FailureKind>>, D::Error> {
    let kind = present(decoder)?;
    if kind == Some(Some(FailureKind::Unknown)) {
        return Err(serde::de::Error::custom("unknown interruption kind"));
    }
    Ok(kind)
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InterruptionDetails {
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub assignment_guidance: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub assignment_required: Option<Option<bool>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub attempts: Option<Option<u64>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub cancelled_dependency_ids: Option<Option<Vec<String>>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub superseded: Option<Option<SupersededCondition>>,
}

/// Saved channel bytes are opaque text: historical values must survive restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SupersededCondition {
    pub blocked_json: Option<String>,
    pub error_annotation: Option<String>,
    pub failed_json: Option<String>,
    pub status: String,
}

#[cfg(test)]
mod tests;
