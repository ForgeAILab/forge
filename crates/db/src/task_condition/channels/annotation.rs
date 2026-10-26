use super::*;
use serde::de::{MapAccess, Visitor};
use serde_json::{Map, Value};
use std::fmt;

/// Shapes sharing a failure kind remain distinct. The optional overlay is a
/// single replacement, including when the queue annotates a step twice.
#[derive(Debug, Clone)]
pub struct ErrorAnnotation {
    pub shape: AnnotationShape,
    step_failure: Option<StepFailureOverlay>,
    order: AnnotationOrder,
}

impl PartialEq for ErrorAnnotation {
    fn eq(&self, other: &Self) -> bool {
        self.shape == other.shape && self.step_failure == other.step_failure
    }
}
impl Eq for ErrorAnnotation {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AnnotationOrder {
    Declaration,
    Sorted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnnotationShape {
    Blocking(BlockingAnnotation),
    Hold(HoldAnnotation),
    TimedBlock(TimedBlockAnnotation),
    RecoveryExecution(RecoveryExecutionAnnotation),
    RemoteCancel(RemoteCancelAnnotation),
    Message(MessageAnnotation),
    Reason(ReasonAnnotation),
    ReviewCandidate(ReviewCandidateAnnotation),
    ReviewPark(ReviewParkAnnotation),
    Detected(DetectedAnnotation),
    DetectedState(DetectedStateAnnotation),
    Budget(BudgetAnnotation),
    StepFailure(StepFailureAnnotation),
    PlacementRefusal(PlacementRefusalAnnotation),
    PlanWait(PlanWaitAnnotation),
    UpgradeDispatch(UpgradeDispatchAnnotation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldAnnotation {
    pub blocked_at: String,
    pub blocked_by: String,
    #[serde(deserialize_with = "super::nullable")]
    pub blocked_execution_id: Option<String>,
    pub blocking_reason: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlockingAnnotation {
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
    pub blocking_reason: String,
    #[serde(deserialize_with = "super::nullable")]
    pub blocked_by: Option<String>,
    #[serde(deserialize_with = "super::nullable")]
    pub blocked_at: Option<String>,
    #[serde(deserialize_with = "super::nullable")]
    pub blocked_execution_id: Option<String>,
    #[serde(deserialize_with = "super::nullable")]
    pub artifact: Option<AnnotationArtifact>,
    #[serde(deserialize_with = "super::nullable")]
    pub message: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub hook: Option<AnnotationHook>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimedBlockAnnotation {
    pub blocked_at: String,
    pub blocked_by: String,
    pub blocking_reason: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryExecutionAnnotation {
    pub blocked_at: String,
    pub blocked_by: String,
    #[serde(deserialize_with = "super::nullable")]
    pub blocked_execution_id: Option<String>,
    pub blocking_reason: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteCancelAnnotation {
    pub blocked_by: String,
    pub blocking_reason: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageAnnotation {
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasonAnnotation {
    pub blocking_reason: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewCandidateAnnotation {
    pub blocked_at: String,
    pub blocking_reason: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewParkAnnotation {
    pub blocked_at: String,
    #[serde(deserialize_with = "super::nullable")]
    pub blocked_execution_id: Option<String>,
    pub blocking_reason: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectedAnnotation {
    pub detected_at: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectedStateAnnotation {
    pub detected_at: String,
    pub message: String,
    pub state: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BudgetAnnotation {
    pub blocking_reason: String,
    pub detected_at: String,
    pub message: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepFailureAnnotation {
    #[serde(
        default,
        deserialize_with = "super::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocking_reason: Option<Option<String>>,
    pub detected_at: String,
    #[serde(deserialize_with = "super::nullable")]
    pub message: Option<String>,
    pub state: String,
    pub task_step_id: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementRefusalAnnotation {
    pub code: PlacementCode,
    pub machines: Vec<RefusedMachine>,
    pub message: String,
    pub missing_capabilities: Vec<String>,
    pub rejected_candidates: Vec<RejectedCandidate>,
    pub state: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanWaitAnnotation {
    pub code: PlanWaitCode,
    pub daemon_id: String,
    pub execution_id: String,
    pub message: String,
    pub retry_at: String,
    #[serde(rename = "type")]
    pub kind: PlanWaitKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpgradeDispatchAnnotation {
    pub code: UpgradeCode,
    pub daemon_ids: Vec<String>,
    pub detected_at: String,
    pub message: String,
    pub state: String,
    #[serde(rename = "type", deserialize_with = "super::failure_kind")]
    pub kind: FailureKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotationArtifact {
    pub kind: String,
    #[serde(deserialize_with = "super::nullable")]
    pub id: Option<String>,
    #[serde(deserialize_with = "super::nullable")]
    pub log_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StepFailureOverlay {
    #[serde(deserialize_with = "super::nullable")]
    pub task_step_error: Option<String>,
    pub task_step_id: String,
}

impl ErrorAnnotation {
    pub fn new(shape: AnnotationShape) -> Self {
        let order = if matches!(
            shape,
            AnnotationShape::Blocking(_) | AnnotationShape::Message(_)
        ) {
            AnnotationOrder::Declaration
        } else {
            AnnotationOrder::Sorted
        };
        Self {
            shape,
            step_failure: None,
            order,
        }
    }

    /// json!-built blocking annotations sort both the object and its artifact.
    pub fn sorted(shape: AnnotationShape) -> Self {
        Self {
            order: AnnotationOrder::Sorted,
            ..Self::new(shape)
        }
    }

    pub fn with_step_failure(&self, overlay: StepFailureOverlay) -> serde_json::Result<String> {
        let mut value = self.clone();
        value.set_step_failure(overlay);
        serde_json::to_string(&value)
    }

    pub fn step_failure(&self) -> Option<&StepFailureOverlay> {
        self.step_failure.as_ref()
    }

    pub fn set_step_failure(&mut self, overlay: StepFailureOverlay) {
        match &mut self.shape {
            AnnotationShape::StepFailure(step) => {
                step.task_step_id.clone_from(&overlay.task_step_id);
            }
            AnnotationShape::DetectedState(detected) => {
                // Adding the step ID makes this indistinguishable from a native
                // step payload. Use the same field-set normal form as decode.
                self.shape = AnnotationShape::StepFailure(StepFailureAnnotation {
                    blocking_reason: None,
                    detected_at: detected.detected_at.clone(),
                    message: Some(detected.message.clone()),
                    state: detected.state.clone(),
                    task_step_id: overlay.task_step_id.clone(),
                    kind: detected.kind,
                });
            }
            _ => {}
        }
        self.step_failure = Some(overlay);
        self.order = AnnotationOrder::Sorted;
    }
}

impl Serialize for ErrorAnnotation {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.order == AnnotationOrder::Declaration && self.step_failure.is_none() {
            return self.shape.serialize(serializer);
        }
        let mut value = serde_json::to_value(&self.shape).map_err(serde::ser::Error::custom)?;
        if let Some(overlay) = &self.step_failure {
            let fields = value
                .as_object_mut()
                .expect("annotation shape is an object");
            fields.insert("task_step_id".into(), overlay.task_step_id.clone().into());
            fields.insert(
                "task_step_error".into(),
                serde_json::to_value(&overlay.task_step_error)
                    .map_err(serde::ser::Error::custom)?,
            );
        }
        value.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ErrorAnnotation {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct AnnotationVisitor;
        impl<'de> Visitor<'de> for AnnotationVisitor {
            type Value = ErrorAnnotation;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a condition annotation object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut fields = Map::new();
                let mut first = None;
                while let Some((key, value)) = map.next_entry::<String, Value>()? {
                    if first.is_none() {
                        first = Some(key.clone());
                    }
                    if fields.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate annotation field"));
                    }
                }
                let step_failure = if let Some(error) = fields.remove("task_step_error") {
                    let id = fields
                        .get("task_step_id")
                        .cloned()
                        .ok_or_else(|| serde::de::Error::missing_field("task_step_id"))?;
                    let overlay = StepFailureOverlay {
                        task_step_error: serde_json::from_value(error)
                            .map_err(serde::de::Error::custom)?,
                        task_step_id: serde_json::from_value(id)
                            .map_err(serde::de::Error::custom)?,
                    };
                    // Native step shapes have a state and a detection time.
                    if !(fields.contains_key("state")
                        && fields.contains_key("detected_at")
                        && !fields.contains_key("code"))
                    {
                        fields.remove("task_step_id");
                    }
                    Some(overlay)
                } else {
                    None
                };
                let shape = serde_json::from_value(Value::Object(fields))
                    .map_err(serde::de::Error::custom)?;
                let order = if first.as_deref() == Some("type") {
                    if step_failure.is_some()
                        || !matches!(
                            shape,
                            AnnotationShape::Blocking(_) | AnnotationShape::Message(_)
                        )
                    {
                        return Err(serde::de::Error::custom(
                            "type-first encoding requires a blocking struct or message literal",
                        ));
                    }
                    AnnotationOrder::Declaration
                } else {
                    AnnotationOrder::Sorted
                };
                Ok(ErrorAnnotation {
                    shape,
                    step_failure,
                    order,
                })
            }
        }
        decoder.deserialize_map(AnnotationVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnnotationHook {
    Dependency(DependencyHook),
    Script(Box<ScriptHook>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyHook {
    pub cancelled_dependency_ids: Vec<String>,
    pub name: DependencyHookName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptHook {
    pub command: String,
    pub duration_ms: u64,
    pub environment: std::collections::BTreeMap<String, String>,
    pub event: ScriptHookEvent,
    #[serde(deserialize_with = "super::nullable")]
    pub exit_code: Option<i32>,
    pub index: usize,
    #[serde(deserialize_with = "super::nullable")]
    pub log_path: Option<String>,
    pub stderr: String,
    pub stdout: String,
    pub timeout: bool,
    #[serde(rename = "type")]
    pub hook_type: ScriptHookType,
    pub working_dir: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyHookName {
    DependencyGate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptHookEvent {
    BeforeWork,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScriptHookType {
    Script,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementCode {
    PlacementUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanWaitCode {
    RuntimeOffline,
    PlanOwnerError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanWaitKind {
    PlanSettlementWait,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeCode {
    DaemonUpgradeRequired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefusedMachine {
    pub daemon_id: String,
    pub filter_codes: Vec<api_types::PlacementFilterCode>,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_error: Option<String>,
    pub machine: String,
    #[serde(deserialize_with = "super::nullable")]
    pub runtime_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectedCandidate {
    #[serde(deserialize_with = "super::nullable")]
    pub daemon_id: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub failing_checks: Option<Vec<String>>,
    pub filter_codes: Vec<api_types::PlacementFilterCode>,
    pub owner_kind: String,
    pub repo_location_id: String,
    #[serde(deserialize_with = "super::nullable")]
    pub runtime_id: Option<String>,
}
