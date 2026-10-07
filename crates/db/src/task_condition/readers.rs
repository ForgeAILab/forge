//! Typed read projection of the persisted condition. Legacy-shaped evidence is
//! decoded here, at the stored-data boundary; live readers never inspect Task
//! interruption columns or condition metadata keys.
use super::*;
use api_types::{FailureKind, InterruptionMetadata, TaskAnnotation, TaskBlockingAnnotation};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConditionRetry {
    pub not_before: String,
    pub reason: String,
    pub target_state: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConditionRefusal {
    pub task_version: i64,
    pub capability: String,
    pub blocker_digest: String,
    pub recorded_at: String,
    pub safe_message: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConditionRetryDisplay {
    pub reason: Option<String>,
    pub not_before: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct ConditionRead {
    pub owner: Option<api_types::ConditionOwner>,
    pub recovery: Option<api_types::ConditionRecovery>,
    pub diagnostic: Option<TaskBlockingAnnotation>,
    pub diagnostic_present: bool,
    pub interruption: Option<InterruptionMetadata>,
    pub interruption_present: bool,
    pub failure_message: Option<String>,
    pub blocked_message: Option<String>,
    pub exception_message: Option<String>,
    pub interruption_execution_id: Option<String>,
    pub hard_failure: bool,
    pub failure_kind: Option<FailureKind>,
    pub entry: Option<Value>,
    pub entry_recorded: bool,
    pub human_wait: bool,
    pub explicit_human_wait: bool,
    pub queued_command: bool,
    pub external_merge: bool,
    pub retry_recorded: bool,
    pub retry: Option<ConditionRetry>,
    pub retry_display: Option<ConditionRetryDisplay>,
    pub retry_kind: Option<String>,
    pub refusal: Option<ConditionRefusal>,
    pub environment: Option<Value>,
    pub environment_recorded: bool,
    pub placement: Option<Value>,
    pub failed_step_id: Option<String>,
    pub slot_blocker: bool,
    pub review_wait: bool,
    pub review_failure: bool,
    pub operator_reason: Option<String>,
    pub placement_matches_diagnostic: bool,
}
impl TaskCondition {
    pub fn read(&self) -> ConditionRead {
        let e = self.evidence();
        let mut read = e.presentation.clone().unwrap_or_default();
        // Typed producer conditions need no imported evidence to be readable.
        for reason in self.reasons().chain(e.observations.iter()) {
            match reason {
                // A hold stored with its own reason (an interruption, or the
                // failure it sits beside) is shown as stored: only a hold
                // that carries nothing else gets the generic diagnostic.
                ParkReason::Held { actor }
                    if !read.diagnostic_present
                        && !read.interruption_present
                        && !read.hard_failure =>
                {
                    read.failure_kind = Some(FailureKind::ManualStop);
                    read.diagnostic = Some(TaskBlockingAnnotation {
                        annotation_type: FailureKind::ManualStop,
                        blocking_reason: "manual_stop".into(),
                        blocked_by: Some(actor.clone()),
                        blocked_at: None,
                        blocked_execution_id: None,
                        artifact: None,
                        message: None,
                        hook: None,
                    });
                }
                ParkReason::Failure { failure_kind } if read.failure_kind.is_none() => {
                    read.failure_kind = Some(*failure_kind)
                }
                ParkReason::HumanDecision { .. } if e.presentation.is_none() => {
                    read.human_wait = true;
                    read.explicit_human_wait = true;
                }
                ParkReason::EntryBlocked { state, .. } if read.entry.is_none() => {
                    read.entry = Some(serde_json::json!({"state":state,"status":"blocked"}))
                }
                // A park migrated from its visible annotation keeps that
                // diagnostic (its message and `blocked_at`) as saved.
                ParkReason::WorkflowInvalid { state, cause, .. }
                    if !read.diagnostic_present && read.diagnostic.is_none() =>
                {
                    read.diagnostic = Some(park_diagnostic("workflow_invalid", &format!("Nothing can continue this Task from `{state}`: {cause}. Owner: the Project Agent. Action: edit the Project workflow or move the Task to a state it defines.")));
                    read.failure_kind = Some(FailureKind::WorkflowGuardRejected);
                }
                ParkReason::UnknownCondition {
                    source:
                        ConditionSource {
                            field: LegacyConditionField::ConditionJson,
                            ..
                        },
                    ..
                } => {
                    read.failure_kind = Some(FailureKind::Unknown);
                    read.diagnostic_present = true;
                    read.diagnostic = Some(TaskBlockingAnnotation {
                        annotation_type: FailureKind::Unknown,
                        blocking_reason: "unknown_condition".into(),
                        blocked_by: None,
                        blocked_at: None,
                        blocked_execution_id: None,
                        artifact: None,
                        message: Some(
                            "Stored Task condition is unreadable; waiting for invariant repair"
                                .into(),
                        ),
                        hook: None,
                    });
                }
                _ => {}
            }
        }
        // A failed Review is history only once the Task has moved past it.
        // A run that is merely live (a reviewer retrying) has not: the
        // failure stays the Task's exception until that run succeeds.
        if matches!(self, TaskCondition::Settled { .. }) {
            read.review_failure = false;
        }
        read.human_wait |= read.interruption_present;
        read.hard_failure |= matches!(self, TaskCondition::Failed { .. });
        read
    }
}
fn park_diagnostic(reason: &str, message: &str) -> TaskBlockingAnnotation {
    TaskBlockingAnnotation {
        annotation_type: FailureKind::WorkflowGuardRejected,
        blocking_reason: reason.into(),
        blocked_by: Some("system:task_dispatcher".into()),
        blocked_at: None,
        blocked_execution_id: None,
        artifact: None,
        message: Some(message.into()),
        hook: None,
    }
}
impl ConditionRead {
    pub fn current_refusal(&self, version: i64) -> Option<ConditionRefusal> {
        self.refusal.clone().filter(|d| {
            d.task_version == version
                || matches!(
                    d.capability.as_str(),
                    "machine_capacity" | "project_capacity"
                )
        })
    }
    pub fn message(&self) -> Option<String> {
        self.diagnostic
            .as_ref()
            .and_then(|a| {
                a.message
                    .clone()
                    .or_else(|| Some(a.blocking_reason.clone()))
            })
            .or_else(|| self.interruption.as_ref().map(|i| i.reason.clone()))
    }
}

impl TaskCondition {
    /// Omit private evidence, ownership witnesses and migrated metadata from live values.
    pub fn public(&self) -> api_types::TaskCondition {
        let read = self.read();
        // Which stored record the one public `interruption` is: the failure
        // record wins, and a blocked record beside it is still reported.
        let evidence = self.evidence();
        // A condition a writer stated carries no copy of the legacy records:
        // which record the interruption is comes from its typed presentation.
        let stated = evidence.presentation.as_ref().filter(|_| evidence.stated);
        let failed = read.interruption.is_some()
            && (evidence.failed_json.is_some() || stated.is_some_and(|p| p.hard_failure));
        let blocked = (evidence.blocked_json.is_some()
            || stated.is_some_and(|p| p.interruption_present))
            && (failed || read.interruption.is_some());
        let details = api_types::ConditionDetails {
            failed,
            blocked,
            execution_id: read.interruption_execution_id.clone().or_else(|| {
                read.diagnostic
                    .as_ref()
                    .and_then(|a| a.blocked_execution_id.clone())
            }),
            owner: read.owner,
            recovery: read.recovery,
            failure_kind: read.failure_kind,
            // An annotation stored before annotations were typed has no
            // typed reading; it stays visible as an unknown diagnostic.
            diagnostic: read.diagnostic.or_else(|| {
                read.diagnostic_present
                    .then(|| untyped_diagnostic(evidence.error_annotation.as_deref()?))
                    .flatten()
            }),
            interruption: read.interruption,
            human_wait: read.human_wait,
            entry_wait: matches!(self, TaskCondition::Entering { .. })
                || self.reasons().any(|r| {
                    matches!(
                        r,
                        ParkReason::EntryBlocked { .. }
                            | ParkReason::BudgetExhausted {
                                source: ConditionSource {
                                    field: LegacyConditionField::EntryBarrierJson,
                                    ..
                                },
                                ..
                            }
                            | ParkReason::UnknownCondition {
                                source: ConditionSource {
                                    field: LegacyConditionField::EntryBarrierJson,
                                    ..
                                },
                                ..
                            }
                    )
                }),
        };
        let mut value = serde_json::to_value(self).expect("condition serializes");
        let object = value.as_object_mut().expect("tagged condition");
        object.remove("evidence");
        object.insert(
            "details".into(),
            serde_json::to_value(details).expect("details serialize"),
        );
        fn remove_sources(v: &mut Value) {
            match v {
                Value::Object(m) => {
                    m.remove("source");
                    for v in m.values_mut() {
                        remove_sources(v);
                    }
                }
                Value::Array(a) => {
                    for v in a {
                        remove_sources(v);
                    }
                }
                _ => {}
            }
        }
        // Only reason sources are private: interruption.source remains a named computed diagnostic.
        for key in ["primary", "additional", "failure"] {
            if let Some(v) = object.get_mut(key) {
                remove_sources(v);
            }
        }
        serde_json::from_value(value).expect("private and public condition variants agree")
    }
}

/// The public reading of an annotation that is not a typed blocking
/// annotation: its own text fields where it has them, otherwise its text.
fn untyped_diagnostic(raw: &str) -> Option<TaskBlockingAnnotation> {
    let value = serde_json::from_str::<Value>(raw).ok();
    let text = |key: &str| {
        value
            .as_ref()
            .and_then(|v| v.get(key))
            .and_then(Value::as_str)
            .map(|text| {
                let mut text = text.to_owned();
                bound_text(&mut text);
                text
            })
    };
    let mut whole = raw.to_owned();
    bound_text(&mut whole);
    Some(TaskBlockingAnnotation {
        annotation_type: FailureKind::Unknown,
        blocking_reason: text("blocking_reason")
            .or_else(|| text("reason"))
            .unwrap_or_default(),
        blocked_by: text("blocked_by"),
        blocked_at: text("blocked_at"),
        blocked_execution_id: text("blocked_execution_id"),
        artifact: None,
        message: text("message").or(Some(whole)),
        hook: None,
    })
}

impl ConditionRead {
    pub(super) fn import(view: &LegacyView<'_>) -> Self {
        let fields: Fields<'_> = view
            .metadata_json
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        let meta = |key: &str| {
            fields
                .get(key)
                .and_then(|r| serde_json::from_str::<Value>(r.get()).ok())
        };
        let diagnostic = view
            .error_annotation
            .and_then(|s| serde_json::from_str::<TaskAnnotation>(s).ok())
            .and_then(|a| match a {
                TaskAnnotation::Blocking(a) => Some(a),
                _ => None,
            });
        let failed = view
            .failed_json
            .and_then(|s| serde_json::from_str::<Value>(s).ok());
        let blocked = view
            .blocked_json
            .and_then(|s| serde_json::from_str::<Value>(s).ok());
        let kind = |v: &Value| {
            v.get("kind")
                .cloned()
                .and_then(|v| serde_json::from_value::<FailureKind>(v).ok())
        };
        let failure_kind = if view.failed_json.is_some() {
            Some(
                failed
                    .as_ref()
                    .and_then(kind)
                    .unwrap_or(FailureKind::ExecutorFailed),
            )
        } else {
            blocked
                .as_ref()
                .and_then(kind)
                .filter(|k| k.is_retry_exhausted_metadata() || k.is_budget_exhausted_annotation())
                .or_else(|| {
                    diagnostic
                        .as_ref()
                        .map(|a| a.annotation_type)
                        .filter(|k| *k != FailureKind::Unknown)
                })
                .or_else(|| blocked.as_ref().and_then(kind))
                .or_else(|| view.error_annotation.map(|_| FailureKind::Unknown))
        };
        let retry_value = meta("deferred_dispatch");
        let retry = retry_value
            .clone()
            .and_then(|v| serde_json::from_value(v).ok());
        let retry_kind = retry_value
            .as_ref()
            .and_then(|v| v["kind"].as_str())
            .map(str::to_owned);
        let read = ConditionRead {
            owner: None,
            recovery: None,
            queued_command: fields.contains_key("queued_recovery"),
            diagnostic,
            diagnostic_present: view.error_annotation.is_some(),
            interruption: view
                .failed_json
                .or(view.blocked_json)
                .and_then(|s| serde_json::from_str(s).ok()),
            interruption_execution_id: failed
                .as_ref()
                .or(blocked.as_ref())
                .and_then(|v| {
                    v.get("execution_id")
                        .or_else(|| v.get("blocked_execution_id"))
                })
                .and_then(Value::as_str)
                .map(str::to_owned),
            failure_message: projected_message(view.failed_json, "Task failed"),
            blocked_message: projected_message(view.blocked_json, "Task is blocked"),
            exception_message: projected_message(
                view.failed_json.or(view.blocked_json),
                "Task interrupted",
            ),
            interruption_present: view.blocked_json.is_some()
                || view.non_text.contains(&LegacyConditionField::BlockedJson),
            hard_failure: view.failed_json.is_some()
                || view.non_text.contains(&LegacyConditionField::FailedJson),
            failure_kind,
            entry_recorded: view.entry_barrier_json.is_some()
                || view
                    .non_text
                    .contains(&LegacyConditionField::EntryBarrierJson),
            entry: view
                .entry_barrier_json
                .and_then(|s| serde_json::from_str(s).ok()),
            human_wait: meta("awaiting_human") == Some(Value::Bool(true)),
            explicit_human_wait: meta("awaiting_human") == Some(Value::Bool(true)),
            external_merge: meta("awaiting_human_reason")
                == Some(Value::String("pull_request_merge".into())),
            retry_recorded: fields.contains_key("deferred_dispatch"),
            retry,
            retry_display: retry_value.as_ref().filter(|v| v.is_object()).map(|v| {
                ConditionRetryDisplay {
                    reason: v["reason"].as_str().map(str::to_owned),
                    not_before: v["not_before"].as_str().map(str::to_owned),
                }
            }),
            retry_kind,
            refusal: meta("dispatch_disposition").and_then(|v| serde_json::from_value(v).ok()),
            environment: meta("environment_wait"),
            environment_recorded: fields.contains_key("environment_wait"),
            placement: meta("placement_refusal"),
            slot_blocker: view
                .error_annotation
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .and_then(|v| v["type"].as_str().map(str::to_owned))
                .is_some_and(|kind| LEGACY_BLOCKING_ANNOTATION_KINDS.contains(&kind.as_str())),
            review_wait: false,
            review_failure: false,
            placement_matches_diagnostic: meta("placement_refusal").is_some_and(|p| {
                p["annotation"]
                    == view
                        .error_annotation
                        .and_then(|s| serde_json::from_str::<Value>(s).ok())
                        .unwrap_or_default()
            }),
            operator_reason: None,
            failed_step_id: view
                .error_annotation
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
                .and_then(|v| v["task_step_id"].as_str().map(str::to_owned)),
        };
        // Bound presentation independently of raw import evidence. Full user data
        // remains in the legacy columns until the storage-removal stage.
        let mut value = serde_json::to_value(read).expect("reader projection serializes");
        bound(&mut value);
        let mut read: ConditionRead =
            serde_json::from_value(value).expect("bounded projection retains its types");
        read.operator_reason = view
            .error_annotation
            .map(bound_operator_reason)
            .or_else(|| {
                blocked
                    .as_ref()
                    .and_then(|v| v["reason"].as_str())
                    .map(|reason| {
                        let mut reason = reason.to_owned();
                        bound_text(&mut reason);
                        reason
                    })
            });
        read
    }
}

/// Translate the existing durable scheduler record into the two visible owner parks.
/// HumanWork (unassigned or person-held roles) deliberately has no visible park.
pub(crate) fn owner_park(raw: &Value) -> Option<(ParkReason, TaskBlockingAnnotation)> {
    let r = &raw["reason"];
    let source = ConditionSource {
        field: LegacyConditionField::SchedulePark,
        key: None,
    };
    if let Some(v) = r.get("WorkflowInvalid") {
        let state = v["state"].as_str()?.to_owned();
        let cause = v["cause"].as_str()?.to_owned();
        let message = format!("Nothing can continue this Task from `{state}`: {cause}. Owner: the Project Agent. Action: edit the Project workflow or move the Task to a state it defines.");
        return Some((
            ParkReason::WorkflowInvalid {
                state,
                cause,
                definition_digest: String::new(),
            },
            park_diagnostic("workflow_invalid", &message),
        ));
    }
    let owner = r.get("UnknownCondition")?["owner"].as_str()?;
    if !matches!(owner, "plan publication cleanup" | "entry hooks") {
        return None;
    }
    let message = format!("Nothing owns this Task: no recorded owner for {owner}. Owner: the Project owner. Action: move the Task back to the previous state and forward again, or cancel it.");
    Some((
        ParkReason::UnknownCondition {
            source,
            problem: UnknownConditionProblem::UnownedEntry,
        },
        park_diagnostic("unknown_condition", &message),
    ))
}

pub(super) fn apply_owner_park(mut condition: TaskCondition, raw: Option<&Value>) -> TaskCondition {
    let Some((reason, mut diagnostic)) = raw.and_then(owner_park) else {
        return condition;
    };
    if let Some(saved) = raw
        .and_then(|p| p.get("diagnostic"))
        .and_then(|a| serde_json::from_value::<TaskBlockingAnnotation>(a.clone()).ok())
    {
        diagnostic = saved;
    }
    // A real interruption has priority over the observer's ownerless diagnosis.
    if condition
        .evidence()
        .presentation
        .as_ref()
        .is_some_and(|p| p.diagnostic_present || p.interruption_present || p.hard_failure)
    {
        return condition;
    }
    let e = condition.evidence_mut();
    let read = e.presentation.get_or_insert_with(Default::default);
    read.owner = Some(if matches!(reason, ParkReason::WorkflowInvalid { .. }) {
        api_types::ConditionOwner::ProjectAgent
    } else {
        api_types::ConditionOwner::Workflow
    });
    read.recovery = Some(if matches!(reason, ParkReason::WorkflowInvalid { .. }) {
        api_types::ConditionRecovery::EditWorkflow
    } else {
        api_types::ConditionRecovery::ReconcileEntry
    });
    read.failure_kind = Some(FailureKind::WorkflowGuardRejected);
    read.diagnostic = Some(diagnostic.clone());
    read.operator_reason = Some(serde_json::to_string(&diagnostic).expect("diagnostic serializes"));
    e.material = Some(MaterialBlocker {
        requires_intervention: true,
        interruption: Some(
            serde_json::json!({"source":"annotation", "kind":"workflow_guard_rejected", "reason":diagnostic.blocking_reason}),
        ),
    });
    TaskCondition::Parked {
        primary: reason,
        additional: Vec::new(),
        resume: ConditionContinuation::Reconcile,
        since: None,
        evidence: std::mem::take(e),
    }
}

/// The most bytes of any one text a condition presents, marker included.
pub const PRESENTATION_TEXT_LIMIT: usize = 1024;
/// Ends every presented text that was cut to [`PRESENTATION_TEXT_LIMIT`].
pub const PRESENTATION_TRUNCATION_MARKER: &str = "… [truncated]";

fn bound_text(text: &mut String) {
    if text.len() <= PRESENTATION_TEXT_LIMIT {
        return;
    }
    let mut end = PRESENTATION_TEXT_LIMIT - PRESENTATION_TRUNCATION_MARKER.len();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str(PRESENTATION_TRUNCATION_MARKER);
}

/// The operator's copy of the stored annotation stays what it was: valid
/// JSON when the annotation is JSON, with each text inside it bounded.
pub(super) fn bound_operator_reason(raw: &str) -> String {
    const WHOLE_LIMIT: usize = 16 * PRESENTATION_TEXT_LIMIT;
    if raw.len() <= PRESENTATION_TEXT_LIMIT {
        // The stored text, byte for byte.
        return raw.to_owned();
    }
    let Ok(mut value) = serde_json::from_str::<Value>(raw) else {
        let mut text = raw.to_owned();
        bound_text(&mut text);
        return text;
    };
    bound(&mut value);
    let encoded = value.to_string();
    if encoded.len() <= WHOLE_LIMIT {
        return encoded;
    }
    // Too many fields to present: one bounded JSON string, still valid JSON.
    let mut text = encoded;
    bound_text(&mut text);
    Value::String(text).to_string()
}

/// A presentation with every text in it cut to [`PRESENTATION_TEXT_LIMIT`].
pub(super) fn bounded_read(read: ConditionRead) -> ConditionRead {
    let mut value = serde_json::to_value(read).expect("reader projection serializes");
    bound(&mut value);
    serde_json::from_value(value).expect("bounded projection retains its types")
}

fn bound(value: &mut Value) {
    match value {
        Value::String(text) => bound_text(text),
        Value::Object(fields) => {
            for value in fields.values_mut() {
                bound(value);
            }
        }
        Value::Array(items) => {
            items.truncate(16);
            for value in items {
                bound(value);
            }
        }
        _ => {}
    }
}

fn projected_message(raw: Option<&str>, fallback: &str) -> Option<String> {
    let value: Value = serde_json::from_str(raw?).ok()?;
    Some(
        value
            .get("reason")
            .or_else(|| value.get("message"))
            .or_else(|| value.get("kind"))
            .and_then(Value::as_str)
            .unwrap_or(fallback)
            .to_owned(),
    )
}
