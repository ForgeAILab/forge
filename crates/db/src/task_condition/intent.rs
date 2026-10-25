//! Owner edits carry typed causes and explicit witness identity. These are
//! producer inputs, not a decoder for legacy annotations or metadata.
use super::*;
use api_types::{FailureKind, InterruptionMetadata, TaskBlockingAnnotation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionReasonOwner {
    Hold,
    Diagnostic,
    Budget { kind: crate::budget::Kind },
    Entry,
    Human,
    Retry,
    Dispatch,
    Placement,
    Environment,
    OwnerConnection,
    PlanTransport,
    Dependencies,
    Coordination,
    ProjectPause,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "witness", rename_all = "snake_case")]
pub enum ConditionIdentity {
    Budget {
        epoch: i64,
        kind: crate::budget::Kind,
        window_id: String,
    },
    Entry {
        epoch: i64,
        transition_id: Option<String>,
    },
    Execution {
        epoch: i64,
        execution_id: String,
    },
    Step {
        epoch: i64,
        step_id: String,
    },
    Review {
        epoch: i64,
        review_id: String,
    },
    Marker {
        epoch: i64,
        marker_id: String,
    },
    Operation {
        epoch: i64,
        operation_id: String,
        generation: i64,
    },
    Machine {
        epoch: i64,
        daemon_id: String,
    },
}
impl ConditionIdentity {
    pub fn epoch(&self) -> i64 {
        match self {
            Self::Budget { epoch, .. }
            | Self::Entry { epoch, .. }
            | Self::Execution { epoch, .. }
            | Self::Step { epoch, .. }
            | Self::Review { epoch, .. }
            | Self::Marker { epoch, .. }
            | Self::Operation { epoch, .. }
            | Self::Machine { epoch, .. } => *epoch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionOwnership {
    pub owner: ConditionReasonOwner,
    pub identity: ConditionIdentity,
    pub reason: Option<ParkReason>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<StatedCondition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub material: Option<MaterialBlocker>,
}

/// All presentation fields have known types. Text is bounded only at the seam,
/// after its full source has been recorded in the diagnostic store.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionPresentation {
    pub diagnostic: Option<ConditionDiagnostic>,
    pub interruption: Option<ConditionInterruption>,
    pub failure_message: Option<String>,
    pub retry: Option<ConditionRetry>,
    pub refusal: Option<ConditionRefusal>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionDiagnostic {
    pub kind: FailureKind,
    pub blocking_reason: String,
    /// The writer's human-facing JSON rendering omits absent optional fields.
    /// This is a typed renderer choice, never stored raw annotation input.
    pub minimal_operator: bool,
    pub message: Option<String>,
    pub actor: Option<String>,
    pub at: Option<String>,
    pub execution_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionInterruption {
    pub kind: FailureKind,
    pub reason: String,
    pub at: String,
    pub source: Option<String>,
    pub execution_id: Option<String>,
}
impl ConditionPresentation {
    pub(super) fn from_read(read: &ConditionRead) -> Self {
        Self {
            diagnostic: read.diagnostic.as_ref().map(|d| ConditionDiagnostic {
                kind: d.annotation_type,
                blocking_reason: d.blocking_reason.clone(),
                minimal_operator: true,
                message: d.message.clone(),
                actor: d.blocked_by.clone(),
                at: d.blocked_at.clone(),
                execution_id: d.blocked_execution_id.clone(),
            }),
            interruption: read.interruption.as_ref().map(|i| ConditionInterruption {
                kind: i.kind.unwrap_or(FailureKind::Unknown),
                reason: i.reason.clone(),
                at: i.created_at.clone(),
                source: i.source.clone(),
                execution_id: i.execution_id.clone(),
            }),
            failure_message: read.failure_message.clone(),
            retry: read.retry.clone(),
            refusal: read.refusal.clone(),
        }
    }
    pub(super) fn read(&self) -> ConditionRead {
        let diagnostic = self.diagnostic.as_ref().map(|d| TaskBlockingAnnotation {
            annotation_type: d.kind,
            blocking_reason: d.blocking_reason.clone(),
            blocked_by: d.actor.clone(),
            blocked_at: d.at.clone(),
            blocked_execution_id: d.execution_id.clone(),
            message: d.message.clone(),
            artifact: None,
            hook: None,
        });
        let interruption = self.interruption.as_ref().map(|i| InterruptionMetadata {
            reason: i.reason.clone(),
            created_at: i.at.clone(),
            kind: Some(i.kind),
            source: i.source.clone(),
            execution_id: i.execution_id.clone(),
            details: None,
        });
        let operator_reason = diagnostic.as_ref().map(|d| {
            if d.annotation_type == FailureKind::ManualStop {
                if let (Some(actor), Some(at)) = (&d.blocked_by, &d.blocked_at) {
                    return ConditionStatement::hold_operator_text(actor, &d.blocking_reason, at);
                }
            }
            let optional = |value: &Option<String>| {
                value.as_ref().map_or(serde_json::Value::Null, |value| {
                    serde_json::Value::String(value.clone())
                })
            };
            let mut fields = serde_json::Map::from_iter([
                (
                    "type".into(),
                    serde_json::Value::String(d.annotation_type.to_string()),
                ),
                (
                    "blocking_reason".into(),
                    serde_json::Value::String(d.blocking_reason.clone()),
                ),
                ("blocked_by".into(), optional(&d.blocked_by)),
                ("blocked_at".into(), optional(&d.blocked_at)),
                (
                    "blocked_execution_id".into(),
                    optional(&d.blocked_execution_id),
                ),
                ("artifact".into(), serde_json::Value::Null),
                ("message".into(), optional(&d.message)),
            ]);
            if self.diagnostic.as_ref().is_some_and(|d| d.minimal_operator) {
                fields.retain(|_, value| !value.is_null());
            }
            serde_json::Value::Object(fields).to_string()
        });
        let failure_kind = self
            .interruption
            .as_ref()
            .map(|i| i.kind)
            .or_else(|| self.diagnostic.as_ref().map(|d| d.kind));
        let blocked_message = self
            .interruption
            .as_ref()
            .filter(|_| self.failure_message.is_none())
            .map(|i| i.reason.clone());
        ConditionRead {
            diagnostic_present: diagnostic.is_some(),
            interruption_present: interruption.is_some() && self.failure_message.is_none(),
            interruption_execution_id: interruption.as_ref().and_then(|i| i.execution_id.clone()),
            slot_blocker: self.diagnostic.as_ref().is_some_and(|d| {
                LEGACY_BLOCKING_ANNOTATION_KINDS.contains(&d.kind.to_string().as_str())
            }),
            exception_message: self
                .failure_message
                .clone()
                .or_else(|| blocked_message.clone()),
            hard_failure: self.failure_message.is_some(),
            failure_message: self.failure_message.clone(),
            failure_kind,
            diagnostic,
            interruption,
            blocked_message,
            operator_reason,
            retry: self.retry.clone(),
            retry_recorded: self.retry.is_some(),
            refusal: self.refusal.clone(),
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StatedCondition {
    Parked {
        reason: ParkReason,
        resume: ConditionContinuation,
        since: Option<String>,
        presentation: ConditionPresentation,
    },
    Failed {
        reason: ParkReason,
        resume: ConditionContinuation,
        since: Option<String>,
        presentation: ConditionPresentation,
    },
    Deferred {
        reason: RetryCause,
        until: Option<String>,
        resume: ConditionContinuation,
        presentation: ConditionPresentation,
    },
}
impl StatedCondition {
    pub(super) fn reason(&self) -> Option<&ParkReason> {
        match self {
            Self::Parked { reason, .. } | Self::Failed { reason, .. } => Some(reason),
            _ => None,
        }
    }
    pub(super) fn presentation(&self) -> &ConditionPresentation {
        match self {
            Self::Parked { presentation, .. }
            | Self::Failed { presentation, .. }
            | Self::Deferred { presentation, .. } => presentation,
        }
    }
}

/// A clear affects just this owner. `expected` fences a marker/attempt/epoch;
/// the enclosing writer must retain its own CAS, assignment and step fences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionOwnerEdit {
    pub owner: ConditionReasonOwner,
    pub expected: Option<ConditionIdentity>,
    pub replacement: Option<ConditionReplacement>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionReplacement {
    pub identity: ConditionIdentity,
    pub condition: StatedCondition,
}

/// Typed rollback data for a durable command/checkpoint, outside condition
/// evidence. The capture removes every legacy debug copy before serialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedTaskCondition {
    condition: TaskCondition,
}
impl SavedTaskCondition {
    pub fn capture(condition: &TaskCondition) -> Self {
        let mut typed = condition.typed();
        typed.evidence_mut().ownership = condition.evidence().ownership.clone();
        typed.evidence_mut().diagnostic_ids = condition.evidence().diagnostic_ids.clone();
        typed.evidence_mut().overflow = condition.evidence().overflow.clone();
        Self { condition: typed }
    }
    pub fn condition(&self) -> &TaskCondition {
        &self.condition
    }
}

pub(super) fn owner_of(reason: &ParkReason) -> Option<ConditionReasonOwner> {
    use ConditionReasonOwner as O;
    Some(match reason {
        ParkReason::Held { .. } => O::Hold,
        ParkReason::Failure { .. } | ParkReason::AgentTimeout { .. } => O::Diagnostic,
        ParkReason::BudgetExhausted {
            budget_kind: Some(kind),
            ..
        } => O::Budget { kind: *kind },
        ParkReason::BudgetExhausted { source, .. }
            if source.field != LegacyConditionField::EntryBarrierJson =>
        {
            O::Diagnostic
        }
        ParkReason::EntryBlocked { .. } | ParkReason::BudgetExhausted { .. } => O::Entry,
        ParkReason::HumanDecision { .. } => O::Human,
        ParkReason::DispatchRefusal { .. }
        | ParkReason::Capacity { .. }
        | ParkReason::DaemonUpgradeRequired { .. } => O::Dispatch,
        ParkReason::PlacementDenied { .. } => O::Placement,
        ParkReason::Environment { .. } => O::Environment,
        ParkReason::OwnerOffline { .. } => O::OwnerConnection,
        ParkReason::PlanSettlementWait { .. } => O::PlanTransport,
        ParkReason::Dependencies { .. } => O::Dependencies,
        ParkReason::Children { .. } => O::Coordination,
        ParkReason::ProjectPaused { state: Some(_) } => O::ProjectPause,
        _ => return None,
    })
}

impl ConditionOwnerEdit {
    pub(super) fn apply_at_epoch(
        &self,
        stored: &TaskCondition,
        epoch: Option<i64>,
    ) -> Result<TaskCondition> {
        if let Some(replacement) = &self.replacement {
            if replacement
                .condition
                .reason()
                .is_some_and(|r| owner_of(r) != Some(self.owner))
            {
                return Err(DbError::Check(
                    "statement cannot replace another owner's reason".into(),
                ));
            }
        }
        let ownership = stored
            .evidence()
            .ownership
            .iter()
            .find(|o| o.owner == self.owner);
        if self
            .expected
            .as_ref()
            .is_some_and(|expected| ownership.is_none_or(|o| &o.identity != expected))
        {
            return Err(DbError::VersionConflict);
        }
        Ok(owner_projection::apply(self, stored, epoch))
    }
}
