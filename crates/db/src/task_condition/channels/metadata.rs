use super::*;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeferredDispatch {
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub kind: Option<EnvironmentWaitKind>,
    pub not_before: String,
    pub reason: String,
    pub target_state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatchDisposition {
    pub blocker_digest: String,
    pub capability: String,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub capacity_scope: Option<String>,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub dependency_ids: Option<Vec<String>>,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub project_wait: Option<super::super::ProjectWait>,
    pub recorded_at: String,
    pub safe_message: String,
    pub task_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentWait {
    pub checks: Vec<String>,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub kind: Option<EnvironmentWaitKind>,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_error: Option<String>,
    pub machine: ConditionMachine,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerWait {
    pub daemon_id: String,
    #[serde(
        default,
        deserialize_with = "super::optional",
        skip_serializing_if = "Option::is_none"
    )]
    pub plan_execution_id: Option<String>,
    pub started_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PausedIntegration {
    pub deferred_at: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlacementRefusal {
    pub annotation: super::PlacementRefusalAnnotation,
    pub eligibility_key: String,
    pub repo_id: String,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanSettlementWait {
    pub daemon_id: String,
    pub execution_id: String,
    pub message: String,
    pub operation: String,
    pub retry_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonUpgradeRefusal {
    pub code: super::UpgradeCode,
    pub daemon_ids: Vec<String>,
    #[serde(deserialize_with = "super::nullable")]
    pub deferred_dispatch: Option<BoundedJson>,
    #[serde(deserialize_with = "super::nullable")]
    pub dispatch_disposition: Option<BoundedJson>,
    pub error_annotation: BoundedJson,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntryBarrier {
    #[serde(
        default,
        deserialize_with = "super::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub blocking_reason: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "super::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub interrupted_at: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "super::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub started_at: Option<Option<String>>,
    pub state: String,
    pub status: BarrierStatus,
    #[serde(
        default,
        deserialize_with = "super::present",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConditionMachine {
    Server,
    Daemon {
        daemon_id: Option<String>,
        runtime_id: Option<String>,
    },
}

impl<'de> Deserialize<'de> for ConditionMachine {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            owner_kind: String,
            #[serde(default, deserialize_with = "super::present")]
            daemon_id: Option<Option<String>>,
            #[serde(default, deserialize_with = "super::present")]
            runtime_id: Option<Option<String>>,
        }
        let fields = Fields::deserialize(decoder)?;
        match fields.owner_kind.as_str() {
            "server" if fields.daemon_id.is_none() && fields.runtime_id.is_none() => {
                Ok(Self::Server)
            }
            "server" => Err(serde::de::Error::custom("server machine has daemon fields")),
            "daemon" => Ok(Self::Daemon {
                daemon_id: fields
                    .daemon_id
                    .ok_or_else(|| serde::de::Error::missing_field("daemon_id"))?,
                runtime_id: fields
                    .runtime_id
                    .ok_or_else(|| serde::de::Error::missing_field("runtime_id"))?,
            }),
            kind => Err(serde::de::Error::unknown_variant(
                kind,
                &["server", "daemon"],
            )),
        }
    }
}
impl Serialize for ConditionMachine {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut fields = serializer.serialize_map(None)?;
        match self {
            Self::Server => fields.serialize_entry("owner_kind", "server")?,
            Self::Daemon {
                daemon_id,
                runtime_id,
            } => {
                fields.serialize_entry("daemon_id", daemon_id)?;
                fields.serialize_entry("owner_kind", "daemon")?;
                fields.serialize_entry("runtime_id", runtime_id)?;
            }
        }
        fields.end()
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentWaitKind {
    EnvironmentProbePending,
    EnvironmentUnverified,
    EnvironmentNotReady,
    ProvisionFailed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BarrierStatus {
    Blocked,
}

/// Snapshot fields copy caller-supplied/stored JSON, rather than defining its
/// schema. Bound only this explicit escape hatch, never historical readers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct BoundedJson(serde_json::Value);

impl BoundedJson {
    pub const MAX_BYTES: usize = 32 * 1024;
    pub const MAX_DEPTH: usize = 32;
    pub fn new(value: serde_json::Value) -> serde_json::Result<Self> {
        fn within_depth(value: &serde_json::Value, remaining: usize) -> bool {
            match value {
                serde_json::Value::Array(values) => {
                    remaining > 0
                        && values
                            .iter()
                            .all(|value| within_depth(value, remaining - 1))
                }
                serde_json::Value::Object(values) => {
                    remaining > 0
                        && values
                            .values()
                            .all(|value| within_depth(value, remaining - 1))
                }
                _ => true,
            }
        }
        if !within_depth(&value, Self::MAX_DEPTH)
            || serde_json::to_vec(&value)?.len() > Self::MAX_BYTES
        {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "opaque condition JSON exceeds bounds",
            ));
        }
        Ok(Self(value))
    }
    pub fn value(&self) -> &serde_json::Value {
        &self.0
    }
}
impl<'de> Deserialize<'de> for BoundedJson {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        Self::new(serde_json::Value::deserialize(decoder)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct BoundedConditionObject(BoundedJson);

impl BoundedConditionObject {
    pub fn new(value: serde_json::Value) -> serde_json::Result<Self> {
        if !value.is_object() {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "carried condition must be an object",
            ));
        }
        BoundedJson::new(value).map(Self)
    }
}

impl<'de> Deserialize<'de> for BoundedConditionObject {
    fn deserialize<D: Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        Self::new(serde_json::Value::deserialize(decoder)?).map_err(serde::de::Error::custom)
    }
}

/// A value for one of the 24 condition metadata keys, without an extra wire tag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum ConditionMetadataValue {
    AwaitingHuman(bool),
    AwaitingHumanMarkerId(String),
    AwaitingHumanReason(String),
    CoordinationReviewPending(bool),
    CoordinationReviewPendingId(String),
    DaemonUpgradeRefusal(DaemonUpgradeRefusal),
    DeferredDispatch(DeferredDispatch),
    DispatchDisposition(DispatchDisposition),
    EnvironmentWait(EnvironmentWait),
    ExecutorUnavailableExecutionId(String),
    LastExecutionFailureAt(String),
    LastExecutionFailureExecutionId(String),
    LastWorkflowGuardExecutionId(String),
    LastWorkflowGuardName(String),
    LastWorkflowGuardReason(String),
    LastWorkflowGuardRejectionAt(String),
    OwnerWait(OwnerWait),
    PausedIntegration(PausedIntegration),
    PausedIntegrationGeneration(i64),
    PlacementRefusal(PlacementRefusal),
    PlanSettlementWait(PlanSettlementWait),
    PlanningCompletedAt(String),
    PlanningExecutionId(String),
    PlanningStateEntryToken(Option<String>),
}
impl ConditionMetadataValue {
    pub const KEYS: [&'static str; 24] = [
        "awaiting_human",
        "awaiting_human_marker_id",
        "awaiting_human_reason",
        "coordination_review_pending",
        "coordination_review_pending_id",
        "daemon_upgrade_refusal",
        "deferred_dispatch",
        "dispatch_disposition",
        "environment_wait",
        "executor_unavailable_execution_id",
        "last_execution_failure_at",
        "last_execution_failure_execution_id",
        "last_workflow_guard_execution_id",
        "last_workflow_guard_name",
        "last_workflow_guard_reason",
        "last_workflow_guard_rejection_at",
        "owner_wait",
        "paused_integration",
        "paused_integration_generation",
        "placement_refusal",
        "plan_settlement_wait",
        "planning_completed_at",
        "planning_execution_id",
        "planning_state_entry_token",
    ];
    pub fn decode(key: &str, raw: &str) -> serde_json::Result<Self> {
        match key {
            "awaiting_human" => serde_json::from_str(raw).map(Self::AwaitingHuman),
            "awaiting_human_marker_id" => {
                serde_json::from_str(raw).map(Self::AwaitingHumanMarkerId)
            }
            "awaiting_human_reason" => serde_json::from_str(raw).map(Self::AwaitingHumanReason),
            "coordination_review_pending" => {
                serde_json::from_str(raw).map(Self::CoordinationReviewPending)
            }
            "coordination_review_pending_id" => {
                serde_json::from_str(raw).map(Self::CoordinationReviewPendingId)
            }
            "daemon_upgrade_refusal" => serde_json::from_str(raw).map(Self::DaemonUpgradeRefusal),
            "deferred_dispatch" => serde_json::from_str(raw).map(Self::DeferredDispatch),
            "dispatch_disposition" => serde_json::from_str(raw).map(Self::DispatchDisposition),
            "environment_wait" => serde_json::from_str(raw).map(Self::EnvironmentWait),
            "executor_unavailable_execution_id" => {
                serde_json::from_str(raw).map(Self::ExecutorUnavailableExecutionId)
            }
            "last_execution_failure_at" => {
                serde_json::from_str(raw).map(Self::LastExecutionFailureAt)
            }
            "last_execution_failure_execution_id" => {
                serde_json::from_str(raw).map(Self::LastExecutionFailureExecutionId)
            }
            "last_workflow_guard_execution_id" => {
                serde_json::from_str(raw).map(Self::LastWorkflowGuardExecutionId)
            }
            "last_workflow_guard_name" => {
                serde_json::from_str(raw).map(Self::LastWorkflowGuardName)
            }
            "last_workflow_guard_reason" => {
                serde_json::from_str(raw).map(Self::LastWorkflowGuardReason)
            }
            "last_workflow_guard_rejection_at" => {
                serde_json::from_str(raw).map(Self::LastWorkflowGuardRejectionAt)
            }
            "owner_wait" => serde_json::from_str(raw).map(Self::OwnerWait),
            "paused_integration" => serde_json::from_str(raw).map(Self::PausedIntegration),
            "paused_integration_generation" => {
                serde_json::from_str(raw).map(Self::PausedIntegrationGeneration)
            }
            "placement_refusal" => serde_json::from_str(raw).map(Self::PlacementRefusal),
            "plan_settlement_wait" => serde_json::from_str(raw).map(Self::PlanSettlementWait),
            "planning_completed_at" => serde_json::from_str(raw).map(Self::PlanningCompletedAt),
            "planning_execution_id" => serde_json::from_str(raw).map(Self::PlanningExecutionId),
            "planning_state_entry_token" => {
                serde_json::from_str(raw).map(Self::PlanningStateEntryToken)
            }
            _ => Err(<serde_json::Error as serde::de::Error>::custom(
                "unknown condition metadata key",
            )),
        }
    }
    pub fn key(&self) -> &'static str {
        match self {
            Self::AwaitingHuman(_) => "awaiting_human",
            Self::AwaitingHumanMarkerId(_) => "awaiting_human_marker_id",
            Self::AwaitingHumanReason(_) => "awaiting_human_reason",
            Self::CoordinationReviewPending(_) => "coordination_review_pending",
            Self::CoordinationReviewPendingId(_) => "coordination_review_pending_id",
            Self::DaemonUpgradeRefusal(_) => "daemon_upgrade_refusal",
            Self::DeferredDispatch(_) => "deferred_dispatch",
            Self::DispatchDisposition(_) => "dispatch_disposition",
            Self::EnvironmentWait(_) => "environment_wait",
            Self::ExecutorUnavailableExecutionId(_) => "executor_unavailable_execution_id",
            Self::LastExecutionFailureAt(_) => "last_execution_failure_at",
            Self::LastExecutionFailureExecutionId(_) => "last_execution_failure_execution_id",
            Self::LastWorkflowGuardExecutionId(_) => "last_workflow_guard_execution_id",
            Self::LastWorkflowGuardName(_) => "last_workflow_guard_name",
            Self::LastWorkflowGuardReason(_) => "last_workflow_guard_reason",
            Self::LastWorkflowGuardRejectionAt(_) => "last_workflow_guard_rejection_at",
            Self::OwnerWait(_) => "owner_wait",
            Self::PausedIntegration(_) => "paused_integration",
            Self::PausedIntegrationGeneration(_) => "paused_integration_generation",
            Self::PlacementRefusal(_) => "placement_refusal",
            Self::PlanSettlementWait(_) => "plan_settlement_wait",
            Self::PlanningCompletedAt(_) => "planning_completed_at",
            Self::PlanningExecutionId(_) => "planning_execution_id",
            Self::PlanningStateEntryToken(_) => "planning_state_entry_token",
        }
    }
}
