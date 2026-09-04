use crate::assignee::AssigneeKind;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::str::FromStr;
use ts_rs::TS;

fn default_hook_timeout() -> u64 {
    30
}

fn default_review_retry_budget() -> i32 {
    3
}

fn default_merge_fix_retry_budget() -> i32 {
    1
}

fn default_execution_retry_budget() -> i32 {
    3
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct RetryBudgets {
    pub review: Option<i32>,
    pub merge_fix: Option<i32>,
    pub execution: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct AutomaticRecoverySettings {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default = "default_automatic_recovery_max_attempts")]
    pub max_attempts: u32,
}

impl Default for AutomaticRecoverySettings {
    fn default() -> Self {
        Self {
            enabled: false,
            agent_id: None,
            max_attempts: default_automatic_recovery_max_attempts(),
        }
    }
}

fn default_automatic_recovery_max_attempts() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum LifecycleEvent {
    BeforeWork,
    OnWorkStart,
    OnWorkStop,
    OnTaskDone,
    OnTaskCancel,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum LifecycleHookDef {
    Script {
        command: String,
        #[serde(default = "default_hook_timeout")]
        timeout_seconds: u64,
        #[serde(default)]
        blocking: bool,
    },
    Plugin {
        name: String,
        #[serde(default)]
        enabled: bool,
        #[serde(default)]
        #[ts(type = "Record<string, unknown> | null")]
        config: Option<serde_json::Value>,
    },
}

pub type LifecycleHooks = std::collections::HashMap<LifecycleEvent, Vec<LifecycleHookDef>>;

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct ProjectSettings {
    #[serde(default)]
    pub retry_budgets: RetryBudgets,
    #[serde(default)]
    pub default_role_assignments: Vec<DefaultRoleAssignment>,
    #[serde(default)]
    pub lifecycle_hooks: LifecycleHooks,
    #[serde(default)]
    pub automatic_recovery: AutomaticRecoverySettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct LifecycleHookTestResponse {
    pub status: String,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub duration_ms: u64,
    pub timeout: bool,
    pub working_dir: String,
    pub environment_preview: BTreeMap<String, String>,
    pub hook_log_path: Option<String>,
}

impl Default for ProjectSettings {
    fn default() -> Self {
        Self {
            retry_budgets: RetryBudgets {
                review: Some(default_review_retry_budget()),
                merge_fix: Some(default_merge_fix_retry_budget()),
                execution: Some(default_execution_retry_budget()),
            },
            default_role_assignments: Vec::new(),
            lifecycle_hooks: std::collections::HashMap::new(),
            automatic_recovery: AutomaticRecoverySettings::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub struct TaskMetadata {
    pub retry_budgets: Option<RetryBudgets>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum StateKind {
    Backlog,
    Initial,
    Active,
    Gate,
    Terminal,
    Custom,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CanonicalPhase {
    Backlog,
    Ready,
    Working,
    Review,
    Done,
}

/// The effective meaning of one state when a Task transition committed.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct TaskWorkflowStateSnapshot {
    pub name: String,
    pub kind: StateKind,
    pub canonical_phase: CanonicalPhase,
    pub requires_user_approval: bool,
    pub is_cancellation: bool,
}

/// Immutable workflow inputs carried by a newly committed Task transition.
#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct TaskWorkflowTransitionSnapshot {
    pub definition_digest: String,
    pub parent_task_id: Option<String>,
    pub source_task_version: i64,
    pub from_state: TaskWorkflowStateSnapshot,
    pub to_state: TaskWorkflowStateSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct TaskTransitionEventPayload {
    pub transition_log_id: String,
    pub project_id: String,
    pub from_state: String,
    pub to_state: String,
    pub trigger_name: Option<String>,
    pub trigger_reason: String,
    pub rejection: bool,
    /// Missing on historical events whose effective workflow was not saved.
    /// Such events retain unknown semantics; current workflow state is not history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub workflow_snapshot: Option<TaskWorkflowTransitionSnapshot>,
}

impl TaskTransitionEventPayload {
    pub fn known_workflow_snapshot(&self) -> Option<&TaskWorkflowTransitionSnapshot> {
        self.workflow_snapshot.as_ref().filter(|snapshot| {
            snapshot.from_state.name == self.from_state
                && snapshot.to_state.name == self.to_state
                && !snapshot.definition_digest.is_empty()
        })
    }
}

impl FromStr for CanonicalPhase {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "backlog" => Ok(Self::Backlog),
            "ready" => Ok(Self::Ready),
            "working" => Ok(Self::Working),
            "review" => Ok(Self::Review),
            "done" => Ok(Self::Done),
            _ => Err(format!("unknown canonical phase: {value}")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum CleanupPolicy {
    Immediate,
    Delayed { seconds: u64 },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum HookAudience {
    #[default]
    All,
    AgentOnly,
    UserOnly,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum FailurePolicy {
    Block,
    #[default]
    Log,
    Cascade(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct GateConfig {
    #[serde(default)]
    pub reject_target: Option<String>,
    pub max_rejections: Option<i32>,
    pub approve_label: Option<String>,
    pub reject_label: Option<String>,
    #[serde(default)]
    #[ts(optional)]
    pub requires_user_approval: Option<bool>,
    #[serde(default)]
    #[ts(optional)]
    pub optional_when_unassigned: Option<bool>,
}

impl GateConfig {
    pub fn requires_user_approval(&self) -> bool {
        self.requires_user_approval.unwrap_or(false)
    }

    pub fn optional_when_unassigned(&self) -> bool {
        self.optional_when_unassigned.unwrap_or(false)
    }
}

fn default_params() -> serde_json::Value {
    serde_json::Value::Object(Default::default())
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct HookSpec {
    pub action: String,
    #[serde(default = "default_params")]
    #[ts(type = "Record<string, unknown>")]
    pub params: serde_json::Value,
    #[serde(default)]
    pub applies_to: HookAudience,
    #[serde(default)]
    pub on_failure: FailurePolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default, PartialEq)]
#[ts(export)]
pub struct StateHooks {
    #[serde(default)]
    pub before_exit: Vec<HookSpec>,
    #[serde(default)]
    pub on_exit: Vec<HookSpec>,
    #[serde(default)]
    pub before_enter: Vec<HookSpec>,
    #[serde(default)]
    pub on_enter: Vec<HookSpec>,
    #[serde(default)]
    pub after_enter: Vec<HookSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub struct StateDefinition {
    pub name: String,
    pub kind: StateKind,
    pub column: String,
    pub display_name: String,
    pub role: Option<String>,
    pub hooks: StateHooks,
    #[serde(default)]
    pub cleanup: Option<CleanupPolicy>,
    #[serde(default)]
    pub canonical_phase: Option<CanonicalPhase>,
    pub gate_config: Option<GateConfig>,
    #[serde(default)]
    pub dispatch: Option<WorkflowDispatch>,
    #[serde(default)]
    pub triggers: WorkflowTriggerMap,
    #[ts(type = "Record<string, unknown>")]
    pub config: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkflowTrigger {
    Accept,
    Reject,
    Fail,
    Retry,
}

impl WorkflowTrigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accept => "accept",
            Self::Reject => "reject",
            Self::Fail => "fail",
            Self::Retry => "retry",
        }
    }

    pub fn system_only(self) -> bool {
        matches!(self, Self::Fail | Self::Retry)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkflowExecutionStrategy {
    NewExecution,
    ResumeLatestTargetRoleThread,
}

pub type WorkflowExecutionPolicy = WorkflowExecutionStrategy;

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkflowPromptConfig {
    pub user_prefix: Option<String>,
    pub user_append: Option<String>,
    pub system_prefix: Option<String>,
    pub system_append: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, Default, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkflowDispatch {
    pub builder: Option<String>,
    pub execution_policy: Option<WorkflowExecutionPolicy>,
    pub prompt: Option<WorkflowPromptConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkflowTriggerDefinition {
    pub to: String,
    #[serde(default)]
    pub dispatch: Option<WorkflowDispatch>,
}

pub type WorkflowTriggerMap = BTreeMap<WorkflowTrigger, WorkflowTriggerDefinition>;

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct RoleDefinition {
    pub name: String,
    pub display_name: String,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[ts(export)]
pub enum WorkflowConfigValueType {
    Integer,
    Text,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(export)]
pub enum WorkflowConfigBinding {
    GateConfig { state: String, field: String },
    StateConfig { state: String, path: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct WorkflowConfigField {
    pub id: String,
    pub label: String,
    pub description: Option<String>,
    pub value_type: WorkflowConfigValueType,
    pub min: Option<i32>,
    #[ts(type = "unknown")]
    pub default_value: Option<serde_json::Value>,
    pub binding: WorkflowConfigBinding,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct PromptBuilderRegistryEntry {
    pub id: String,
    pub label: String,
    pub compatible_role_hints: Vec<String>,
    pub description: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, TS, PartialEq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct WorkflowDefinition {
    pub roles: Vec<RoleDefinition>,
    pub states: Vec<StateDefinition>,
    #[serde(default)]
    pub configuration: Vec<WorkflowConfigField>,
    #[serde(default)]
    pub cancellation_state: Option<String>,
}

impl WorkflowDefinition {
    fn state_index(&self, status: &str) -> Option<usize> {
        self.states.iter().position(|state| state.name == status)
    }

    fn implicit_accept_target(&self, from: &str) -> Option<&str> {
        let index = self.state_index(from)?;
        let state = &self.states[index];
        if state.kind == StateKind::Terminal
            || state.triggers.contains_key(&WorkflowTrigger::Accept)
        {
            return None;
        }
        self.states.get(index + 1).map(|state| state.name.as_str())
    }

    pub fn state_kind(&self, status: &str) -> Option<StateKind> {
        self.states
            .iter()
            .find(|state| state.name == status)
            .map(|state| state.kind)
    }

    pub fn canonical_phase_for_state(&self, status: &str) -> CanonicalPhase {
        let state = self.states.iter().find(|state| state.name == status);

        if let Some(phase) = state.and_then(|state| state.canonical_phase) {
            return phase;
        }

        if let Some(phase) = state.and_then(|state| canonical_phase_for_column(&state.column)) {
            return phase;
        }

        let normalized_status = status.trim().to_ascii_lowercase();
        if let Some(phase) = canonical_phase_for_legacy_state_name(&normalized_status) {
            return phase;
        }

        if let Some(state) = state {
            return match state.kind {
                StateKind::Backlog => CanonicalPhase::Backlog,
                StateKind::Initial => CanonicalPhase::Ready,
                StateKind::Active => CanonicalPhase::Working,
                StateKind::Gate => {
                    let normalized_name = state.name.to_ascii_lowercase();
                    if normalized_name.contains("review") || normalized_name.contains("merge") {
                        CanonicalPhase::Review
                    } else {
                        CanonicalPhase::Working
                    }
                }
                StateKind::Terminal => CanonicalPhase::Done,
                StateKind::Custom => CanonicalPhase::Working,
            };
        }

        tracing::warn!(
            status = %status,
            "unknown workflow state has no canonical phase; defaulting to working"
        );
        CanonicalPhase::Working
    }

    pub fn auto_transition_target(&self, from: &str) -> Option<&str> {
        self.states
            .iter()
            .find(|state| state.name == from)
            .and_then(|state| {
                state
                    .triggers
                    .iter()
                    .find(|(trigger, _)| matches!(trigger, WorkflowTrigger::Accept))
                    .map(|(_, definition)| definition.to.as_str())
            })
            .or_else(|| self.implicit_accept_target(from))
    }

    pub fn gate_reject_target(&self, gate_state: &str) -> Option<&str> {
        self.states
            .iter()
            .find(|state| state.name == gate_state && state.kind == StateKind::Gate)
            .and_then(|state| state.gate_config.as_ref())
            .and_then(|config| config.reject_target.as_deref())
    }

    pub fn cleanup_policy_for(&self, state_name: &str) -> Option<CleanupPolicy> {
        let state = self.states.iter().find(|state| state.name == state_name)?;
        if state.kind != StateKind::Terminal {
            return None;
        }
        if let Some(cleanup) = state.cleanup.as_ref() {
            return Some(cleanup.clone());
        }
        if self.cancellation_state.as_deref() == Some(state_name) {
            return Some(CleanupPolicy::Delayed { seconds: 86_400 });
        }
        Some(CleanupPolicy::Immediate)
    }

    pub fn trigger_between(&self, from: &str, to: &str) -> Option<WorkflowTrigger> {
        let explicit = self
            .states
            .iter()
            .find(|state| state.name == from)
            .and_then(|state| {
                state
                    .triggers
                    .iter()
                    .find(|(_, definition)| definition.to == to)
                    .map(|(trigger, _)| *trigger)
            });
        explicit.or_else(|| {
            self.implicit_accept_target(from)
                .filter(|target| *target == to)
                .map(|_| WorkflowTrigger::Accept)
        })
    }

    pub fn trigger_definition_between(
        &self,
        from: &str,
        to: &str,
    ) -> Option<(WorkflowTrigger, &WorkflowTriggerDefinition)> {
        self.states
            .iter()
            .find(|state| state.name == from)
            .and_then(|state| {
                state
                    .triggers
                    .iter()
                    .find(|(_, definition)| definition.to == to)
                    .map(|(trigger, definition)| (*trigger, definition))
            })
    }

    pub fn outgoing_trigger_targets(
        &self,
        from: &str,
    ) -> impl Iterator<Item = (WorkflowTrigger, String)> {
        let trigger_targets = self
            .states
            .iter()
            .find(|state| state.name == from)
            .map(|state| {
                let mut targets = state
                    .triggers
                    .iter()
                    .map(|(trigger, definition)| (*trigger, definition.to.clone()))
                    .collect::<Vec<_>>();
                if !state.triggers.contains_key(&WorkflowTrigger::Accept) {
                    if let Some(target) = self.implicit_accept_target(from) {
                        targets.push((WorkflowTrigger::Accept, target.to_owned()));
                    }
                }
                targets
            })
            .unwrap_or_default();
        if !trigger_targets.is_empty() {
            return trigger_targets.into_iter();
        }
        Vec::new().into_iter()
    }
}

fn canonical_phase_for_column(column: &str) -> Option<CanonicalPhase> {
    match column.trim().to_ascii_lowercase().as_str() {
        "backlog" => Some(CanonicalPhase::Backlog),
        "todo" | "ready" => Some(CanonicalPhase::Ready),
        "in progress" | "working" => Some(CanonicalPhase::Working),
        "review" => Some(CanonicalPhase::Review),
        "done" => Some(CanonicalPhase::Done),
        _ => None,
    }
}

fn canonical_phase_for_legacy_state_name(name: &str) -> Option<CanonicalPhase> {
    match name {
        "backlog" => Some(CanonicalPhase::Backlog),
        "todo" => Some(CanonicalPhase::Ready),
        "planning" | "in_progress" => Some(CanonicalPhase::Working),
        "review" | "merging" | "merge_failed" => Some(CanonicalPhase::Review),
        "done" | "cancelled" => Some(CanonicalPhase::Done),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CanonicalPhase, StateDefinition, StateHooks, StateKind, WorkflowDefinition,
        WorkflowTrigger, WorkflowTriggerDefinition,
    };

    fn state(name: &str, kind: StateKind) -> StateDefinition {
        StateDefinition {
            name: name.to_owned(),
            kind,
            column: name.to_owned(),
            display_name: name.to_owned(),
            role: None,
            hooks: StateHooks::default(),
            cleanup: None,
            canonical_phase: None,
            gate_config: None,
            dispatch: None,
            triggers: Default::default(),
            config: serde_json::json!({}),
        }
    }

    fn workflow(states: Vec<StateDefinition>) -> WorkflowDefinition {
        WorkflowDefinition {
            roles: Vec::new(),
            states,
            configuration: Vec::new(),
            cancellation_state: None,
        }
    }

    #[test]
    fn accept_defaults_to_next_state_when_omitted() {
        let workflow = workflow(vec![
            state("todo", StateKind::Initial),
            state("in_progress", StateKind::Active),
            state("done", StateKind::Terminal),
        ]);

        assert_eq!(workflow.auto_transition_target("todo"), Some("in_progress"));
        assert_eq!(
            workflow.trigger_between("todo", "in_progress"),
            Some(WorkflowTrigger::Accept)
        );
        assert_eq!(
            workflow
                .outgoing_trigger_targets("todo")
                .collect::<Vec<_>>(),
            vec![(WorkflowTrigger::Accept, "in_progress".to_owned())]
        );
    }

    #[test]
    fn explicit_accept_overrides_next_state_default() {
        let mut todo = state("todo", StateKind::Initial);
        todo.triggers.insert(
            WorkflowTrigger::Accept,
            WorkflowTriggerDefinition {
                to: "done".to_owned(),
                dispatch: None,
            },
        );
        let workflow = workflow(vec![
            todo,
            state("in_progress", StateKind::Active),
            state("done", StateKind::Terminal),
        ]);

        assert_eq!(workflow.auto_transition_target("todo"), Some("done"));
        assert_eq!(workflow.trigger_between("todo", "in_progress"), None);
    }

    #[test]
    fn terminal_states_do_not_get_implicit_accept_edges() {
        let workflow = workflow(vec![
            state("todo", StateKind::Initial),
            state("done", StateKind::Terminal),
            state("archived", StateKind::Terminal),
        ]);

        assert_eq!(workflow.auto_transition_target("done"), None);
        assert_eq!(workflow.trigger_between("done", "archived"), None);
        assert!(workflow.outgoing_trigger_targets("done").next().is_none());
    }

    #[test]
    fn canonical_phase_uses_the_documented_fallback_order() {
        let mut explicit = state("explicit", StateKind::Custom);
        explicit.column = "Backlog".to_owned();
        explicit.canonical_phase = Some(CanonicalPhase::Done);

        let mut column = state("column", StateKind::Custom);
        column.column = "Ready".to_owned();

        let mut legacy = state("merge_failed", StateKind::Custom);
        legacy.column = "Other".to_owned();

        let mut gate = state("approval", StateKind::Gate);
        gate.column = "Other".to_owned();

        let mut review_gate = state("code_review", StateKind::Gate);
        review_gate.column = "Other".to_owned();

        let workflow = workflow(vec![explicit, column, legacy, gate, review_gate]);

        assert_eq!(
            workflow.canonical_phase_for_state("explicit"),
            CanonicalPhase::Done
        );
        assert_eq!(
            workflow.canonical_phase_for_state("column"),
            CanonicalPhase::Ready
        );
        assert_eq!(
            workflow.canonical_phase_for_state("merge_failed"),
            CanonicalPhase::Review
        );
        assert_eq!(
            workflow.canonical_phase_for_state("approval"),
            CanonicalPhase::Working
        );
        assert_eq!(
            workflow.canonical_phase_for_state("code_review"),
            CanonicalPhase::Review
        );
    }

    #[test]
    fn unknown_state_defaults_to_working() {
        let workflow = workflow(Vec::new());

        assert_eq!(
            workflow.canonical_phase_for_state("unknown"),
            CanonicalPhase::Working
        );
    }

    #[test]
    fn legacy_state_definition_without_canonical_phase_deserializes() {
        let workflow: WorkflowDefinition = serde_json::from_value(serde_json::json!({
            "roles": [],
            "states": [{
                "name": "todo",
                "kind": "initial",
                "column": "Todo",
                "display_name": "Todo",
                "role": null,
                "hooks": {},
                "gate_config": null,
                "config": {}
            }]
        }))
        .expect("legacy workflow should deserialize");

        assert_eq!(workflow.states[0].canonical_phase, None);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct TaskRoleAssignmentResponse {
    pub id: String,
    pub task_id: String,
    pub role_name: String,
    pub assignee_type: Option<String>,
    pub assignee_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct AssignRoleRequest {
    pub assignee_type: String,
    pub assignee_id: Option<String>,
    pub reset_workspace: Option<bool>,
    pub reset_worktree: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct DefaultRoleAssignment {
    pub role_name: String,
    pub assignee_type: String,
    pub assignee_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[ts(export)]
pub struct InitialRoleAssignment {
    pub role_name: String,
    pub assignee_type: AssigneeKind,
    pub assignee_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct TransitionLogEntry {
    pub id: String,
    pub task_id: String,
    pub from_state: String,
    pub to_state: String,
    pub triggered_by: String,
    pub trigger_reason: String,
    pub hook_results_json: Vec<HookResultEntry>,
    pub rejection: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, TS, PartialEq, Eq)]
#[ts(export)]
pub struct HookResultEntry {
    pub action: String,
    pub phase: String,
    pub outcome: String,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
}
