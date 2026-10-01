use std::{
    collections::HashMap,
    sync::{Arc, OnceLock, RwLock},
};

use serde_json::Value;

use crate::workflow::default_roles;

pub mod coder_prompt;
pub mod generic_prompt;
pub mod loader;
pub mod planner_prompt;
pub mod read_only_task_prompt;
pub mod reviewer_prompt;
pub mod worker_prompt;

#[cfg(test)]
mod tests;

pub const BUILDER_ID_CODER_IMPLEMENTATION_V2: &str = "coder.implementation.v2";
pub const BUILDER_ID_CODER_REVIEW_FIX_V2: &str = "coder.review_fix.v2";
pub const BUILDER_ID_CODER_MERGE_FIX_V2: &str = "coder.merge_fix.v2";
pub const BUILDER_ID_WORKER_AUTONOMOUS_V1: &str = "worker.autonomous.v1";
pub const BUILDER_ID_WORKER_REVIEW_FIX_V1: &str = "worker.review_fix.v1";
pub const BUILDER_ID_WORKER_MERGE_FIX_V1: &str = "worker.merge_fix.v1";
pub const BUILDER_ID_REVIEWER_CONFORMANCE_V1: &str = "reviewer.conformance.v1";
pub const BUILDER_ID_PLANNER_DEFAULT_V2: &str = "planner.default.v2";
pub const BUILDER_ID_READ_ONLY_TASK_V1: &str = "task.read_only.v1";
pub const BUILDER_ID_GENERIC_DEFAULT_V2: &str = "generic.default.v2";

pub(crate) const MANAGED_EXECUTION_CONTRACT: &str = "\
Managed execution:
- Before acting, restate objective, constraints, and acceptance criteria.
- Use provided plans, comments, and prior review feedback before fresh exploration.
- Keep work scoped to the requested task.
- Failure taxonomy: classify any blocker using exactly this taxonomy: transient | input_missing | environment | code_bug | design_gap | review_failed | systemic.
- Never hide failed verification; report failures explicitly.";

/// How the dispatched agent delivers worklog entries and evidence.
///
/// A native agent's Task session carries the `task.worklog` and
/// `task.evidence` tools. A CLI harness has no Forge tool channel, so it
/// writes to its execution outbox and Forge ingests the files when the run
/// ends. Naming a tool the agent cannot call wastes its first turns hunting
/// for it, so the prompt must describe the channel the agent actually has.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskDelivery {
    #[default]
    NativeTools,
    Outbox,
}

impl TaskDelivery {
    #[must_use]
    pub fn for_backend_kind(backend_kind: &str) -> Self {
        if backend_kind == "native" {
            Self::NativeTools
        } else {
            Self::Outbox
        }
    }
}

const NATIVE_WORKLOG_CONTRACT: &str = "\
Worklog: append entries with `task.worklog` (append) as you work, so the reviewer reads what you actually did.
Write one after a meaningful milestone -- your initial approach, a completed slice, validation results, a deviation, a blocker -- and not for every tool call or file edit.
Use kind `progress`, `decision`, `validation`, or `blocker`. Forge derives the Task, execution, role, and identity from your session, and posts the final completion summary itself once the execution is accepted, so no separate handoff block is needed.
Proof of behaviour is a captured artifact, not prose: when a change alters UI or runtime behaviour, capture it with `task.evidence` (capture) -- a screenshot or recording by `path`, or verbatim command output as `content` -- and say in the worklog what you captured. If you could not capture proof, say so and why.
A worklog entry never moves the Task and never satisfies an acceptance check.";

const OUTBOX_WORKLOG_CONTRACT: &str = "\
Worklog and evidence: this harness has no Forge tools. Report through files in the directory named by the `FORGE_OUTBOX` environment variable; it sits outside the worktree, so nothing there is committed. Forge ingests it when this run ends, before review.
Worklog: append one JSON object per line to `$FORGE_OUTBOX/worklog.jsonl`, for example {\"kind\":\"validation\",\"summary\":\"python3 -m unittest: 20 tests OK\"}, so the reviewer reads what you actually did.
Write one after a meaningful milestone -- your initial approach, a completed slice, validation results, a deviation, a blocker -- and not for every tool call or file edit. `kind` is `progress`, `decision`, `validation`, or `blocker`. Forge records the Task, execution, role, and identity itself, and posts the final completion summary once the execution is accepted.
Proof of behaviour is a captured artifact, not prose: when a change alters UI or runtime behaviour, append one JSON object per line to `$FORGE_OUTBOX/evidence.jsonl`, one per artifact -- {\"kind\":\"screenshot\",\"caption\":\"...\",\"path\":\"shot.png\"} for a file (a worktree-relative path, or an absolute path to a file you wrote under `$FORGE_OUTBOX`), or {\"kind\":\"log\",\"caption\":\"...\",\"content\":\"<verbatim output>\"}. `kind` is `screenshot`, `walkthrough_video`, `log`, `report`, or `other`. If you could not capture proof, say so in the worklog and why.
A worklog entry never moves the Task and never satisfies an acceptance check.";

const NATIVE_REVIEW_REPORT_CONTRACT: &str = "\
Review report: record what you verified with `task.worklog` (append) entries -- kind `validation` for a check you ran, `blocker` for what stopped you -- and capture the command output or screenshot behind a finding with `task.evidence` (capture). These are your only Forge writes; the verdict itself belongs in the result block that ends your reply.";

const OUTBOX_REVIEW_REPORT_CONTRACT: &str = "\
Review report: this harness has no Forge tools. The directory named by the `FORGE_OUTBOX` environment variable is the only place you may write your report (build output and scratch files elsewhere are discarded with the worktree); Forge ingests it when this run ends. Append one JSON object per line to `$FORGE_OUTBOX/worklog.jsonl` for what you verified -- {\"kind\":\"validation\",\"summary\":\"...\"} for a check you ran, `blocker` for what stopped you -- and one JSON object per line to `$FORGE_OUTBOX/evidence.jsonl`, one per artifact, for the output behind a finding -- {\"kind\":\"log\",\"caption\":\"...\",\"content\":\"<verbatim output>\"}, or a `path` to a file you saved under `$FORGE_OUTBOX`, such as a screenshot. Evidence `kind` is `screenshot`, `walkthrough_video`, `log`, `report`, or `other`. The verdict itself belongs in the result block that ends your reply.";

/// Tell an implementing agent exactly which checks gate review and how they
/// run. Without this the agent validates with whatever interpreter or command
/// it prefers, passes locally, and learns the real gate only from a rejection.
pub(crate) fn required_checks_section(ctx: &AgentDispatchContext) -> Option<String> {
    if ctx.read_only_task || ctx.review_ci_steps.is_empty() {
        return None;
    }
    let mut section = String::from(
        "Required checks: when you finish, Forge runs each command below with `bash -lc` from the worktree root against your committed result. If any exits non-zero the Task comes back to you with its output. Run them yourself exactly that way -- `bash -lc '<command>'` from the worktree root, so the same shell, PATH, and interpreter versions apply -- and make every one pass before you finish:",
    );
    for step in &ctx.review_ci_steps {
        section.push_str("\n- `");
        section.push_str(step);
        section.push('`');
    }
    Some(section)
}

/// The worklog and evidence contract for an agent that implements the Task.
pub(crate) fn implementation_report_contract(delivery: TaskDelivery) -> &'static str {
    match delivery {
        TaskDelivery::NativeTools => NATIVE_WORKLOG_CONTRACT,
        TaskDelivery::Outbox => OUTBOX_WORKLOG_CONTRACT,
    }
}

/// The report contract for a read-only role, which may write nothing else.
pub(crate) fn read_only_report_contract(delivery: TaskDelivery) -> &'static str {
    match delivery {
        TaskDelivery::NativeTools => NATIVE_REVIEW_REPORT_CONTRACT,
        TaskDelivery::Outbox => OUTBOX_REVIEW_REPORT_CONTRACT,
    }
}

pub const EXECUTION_POLICY_NEW_EXECUTION: &str = "new_execution";
pub const EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD: &str =
    "resume_latest_target_role_thread";

#[derive(Debug, Clone)]
pub struct AgentDispatchContext {
    pub task: db::Task,
    pub role: String,
    pub state_name: String,
    pub state_config: Value,
    pub transition_log: Vec<db::TransitionLog>,
    pub comments: Vec<db::TaskComment>,
    pub plan: Option<String>,
    pub prior_reviews: Vec<db::Review>,
    pub parent_task: Option<db::Task>,
    pub sub_tasks: Vec<db::Task>,
    pub last_manual_bounce_reason: Option<String>,
    pub continuation_of_execution_id: Option<String>,
    pub continuation_logs_path: Option<String>,
    pub latest_review_feedback: Option<String>,
    pub latest_review_execution_id: Option<String>,
    pub latest_review_logs_path: Option<String>,
    pub read_only_task: bool,
    pub delivery: TaskDelivery,
    /// Checks Forge runs before review; see [`required_checks_section`].
    pub review_ci_steps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentPrompt {
    pub system: String,
    pub user: String,
    pub tools: Vec<String>,
}

impl AgentPrompt {
    /// Render the complete workflow prompt into the transport-neutral input
    /// persisted on an execution. CLI adapters expose one turn-text channel,
    /// so dropping `system` here would silently discard the role boundary.
    #[must_use]
    pub fn execution_input(&self, user_context: Option<&str>) -> String {
        let user = match user_context {
            Some(context) => format!("[User context: {context}]\n\n{}", self.user),
            None => self.user.clone(),
        };
        format!(
            "Forge role contract (authoritative):\n{}\n\nExecution request:\n{user}",
            self.system
        )
    }
}

pub trait PromptBuilder: Send + Sync {
    fn id(&self) -> &'static str;
    fn build(&self, ctx: &AgentDispatchContext) -> AgentPrompt;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptBuilderRegistryEntry {
    pub id: &'static str,
    pub label: &'static str,
    pub compatible_role_hints: &'static [&'static str],
    pub description: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchIntent {
    pub builder_id: Option<String>,
    pub execution_policy: Option<String>,
    pub prompt_config: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EffectivePromptSelection {
    pub builder_id: String,
    pub execution_policy: String,
}

type BuilderRegistry = Arc<RwLock<HashMap<String, Arc<dyn PromptBuilder>>>>;
type DefaultRoleBuilderMap = Arc<RwLock<HashMap<String, String>>>;

static PROMPT_BUILDERS: OnceLock<BuilderRegistry> = OnceLock::new();
static DEFAULT_ROLE_BUILDERS: OnceLock<DefaultRoleBuilderMap> = OnceLock::new();

fn registry() -> &'static BuilderRegistry {
    PROMPT_BUILDERS.get_or_init(|| {
        let mut builders: HashMap<String, Arc<dyn PromptBuilder>> = HashMap::new();
        let defaults: [Arc<dyn PromptBuilder>; 10] = [
            Arc::new(coder_prompt::CoderImplementationPromptBuilder),
            Arc::new(coder_prompt::CoderReviewFixPromptBuilder),
            Arc::new(coder_prompt::CoderMergeFixPromptBuilder),
            Arc::new(worker_prompt::WorkerAutonomousPromptBuilder),
            Arc::new(worker_prompt::WorkerReviewFixPromptBuilder),
            Arc::new(worker_prompt::WorkerMergeFixPromptBuilder),
            Arc::new(reviewer_prompt::ReviewerPromptBuilder),
            Arc::new(planner_prompt::PlannerPromptBuilder),
            Arc::new(read_only_task_prompt::ReadOnlyTaskPromptBuilder),
            Arc::new(generic_prompt::GenericPromptBuilder),
        ];

        for builder in defaults {
            builders.insert(builder.id().to_string(), builder);
        }

        Arc::new(RwLock::new(builders))
    })
}

fn default_role_builders() -> &'static DefaultRoleBuilderMap {
    DEFAULT_ROLE_BUILDERS.get_or_init(|| {
        let defaults = HashMap::from([
            (
                default_roles::CODER.to_string(),
                BUILDER_ID_CODER_IMPLEMENTATION_V2.to_string(),
            ),
            (
                default_roles::REVIEWER.to_string(),
                BUILDER_ID_REVIEWER_CONFORMANCE_V1.to_string(),
            ),
            (
                default_roles::PLANNER.to_string(),
                BUILDER_ID_PLANNER_DEFAULT_V2.to_string(),
            ),
            (
                default_roles::WORKER.to_string(),
                BUILDER_ID_WORKER_AUTONOMOUS_V1.to_string(),
            ),
        ]);
        Arc::new(RwLock::new(defaults))
    })
}

pub fn register_prompt_builder(builder: Arc<dyn PromptBuilder>) {
    let mut builders = registry()
        .write()
        .expect("prompt builder registry lock poisoned");
    builders.insert(builder.id().to_string(), builder);
}

pub fn register_default_role_builder(role: &str, builder_id: &str) {
    let mut role_defaults = default_role_builders()
        .write()
        .expect("default role builder mapping lock poisoned");
    role_defaults.insert(role.to_string(), builder_id.to_string());
}

pub fn resolve_prompt_builder(builder_id: &str) -> Arc<dyn PromptBuilder> {
    if let Some(builder) = registry()
        .read()
        .expect("prompt builder registry lock poisoned")
        .get(builder_id)
        .cloned()
    {
        return builder;
    }
    registry()
        .read()
        .expect("prompt builder registry lock poisoned")
        .get(BUILDER_ID_GENERIC_DEFAULT_V2)
        .cloned()
        .unwrap_or_else(|| Arc::new(generic_prompt::GenericPromptBuilder))
}

pub fn resolve_default_builder_id_for_role(role: &str) -> Option<String> {
    default_role_builders()
        .read()
        .expect("default role builder mapping lock poisoned")
        .get(role)
        .cloned()
}

pub fn prompt_builder_registry_entries() -> Vec<PromptBuilderRegistryEntry> {
    vec![
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_CODER_IMPLEMENTATION_V2,
            label: "Coder (Implementation)",
            compatible_role_hints: &[default_roles::CODER],
            description: "Implementation-focused prompt for normal coding tasks.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_CODER_REVIEW_FIX_V2,
            label: "Coder (Review Fix)",
            compatible_role_hints: &[default_roles::CODER],
            description: "Focused rework prompt for rejected reviews.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_CODER_MERGE_FIX_V2,
            label: "Coder (Merge Fix)",
            compatible_role_hints: &[default_roles::CODER],
            description: "Dirty Task-worktree completion prompt for integration retry loops.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_WORKER_AUTONOMOUS_V1,
            label: "Worker (Autonomous)",
            compatible_role_hints: &[default_roles::WORKER],
            description:
                "Single-agent prompt for planning, implementation, self-validation, and recovery.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_WORKER_REVIEW_FIX_V1,
            label: "Worker (Review Fix)",
            compatible_role_hints: &[default_roles::WORKER],
            description: "Same-worker prompt for addressing review and validation feedback.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_WORKER_MERGE_FIX_V1,
            label: "Worker (Merge Fix)",
            compatible_role_hints: &[default_roles::WORKER],
            description:
                "Same-worker prompt for finishing dirty Task-worktree changes and revalidating delivery.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_REVIEWER_CONFORMANCE_V1,
            label: "Reviewer (Default)",
            compatible_role_hints: &[default_roles::REVIEWER],
            description: "Read-only review prompt with pass/fail verdict instructions.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_PLANNER_DEFAULT_V2,
            label: "Planner (Default)",
            compatible_role_hints: &[default_roles::PLANNER],
            description: "Planning prompt for structured implementation plans.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_READ_ONLY_TASK_V1,
            label: "Read-only Task",
            compatible_role_hints: &[default_roles::CODER, default_roles::WORKER],
            description:
                "Discovery and planning prompt that reports findings without repository changes.",
        },
        PromptBuilderRegistryEntry {
            id: BUILDER_ID_GENERIC_DEFAULT_V2,
            label: "Generic",
            compatible_role_hints: &[],
            description: "Fallback prompt for custom roles without a specialized builder.",
        },
    ]
}

pub fn dispatch_intent_from_config(value: &Value) -> DispatchIntent {
    let dispatch = value.get("dispatch").unwrap_or(value);
    let prompt_config = dispatch
        .get("prompt")
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));

    DispatchIntent {
        builder_id: dispatch
            .get("builder")
            .and_then(Value::as_str)
            .map(str::to_owned),
        execution_policy: dispatch
            .get("execution_policy")
            .and_then(Value::as_str)
            .map(str::to_owned),
        prompt_config,
    }
}

pub fn dispatch_intent_from_workflow_dispatch(
    dispatch: Option<&api_types::WorkflowDispatch>,
) -> Option<DispatchIntent> {
    dispatch.map(|dispatch| DispatchIntent {
        builder_id: dispatch.builder.clone(),
        execution_policy: dispatch.execution_policy.map(|policy| match policy {
            api_types::WorkflowExecutionPolicy::NewExecution => {
                EXECUTION_POLICY_NEW_EXECUTION.to_string()
            }
            api_types::WorkflowExecutionPolicy::ResumeLatestTargetRoleThread => {
                EXECUTION_POLICY_RESUME_LATEST_TARGET_ROLE_THREAD.to_string()
            }
        }),
        prompt_config: dispatch
            .prompt
            .as_ref()
            .and_then(|prompt| serde_json::to_value(prompt).ok())
            .unwrap_or_else(|| Value::Object(Default::default())),
    })
}

pub fn effective_prompt_selection(
    role: &str,
    trigger_dispatch: Option<&DispatchIntent>,
    state_dispatch: Option<&DispatchIntent>,
) -> EffectivePromptSelection {
    let builder_id = trigger_dispatch
        .and_then(|intent| intent.builder_id.clone())
        .or_else(|| state_dispatch.and_then(|intent| intent.builder_id.clone()))
        .or_else(|| resolve_default_builder_id_for_role(role))
        .unwrap_or_else(|| BUILDER_ID_GENERIC_DEFAULT_V2.to_string());
    let execution_policy = trigger_dispatch
        .and_then(|intent| intent.execution_policy.clone())
        .or_else(|| state_dispatch.and_then(|intent| intent.execution_policy.clone()))
        .unwrap_or_else(|| EXECUTION_POLICY_NEW_EXECUTION.to_string());

    EffectivePromptSelection {
        builder_id,
        execution_policy,
    }
}

pub fn apply_prompt_overrides(mut prompt: AgentPrompt, prompt_config: &Value) -> AgentPrompt {
    let Some(config) = prompt_config.as_object() else {
        return prompt;
    };

    if let Some(system) = config.get("system").and_then(Value::as_str) {
        prompt.system = system.to_owned();
    }
    if let Some(system_prefix) = config.get("system_prefix").and_then(Value::as_str) {
        prompt.system = format!("{system_prefix}\n\n{}", prompt.system);
    }
    if let Some(system_append) = config.get("system_append").and_then(Value::as_str) {
        prompt.system.push_str("\n\n");
        prompt.system.push_str(system_append);
    }

    if let Some(user) = config.get("user").and_then(Value::as_str) {
        prompt.user = user.to_owned();
    }
    if let Some(user_prefix) = config.get("user_prefix").and_then(Value::as_str) {
        prompt.user = format!("{user_prefix}\n\n{}", prompt.user);
    }
    if let Some(user_append) = config.get("user_append").and_then(Value::as_str) {
        prompt.user.push_str("\n\n");
        prompt.user.push_str(user_append);
    }

    prompt
}

pub fn build_effective_prompt(
    dispatch_ctx: &AgentDispatchContext,
    trigger_dispatch: Option<&DispatchIntent>,
    state_dispatch: Option<&DispatchIntent>,
) -> (AgentPrompt, EffectivePromptSelection) {
    let mut selection =
        effective_prompt_selection(&dispatch_ctx.role, trigger_dispatch, state_dispatch);
    let read_only_worker = dispatch_ctx.read_only_task
        && matches!(
            dispatch_ctx.role.as_str(),
            default_roles::CODER | default_roles::WORKER
        );
    if read_only_worker {
        selection.builder_id = BUILDER_ID_READ_ONLY_TASK_V1.to_owned();
    }
    let base_prompt = resolve_prompt_builder(&selection.builder_id).build(dispatch_ctx);
    let prompt = apply_prompt_overrides(base_prompt, &dispatch_ctx.state_config);
    let prompt = apply_prompt_overrides(
        prompt,
        state_dispatch
            .map(|intent| &intent.prompt_config)
            .unwrap_or(&Value::Object(Default::default())),
    );
    let mut prompt = apply_prompt_overrides(
        prompt,
        trigger_dispatch
            .map(|intent| &intent.prompt_config)
            .unwrap_or(&Value::Object(Default::default())),
    );
    if read_only_worker {
        prompt.user.push_str(
            "\n\nExecution authority: this is a read-only discovery/planning Task. Do not modify, create, delete, or commit repository files. Inspect the available material and report the requested findings, evidence, decisions, and remaining uncertainty. A clean unchanged worktree is the expected completion state.",
        );
    }
    (prompt, selection)
}

pub(crate) fn default_tool_names(role: &str) -> Vec<String> {
    match role {
        default_roles::CODER | default_roles::WORKER => vec![
            "read_files".to_string(),
            "edit_files".to_string(),
            "run_tests".to_string(),
        ],
        default_roles::REVIEWER => vec![
            "read_files".to_string(),
            "run_tests".to_string(),
            "comment".to_string(),
        ],
        default_roles::PLANNER => vec!["read_files".to_string(), "write_plan".to_string()],
        _ => Vec::new(),
    }
}
