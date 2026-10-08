//! Characterization of today's execution families; no caller executes this spec.
use crate::contract::DEFAULT_CHECK_TIMEOUT_SECONDS;
use api_types::*;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Uses the same source snapshot as `contract::context_from_source` and the
/// effective workflow/Project/Task merge. Lifecycle hooks are the already
/// selected event's definitions; readiness probes are the already selected
/// probe set. Dynamic agent commands have no configured authoritative bundle.
#[derive(Debug, Clone, Copy)]
pub enum QueueHeadCheckBundle {
    CiOnly,
    Conformance,
}

pub struct CheckSpecConfiguration<'a> {
    pub source: &'a Value,
    /// Current hook state's already-effective config; custom workflows can
    /// place run_ci_steps outside the review state. None selects review config.
    pub entry_state_config: Option<&'a Value>,
    pub environment: &'a ProjectEnvironment,
    pub hooks: &'a [LifecycleHookDef],
    /// Owner hook-test selection bypasses asynchronous blocking-hook filtering.
    pub hook_test_index: Option<usize>,
    /// The exported single-check helper ignores the check's role filter.
    pub single_environment_check: Option<&'a EnvironmentCheck>,
    pub event: LifecycleEvent,
    pub role: &'a str,
    pub owner_is_daemon: bool,
    pub has_workspace: bool,
    pub whole_run_timeout_seconds: u64,
    /// Stage A makes no queue-head gating decision; the future consumer chooses.
    pub queue_head_bundle: Option<QueueHeadCheckBundle>,
}

/// A single builder describes all configured families. Existing commands have
/// no controlled-input declaration: upgrading never makes them cacheable.
/// A later configuration surface can supply explicit declarations using the
/// shared CheckCommandSpec, without changing any execution path in stage A.
pub fn build_check_spec(
    purpose: CheckPurpose,
    config: &CheckSpecConfiguration<'_>,
) -> Result<CheckSpec, String> {
    let family = if purpose == CheckPurpose::QueueHeadCi {
        match config
            .queue_head_bundle
            .ok_or("queue-head bundle policy is not settled")?
        {
            QueueHeadCheckBundle::CiOnly => CheckPurpose::EntryCi,
            QueueHeadCheckBundle::Conformance => CheckPurpose::Conformance,
        }
    } else {
        purpose
    };
    let mut spec = CheckSpec {
        schema_revision: CHECK_SPEC_REVISION,
        purpose,
        commands: Vec::new(),
        whole_run_timeout_seconds: config.whole_run_timeout_seconds,
        execution_policy: if config.owner_is_daemon {
            "legacy-daemon/1"
        } else {
            "legacy-server/1"
        }
        .into(),
    };
    let keys: BTreeSet<_> = config.environment.env.keys().cloned().collect();
    let mut add = |id: String,
                   command: String,
                   cwd,
                   timeout,
                   failure_policy,
                   keys: &BTreeSet<String>,
                   requirements: BTreeSet<String>| {
        spec.commands.push(CheckCommandSpec {
            id,
            shell_text: command,
            shell: "bash -lc".into(),
            working_directory: cwd,
            environment_keys: keys.clone(),
            timeout_seconds: timeout,
            failure_policy,
            cacheability: CheckCacheability::Uncacheable,
            requirement_ids: requirements,
        });
    };
    match family {
        CheckPurpose::EntryCi | CheckPurpose::ReviewCi => {
            let effective = if family == CheckPurpose::EntryCi {
                if task_scope_is_read_only(config.source) {
                    serde_json::json!({})
                } else if let Some(state) = config.entry_state_config {
                    state.clone()
                } else {
                    effective_review_config(config.source)?
                }
            } else {
                effective_review_config(config.source)?
            };
            if let Some(steps) = effective.get("ci_steps") {
                for (index, step) in steps
                    .as_array()
                    .ok_or("review ci_steps must be an array")?
                    .iter()
                    .enumerate()
                {
                    let command = step
                        .as_str()
                        .filter(|s| !s.trim().is_empty())
                        .ok_or("invalid required CI command")?;
                    add(
                        format!("ci:{index}"),
                        command.into(),
                        CheckWorkingDirectory::TaskRoot,
                        None,
                        CheckFailurePolicy::StopBundle,
                        &keys,
                        BTreeSet::new(),
                    );
                }
            }
        }
        CheckPurpose::Conformance | CheckPurpose::QueueHeadCi => {
            // Reuse requirement selection, read-only behavior and validation;
            // do not invent a second interpretation of governing links.
            let context = crate::contract::context_from_source(config.source)?;
            let timeout = Some(u64::from(
                context
                    .check_timeout_seconds
                    .unwrap_or(DEFAULT_CHECK_TIMEOUT_SECONDS),
            ));
            for (index, command) in context.setup_steps.into_iter().enumerate() {
                add(
                    format!("setup:{index}"),
                    command,
                    CheckWorkingDirectory::TaskRoot,
                    timeout,
                    CheckFailurePolicy::StopBundle,
                    &keys,
                    BTreeSet::new(),
                );
            }
            for check in context.required_checks {
                add(
                    check.id,
                    check.command,
                    CheckWorkingDirectory::TaskRoot,
                    timeout,
                    CheckFailurePolicy::Continue,
                    &keys,
                    check.requirement_ids.into_iter().collect(),
                );
            }
        }
        CheckPurpose::BeforeWork | CheckPurpose::Lifecycle => {
            let mut hook_keys = keys.clone();
            hook_keys.extend(
                [
                    "FORGE_EVENT",
                    "FORGE_TASK_ID",
                    "FORGE_TASK_TITLE",
                    "FORGE_TASK_STATUS",
                    "FORGE_TASK_PREVIOUS_STATUS",
                    "FORGE_PROJECT_ID",
                    "FORGE_PROJECT_NAME",
                    "FORGE_REPO_PATH",
                    "FORGE_WORKTREE_PATH",
                    "FORGE_AGENT_ID",
                    "FORGE_EXECUTION_ID",
                ]
                .map(str::to_owned),
            );
            for (index, hook) in config.hooks.iter().enumerate() {
                let LifecycleHookDef::Script {
                    command,
                    timeout_seconds,
                    blocking,
                } = hook
                else {
                    continue;
                };
                if let Some(selected) = config.hook_test_index {
                    if purpose != CheckPurpose::Lifecycle || index != selected {
                        continue;
                    }
                } else if purpose == CheckPurpose::BeforeWork {
                    if config.event != LifecycleEvent::BeforeWork || !blocking {
                        continue;
                    }
                } else if config.event == LifecycleEvent::BeforeWork && *blocking {
                    continue;
                }
                add(
                    format!("hook:{index}"),
                    command.clone(),
                    if config.has_workspace {
                        CheckWorkingDirectory::TaskRoot
                    } else {
                        CheckWorkingDirectory::LifecycleFallback
                    },
                    Some(if *timeout_seconds == 0 {
                        30
                    } else {
                        *timeout_seconds
                    }),
                    if purpose == CheckPurpose::BeforeWork {
                        CheckFailurePolicy::StopBundle
                    } else {
                        CheckFailurePolicy::Continue
                    },
                    &hook_keys,
                    BTreeSet::new(),
                );
            }
        }
        CheckPurpose::EnvironmentPreflight
        | CheckPurpose::ReadinessProbe
        | CheckPurpose::EnvironmentHelper => {
            let checks = if purpose == CheckPurpose::EnvironmentHelper {
                config
                    .single_environment_check
                    .map_or(config.environment.checks.as_slice(), std::slice::from_ref)
            } else {
                config.environment.checks.as_slice()
            };
            for check in checks {
                if purpose != CheckPurpose::ReadinessProbe
                    && !(purpose == CheckPurpose::EnvironmentHelper
                        && config.single_environment_check.is_some())
                    && !check.applies_to(config.role)
                {
                    continue;
                }
                add(
                    check.name.clone(),
                    check.command.clone(),
                    match purpose {
                        CheckPurpose::EnvironmentPreflight => CheckWorkingDirectory::TaskRoot,
                        CheckPurpose::ReadinessProbe => {
                            CheckWorkingDirectory::RepositoryOrProbeScratch
                        }
                        _ => CheckWorkingDirectory::SuppliedDirectory,
                    },
                    Some(check.timeout_seconds.clamp(1, 300)),
                    if purpose == CheckPurpose::ReadinessProbe {
                        CheckFailurePolicy::Continue
                    } else {
                        CheckFailurePolicy::StopBundle
                    },
                    &keys,
                    BTreeSet::new(),
                );
            }
        }
        // Runtime agent commands cannot be derived from Project configuration
        // and are not automatically promoted to authoritative candidate checks.
        CheckPurpose::AgentSelected => {}
    }
    spec.validate()?;
    Ok(spec)
}

/// Values for a future identity builder are explicit and classified. Project
/// environment values are NOT copied here: today they have no secret metadata.
pub fn declared_identity_keys(spec: &CheckSpec) -> BTreeMap<String, CheckEnvironmentValue> {
    spec.commands
        .iter()
        .flat_map(|c| c.environment_keys.iter().cloned())
        .map(|key| (key, CheckEnvironmentValue::Volatile))
        .collect()
}

#[cfg(test)]
mod tests;
