//! The check spec of every execution family. With `canonical_policy`, entry
//! CI on a server-owned checkout is built for the durable runner's reusable
//! policy; every other spec characterizes a path that executes outside it.
use crate::contract::DEFAULT_CHECK_TIMEOUT_SECONDS;
use api_types::*;
use serde_json::Value;
use std::collections::BTreeSet;

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
    /// Build the bundle for the durable runner's canonical, reusable policy
    /// instead of the frozen policy of the inline path. Entry CI on a
    /// server-owned checkout only.
    pub canonical_policy: bool,
    /// The Task worktree the bundle would run in, when one exists: workspace
    /// id and placement generation. Families that act on the worktree put it
    /// in the run identity; families that only read a commit never do.
    pub workspace: Option<CheckWorkspaceIdentity<'a>>,
    /// Stage A makes no queue-head gating decision; the future consumer chooses.
    pub queue_head_bundle: Option<QueueHeadCheckBundle>,
}

#[derive(Debug, Clone, Copy)]
pub struct CheckWorkspaceIdentity<'a> {
    pub workspace_id: &'a str,
    pub generation: u64,
}

/// Who may share one run of a family's bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckFamilyScope {
    /// Only reads the commit: every Task at that commit shares the run.
    Commit,
    /// Acts on, or judges the state of, one worktree: scoped to the workspace
    /// and its generation, or to the Task when there is no worktree.
    Worktree,
    /// A machine or directory probe: scoped to the workspace when it runs in
    /// one, otherwise commit-scoped (no Task exists for it).
    Probe,
}

/// The classification of every family. `has_setup` is whether a conformance
/// bundle includes setup steps, which prepare the worktree before the checks.
pub fn check_family_scope(
    purpose: CheckPurpose,
    queue_head_bundle: Option<QueueHeadCheckBundle>,
    has_setup: bool,
) -> CheckFamilyScope {
    match purpose {
        CheckPurpose::EntryCi | CheckPurpose::ReviewCi | CheckPurpose::AgentSelected => {
            CheckFamilyScope::Commit
        }
        CheckPurpose::QueueHeadCi
            if !matches!(queue_head_bundle, Some(QueueHeadCheckBundle::Conformance)) =>
        {
            CheckFamilyScope::Commit
        }
        CheckPurpose::Conformance | CheckPurpose::QueueHeadCi => {
            if has_setup {
                CheckFamilyScope::Worktree
            } else {
                CheckFamilyScope::Commit
            }
        }
        CheckPurpose::BeforeWork | CheckPurpose::Lifecycle | CheckPurpose::EnvironmentPreflight => {
            CheckFamilyScope::Worktree
        }
        CheckPurpose::ReadinessProbe | CheckPurpose::EnvironmentHelper => CheckFamilyScope::Probe,
    }
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
    // The canonical policy is defined for the CI bundle on a server-owned
    // checkout. A daemon owner attests no inputs yet, and the manual
    // ReviewRunner, conformance and every other family keep the frozen policy
    // of their own execution path.
    let canonical = config.canonical_policy;
    if canonical && (family != CheckPurpose::EntryCi || config.owner_is_daemon) {
        return Err("the canonical policy runs entry CI on a server-owned checkout only".into());
    }
    let mut has_setup = false;
    let mut spec = CheckSpec {
        schema_revision: CHECK_SPEC_REVISION,
        scope: CheckScope::Commit,
        commands: Vec::new(),
        // No family configured today has a cleanup step.
        declares_cleanup: false,
        configured_commands: 0,
        blank_commands: Vec::new(),
        execution_policy: if config.owner_is_daemon {
            "legacy-daemon/1"
        } else if canonical {
            CANONICAL_CI_POLICY
        } else {
            "legacy-server/1"
        }
        .into(),
    };
    let mut keys: BTreeSet<_> = config.environment.env.keys().cloned().collect();
    if canonical {
        // The canonical policy clears the ambient environment, so the spec
        // names what replaces it and the digest covers it.
        keys.extend(CANONICAL_ENVIRONMENT_KEYS.map(str::to_owned));
    }
    let mut add = |id: String,
                   command: String,
                   cwd,
                   timeout,
                   failure_policy,
                   keys: &BTreeSet<String>,
                   requirements: BTreeSet<String>| {
        // Production runs a blank step and it passes: `bash -lc ""` does
        // nothing. Dropping it changes no outcome and runs nothing; the spec
        // keeps where it was so evidence can still name it.
        let position = spec.configured_commands;
        spec.configured_commands += 1;
        if command.trim().is_empty() {
            spec.blank_commands.push(position);
            return false;
        }
        spec.commands.push(CheckCommandSpec {
            id,
            shell_text: command,
            shell: "bash -lc".into(),
            working_directory: cwd,
            environment_keys: keys.clone(),
            timeout_seconds: timeout,
            failure_policy,
            cacheability: if canonical {
                CheckCacheability::DeclaredControlledInputs
            } else {
                CheckCacheability::Uncacheable
            },
            requirement_ids: requirements,
        });
        true
    };
    match family {
        CheckPurpose::EntryCi | CheckPurpose::ReviewCi => {
            let effective = if family == CheckPurpose::EntryCi {
                if task_scope_is_read_only(config.source) {
                    serde_json::json!({})
                } else if let Some(state) = config.entry_state_config {
                    // `review_ci_steps` reads a nested `review` object first.
                    state.get("review").unwrap_or(state).clone()
                } else {
                    effective_review_config(config.source)?
                }
            } else {
                effective_review_config(config.source)?
            };
            if let Some(steps) = effective.get("ci_steps") {
                // IDs number the steps that run, so a list with blanks has
                // the identity of the same list without them.
                let mut kept = 0;
                for step in steps.as_array().ok_or("review ci_steps must be an array")? {
                    let command = step
                        .as_str()
                        .ok_or("review ci_steps entries must be strings")?;
                    if add(
                        format!("ci:{kept}"),
                        command.into(),
                        CheckWorkingDirectory::TaskRoot,
                        None,
                        CheckFailurePolicy::StopBundle,
                        &keys,
                        BTreeSet::new(),
                    ) {
                        kept += 1;
                    }
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
                has_setup |= add(
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
                    if config.workspace.is_some() {
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
    let workspace = config.workspace.map(|workspace| CheckScope::Workspace {
        workspace_id: workspace.workspace_id.into(),
        generation: workspace.generation,
    });
    spec.scope = match check_family_scope(purpose, config.queue_head_bundle, has_setup) {
        CheckFamilyScope::Commit => CheckScope::Commit,
        CheckFamilyScope::Probe => workspace.unwrap_or(CheckScope::Commit),
        CheckFamilyScope::Worktree => match workspace {
            Some(scope) => scope,
            // A script with no worktree still runs once for its own Task.
            None => CheckScope::Task {
                task_id: config.source["task_id"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .ok_or("a worktree-scoped check needs a workspace or a Task")?
                    .into(),
            },
        },
    };
    spec.validate()?;
    Ok(spec)
}

#[cfg(test)]
mod tests;
