//! Durable CI subchecks and pure classification. These facts change routing,
//! never the immutable check result or its certification.
use super::consumer::{
    CheckApplication, CheckVerdict, CheckWaitState, TaskCheckConsumers, TaskCheckRequest,
};
use crate::{Result, ServiceError};
use api_types::*;
use db::{
    CheckConsumerOrigin, CheckRunRepo, ProjectRepo, RepoRepo, SqliteDb, StoredCheckResult,
    StoredCheckRun,
};
use std::collections::BTreeSet;

pub(crate) enum Progress {
    Pending(String),
    Complete(Vec<StepResultEntry>),
}

pub(crate) fn steps(result: &StoredCheckResult) -> Vec<StepResultEntry> {
    result
        .commands
        .iter()
        .map(|command| {
            serde_json::from_value(serde_json::to_value(command).expect("command serializes"))
                .expect("step facts deserialize")
        })
        .collect()
}

pub(crate) async fn original_application(
    db: &SqliteDb,
    task: &db::Task,
    epoch: i64,
    origin: CheckConsumerOrigin,
    authority: &str,
) -> Result<CheckApplication> {
    let key = format!("task-step/1|{origin}|{}|{epoch}|{authority}", task.id);
    let consumer = db
        .ci_triage_consumer(&key)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let run = db
        .check_run(consumer.run_id.as_deref().ok_or(db::DbError::NotFound)?)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let result = db
        .check_result(consumer.result_id.as_deref().ok_or(db::DbError::NotFound)?)
        .await?
        .ok_or(db::DbError::NotFound)?;
    Ok(CheckApplication {
        task_id: task.id.clone(),
        status_epoch: epoch,
        authority: authority.to_owned(),
        consumer_id: consumer.id,
        run,
        verdict: if result.outcome == db::CheckResultOutcome::InfrastructureFailed {
            CheckVerdict::InfrastructureExhausted(result)
        } else {
            CheckVerdict::Result(result)
        },
    })
}

/// Calling again after a wake joins exactly the same subchecks. The volatile
/// allowance admits one rerun bundle; durable consumer keys fence redelivery.
pub(crate) async fn triage(
    db: &SqliteDb,
    consumers: &TaskCheckConsumers,
    task: &db::Task,
    application: &CheckApplication,
    config: &serde_json::Value,
    target_tip: Option<&str>,
) -> Result<Progress> {
    let CheckVerdict::Result(result) = &application.verdict else {
        return Ok(Progress::Complete(Vec::new()));
    };
    let mut entries = steps(result);
    if result.outcome != db::CheckResultOutcome::Fail {
        return Ok(Progress::Complete(entries));
    }
    let project = ProjectRepo::get_by_id(db, &task.project_id)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let settings: ProjectSettings = serde_json::from_str(&project.settings)
        .map_err(|e| ServiceError::invalid_operation(e.to_string()))?;
    let mode = settings.review_ci_triage;
    if mode == ReviewCiTriage::Off {
        return Ok(Progress::Complete(entries));
    }
    let mut allowance = db::budget::Invocation::new(db::budget::Kind::ReviewCiTriage);
    if !allowance.consume() {
        return Ok(Progress::Complete(entries));
    }
    let failed: Vec<usize> = entries
        .iter()
        .filter(|step| step.exit_code != 0)
        .map(|step| step.index)
        .collect();
    if failed.is_empty() {
        return Ok(Progress::Complete(entries));
    }
    let run = &application.run;
    let mut rerun_identity = run.identity.clone();
    rerun_identity.inputs.spec.commands = failed
        .iter()
        .map(|index| {
            let mut command = run.identity.inputs.spec.commands[*index].clone();
            command.id = format!("triage-rerun:{}", command.id);
            command.failure_policy = CheckFailurePolicy::Continue;
            command.cacheability = CheckCacheability::Uncacheable;
            command
        })
        .collect();
    rerun_identity.inputs.spec.configured_commands = rerun_identity.inputs.spec.commands.len();
    rerun_identity.inputs.spec.blank_commands.clear();
    let rerun = request(db, consumers, application, rerun_identity, "rerun").await?;
    let (rerun_id, rerun) = match rerun {
        Answer::Pending(id) => return Ok(Progress::Pending(id)),
        Answer::Result(id, result) => (id, result),
    };
    let consistent: Vec<usize> = failed
        .iter()
        .enumerate()
        .filter_map(|(position, index)| {
            rerun
                .commands
                .get(position)
                .is_none_or(|again| {
                    again.exit_code != 0
                        && (again.exit_code < 0
                            || entries[*index].failures_truncated
                            || again.failures_truncated
                            || entries[*index].failures.is_empty()
                            || again.failures.is_empty()
                            || entries[*index]
                                .failures
                                .iter()
                                .any(|signature| again.failures.contains(signature)))
                })
                .then_some(*index)
        })
        .collect();
    let mut base_status = CiBaseStatus::NotNeeded;
    let mut reason = None;
    let mut base_commit = None;
    let mut base_result = None;
    let mut base_run_id = None;
    let mut setup_count = 0;
    if !consistent.is_empty() {
        let setup: Vec<String> = config["setup_steps"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|v| v.as_str())
            .filter(|s| !s.trim().is_empty())
            .map(str::to_owned)
            .collect();
        if run.machine_id.is_some()
            || run.identity.inputs.spec.execution_policy != CANONICAL_CI_POLICY
        {
            base_status = CiBaseStatus::BaseUnavailable;
            reason = Some("base comparison is unavailable on daemon placements".to_owned());
        } else if setup.is_empty() {
            base_status = CiBaseStatus::BaseUnavailable;
            reason = Some("set `setup_steps` to enable base comparison".to_owned());
        } else {
            base_commit = base_sha(db, task, run, target_tip).await?;
            if let Some(commit) = &base_commit {
                let mut identity = run.identity.clone();
                identity.commit_sha = commit.clone();
                identity.inputs.spec.scope = CheckScope::Commit;
                let last = *consistent.last().expect("consistent steps");
                let mut commands = Vec::new();
                for (index, text) in setup.iter().enumerate() {
                    let mut command = identity.inputs.spec.commands[0].clone();
                    command.id = format!("triage-base:setup:{index}");
                    command.shell_text = text.clone();
                    command.failure_policy = CheckFailurePolicy::StopBundle;
                    commands.push(command);
                }
                setup_count = commands.len();
                for mut command in identity.inputs.spec.commands.iter().take(last + 1).cloned() {
                    command.id = format!("triage-base:{}", command.id);
                    // Collect every CI failure through the last candidate failure.
                    command.failure_policy = CheckFailurePolicy::Continue;
                    commands.push(command);
                }
                identity.inputs.spec.commands = commands;
                identity.inputs.spec.configured_commands = identity.inputs.spec.commands.len();
                identity.inputs.spec.blank_commands.clear();
                let base = request(db, consumers, application, identity, "base").await?;
                match base {
                    Answer::Pending(id) => return Ok(Progress::Pending(id)),
                    Answer::Result(id, result) => {
                        base_run_id = Some(id);
                        base_result = Some(result);
                    }
                }
                base_status = CiBaseStatus::Available;
            } else {
                base_status = CiBaseStatus::BaseUnavailable;
                reason = Some("the base commit is unavailable on the candidate owner".to_owned());
            }
        }
    }
    if let Some(base) = &base_result {
        // A Project environment changed while a check waited: the owner may
        // still report its real verdict, but it attests none of the old inputs.
        // Such evidence cannot establish a comparable base for a waiver.
        let expected = &run.identity.inputs.environment_identity;
        let comparable = matches!(expected, CheckEnvironmentIdentity::Attested { .. })
            && owner_inputs_match(db, &run.id, expected).await?
            && owner_inputs_match(db, &rerun_id, expected).await?
            && owner_inputs_match(db, &base.run_id, expected).await?;
        if !comparable
            || !matches!(
                base.outcome,
                db::CheckResultOutcome::Pass | db::CheckResultOutcome::Fail
            )
            || base.commands.len() < setup_count
            || base
                .commands
                .iter()
                .take(setup_count)
                .any(|step| step.exit_code != 0)
            || consistent.iter().any(|index| {
                base.commands.get(setup_count + index).is_none_or(|step| {
                    step.exit_code != 0
                        && step.failures.is_empty()
                        && !entries[*index].failures.is_empty()
                })
            })
        {
            base_status = CiBaseStatus::BaseRunInvalid;
            reason = Some(
                match base
                    .commands
                    .iter()
                    .take(setup_count)
                    .find(|step| step.exit_code != 0)
                {
                    Some(step) => format!(
                        "base_run_invalid: setup `{}` exited {}: {}",
                        step.command,
                        step.exit_code,
                        step.output_tail.chars().take(240).collect::<String>()
                    ),
                    None if !comparable => "base_run_invalid: candidate, rerun and base owner inputs are not comparable".to_owned(),
                    None => {
                        "base_run_invalid: CI did not finish, or base signatures are unavailable"
                            .to_owned()
                    }
                },
            );
        }
    }
    for (position, index) in failed.iter().enumerate() {
        let entry = &mut entries[*index];
        let again = rerun
            .commands
            .get(position)
            .filter(|step| step.exit_code >= 0);
        let base = (base_status == CiBaseStatus::Available)
            .then(|| {
                base_result
                    .as_ref()
                    .and_then(|base| base.commands.get(setup_count + index))
            })
            .flatten();
        entry.triage = Some(classify(
            entry,
            again,
            base,
            StepCiTriage {
                mode,
                base_status,
                base_commit: base_commit.clone(),
                rerun_run_id: Some(rerun_id.clone()),
                base_run_id: base_run_id.clone(),
                reason: reason.clone(),
                class: None,
                failures: Vec::new(),
                blocking: true,
            },
        ));
    }
    super::ci_triage_attention::record(
        db,
        task,
        application,
        &entries,
        base_result.as_ref(),
        setup_count,
    )
    .await?;
    Ok(Progress::Complete(entries))
}

pub(crate) fn classify(
    entry: &StepResultEntry,
    again: Option<&CheckCommandOutcome>,
    base: Option<&CheckCommandOutcome>,
    mut triage: StepCiTriage,
) -> StepCiTriage {
    let rerun_passed = again.is_some_and(|step| step.exit_code == 0);
    let mut unclassified = false;
    if rerun_passed {
        triage.class = Some(CiFailureClass::Flaky);
        triage.failures = entry
            .failures
            .iter()
            .map(|signature| CiFailureTriage {
                signature: signature.clone(),
                class: CiFailureClass::Flaky,
            })
            .collect();
    } else if let Some(again) =
        again.filter(|again| !entry.failures.is_empty() && !again.failures.is_empty())
    {
        let signatures: BTreeSet<_> = entry
            .failures
            .iter()
            .chain(&again.failures)
            .cloned()
            .collect();
        for signature in signatures {
            let consistent =
                entry.failures.contains(&signature) && again.failures.contains(&signature);
            let class = if !consistent && !entry.failures_truncated && !again.failures_truncated {
                Some(CiFailureClass::Flaky)
            } else {
                base.map(|step| {
                    if step.exit_code != 0 && step.failures.contains(&signature) {
                        CiFailureClass::PreExisting
                    } else {
                        CiFailureClass::Introduced
                    }
                })
            };
            match class {
                Some(class) => triage.failures.push(CiFailureTriage { signature, class }),
                None => unclassified = true,
            }
        }
    } else if let (Some(_), Some(base)) = (again, base) {
        triage.class = Some(if base.exit_code != 0 {
            triage.reason = Some("The base also fails this step without comparable test signatures; new failures may be hidden, so it still blocks".to_owned());
            CiFailureClass::PreExistingUnverified
        } else {
            CiFailureClass::Introduced
        });
    } else {
        unclassified = true;
    }
    // Missing/truncated evidence never proves that every failure was harmless.
    let incomplete = entry.failures_truncated || again.is_some_and(|step| step.failures_truncated);
    triage.blocking = triage.mode != ReviewCiTriage::Full
        || unclassified
        || (!rerun_passed && incomplete)
        || triage.class.is_some_and(CiFailureClass::blocks)
        || triage.failures.iter().any(|failure| failure.class.blocks());
    triage
}

pub(crate) fn blocks(step: &StepResultEntry) -> bool {
    step.exit_code != 0 && step.triage.as_ref().is_none_or(|triage| triage.blocking)
}

/// Repair messages name only the failures this Task must address. Full raw
/// output remains in the immutable evidence and Review details.
pub(crate) fn failure_summary(entries: &[StepResultEntry]) -> String {
    entries
        .iter()
        .filter(|entry| blocks(entry))
        .map(|entry| {
            let failures: Vec<_> = entry
                .triage
                .as_ref()
                .into_iter()
                .flat_map(|triage| &triage.failures)
                .filter(|failure| failure.class.blocks())
                .take(20)
                .map(|failure| failure.signature.as_str())
                .collect();
            let mut message = format!("`{}` exited {}", entry.command, entry.exit_code);
            if !failures.is_empty() {
                message.push_str(": ");
                message.push_str(&failures.join(", "));
            }
            if let Some(reason) = entry
                .triage
                .as_ref()
                .and_then(|triage| triage.reason.as_deref())
            {
                message.push_str("; ");
                message.push_str(reason);
            }
            message
        })
        .collect::<Vec<_>>()
        .join("; ")
}

enum Answer {
    Pending(String),
    Result(String, StoredCheckResult),
}
async fn request(
    db: &SqliteDb,
    consumers: &TaskCheckConsumers,
    application: &CheckApplication,
    identity: db::CheckRunIdentity,
    phase: &str,
) -> Result<Answer> {
    let origin = db::CheckDeliveryRepo::check_consumer_delivery(db, &application.consumer_id)
        .await?
        .ok_or(db::DbError::NotFound)?
        .consumer
        .origin;
    let requested = consumers
        .request(TaskCheckRequest {
            task_id: application.task_id.clone(),
            status_epoch: application.status_epoch,
            authority: format!("ci-triage:{phase}:{}", application.authority),
            origin,
            purpose: if origin == CheckConsumerOrigin::Integration {
                CheckPurpose::QueueHeadCi
            } else {
                CheckPurpose::EntryCi
            },
            identity,
            workspace_id: application.run.workspace_id.clone(),
            machine_id: application.run.machine_id.clone(),
            wall_timeout_seconds: application
                .run
                .applied_timeout_seconds
                .and_then(|n| u64::try_from(n).ok())
                .unwrap_or(3600),
        })
        .await?;
    let consumer = requested.consumer;
    match consumers.wait_state(&consumer.id).await? {
        CheckWaitState::Result(result) => Ok(Answer::Result(result.run_id.clone(), result)),
        CheckWaitState::Pending | CheckWaitState::Exhausted => Ok(Answer::Pending(consumer.id)),
        CheckWaitState::Lost => Err(ServiceError::invalid_operation(
            "CI triage subcheck was withdrawn",
        )),
    }
}

async fn owner_inputs_match(
    db: &SqliteDb,
    run: &str,
    expected: &CheckEnvironmentIdentity,
) -> Result<bool> {
    Ok(db::CheckWorkerRepo::check_worker_record(db, run)
        .await?
        .receipt
        .is_some_and(|receipt| receipt.execution_inputs == *expected))
}

async fn base_sha(
    db: &SqliteDb,
    task: &db::Task,
    run: &StoredCheckRun,
    target_tip: Option<&str>,
) -> Result<Option<String>> {
    if let Some(tip) = target_tip {
        return Ok(Some(tip.to_owned()));
    }
    let Some(workspace) = run.workspace_id.as_deref() else {
        return Ok(None);
    };
    let Some(placement) = db::WorkspacePlacementRepo::get_by_workspace_id(db, workspace).await?
    else {
        return Ok(None);
    };
    let Some(path) = placement.workspace_handle else {
        return Ok(None);
    };
    let repo = RepoRepo::get_by_id(db, &run.identity.repo_id)
        .await?
        .ok_or(db::DbError::NotFound)?;
    let target = crate::merge_service::target_branch(&task.merge_config, &repo.default_branch)?;
    let target = target.strip_prefix("refs/heads/").unwrap_or(&target);
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        git::command_output(
            std::path::Path::new(&path),
            &[
                "merge-base",
                &run.identity.commit_sha,
                &format!("refs/heads/{target}"),
            ],
        ),
    )
    .await;
    Ok(match output {
        Ok(Ok(output)) if output.status.success() => String::from_utf8(output.stdout)
            .ok()
            .map(|s| s.trim().to_owned()),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn step(failures: &[&str]) -> StepResultEntry {
        serde_json::from_value(serde_json::json!({"index":0,"command":"test","exit_code":1,"stderr_tail":"","failures":failures})).unwrap()
    }
    fn command(failures: &[&str], exit: i32) -> CheckCommandOutcome {
        serde_json::from_value(serde_json::json!({"index":0,"command":"test","exit_code":exit,"stderr_tail":"","output_tail":"","started_at":"now","finished_at":"now","failures":failures})).unwrap()
    }
    fn facts() -> StepCiTriage {
        StepCiTriage {
            mode: ReviewCiTriage::Full,
            base_status: CiBaseStatus::Available,
            base_commit: None,
            rerun_run_id: None,
            base_run_id: None,
            reason: None,
            class: None,
            failures: Vec::new(),
            blocking: true,
        }
    }
    #[test]
    fn ci_triage_symmetric_difference_is_flaky_and_only_new_consistent_failure_blocks() {
        let result = classify(
            &step(&["old", "new", "flake-a"]),
            Some(&command(&["old", "new", "flake-b"], 1)),
            Some(&command(&["old"], 1)),
            facts(),
        );
        let classes: Vec<_> = result
            .failures
            .iter()
            .map(|f| (f.signature.as_str(), f.class))
            .collect();
        assert_eq!(
            classes,
            [
                ("flake-a", CiFailureClass::Flaky),
                ("flake-b", CiFailureClass::Flaky),
                ("new", CiFailureClass::Introduced),
                ("old", CiFailureClass::PreExisting)
            ]
        );
        assert!(result.blocking);
    }
    #[test]
    fn ci_triage_invalid_base_leaves_consistent_failures_unclassified() {
        let mut metadata = facts();
        metadata.base_status = CiBaseStatus::BaseRunInvalid;
        let result = classify(
            &step(&["consistent", "flake-a"]),
            Some(&command(&["consistent", "flake-b"], 1)),
            None,
            metadata,
        );
        assert!(result.blocking);
        assert!(result.class.is_none());
        assert_eq!(result.failures.len(), 2);
        assert!(result
            .failures
            .iter()
            .all(|failure| failure.class == CiFailureClass::Flaky));
    }
    #[test]
    fn ci_triage_truncated_signatures_cannot_waive_unknown_failures() {
        let mut entry = step(&["old"]);
        entry.failures_truncated = true;
        let result = classify(
            &entry,
            Some(&command(&["old"], 1)),
            Some(&command(&["old"], 1)),
            facts(),
        );
        assert!(result.blocking);
    }
    #[test]
    fn ci_triage_rerun_with_no_verdict_does_not_prove_a_flake() {
        let result = classify(&step(&["old"]), None, None, facts());
        assert!(result.blocking);
        assert_ne!(result.class, Some(CiFailureClass::Flaky));
    }
}
