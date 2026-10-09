//! Certification happens here, never from a successful transport alone.
use crate::{Result, ServiceError};
use api_types::*;
use db::{
    CheckCleanup, CheckDispatchIntent, CheckResultEvidence, CheckResultOutcome, StoredCheckRun,
};

pub(super) fn validate_receipt(
    run: &StoredCheckRun,
    intent: &CheckDispatchIntent,
    receipt: &CheckReceipt,
) -> Result<CheckResultEvidence> {
    let refuse =
        || ServiceError::invalid_operation("check receipt does not certify the admitted inputs");
    if receipt.operation_id != run.operation_id
        || receipt.owner != intent.owner
        || receipt.execution_inputs != run.identity.inputs.environment_identity
    {
        return Err(refuse());
    }
    let started =
        chrono::DateTime::parse_from_rfc3339(&receipt.started_at).map_err(|_| refuse())?;
    let finished =
        chrono::DateTime::parse_from_rfc3339(&receipt.finished_at).map_err(|_| refuse())?;
    if finished < started || receipt.commands.len() > run.identity.inputs.spec.commands.len() {
        return Err(refuse());
    }
    let legacy = matches!(
        run.identity.inputs.spec.execution_policy.as_str(),
        "legacy-server/1" | "legacy-daemon/1"
    );
    let mut previous = started;
    let mut commands = Vec::new();
    for (index, command) in receipt.commands.iter().enumerate() {
        let expected = &run.identity.inputs.spec.commands[index];
        let from =
            chrono::DateTime::parse_from_rfc3339(&command.started_at).map_err(|_| refuse())?;
        let to =
            chrono::DateTime::parse_from_rfc3339(&command.finished_at).map_err(|_| refuse())?;
        if command.id != expected.id
            || command.command != expected.shell_text
            || from < previous
            || to < from
            || to > finished
            || (!legacy && !command.process_tree_stopped)
        {
            return Err(refuse());
        }
        if (command.outcome == CheckExecutionOutcome::Passed && command.exit_code != Some(0))
            || (command.outcome == CheckExecutionOutcome::Failed
                && command.exit_code.is_none_or(|code| code == 0))
        {
            return Err(refuse());
        }
        if index + 1 < receipt.commands.len()
            && command.outcome != CheckExecutionOutcome::Passed
            && expected.failure_policy == CheckFailurePolicy::StopBundle
        {
            return Err(refuse());
        }
        previous = to;
        commands.push(CheckCommandOutcome {
            index,
            command: command.command.clone(),
            exit_code: command.exit_code.unwrap_or(-1),
            stderr_tail: command.stderr_tail.clone(),
            output_tail: command.stdout_tail.clone(),
            started_at: command.started_at.clone(),
            finished_at: command.finished_at.clone(),
        });
    }
    if receipt.outcome == CheckExecutionOutcome::Passed {
        if receipt.commands.len() != run.identity.inputs.spec.commands.len()
            || receipt
                .commands
                .iter()
                .any(|c| c.outcome != CheckExecutionOutcome::Passed || c.exit_code != Some(0))
        {
            return Err(refuse());
        }
        // Legacy policies deliberately take no adjacent Git witness. They can
        // produce uncached mechanical evidence, never an unwitnessed cache pass.
        if (!legacy || run.cacheable)
            && (receipt.prepared_head.as_deref() != Some(&run.identity.commit_sha)
                || receipt.finished_head != receipt.prepared_head
                || receipt.tracked_changes != Some(false))
        {
            return Err(refuse());
        }
    }
    let cleanup = match receipt.cleanup.outcome {
        CheckCleanupOutcome::NotPerformed => CheckCleanup::NotPerformed,
        CheckCleanupOutcome::Success => CheckCleanup::Success,
        CheckCleanupOutcome::Failed
        | CheckCleanupOutcome::TimedOut
        | CheckCleanupOutcome::Uncertain => CheckCleanup::Failed,
    };
    if run.identity.inputs.spec.declares_cleanup
        && cleanup == CheckCleanup::Success
        && (receipt.cleanup.commands.is_empty()
            || receipt.cleanup.commands.iter().any(|c| {
                c.outcome != CheckExecutionOutcome::Passed
                    || c.exit_code != Some(0)
                    || !c.process_tree_stopped && !legacy
            }))
    {
        return Err(refuse());
    }
    let outcome = match receipt.outcome {
        CheckExecutionOutcome::Passed => CheckResultOutcome::Pass,
        CheckExecutionOutcome::Failed => CheckResultOutcome::Fail,
        CheckExecutionOutcome::TimedOut => CheckResultOutcome::TimedOut,
        CheckExecutionOutcome::Cancelled => CheckResultOutcome::Cancelled,
        CheckExecutionOutcome::Infrastructure => CheckResultOutcome::InfrastructureFailed,
    };
    Ok(CheckResultEvidence {
        outcome,
        cleanup,
        commands,
        output_truncated: receipt
            .commands
            .iter()
            .chain(&receipt.cleanup.commands)
            .any(|c| {
                c.stdout_truncated
                    || c.stderr_truncated
                    || c.stdout_drain_incomplete
                    || c.stderr_drain_incomplete
            }),
        redaction_values: vec![],
    })
}
