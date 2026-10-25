//! Persisted CI check evidence and exact review-attempt binding.

use super::*;

pub(super) async fn completed_ci_check_results(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
    context: &ReviewGoverningContext,
) -> Result<Vec<ConformanceCheckResult>, String> {
    let execution = ExecutionRepo::get_by_id(db, execution_id)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| format!("review execution {execution_id} does not exist"))?;
    let Some(review) = review_bound_to_execution(db, task_id, execution_id).await? else {
        if matches!(execution.role.as_str(), "reviewer" | "auditor")
            || context.required_checks.iter().any(|check| {
                check
                    .id
                    .strip_prefix("ci:")
                    .is_some_and(|index| index.parse::<usize>().is_ok())
            })
        {
            return Err(format!(
                "no review attempt is bound to execution {execution_id}"
            ));
        }
        return Ok(Vec::new());
    };
    let details: Value = serde_json::from_str(&review.step_results_json)
        .map_err(|error| format!("invalid stored review check results: {error}"))?;
    let stored = parse_stored_ci_steps(details)?;
    let mut results = Vec::with_capacity(stored.len());
    let mut triage_signature_budget = 20;
    let mut seen_indices = BTreeSet::new();
    for result in stored {
        let index = result.index;
        if !seen_indices.insert(index) {
            return Err(format!(
                "stored review check results contain duplicate ci step index {index}"
            ));
        }
        let check_id = format!("ci:{index}");
        let Some(required) = context
            .required_checks
            .iter()
            .find(|check| check.id == check_id)
        else {
            return Err(format!(
                "stored result for {check_id} does not match a required check"
            ));
        };
        if result.command != required.command {
            return Err(format!(
                "stored result for {check_id} does not match the required command"
            ));
        }
        let output = if result.output_tail.is_empty() {
            result.stderr_tail.clone()
        } else {
            result.output_tail.clone()
        };
        results.push(ConformanceCheckResult {
            triage: compact_ci_triage(result.triage, &mut triage_signature_budget),
            check_id,
            command: result.command,
            exit_code: result.exit_code,
            output,
        });
    }
    results.sort_by(|left, right| left.check_id.cmp(&right.check_id));
    if let Some(missing) = context.required_checks.iter().find(|check| {
        check
            .id
            .strip_prefix("ci:")
            .is_some_and(|index| index.parse::<usize>().is_ok())
            && !results.iter().any(|result| result.check_id == check.id)
    }) {
        return Err(format!(
            "pre-review result for required check {} is unavailable",
            missing.id
        ));
    }
    Ok(results)
}

/// Frozen reviewer input is a bounded view. Full classifications remain on
/// the Review/ledger; settlement only needs each exact check's blocking flag.
fn compact_ci_triage(
    mut triage: Option<StepCiTriage>,
    remaining: &mut usize,
) -> Option<StepCiTriage> {
    if let Some(triage) = &mut triage {
        let total = triage.failures.len();
        let keep = total.min(*remaining);
        *remaining -= keep;
        if keep < total {
            let reason = triage.reason.get_or_insert_with(String::new);
            if !reason.is_empty() {
                reason.push_str("; ");
            }
            reason.push_str(&format!("Showing {keep} of {total} classified signatures; full evidence remains on the Review"));
            triage.failures.truncate(keep);
        }
    }
    triage
}

/// Resolve the Review attempt that supplied pre-review checks for this
/// reviewer/auditor execution.  Attempt identity is explicit: the current
/// execution must be the Review's reviewer/auditor binding, or an exact direct
/// legacy `Review.execution_id` binding. Candidate parentage and timestamps
/// are not attempt identity and are deliberately ignored.
async fn review_bound_to_execution(
    db: &SqliteDb,
    task_id: &str,
    execution_id: &str,
) -> Result<Option<Review>, String> {
    let execution = ExecutionRepo::get_by_id(db, execution_id)
        .await
        .map_err(|error| error.to_string())?;
    let Some(execution) = execution else {
        return Ok(None);
    };
    if execution.task_id != task_id {
        return Err(format!(
            "execution {execution_id} does not belong to Task {task_id}"
        ));
    }
    let reviews = ReviewRepo::list_by_task(db, task_id)
        .await
        .map_err(|error| error.to_string())?;
    let bound = reviews
        .iter()
        .filter(|review| exact_review_binding_matches(&execution, review))
        .collect::<Vec<_>>();
    match bound.as_slice() {
        [] => Ok(None),
        [review] => Ok(Some((*review).clone())),
        _ => Err(format!(
            "execution {execution_id} has multiple exact Review attempt bindings"
        )),
    }
}

/// An exact Review-attempt identity relation. `Review.execution_id` is kept as
/// a direct legacy binding because historical rows used that shape; current
/// reviewer/auditor rows use their role-specific execution binding. A shared
/// candidate parent is intentionally not considered here because several
/// Review attempts may review the same candidate execution.
fn exact_review_binding_matches(execution: &Execution, review: &Review) -> bool {
    if review.reviewer_execution_id.is_some() || review.auditor_execution_id.is_some() {
        review.reviewer_execution_id.as_deref() == Some(execution.id.as_str())
            || review.auditor_execution_id.as_deref() == Some(execution.id.as_str())
    } else {
        review.execution_id == execution.id
    }
}

fn parse_stored_ci_steps(details: Value) -> Result<Vec<StepResultEntry>, String> {
    match details {
        Value::Array(steps) => serde_json::from_value(Value::Array(steps))
            .map_err(|error| format!("invalid stored review check results: {error}")),
        Value::Object(details) => {
            validate_persisted_review_detail_keys(&details)?;
            let details: ReviewDetails = serde_json::from_value(Value::Object(details))
                .map_err(|error| format!("invalid stored review details: {error}"))?;
            Ok(details.ci_steps)
        }
        value => Err(format!(
            "invalid stored review check results: expected an object or step-result array, got {value}"
        )),
    }
}

const PERSISTED_REVIEW_DETAIL_KEYS: &[&str] = &[
    "ci_steps",
    "verify",
    "conformance",
    "auditor",
    "user_approval",
    "execution",
    "execution_retry",
];

fn validate_persisted_review_detail_keys(
    details: &serde_json::Map<String, Value>,
) -> Result<(), String> {
    for key in details.keys() {
        if !PERSISTED_REVIEW_DETAIL_KEYS.contains(&key.as_str()) {
            return Err(format!("unknown persisted review detail field: {key}"));
        }
    }
    if let Some(ci_steps) = details.get("ci_steps") {
        if !ci_steps.is_array() {
            return Err("persisted review ci_steps must be an array".into());
        }
    }
    for key in ["user_approval", "execution", "execution_retry", "verify"] {
        if let Some(value) = details.get(key) {
            if !value.is_object() {
                return Err(format!("persisted review {key} must be an object"));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
