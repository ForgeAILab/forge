//! Select the next Project action from overview state.

use super::*;

macro_rules! project_action {
    (
        $code:expr,
        $required_principal:expr,
        $target_type:expr,
        $target_id:expr,
        $title:expr,
        $explanation:expr,
        $action_kind:expr,
        $route_or_operation:expr,
        $blocking:expr,
        $expected_version:expr $(,)?
    ) => {
        ProjectNextAction {
            code: $code.to_owned(),
            required_principal: $required_principal.to_owned(),
            target_type: $target_type.to_owned(),
            target_id: ($target_id).into(),
            title: $title.to_owned(),
            explanation: $explanation.to_owned(),
            action_kind: $action_kind.to_owned(),
            route_or_operation: $route_or_operation.to_owned(),
            blocking: $blocking,
            expected_version: $expected_version,
        }
    };
}

pub(super) fn next_action(context: NextActionContext<'_>) -> Option<ProjectNextAction> {
    let NextActionContext {
        project_id,
        project_version,
        charter_setup_required,
        no_milestones,
        execution_setup,
        milestones,
        documents,
        releases,
        pending_decision_ids,
        task_counts,
        failed_task_count,
        checks,
        reconciliation_required,
        stale,
    } = context;
    // A released milestone has no outstanding work. It stays in the overview's
    // milestone list — it can still be the Project's primary milestone, which
    // is how it reaches this projection at all — but it must never produce a
    // blocking next action.
    let unreleased: Vec<Value> = milestones
        .iter()
        .filter(|milestone| {
            milestone
                .get("milestone")
                .and_then(|milestone| milestone.get("lifecycle"))
                .and_then(Value::as_str)
                != Some("released")
        })
        .cloned()
        .collect();
    let milestones: &[Value] = &unreleased;
    // This order is part of the public projection contract. More specific
    // blockers must never be hidden behind a generic stale banner.
    if charter_setup_required {
        return Some(project_action!(
            "charter_adoption",
            "user",
            "project",
            project_id,
            "Adopt the Project Charter",
            "An approved Charter is required before Project work can be governed.",
            "approval",
            "project.charter.adoption",
            true,
            Some(project_version),
        ));
    }
    if reconciliation_required {
        return Some(project_action!(
            "reconciliation_required",
            "user",
            "project",
            project_id,
            "Resolve the Project reconciliation",
            "A canonical conflict is waiting for an explicit user resolution.",
            "reconciliation",
            "project.reconciliation.resolve",
            true,
            Some(project_version),
        ));
    }
    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone
            .get("milestone")
            .and_then(|value| value.get("projection_reasons"))
            .and_then(Value::as_array)
            .is_some_and(|reasons| {
                reasons.iter().any(|reason| {
                    matches!(
                        reason.get("kind").and_then(Value::as_str),
                        Some("reconciliation") | Some("conflict")
                    )
                })
            })
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "milestone_reconciliation",
            "user",
            "milestone",
            milestone_id,
            "Reconcile the milestone",
            "The current milestone projection contains an unresolved canonical conflict.",
            "reconciliation",
            "project.milestone.revision.save",
            true,
            version,
        ));
    }
    if documents.iter().any(|document| {
        matches!(
            document.get("status").and_then(Value::as_str),
            Some("stale") | Some("reconciliation_required") | Some("unavailable")
        )
    }) {
        return Some(project_action!(
            "document_reconciliation",
            "project_agent",
            "project",
            project_id,
            "Reconcile Project documents",
            "A governing Document pointer or revision is incomplete and cannot be used as Project truth.",
            "reconciliation",
            "project.document.reconcile",
            true,
            Some(project_version),
        ));
    }

    if execution_setup_requires_action(execution_setup) {
        let (code, title, explanation, operation) = match execution_setup.execution_setup_state {
            api_types::ExecutionSetupState::Provisioning => (
                "execution_setup_provisioning",
                "Finish execution setup",
                "Repository and execution principals are still being provisioned.",
                "project.execution_setup.retry_provisioning",
            ),
            api_types::ExecutionSetupState::Failed => (
                "execution_setup_failed",
                "Repair execution setup",
                "Execution setup failed and needs explicit configuration or retry.",
                "project.execution_setup.retry_provisioning",
            ),
            api_types::ExecutionSetupState::Unavailable => (
                "execution_setup_unavailable",
                "Refresh execution setup",
                "The authoritative execution setup projection is unavailable.",
                "project.execution_setup.refresh",
            ),
            _ => (
                "execution_setup_required",
                "Complete execution setup",
                "Select the execution roles and attach a repository before running Tasks.",
                "project.execution_setup",
            ),
        };
        return Some(project_action!(
            code,
            "user",
            "project",
            project_id,
            title,
            explanation,
            "setup",
            operation,
            true,
            Some(execution_setup.project_version),
        ));
    }

    match execution_setup.execution_gate {
        api_types::ExecutionGate::BaselineApprovalRequired
        | api_types::ExecutionGate::PreBaselineReadOnly
        | api_types::ExecutionGate::Active => {}
        api_types::ExecutionGate::ReconciliationRequired
        | api_types::ExecutionGate::Unavailable => {
            // Same underlying `project_reconciliation_record` source as the
            // Project-wide `reconciliation_required` branch above -- this
            // branch is reached only when that projection disagrees with the
            // execution-setup projection, so it points at the same real,
            // registered resolve operation rather than a second invented one.
            return Some(project_action!(
                "execution_gate_reconciliation",
                "user",
                "project",
                project_id,
                "Reconcile the execution gate",
                "Forge could not verify the current Charter-backed execution state.",
                "reconciliation",
                "project.reconciliation.resolve",
                true,
                Some(execution_setup.project_version),
            ));
        }
    }

    // A milestone whose current definition revision is still `draft` or
    // `proposed` has no approved contract; validation, evidence, and
    // readiness would all be measured against a definition nobody approved.
    // The user's approval is the blocker there, not the Worker's evidence, and
    // the stale banner alone gave the user nothing to act on.
    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone
            .get("definition")
            .and_then(|definition| definition.get("lifecycle"))
            .and_then(Value::as_str)
            .is_some_and(|lifecycle| lifecycle != "approved")
    }) {
        let revision_id = milestone.get("definition")?.get("id")?.as_str()?;
        let version = milestone
            .get("milestone")?
            .get("version")
            .and_then(Value::as_i64);
        return Some(project_action!(
            "milestone_definition_approval",
            "user",
            "milestone_revision",
            revision_id,
            "Approve the milestone definition revision",
            "The milestone's current definition revision is not approved. Validation, evidence, and readiness are measured against an unapproved contract until a user approves or supersedes it.",
            "approval",
            "project.milestone.revision.transition",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones
        .iter()
        .find(|milestone| readiness_requires_contract_reconciliation(milestone))
    {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "acceptance_contract_reconciliation",
            "project_agent",
            "milestone",
            milestone_id,
            "Reconcile the acceptance and evidence contract",
            "The active baseline and current milestone use different stable check or evidence identities. Revise the milestone and propose one exact replacement baseline before collecting release inputs.",
            "reconciliation",
            "project.milestone.revision.save",
            true,
            version,
        ));
    }

    if value_i64(task_counts, "blocked") > 0 || failed_task_count > 0 {
        return Some(project_action!(
            if value_i64(task_counts, "blocked") > 0 {
                "task_blocked_remediation"
            } else {
                "task_failure_remediation"
            },
            "project_agent",
            "project",
            project_id,
            if value_i64(task_counts, "blocked") > 0 {
                "Unblock blocked Tasks"
            } else {
                "Remediate failed Tasks"
            },
            if value_i64(task_counts, "blocked") > 0 {
                "One or more Tasks are blocked and need an explicit dependency or scope remediation."
            } else {
                "One or more Tasks have failed and must be repaired or rerun before validation."
            },
            "remediation",
            "task.remediate",
            true,
            None,
        ));
    }
    if let Some((_, check)) = manual_check_requiring_user(milestones) {
        let check_id = check.get("id")?.as_str()?;
        let version = check.get("version").and_then(Value::as_i64);
        let latest = check.get("latest_result").and_then(Value::as_str);
        return Some(project_action!(
            "manual_attestation_required",
            "user",
            "milestone_check",
            check_id,
            if latest.is_some() {
                "Review the manual acceptance result"
            } else {
                "Record a manual acceptance result"
            },
            "This check requires an explicit user Pass or Fail result. Attestation does not replace its separate evidence requirement.",
            "attestation",
            "project.milestone.check.record",
            true,
            version,
        ));
    }
    if value_i64(checks, "failed") > 0 {
        return Some(project_action!(
            "validation_failure_remediation",
            "worker",
            "project",
            project_id,
            "Resolve failed validation",
            "A required acceptance check failed and needs a new authoritative result.",
            "validation",
            "project.milestone.check.evaluate",
            true,
            None,
        ));
    }
    if value_i64(checks, "stale") > 0
        || value_i64(checks, "missing") > 0
        || value_i64(checks, "unavailable") > 0
    {
        return Some(project_action!(
            "validation_required",
            "worker",
            "project",
            project_id,
            "Complete required validation",
            "Required acceptance checks are missing, stale, or unavailable.",
            "validation",
            "project.milestone.check.evaluate",
            true,
            None,
        ));
    }
    if !pending_decision_ids.is_empty() {
        return Some(project_action!(
            "decision_resolution",
            "user",
            "decision",
            pending_decision_ids[0].clone(),
            "Resolve the open decision",
            "A draft or proposed decision is still awaiting explicit resolution.",
            "reconciliation",
            "project.decision.resolve",
            true,
            None,
        ));
    }
    if let Some(milestone) = milestones
        .iter()
        .find(|milestone| evidence_requires_attention(milestone))
    {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "evidence_required",
            "worker",
            "milestone",
            milestone_id,
            "Attach required evidence",
            "The milestone's evidence contract is not satisfied by available authoritative attachments.",
            "evidence",
            "project.milestone.evidence.attach",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone.get("latest_readiness").is_none_or(Value::is_null)
            || milestone
                .get("readiness_freshness")
                .and_then(|freshness| freshness.get("status"))
                .and_then(Value::as_str)
                .is_some_and(|status| status != "current")
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "readiness_request",
            "project_agent",
            "milestone",
            milestone_id,
            "Request a readiness snapshot",
            "Validation and evidence are present; compute a fresh immutable readiness snapshot.",
            "readiness",
            "project.milestone.readiness",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones.iter().find(|milestone| {
        milestone
            .get("latest_readiness")
            .and_then(|snapshot| snapshot.get("result"))
            .and_then(Value::as_str)
            .is_some_and(|result| result != "ready")
            && milestone
                .get("readiness_freshness")
                .and_then(|freshness| freshness.get("status"))
                .and_then(Value::as_str)
                == Some("current")
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        let version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "readiness_blocked",
            "project_agent",
            "milestone",
            milestone_id,
            "Address readiness blockers",
            "The latest authoritative readiness snapshot is not ready for release; resolve its typed reasons and evaluate again.",
            "readiness",
            "project.milestone.readiness",
            true,
            version,
        ));
    }

    if let Some(milestone) = milestones.iter().find(|milestone| {
        let Some(snapshot) = milestone.get("latest_readiness") else {
            return false;
        };
        snapshot.get("result").and_then(Value::as_str) == Some("ready")
            && milestone
                .get("readiness_freshness")
                .and_then(|freshness| freshness.get("status"))
                .and_then(Value::as_str)
                == Some("current")
            && milestone_release_missing(milestone, releases)
    }) {
        let milestone = milestone.get("milestone")?;
        let milestone_id = milestone.get("id")?.as_str()?;
        // Release CAS is against the current mutable milestone row. The
        // immutable readiness candidate's expected version is one step behind
        // after readiness evaluation advances the lifecycle/version.
        let expected_version = milestone.get("version").and_then(Value::as_i64);
        return Some(project_action!(
            "user_release",
            "user",
            "milestone",
            milestone_id,
            "Release the ready milestone",
            "The latest readiness snapshot is ready for the exact user-authorized release operation.",
            "release",
            "project.milestone.release",
            true,
            expected_version,
        ));
    }

    if no_milestones {
        return Some(project_action!(
            "milestone_definition",
            "project_agent",
            "project",
            project_id,
            "Define the next bounded milestone",
            "No active milestone is available; define the next bounded outcome and acceptance contract.",
            "planning",
            "project.milestone.create",
            true,
            Some(project_version),
        ));
    }
    if stale {
        return Some(project_action!(
            "projection_reconciliation",
            "project_agent",
            "project",
            project_id,
            "Reconcile stale Project records",
            "The Overview contains records that no longer prove a current authoritative projection.",
            "reconciliation",
            "project.projection.reconcile",
            true,
            Some(project_version),
        ));
    }
    None
}

fn manual_check_requiring_user(milestones: &[Value]) -> Option<(&Value, &Value)> {
    milestones.iter().find_map(|milestone| {
        milestone
            .get("current_checks")?
            .as_array()?
            .iter()
            .find(|check| {
                check.get("required").and_then(Value::as_bool) == Some(true)
                    && check.get("source_kind").and_then(Value::as_str) == Some("manual")
                    && !matches!(
                        check.get("latest_result").and_then(Value::as_str),
                        Some("pass") | Some("waived")
                    )
            })
            .map(|check| (milestone, check))
    })
}

fn readiness_requires_contract_reconciliation(milestone: &Value) -> bool {
    milestone
        .pointer("/latest_readiness/reasons")
        .and_then(Value::as_array)
        .is_some_and(|reasons| {
            reasons.iter().any(|reason| {
                reason
                    .get("code")
                    .and_then(Value::as_str)
                    .is_some_and(|code| code.contains("reconciliation_required"))
            })
        })
}

fn value_i64(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or_default()
}

fn execution_setup_requires_action(setup: &api_types::ProjectExecutionSetupResponse) -> bool {
    setup.coordination_state != api_types::CoordinationState::Ready
        || !matches!(
            setup.execution_setup_state,
            api_types::ExecutionSetupState::Ready
        )
        || !setup.setup_requirements.is_empty()
}

fn evidence_requires_attention(milestone: &Value) -> bool {
    let Some(definition) = milestone.get("definition") else {
        return false;
    };
    let requirements = definition
        .pointer("/content/evidence_requirements")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if requirements.is_empty() {
        return false;
    }
    let evidence = milestone
        .get("evidence")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    if evidence.is_empty() {
        return true;
    }
    let available = evidence
        .iter()
        .filter(|item| item.get("availability").and_then(Value::as_str) == Some("available"));
    let available = available.count();
    let required_count = requirements
        .iter()
        .filter(|requirement| {
            requirement
                .get("required")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        })
        .count();
    // The definition and attachment schemas can evolve independently; when a
    // requirement has no explicit id, count-based availability is the only
    // safe projection and still fails closed.
    available < required_count.max(1)
}

fn milestone_release_missing(milestone: &Value, releases: &[Value]) -> bool {
    let milestone_id = milestone
        .get("milestone")
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str);
    milestone_id.is_some_and(|id| {
        !releases
            .iter()
            .any(|release| release.get("milestone_id").and_then(Value::as_str) == Some(id))
    })
}
