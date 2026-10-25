use super::*;
use serde_json::json;

#[test]
fn ci_triage_reviewer_input_bounds_signature_lists_without_changing_routing() {
    let triage = StepCiTriage {
        mode: ReviewCiTriage::Full,
        base_status: CiBaseStatus::Available,
        base_commit: None,
        rerun_run_id: None,
        base_run_id: None,
        reason: None,
        class: None,
        blocking: false,
        failures: (0..200)
            .map(|index| CiFailureTriage {
                signature: format!("test-{index}"),
                class: CiFailureClass::PreExisting,
            })
            .collect(),
    };
    let mut budget = 20;
    let first = compact_ci_triage(Some(triage.clone()), &mut budget).unwrap();
    let second = compact_ci_triage(Some(triage.clone()), &mut budget).unwrap();
    assert_eq!(first.failures.len(), 20);
    assert!(second.failures.is_empty());
    assert!(!first.blocking && !second.blocking);
    assert_eq!(
        triage.failures.len(),
        200,
        "source evidence remains complete"
    );
    assert!(first.reason.unwrap().contains("20 of 200"));
    assert!(second.reason.unwrap().contains("0 of 200"));
}
#[test]
fn persisted_ci_step_details_are_typed_and_fail_closed() {
    let step = json!({
        "index": 0,
        "command": "cargo test",
        "exit_code": 0,
        "stderr_tail": "",
        "output_tail": "ok"
    });
    assert_eq!(
        parse_stored_ci_steps(json!([step.clone()])).unwrap().len(),
        1
    );
    assert_eq!(
        parse_stored_ci_steps(json!({"ci_steps": [step]}))
            .unwrap()
            .len(),
        1
    );

    for malformed in [
        json!(null),
        json!("not-review-details"),
        json!({"garbage": 1}),
        json!({"ci_steps": "not-an-array"}),
        json!({"ci_steps": [{"index": 0, "command": "cargo test"}]}),
    ] {
        assert!(
            parse_stored_ci_steps(malformed).is_err(),
            "malformed persisted review details must not become an empty check set"
        );
    }
}

fn lineage_execution(id: &str, role: &str) -> Execution {
    Execution {
        id: id.to_owned(),
        task_id: "task".to_owned(),
        agent_id: None,
        role: role.to_owned(),
        status: db::ExecutionStatus::Completed,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: Some("candidate".to_owned()),
        agent_session_id: None,
        agent_message_id: None,
        last_activity_at: None,
        prompt: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: None,
        execution_version: 1,
        lease_owner: None,
        lease_expires_at: None,
        hard_deadline_at: None,
        last_heartbeat_at: None,
        last_progress_at: None,
        created_at: "now".to_owned(),
        updated_at: "now".to_owned(),
    }
}

fn lineage_review(
    id: &str,
    attempt_number: i64,
    reviewer_execution_id: Option<&str>,
    auditor_execution_id: Option<&str>,
) -> Review {
    Review {
        id: id.to_owned(),
        task_id: "task".to_owned(),
        execution_id: "candidate".to_owned(),
        reviewer_execution_id: reviewer_execution_id.map(str::to_owned),
        auditor_execution_id: auditor_execution_id.map(str::to_owned),
        attempt_number,
        status: db::ReviewStatus::Running,
        step_results_json: "[]".to_owned(),
        started_at: "now".to_owned(),
        finished_at: None,
        created_at: "now".to_owned(),
        updated_at: "now".to_owned(),
    }
}

#[test]
fn review_binding_is_attempt_exact_and_retry_replacement_rejects_old_execution() {
    let old_reviewer = lineage_execution("reviewer-old", "reviewer");
    let new_reviewer = lineage_execution("reviewer-new", "reviewer");
    let auditor = lineage_execution("auditor", "auditor");
    let first = lineage_review("review-1", 1, Some(&old_reviewer.id), None);
    let second = lineage_review("review-2", 2, Some(&new_reviewer.id), None);

    assert!(exact_review_binding_matches(&old_reviewer, &first));
    assert!(!exact_review_binding_matches(&old_reviewer, &second));
    assert!(exact_review_binding_matches(&new_reviewer, &second));

    let auditor_review = lineage_review("review-auditor", 3, None, Some(&auditor.id));
    assert!(exact_review_binding_matches(&auditor, &auditor_review));

    let replaced = Review {
        id: "review-replaced".to_owned(),
        task_id: "task".to_owned(),
        // Even if a legacy-shaped row happens to name the old reviewer in
        // execution_id, an explicit replacement binding is authoritative.
        execution_id: old_reviewer.id.clone(),
        reviewer_execution_id: Some(new_reviewer.id.clone()),
        auditor_execution_id: None,
        attempt_number: 4,
        status: db::ReviewStatus::Running,
        step_results_json: "[]".to_owned(),
        started_at: "now".to_owned(),
        finished_at: None,
        created_at: "now".to_owned(),
        updated_at: "now".to_owned(),
    };
    assert!(!exact_review_binding_matches(&old_reviewer, &replaced));

    let unbound = lineage_review("review-unbound", 5, None, None);
    assert!(!exact_review_binding_matches(&old_reviewer, &unbound));
    let legacy_direct = Review {
        execution_id: old_reviewer.id.clone(),
        ..unbound
    };
    assert!(exact_review_binding_matches(&old_reviewer, &legacy_direct));
}
