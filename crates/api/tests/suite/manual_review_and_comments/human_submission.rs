//! Owner review actions and checks for human branch submissions.

use super::*;

#[tokio::test]
async fn review_without_candidate_owner_actions_work_over_http() {
    for verb in ["approve", "approve_override", "send_back", "cancel"] {
        let (harness, task_id, _dirs) =
            human_submission_fixture(&format!("forge-review-no-candidate-{verb}"), false, false)
                .await;
        let uri = format!("/api/v1/tasks/{task_id}/actions");
        let _: TaskResponse = json_request(&harness.app, Method::POST, &uri,
            json!({"version":common::task_action_version(&harness.app,&uri).await,
                "action":{"verb":"approve","override":true,"reason":"Submit human work for review"}}), StatusCode::OK).await;
        let parked = common::drain(&harness.state, &harness.app, &task_id).await;
        assert_eq!(parked.status, "review");
        assert!(
            parked
                .condition
                .details()
                .diagnostic
                .as_ref()
                .is_some_and(|annotation| annotation.blocking_reason == "review_candidate_missing"),
            "{:?}",
            parked.condition
        );
        assert!(ReviewRepo::list_by_task(&*harness.state.db, &task_id)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            count(
                &harness.state.db,
                "SELECT COUNT(*) FROM check_consumer WHERE task_id=?",
                &task_id
            )
            .await,
            0,
            "missing work is no check verdict"
        );
        let action = match verb {
            "send_back" => {
                json!({"verb":"send_back","guidance":"Implement and commit the missing work"})
            }
            "cancel" => json!({"verb":"cancel"}),
            _ => {
                json!({"verb":"approve","override":verb == "approve_override","reason":"Owner decides the empty review"})
            }
        };
        let result: serde_json::Value = json_request(
            &harness.app,
            Method::POST,
            &uri,
            json!({"version":common::task_action_version(&harness.app,&uri).await,"action":action}),
            StatusCode::OK,
        )
        .await;
        // The owner's Approve is a deliberate decision and keeps working: it
        // passes the gate into merging and the park is gone.
        assert_eq!(
            result["status"],
            match verb {
                "cancel" => "cancelled",
                "send_back" => "in_progress",
                _ => "merging",
            },
            "{verb}: {result}"
        );
        let after = common::drain(&harness.state, &harness.app, &task_id).await;
        assert!(
            !after
                .condition
                .details()
                .diagnostic
                .as_ref()
                .is_some_and(|annotation| annotation.blocking_reason == "review_candidate_missing"),
            "{verb}: {:?}",
            after.condition
        );
    }
}

#[tokio::test]
async fn review_of_human_branch_submission_runs_checks_and_reviewer_before_merge() {
    let (harness, task_id, _dirs) =
        human_submission_fixture("forge-human-review-candidate", true, true).await;
    let uri = format!("/api/v1/tasks/{task_id}/actions");
    let _: TaskResponse = json_request(&harness.app, Method::POST, &uri,
        json!({"version":common::task_action_version(&harness.app,&uri).await,
            "action":{"verb":"approve","override":true,"reason":"Submit human commits for review"}}), StatusCode::OK).await;
    let mut settled = common::drain(&harness.state, &harness.app, &task_id).await;
    for _ in 0..300 {
        if settled.status == "done" {
            break;
        }
        harness
            .state
            .task_dispatcher
            .as_ref()
            .unwrap()
            .check_once()
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        settled = common::drain(&harness.state, &harness.app, &task_id).await;
    }
    assert_eq!(settled.status, "done", "{:?}", settled.condition);
    let reviews = ReviewRepo::list_by_task(&*harness.state.db, &task_id)
        .await
        .unwrap();
    assert_eq!(reviews.len(), 1);
    assert_eq!(reviews[0].status, ReviewStatus::Passed);
    let details: serde_json::Value = serde_json::from_str(&reviews[0].step_results_json).unwrap();
    assert_eq!(details["ci_steps"][0]["exit_code"], 0, "{details}");
    assert_eq!(details["conformance"]["status"], "passed", "{details}");
    assert_eq!(
        count(
            &harness.state.db,
            "SELECT COUNT(*) FROM check_run WHERE ? IS NOT NULL",
            &task_id
        )
        .await,
        1
    );
    let candidate = ExecutionRepo::get_by_id(&*harness.state.db, &reviews[0].execution_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        candidate.agent_id.is_none()
            && candidate.lease_owner.is_none()
            && candidate.logs_path.is_none()
    );
    assert!(candidate
        .summary
        .unwrap()
        .contains("human branch submission"));
    let reviewed = db::ReviewConformanceRepo::review_contract(
        &*harness.state.db,
        reviews[0].reviewer_execution_id.as_deref().unwrap(),
    )
    .await
    .unwrap();
    let reviewed = reviewed.unwrap();
    assert_ne!(reviewed.commit_sha, reviewed.base_sha);
    assert_eq!(
        candidate.after_sha.as_deref(),
        Some(reviewed.commit_sha.as_str())
    );
    assert_eq!(count(&harness.state.db, "SELECT COUNT(*) FROM execution WHERE task_id=? AND agent_id IS NOT NULL AND role='coder'", &task_id).await, 0);
    assert_eq!(count(&harness.state.db, "SELECT COUNT(*) FROM execution WHERE task_id=? AND role='reviewer' AND status='completed'", &task_id).await, 1);
}
