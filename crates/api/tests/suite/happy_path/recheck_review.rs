use super::*;

async fn wait_for_review(harness: &TestHarness, task_id: &str, attempt: usize) -> TaskResponse {
    let mut last = String::new();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let task = common::drain(&harness.state, &harness.app, task_id).await;
            let reviews = ReviewRepo::list_by_task(&*harness.state.db, task_id)
                .await
                .unwrap();
            let executions = db::ExecutionRepo::list_by_task_and_role(
                &*harness.state.db,
                task_id,
                "reviewer",
                db::PageRequest {
                    cursor: None,
                    limit: 10,
                    include_total: false,
                    sort_by: db::SortBy::CreatedAt,
                    sort_order: db::SortOrder::Asc,
                },
            )
            .await
            .unwrap();
            last = format!(
                "status={}, condition={:?}, reviews={:?}, executions={:?}",
                task.status,
                task.condition,
                reviews
                    .iter()
                    .map(|review| (review.attempt_number, &review.status))
                    .collect::<Vec<_>>(),
                executions
                    .items
                    .iter()
                    .map(|execution| (&execution.id, &execution.status, &execution.error))
                    .collect::<Vec<_>>()
            );
            if reviews.len() == attempt
                && reviews.iter().any(|review| {
                    review.attempt_number == attempt as i64
                        && review.status == db::ReviewStatus::AwaitingHuman
                })
                && executions.items.len() == attempt
                && executions
                    .items
                    .iter()
                    .all(|execution| execution.status == db::ExecutionStatus::Completed)
            {
                return task;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("review attempt {attempt} must dispatch and finish its reviewer: {last}")
    })
}

async fn recheck_review(gate: &str, checks: bool, agent_requested: bool) {
    let repo_dir = common::TestDir::new("recheck-repo");
    let repo_path = setup_git_repo(repo_dir.path()).await;
    let workspaces = common::TestDir::new("recheck-workspaces");
    let harness = test_app(workspaces.path()).await;
    let (project, agent) = repair_release_project(&harness, &repo_path, workspaces.path()).await;
    let mut workflow = services::workflow::default_workflow::default_workflow();
    workflow.configuration.clear();
    let ci_steps = if checks {
        vec!["test -f greeting.txt"]
    } else {
        vec![]
    };
    for state in &mut workflow.states {
        for trigger in state.triggers.values_mut() {
            if trigger.to == "review" {
                trigger.to = gate.into();
            }
        }
        if state.name == "review" {
            state.name = gate.into();
            state.config = json!({"ci_steps": ci_steps});
            state.gate_config.as_mut().unwrap().requires_user_approval = Some(true);
        }
    }
    let _: api_types::WorkflowDefinition = json_request(
        &harness.app,
        Method::PUT,
        &format!("/api/v1/projects/{}/workflow", project.id),
        json!({"definition": workflow}),
        StatusCode::OK,
    )
    .await;
    let review_prompt =
        "test -f greeting.txt && printf '%s\\n' '{\"result\":\"pass\",\"reason\":\"reviewed\"}'";
    let task: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({
            "title": "Recheck reviewer dispatch",
            "description": "printf 'hello\\n' > greeting.txt && git add greeting.txt && git commit -m greeting",
            "review_config": {
                "review_prompt": review_prompt,
                "ci_steps": ci_steps
            }
        }),
        StatusCode::OK,
    )
    .await;
    let _: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/claim", task.id),
        json!({"agent_id": agent.id}),
        StatusCode::OK,
    )
    .await;
    let dispatcher = Arc::new(services::TaskDispatcher::with_check_interval(
        harness.state.db.clone(),
        harness.event_bus.clone(),
        harness.state.task_service.clone(),
        Duration::from_millis(25),
    ));
    let dispatch_loop = dispatcher.clone().start();
    let first = wait_for_review(&harness, &task.id, 1).await;
    assert_eq!(first.status, gate);
    if agent_requested {
        harness
            .state
            .task_service
            .rerun_review(
                uuid::Uuid::parse_str(&task.id).unwrap(),
                api_types::Actor::agent(&agent.id),
            )
            .await
            .unwrap();
    } else {
        let _: TaskResponse = json_request(
            &harness.app,
            Method::POST,
            &format!("/api/v1/tasks/{}/actions", task.id),
            json!({"version": first.version, "action": {"verb": "retry"}}),
            StatusCode::OK,
        )
        .await;
    }
    let second = wait_for_review(&harness, &task.id, 2).await;
    dispatcher.stop();
    dispatch_loop.await.unwrap();
    assert_eq!(second.status, gate);
    assert!(
        second.available_actions.iter().any(|offer| {
            matches!(
                offer.action,
                api_types::TaskAction::Approve {
                    override_checks: Some(false),
                    ..
                }
            )
        }),
        "the new review offers ordinary approval: {:?}",
        second.available_actions
    );
    let mut reviews = ReviewRepo::list_by_task(&*harness.state.db, &task.id)
        .await
        .unwrap();
    reviews.sort_by_key(|review| review.attempt_number);
    assert_eq!(reviews[0].status, db::ReviewStatus::Cancelled);
    assert_eq!(reviews[1].status, db::ReviewStatus::AwaitingHuman);
    assert_eq!(reviews[0].execution_id, reviews[1].execution_id);
    assert_ne!(
        reviews[0].reviewer_execution_id,
        reviews[1].reviewer_execution_id
    );
    let details: Value = serde_json::from_str(&reviews[1].step_results_json).unwrap();
    if checks {
        assert_eq!(details["ci_steps"][0]["exit_code"], 0);
        for review in &reviews {
            let details: Value = serde_json::from_str(&review.step_results_json).unwrap();
            let reviewer = db::ExecutionRepo::get_by_id(
                &*harness.state.db,
                review.reviewer_execution_id.as_deref().unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
            let checked = chrono::DateTime::parse_from_rfc3339(
                details["ci_steps"][0]["finished_at"].as_str().unwrap(),
            )
            .unwrap();
            let dispatched = chrono::DateTime::parse_from_rfc3339(&reviewer.created_at).unwrap();
            assert!(
                checked <= dispatched,
                "checks finish before reviewer dispatch"
            );
        }
    } else {
        assert_eq!(details["ci_steps"], json!([]));
    }
}

#[tokio::test]
async fn owner_recheck_no_checks_builtin_review() {
    recheck_review("review", false, false).await;
}

#[tokio::test]
async fn owner_recheck_no_checks_renamed_review() {
    recheck_review("qa_review", false, false).await;
}

#[tokio::test]
async fn owner_recheck_with_checks_builtin_review() {
    recheck_review("review", true, false).await;
}

#[tokio::test]
async fn owner_recheck_with_checks_renamed_review() {
    recheck_review("qa_review", true, false).await;
}

#[tokio::test]
async fn agent_recheck_no_checks_builtin_review() {
    recheck_review("review", false, true).await;
}

#[tokio::test]
async fn agent_recheck_no_checks_renamed_review() {
    recheck_review("qa_review", false, true).await;
}

#[tokio::test]
async fn agent_recheck_with_checks_builtin_review() {
    recheck_review("review", true, true).await;
}

#[tokio::test]
async fn agent_recheck_with_checks_renamed_review() {
    recheck_review("qa_review", true, true).await;
}
