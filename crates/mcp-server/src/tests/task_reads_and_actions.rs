//! MCP task offers, integration queue reads and review actions.

use super::*;

#[test]
fn task_actions_mcp_reads_share_offers_and_ignore_stored_lists() {
    run_async(async {
        let state = sqlite_state().await;
        let _agent = seed_agent(&state, "action-agent").await;
        for condition in ["failed_review", "stored_annotation", "blocked_budget"] {
            let task = seed_task(&state).await;
            let execution_id = seed_execution(&state, task.id.clone()).await;
            sqlx::query("UPDATE execution SET status = 'completed' WHERE id = ?")
                .bind(&execution_id)
                .execute(state.db.pool())
                .await
                .unwrap();
            let status = if condition == "failed_review" {
                "review"
            } else {
                "in_progress"
            };
            sqlx::query("UPDATE task SET status = ? WHERE id = ?")
                .bind(status)
                .bind(&task.id)
                .execute(state.db.pool())
                .await
                .unwrap();
            if condition == "failed_review" {
                let now = now_rfc3339();
                db::ReviewRepo::create(
                    &*state.db,
                    db::CreateReview {
                        id: new_uuid_v4(),
                        task_id: task.id.clone(),
                        execution_id,
                        attempt_number: 1,
                        status: db::ReviewStatus::Failed,
                        step_results_json: json!({"ci_steps":[{"command":"check","exit_code":1}]})
                            .to_string(),
                        started_at: now.clone(),
                        created_at: now.clone(),
                        updated_at: now,
                    },
                )
                .await
                .unwrap();
            } else if condition == "stored_annotation" {
                sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
                    .bind(json!({"type":"executor_failed","blocking_reason":"fixture","recovery_actions":["return_to_implementation","retry_pr_publication"]}).to_string())
                    .bind(&task.id).execute(state.db.pool()).await.unwrap();
            } else {
                sqlx::query("UPDATE task SET blocked_json = ? WHERE id = ?")
                    .bind(json!({"kind":"retry_exhausted","reason":"spent","recovery_actions":["return_to_implementation"]}).to_string())
                    .bind(&task.id).execute(state.db.pool()).await.unwrap();
            }
            state
                .db
                .check_task_conditions_of(std::slice::from_ref(&task.id))
                .await
                .unwrap();
            let offers = state
                .task_service
                .task_action_offers(
                    &task.id,
                    &api_types::Actor::user(api_types::UserActionSource::Api),
                )
                .await
                .unwrap();
            let result = call_tool(&state, "forge_get_task", json!({"task_id":task.id})).await;
            assert_eq!(result["available_actions"], json!(offers.available_actions));
            assert!(result.get("execution_actions").is_none());
            assert_eq!(
                result["workflow_exception"]["actions"],
                result["available_actions"]
            );
            assert!(result.get("error_annotation").is_none());
            assert!(result.get("blocked").is_none());
            assert!(result.get("failed").is_none());
            assert!(result["condition"]["details"]["diagnostic"]
                .get("recovery_actions")
                .is_none());
            assert_eq!(result["version"], offers.version);
        }
    });
}

#[test]
fn task_lists_omit_offers_but_single_task_reads_keep_them() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let root = seed_task_in_project(&state, project_id.clone()).await;
        let listed = call_tool(&state, "forge_list_tasks", json!({"project_id":project_id})).await;
        for task in listed["items"].as_array().unwrap() {
            assert!(task.get("available_actions").is_none());
        }
        let created = call_tool(
            &state,
            "forge_create_sub_tasks",
            json!({"parent_task_id":root.id, "subtasks":[{"title":"Child"}]}),
        )
        .await;
        for task in created["subtasks"].as_array().unwrap() {
            assert!(task.get("available_actions").is_none());
        }
        let children = call_tool(
            &state,
            "forge_list_sub_tasks",
            json!({"parent_task_id":root.id}),
        )
        .await;
        for task in children["subtasks"].as_array().unwrap() {
            assert!(task.get("available_actions").is_none());
        }
        let single = call_tool(&state, "forge_get_task", json!({"task_id":root.id})).await;
        assert!(single["available_actions"].is_array());
    });
}

#[test]
fn task_reads_carry_the_merge_queue_summary() {
    use db::IntegrationQueueRepo;
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, repo_id) = seed_project_repo(&state).await;
        let head = seed_task_in_project(&state, project_id.clone()).await;
        let waiter = seed_task_in_project(&state, project_id.clone()).await;
        let outside = seed_task_in_project(&state, project_id.clone()).await;
        let queue = state
            .db
            .create_or_get_integration_queue(&repo_id, "main")
            .await
            .unwrap();
        let mut attempts = Vec::new();
        for task in [&head, &waiter] {
            attempts.push(
                state
                    .db
                    .admit_integration_attempt(db::IntegrationAttempt::new(
                        Some(queue.id.clone()),
                        task.id.clone(),
                        project_id.clone(),
                        format!("admission-{}", task.id),
                        "merging".into(),
                        0,
                        1,
                    ))
                    .await
                    .unwrap(),
            );
        }
        // What the queue worker writes when it takes the head and rebases.
        sqlx::query("UPDATE integration_queue SET state='open',last_error_kind=NULL,last_error=NULL,head_attempt_id=? WHERE id=?")
            .bind(&attempts[0].id)
            .bind(&queue.id)
            .execute(state.db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE integration_attempt SET state='rebasing',started_at=? WHERE id=?")
            .bind(now_rfc3339())
            .bind(&attempts[0].id)
            .execute(state.db.pool())
            .await
            .unwrap();

        let single = call_tool(&state, "forge_get_task", json!({"task_id":head.id})).await;
        assert_eq!(single["integration"]["attempt_id"], attempts[0].id);
        assert_eq!(single["integration"]["target_branch"], "main");
        assert_eq!(single["integration"]["state"], "owned");
        assert_eq!(single["integration"]["phase"], "rebasing");
        assert_eq!(single["integration"]["position"], 0);
        let single = call_tool(&state, "forge_get_task", json!({"task_id":outside.id})).await;
        assert_eq!(single["integration"], Value::Null);

        let listed = call_tool(&state, "forge_list_tasks", json!({"project_id":project_id})).await;
        let item = |id: &str| {
            listed["items"]
                .as_array()
                .unwrap()
                .iter()
                .find(|task| task["id"] == id)
                .unwrap()
                .clone()
        };
        assert_eq!(item(&head.id)["integration"]["state"], "owned");
        assert_eq!(item(&waiter.id)["integration"]["state"], "waiting");
        assert_eq!(item(&waiter.id)["integration"]["position"], 1);
        assert_eq!(item(&waiter.id)["integration"]["wait"]["code"], "queue");
        assert_eq!(item(&outside.id)["integration"], Value::Null);
    });
}

#[test]
fn forge_get_integration_queue_reads_the_projects_own_queue_and_nothing_of_another() {
    use db::IntegrationQueueRepo;
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, repo_id) = seed_project_repo(&state).await;
        sqlx::query("UPDATE project SET primary_repo_id=? WHERE id=?")
            .bind(&repo_id)
            .bind(&project_id)
            .execute(state.db.pool())
            .await
            .unwrap();
        let empty = call_tool(
            &state,
            "forge_get_integration_queue",
            json!({"project_id":project_id}),
        )
        .await;
        assert_eq!(empty["repo_id"], repo_id);
        assert_eq!(empty["items"], json!([]));
        let head = seed_task_in_project(&state, project_id.clone()).await;
        let waiter = seed_task_in_project(&state, project_id.clone()).await;
        let mut queues = Vec::new();
        for branch in ["main", "release"] {
            queues.push(
                state
                    .db
                    .create_or_get_integration_queue(&repo_id, branch)
                    .await
                    .unwrap(),
            );
        }
        for task in [&head, &waiter] {
            state
                .db
                .admit_integration_attempt(db::IntegrationAttempt::new(
                    Some(queues[0].id.clone()),
                    task.id.clone(),
                    project_id.clone(),
                    format!("admission-{}", task.id),
                    "merging".into(),
                    0,
                    1,
                ))
                .await
                .unwrap();
        }
        let arguments = json!({"project_id":project_id});
        let read = call_tool(&state, "forge_get_integration_queue", arguments.clone()).await;
        assert_eq!(read["project_id"], project_id);
        assert_eq!(read["has_more"], false);
        let items = read["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["target_branch"], "main");
        assert_eq!(items[0]["member_count"], 2);
        let members = items[0]["members"].as_array().unwrap();
        assert_eq!(members[0]["task_id"], head.id);
        assert_eq!(members[1]["task_id"], waiter.id);
        assert_eq!(members[1]["integration"]["position"], 1);
        // The same arguments as the native read: one branch, a page size.
        let one = call_tool(
            &state,
            "forge_get_integration_queue",
            json!({"project_id":project_id,"target_branch":"release"}),
        )
        .await;
        assert_eq!(one["items"].as_array().unwrap().len(), 1);
        assert_eq!(one["items"][0]["target_branch"], "release");
        let paged = call_tool(
            &state,
            "forge_get_integration_queue",
            json!({"project_id":project_id,"limit":1}),
        )
        .await;
        assert_eq!(paged["items"].as_array().unwrap().len(), 1);
        assert_eq!(paged["has_more"], true);
        // A Project-constrained session reads its own Project without naming
        // it, and may not name another.
        let scoped = call_tool_scoped(
            &state,
            &project_id,
            "forge_get_integration_queue",
            json!({}),
        )
        .await;
        assert_eq!(scoped, read);
        let (other_project, _) = seed_project_repo(&state).await;
        let error = dispatch_with_context(
            &state,
            &McpContext {
                project_id: Some(other_project.clone()),
                user_id: Some("mcp-test-user".to_owned()),
            },
            "tools/call",
            json!({"name":"forge_get_integration_queue","arguments":arguments}),
        )
        .await
        .expect_err("another Project's queue is outside the session");
        assert_eq!(error.code, -32602);
        // A Project that names another Project's repository as its checkout
        // learns how long that line is, and nothing of who is in it.
        sqlx::query("UPDATE project SET primary_repo_id=? WHERE id=?")
            .bind(&repo_id)
            .bind(&other_project)
            .execute(state.db.pool())
            .await
            .unwrap();
        let foreign = dispatch(
            &state,
            "tools/call",
            json!({"name":"forge_get_integration_queue","arguments":{"project_id":other_project}}),
        )
        .await
        .expect("tool succeeds");
        let text = foreign["content"][0]["text"].as_str().unwrap();
        assert!(
            !text.contains(&head.id) && !text.contains(&waiter.id),
            "{text}"
        );
        let foreign: Value = serde_json::from_str(text).unwrap();
        assert_eq!(foreign["items"][0]["member_count"], 2);
        assert_eq!(foreign["items"][0]["members"], json!([]));
        assert_eq!(foreign["items"][0]["head_task_id"], Value::Null);
    });
}

#[test]
fn paused_task_action_error_preserves_wait_cause_and_turn_scope() {
    for cause in [
        api_types::DeniedBy::TargetAgentPaused,
        api_types::DeniedBy::ProjectPaused("environment_not_ready".to_owned()),
    ] {
        let error: crate::error::McpToolError = services::ServiceError::TaskActionUnavailable {
            available_actions: vec![],
            reason: "retry unavailable".to_owned(),
            wait_cause: Some(cause.clone()),
        }
        .into();
        assert_eq!(error.code, -32010);
        let response = error.into_response(json!(1));
        let data = response.error.unwrap().data.unwrap();
        assert_eq!(data["code"], "action_unavailable");
        assert_eq!(data["denied_by"], cause.to_string());
        assert_eq!(data["retry"]["scope"], "turn");
        let response = McpToolError::from(services::ServiceError::TaskActionUnavailable {
            available_actions: vec![],
            reason: "retry unavailable".to_owned(),
            wait_cause: Some(cause.clone()),
        })
        .into_tool_response(json!(2));
        let outcome = &response.result.unwrap()["structuredContent"];
        assert_eq!(outcome["code"], "action_unavailable");
        assert_eq!(outcome["denied_by"], cause.to_string());
        assert_eq!(outcome["retry"]["scope"], "turn");
        assert_eq!(outcome["retry"]["action"], "none");
    }
}

/// MCP credentials cannot be told apart from an agent's, so an MCP-hosted
/// Project Agent's send-back spends the review budget like an agent's would;
/// the owner's REST send-back of the same Task spends nothing.
#[test]
fn mcp_send_back_spends_the_review_budget() {
    run_async(async {
        let state = sqlite_state().await;
        let (project_id, _) = seed_project_repo(&state).await;
        let task = seed_task_in_project(&state, project_id).await;
        sqlx::query("UPDATE task SET status = 'in_progress' WHERE id = ?")
            .bind(&task.id)
            .execute(state.db.pool())
            .await
            .unwrap();
        let now = now_rfc3339();
        TaskRoleAssignmentRepo::assign(
            &*state.db,
            CreateTaskRoleAssignment {
                id: new_uuid_v4(),
                task_id: task.id.clone(),
                role_name: "reviewer".to_owned(),
                assignee_type: Some(AssigneeKind::User),
                assignee_id: Some("human".to_owned()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        let version = TaskRepo::get_by_id(&*state.db, &task.id, false)
            .await
            .unwrap()
            .unwrap()
            .version;
        state
            .task_service
            .transition(
                task.id.clone(),
                "review".to_owned(),
                (version, Some("ready for review".to_owned())),
            )
            .await
            .unwrap();
        let in_review = state.task_service.drain(&task.id).await.unwrap();
        assert_eq!(in_review.status, "review");
        let spent = |state: &AppState, task_id: String| {
            let pool = state.db.pool().clone();
            async move {
                db::budget::spent(&pool, &task_id, db::budget::Kind::Review.key())
                    .await
                    .unwrap()
            }
        };
        assert_eq!(spent(&state, task.id.clone()).await, 0);

        let sent_back = call_tool(
            &state,
            "forge_task_action",
            json!({
                "task_id": task.id,
                "action": {"verb": "send_back", "guidance": "failed CI"},
                "version": in_review.version,
            }),
        )
        .await;
        assert_eq!(sent_back["status"], "in_progress");
        assert_eq!(
            spent(&state, task.id.clone()).await,
            1,
            "an MCP send-back is charged like an agent's"
        );
    });
}
