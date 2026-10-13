//! Review needs delivery evidence, including human commits, or an actionable park.
use super::root_lead::{executions, passes, reload, until};
use super::*;

fn owner() -> Actor {
    Actor::user(api_types::UserActionSource::Api)
}

async fn write_counts(db: &db::SqliteDb) -> (i64, i64, i64, i64, i64) {
    sqlx::query_as("SELECT (SELECT COUNT(*) FROM task_step), (SELECT COUNT(*) FROM domain_event), (SELECT SUM(version) FROM task), (SELECT COUNT(*) FROM execution), (SELECT COALESCE(SUM(version),0) FROM attention_projection)").fetch_one(db.pool()).await.unwrap()
}

async fn offers(lead: &root_lead::Lead, id: &str) -> Vec<api_types::Offer> {
    lead.dispatcher
        .task_service
        .task_action_offers(id, &owner())
        .await
        .unwrap()
        .available_actions
}

async fn assert_missing_candidate(lead: &root_lead::Lead, id: &str) {
    passes(lead, 4).await;
    let task = reload(&lead.db, id).await;
    assert_eq!(task.status, "review");
    assert!(
        matches!(
            task.condition.reasons().next(),
            Some(db::ParkReason::Failure {
                failure_kind: api_types::FailureKind::ReviewBlocked
            })
        ),
        "{:?}",
        task.condition
    );
    assert!(task
        .condition
        .read()
        .interruption
        .unwrap()
        .reason
        .contains("nothing to review"));
    let actions = offers(lead, id).await;
    assert!(
        actions
            .iter()
            .any(|offer| offer.action.verb() == "send_back"),
        "{actions:?}"
    );
    assert!(actions.iter().any(|offer| offer.action.verb() == "cancel"));
    assert!(
        !actions
            .iter()
            .any(|offer| matches!(offer.action.verb(), "approve" | "retry")),
        "{actions:?}"
    );
    let service = crate::AttentionService::new(lead.db.clone());
    service.project_once(1000).await.unwrap();
    let attention = service
        .mission_control_home("test-user", None, 50)
        .await
        .unwrap();
    assert!(
        attention
            .needs_attention
            .iter()
            .any(|item| item.details["entity_id"] == id
                && item.category == api_types::AttentionCategory::ExecutionFailed),
        "{attention:?}"
    );
    let observe = || async { (reload(&lead.db, id).await, write_counts(&lead.db).await) };
    let before = observe().await;
    passes(lead, 20).await;
    assert_eq!(
        observe().await,
        before,
        "twenty unchanged passes write nothing"
    );
}

#[tokio::test]
async fn all_cancelled_root_without_a_lead_parks_and_owner_actions_work() {
    for action in ["approve", "send_back", "cancel"] {
        let lead = root_lead::lead_fixture(false).await;
        let root = seed_task(&lead.db, &lead.project, "all cancelled", "in_progress", 0).await;
        seed_subtask(&lead.db, &root, "cancelled", "cancelled", 0).await;
        assign_role(&lead.db, &root.id, "reviewer", &lead.coder).await;
        assert_missing_candidate(&lead, &root.id).await;
        let task = reload(&lead.db, &root.id).await;
        let command = match action {
            "approve" => api_types::TaskAction::Approve {
                reason: None,
                override_checks: Some(false),
            },
            "send_back" => api_types::TaskAction::SendBack {
                guidance: "Add the missing work".into(),
            },
            _ => api_types::TaskAction::Cancel { reason: None },
        };
        let result = lead
            .dispatcher
            .task_service
            .perform_task_action_as(&root.id, command, task.version, owner())
            .await;
        if action == "approve" {
            assert!(result.is_err(), "missing validation cannot be approved");
            assert_eq!(reload(&lead.db, &root.id).await, task);
        } else {
            result.unwrap();
            passes(&lead, 4).await;
            let current = reload(&lead.db, &root.id).await;
            assert_eq!(
                current.status,
                if action == "cancel" {
                    "cancelled"
                } else {
                    "in_progress"
                }
            );
            if action == "send_back" {
                assert!(current
                    .condition
                    .reasons()
                    .any(|reason| matches!(reason, db::ParkReason::Held { .. })));
                let children = lead
                    .dispatcher
                    .task_service
                    .create_subtasks(
                        current.id.clone(),
                        vec![crate::task_service::NewSubtaskInput {
                            title: "corrective work".into(),
                            description: None,
                            assignee_id: Some(lead.coder.clone()),
                        }],
                    )
                    .await
                    .unwrap();
                assert_eq!(children.len(), 1);
                passes(&lead, 2).await;
                assert!(reload(&lead.db, &children[0].id).await.condition.reasons().any(|reason| matches!(reason, db::ParkReason::Parent { cause, .. } if cause == "held")));
            }
        }
    }
}

#[tokio::test]
async fn all_cancelled_root_with_a_lead_replans_within_its_existing_budget() {
    let lead = root_lead::lead_fixture(true).await;
    let root = seed_task(&lead.db, &lead.project, "all cancelled", "in_progress", 0).await;
    seed_subtask(&lead.db, &root, "cancelled", "cancelled", 0).await;
    assign_role(&lead.db, &root.id, "planner", &lead.planner).await;
    assign_role(&lead.db, &root.id, "reviewer", &lead.coder).await;
    until(
        &lead,
        "the missing-work re-plan budget parks the root",
        async || {
            reload(&lead.db, &root.id)
                .await
                .condition
                .reasons()
                .any(|reason| {
                    matches!(
                        reason,
                        db::ParkReason::BudgetExhausted {
                            budget_kind: Some(db::budget::Kind::Replan),
                            ..
                        }
                    )
                })
        },
    )
    .await;
    let task = reload(&lead.db, &root.id).await;
    assert_eq!(task.status, "review");
    let spent = db::budget::spent(lead.db.pool(), &root.id, "replan")
        .await
        .unwrap();
    assert_eq!(spent, i64::from(db::budget::Kind::Replan.default_limit()));
    assert!(ReviewRepo::list_by_task(&*lead.db, &root.id)
        .await
        .unwrap()
        .is_empty());
    assert!(executions(&lead.db, &root.id)
        .await
        .iter()
        .all(|(role, _)| role == "planner"));
    let before = task;
    let counts = write_counts(&lead.db).await;
    passes(&lead, 20).await;
    assert_eq!(reload(&lead.db, &root.id).await, before);
    assert_eq!(write_counts(&lead.db).await, counts);
}

#[tokio::test]
async fn all_cancelled_unconfigured_root_finishes_without_a_merge() {
    let lead = root_lead::lead_fixture(false).await;
    let root = seed_task(
        &lead.db,
        &lead.project,
        "empty unconfigured root",
        "in_progress",
        0,
    )
    .await;
    seed_subtask(&lead.db, &root, "cancelled", "cancelled", 0).await;
    passes(&lead, 4).await;
    assert_eq!(reload(&lead.db, &root.id).await.status, "done");
    assert!(ReviewRepo::list_by_task(&*lead.db, &root.id)
        .await
        .unwrap()
        .is_empty());
    assert!(executions(&lead.db, &root.id).await.is_empty());
    assert!(super::root_replan::log(&lead, &root.id)
        .await
        .iter()
        .any(|(from, to, _)| from == "review" && to == "merging"));
}

#[tokio::test]
async fn an_old_failed_review_cannot_pay_for_new_empty_entries_or_supply_a_cancelled_candidate() {
    let lead = root_lead::lead_fixture(true).await;
    let root = seed_task(
        &lead.db,
        &lead.project,
        "cancelled after old review",
        "in_progress",
        0,
    )
    .await;
    let child = seed_subtask(&lead.db, &root, "cancelled", "cancelled", 0).await;
    let candidate = seed_completed_coder_execution(&lead.db, &child.id).await;
    let now = now_rfc3339();
    ReviewRepo::create(
        &*lead.db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            execution_id: candidate,
            attempt_number: 1,
            status: ReviewStatus::Failed,
            step_results_json: serde_json::json!({"ci_steps":[]}).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    assign_role(&lead.db, &root.id, "planner", &lead.planner).await;
    assign_role(&lead.db, &root.id, "reviewer", &lead.coder).await;
    until(
        &lead,
        "empty entries spend the re-plan budget despite an older failed review",
        async || {
            reload(&lead.db, &root.id)
                .await
                .condition
                .reasons()
                .any(|reason| {
                    matches!(
                        reason,
                        db::ParkReason::BudgetExhausted {
                            budget_kind: Some(db::budget::Kind::Replan),
                            ..
                        }
                    )
                })
        },
    )
    .await;
    assert_eq!(
        db::budget::spent(lead.db.pool(), &root.id, "replan")
            .await
            .unwrap(),
        i64::from(db::budget::Kind::Replan.default_limit())
    );
    assert_eq!(
        ReviewRepo::list_by_task(&*lead.db, &root.id)
            .await
            .unwrap()
            .len(),
        1
    );
    let actions = offers(&lead, &root.id).await;
    assert!(
        !actions.iter().any(|offer| offer.action.verb() == "approve"),
        "a cancelled child is no current candidate: {actions:?}"
    );
    assert!(actions
        .iter()
        .any(|offer| offer.action.verb() == "send_back"));
    let counts = write_counts(&lead.db).await;
    passes(&lead, 20).await;
    assert_eq!(write_counts(&lead.db).await, counts);
}

#[tokio::test]
async fn a_direct_move_to_merging_cannot_pass_a_missing_configured_review() {
    let lead = root_lead::lead_fixture(false).await;
    let root = seed_task(
        &lead.db,
        &lead.project,
        "configured empty root",
        "review",
        0,
    )
    .await;
    seed_subtask(&lead.db, &root, "cancelled", "cancelled", 0).await;
    assign_role(&lead.db, &root.id, "reviewer", &lead.coder).await;
    let root = reload(&lead.db, &root.id).await;
    let _ = lead
        .dispatcher
        .task_service
        .transition(
            root.id.clone(),
            "merging".into(),
            crate::task_service::TransitionOptions {
                triggered_by: owner(),
                ..root.version.into()
            },
        )
        .await;
    passes(&lead, 4).await;
    assert_ne!(reload(&lead.db, &root.id).await.status, "done");
    assert!(ReviewRepo::list_by_task(&*lead.db, &root.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn owner_move_into_review_without_a_run_parks() {
    let lead = root_lead::lead_fixture(false).await;
    let task = seed_task(&lead.db, &lead.project, "owner moved", "in_progress", 0).await;
    assign_role(&lead.db, &task.id, "reviewer", &lead.coder).await;
    let task = reload(&lead.db, &task.id).await;
    lead.dispatcher
        .task_service
        .transition(
            task.id.clone(),
            "review".to_owned(),
            crate::task_service::TransitionOptions {
                version: task.version,
                triggered_by: owner(),
                ..task.version.into()
            },
        )
        .await
        .unwrap();
    assert_missing_candidate(&lead, &task.id).await;
}

#[tokio::test]
async fn owner_approval_into_review_without_a_run_parks() {
    let lead = root_lead::lead_fixture(false).await;
    let task = seed_task(&lead.db, &lead.project, "owner approved", "in_progress", 0).await;
    assign_role(&lead.db, &task.id, "reviewer", &lead.coder).await;
    let task = reload(&lead.db, &task.id).await;
    lead.dispatcher
        .task_service
        .perform_task_action_as(
            &task.id,
            api_types::TaskAction::Approve {
                reason: Some("Submit human work for validation".into()),
                override_checks: Some(true),
            },
            task.version,
            owner(),
        )
        .await
        .unwrap();
    assert_missing_candidate(&lead, &task.id).await;
}

#[tokio::test]
async fn a_cached_missing_candidate_refusal_recovers_to_an_owner_park() {
    let lead = root_lead::lead_fixture(false).await;
    let task = seed_task(&lead.db, &lead.project, "cached review", "review", 0).await;
    assign_role(&lead.db, &task.id, "reviewer", &lead.coder).await;
    let task = reload(&lead.db, &task.id).await;
    crate::deferred_dispatch::record_dispatch_disposition(
        &lead.db,
        &task,
        "review",
        "no implementation execution to review",
    )
    .await
    .unwrap();
    assert_missing_candidate(&lead, &task.id).await;
    assert!(crate::deferred_dispatch::current_dispatch_disposition(
        &reload(&lead.db, &task.id).await
    )
    .is_none());
}

#[tokio::test]
async fn human_branch_commits_are_reviewed_without_an_agent_implementation_run() {
    let db = Arc::new(sqlite_db().await);
    let (repo, ws) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, repo_id) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "human work", "in_progress", 0).await;
    assign_role(&db, &task.id, "reviewer", &agent).await;
    let base = run_git(repo.path(), &["rev-parse", "HEAD"]);
    let branch = workspace::task_branch_name(&task.id);
    let path = ws.path().join(&task.id).join("forge");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    git::create_worktree(repo.path(), &branch, &path)
        .await
        .unwrap();
    std::fs::write(path.join("human.txt"), "human work\n").unwrap();
    run_git(&path, &["add", "-A"]);
    run_git(&path, &["commit", "-m", "human work"]);
    let head = run_git(&path, &["rev-parse", "HEAD"]);
    let now = now_rfc3339();
    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: new_uuid_v4(),
            task_id: task.id.clone(),
            repo_id,
            worktree_path: path.to_string_lossy().into_owned(),
            branch,
            status: WorkspaceStatus::Ready,
            before_sha: Some(base.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    let (dispatcher, mut rx) = build_dispatcher(db.clone(), ws.path()).await;
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    dispatcher
        .task_service
        .transition(
            task.id.clone(),
            "review".to_owned(),
            crate::task_service::TransitionOptions {
                version: task.version,
                triggered_by: owner(),
                ..task.version.into()
            },
        )
        .await
        .unwrap();
    for _ in 0..4 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    let ctx = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the reviewer is dispatched")
        .unwrap();
    let reviewer = ExecutionRepo::get_by_id(&*db, &ctx.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reviewer.role, "reviewer");
    let reviews = ReviewRepo::list_by_task(&*db, &task.id).await.unwrap();
    assert_eq!(reviews.len(), 1);
    let candidate = ExecutionRepo::get_by_id(&*db, &reviews[0].execution_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        candidate.agent_id.is_none() && candidate.lease_owner.is_none(),
        "no implementation agent or process ran"
    );
    assert_eq!(candidate.before_sha.as_deref(), Some(base.as_str()));
    assert_eq!(candidate.after_sha.as_deref(), Some(head.as_str()));
    assert!(candidate.summary.unwrap().contains("human"));
}
