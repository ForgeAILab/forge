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
    // The owner's Approve stays: it is their decision, as before the park.
    assert!(
        actions.iter().any(|offer| offer.action.verb() == "approve"),
        "{actions:?}"
    );
    assert!(
        !actions.iter().any(|offer| offer.action.verb() == "retry"),
        "a reviewer retry has no candidate: {actions:?}"
    );
    let service = crate::AttentionService::new(
        lead.db.clone(),
        std::sync::Arc::new(crate::WakeTurnConsumer::new(lead.db.clone())),
    );
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
            // The owner decided the empty review. Nothing was delivered, so
            // nothing is merged and the root finishes.
            result.unwrap();
            passes(&lead, 4).await;
            let current = reload(&lead.db, &root.id).await;
            assert_eq!(current.status, "done", "{:?}", current.condition);
            assert!(executions(&lead.db, &root.id).await.is_empty());
        } else {
            result.unwrap();
            passes(&lead, 4).await;
            let current = reload(&lead.db, &root.id).await;
            assert!(
                action == "cancel"
                    || current
                        .condition
                        .read()
                        .diagnostic
                        .as_ref()
                        .is_none_or(|annotation| annotation.blocking_reason
                            != crate::task_service::MISSING_REVIEW_CANDIDATE),
                "the park ends with the owner's decision: {:?}",
                current.condition
            );
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

/// A branch that only merged its target has no commits of its own: it is not
/// a human submission, and the review parks for the owner.
#[tokio::test]
async fn a_branch_with_only_merges_of_its_target_is_no_candidate() {
    let db = Arc::new(sqlite_db().await);
    let (repo, ws) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let (project, repo_id) = seed_project_repo(&db, repo.path()).await;
    let agent = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    let task = seed_task(&db, &project, "merged target only", "in_progress", 0).await;
    assign_role(&db, &task.id, "reviewer", &agent).await;
    let base = run_git(repo.path(), &["rev-parse", "HEAD"]);
    let target = run_git(repo.path(), &["rev-parse", "--abbrev-ref", "HEAD"]);
    let branch = workspace::task_branch_name(&task.id);
    let path = ws.path().join(&task.id).join("forge");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    git::create_worktree(repo.path(), &branch, &path)
        .await
        .unwrap();
    std::fs::write(repo.path().join("target.txt"), "target moved\n").unwrap();
    run_git(repo.path(), &["add", "-A"]);
    run_git(repo.path(), &["commit", "-m", "target moved"]);
    run_git(&path, &["merge", "--no-ff", "-m", "merge target", &target]);
    assert_ne!(run_git(&path, &["rev-parse", "HEAD"]), base);
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
            before_sha: Some(base),
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
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err(),
        "no reviewer runs without a candidate"
    );
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "review");
    assert!(
        task.condition
            .read()
            .diagnostic
            .as_ref()
            .is_some_and(|annotation| annotation.blocking_reason
                == crate::task_service::MISSING_REVIEW_CANDIDATE),
        "{:?}",
        task.condition
    );
    let runs: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM execution WHERE task_id=?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(runs, 0, "no delivery evidence is recorded");
}

/// The Task's branch is on a machine that is away: whether a human submitted
/// work is unknown, so no "nothing to review" park is stated and nothing is
/// written per pass. The review waits for the machine.
#[tokio::test]
async fn a_review_whose_machine_is_away_is_not_parked_as_empty() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    assert_eq!(placement.state, db::PlacementState::Disconnected);
    let agent = seed_agent(&db, 3, DaemonStatus::Online, AgentStatus::Idle).await;
    // The run delivered nothing: it is no candidate.
    sqlx::query("UPDATE execution SET status='failed' WHERE id=?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='review', version=version+1 WHERE id=?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    assign_role(&db, &task.id, "reviewer", &agent).await;
    let bus = Arc::new(EventBus::new(16));
    let service = Arc::new(TaskService::new(db.clone(), bus.clone()));
    let dispatcher = TaskDispatcher::new(db.clone(), bus, service);
    let mut settled = None;
    for pass in 1..=20 {
        dispatcher.reconcile_all().await.unwrap();
        dispatcher.drain_steps().await.unwrap();
        let after = (
            TaskRepo::get_by_id(&*db, &task.id, false)
                .await
                .unwrap()
                .unwrap(),
            write_counts(&db).await,
        );
        if pass == 3 {
            settled = Some(after);
        } else if pass > 3 {
            assert_eq!(Some(after), settled, "pass {pass}");
        }
    }
    let waiting = settled.unwrap().0;
    assert_eq!(waiting.status, "review");
    assert!(
        waiting
            .condition
            .read()
            .diagnostic
            .as_ref()
            .is_none_or(|annotation| annotation.blocking_reason
                != crate::task_service::MISSING_REVIEW_CANDIDATE),
        "{:?}",
        waiting.condition
    );
    assert_ne!(
        waiting.condition.read().failure_kind,
        Some(api_types::FailureKind::ReviewBlocked),
        "{:?}",
        waiting.condition
    );
    let park: Option<String> =
        sqlx::query_scalar("SELECT reason_json FROM task_schedule_park WHERE task_id=?")
            .bind(&task.id)
            .fetch_optional(db.pool())
            .await
            .unwrap();
    assert!(
        park.as_deref()
            .is_some_and(|park| park.contains(r#""owner":"Machine""#)),
        "the review waits for its machine: {park:?}"
    );
}
