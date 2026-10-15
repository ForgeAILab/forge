use super::*;

async fn enter(f: &FailedCiFixture, actor: api_types::Actor) -> db::Task {
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = f
        .engine
        .workflow_execution()
        .transition_with_authority(
            &task.id,
            "review",
            task.version,
            &f.workflow,
            &actor,
            "coder completed repair",
            false,
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    drain_result(f.engine.clone(), result).await.task
}
fn workflow_actor() -> api_types::Actor {
    api_types::Actor::system(api_types::SystemComponent::Workflow)
}
async fn reviews(f: &FailedCiFixture) -> Vec<db::Review> {
    db::ReviewRepo::list_by_task(&*f.db, &f.task.id)
        .await
        .unwrap()
}
async fn check_runs(f: &FailedCiFixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(f.db.pool())
        .await
        .unwrap()
}
async fn spending(f: &FailedCiFixture) -> (i64, i64, i64) {
    let mut counts = Vec::new();
    for key in ["review", "gate:review", "repair_no_change"] {
        counts.push(
            db::budget::spent(f.db.pool(), &f.task.id, key)
                .await
                .unwrap(),
        );
    }
    (counts[0], counts[1], counts[2])
}
async fn repair(f: &FailedCiFixture) {
    let now = now_rfc3339();
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    let head = git::get_current_sha(&path).await.unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,before_sha,after_sha,workspace_id,created_at,updated_at) VALUES(?,?,'coder','completed',?,?,?,?,?)")
        .bind(new_uuid_v4()).bind(&f.task.id).bind(&head).bind(&head).bind(&f.workspace.id).bind(&now).bind(&now)
        .execute(f.db.pool()).await.unwrap();
}
async fn red_fixture() -> FailedCiFixture {
    let f = failed_ci_fixture(3, FailurePolicy::Block).await;
    assert_eq!(enter(&f, workflow_actor()).await.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(spending(&f).await, (1, 1, 0));
    f
}

#[tokio::test]
async fn no_change_once_sends_back_without_review_check_or_review_budget() {
    let f = red_fixture().await;
    repair(&f).await;
    let before_runs = check_runs(&f).await;
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(check_runs(&f).await, before_runs);
    assert_eq!(spending(&f).await, (1, 1, 1));
    let logs = TransitionLogRepo::list_by_task(&*f.db, &f.task.id)
        .await
        .unwrap();
    let bounce = logs
        .iter()
        .rev()
        .find(|entry| entry.from_state == "review")
        .unwrap();
    assert!(bounce
        .trigger_reason
        .starts_with("No change since review 1\n"));
    assert!(!bounce.rejection);
    assert_eq!(
        bounce.bridge.bridge_payload.as_ref().unwrap()["verb"],
        "repair_no_change"
    );
}

#[tokio::test]
async fn no_change_twice_parks_without_another_review_or_check() {
    let f = red_fixture().await;
    repair(&f).await;
    assert_eq!(enter(&f, workflow_actor()).await.status, "in_progress");
    repair(&f).await;
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "review");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(check_runs(&f).await, 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["type"], "review_needs_owner");
    assert_eq!(annotation["message"], "no change after repair round");
    let blocked: serde_json::Value =
        serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(blocked["kind"], "review_needs_owner");
    assert_eq!(blocked["reason"], "no change after repair round");
}

#[tokio::test]
async fn no_change_owner_rereview_of_same_commit_runs() {
    let f = red_fixture().await;
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    repair(&f).await;
    assert_eq!(enter(&f, workflow_actor()).await.status, "review");
    let before = spending(&f).await;
    f.engine
        .rerun_review(
            uuid::Uuid::parse_str(&f.task.id).unwrap(),
            api_types::Actor::user(api_types::UserActionSource::Api),
        )
        .await
        .unwrap();
    drain(f.engine.clone(), &f.task.id).await;
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await, before);
}

#[tokio::test]
async fn no_change_changed_commit_reviews_normally() {
    let f = red_fixture().await;
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    std::fs::write(path.join("repair.txt"), "repair").unwrap();
    run_git(&path, &["add", "repair.txt"]);
    run_git(
        &path,
        &[
            "-c",
            "user.name=Forge Test",
            "-c",
            "user.email=test@forge.dev",
            "commit",
            "-m",
            "repair",
        ],
    );
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await, (2, 2, 0));
}

#[tokio::test]
async fn no_change_dirty_worktree_is_not_rejected_by_guard() {
    let f = red_fixture().await;
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    std::fs::write(path.join("repair.txt"), "uncommitted repair").unwrap();
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await, (2, 2, 0));
}

#[tokio::test]
async fn no_change_without_ci_still_creates_no_review() {
    let f = red_fixture().await;
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"ci_steps":[]}}).to_string())
        .bind(&f.task.id)
        .execute(f.db.pool())
        .await
        .unwrap();
    repair(&f).await;
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(check_runs(&f).await, 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
}

#[tokio::test]
async fn no_change_explicit_review_window_reset_ignores_prior_failure() {
    let f = red_fixture().await;
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    assert_eq!(spending(&f).await, (1, 1, 1));
    let now = now_rfc3339();
    let boundary = new_uuid_v4();
    sqlx::query("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,bridge_kind,created_at) VALUES(?,?,'review','review','user:api','reset','retry_window_reset',?)")
        .bind(&boundary).bind(&f.task.id).bind(&now).execute(f.db.pool()).await.unwrap();
    let mut tx = db::begin_immediate(f.db.pool()).await.unwrap();
    db::budget::reset_all(&mut tx, &f.task.id, &boundary)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert!(db::budget::failed_review_for_no_change(&f.db, &f.task.id)
        .await
        .unwrap()
        .is_none());
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await, (1, 1, 0));
}

#[tokio::test]
async fn no_change_worker_repair_uses_the_same_guard() {
    let mut f = red_fixture().await;
    f.workflow
        .states
        .iter_mut()
        .find(|state| state.name == "in_progress")
        .unwrap()
        .role = Some("worker".into());
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    repair(&f).await;
    sqlx::query("UPDATE execution SET role='worker' WHERE task_id=?")
        .bind(&f.task.id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(check_runs(&f).await, 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
}

#[tokio::test]
async fn no_change_owner_started_entry_after_repair_is_exempt() {
    let f = red_fixture().await;
    repair(&f).await;
    enter(&f, api_types::Actor::user(api_types::UserActionSource::Api)).await;
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await.2, 0);
}
