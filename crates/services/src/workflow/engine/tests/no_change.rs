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

async fn forge_comments(f: &FailedCiFixture) -> Vec<db::TaskComment> {
    db::TaskCommentRepo::list_comments(
        &*f.db,
        &f.task.id,
        db::PageRequest {
            cursor: None,
            limit: 100,
            sort_by: db::SortBy::CreatedAt,
            sort_order: db::SortOrder::Asc,
            include_total: false,
        },
    )
    .await
    .unwrap()
    .items
    .into_iter()
    .filter(|comment| comment.author_name == "Forge")
    .collect()
}

#[tokio::test]
async fn no_change_send_back_posts_forge_comment_with_head_and_budget() {
    let f = red_fixture().await;
    repair(&f).await;
    assert_eq!(enter(&f, workflow_actor()).await.status, "in_progress");
    let comments = forge_comments(&f).await;
    let reminders: Vec<_> = comments
        .iter()
        .filter(|comment| comment.content.starts_with("No change since review 1"))
        .collect();
    assert_eq!(reminders.len(), 1, "{comments:?}");
    let reminder = reminders[0];
    assert_eq!(reminder.author_type, db::CommentAuthorType::System);
    assert!(reminder
        .content
        .contains("HEAD still matches the failed candidate"));
    assert!(reminder
        .content
        .contains("repair_no_change budget 1 of 1 spent; 0 remaining"));
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
    let review = reviews(&f).await.remove(0);
    // A replay of the same review entry posts nothing new.
    let entry_step = reminder
        .idempotency_key
        .as_deref()
        .and_then(|key| key.strip_prefix(&format!("review:{}:repair_no_change:", review.id)))
        .unwrap()
        .to_owned();
    for _ in 0..2 {
        f.engine
            .publish_review_no_change_comment(&review, &entry_step)
            .await
            .unwrap();
    }
    assert_eq!(
        forge_comments(&f)
            .await
            .iter()
            .filter(|comment| comment.content.starts_with("No change since review 1"))
            .count(),
        1
    );
}

#[tokio::test]
async fn no_change_park_posts_forge_comment_with_findings_and_owner_actions() {
    let f = red_fixture().await;
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    repair(&f).await;
    assert_eq!(enter(&f, workflow_actor()).await.status, "review");
    let comments = forge_comments(&f).await;
    let parks: Vec<_> = comments
        .iter()
        .filter(|comment| {
            comment
                .content
                .starts_with("Task parked after review attempt 1")
        })
        .collect();
    assert_eq!(parks.len(), 1, "{comments:?}");
    let park = parks[0];
    assert_eq!(park.author_type, db::CommentAuthorType::System);
    assert!(park.content.contains("no change after repair round"));
    assert!(park.content.contains("Open findings: CI-0"));
    assert!(park
        .content
        .contains("Owner actions: re-run review, mark reviewed with a reason, or cancel the Task"));
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
    let review = reviews(&f).await.remove(0);
    // A replay of the same review entry posts nothing new.
    let kind = park
        .idempotency_key
        .as_deref()
        .and_then(|key| key.strip_prefix(&format!("review:{}:", review.id)))
        .unwrap()
        .to_owned();
    assert!(kind.starts_with("park:"), "{kind}");
    for _ in 0..2 {
        f.engine
            .publish_review_park_comment_as(&review, "no change after repair round", &kind)
            .await
            .unwrap();
    }
    assert_eq!(
        forge_comments(&f)
            .await
            .iter()
            .filter(|comment| comment
                .content
                .starts_with("Task parked after review attempt 1"))
            .count(),
        1
    );
}

#[tokio::test]
async fn no_change_park_after_owner_send_back_posts_its_own_comment() {
    let f = red_fixture().await;
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    repair(&f).await;
    assert_eq!(enter(&f, workflow_actor()).await.status, "review");
    // The owner sends the parked Task back, and the coder again changes
    // nothing: the same failed Review parks a second time.
    let owner = api_types::Actor::user(api_types::UserActionSource::Api);
    let snapshot = f
        .engine
        .task_action_snapshot(&f.task.id, &owner)
        .await
        .unwrap();
    f.engine
        .perform_task_action(
            f.task.id.clone(),
            api_types::TaskAction::SendBack {
                guidance: "change the code this time".to_owned(),
            },
            snapshot.task.version,
        )
        .await
        .unwrap();
    assert_eq!(
        drain(f.engine.clone(), &f.task.id).await.status,
        "in_progress"
    );
    repair(&f).await;
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "review");
    let blocked: serde_json::Value =
        serde_json::from_str(task.blocked_json.as_deref().unwrap()).unwrap();
    assert_eq!(blocked["reason"], "no change after repair round");
    assert_eq!(reviews(&f).await.len(), 1);
    let comments = forge_comments(&f).await;
    let parks = comments
        .iter()
        .filter(|comment| comment.content.contains("no change after repair round"))
        .count();
    assert_eq!(parks, 2, "{comments:?}");
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
    let comments = forge_comments(&f).await;
    assert!(
        !comments.iter().any(
            |comment| comment.content.starts_with("No change since review")
                || comment.content.contains("no change after repair round")
        ),
        "{comments:?}"
    );
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
    let comments = forge_comments(&f).await;
    assert!(
        !comments.iter().any(
            |comment| comment.content.starts_with("No change since review")
                || comment.content.contains("no change after repair round")
        ),
        "{comments:?}"
    );
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
async fn no_change_send_back_leaves_an_owner_approval_review_gate() {
    // The autonomous default's review gate waits for the owner. The reminder
    // is not a rejection, yet it must still leave the gate: a superseded
    // cascade would strand the Task in review with nothing running.
    let mut f = red_fixture().await;
    f.workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    repair(&f).await;
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
}

/// The workspace owner is away while `away` is set: `describe` is refused.
struct AwayOwner(
    Arc<std::sync::atomic::AtomicBool>,
    Arc<dyn crate::workspace_backend::WorkspaceBackend>,
);
#[async_trait::async_trait]
impl crate::workspace_backend::WorkspaceBackend for AwayOwner {
    async fn prepare(
        &self,
        p: &db::WorkspacePlacement,
        s: &crate::workspace_backend::PrepareSpec,
    ) -> crate::workspace_backend::Result<crate::workspace_backend::PreparedWorkspace> {
        self.1.prepare(p, s).await
    }
    async fn describe(
        &self,
        p: &db::WorkspacePlacement,
    ) -> crate::workspace_backend::Result<crate::workspace_backend::WorkspaceState> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(
                crate::workspace_backend::WorkspaceBackendError::OwnerUnreachable {
                    daemon_id: "away-owner".into(),
                },
            );
        }
        self.1.describe(p).await
    }
    async fn run(
        &self,
        p: &db::WorkspacePlacement,
        s: &crate::workspace_backend::RunSpec,
    ) -> crate::workspace_backend::Result<crate::workspace_backend::RunResult> {
        self.1.run(p, s).await
    }
    async fn diff(
        &self,
        p: &db::WorkspacePlacement,
        s: &crate::workspace_backend::DiffSpec,
    ) -> crate::workspace_backend::Result<crate::workspace_backend::Diff> {
        self.1.diff(p, s).await
    }
    async fn read(
        &self,
        p: &db::WorkspacePlacement,
        path: &str,
        limit: u64,
    ) -> crate::workspace_backend::Result<Vec<u8>> {
        self.1.read(p, path, limit).await
    }
    async fn reset(
        &self,
        p: &db::WorkspacePlacement,
        s: &crate::workspace_backend::ResetSpec,
    ) -> crate::workspace_backend::Result<crate::workspace_backend::PreparedWorkspace> {
        self.1.reset(p, s).await
    }
    async fn cleanup(
        &self,
        p: &db::WorkspacePlacement,
    ) -> crate::workspace_backend::Result<crate::workspace_backend::CleanupAck> {
        self.1.cleanup(p).await
    }
    async fn harvest_outbox(
        &self,
        p: &db::WorkspacePlacement,
        execution: &str,
    ) -> crate::workspace_backend::Result<crate::workspace_backend::OutboxHarvest> {
        self.1.harvest_outbox(p, execution).await
    }
    async fn consume_outbox(
        &self,
        p: &db::WorkspacePlacement,
        execution: &str,
    ) -> crate::workspace_backend::Result<()> {
        self.1.consume_outbox(p, execution).await
    }
}

#[tokio::test]
async fn no_change_away_owner_keeps_the_typed_review_entry_wait() {
    // An unreachable owner decides nothing at the guard: the entry meets the
    // outage where every review entry does and waits as a typed interruption,
    // instead of failing its hooks phase once the step's retries run out.
    let mut f = red_fixture().await;
    repair(&f).await;
    let owner = f
        .engine
        .workspace_backend_router
        .resolve(&f.db, &f.workspace)
        .await
        .unwrap()
        .backend;
    let away = Arc::new(std::sync::atomic::AtomicBool::new(true));
    f.engine.workspace_backend_router =
        Arc::new(crate::workspace_backend::WorkspaceBackendRouter::new(
            Arc::new(AwayOwner(away.clone(), owner)),
        ));
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "review");
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "review_ci_infrastructure");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(spending(&f).await, (1, 1, 0));
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

#[tokio::test]
async fn no_change_latest_pass_on_the_same_commit_reviews_normally() {
    let f = red_fixture().await;
    let failed = reviews(&f).await.remove(0);
    let now = now_rfc3339();
    db::ReviewRepo::create(
        &*f.db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: f.task.id.clone(),
            execution_id: failed.execution_id,
            attempt_number: 2,
            status: db::ReviewStatus::Passed,
            step_results_json: failed.step_results_json,
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    // Do not rely on the Task's acceptance marker: the latest Review itself
    // must suppress the older failure, even after acceptance was invalidated.
    assert!(TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap()
        .review_passed_at
        .is_none());
    repair(&f).await;
    enter(&f, workflow_actor()).await;
    assert_eq!(reviews(&f).await.len(), 3);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await, (2, 2, 0));
}

#[tokio::test]
async fn no_change_pre_gate_entry_checkpoint_resumes_without_a_reminder() {
    use db::TaskStepRepo;
    // Old entries retain a settled entry checkpoint, or the open checkpoint of
    // an entry hook that waited. Neither had a reserved gate checkpoint.
    for checkpoint_only in [true, false] {
        let f = red_fixture().await;
        repair(&f).await;
        let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
            .await
            .unwrap()
            .unwrap();
        f.engine
            .workflow_execution()
            .transition_with_authority(
                &task.id,
                "review",
                task.version,
                &f.workflow,
                &workflow_actor(),
                "pre-gate repair entry",
                false,
                f.workflow_authority().await,
            )
            .await
            .unwrap();
        let step =
            f.db.claim_step(
                "pre-gate-worker",
                Some(&task.id),
                &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(step.kind, "hooks");
        sqlx::query("UPDATE task_step SET payload_json=json_remove(payload_json,'$.entry_origin') WHERE id=?")
            .bind(&step.id).execute(f.db.pool()).await.unwrap();
        let index = f
            .workflow
            .states
            .iter()
            .find(|s| s.name == "in_progress")
            .unwrap()
            .hooks
            .on_exit
            .len() as i64;
        f.db.start_hook(&step, index).await.unwrap();
        if checkpoint_only {
            f.db.finish_hook(
                &step,
                index,
                &serde_json::to_string(&crate::workflow::HookResult::Ok).unwrap(),
            )
            .await
            .unwrap();
            f.db.retry_step(
                &step,
                "worker interrupted before the gate shipped",
                &now_rfc3339(),
            )
            .await
            .unwrap();
        } else {
            f.db.suspend_hook_step(&step, &now_rfc3339(), "disk")
                .await
                .unwrap();
            f.db.wake_suspended_hooks(&task.id, "disk").await.unwrap();
        }
        drain(f.engine.clone(), &task.id).await;
        assert_eq!(
            reviews(&f).await.len(),
            2,
            "checkpoint_only={checkpoint_only}"
        );
        assert_eq!(check_runs(&f).await, 2);
        assert_eq!(spending(&f).await, (2, 2, 0));
        let decision: String = sqlx::query_scalar(
            "SELECT result_json FROM task_hook_checkpoint WHERE step_id=? AND hook_index=-1",
        )
        .bind(&step.id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
        assert_eq!(decision, "\"Continue\"");
    }
}

#[tokio::test]
async fn no_change_exit_hook_wait_does_not_skip_the_gate() {
    use db::TaskStepRepo;
    let mut f = red_fixture().await;
    let exit_hook = f
        .workflow
        .states
        .iter()
        .find(|state| state.name == "review")
        .unwrap()
        .hooks
        .before_enter[0]
        .clone();
    f.workflow
        .states
        .iter_mut()
        .find(|state| state.name == "in_progress")
        .unwrap()
        .hooks
        .on_exit
        .push(exit_hook);
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    repair(&f).await;
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    f.engine
        .workflow_execution()
        .transition_with_authority(
            &task.id,
            "review",
            task.version,
            &f.workflow,
            &workflow_actor(),
            "repair entry",
            false,
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    let step =
        f.db.claim_step(
            "exit-wait-worker",
            Some(&task.id),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    // The exit hook opened its checkpoint and waited for disk. The woken
    // step keeps that wait recorded; it was not an entry past the guard.
    f.db.start_hook(&step, 0).await.unwrap();
    f.db.suspend_hook_step(&step, &now_rfc3339(), "disk")
        .await
        .unwrap();
    f.db.wake_suspended_hooks(&task.id, "disk").await.unwrap();
    drain(f.engine.clone(), &task.id).await;
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(check_runs(&f).await, 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
}

#[tokio::test]
async fn no_change_exit_checkpoint_does_not_skip_the_gate() {
    let mut f = red_fixture().await;
    let exit_hook = f
        .workflow
        .states
        .iter()
        .find(|state| state.name == "review")
        .unwrap()
        .hooks
        .before_enter[0]
        .clone();
    f.workflow
        .states
        .iter_mut()
        .find(|state| state.name == "in_progress")
        .unwrap()
        .hooks
        .on_exit
        .push(exit_hook);
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    repair(&f).await;
    assert_eq!(enter(&f, workflow_actor()).await.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(check_runs(&f).await, 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
}

async fn retry_offline_entry(f: &FailedCiFixture) -> db::Task {
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = f
        .engine
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            &task.id,
            task.version,
            &f.workflow,
            &api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "owner reconnected",
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    drain_result(f.engine.clone(), result).await.task
}

async fn make_owner_away(f: &mut FailedCiFixture) -> Arc<std::sync::atomic::AtomicBool> {
    let owner = f
        .engine
        .workspace_backend_router
        .resolve(&f.db, &f.workspace)
        .await
        .unwrap()
        .backend;
    let away = Arc::new(std::sync::atomic::AtomicBool::new(true));
    f.engine.workspace_backend_router =
        Arc::new(crate::workspace_backend::WorkspaceBackendRouter::new(
            Arc::new(AwayOwner(away.clone(), owner)),
        ));
    away
}

#[tokio::test]
async fn no_change_offline_entry_retry_retains_the_repair_origin() {
    let mut f = red_fixture().await;
    repair(&f).await;
    let away = make_owner_away(&mut f).await;
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "review");
    let annotation: serde_json::Value =
        serde_json::from_str(task.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "review_ci_infrastructure");
    // More than one same-state retry must retain the first source role.
    retry_offline_entry(&f).await;
    away.store(false, std::sync::atomic::Ordering::SeqCst);
    let task = retry_offline_entry(&f).await;
    assert_eq!(task.status, "in_progress");
    assert!(task.entry_barrier_json.is_none());
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(task.blocked_json.is_none());
    assert_eq!(reviews(&f).await.len(), 1);
    assert_eq!(check_runs(&f).await, 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
    assert!(TransitionLogRepo::list_by_task(&*f.db, &f.task.id)
        .await
        .unwrap()
        .iter()
        .any(|entry| entry
            .trigger_reason
            .starts_with("No change since review 1\n")));
}

#[tokio::test]
async fn no_change_offline_owner_entry_retry_retains_the_owner_exemption() {
    let mut f = red_fixture().await;
    // This owner-controlled gate stays blocked on a failed entry instead of
    // routing user-entry failures back to the coder, so it has a retry barrier.
    f.workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .reject_target = None;
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    repair(&f).await;
    let away = make_owner_away(&mut f).await;
    enter(&f, api_types::Actor::user(api_types::UserActionSource::Api)).await;
    away.store(false, std::sync::atomic::Ordering::SeqCst);
    retry_offline_entry(&f).await;
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await.2, 0);
}

#[tokio::test]
async fn no_change_cancelled_review_does_not_hide_latest_settled_failure() {
    let f = red_fixture().await;
    let failed = reviews(&f).await.remove(0);
    let now = now_rfc3339();
    db::ReviewRepo::create(
        &*f.db,
        db::CreateReview {
            id: new_uuid_v4(),
            task_id: f.task.id.clone(),
            execution_id: failed.execution_id,
            attempt_number: 2,
            status: db::ReviewStatus::Cancelled,
            step_results_json: failed.step_results_json,
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    repair(&f).await;
    let task = enter(&f, workflow_actor()).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 1);
    assert_eq!(spending(&f).await, (1, 1, 1));
}

#[tokio::test]
async fn no_change_owner_retry_of_offline_coder_entry_is_exempt() {
    let mut f = red_fixture().await;
    repair(&f).await;
    let away = make_owner_away(&mut f).await;
    let task = enter(&f, workflow_actor()).await;
    assert!(task.entry_barrier_json.is_some());
    away.store(false, std::sync::atomic::Ordering::SeqCst);
    // A successful check proves that the normal entry completes and clears
    // the infrastructure annotation and barrier, without a no-change park.
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"ci_steps":["true"]}}).to_string())
        .bind(&f.task.id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = f
        .engine
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            &task.id,
            task.version,
            &f.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Api),
            "owner presses Retry",
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    let task = drain_result(f.engine.clone(), result).await.task;
    assert!(
        task.entry_barrier_json.is_none(),
        "{:?}",
        task.entry_barrier_json
    );
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(task.blocked_json.is_none(), "{:?}", task.blocked_json);
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await, (1, 1, 0));
}

#[tokio::test]
async fn no_change_owner_retry_exemption_survives_another_offline_retry() {
    let mut f = red_fixture().await;
    let gate = f
        .workflow
        .states
        .iter_mut()
        .find(|state| state.name == "review")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap();
    gate.reject_target = None;
    gate.requires_user_approval = Some(true);
    sqlx::query("UPDATE project SET workflow_definition=? WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap())
        .bind(&f.task.project_id)
        .execute(f.db.pool())
        .await
        .unwrap();
    repair(&f).await;
    let away = make_owner_away(&mut f).await;
    let task = enter(&f, workflow_actor()).await;
    assert!(task.entry_barrier_json.is_some());
    let result = f
        .engine
        .workflow_execution()
        .retry_entry_barrier_with_authority(
            &task.id,
            task.version,
            &f.workflow,
            &api_types::Actor::user(api_types::UserActionSource::Api),
            "owner retries while still offline",
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    let waiting = drain_result(f.engine.clone(), result).await.task;
    assert!(waiting.entry_barrier_json.is_some());
    assert_eq!(spending(&f).await.2, 0);
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"ci_steps":["true"]}}).to_string())
        .bind(&f.task.id)
        .execute(f.db.pool())
        .await
        .unwrap();
    away.store(false, std::sync::atomic::Ordering::SeqCst);
    let task = retry_offline_entry(&f).await;
    assert!(task.entry_barrier_json.is_none());
    assert!(
        task.error_annotation.is_none(),
        "{:?}",
        task.error_annotation
    );
    assert!(task.blocked_json.is_none());
    assert_eq!(reviews(&f).await.len(), 2);
    assert_eq!(check_runs(&f).await, 2);
    assert_eq!(spending(&f).await.2, 0);
}
