//! Audit 2.3a: a version-only bump between a producing commit and its queued
//! cascade silently supersedes the cascade, and nothing re-drives the Task.
use super::*;
use db::TaskStepRepo;

fn system_options(version: i64, reason: &str) -> crate::task_service::TransitionOptions {
    crate::task_service::TransitionOptions {
        version,
        reason: Some(reason.to_owned()),
        triggered_by: Actor::system(SystemComponent::General),
        rejection: false,
        defer_dispatch_seconds: None,
    }
}

fn title_edit(task: &Task) -> UpdateTask {
    UpdateTask {
        id: task.id.clone(),
        expected_version: task.version,
        title: Some(format!("{} (renamed)", task.title)),
        description: None,
        priority: None,
        merge_config: None,
        plan: None,
        error_annotation: None,
        blocked_json: None,
        failed_json: None,
        task_state_config: None,
        parent_task_id: None,
        updated_at: now_rfc3339(),
    }
}

/// Control: the same transition with no concurrent edit leaves `review`.
#[tokio::test]
async fn audit_23a_control_unconfigured_review_cascades_to_merging() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "control", "in_progress", 1).await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let result = dispatcher
        .task_service
        .transition(
            task.id.clone(),
            "review".to_owned(),
            system_options(task.version, "agent completed"),
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "review");
    assert_eq!(result.pending_steps, 1);
    let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
    assert_eq!(settled.status, "merging");
    let steps = db.task_steps(&task.id).await.unwrap();
    assert_eq!(steps[0].status, "done");
}

/// A title edit (any version-bumping write) that lands before the worker
/// claims the step supersedes the review -> merging cascade. The step is
/// recorded `superseded` with no annotation, no block and no event, and
/// dispatcher sweeps never move the Task again.
#[tokio::test]
async fn audit_23a_unrelated_title_edit_preserves_review_cascade() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "edited", "in_progress", 1).await;
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;

    let result = dispatcher
        .task_service
        .transition(
            task.id.clone(),
            "review".to_owned(),
            system_options(task.version, "agent completed"),
        )
        .await
        .unwrap();
    assert_eq!(result.task.status, "review");
    assert_eq!(result.pending_steps, 1);

    // Unrelated metadata edit (REST PATCH title) before the worker runs.
    let edited = TaskRepo::update(&*db, title_edit(&result.task))
        .await
        .unwrap();
    assert_eq!(edited.status, "review");
    assert!(edited.version > result.task.version);

    let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
    let steps = db.task_steps(&task.id).await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(
        steps[0].status, "done",
        "title edits preserve the producing entry"
    );
    assert_eq!(settled.status, "merging");
    assert!(settled.error_annotation.is_none(), "supersession is silent");
    assert!(settled.blocked_json.is_none(), "supersession is silent");
    assert_eq!(db.pending_steps(&task.id).await.unwrap(), 0);

    // Nothing re-drives it: several dispatcher sweeps leave it in review.
    for _ in 0..3 {
        dispatcher.check_once_and_drain().await.unwrap();
    }
    let after = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.status, "merging");
}

/// `advance_task_condition` calls the engine directly (no PRODUCER_TASK
/// scope), so the engine readies the step at the end of transition_inner,
/// and then the action bumps the version itself in
/// `clear_manual_advance_error_annotation`. Its own write supersedes its own
/// cascade whenever it beats the worker (here: deterministically, no worker).
#[tokio::test]
async fn audit_23a_manual_advance_preserves_its_own_cascade() {
    let db = Arc::new(sqlite_db().await);
    let repo_dir = TempDir::new().unwrap();
    let workspace_dir = TempDir::new().unwrap();
    let (project_id, _) = seed_project_repo(&db, repo_dir.path()).await;
    let task = seed_task(&db, &project_id, "advanced", "todo", 1).await;
    // A Task an owner advances usually carries an annotation explaining why.
    let annotated = TaskRepo::update(
        &*db,
        UpdateTask {
            error_annotation: Some(Some(
                serde_json::json!({"type": "executor_failed", "message": "agent stopped"})
                    .to_string(),
            )),
            title: None,
            ..title_edit(&task)
        },
    )
    .await
    .unwrap();
    let (dispatcher, _rx) = build_dispatcher(Arc::clone(&db), workspace_dir.path()).await;
    let project = ProjectRepo::get_by_id(&*db, &project_id)
        .await
        .unwrap()
        .unwrap();
    let actor = Actor::user(api_types::UserActionSource::Test);
    let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
        &annotated,
        &project.workflow_definition,
        &actor,
    );

    let advanced = dispatcher
        .task_service
        .advance_task_condition(
            &annotated,
            &workflow,
            "planning".to_owned(),
            "owner advance".to_owned(),
            actor,
        )
        .await
        .unwrap();
    assert_eq!(advanced.status, "planning");
    assert!(
        advanced.error_annotation.is_none(),
        "advance cleared the annotation (version bump)"
    );

    let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
    let steps = db.task_steps(&task.id).await.unwrap();
    println!(
        "advance steps: {:?}",
        steps
            .iter()
            .map(|s| (&s.status, s.expected_version, &s.last_error))
            .collect::<Vec<_>>()
    );
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].status, "done");
    assert_eq!(settled.status, "in_progress");
}

#[tokio::test]
async fn audit_23a_annotation_clear_and_board_reorder_preserve_review_entry() {
    use db::TaskBoardRepo;
    for reorder in [false, true] {
        let db = Arc::new(sqlite_db().await);
        let repo = TempDir::new().unwrap();
        let roots = TempDir::new().unwrap();
        let (project, _) = seed_project_repo(&db, repo.path()).await;
        let task = seed_task(&db, &project, "edited", "in_progress", 1).await;
        let (dispatcher, _rx) = build_dispatcher(db.clone(), roots.path()).await;
        let result = dispatcher
            .task_service
            .transition(
                &task.id,
                "review".into(),
                system_options(task.version, "completed"),
            )
            .await
            .unwrap();
        let before = db.task_steps(&task.id).await.unwrap()[0]
            .producing_transition_id
            .clone();
        if reorder {
            let neighbor = seed_task(&db, &project, "neighbor", "review", 1).await;
            let revision = TaskBoardRepo::board_revision(&*db, &project).await.unwrap();
            dispatcher
                .task_service
                .move_task(
                    &task.id,
                    api_types::MoveTaskRequest {
                        operation_id: new_uuid_v4(),
                        task_version: result.task.version,
                        board_revision: revision,
                        target_status: "review".into(),
                        before_id: None,
                        after_id: Some(neighbor.id),
                    },
                )
                .await
                .unwrap();
        } else {
            let annotated = TaskRepo::update(
                &*db,
                UpdateTask {
                    title: None,
                    error_annotation: Some(Some(
                        r#"{"type":"executor_failed","message":"old"}"#.into(),
                    )),
                    ..title_edit(&result.task)
                },
            )
            .await
            .unwrap();
            TaskRepo::update(
                &*db,
                UpdateTask {
                    title: None,
                    error_annotation: Some(None),
                    ..title_edit(&annotated)
                },
            )
            .await
            .unwrap();
        }
        let settled = dispatcher.task_service.drain(&task.id).await.unwrap();
        assert_eq!(
            settled.status,
            "merging",
            "reorder={reorder} steps={:?}",
            db.task_steps(&task.id).await.unwrap()
        );
        let rows = db.task_steps(&task.id).await.unwrap();
        assert_eq!(rows[0].status, "done");
        assert_eq!(rows[0].producing_transition_id, before);
    }
}

#[tokio::test]
async fn audit_23a_background_worker_races_owner_advance_without_409_or_lost_cascade() {
    let db = Arc::new(sqlite_db().await);
    let repo = TempDir::new().unwrap();
    let roots = TempDir::new().unwrap();
    let (project, _) = seed_project_repo(&db, repo.path()).await;
    let (dispatcher, _rx) = build_dispatcher(db.clone(), roots.path()).await;
    let workflow = crate::workflow::default_workflow::default_workflow();
    let (stop, rx) = tokio::sync::watch::channel(false);
    let worker = dispatcher.task_service.task_step_worker().start(rx);
    for n in 0..10 {
        let task = seed_task(&db, &project, &format!("owner advance {n}"), "todo", 1).await;
        let source = TaskRepo::update(
            &*db,
            UpdateTask {
                title: None,
                error_annotation: Some(Some(
                    r#"{"type":"executor_failed","message":"old"}"#.into(),
                )),
                ..title_edit(&task)
            },
        )
        .await
        .unwrap();
        dispatcher
            .task_service
            .advance_task_condition(
                &source,
                &workflow,
                "planning".into(),
                "owner advance".into(),
                Actor::user(api_types::UserActionSource::Test),
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let task = TaskRepo::get_by_id(&*db, &source.id, false)
                    .await
                    .unwrap()
                    .unwrap();
                if task.status == "in_progress" && !db.task_step_is_running(&task.id) {
                    assert!(task.error_annotation.is_none());
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(db.task_steps(&source.id).await.unwrap()[0].status, "done");
    }
    stop.send(true).unwrap();
    worker.await.unwrap();
}
