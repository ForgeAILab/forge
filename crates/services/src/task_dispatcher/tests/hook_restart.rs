//! Tests for hook restart in the task dispatcher.

use super::*;
#[tokio::test]
async fn hook_restart_recognizes_merge_landed_before_checkpoint_even_after_sibling_merge() {
    use db::TaskStepRepo;
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let engine = fixture.dispatcher.task_service.clone();
    engine
        .workflow_execution()
        .manual_override_transition_with_authority(
            &fixture.task.id,
            "merging",
            fixture.task.version,
            &workflow,
            api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "resume integration",
            false,
            Some(crate::workflow::engine::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
                clear_review_passed_at_on_commit: false,
            }),
        )
        .await
        .unwrap();
    let step = fixture
        .db
        .claim_step(
            "crashed-merge",
            Some(&fixture.task.id),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    fixture.db.start_hook(&step, 0).await.unwrap();
    // What the pre-upgrade hook recorded before it merged.
    let facts = fixture
        .merge_service
        .integration_admission_facts(&fixture.task)
        .await
        .unwrap();
    let intent = crate::merge_service::MergeIntent {
        execution_id: facts.execution_id.expect("implementation execution"),
        workspace_id: facts.workspace.id,
        candidate_sha: facts.candidate_sha,
        target_branch: facts.target_branch,
    };
    fixture
        .db
        .record_hook_effect(
            &step,
            0,
            "merge_intent",
            &serde_json::to_string(&intent).unwrap(),
        )
        .await
        .unwrap();
    // Seed the pre-queue Git effect itself. No current service may bypass
    // the queue to reproduce a merge from an older installed build.
    run_git(
        fixture._repo_dir.path(),
        &["merge", "--ff-only", &intent.candidate_sha],
    );
    std::fs::write(
        fixture._repo_dir.path().join("later-user-edit.txt"),
        "preserved after landed merge",
    )
    .unwrap();
    // Crash after Git integration, before recording the hook's outcome.
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    // A sibling lands on the target after the crash, before the resume.
    std::fs::write(
        fixture._repo_dir.path().join("sibling-after-crash.txt"),
        "sibling merged after the crash\n",
    )
    .unwrap();
    run_git(
        fixture._repo_dir.path(),
        &["add", "sibling-after-crash.txt"],
    );
    run_git(
        fixture._repo_dir.path(),
        &["commit", "-m", "sibling merged after crash"],
    );
    let target_tip = run_git(fixture._repo_dir.path(), &["rev-parse", "HEAD"]);
    assert_ne!(target_tip, intent.candidate_sha);
    let settled = crate::worker_runtime::queue::TaskStepWorker::new(engine.as_ref().clone())
        .drain(&fixture.task.id)
        .await
        .unwrap();
    assert_eq!(settled.status, "done");
    assert!(settled.error_annotation.is_none());
    assert_eq!(
        fixture
            .db
            .task_steps(&fixture.task.id)
            .await
            .unwrap()
            .iter()
            .find(|row| row.id == step.id)
            .unwrap()
            .attempts,
        2
    );
    // The recorded merge result is the candidate, never the target's tip.
    let after_sha: Option<String> =
        sqlx::query_scalar("SELECT after_sha FROM execution WHERE id = ?")
            .bind(&intent.execution_id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    assert_eq!(after_sha.as_deref(), Some(intent.candidate_sha.as_str()));
    let merged_comments: Vec<String> = sqlx::query_scalar(
        "SELECT content FROM task_comment WHERE task_id = ? AND content LIKE 'Changes merged%'",
    )
    .bind(&fixture.task.id)
    .fetch_all(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(
        merged_comments,
        vec![format!(
            "Changes merged to main (SHA: {})",
            intent.candidate_sha
        )]
    );
}

// A Task left in `merging` by a pre-upgrade binary (merge interrupted, no
// task_step row) is taken into the integration queue by the start-time
// importer, once, and merged by the queue (importer row P18).
#[tokio::test]
async fn legacy_merging_task_without_hook_step_is_merged_after_one_startup() {
    use db::{IntegrationQueueRepo, TaskStepRepo};
    let fixture = merge_gate_fixture(chrono::Duration::minutes(30)).await;
    assert!(fixture
        .db
        .task_steps(&fixture.task.id)
        .await
        .unwrap()
        .iter()
        .all(|step| !matches!(step.kind.as_str(), "hooks" | "cascade") && step.status == "done"));
    // The wedge an older binary left: the review passed, the Task entered
    // `merging`, and the merge's hooks row was lost.
    review_passed(&fixture).await;
    // The runtime imports before the dispatcher's first lap
    // (`RuntimeWorkers::start`): the importer takes the Task, and the
    // dispatcher's reconciler then finds it owned.
    let first = crate::integration_steps::import::import_at_start(&fixture.db)
        .await
        .unwrap();
    assert_eq!((first.imported, first.continuations), (1, 1));
    let imported = fixture
        .db
        .current_integration_attempt(&fixture.task.id)
        .await
        .unwrap()
        .expect("the wedged entry is imported");
    // Queued once the candidate's present HEAD is read: the continuation
    // step reads it by admitting the Task again.
    assert_eq!(
        (imported.state, imported.resume_state),
        (
            db::IntegrationAttemptState::Parked,
            Some(db::IntegrationAttemptState::Queued)
        ),
        "{:?}",
        imported.failure_message
    );
    assert_eq!(
        imported.admission_key,
        format!("import:{}:0", fixture.task.id)
    );
    // A second start imports nothing and enqueues nothing.
    let second = crate::integration_steps::import::import_at_start(&fixture.db)
        .await
        .unwrap();
    assert_eq!((second.imported, second.continuations), (0, 0));
    let steps = fixture.db.task_steps(&fixture.task.id).await.unwrap();
    assert_eq!(
        steps
            .iter()
            .filter(|step| step.causation_key == format!("integration-import:{}", imported.id))
            .count(),
        1
    );
    fixture.dispatcher.drain_steps().await.unwrap();
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, "done", "legacy merging Task is stranded");
    assert!(current.error_annotation.is_none());
    assert!(fixture
        .db
        .current_integration_attempt(&fixture.task.id)
        .await
        .unwrap()
        .is_none());
    let third = crate::integration_steps::import::import_at_start(&fixture.db)
        .await
        .unwrap();
    assert_eq!(third.continuations, 0);
}

// A pause marker left on a `merging` Task by an older binary is not the
// dispatcher's any more: no scan re-enters `merging` for it. The start-time
// importer's continuation step clears the marker and admits the Task, and
// the queue merges it (importer rows P11 / P12).
#[tokio::test]
async fn a_paused_integration_marker_on_a_merging_task_is_continued_by_the_importer() {
    use db::TaskStepRepo;
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    review_passed(&fixture).await;
    let task = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    crate::deferred_dispatch::defer_integration_for_pause(&fixture.db, &task)
        .await
        .unwrap();
    // The runtime imports before the dispatcher's first lap; the marker is
    // the importer's to continue (the dispatcher's own resume step is for
    // states that do not admit to the queue).
    let marked = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(marked.status, "merging");
    assert!(crate::deferred_dispatch::paused_integration(&marked).is_some());

    let report = crate::integration_steps::import::import_at_start(&fixture.db)
        .await
        .unwrap();
    assert_eq!(report.continuations, 1, "{report:?}");
    let imported =
        db::IntegrationQueueRepo::current_integration_attempt(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .expect("imported");
    let decision = imported.import_source_json.clone().unwrap()["decision"].clone();
    fixture.dispatcher.drain_steps().await.unwrap();
    let steps = fixture.db.task_steps(&fixture.task.id).await.unwrap();
    assert!(
        steps.iter().all(|s| s.status != "failed"),
        "{:?}",
        steps
            .iter()
            .map(|s| (&s.kind, &s.status, &s.last_error))
            .collect::<Vec<_>>()
    );
    let current = TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let attempts: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT admission_key, state||'/'||(SELECT q.state||'/'||COALESCE(q.last_error_kind,'-')||'/'||COALESCE(q.head_attempt_id,'-')||'/'||COALESCE(q.lease_until,'-') FROM integration_queue q WHERE q.id=integration_attempt.queue_id)||'/'||COALESCE(available_at,'-')||'/'||operation_receipts_json, failure_message FROM integration_attempt WHERE task_ref=? ORDER BY created_at",
    )
    .bind(&fixture.task.id)
    .fetch_all(fixture.db.pool())
    .await
    .unwrap();
    assert_eq!(
        current.status,
        "done",
        "{decision}\n{attempts:?}\n{:?}",
        steps
            .iter()
            .map(|s| (&s.kind, &s.status, &s.causation_key, &s.last_error))
            .collect::<Vec<_>>()
    );
    assert!(crate::deferred_dispatch::paused_integration(&current).is_none());
    assert_eq!(
        TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .iter()
            .filter(|l| l.to_state == "merging")
            .count(),
        1,
        "the Task never re-entered merging"
    );
}

// A crash inside `rebase_target` leaves the rebase stopped on its conflict.
// HEAD already descends from the target, so the resume must not read it as a
// landed rebase: it finishes the conflict handoff with its paths and leaves
// no rebase in progress for the coder.
#[tokio::test]
async fn hook_restart_resumes_rebase_interrupted_mid_conflict_as_handoff() {
    use db::TaskStepRepo;
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    let repo = fixture._repo_dir.path();
    let worktree = fixture._workspace_dir.path().join("worktree");
    // Target never received the candidate; a sibling changed the same file.
    run_git(repo, &["reset", "--hard", "HEAD~2"]);
    std::fs::write(repo.join("feature.txt"), "sibling version\n").unwrap();
    run_git(repo, &["add", "-A"]);
    run_git(repo, &["commit", "-m", "sibling adds feature"]);
    let target = run_git(repo, &["rev-parse", "main"]);
    let branch = workspace::task_branch_name(&fixture.task.id);
    let candidate = run_git(&worktree, &["rev-parse", "HEAD"]);

    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    let engine = fixture.dispatcher.task_service.clone();
    engine
        .workflow_execution()
        .manual_override_transition_with_authority(
            &fixture.task.id,
            "merging",
            fixture.task.version,
            &workflow,
            api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "resume integration",
            false,
            Some(crate::workflow::engine::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
                clear_review_passed_at_on_commit: false,
            }),
        )
        .await
        .unwrap();
    let step = fixture
        .db
        .claim_step(
            "crashed-rebase",
            Some(&fixture.task.id),
            &(chrono::Utc::now() + chrono::Duration::seconds(60)).to_rfc3339(),
        )
        .await
        .unwrap()
        .unwrap();
    fixture.db.start_hook(&step, 0).await.unwrap();
    let moved = crate::merge_service::MergeOutcome::TargetMoved {
        reason: "main advanced since review".into(),
        target_branch: "main".into(),
    };
    fixture
        .db
        .record_hook_effect(
            &step,
            0,
            "merge_outcome",
            &serde_json::to_string(&moved).unwrap(),
        )
        .await
        .unwrap();
    fixture
        .db
        .record_hook_effect(
            &step,
            0,
            "rebase_target",
            &serde_json::json!(target).to_string(),
        )
        .await
        .unwrap();
    // The process died inside `git rebase`, which stopped on the conflict.
    let rebase = std::process::Command::new("git")
        .args(["rebase", "main"])
        .current_dir(&worktree)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .unwrap();
    assert!(!rebase.status.success());
    assert!(git::detect_rebase_in_progress(&worktree).await.unwrap());
    sqlx::query("UPDATE task_step SET lease_until='2000-01-01T00:00:00Z' WHERE id=?")
        .bind(&step.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    crate::worker_runtime::queue::TaskStepWorker::new(engine.as_ref().clone())
        .drain(&fixture.task.id)
        .await
        .unwrap();

    let handoff = TransitionLogRepo::list_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .into_iter()
        .find(|entry| {
            entry.from_state == "merging"
                && entry.to_state == "merge_failed"
                && entry.bridge.bridge_kind
                    == Some(api_types::TransitionBridgeKind::ConflictHandoff)
        })
        .expect("the resumed rebase hands its conflict to the coder");
    assert_eq!(
        handoff.triggered_by,
        api_types::Actor::system(api_types::SystemComponent::Workflow).display()
    );
    assert_eq!(
        handoff.bridge.conflict_paths(),
        Some(vec!["feature.txt".to_owned()])
    );
    assert!(!handoff.bridge.is_review_refresh());
    // The coder gets the branch on the new target, not a stopped rebase.
    assert!(!git::detect_rebase_in_progress(&worktree).await.unwrap());
    assert!(run_git(&worktree, &["status", "--porcelain"]).is_empty());
    let head = run_git(&worktree, &["rev-parse", "HEAD"]);
    assert_eq!(run_git(&worktree, &["rev-parse", &branch]), head);
    assert_ne!(head, candidate);
    assert_eq!(run_git(&worktree, &["merge-base", "HEAD", "main"]), target);
    let feature = std::fs::read_to_string(worktree.join("feature.txt")).unwrap();
    assert!(feature.contains("<<<<<<< ") && feature.contains(">>>>>>> "));
    assert!(fixture
        .db
        .task_steps(&fixture.task.id)
        .await
        .unwrap()
        .iter()
        .all(|s| s.status != "failed"));
}

// Resetting the workspace while the Task's hook step is queued or running
// would delete the worktree (and Workspace row) that its CI, merge or
// dispatch captured. The step table is the witness the in-memory completion
// slot used to be.
#[tokio::test]
async fn workspace_reset_waits_for_in_flight_hook_steps() {
    use db::TaskStepRepo;
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    let project = ProjectRepo::get_by_id(&*fixture.db, &fixture.task.project_id)
        .await
        .unwrap()
        .unwrap();
    let workflow = WorkflowEngine::resolve_workflow(&project.workflow_definition);
    fixture
        .dispatcher
        .task_service
        .workflow_execution()
        .manual_override_transition_with_authority(
            &fixture.task.id,
            "merging",
            fixture.task.version,
            &workflow,
            api_types::Actor::system(api_types::SystemComponent::TaskDispatcher),
            "resume integration",
            false,
            Some(crate::workflow::engine::WorkflowAuthority {
                project_version: project.version,
                workflow_definition: project.workflow_definition,
                clear_review_passed_at_on_commit: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(fixture.db.pending_steps(&fixture.task.id).await.unwrap(), 1);
    let error = fixture
        .dispatcher
        .task_service
        .reset_task_workspace(&fixture.task.id)
        .await
        .unwrap_err();
    assert!(matches!(error, ServiceError::Conflict(_)), "{error:?}");
    assert!(
        WorkspaceRepo::get_by_task_id(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(fixture._workspace_dir.path().join("worktree").exists());
}

// Without conflict handoff (a coordination root), an interrupted rebase is
// aborted as a fresh conflicting rebase is, restoring the candidate branch.
#[tokio::test]
async fn embedded_rebase_target_aborts_an_interrupted_rebase_without_handoff() {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    let repo = fixture._repo_dir.path();
    let worktree = fixture._workspace_dir.path().join("worktree");
    run_git(repo, &["reset", "--hard", "HEAD~2"]);
    std::fs::write(repo.join("feature.txt"), "sibling version\n").unwrap();
    run_git(repo, &["add", "-A"]);
    run_git(repo, &["commit", "-m", "sibling adds feature"]);
    let candidate = run_git(&worktree, &["rev-parse", "HEAD"]);
    assert!(git::rebase(&worktree, "main").await.is_err());
    assert!(git::detect_rebase_in_progress(&worktree).await.unwrap());
    let workspace = WorkspaceRepo::get_by_task_id(&*fixture.db, &fixture.task.id)
        .await
        .unwrap()
        .unwrap();
    let resolved = crate::workspace_backend::EmbeddedWorkspaceBackend::resolve_workspace(
        fixture
            .dispatcher
            .task_service
            .workspace_backend_router()
            .as_ref(),
        &fixture.db,
        &workspace,
        fixture._workspace_dir.path(),
    )
    .await
    .unwrap();
    assert!(matches!(
        resolved.rebase_target("main", false).await.unwrap(),
        api_types::WorkspaceOwnerOperationOutcome::Conflict { conflict_paths, .. }
            if conflict_paths.is_empty()
    ));
    assert!(!git::detect_rebase_in_progress(&worktree).await.unwrap());
    assert_eq!(run_git(&worktree, &["rev-parse", "HEAD"]), candidate);
}
