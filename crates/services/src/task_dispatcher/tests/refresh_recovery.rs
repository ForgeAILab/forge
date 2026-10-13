//! Interrupted refreshes must either advance or park once for their owner.
use super::*;

async fn refresh_fixture() -> MergeGateFixture {
    let fixture = merge_gate_fixture(chrono::Duration::minutes(3)).await;
    sqlx::query("UPDATE task SET status='merge_failed' WHERE id=?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    TransitionLogRepo::insert(
        &*fixture.db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: fixture.task.id.clone(),
            from_state: "merging".into(),
            to_state: "merge_failed".into(),
            trigger_name: None,
            triggered_by: Actor::system(SystemComponent::Workflow).display(),
            bridge: api_types::TransitionBridge::new(
                api_types::TransitionBridgeKind::ReviewRefresh,
            ),
            trigger_reason: "fresh review required".into(),
            hook_results_json: None,
            rejection: false,
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    fixture
}

async fn bookkeeping(db: &db::SqliteDb, id: &str) -> (i64, i64) {
    sqlx::query_as("SELECT (SELECT COUNT(*) FROM task_step WHERE task_id=task.id), COALESCE((SELECT generation FROM task_schedule_dirty WHERE task_id=task.id),0) FROM task WHERE id=?")
        .bind(id).fetch_one(db.pool()).await.unwrap()
}

#[tokio::test]
async fn unrecoverable_review_refresh_parks_once_instead_of_appending_commands() {
    for missing in ["execution_pin", "workspace_row", "recorded_failure"] {
        let fixture = refresh_fixture().await;
        let id = &fixture.task.id;
        match missing {
            "workspace_row" => {
                sqlx::query("DELETE FROM workspace WHERE task_id=?")
                    .bind(id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
            _ => {
                sqlx::query("UPDATE execution SET workspace_id=NULL WHERE task_id=?")
                    .bind(id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
            }
        }
        if missing == "recorded_failure" {
            sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
                .bind(serde_json::json!({"type":"cascade_failed", "state":"merge_failed", "message":"executor execution missing workspace_id"}).to_string())
                .bind(id).execute(fixture.db.pool()).await.unwrap();
            // A prior cascade failure carries its interruption event as well
            // as its annotation; seed both durable facts of that history.
            let mut tx = db::begin_immediate(fixture.db.pool()).await.unwrap();
            fixture.db.sync_condition_in_tx(&mut tx, id).await.unwrap();
            tx.commit().await.unwrap();
            let seeded = TaskRepo::get_by_id(&*fixture.db, id, false)
                .await
                .unwrap()
                .unwrap();
            db::DomainEventRepo::append_event(
                &*fixture.db,
                db::CreateDomainEvent::task_interruption_changed(&seeded),
            )
            .await
            .unwrap();
        }
        fixture.dispatcher.reconcile_all().await.unwrap();
        fixture.dispatcher.drain_steps().await.unwrap();
        let first = bookkeeping(&fixture.db, id).await;
        let mut generation = None;
        for pass in 2..=20 {
            fixture.dispatcher.reconcile_all().await.unwrap();
            fixture.dispatcher.drain_steps().await.unwrap();
            let after = bookkeeping(&fixture.db, id).await;
            assert_eq!(
                after.0, first.0,
                "unchanged refresh appended a step on pass {pass}; missing={missing}"
            );
            if pass == 2 {
                generation = Some(after.1);
            } else {
                assert_eq!(Some(after.1), generation, "missing={missing}; pass={pass}");
            }
        }
        let parked = TaskRepo::get_by_id(&*fixture.db, id, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parked.status, "merge_failed");
        assert_eq!(
            parked.condition.read().failure_kind,
            Some(api_types::FailureKind::CascadeFailed),
            "missing={missing}; {parked:?}; steps={:?}",
            db::TaskStepRepo::task_steps(&*fixture.db, id)
                .await
                .unwrap()
                .iter()
                .map(|s| (&s.kind, &s.status, &s.last_error))
                .collect::<Vec<_>>()
        );
        assert!(db::material_blocker(&parked.condition).requires_intervention);
        let offers = fixture
            .dispatcher
            .task_service
            .task_action_offers(id, &Actor::user(api_types::UserActionSource::Api))
            .await
            .unwrap();
        for verb in ["restart", "cancel"] {
            assert!(
                offers
                    .available_actions
                    .iter()
                    .any(|offer| offer.action.verb() == verb),
                "{offers:?}"
            );
        }
        crate::AttentionService::new(fixture.db.clone())
            .project_once(100)
            .await
            .unwrap();
        let attention: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection WHERE status='open' AND json_extract(details_json,'$.entity_id')=?")
            .bind(id).fetch_one(fixture.db.pool()).await.unwrap();
        assert_eq!(attention, 1);
        // Cancel is a working exit even when the historical workspace is absent.
        fixture
            .dispatcher
            .task_service
            .perform_task_action(
                id,
                api_types::TaskAction::Cancel {
                    reason: Some("cannot recover review provenance".into()),
                },
                parked.version,
            )
            .await
            .unwrap();
        fixture.dispatcher.drain_steps().await.unwrap();
        assert_eq!(
            TaskRepo::get_by_id(&*fixture.db, id, false)
                .await
                .unwrap()
                .unwrap()
                .status,
            "cancelled"
        );
    }
}

#[tokio::test]
async fn recoverable_review_refresh_advances_without_a_historical_review_row() {
    let fixture = refresh_fixture().await;
    let id = &fixture.task.id;
    assert!(ReviewRepo::list_by_task(&*fixture.db, id)
        .await
        .unwrap()
        .is_empty());
    fixture.dispatcher.reconcile_all().await.unwrap();
    fixture.dispatcher.drain_steps().await.unwrap();
    let task = TaskRepo::get_by_id(&*fixture.db, id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(
        TransitionLogRepo::list_by_task(&*fixture.db, id)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.from_state == "merge_failed" && entry.to_state == "review"),
        "{:?}",
        task.condition
    );
    assert_ne!(
        task.condition.read().failure_kind,
        Some(api_types::FailureKind::CascadeFailed)
    );
}

#[tokio::test]
async fn unrecoverable_paused_integration_hop_also_parks_once() {
    let fixture = refresh_fixture().await;
    let id = &fixture.task.id;
    sqlx::query("UPDATE transition_log SET bridge_kind=NULL WHERE task_id=?")
        .bind(id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE execution SET workspace_id=NULL WHERE task_id=?")
        .bind(id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET metadata_json=? WHERE id=?")
        .bind(serde_json::json!({"paused_integration":{"state":"merge_failed","deferred_at":now_rfc3339()}}).to_string())
        .bind(id).execute(fixture.db.pool()).await.unwrap();
    fixture.dispatcher.reconcile_all().await.unwrap();
    fixture.dispatcher.drain_steps().await.unwrap();
    let first = bookkeeping(&fixture.db, id).await;
    for pass in 2..=20 {
        fixture.dispatcher.reconcile_all().await.unwrap();
        fixture.dispatcher.drain_steps().await.unwrap();
        assert_eq!(
            bookkeeping(&fixture.db, id).await,
            first,
            "paused integration wrote on unchanged pass {pass}"
        );
    }
    let parked = TaskRepo::get_by_id(&*fixture.db, id, false)
        .await
        .unwrap()
        .unwrap();
    assert!(db::material_blocker(&parked.condition).requires_intervention);
}

#[tokio::test]
async fn deleted_branch_refresh_settles_in_coder_repair_with_a_working_cancel() {
    let fixture = refresh_fixture().await;
    let id = &fixture.task.id;
    let tree = fixture._workspace_dir.path().join("worktree");
    run_git(
        fixture._repo_dir.path(),
        &["worktree", "remove", "--force", tree.to_str().unwrap()],
    );
    run_git(
        fixture._repo_dir.path(),
        &["branch", "-D", &workspace::task_branch_name(id)],
    );
    let mut baseline = None;
    for pass in 1..=20 {
        fixture.dispatcher.reconcile_all().await.unwrap();
        fixture.dispatcher.drain_steps().await.unwrap();
        let after = bookkeeping(&fixture.db, id).await;
        if pass == 2 {
            baseline = Some(after);
        }
        if pass > 2 {
            assert_eq!(Some(after), baseline);
        }
    }
    let parked = TaskRepo::get_by_id(&*fixture.db, id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(parked.status, "merge_failed");
    let park: String =
        sqlx::query_scalar("SELECT reason_json FROM task_schedule_park WHERE task_id=?")
            .bind(id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    let park: super::super::next_step::Park = serde_json::from_str(&park).unwrap();
    assert_eq!(park.owner, super::super::next_step::Owner::ProjectAgent);
    assert_eq!(park.recovery, super::super::next_step::Action::AssignRole);
    fixture
        .dispatcher
        .task_service
        .perform_task_action(
            id,
            api_types::TaskAction::Cancel { reason: None },
            parked.version,
        )
        .await
        .unwrap();
    fixture.dispatcher.drain_steps().await.unwrap();
    assert_eq!(
        TaskRepo::get_by_id(&*fixture.db, id, false)
            .await
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
}

async fn refresh_bridge(db: &db::SqliteDb, id: &str) {
    TransitionLogRepo::insert(
        db,
        db::CreateTransitionLog {
            id: new_uuid_v4(),
            task_id: id.to_owned(),
            from_state: "merging".into(),
            to_state: "merge_failed".into(),
            trigger_name: None,
            triggered_by: Actor::system(SystemComponent::Workflow).display(),
            bridge: api_types::TransitionBridge::new(
                api_types::TransitionBridgeKind::ReviewRefresh,
            ),
            trigger_reason: "fresh review required".into(),
            hook_results_json: None,
            rejection: false,
            created_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
}

async fn pass(fixture: &MergeGateFixture) -> (i64, i64) {
    fixture.dispatcher.reconcile_all().await.unwrap();
    fixture.dispatcher.drain_steps().await.unwrap();
    bookkeeping(&fixture.db, &fixture.task.id).await
}

async fn current(fixture: &MergeGateFixture) -> Task {
    TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap()
}

/// An unpinned execution parked the refresh; also the workspace it named.
async fn unpinned_and_parked() -> (MergeGateFixture, String) {
    let fixture = refresh_fixture().await;
    let workspace: String =
        sqlx::query_scalar("SELECT workspace_id FROM execution WHERE task_id=?")
            .bind(&fixture.task.id)
            .fetch_one(fixture.db.pool())
            .await
            .unwrap();
    sqlx::query("UPDATE execution SET workspace_id=NULL WHERE task_id=?")
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    pass(&fixture).await;
    pass(&fixture).await;
    assert_eq!(
        current(&fixture).await.condition.read().failure_kind,
        Some(api_types::FailureKind::CascadeFailed)
    );
    (fixture, workspace)
}

/// Restart is offered on the park, and it works: the Task leaves the repair
/// state without the failure, and the dispatcher settles again.
#[tokio::test]
async fn restart_is_a_working_exit_from_the_unrecoverable_refresh_park() {
    let (fixture, _) = unpinned_and_parked().await;
    let parked = current(&fixture).await;
    fixture
        .dispatcher
        .task_service
        .perform_task_action(
            &parked.id,
            api_types::TaskAction::Restart { reason: None },
            parked.version,
        )
        .await
        .unwrap();
    fixture.dispatcher.drain_steps().await.unwrap();
    pass(&fixture).await;
    let settled = pass(&fixture).await;
    for _ in 0..10 {
        assert_eq!(pass(&fixture).await, settled);
    }
    let restarted = current(&fixture).await;
    assert_ne!(
        restarted.status, "merge_failed",
        "{:?}",
        restarted.condition
    );
    assert_ne!(
        restarted.condition.read().failure_kind,
        Some(api_types::FailureKind::CascadeFailed)
    );
}

/// The park is lifted without the Task leaving its state (the owner cleared
/// the failure). The ended attempt must not swallow the next one: a repaired
/// cause advances to review, an unrepaired one fails once more and parks
/// again. Neither leaves the Task with no step and no park.
#[tokio::test]
async fn a_lifted_park_gets_one_new_attempt_that_advances_or_parks_again() {
    for repaired in [true, false] {
        let (fixture, workspace) = unpinned_and_parked().await;
        let id = &fixture.task.id;
        if repaired {
            sqlx::query("UPDATE execution SET workspace_id=? WHERE task_id=?")
                .bind(&workspace)
                .bind(id)
                .execute(fixture.db.pool())
                .await
                .unwrap();
        }
        let steps = bookkeeping(&fixture.db, id).await.0;
        let mut tx = db::begin_immediate(fixture.db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET error_annotation=NULL, version=version+1 WHERE id=?")
            .bind(id)
            .execute(&mut *tx)
            .await
            .unwrap();
        fixture.db.sync_condition_in_tx(&mut tx, id).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            current(&fixture).await.condition.read().failure_kind,
            None,
            "the park is lifted"
        );
        pass(&fixture).await;
        let settled = pass(&fixture).await;
        for _ in 0..10 {
            assert_eq!(pass(&fixture).await, settled, "repaired={repaired}");
        }
        let task = current(&fixture).await;
        let reviewed = TransitionLogRepo::list_by_task(&*fixture.db, id)
            .await
            .unwrap()
            .iter()
            .any(|entry| entry.from_state == "merge_failed" && entry.to_state == "review");
        if repaired {
            assert!(reviewed, "{:?}", task.condition);
        } else {
            assert!(!reviewed);
            assert_eq!(task.status, "merge_failed");
            assert_eq!(
                task.condition.read().failure_kind,
                Some(api_types::FailureKind::CascadeFailed)
            );
            // Exactly one more attempt, not one per pass.
            assert!(
                settled.0 > steps && settled.0 <= steps + 2,
                "{steps} -> {settled:?}"
            );
        }
    }
}

/// The Task's machine is away: that is a wait for the machine, with no
/// recovery issued, no failure recorded and nothing written per pass.
#[tokio::test]
async fn a_refresh_whose_machine_is_away_waits_for_the_machine() {
    let db = Arc::new(sqlite_db().await);
    let (task, placement, execution) = crate::recovery::tests::daemon_owned_fixture(&db).await;
    assert_eq!(placement.state, db::PlacementState::Disconnected);
    sqlx::query("UPDATE execution SET status='completed' WHERE id=?")
        .bind(&execution.id)
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='merge_failed', version=version+1 WHERE id=?")
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    refresh_bridge(&db, &task.id).await;
    let bus = Arc::new(EventBus::new(16));
    let service = Arc::new(TaskService::new(db.clone(), bus.clone()));
    let dispatcher = TaskDispatcher::new(db.clone(), bus, service);
    let mut settled = None;
    for pass in 1..=20 {
        dispatcher.reconcile_all().await.unwrap();
        dispatcher.drain_steps().await.unwrap();
        let after = bookkeeping(&db, &task.id).await;
        if pass == 2 {
            settled = Some(after);
        } else if pass > 2 {
            assert_eq!(Some(after), settled, "pass {pass}");
        }
    }
    let waiting = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(waiting.status, "merge_failed");
    assert_eq!(waiting.condition.read().failure_kind, None);
    assert!(!db::material_blocker(&waiting.condition).requires_intervention);
    let steps = db::TaskStepRepo::task_steps(&*db, &task.id).await.unwrap();
    assert!(
        steps.iter().all(|step| step.kind != "cascade"),
        "{:?}",
        steps
            .iter()
            .map(|s| (&s.kind, &s.status))
            .collect::<Vec<_>>()
    );
    let park: String =
        sqlx::query_scalar("SELECT reason_json FROM task_schedule_park WHERE task_id=?")
            .bind(&task.id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    let park: super::super::next_step::Park = serde_json::from_str(&park).unwrap();
    assert_eq!(
        park.owner,
        super::super::next_step::Owner::Machine,
        "{park:?}"
    );
}
