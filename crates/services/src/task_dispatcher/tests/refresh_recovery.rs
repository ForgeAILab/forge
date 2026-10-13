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
