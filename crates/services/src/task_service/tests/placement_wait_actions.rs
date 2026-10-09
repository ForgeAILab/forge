use super::helpers::*;
use super::*;
use crate::placement::{CandidateRejection, PlacementFilterCode, PlacementUnavailable};
use api_types::{FailureKind, TaskAction};

struct PendingExecutor;

#[async_trait::async_trait]
impl TaskExecutor for PendingExecutor {
    async fn execute(
        &self,
        _context: executors::ExecutionContext,
    ) -> std::result::Result<executors::ExecutionResult, executors::ExecutorError> {
        std::future::pending().await
    }
    async fn cancel(
        &self,
        _execution_id: &str,
    ) -> std::result::Result<(), executors::ExecutorError> {
        Ok(())
    }
}

struct Fixture {
    db: Arc<SqliteDb>,
    service: TaskService,
    project: String,
    task: Task,
    _repo: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo);
    let agent = super::condition_scenarios::scenario_agent(&db).await;
    let task = seed_task_with_status(&db, &project, "in_progress").await;
    seed_role_assignment(&db, &task.id, "coder", Some(&agent)).await;
    let service = TaskService::new_for_test(db.clone(), Arc::new(EventBus::new(64)))
        .with_workspace_root(repo.path().join("workspaces"))
        .with_task_executor(Arc::new(PendingExecutor));
    Fixture {
        db,
        service,
        project,
        task,
        _repo: repo,
    }
}

async fn reload(fixture: &Fixture) -> Task {
    TaskRepo::get_by_id(&*fixture.db, &fixture.task.id, false)
        .await
        .unwrap()
        .unwrap()
}

fn refusal(task: &Task, code: PlacementFilterCode) -> ServiceError {
    ServiceError::PlacementUnavailable(PlacementUnavailable {
        task_id: task.id.clone(),
        repo_id: "fixture-repository".to_owned(),
        rejected_candidates: vec![CandidateRejection {
            failing_checks: vec!["tooling".to_owned()],
            repo_location_id: "fixture-location".to_owned(),
            owner_kind: "server".to_owned(),
            daemon_id: None,
            runtime_id: None,
            filter_codes: vec![code],
        }],
    })
}

async fn queued(fixture: &Fixture) -> Task {
    sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
        .bind(json!({"type":"executor_failed","blocking_reason":"original failure"}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture
        .db
        .check_task_conditions_of(std::slice::from_ref(&fixture.task.id))
        .await
        .unwrap();
    fixture
        .service
        .perform_task_action(&fixture.task.id, TaskAction::retry(), fixture.task.version)
        .await
        .unwrap()
        .task
}

#[tokio::test]
async fn scheduling_waits_offer_hold_cancel_and_hold_parks_without_launching() {
    for kind in ["machine", "environment", "probe", "project", "queued"] {
        let fixture = fixture().await;
        match kind {
            "machine" => {
                crate::deferred_dispatch::record_dispatch_disposition(
                    &fixture.db,
                    &fixture.task,
                    "machine_capacity",
                    "machine_capacity: waiting for a machine run slot",
                )
                .await
                .unwrap();
            }
            "project" => {
                sqlx::query("UPDATE project SET paused_at = ?, system_pause_reason = 'environment_not_ready' WHERE id = ?")
                    .bind(now_rfc3339()).bind(&fixture.project).execute(fixture.db.pool()).await.unwrap();
            }
            "queued" => {
                queued(&fixture).await;
            }
            _ => {
                let value = if kind == "environment" {
                    json!({"environment_wait":{"machine":db::EnvironmentMachine::Server,
                        "checks":["tooling"]}})
                } else {
                    json!({"deferred_dispatch":{"kind":"environment_probe_pending",
                        "not_before":(chrono::Utc::now()+chrono::Duration::seconds(30)).to_rfc3339(),
                        "target_state":fixture.task.status,"reason":"environment probe pending"}})
                };
                sqlx::query("UPDATE task SET metadata_json = ? WHERE id = ?")
                    .bind(value.to_string())
                    .bind(&fixture.task.id)
                    .execute(fixture.db.pool())
                    .await
                    .unwrap();
                fixture
                    .db
                    .check_task_conditions_of(std::slice::from_ref(&fixture.task.id))
                    .await
                    .unwrap();
            }
        }
        let task = reload(&fixture).await;
        let snapshot = fixture
            .service
            .task_action_snapshot(&task.id, &Actor::user(UserActionSource::Test))
            .await
            .unwrap();
        assert_eq!(snapshot.condition(), None, "{kind}");
        let offers = crate::available_actions(&snapshot);
        assert_eq!(
            offers
                .iter()
                .map(|offer| offer.action.verb())
                .collect::<Vec<_>>(),
            ["cancel", "hold"],
            "{kind}"
        );
        let held = fixture
            .service
            .perform_task_action(
                &task.id,
                TaskAction::Hold {
                    reason: Some("Wait for the operator".to_owned()),
                },
                task.version,
            )
            .await
            .unwrap()
            .task;
        assert_eq!(held.status, task.status);
        assert_eq!(
            crate::task_actions::task_condition(&held),
            Some(FailureKind::ManualStop)
        );
        assert!(crate::deferred_dispatch::queued_recovery(&held).is_none());
        assert!(ExecutionRepo::list_running_by_task(&*fixture.db, &task.id)
            .await
            .unwrap()
            .is_empty());
        let metadata = db::TaskMetadata::parse(held.metadata_json.as_deref()).unwrap();
        for key in [
            "queued_recovery",
            "environment_wait",
            "deferred_dispatch",
            "dispatch_disposition",
        ] {
            assert!(!metadata.extra.contains_key(key), "{kind}: {key}");
        }
    }
}

#[tokio::test]
async fn deterministic_placement_refusal_has_an_applyable_reattempt() {
    let fixture = fixture().await;
    let error = refusal(&fixture.task, PlacementFilterCode::CapabilityMissing);
    assert!(fixture
        .service
        .record_placement_dispatch_refusal(&fixture.task, &error)
        .await
        .unwrap());
    let task = reload(&fixture).await;
    let offers = fixture
        .service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    let offer = offers
        .available_actions
        .iter()
        .find(|offer| offer.reason == "placement_retry")
        .unwrap();
    assert_eq!(offer.action.verb(), "retry");
    assert!(offers
        .available_actions
        .iter()
        .any(|offer| offer.action.verb() == "restart"));
    let mut action = offer.action.clone();
    if let TaskAction::Retry { reason, .. } = &mut action {
        *reason = Some("Recheck the updated owner".to_owned());
    }
    let retried = fixture
        .service
        .perform_task_action(&task.id, action, task.version)
        .await
        .unwrap()
        .task;
    assert!(retried.error_annotation.is_none());
    let metadata = db::TaskMetadata::parse(retried.metadata_json.as_deref()).unwrap();
    assert!(!metadata.extra.contains_key("placement_refusal"));
    assert!(!metadata.extra.contains_key("dispatch_disposition"));
    assert!(ExecutionRepo::list_running_by_task(&*fixture.db, &task.id)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn replay_capacity_and_environment_refusals_wait_but_deterministic_refusals_restore() {
    for code in [
        PlacementFilterCode::MachineCapacity,
        PlacementFilterCode::EnvironmentProbePending,
        PlacementFilterCode::EnvironmentNotReady,
        PlacementFilterCode::CapabilityMissing,
    ] {
        let fixture = fixture().await;
        let task = queued(&fixture).await;
        let marker = crate::deferred_dispatch::queued_recovery(&task).unwrap();
        let error = refusal(&task, code);
        let waiting = fixture
            .service
            .settle_queued_task_action_refusal(&task, &error)
            .await
            .unwrap();
        let current = reload(&fixture).await;
        if code == PlacementFilterCode::CapabilityMissing {
            assert!(!waiting);
            assert!(crate::deferred_dispatch::queued_recovery(&current).is_none());
            let annotation: Value =
                serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
            assert_eq!(annotation["type"], "executor_failed");
            assert_eq!(annotation["message"], error.to_string());
            assert!(fixture
                .service
                .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
                .await
                .unwrap()
                .available_actions
                .iter()
                .any(|offer| offer.action.verb() != "cancel"));
        } else {
            assert!(waiting);
            assert_eq!(current.version, task.version);
            assert_eq!(
                crate::deferred_dispatch::queued_recovery(&current)
                    .unwrap()
                    .id,
                marker.id
            );
            assert!(
                current.error_annotation.is_none()
                    && current.blocked_json.is_none()
                    && current.failed_json.is_none()
            );
        }
    }
}

#[tokio::test]
async fn replay_project_environment_pause_keeps_the_accepted_intent() {
    let fixture = fixture().await;
    let task = queued(&fixture).await;
    sqlx::query("UPDATE project SET paused_at = ?, system_pause_reason = 'environment_not_ready' WHERE id = ?")
        .bind(now_rfc3339()).bind(&fixture.project).execute(fixture.db.pool()).await.unwrap();
    let error = ServiceError::ProjectPaused {
        project_id: fixture.project.clone(),
    };
    assert!(fixture
        .service
        .settle_queued_task_action_refusal(&task, &error)
        .await
        .unwrap());
    assert_eq!(reload(&fixture).await, task);
}

#[tokio::test]
async fn expired_environment_probe_deferral_is_claimed_without_losing_its_extra_fields() {
    let fixture = fixture().await;
    let task = queued(&fixture).await;
    let deferred = json!({"kind":"environment_probe_pending",
        "reason":"environment probe pending", "target_state":task.status,
        "not_before":(chrono::Utc::now()-chrono::Duration::seconds(1)).to_rfc3339()});
    sqlx::query("UPDATE task SET metadata_json = json_set(metadata_json, '$.deferred_dispatch', json(?)) WHERE id = ?")
        .bind(deferred.to_string()).bind(&task.id).execute(fixture.db.pool()).await.unwrap();
    fixture
        .db
        .check_task_conditions_of(std::slice::from_ref(&task.id))
        .await
        .unwrap();
    let task = reload(&fixture).await;
    assert!(fixture
        .service
        .dispatch_queued_recovery(&task)
        .await
        .unwrap());
    let current = reload(&fixture).await;
    assert!(crate::deferred_dispatch::queued_recovery(&current).is_none());
    let running = ExecutionRepo::list_running_by_task(&*fixture.db, &task.id)
        .await
        .unwrap();
    assert_eq!(running.len(), 1);
    fixture
        .service
        .stop_execution(&running[0].id, "fixture cleanup".to_owned())
        .await
        .unwrap();
}

#[tokio::test]
async fn running_task_with_paused_agent_offers_the_execution_hold() {
    let fixture = fixture().await;
    let agent = sqlx::query_scalar::<_, String>(
        "SELECT assignee_id FROM task_role_assignment WHERE task_id = ? AND role_name = 'coder'",
    )
    .bind(&fixture.task.id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    seed_execution(
        &fixture.db,
        &fixture.task.id,
        Some(&agent),
        "coder",
        ExecutionStatus::Running,
        Some("session"),
        "2026-10-02T00:00:00Z",
    )
    .await;
    sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = ?")
        .bind(&agent)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    let owner = Actor::user(UserActionSource::Test);
    let offers = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    let holds = offers
        .available_actions
        .iter()
        .filter(|offer| offer.action.verb() == "hold")
        .map(|offer| offer.reason.as_str())
        .collect::<Vec<_>>();
    assert_eq!(holds, ["execution_running"]);

    let current = reload(&fixture).await;
    fixture
        .service
        .perform_task_action_as(
            &fixture.task.id,
            TaskAction::Hold { reason: None },
            current.version,
            owner.clone(),
        )
        .await
        .expect("the offered Hold applies to a running Task");
    let held = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(!held
        .available_actions
        .iter()
        .any(|offer| offer.action.verb() == "hold"));
}

/// Plan 3.1 stage 5a, per-writer equivalence. The writer under test stated
/// its condition: the stored condition carries no copy of the legacy fields,
/// equals what the legacy mapping says of the fields the writer just wrote
/// (the only differences are the ones `typed()` removes: the legacy copies
/// and the `stated` mark), reads the same publicly, and is left alone by the
/// background check.
async fn assert_stated_as_the_legacy_mapping(
    fixture: &Fixture,
    context: &str,
) -> db::TaskCondition {
    let row = reload(fixture).await;
    let mut connection = fixture.db.pool().acquire().await.unwrap();
    let mut input = db::LegacyConditionInput::from(&row);
    input.facts = Some(
        db::ConditionFacts::load(&mut connection, &row.id)
            .await
            .unwrap(),
    );
    drop(connection);
    let mapped = db::map_legacy_condition(&input);
    let stored = row.condition.clone();
    let evidence = stored.evidence();
    assert!(evidence.stated, "{context}: {stored:?}");
    assert!(
        evidence.error_annotation.is_none()
            && evidence.blocked_json.is_none()
            && evidence.failed_json.is_none()
            && evidence.entry_barrier_json.is_none()
            && evidence.metadata.is_empty()
            && evidence.unparsed_metadata.is_none(),
        "{context}: no legacy copy"
    );
    assert_eq!(stored.typed(), mapped.typed(), "{context}");
    assert_eq!(stored.public(), mapped.public(), "{context}");
    assert_eq!(
        db::material_blocker(&stored),
        db::material_blocker(&mapped),
        "{context}"
    );
    let repaired = fixture.db.condition_check_status().repaired;
    fixture
        .db
        .check_task_conditions(db::CONDITION_CHECK_PAGE)
        .await
        .unwrap();
    assert_eq!(
        fixture.db.condition_check_status().repaired,
        repaired,
        "{context}"
    );
    assert_eq!(reload(fixture).await.condition, stored, "{context}");
    stored
}

#[tokio::test]
async fn waiting_task_held_without_an_agent_can_be_released_to_the_queue() {
    let fixture = fixture().await;
    sqlx::query("UPDATE agent_identity SET paused = 1")
        .execute(fixture.db.pool())
        .await
        .unwrap();
    let owner = Actor::user(UserActionSource::Test);
    let verbs = |offers: &api_types::TaskActionsResponse| {
        offers
            .available_actions
            .iter()
            .map(|offer| format!("{}[{}]", offer.action.verb(), offer.reason))
            .collect::<Vec<_>>()
    };

    let waiting = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(verbs(&waiting).contains(&"hold[dispatch_wait]".to_owned()));
    let current = reload(&fixture).await;
    fixture
        .service
        .perform_task_action_as(
            &fixture.task.id,
            TaskAction::Hold { reason: None },
            current.version,
            owner.clone(),
        )
        .await
        .unwrap();

    let stated = assert_stated_as_the_legacy_mapping(&fixture, "hold").await;
    assert!(matches!(
        &stated,
        db::TaskCondition::Parked { primary: db::ParkReason::Held { actor }, .. } if actor == "user"
    ));
    let row = reload(&fixture).await;
    assert!(
        row.blocked_json.is_some(),
        "the legacy fields are still written"
    );
    // What the operator reads is the legacy annotation, byte for byte.
    assert_eq!(stated.read().operator_reason, row.error_annotation);

    let held = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(
        verbs(&held).contains(&"release[held_waiting]".to_owned()),
        "{:?}",
        verbs(&held)
    );
    let current = reload(&fixture).await;
    fixture
        .service
        .perform_task_action_as(
            &fixture.task.id,
            TaskAction::Release { reason: None },
            current.version,
            owner.clone(),
        )
        .await
        .unwrap();
    assert!(reload(&fixture).await.error_annotation.is_none());
    let stated = assert_stated_as_the_legacy_mapping(&fixture, "release").await;
    assert!(
        !stated
            .reasons()
            .any(|reason| matches!(reason, db::ParkReason::Held { .. })),
        "{stated:?}"
    );
    assert!(
        ExecutionRepo::list_running_by_task(&*fixture.db, &fixture.task.id)
            .await
            .unwrap()
            .is_empty()
    );
    let after = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(verbs(&after).contains(&"hold[dispatch_wait]".to_owned()));
}

/// A hold placed on a Task that has a queued action and a cancelled
/// dependency used to overwrite the `dependency_cancelled` blocker and the
/// condition it carried. The hold now waits behind the blocker, and
/// resolving the blocker leaves the Task held with its release on offer.
#[tokio::test]
async fn hold_over_a_cancelled_dependency_keeps_the_blocker_and_survives_its_removal() {
    let fixture = fixture().await;
    let owner = Actor::user(UserActionSource::Test);
    let verbs = |offers: &api_types::TaskActionsResponse| {
        offers
            .available_actions
            .iter()
            .map(|offer| format!("{}[{}]", offer.action.verb(), offer.reason))
            .collect::<Vec<_>>()
    };
    let queued = queued(&fixture).await;
    let blocked = fixture
        .service
        .block_cancelled_dependencies(&queued, &["prerequisite".to_owned()])
        .await
        .unwrap();
    let offers = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(
        verbs(&offers).contains(&"hold[dispatch_wait]".to_owned()),
        "{:?}",
        verbs(&offers)
    );
    fixture
        .service
        .perform_task_action_as(
            &fixture.task.id,
            TaskAction::Hold { reason: None },
            blocked.version,
            owner.clone(),
        )
        .await
        .unwrap();

    let held = reload(&fixture).await;
    assert!(
        held.error_annotation
            .as_deref()
            .is_some_and(|annotation| annotation.contains("dependency_cancelled")),
        "the hold displaced the dependency blocker: {:?}",
        held.error_annotation
    );
    let stash: serde_json::Value =
        serde_json::from_str(held.blocked_json.as_deref().unwrap()).unwrap();
    assert!(
        stash["details"]["superseded"]["error_annotation"]
            .as_str()
            .is_some_and(|annotation| annotation.contains("manual_stop")),
        "the blocker does not carry the hold: {stash}"
    );
    assert!(
        crate::deferred_dispatch::queued_recovery(&held).is_none(),
        "a hold drops the queued action"
    );
    let offers = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert_eq!(verbs(&offers), ["cancel[cancellable]"]);

    // The dependency is gone (no such edge is stored): the blocker resolves
    // and the hold it carried is the Task's condition again.
    fixture
        .service
        .remove_task_dependency(&fixture.task.id, "prerequisite")
        .await
        .unwrap();
    let restored = reload(&fixture).await;
    assert!(
        restored
            .error_annotation
            .as_deref()
            .is_some_and(|annotation| annotation.contains("manual_stop")),
        "{:?}",
        restored.error_annotation
    );
    let offers = fixture
        .service
        .task_action_offers(&fixture.task.id, &owner)
        .await
        .unwrap();
    assert!(
        verbs(&offers)
            .iter()
            .any(|verb| verb.starts_with("release[")),
        "{:?}",
        verbs(&offers)
    );
}

async fn parked_restart_fixture(other_owner: bool) -> (Fixture, Task) {
    let fixture = fixture().await;
    let repo_id: String = sqlx::query_scalar("SELECT id FROM repo WHERE project_id=?")
        .bind(&fixture.project)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,registration_token_hash,created_at,updated_at) VALUES ('dead-machine','dead-machine','Lost workstation','linux','x86_64','offline','hash',?,?)")
        .bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
    // A cleaned historical workspace can still have FK-free daemon cleanup.
    // Restart must proceed without resetting that retired owner's workspace.
    db::WorkspaceRepo::create(
        &*fixture.db,
        db::CreateWorkspace {
            id: "historical-workspace".into(),
            task_id: fixture.task.id.clone(),
            repo_id,
            worktree_path: fixture
                ._repo
                .path()
                .join("old-worktree")
                .to_string_lossy()
                .into_owned(),
            branch: "old-branch".into(),
            status: db::WorkspaceStatus::Cleaned,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO pending_remote_cancel VALUES ('lost-operation','lost-step','historical-workspace','lost-placement','dead-machine','lost-runtime',1,0,?)")
        .bind(&now).execute(fixture.db.pool()).await.unwrap();
    if other_owner {
        sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,created_at,updated_at) VALUES ('other-machine','other-machine','Other workstation','linux','x86_64','offline',?,?)")
            .bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
        sqlx::query("INSERT INTO pending_remote_cancel VALUES ('other-operation','other-step','historical-workspace','other-placement','other-machine','other-runtime',1,0,?)")
            .bind(&now).execute(fixture.db.pool()).await.unwrap();
    }
    sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
        .bind(json!({"type":"executor_failed","blocking_reason":"original failure"}).to_string())
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture
        .db
        .check_task_conditions_of(std::slice::from_ref(&fixture.task.id))
        .await
        .unwrap();
    let parked = fixture
        .service
        .perform_task_action(
            &fixture.task.id,
            TaskAction::Restart { reason: None },
            fixture.task.version,
        )
        .await
        .unwrap()
        .task;
    assert_eq!(parked.status, "in_progress");
    assert!(crate::deferred_dispatch::queued_recovery(&parked).is_some());
    assert!(parked
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("Lost workstation"));
    (fixture, parked)
}

/// Removal queues every Task change: the row is untouched. Only the stored
/// condition's internal witness of the cancellation leaves with the pending
/// row it witnessed, and what the condition says publicly does not move.
fn assert_unchanged_but_for_the_cancel_witness(after: &Task, parked: &Task) {
    assert_eq!(
        Task {
            condition: parked.condition.clone(),
            ..after.clone()
        },
        *parked,
        "removal queues every Task change"
    );
    assert_eq!(
        serde_json::to_value(after.condition.public()).unwrap(),
        serde_json::to_value(parked.condition.public()).unwrap()
    );
}

#[tokio::test]
async fn remove_machine_clears_pending_cancel_and_replays_parked_restart() {
    let (fixture, parked) = parked_restart_fixture(false).await;
    fixture
        .db
        .remove_daemon("dead-machine", "admin", true, "local", false)
        .await
        .unwrap();
    assert!(!fixture
        .db
        .task_has_pending_remote_cancel(&fixture.task.id)
        .await
        .unwrap());
    assert_unchanged_but_for_the_cancel_witness(&reload(&fixture).await, &parked);
    let current = fixture.service.drain(&fixture.task.id).await.unwrap();
    assert_eq!(current.status, "todo");
    assert!(current.error_annotation.is_none());
    assert!(crate::deferred_dispatch::queued_recovery(&current).is_none());
    assert!(db::TaskStepRepo::task_steps(&*fixture.db, &current.id)
        .await
        .unwrap()
        .iter()
        .any(|step| step.kind == "command"
            && step.payload_json.contains("settle_removed_machine")
            && step.status == "done"
            && !step.entry_fenced));
}

#[tokio::test]
async fn remove_machine_keeps_another_owners_fence_and_refreshes_the_machine_hint() {
    let (fixture, _) = parked_restart_fixture(true).await;
    fixture
        .db
        .remove_daemon("dead-machine", "admin", true, "local", false)
        .await
        .unwrap();
    let current = fixture.service.drain(&fixture.task.id).await.unwrap();
    assert_eq!(current.status, "in_progress");
    assert!(crate::deferred_dispatch::queued_recovery(&current).is_some());
    let annotation: Value =
        serde_json::from_str(current.error_annotation.as_deref().unwrap()).unwrap();
    assert_eq!(annotation["blocking_reason"], "pending_remote_cancel");
    let message = annotation["message"].as_str().unwrap();
    assert!(message.contains("Other workstation"));
    assert!(!message.contains("Lost workstation"));
    let pending = fixture.db.pending_remote_cancels(None, None).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].daemon_id, "other-machine");
}

/// The Task's own live workspace on a machine that will never return.
async fn live_workspace_on_dead_machine(fixture: &Fixture, state: db::PlacementState) -> String {
    let repo_id: String = sqlx::query_scalar("SELECT id FROM repo WHERE project_id=?")
        .bind(&fixture.project)
        .fetch_one(fixture.db.pool())
        .await
        .unwrap();
    let now = now_rfc3339();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,registration_token_hash,created_at,updated_at) VALUES ('dead-machine','dead-machine','Lost workstation','linux','x86_64','offline','hash',?,?)")
        .bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO runtime(id,daemon_id,kind,workspace_root,status,created_at,updated_at) VALUES ('dead-runtime','dead-machine','local','/owner-only','ready',?,?)")
        .bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,daemon_id,runtime_id,path,kind,is_default,status,created_at,updated_at) VALUES ('dead-location',?,'daemon','dead-machine','dead-runtime','/owner-only/repo','managed_clone',0,'ready',?,?)")
        .bind(&repo_id).bind(&now).bind(&now).execute(fixture.db.pool()).await.unwrap();
    db::WorkspaceRepo::create(
        &*fixture.db,
        db::CreateWorkspace {
            id: "live-workspace".into(),
            task_id: fixture.task.id.clone(),
            repo_id,
            worktree_path: String::new(),
            branch: ::workspace::task_branch_name(&fixture.task.id),
            status: db::WorkspaceStatus::Ready,
            before_sha: Some("base-head".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    db::WorkspacePlacementRepo::create(
        &*fixture.db,
        db::CreateWorkspacePlacement {
            id: "live-placement".into(),
            workspace_id: "live-workspace".into(),
            task_id: fixture.task.id.clone(),
            agent_id: None,
            owner_kind: db::PlacementOwnerKind::Daemon,
            daemon_id: Some("dead-machine".into()),
            runtime_id: Some("dead-runtime".into()),
            repo_location_id: "dead-location".into(),
            execution_daemon_id: Some("dead-machine".into()),
            workspace_handle: Some("opaque-owner-handle".into()),
            generation: 1,
            disconnected_at: (state == db::PlacementState::Disconnected).then(|| now.clone()),
            failure_cause: None,
            state,
            selected_by: db::PlacementSelectedBy::Scheduler,
            selection_reason: "{}".into(),
            reserved_until: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    "live-placement".to_owned()
}

async fn coder(fixture: &Fixture) -> Agent {
    let id: String = sqlx::query_scalar(
        "SELECT assignee_id FROM task_role_assignment WHERE task_id=? AND role_name='coder'",
    )
    .bind(&fixture.task.id)
    .fetch_one(fixture.db.pool())
    .await
    .unwrap();
    AgentRepo::get_by_id(&*fixture.db, &id)
        .await
        .unwrap()
        .unwrap()
}

async fn offer_reasons(fixture: &Fixture) -> Vec<String> {
    let snapshot = fixture
        .service
        .task_action_snapshot(&fixture.task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap();
    crate::available_actions(&snapshot)
        .into_iter()
        .map(|offer| format!("{}[{}]", offer.action.verb(), offer.reason))
        .collect()
}

/// The brief's primary case on the Task's own live workspace: a Restart parked
/// behind a remote cancel the removed machine can never confirm.
#[tokio::test]
async fn remove_machine_replays_a_restart_parked_on_the_tasks_live_workspace_and_re_places_it() {
    let fixture = fixture().await;
    let agent = coder(&fixture).await;
    let placement_id =
        live_workspace_on_dead_machine(&fixture, db::PlacementState::Disconnected).await;
    sqlx::query("INSERT INTO pending_remote_cancel VALUES ('lost-operation','lost-step','live-workspace',?,'dead-machine','dead-runtime',1,0,?)")
        .bind(&placement_id).bind(now_rfc3339()).execute(fixture.db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET error_annotation=? WHERE id=?")
        .bind(
            json!({"type":"workspace_reset_required","blocking_reason":"original failure"})
                .to_string(),
        )
        .bind(&fixture.task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture
        .db
        .check_task_conditions_of(std::slice::from_ref(&fixture.task.id))
        .await
        .unwrap();
    let parked = fixture
        .service
        .perform_task_action(
            &fixture.task.id,
            TaskAction::Restart { reason: None },
            fixture.task.version,
        )
        .await
        .unwrap()
        .task;
    assert!(crate::deferred_dispatch::queued_recovery(&parked).is_some());
    assert!(parked
        .error_annotation
        .as_deref()
        .unwrap()
        .contains("Lost workstation"));

    let result = fixture
        .db
        .remove_daemon("dead-machine", "admin", true, "local", false)
        .await
        .unwrap();
    assert_eq!(result.pending_remote_cancels_cleared, 1);
    assert_eq!(result.placements_failed, 1);
    assert_eq!(result.tasks_to_replace, 1);
    assert_unchanged_but_for_the_cancel_witness(&reload(&fixture).await, &parked);

    let current = fixture.service.drain(&fixture.task.id).await.unwrap();
    assert_eq!(current.status, "todo", "the parked Restart proceeded");
    assert!(current.error_annotation.is_none());
    assert!(crate::deferred_dispatch::queued_recovery(&current).is_none());
    let released = db::WorkspacePlacementRepo::get_by_id(&*fixture.db, &placement_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(released.state, db::PlacementState::Cleaned);
    assert!(released.workspace_handle.is_none());
    let offers = offer_reasons(&fixture).await;
    assert!(
        !offers.iter().any(|offer| offer.contains("owner_reconcile")),
        "no retry on a removed owner: {offers:?}"
    );

    // The next admission selects another owner for the same Task workspace.
    let admission = fixture
        .service
        .reserve_claim_workspace(&current, Some(&agent), "coder")
        .await
        .unwrap();
    assert_eq!(admission.placement.id, placement_id);
    assert_eq!(
        admission.placement.owner_kind,
        db::PlacementOwnerKind::Server
    );
    assert_eq!(admission.placement.daemon_id, None);
    assert_eq!(admission.placement.state, db::PlacementState::Reserved);
    assert_eq!(admission.placement.generation, 2);
}

/// A run that was live on the removed machine fails once, attributed to the
/// removing user, and the Task is admitted again on another machine.
#[tokio::test]
async fn remove_machine_fails_the_live_run_and_retries_the_task_on_another_machine() {
    let fixture = fixture().await;
    let agent = coder(&fixture).await;
    let placement_id = live_workspace_on_dead_machine(&fixture, db::PlacementState::Ready).await;
    let lost = seed_execution(
        &fixture.db,
        &fixture.task.id,
        Some(&agent.id),
        "coder",
        ExecutionStatus::Running,
        None,
        &now_rfc3339(),
    )
    .await;
    sqlx::query("UPDATE execution SET workspace_id='live-workspace' WHERE id=?")
        .bind(&lost.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();

    let result = fixture
        .db
        .remove_daemon("dead-machine", "owner-user", true, "local", false)
        .await
        .unwrap();
    assert_eq!(result.tasks_to_replace, 1);
    assert_eq!(result.agents_retired, 0);
    let current = fixture.service.drain(&fixture.task.id).await.unwrap();

    let failed = ExecutionRepo::get_by_id(&*fixture.db, &lost.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, ExecutionStatus::Failed);
    assert_eq!(failed.stop_reason, Some(db::StopReason::DaemonDisconnected));
    assert_eq!(failed.stopped_by.as_deref(), Some("user:api"));
    assert!(failed.error.as_deref().unwrap().contains("machine_removed"));

    let offers = offer_reasons(&fixture).await;
    assert!(
        !offers.iter().any(|offer| offer.contains("owner_reconcile")),
        "no retry on a removed owner: {offers:?}"
    );
    let placement = db::WorkspacePlacementRepo::get_by_id(&*fixture.db, &placement_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (placement.owner_kind.clone(), placement.daemon_id.clone()),
        (db::PlacementOwnerKind::Server, None),
        "status={} annotation={:?} placement={:?}/{:?} offers={offers:?}",
        current.status,
        current.error_annotation,
        placement.state,
        placement.failure_cause
    );
    let running = ExecutionRepo::list_running_by_task(&*fixture.db, &fixture.task.id)
        .await
        .unwrap();
    assert_eq!(running.len(), 1, "the Task restarted");
    assert_ne!(running[0].id, lost.id);
    assert_eq!(running[0].workspace_id.as_deref(), Some("live-workspace"));
}
