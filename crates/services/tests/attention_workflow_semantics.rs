use std::sync::Arc;

use api_types::{
    Actor, CanonicalPhase, StateKind, TaskTransitionEventPayload, UserActionSource,
    WorkflowDefinition,
};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, CreateDomainEvent, CreateProject,
    CreateTask, DomainEventRepo, ProjectRepo, SqliteDb, TaskBoardRepo, TaskRepo, UpdateTaskStatus,
};
use services::{
    workflow::{
        default_workflow::default_workflow,
        engine::{BoardMoveRequest, WorkflowEngine},
    },
    AttentionService,
};

fn renamed_workflow() -> WorkflowDefinition {
    let mut workflow = default_workflow();
    let rename = |name: &str| match name {
        "done" => "shipped".to_owned(),
        "cancelled" => "abandoned".to_owned(),
        "review" => "approval".to_owned(),
        other => other.to_owned(),
    };
    for state in &mut workflow.states {
        state.name = rename(&state.name);
        state.hooks = api_types::StateHooks::default();
        for trigger in state.triggers.values_mut() {
            trigger.to = rename(&trigger.to);
        }
        if let Some(gate) = &mut state.gate_config {
            gate.reject_target = gate.reject_target.as_deref().map(rename);
            if state.name == "approval" {
                gate.requires_user_approval = Some(true);
            }
        }
    }
    workflow.cancellation_state = Some("abandoned".to_owned());
    workflow
}

struct Fixture {
    db: Arc<SqliteDb>,
    project_id: String,
}

impl Fixture {
    async fn new(workflow: WorkflowDefinition) -> Self {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let now = now_rfc3339();
        let project = ProjectRepo::create(
            &*db,
            CreateProject {
                id: new_uuid_v4(),
                name: "Custom workflow Attention".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: serde_json::to_string(&workflow).unwrap(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        Self {
            db,
            project_id: project.id,
        }
    }

    async fn task(&self, status: &str, parent_task_id: Option<String>) -> db::Task {
        let now = now_rfc3339();
        TaskRepo::create(
            &*self.db,
            CreateTask {
                id: new_uuid_v4(),
                project_id: self.project_id.clone(),
                repo_id: None,
                parent_task_id,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "Workflow semantics".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: status.to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap()
    }

    async fn transition_event(&self, task: &db::Task, from: &str, to: &str, actor: &str) {
        let project = ProjectRepo::get_by_id(&*self.db, &self.project_id)
            .await
            .unwrap()
            .unwrap();
        let mut source_task = task.clone();
        source_task.status = from.to_owned();
        let resolved_actor = if actor.starts_with("user:") {
            api_types::Actor::user(api_types::UserActionSource::Api)
        } else {
            api_types::Actor::system(api_types::SystemComponent::Workflow)
        };
        let workflow = services::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
            &source_task,
            &project.workflow_definition,
            &resolved_actor,
        );
        let snapshot = services::workflow::transition_event::transition_workflow_snapshot(
            &source_task,
            &workflow,
            from,
            to,
        )
        .unwrap();
        DomainEventRepo::append_event(
            &*self.db,
            CreateDomainEvent::task_transition(
                new_uuid_v4(),
                task.id.clone(),
                self.project_id.clone(),
                from,
                to,
                Some("accept"),
                actor,
                "workflow transition",
                false,
                now_rfc3339(),
                snapshot,
            ),
        )
        .await
        .unwrap();
    }

    async fn move_task(&self, task: &db::Task, target: &str) {
        TaskRepo::update_status(
            &*self.db,
            UpdateTaskStatus {
                id: task.id.clone(),
                expected_version: task.version,
                status: target.to_owned(),
                assignee_id: None,
                error_annotation: None,
                blocked_json: None,
                failed_json: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        self.transition_event(task, &task.status, target, "system:workflow")
            .await;
    }

    async fn project(&self) {
        AttentionService::new(Arc::clone(&self.db))
            .project_once(100)
            .await
            .unwrap();
    }

    async fn commit_transition(
        &self,
        task: &db::Task,
        target: &str,
        board_move: bool,
    ) -> (db::Task, TaskTransitionEventPayload, String) {
        let project = ProjectRepo::get_by_id(&*self.db, &self.project_id)
            .await
            .unwrap()
            .unwrap();
        let actor = Actor::user(UserActionSource::Api);
        let workflow =
            WorkflowEngine::resolve_workflow_for_task(task, &project.workflow_definition, &actor);
        let engine = WorkflowEngine {
            db: Arc::clone(&self.db),
            event_bus: Arc::new(events::EventBus::new(16)),
            review_runner: None,
            merge_service: None,
            cleanup_scheduler: None,
            task_executor: None,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: std::path::PathBuf::new(),
            repo_cache_locks: None,
        };
        let result = if board_move {
            engine
                .move_task(
                    &task.id,
                    target,
                    task.version,
                    &workflow,
                    &actor,
                    "snapshot test",
                    BoardMoveRequest {
                        operation_id: new_uuid_v4(),
                        project_id: self.project_id.clone(),
                        board_revision: TaskBoardRepo::board_revision(&*self.db, &self.project_id)
                            .await
                            .unwrap(),
                        target_column_statuses: vec![target.to_owned()],
                        before_id: None,
                        after_id: None,
                    },
                )
                .await
                .unwrap()
        } else {
            engine
                .transition(
                    &task.id,
                    target,
                    task.version,
                    &workflow,
                    &actor,
                    "snapshot test",
                    false,
                )
                .await
                .unwrap()
        };
        let raw = self.transition_payload(&task.id).await;
        (result.task, serde_json::from_str(&raw).unwrap(), raw)
    }

    async fn transition_payload(&self, task_id: &str) -> String {
        sqlx::query_scalar("SELECT payload_json FROM domain_event WHERE entity_id = ? AND event_type = 'task.transitioned' ORDER BY sequence DESC LIMIT 1")
            .bind(task_id).fetch_one(self.db.pool()).await.unwrap()
    }

    async fn replace_workflow(&self, workflow: &WorkflowDefinition) {
        sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
            .bind(serde_json::to_string(workflow).unwrap())
            .bind(&self.project_id)
            .execute(self.db.pool())
            .await
            .unwrap();
    }

    async fn attention(&self) -> Vec<(String, String)> {
        sqlx::query_as(
            "SELECT attention_type, status FROM attention_projection
             WHERE scope_id = ? ORDER BY attention_type",
        )
        .bind(&self.project_id)
        .fetch_all(self.db.pool())
        .await
        .unwrap()
    }
}

#[tokio::test]
async fn renamed_review_and_success_keep_attention_and_delivery_flow() {
    let fixture = Fixture::new(renamed_workflow()).await;
    let task = fixture.task("approval", None).await;
    fixture
        .transition_event(&task, "in_progress", "approval", "system:workflow")
        .await;
    fixture.project().await;
    assert_eq!(
        fixture.attention().await,
        vec![("review_ready".to_owned(), "open".to_owned())]
    );

    fixture.move_task(&task, "shipped").await;
    fixture.project().await;
    assert_eq!(
        fixture.attention().await,
        vec![
            ("delivery_followup".to_owned(), "open".to_owned()),
            ("review_ready".to_owned(), "resolved".to_owned()),
        ]
    );
}

#[tokio::test]
async fn renamed_cancellation_resolves_review_without_claiming_delivery() {
    let fixture = Fixture::new(renamed_workflow()).await;
    let task = fixture.task("approval", None).await;
    fixture
        .transition_event(&task, "in_progress", "approval", "system:workflow")
        .await;
    fixture.project().await;
    fixture.move_task(&task, "abandoned").await;
    fixture.project().await;
    assert_eq!(
        fixture.attention().await,
        vec![("review_ready".to_owned(), "resolved".to_owned())]
    );
}

#[tokio::test]
async fn legacy_spelling_does_not_make_an_active_state_terminal() {
    let mut workflow = renamed_workflow();
    let state = workflow
        .states
        .iter_mut()
        .find(|state| state.name == "in_progress")
        .unwrap();
    state.name = "done".to_owned();
    state.kind = StateKind::Active;
    state.canonical_phase = Some(CanonicalPhase::Working);
    let fixture = Fixture::new(workflow).await;
    let task = fixture.task("done", None).await;
    fixture
        .transition_event(&task, "planning", "done", "system:workflow")
        .await;
    fixture.project().await;
    assert!(fixture.attention().await.is_empty());
}

#[tokio::test]
async fn delayed_subtask_projection_uses_recorded_source_state_and_actor() {
    for (actor, target) in [("system:workflow", "done"), ("user:board_drag", "shipped")] {
        let fixture = Fixture::new(renamed_workflow()).await;
        let parent = fixture.task("in_progress", None).await;
        // The live state would select the Project workflow even for system
        // actors. The recorded source instead selects inherited subtask flow.
        let task = fixture.task("approval", Some(parent.id)).await;
        fixture
            .transition_event(&task, "in_progress", target, actor)
            .await;
        fixture.project().await;
        assert_eq!(
            fixture.attention().await,
            vec![("delivery_followup".to_owned(), "open".to_owned())],
            "event actor {actor} must select the applicable source workflow"
        );
    }
}

#[tokio::test]
async fn committed_completion_survives_definition_changes_and_reparenting() {
    let fixture = Fixture::new(renamed_workflow()).await;
    let task = fixture.task("approval", None).await;
    let (completed, event, raw) = fixture.commit_transition(&task, "shipped", false).await;
    let snapshot = event.known_workflow_snapshot().unwrap();
    assert_eq!(snapshot.source_task_version, task.version);
    assert_eq!(snapshot.parent_task_id, None);
    assert_eq!(snapshot.to_state.kind, StateKind::Terminal);
    assert!(!snapshot.to_state.is_cancellation);
    assert!(snapshot.definition_digest.starts_with("sha256:"));

    let parent = fixture.task("in_progress", None).await;
    sqlx::query(
        "UPDATE task SET parent_task_id = ?, version = version + 1 WHERE id = ? AND version = ?",
    )
    .bind(parent.id)
    .bind(&completed.id)
    .bind(completed.version)
    .execute(fixture.db.pool())
    .await
    .unwrap();
    let mut changed = renamed_workflow();
    let shipped = changed
        .states
        .iter_mut()
        .find(|state| state.name == "shipped")
        .unwrap();
    shipped.kind = StateKind::Active;
    shipped.canonical_phase = Some(CanonicalPhase::Working);
    fixture.replace_workflow(&changed).await;
    fixture.project().await;

    assert_eq!(
        fixture.attention().await,
        vec![("delivery_followup".to_owned(), "open".to_owned())]
    );
    assert_eq!(
        fixture.transition_payload(&task.id).await,
        raw,
        "replay must not rewrite historical workflow truth"
    );
}

#[tokio::test]
async fn board_move_cancellation_stays_cancelled_after_definition_change() {
    let fixture = Fixture::new(renamed_workflow()).await;
    let task = fixture.task("approval", None).await;
    fixture
        .transition_event(&task, "in_progress", "approval", "system:workflow")
        .await;
    fixture.project().await;
    let (_, event, _) = fixture.commit_transition(&task, "abandoned", true).await;
    assert!(
        event
            .known_workflow_snapshot()
            .unwrap()
            .to_state
            .is_cancellation
    );

    let mut changed = renamed_workflow();
    changed.cancellation_state = Some("shipped".to_owned());
    fixture.replace_workflow(&changed).await;
    fixture.project().await;
    assert_eq!(
        fixture.attention().await,
        vec![("review_ready".to_owned(), "resolved".to_owned())]
    );
}

#[tokio::test]
async fn historical_review_is_preserved_but_current_workflow_controls_actionability() {
    let fixture = Fixture::new(renamed_workflow()).await;
    let task = fixture.task("in_progress", None).await;
    let (_, event, _) = fixture.commit_transition(&task, "approval", false).await;
    assert!(
        event
            .known_workflow_snapshot()
            .unwrap()
            .to_state
            .requires_user_approval
    );
    let mut changed = renamed_workflow();
    changed
        .states
        .iter_mut()
        .find(|state| state.name == "approval")
        .unwrap()
        .gate_config
        .as_mut()
        .unwrap()
        .requires_user_approval = Some(false);
    fixture.replace_workflow(&changed).await;
    fixture.project().await;
    assert_eq!(
        fixture.attention().await,
        vec![("review_ready".to_owned(), "resolved".to_owned())]
    );
}

#[tokio::test]
async fn deleted_task_keeps_frozen_completion_and_project_scope() {
    let fixture = Fixture::new(renamed_workflow()).await;
    let task = fixture.task("approval", None).await;
    fixture.commit_transition(&task, "shipped", false).await;
    // Exercise a removed live row, not just a still-queryable soft deletion.
    sqlx::query("DELETE FROM task WHERE id = ?")
        .bind(&task.id)
        .execute(fixture.db.pool())
        .await
        .unwrap();
    fixture.project().await;
    assert_eq!(fixture.attention().await.len(), 1);
    assert_eq!(fixture.attention().await[0].0, "delivery_followup");
}

#[tokio::test]
async fn missing_historical_snapshot_is_unknown_not_reconstructed_from_current_workflow() {
    let fixture = Fixture::new(default_workflow()).await;
    let task = fixture.task("done", None).await;
    let mut event = CreateDomainEvent::task_transition(
        new_uuid_v4(),
        task.id.clone(),
        fixture.project_id.clone(),
        "in_progress",
        "done",
        Some("accept"),
        "system:workflow",
        "legacy transition",
        false,
        now_rfc3339(),
        serde_json::Value::Null,
    );
    let mut payload: serde_json::Value = serde_json::from_str(&event.payload_json).unwrap();
    payload.as_object_mut().unwrap().remove("workflow_snapshot");
    event.payload_json = payload.to_string();
    let historical_raw = event.payload_json.clone();
    DomainEventRepo::append_event(&*fixture.db, event)
        .await
        .unwrap();
    fixture.project().await;
    assert!(fixture.attention().await.is_empty());
    assert_eq!(fixture.transition_payload(&task.id).await, historical_raw);
}

#[tokio::test]
async fn same_state_snapshot_is_not_a_new_terminal_outcome() {
    let fixture = Fixture::new(renamed_workflow()).await;
    let task = fixture.task("shipped", None).await;
    fixture
        .transition_event(&task, "shipped", "shipped", "user:board_drag")
        .await;
    fixture.project().await;
    assert!(fixture.attention().await.is_empty());
}

#[tokio::test]
async fn committed_long_state_identifier_stays_lossless_and_classifiable() {
    let target = format!("shipped_{}", "x".repeat(128));
    let mut workflow = renamed_workflow();
    for state in &mut workflow.states {
        if state.name == "shipped" {
            state.name = target.clone();
        }
        for trigger in state.triggers.values_mut() {
            if trigger.to == "shipped" {
                trigger.to = target.clone();
            }
        }
    }
    let fixture = Fixture::new(workflow).await;
    let task = fixture.task("approval", None).await;
    let (_, event, _) = fixture.commit_transition(&task, &target, false).await;
    assert_eq!(event.to_state, target);
    assert_eq!(
        event.known_workflow_snapshot().unwrap().to_state.name,
        target
    );
    fixture.project().await;
    assert_eq!(fixture.attention().await[0].0, "delivery_followup");
}
