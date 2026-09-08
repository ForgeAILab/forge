#![allow(dead_code, clippy::assertions_on_constants)]
mod common;

use std::{collections::HashSet, sync::Arc, time::Duration};

use api::{build_router, AppState};
use api_types::{
    CanonicalPhase, ProjectResponse, RepoResponse, StateDefinition, StateHooks, StateKind,
    TaskResponse, TransitionTaskResponse, WorkflowDefinition, WorkflowTrigger,
    WorkflowTriggerDefinition,
};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::{now_rfc3339, ProjectHookRun, ProjectHookRunRepo, ProjectHookRunStatus, Task, TaskRepo};
use events::{EventBus, EventContext, ForgeEvent, PROJECT_HOOK_RUN_CHANGED_EVENT};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use sqlx::Row;
use tower::ServiceExt;

#[tokio::test]
async fn all_work_completed_create_task_refires_after_hook_created_work_completes() {
    let harness = test_app().await;
    let (project, _repo) = create_project_with_repo_and_completion_workflow(&harness).await;
    configure_create_task_hook(&harness.app, &project.id).await;
    let mut events_rx = harness.state.event_bus.subscribe();

    let source: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({
            "title": "Finish milestone",
            "description": "complete the source task",
            "task_type": "task",
            "priority": 0,
            "review_config": null,
            "merge_config": null
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(source.status, "todo");

    let completed: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", source.id),
        json!({
            "status": "done",
            "version": source.version,
            "reason": "source complete"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(completed.task.status, "done");

    let first_runs = wait_for_hook_runs(&harness.state.db, &project.id, 1).await;
    assert_eq!(first_runs.len(), 1);
    assert_eq!(first_runs[0].status, ProjectHookRunStatus::Completed);
    let completed_event =
        wait_for_hook_run_changed_event(&mut events_rx, &first_runs[0].id, "completed").await;
    assert_project_hook_run_changed_event(
        &completed_event,
        &project.id,
        &first_runs[0].id,
        &source.id,
    );

    let follow_ups = tasks_with_title(&harness.state.db, &project.id, "Hook follow-up").await;
    assert_eq!(follow_ups.len(), 1);
    assert!(!follow_ups[0].is_automation);

    let follow_up = &follow_ups[0];
    let completed_follow_up: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", follow_up.id),
        json!({
            "status": "done",
            "version": follow_up.version,
            "reason": "follow-up complete"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(completed_follow_up.task.status, "done");

    let second_runs = wait_for_hook_runs(&harness.state.db, &project.id, 2).await;
    assert_eq!(second_runs.len(), 2);
    assert!(second_runs
        .iter()
        .all(|run| run.status == ProjectHookRunStatus::Completed));
    let dedupe_keys = second_runs
        .iter()
        .map(|run| run.dedupe_key.as_str())
        .collect::<HashSet<_>>();
    assert_eq!(dedupe_keys.len(), 2);
    assert!(dedupe_keys.contains("project.all_work_completed:1"));
    assert!(dedupe_keys.contains("project.all_work_completed:2"));
}

#[tokio::test]
async fn hook_action_failure_does_not_roll_back_source_task_transition() {
    let harness = test_app().await;
    let (project, _repo) = create_project_with_repo_and_completion_workflow(&harness).await;
    configure_dispatch_missing_agent_hook(&harness.app, &project.id).await;

    let source: TaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/tasks", project.id),
        json!({
            "title": "Finish despite hook failure",
            "description": "the source transition must stay committed",
            "task_type": "task",
            "priority": 0,
            "review_config": null,
            "merge_config": null
        }),
        StatusCode::OK,
    )
    .await;

    let completed: TransitionTaskResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/tasks/{}/transition", source.id),
        json!({
            "status": "done",
            "version": source.version,
            "reason": "source complete"
        }),
        StatusCode::OK,
    )
    .await;
    assert_eq!(completed.task.status, "done");

    let runs = wait_for_hook_runs(&harness.state.db, &project.id, 1).await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, ProjectHookRunStatus::Failed);
    let reason = runs[0].reason.as_deref().unwrap_or_default();
    assert!(
        reason.contains("agent") && reason.contains("missing-agent"),
        "failure reason should mention the missing agent: {reason}"
    );

    let persisted = TaskRepo::get_by_id(&*harness.state.db, &source.id, false)
        .await
        .expect("task loads")
        .expect("task exists");
    assert_eq!(persisted.status, "done");
}

struct Harness {
    app: Router,
    state: Arc<AppState>,
    hook_service_handle: tokio::task::JoinHandle<()>,
    _web_dist_dir: common::TestDir,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.hook_service_handle.abort();
    }
}

async fn test_app() -> Harness {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(db::SqliteDb::new(pool));
    let adapter_registry = Arc::new(cli_adapters::default_registry());
    services::ensure_default_agents(db.as_ref(), &adapter_registry)
        .await
        .expect("default agents upsert");
    let event_bus = Arc::new(EventBus::new(256));
    let state = Arc::new(AppState::with_adapter_registry(
        db,
        Arc::clone(&event_bus),
        true,
        adapter_registry,
    ));
    let hook_service_handle = Arc::clone(&state.project_hook_service).start();
    tokio::task::yield_now().await;

    let web_dist_dir = common::TestDir::new("forge-project-hooks-web");
    std::fs::write(web_dist_dir.path().join("index.html"), "<html></html>").expect("write index");
    let app = build_router((*state).clone(), web_dist_dir.path().to_path_buf());

    Harness {
        app,
        state,
        hook_service_handle,
        _web_dist_dir: web_dist_dir,
    }
}

async fn create_project_with_repo_and_completion_workflow(
    harness: &Harness,
) -> (ProjectResponse, RepoResponse) {
    let project: ProjectResponse = json_request(
        &harness.app,
        Method::POST,
        "/api/v1/projects",
        json!({ "name": "Project hooks" }),
        StatusCode::OK,
    )
    .await;
    sqlx::query("UPDATE project SET workflow_definition = ?, updated_at = ? WHERE id = ?")
        .bind(completion_workflow_json())
        .bind(now_rfc3339())
        .bind(&project.id)
        .execute(harness.state.db.pool())
        .await
        .expect("workflow updates");

    let repo: RepoResponse = json_request(
        &harness.app,
        Method::POST,
        &format!("/api/v1/projects/{}/repos", project.id),
        json!({
            "name": "repo",
            "remote_url": "https://example.com/repo.git",
            "local_path": null,
            "default_branch": "main"
        }),
        StatusCode::OK,
    )
    .await;

    (project, repo)
}

async fn configure_create_task_hook(app: &Router, project_id: &str) {
    let project: ProjectResponse = json_request(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    let _: ProjectResponse = json_request(
        app,
        Method::PATCH,
        &format!("/api/v1/projects/{project_id}"),
        json!({
            "version": project.version,
            "project_hooks": [{
                "id": "create-follow-up",
                "enabled": true,
                "name": "Create follow-up",
                "trigger": { "type": "project.all_work_completed" },
                "filters": null,
                "action": {
                    "type": "create_task",
                    "title": "Hook follow-up",
                    "description": "Created after all project work completed",
                    "task_type": "task",
                    "priority": 0
                },
                "cooldown_seconds": null,
                "max_concurrent_runs": 1
            }]
        }),
        StatusCode::OK,
    )
    .await;
}

async fn configure_dispatch_missing_agent_hook(app: &Router, project_id: &str) {
    let project: ProjectResponse = json_request(
        app,
        Method::GET,
        &format!("/api/v1/projects/{project_id}"),
        Value::Null,
        StatusCode::OK,
    )
    .await;
    let _: ProjectResponse = json_request(
        app,
        Method::PATCH,
        &format!("/api/v1/projects/{project_id}"),
        json!({
            "version": project.version,
            "project_hooks": [{
                "id": "dispatch-missing-agent",
                "enabled": true,
                "name": "Dispatch missing agent",
                "trigger": { "type": "project.all_work_completed" },
                "filters": null,
                "action": {
                    "type": "dispatch_agent",
                    "agent_id": "missing-agent",
                    "prompt": "Evaluate completed work",
                    "follow_up": null
                },
                "cooldown_seconds": null,
                "max_concurrent_runs": 1
            }]
        }),
        StatusCode::OK,
    )
    .await;
}

async fn wait_for_hook_runs(
    db: &Arc<db::SqliteDb>,
    project_id: &str,
    expected_count: usize,
) -> Vec<ProjectHookRun> {
    for _ in 0..100 {
        let runs = ProjectHookRunRepo::list_recent_for_project(&**db, project_id, 20)
            .await
            .expect("hook runs load");
        if runs.len() >= expected_count && runs.iter().all(is_terminal_hook_run) {
            return runs;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("project hook runs did not reach {expected_count} terminal rows");
}

async fn wait_for_hook_run_changed_event(
    rx: &mut tokio::sync::broadcast::Receiver<ForgeEvent>,
    run_id: &str,
    status: &str,
) -> ForgeEvent {
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            Ok(Ok(event)) => {
                if event.event_type != PROJECT_HOOK_RUN_CHANGED_EVENT {
                    continue;
                }
                let EventContext::ProjectHookRunChanged {
                    run_id: event_run_id,
                    status: event_status,
                    ..
                } = &event.context
                else {
                    continue;
                };
                if event_run_id == run_id && event_status == status {
                    return event;
                }
            }
            Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) | Err(_) => continue,
            Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                panic!("event bus closed before project hook run event was observed")
            }
        }
    }
    panic!("project_hook.run_changed event for run {run_id} with status {status} was not observed");
}

fn assert_project_hook_run_changed_event(
    event: &ForgeEvent,
    project_id: &str,
    run_id: &str,
    source_task_id: &str,
) {
    assert_eq!(event.event_type, PROJECT_HOOK_RUN_CHANGED_EVENT);
    assert_eq!(event.entity_id, run_id);
    let EventContext::ProjectHookRunChanged {
        project_id: event_project_id,
        run_id: event_run_id,
        rule_id,
        trigger_type,
        dedupe_key,
        status,
        source_task_id: event_source_task_id,
        automation_task_id,
        execution_id,
        agent_id,
        reason,
    } = &event.context
    else {
        panic!("expected ProjectHookRunChanged event context");
    };
    assert_eq!(event_project_id, project_id);
    assert_eq!(event_run_id, run_id);
    assert_eq!(rule_id, "create-follow-up");
    assert_eq!(trigger_type, "project.all_work_completed");
    assert_eq!(dedupe_key, "project.all_work_completed:1");
    assert_eq!(status, "completed");
    assert_eq!(event_source_task_id.as_deref(), Some(source_task_id));
    assert!(automation_task_id.is_none());
    assert!(execution_id.is_none());
    assert!(agent_id.is_none());
    assert!(reason
        .as_deref()
        .unwrap_or_default()
        .contains("created task"));
}

fn is_terminal_hook_run(run: &ProjectHookRun) -> bool {
    matches!(
        run.status,
        ProjectHookRunStatus::Completed
            | ProjectHookRunStatus::Dispatched
            | ProjectHookRunStatus::Failed
            | ProjectHookRunStatus::Skipped
    )
}

async fn tasks_with_title(db: &Arc<db::SqliteDb>, project_id: &str, title: &str) -> Vec<Task> {
    let rows = sqlx::query(
        "SELECT id FROM task \
         WHERE project_id = ? AND title = ? AND deleted_at IS NULL \
         ORDER BY created_at ASC, id ASC",
    )
    .bind(project_id)
    .bind(title)
    .fetch_all(db.pool())
    .await
    .expect("task ids load");

    let mut tasks = Vec::with_capacity(rows.len());
    for row in rows {
        let id: String = row.try_get("id").expect("id column");
        let task = TaskRepo::get_by_id(&**db, &id, false)
            .await
            .expect("task loads")
            .expect("task exists");
        tasks.push(task);
    }
    tasks
}

fn completion_workflow_json() -> String {
    let mut todo = workflow_state("todo", StateKind::Initial);
    todo.triggers.insert(
        WorkflowTrigger::Accept,
        WorkflowTriggerDefinition {
            to: "done".to_owned(),
            dispatch: None,
        },
    );

    serde_json::to_string(&WorkflowDefinition {
        roles: Vec::new(),
        states: vec![
            todo,
            workflow_state("done", StateKind::Terminal),
            workflow_state("cancelled", StateKind::Terminal),
        ],
        configuration: Vec::new(),
        cancellation_state: Some("cancelled".to_owned()),
    })
    .expect("workflow serializes")
}

fn workflow_state(name: &str, kind: StateKind) -> StateDefinition {
    StateDefinition {
        name: name.to_owned(),
        kind,
        column: name.to_owned(),
        display_name: name.to_owned(),
        role: None,
        hooks: StateHooks::default(),
        cleanup: None,
        canonical_phase: Some(match kind {
            StateKind::Backlog => CanonicalPhase::Backlog,
            StateKind::Initial => CanonicalPhase::Ready,
            StateKind::Active => CanonicalPhase::Working,
            StateKind::Gate => CanonicalPhase::Working,
            StateKind::Terminal => CanonicalPhase::Done,
            StateKind::Custom => CanonicalPhase::Working,
        }),
        gate_config: None,
        dispatch: None,
        triggers: std::collections::BTreeMap::new(),
        config: json!({}),
    }
}

async fn json_request<T>(
    app: &Router,
    method: Method,
    uri: &str,
    body: Value,
    expected_status: StatusCode,
) -> T
where
    T: DeserializeOwned,
{
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .header(header::AUTHORIZATION, format!("Bearer {}", test_jwt()))
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .expect("build JSON request"),
        )
        .await
        .expect("router response");
    parse_response(response, expected_status).await
}

async fn parse_response<T>(response: axum::response::Response, expected_status: StatusCode) -> T
where
    T: DeserializeOwned,
{
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read response body");
    assert_eq!(
        status,
        expected_status,
        "unexpected response status with body: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("parse JSON response")
}

fn test_jwt() -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let claims = serde_json::json!({
        "sub": "test-user-id",
        "email": "test@example.com",
        "is_admin": true,
        "iat": now,
        "exp": now + 900,
    });
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-jwt-secret-for-development"),
    )
    .expect("encode test jwt")
}
