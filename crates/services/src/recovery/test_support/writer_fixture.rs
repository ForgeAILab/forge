use super::conditions::{attention_incident_digest, fingerprint, normalize_ids};
use crate::workflow::HookContext;
use db::{ExecutionRepo, TaskRepo};
use serde_json::{json, Value};
use std::sync::Arc;

pub(crate) struct Fixture {
    _clock: db::test_clock::Guard,
    pub(crate) db: Arc<db::SqliteDb>,
    pub(crate) task: db::Task,
    pub(crate) ctx: HookContext,
    pub(crate) step: db::TaskStep,
}
impl Fixture {
    pub(crate) async fn new(status: &str) -> Self {
        let db = Arc::new(crate::recovery::test_support::sqlite_db().await);
        // Template initialization can use the test clock; each writer starts
        // from the same sequence after its database has been created.
        let clock = crate::recovery::test_support::conditions::freeze_clock();
        let (project, _) = crate::recovery::test_support::seed_project_repo(&db).await;
        let task =
            crate::recovery::test_support::seed_task(&db, project.clone(), status.into(), None)
                .await;
        let step = crate::recovery::test_support::conditions::claim(&db, &task).await;
        let event_bus = Arc::new(events::EventBus::default());
        let service = crate::TaskService::new(db.clone(), event_bus.clone());
        let ctx = HookContext {
            task_id: task.id.clone(),
            project_id: project,
            from_state: status.into(),
            to_state: status.into(),
            db: db.clone(),
            event_bus,
            gate_config: None,
            workflow: Arc::new(crate::workflow::default_workflow::default_workflow()),
            project_version: None,
            project_workflow_definition: None,
            triggered_by: api_types::Actor::system(api_types::SystemComponent::Test),
            merge_service: None,
            cleanup_scheduler: None,
            task_service: service,
            daemon_connections: None,
            workspace_exec_locks: None,
            terminal_activity: None,
            workspace_root: Default::default(),
            repo_cache_locks: None,
            workspace_backend_router: crate::diff::embedded_read_router_for_test(db.clone()),
            workspace_id: None,
            agent_id: None,
            execution_id: None,
            state_config: json!({}),
        };
        Self {
            _clock: clock,
            db,
            task,
            ctx,
            step,
        }
    }
    pub(crate) async fn execution(&self, status: db::ExecutionStatus) -> db::Execution {
        let project = db::ProjectRepo::get_by_id(&*self.db, &self.task.project_id)
            .await
            .unwrap()
            .unwrap();
        ExecutionRepo::create(
            &*self.db,
            db::CreateExecution {
                id: "execution".into(),
                task_id: self.task.id.clone(),
                agent_id: None,
                role: "coder".into(),
                status,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: Some("execution.jsonl".into()),
                before_sha: None,
                after_sha: None,
                error: Some("é😀\n\"\\\u{7}".into()),
                executor_config_snapshot_json: Some(
                    json!({"project_version":project.version}).to_string(),
                ),
                workspace_id: None,
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap()
    }
    pub(crate) async fn read(&self) -> db::Task {
        TaskRepo::get_by_id(&*self.db, &self.task.id, false)
            .await
            .unwrap()
            .unwrap()
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SnapshotOptions {
    pub(crate) oracle: &'static str,
    pub(crate) metadata_times: &'static [(&'static str, &'static str)],
    pub(crate) condition_times: &'static [(&'static str, &'static str)],
    pub(crate) workflow_offers: Option<&'static str>,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self {
            oracle: include_str!("writer_fixture/fixtures/main-94609411.jsonl"),
            metadata_times: &[],
            condition_times: &[],
            workflow_offers: None,
        }
    }
}

pub(crate) async fn snapshot(f: &Fixture, case: &str, phase: &str, options: SnapshotOptions) {
    let row = f.read().await;
    let repo = db::ProjectRepo::get_by_id(&*f.db, &row.project_id)
        .await
        .unwrap()
        .unwrap()
        .primary_repo_id;
    let metadata: Value = row
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or(Value::Null);
    let mut replacements = vec![
        (row.id.clone(), "fixture-task".to_owned()),
        (row.project_id.clone(), "fixture-project".to_owned()),
    ];
    if let Some(repo) = repo {
        replacements.push((repo, "fixture-repo".to_owned()));
    }
    for (key, replacement) in [
        ("/coordination_review_pending_id", "fixture-coordination"),
        ("/queued_recovery/id", "fixture-command"),
        ("/placement_refusal/eligibility_key", "fixture-eligibility"),
    ] {
        if let Some(id) = metadata.pointer(key).and_then(Value::as_str) {
            replacements.push((id.into(), replacement.into()));
        }
    }
    for (pointer, token) in options.metadata_times {
        if let Some(at) = metadata.pointer(pointer).and_then(Value::as_str) {
            chrono::DateTime::parse_from_rfc3339(at).unwrap();
            replacements.push((at.into(), (*token).into()));
        }
    }
    for (pointer, token) in options.condition_times {
        for raw in [&row.error_annotation, &row.blocked_json]
            .into_iter()
            .flatten()
        {
            let value: Value = serde_json::from_str(raw).unwrap();
            if let Some(at) = value.pointer(pointer).and_then(Value::as_str) {
                chrono::DateTime::parse_from_rfc3339(at).unwrap();
                replacements.push((at.into(), (*token).into()));
            }
        }
    }
    let normalize = |raw: &str| normalize_ids(raw, &replacements);
    let condition: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&row.id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    let mut canonical = row.clone();
    canonical.id = "fixture-task".into();
    canonical.condition = serde_json::from_str(&normalize(&condition)).unwrap();
    let event = db::CreateDomainEvent::task_interruption_changed(&canonical);
    let offers = if let Some(expected) = options.workflow_offers {
        let actor = api_types::Actor::user(api_types::UserActionSource::Api);
        let snapshot = f
            .ctx
            .task_service
            .task_action_snapshot_for_task(row.clone(), &actor)
            .await
            .unwrap();
        let offers =
            serde_json::to_value(crate::task_actions::available_actions(&snapshot)).unwrap();
        assert_eq!(
            offers,
            serde_json::from_str::<Value>(expected).unwrap(),
            "workflow offers"
        );
        offers
    } else {
        crate::task_actions::test_support::condition_offers(&f.db, &row).await
    };
    let projector = crate::recovery::test_support::conditions::attention_projector(&f.db);
    let incident = attention_incident_digest(
        &f.db,
        &projector,
        &row,
        &format!("{case}/{phase}"),
        &replacements,
    )
    .await;
    let fields = json!({
        "error_annotation": row.error_annotation, "blocked_json": row.blocked_json,
        "failed_json": row.failed_json, "entry_barrier_json": row.entry_barrier_json,
        "metadata_json": row.metadata_json, "condition_json": condition,
        "public": row.condition.public(), "read": row.condition.read(),
        "failure_kind": row.condition.read().failure_kind,
        "operations_text": row.condition.read().operator_reason,
        "offers": offers, "attention_digest": event.dedupe_key, "attention_incident_digest": incident,
    });
    let fields: serde_json::Map<String, Value> = fields
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, value)| (key.clone(), fingerprint(&normalize(&value.to_string()))))
        .collect();
    let actual = json!({"case":format!("{case}/{phase}"), "fields":fields});
    let expected = options
        .oracle
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|value| value["case"] == actual["case"])
        .expect("archived main case");
    assert_eq!(actual, expected, "archived main parity: {case}/{phase}");
}

pub(crate) async fn after(f: &Fixture, case: &str, options: SnapshotOptions) {
    snapshot(f, case, "first", options).await;
    TaskRepo::mutate_metadata(
        &*f.db,
        &f.task.id,
        None,
        vec![db::TaskMetadataMutation::Set {
            key: "awaiting_human".into(),
            value: json!(false),
        }],
        &db::now_rfc3339(),
    )
    .await
    .unwrap();
    snapshot(f, case, "following-unconverted-write", options).await;
    let row = f.read().await;
    if row.error_annotation.is_some() {
        crate::worker_runtime::queue::condition_tests::merge_after_writer(&f.db, &row, &f.step)
            .await;
        snapshot(f, case, "queue-overlay", options).await;
    }
}
