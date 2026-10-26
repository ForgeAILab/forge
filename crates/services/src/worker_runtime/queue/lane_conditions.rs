use crate::workflow::HookContext;
use db::ExecutionRepo;
use serde_json::json;
use std::sync::Arc;

pub(crate) fn assert_or_record(name: &str, raw: &str) {
    let expected = include_str!("fixtures/lane-main-94609411.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|record| record["name"] == name)
        .unwrap_or_else(|| panic!("missing main oracle {name}"));
    assert_eq!(
        raw,
        expected["raw"].as_str().unwrap(),
        "archived-main parity: {name}"
    );
}
pub(crate) struct Fixture {
    pub(crate) db: Arc<db::SqliteDb>,
    pub(crate) task: db::Task,
    pub(crate) ctx: HookContext,
    pub(crate) step: db::TaskStep,
}
impl Fixture {
    pub(crate) async fn new(status: &str) -> Self {
        let db = {
            // Template initialization is process-global; its migration clock
            // calls must not consume this writer case's frozen sequence.
            let _setup_clock =
                db::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap());
            Arc::new(crate::recovery::test_support::sqlite_db().await)
        };
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
    pub(crate) async fn review(&self, execution: &db::Execution) {
        db::ReviewRepo::create(
            &*self.db,
            db::CreateReview {
                id: "review".into(),
                task_id: self.task.id.clone(),
                execution_id: execution.id.clone(),
                attempt_number: 1,
                status: db::ReviewStatus::Failed,
                step_results_json: "{}".into(),
                started_at: db::now_rfc3339(),
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .unwrap();
    }
    pub(crate) async fn emit(&self, name: &str) {
        self.snapshot(name, "first").await;
        db::TaskRepo::mutate_metadata(
            &*self.db,
            &self.task.id,
            None,
            vec![db::TaskMetadataMutation::Set {
                key: "awaiting_human".into(),
                value: json!(false),
            }],
            "2026-10-25T00:00:00Z",
        )
        .await
        .unwrap();
        self.snapshot(name, "second").await;
        let original = db::TaskRepo::get_by_id(&*self.db, &self.task.id, false)
            .await
            .unwrap()
            .unwrap();
        if original.error_annotation.is_some() {
            let worker = super::TaskStepWorker::new(self.ctx.task_service.clone());
            let mut tx = self.db.pool().begin().await.unwrap();
            let mut overlaid = original;
            worker
                .write_annotation_in_tx(
                    &mut tx,
                    &self.step,
                    &mut overlaid,
                    "failed",
                    api_types::FailureKind::CascadeFailed,
                    Some("later hook failed"),
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
            self.snapshot(name, "queue").await;
        }
    }
    async fn snapshot(&self, name: &str, phase: &str) {
        let row = db::TaskRepo::get_by_id(&*self.db, &self.task.id, false)
            .await
            .unwrap()
            .unwrap();
        let condition: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(&row.id)
            .fetch_one(self.db.pool())
            .await
            .unwrap();
        let offers = crate::task_actions::test_support::condition_offers(&self.db, &row).await;
        let mut replacements = vec![
            (row.id.clone(), "fixture-task".to_owned()),
            (row.project_id.clone(), "fixture-project".to_owned()),
        ];
        if let Some(marker) = row
            .metadata_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
            .and_then(|m| {
                m.get("awaiting_human_marker_id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
        {
            replacements.push((marker, "fixture-marker".into()));
        }
        let transitions: Vec<String> =
            sqlx::query_scalar("SELECT id FROM transition_log WHERE task_id=? ORDER BY rowid")
                .bind(&row.id)
                .fetch_all(self.db.pool())
                .await
                .unwrap();
        for (index, id) in transitions.into_iter().enumerate() {
            replacements.push((id, format!("fixture-transition-{index}")));
        }
        let steps: Vec<String> =
            sqlx::query_scalar("SELECT id FROM task_step WHERE task_id=? ORDER BY seq")
                .bind(&row.id)
                .fetch_all(self.db.pool())
                .await
                .unwrap();
        for (index, id) in steps.into_iter().enumerate() {
            if id != self.step.id {
                replacements.push((id, format!("fixture-generated-step-{index}")));
            }
        }
        let normalize = |raw: &str| {
            replacements
                .iter()
                .fold(raw.to_owned(), |raw, (id, replacement)| {
                    raw.replace(id, replacement)
                })
        };
        let mut canonical = row.clone();
        canonical.id = "fixture-task".into();
        canonical.condition = serde_json::from_str(&normalize(&condition)).unwrap();
        let event = db::CreateDomainEvent::task_interruption_changed(&canonical);
        let fields = json!({
            "annotation": row.error_annotation, "blocked": row.blocked_json, "failed": row.failed_json,
            "barrier": row.entry_barrier_json, "metadata": row.metadata_json, "condition": condition,
            "public": row.condition.public(), "read": row.condition.read(), "offers": offers,
            "event_digest": event.dedupe_key,
        });
        let mut raw = normalize(&fields.to_string());
        // Transport deliberately uses Utc::now rather than the DB test clock.
        // Match the timestamp spelling and relationships separately below.
        if name == "plan-wait" {
            let metadata: serde_json::Value =
                serde_json::from_str(row.metadata_json.as_deref().unwrap()).unwrap();
            let started = metadata["owner_wait"]["started_at"].as_str().unwrap();
            let retry = metadata["plan_settlement_wait"]["retry_at"]
                .as_str()
                .unwrap();
            assert_eq!(
                chrono::DateTime::parse_from_rfc3339(retry).unwrap()
                    - chrono::DateTime::parse_from_rfc3339(started).unwrap(),
                chrono::Duration::seconds(5)
            );
            raw = raw
                .replace(started, "<transport-now>")
                .replace(retry, "<transport-retry>");
            // The event digest includes the uncontrolled transport times.
            let mut fields: serde_json::Value = serde_json::from_str(&raw).unwrap();
            fields.as_object_mut().unwrap().remove("event_digest");
            raw = fields.to_string();
        }
        if name == "retry-metadata" {
            let metadata: serde_json::Value =
                serde_json::from_str(row.metadata_json.as_deref().unwrap()).unwrap();
            let deadline = metadata["deferred_dispatch"]["not_before"]
                .as_str()
                .unwrap();
            let parsed = chrono::DateTime::parse_from_rfc3339(deadline).unwrap();
            assert_eq!(parsed.to_rfc3339(), deadline);
            raw = raw.replace(deadline, "<execution-retry-deadline>");
        }
        assert_or_record(&format!("{name}/{phase}"), &raw);
    }
}

pub(crate) fn hook_failure() -> crate::lifecycle::LifecycleHookRun {
    crate::lifecycle::LifecycleHookRun {
        index: 0,
        entry: json!({}),
        status: "failed".into(),
        exit_code: Some(1),
        timed_out: false,
        stdout: "out".into(),
        stderr: "err".into(),
        error: Some("hook failed".into()),
        command: Some("false".into()),
        working_dir: "workspace".into(),
        duration_ms: 7,
        log_path: Some("hook.log".into()),
        environment_preview: std::collections::BTreeMap::from([("LANG".into(), "C".into())]),
    }
}
pub(crate) fn channel_sample(name: &str) -> String {
    include_str!("../../../../db/src/task_condition/channels/fixtures/main-13bc2ae1.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find(|record| record["name"] == name && record["channel"] == "annotation")
        .unwrap()["raw"]
        .as_str()
        .unwrap()
        .into()
}
macro_rules! case {
    ($name:expr,$state:expr,$f:ident,$body:block) => {{
        let _clock = db::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap());
        let $f = $crate::worker_runtime::queue::lane_conditions::Fixture::new($state).await;
        db::task_writer::in_task_step($f.step.clone(), async {
            $body;
            $f.emit($name).await;
        })
        .await;
    }};
}
pub(crate) use case;

#[tokio::test]
async fn queue_channels_and_overlays() {
    for (kind, status, error) in [
        (
            api_types::FailureKind::CascadeFailed,
            "failed",
            Some("failed"),
        ),
        (
            api_types::FailureKind::WorkspaceError,
            "failed",
            Some("workspace"),
        ),
        (api_types::FailureKind::WorkspaceError, "failed", None),
        (api_types::FailureKind::WorkflowLoop, "parked", None),
        (
            api_types::FailureKind::MergeConflict,
            "failed",
            Some("merge"),
        ),
        (api_types::FailureKind::CiFailed, "failed", Some("ci")),
    ] {
        case!(
            &format!("step-{kind}-{}", error.is_some()),
            "in_progress",
            f,
            {
                let worker = super::TaskStepWorker::new(f.ctx.task_service.clone());
                let mut tx = f.db.pool().begin().await.unwrap();
                let mut task = f.task.clone();
                worker
                    .write_annotation_in_tx(&mut tx, &f.step, &mut task, status, kind, error)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        );
    }
    for sample in [
        "executor",
        "hold",
        "script-hook",
        "dispatch-state",
        "dispatch-upgrade",
        "lifecycle-reset",
        "owner-recovery",
        "removed-machine",
        "recovered-task",
        "budget",
        "review-finding",
        "workflow-guard",
        "review-candidate",
        "review-ci",
        "placement",
        "plan-wait",
        "review-park-null",
        "historical",
    ] {
        case!(&format!("overlay-{sample}"), "in_progress", f, {
            let raw = if sample == "historical" {
                "{\"type\":\"unknown\",\"extra\":true}".into()
            } else {
                channel_sample(sample)
            };
            sqlx::query("UPDATE task SET error_annotation=?,blocked_json='{}' WHERE id=?")
                .bind(raw)
                .bind(&f.task.id)
                .execute(f.db.pool())
                .await
                .unwrap();
            let mut task = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
                .await
                .unwrap()
                .unwrap();
            let worker = super::TaskStepWorker::new(f.ctx.task_service.clone());
            let mut tx = f.db.pool().begin().await.unwrap();
            worker
                .write_annotation_in_tx(
                    &mut tx,
                    &f.step,
                    &mut task,
                    "failed",
                    api_types::FailureKind::CascadeFailed,
                    Some("later hook failed"),
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
        });
    }
}
