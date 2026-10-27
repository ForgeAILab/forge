use super::conditions::{attention_incident_digest, fingerprint, normalize_ids};
use crate::workflow::HookContext;
use db::{ExecutionRepo, TaskRepo};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(crate) enum ClockMode {
    RestartAfterSetup,
    PreserveCaller,
}

pub(crate) struct Fixture {
    _clock: Option<db::test_clock::Guard>,
    pub(crate) db: Arc<db::SqliteDb>,
    pub(crate) task: db::Task,
    pub(crate) ctx: HookContext,
    pub(crate) step: db::TaskStep,
}
impl Fixture {
    pub(crate) async fn new(status: &str) -> Self {
        Self::with_clock(status, ClockMode::RestartAfterSetup).await
    }

    pub(crate) async fn with_clock(status: &str, mode: ClockMode) -> Self {
        let (db, clock) = match mode {
            ClockMode::RestartAfterSetup => {
                let db = Arc::new(super::sqlite_db().await);
                (db, Some(super::conditions::freeze_clock()))
            }
            ClockMode::PreserveCaller => {
                // Migration clock calls must not advance the caller's sequence.
                let _setup_clock = super::conditions::freeze_clock();
                (Arc::new(super::sqlite_db().await), None)
            }
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
        self.emit_with(name, ClockNormalization::None).await;
    }

    pub(crate) async fn emit_with(&self, name: &str, normalization: ClockNormalization) {
        after(self, name, SnapshotOptions::verbatim(normalization)).await;
    }
}

#[derive(Clone, Copy)]
pub(crate) enum Comparison {
    FieldFingerprints,
    VerbatimWithProjectionHash,
}

#[derive(Clone, Copy)]
pub(crate) enum ClockNormalization {
    None,
    PlanWait,
    ExecutionRetry,
}

impl ClockNormalization {
    fn normalize(self, row: &db::Task, raw: &str) -> String {
        match self {
            Self::None => raw.to_owned(),
            Self::PlanWait => {
                let metadata: Value =
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
                let raw = raw
                    .replace(started, "<transport-now>")
                    .replace(retry, "<transport-retry>");
                // This digest includes the uncontrolled transport times.
                let mut fields: Value = serde_json::from_str(&raw).unwrap();
                fields.as_object_mut().unwrap().remove("event_digest");
                fields.to_string()
            }
            Self::ExecutionRetry => {
                let metadata: Value =
                    serde_json::from_str(row.metadata_json.as_deref().unwrap()).unwrap();
                let deadline = metadata["deferred_dispatch"]["not_before"]
                    .as_str()
                    .unwrap();
                let parsed = chrono::DateTime::parse_from_rfc3339(deadline).unwrap();
                assert_eq!(parsed.to_rfc3339(), deadline);
                raw.replace(deadline, "<execution-retry-deadline>")
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct SnapshotOptions {
    pub(crate) comparison: Comparison,
    pub(crate) metadata_times: &'static [(&'static str, &'static str)],
    pub(crate) condition_times: &'static [(&'static str, &'static str)],
    pub(crate) workflow_offers: Option<&'static str>,
    pub(crate) clock_normalization: ClockNormalization,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self {
            comparison: Comparison::FieldFingerprints,
            metadata_times: &[],
            condition_times: &[],
            workflow_offers: None,
            clock_normalization: ClockNormalization::None,
        }
    }
}

impl SnapshotOptions {
    pub(crate) fn verbatim(clock_normalization: ClockNormalization) -> Self {
        Self {
            comparison: Comparison::VerbatimWithProjectionHash,
            clock_normalization,
            ..Default::default()
        }
    }
}

async fn replacements(
    f: &Fixture,
    row: &db::Task,
    options: SnapshotOptions,
) -> Vec<(String, String)> {
    let metadata: Value = row
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or(Value::Null);
    let mut replacements = vec![
        (row.id.clone(), "fixture-task".to_owned()),
        (row.project_id.clone(), "fixture-project".to_owned()),
    ];
    match options.comparison {
        Comparison::FieldFingerprints => {
            let repo = db::ProjectRepo::get_by_id(&*f.db, &row.project_id)
                .await
                .unwrap()
                .unwrap()
                .primary_repo_id;
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
        }
        Comparison::VerbatimWithProjectionHash => {
            if let Some(marker) = metadata
                .get("awaiting_human_marker_id")
                .and_then(Value::as_str)
            {
                replacements.push((marker.into(), "fixture-marker".into()));
            }
            let transitions: Vec<String> =
                sqlx::query_scalar("SELECT id FROM transition_log WHERE task_id=? ORDER BY rowid")
                    .bind(&row.id)
                    .fetch_all(f.db.pool())
                    .await
                    .unwrap();
            for (index, id) in transitions.into_iter().enumerate() {
                replacements.push((id, format!("fixture-transition-{index}")));
            }
            let steps: Vec<String> =
                sqlx::query_scalar("SELECT id FROM task_step WHERE task_id=? ORDER BY seq")
                    .bind(&row.id)
                    .fetch_all(f.db.pool())
                    .await
                    .unwrap();
            for (index, id) in steps.into_iter().enumerate() {
                if id != f.step.id {
                    replacements.push((id, format!("fixture-generated-step-{index}")));
                }
            }
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
    replacements
}

pub(crate) async fn snapshot(f: &Fixture, case: &str, phase: &str, options: SnapshotOptions) {
    let row = f.read().await;
    let replacements = replacements(f, &row, options).await;
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
    let mut fields = json!({
        "error_annotation": row.error_annotation, "blocked_json": row.blocked_json,
        "failed_json": row.failed_json, "entry_barrier_json": row.entry_barrier_json,
        "metadata_json": row.metadata_json, "condition_json": condition,
        "public": row.condition.public(), "read": row.condition.read(),
        "offers": offers, "attention_digest": event.dedupe_key,
    });
    let actual = match options.comparison {
        Comparison::FieldFingerprints => {
            let projector = super::conditions::attention_projector(&f.db);
            let incident = attention_incident_digest(
                &f.db,
                &projector,
                &row,
                &format!("{case}/{phase}"),
                &replacements,
            )
            .await;
            fields["failure_kind"] = json!(row.condition.read().failure_kind);
            fields["operations_text"] = json!(row.condition.read().operator_reason);
            fields["attention_incident_digest"] = json!(incident);
            Value::Object(
                fields
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(key, value)| (key.clone(), fingerprint(&normalize(&value.to_string()))))
                    .collect(),
            )
        }
        Comparison::VerbatimWithProjectionHash => {
            let fields = fields.as_object_mut().unwrap();
            for (column, alias) in [
                ("error_annotation", "annotation"),
                ("blocked_json", "blocked"),
                ("failed_json", "failed"),
                ("entry_barrier_json", "barrier"),
                ("metadata_json", "metadata"),
                ("condition_json", "condition"),
                ("attention_digest", "event_digest"),
            ] {
                let value = fields.remove(column).unwrap();
                fields.insert(alias.into(), value);
            }
            let raw = options
                .clock_normalization
                .normalize(&row, &normalize(&serde_json::to_string(fields).unwrap()));
            let mut fields: Value = serde_json::from_str(&raw).unwrap();
            let fields = fields.as_object_mut().unwrap();
            let mut projections = serde_json::Map::new();
            for key in ["read", "public", "offers", "event_digest"] {
                if let Some(value) = fields.remove(key) {
                    projections.insert(key.into(), value);
                }
            }
            let canonical = serde_json::to_vec(&projections).unwrap();
            fields.insert(
                "projection_sha256".into(),
                json!(hex::encode(Sha256::digest(canonical))),
            );
            json!(serde_json::to_string(fields).unwrap())
        }
    };
    assert_main_parity(&format!("{case}/{phase}"), &actual, options.comparison);
}

pub(crate) fn assert_main_parity(name: &str, actual: &Value, comparison: Comparison) {
    let (oracle, key, field) = match comparison {
        Comparison::FieldFingerprints => (
            include_str!("writer_fixture/fixtures/main-94609411.jsonl"),
            "case",
            "fields",
        ),
        Comparison::VerbatimWithProjectionHash => (
            include_str!("writer_fixture/fixtures/writer-main-94609411.jsonl"),
            "name",
            "raw",
        ),
    };
    let expected = oracle
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|value| value[key] == name)
        .expect("archived main case");
    let actual = json!({(key): name, (field): actual});
    assert_eq!(actual, expected, "archived main parity: {name}");
}

pub(crate) async fn after(f: &Fixture, case: &str, options: SnapshotOptions) {
    snapshot(f, case, "first", options).await;
    let (second, overlay, at) = match options.comparison {
        Comparison::FieldFingerprints => (
            "following-unconverted-write",
            "queue-overlay",
            db::now_rfc3339(),
        ),
        Comparison::VerbatimWithProjectionHash => {
            ("second", "queue", "2026-10-25T00:00:00Z".into())
        }
    };
    TaskRepo::mutate_metadata(
        &*f.db,
        &f.task.id,
        None,
        vec![db::TaskMetadataMutation::Set {
            key: "awaiting_human".into(),
            value: json!(false),
        }],
        &at,
    )
    .await
    .unwrap();
    snapshot(f, case, second, options).await;
    let row = f.read().await;
    if row.error_annotation.is_some() {
        match options.comparison {
            Comparison::FieldFingerprints => {
                crate::worker_runtime::queue::condition_tests::merge_after_writer(
                    &f.db, &row, &f.step,
                )
                .await;
            }
            Comparison::VerbatimWithProjectionHash => {
                let worker =
                    crate::worker_runtime::queue::TaskStepWorker::new(f.ctx.task_service.clone());
                let mut tx = f.db.pool().begin().await.unwrap();
                let mut overlaid = row;
                worker
                    .write_annotation_for_test_in_tx(
                        &mut tx,
                        &f.step,
                        &mut overlaid,
                        "failed",
                        api_types::FailureKind::CascadeFailed,
                        Some("later hook failed"),
                    )
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            }
        }
        snapshot(f, case, overlay, options).await;
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
        $crate::recovery::test_support::writer_fixture::case!(
            $name,
            $state,
            $f,
            $body,
            $crate::recovery::test_support::writer_fixture::ClockNormalization::None
        );
    }};
    ($name:expr,$state:expr,$f:ident,$body:block,$normalization:expr) => {{
        let _clock = db::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap());
        let $f = $crate::recovery::test_support::writer_fixture::Fixture::with_clock(
            $state,
            $crate::recovery::test_support::writer_fixture::ClockMode::PreserveCaller,
        )
        .await;
        db::task_writer::in_task_step($f.step.clone(), async {
            $body;
            $f.emit_with($name, $normalization).await;
        })
        .await;
    }};
}
pub(crate) use case;
