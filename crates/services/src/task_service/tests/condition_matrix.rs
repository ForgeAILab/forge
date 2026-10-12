//! Condition-only reproduction matrix: command commit + dispatcher consumption.
use super::helpers::*;
use super::*;
use api_types::{FailureKind, TaskAction};
use std::fmt::Write as _;

struct ConditionPendingExecutor;
#[async_trait::async_trait]
impl TaskExecutor for ConditionPendingExecutor {
    async fn execute(
        &self,
        _context: ExecutionContext,
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

fn condition_role_for(workflow: &str, state: &str) -> Option<&'static str> {
    match (workflow, state) {
        ("std", "planning") => Some("planner"),
        ("std", "in_progress" | "merge_failed") => Some("coder"),
        ("std", "review") => Some("reviewer"),
        ("auto", "working" | "merge_failed") => Some("worker"),
        _ => None,
    }
}

async fn condition_fixture(
    db: &SqliteDb,
    project: &str,
    agent: &str,
    workflow: &str,
    state: &str,
    kind: Option<FailureKind>,
    variant: &str,
) -> Task {
    let task = seed_task_with_status(db, project, state).await;
    if workflow == "std" {
        seed_role_assignment(db, &task.id, "coder", Some(agent)).await;
        seed_role_assignment(db, &task.id, "planner", Some(agent)).await;
        seed_role_assignment(db, &task.id, "reviewer", Some(agent)).await;
    } else {
        seed_role_assignment(db, &task.id, "worker", Some(agent)).await;
    }
    if let Some(kind) = kind {
        sqlx::query("UPDATE task SET error_annotation = ? WHERE id = ?")
            .bind(json!({"type":kind,"blocking_reason":"fixture","recovery_actions":["retry_hook","return_to_implementation"]}).to_string())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        db.check_task_conditions_of(std::slice::from_ref(&task.id))
            .await
            .unwrap();
    }
    let work_role = if workflow == "std" { "coder" } else { "worker" };
    let reject_target = if workflow == "std" {
        "in_progress"
    } else {
        "working"
    };
    match variant {
        "role_exec_done" => {
            if let Some(role) = condition_role_for(workflow, state) {
                seed_execution(
                    db,
                    &task.id,
                    Some(agent),
                    role,
                    ExecutionStatus::Completed,
                    Some("thread"),
                    "2026-10-02T00:00:00Z",
                )
                .await;
            } else {
                seed_execution(
                    db,
                    &task.id,
                    Some(agent),
                    work_role,
                    ExecutionStatus::Completed,
                    Some("thread"),
                    "2026-10-02T00:00:00Z",
                )
                .await;
            }
        }
        "review_failed" | "review_awaiting" | "review_running_orphan" | "review_passed" => {
            let execution = seed_execution(
                db,
                &task.id,
                Some(agent),
                work_role,
                ExecutionStatus::Completed,
                Some("candidate"),
                "2026-10-02T00:00:00Z",
            )
            .await;
            let status = match variant {
                "review_failed" => ReviewStatus::Failed,
                "review_awaiting" => ReviewStatus::AwaitingHuman,
                "review_passed" => ReviewStatus::Passed,
                _ => ReviewStatus::Running,
            };
            let now = now_rfc3339();
            db::ReviewRepo::create(
                db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    execution_id: execution.id.clone(),
                    attempt_number: 1,
                    status,
                    step_results_json: json!({"ci_steps":[]}).to_string(),
                    started_at: now.clone(),
                    created_at: now.clone(),
                    updated_at: now,
                },
            )
            .await
            .unwrap();
        }
        "rejections_max" => {
            seed_execution(
                db,
                &task.id,
                Some(agent),
                work_role,
                ExecutionStatus::Completed,
                Some("candidate"),
                "2026-10-02T00:00:00Z",
            )
            .await;
            for _ in 0..3 {
                db::TransitionLogRepo::insert(
                    db,
                    db::CreateTransitionLog {
                        id: new_uuid_v4(),
                        task_id: task.id.clone(),
                        from_state: "review".to_owned(),
                        to_state: reject_target.to_owned(),
                        trigger_name: Some("reject".to_owned()),
                        triggered_by: "user".to_owned(),
                        bridge: Default::default(),
                        trigger_reason: "fixture rejection".to_owned(),
                        hook_results_json: None,
                        rejection: true,
                        created_at: now_rfc3339(),
                    },
                )
                .await
                .unwrap();
            }
        }
        "failed_json" => {
            sqlx::query("UPDATE task SET failed_json = ? WHERE id = ?")
                .bind(json!({"kind":"executor_failed","reason":"failed fixture"}).to_string())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
            db.check_task_conditions_of(std::slice::from_ref(&task.id))
                .await
                .unwrap();
        }
        "barrier_blocked" => {
            sqlx::query("UPDATE task SET entry_barrier_json = ? WHERE id = ?")
                .bind(json!({"state":state,"status":"blocked","hook_results":[]}).to_string())
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
            db.check_task_conditions_of(std::slice::from_ref(&task.id))
                .await
                .unwrap();
        }
        "blocked_retry_exhausted" => {
            sqlx::query("UPDATE task SET blocked_json = ? WHERE id = ?")
                .bind(
                    json!({"kind":"retry_exhausted","reason":"spent","created_at":now_rfc3339()})
                        .to_string(),
                )
                .bind(&task.id)
                .execute(db.pool())
                .await
                .unwrap();
            db.check_task_conditions_of(std::slice::from_ref(&task.id))
                .await
                .unwrap();
        }
        _ => {}
    }
    TaskRepo::get_by_id(db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
}

fn condition_short(error: &ServiceError) -> String {
    let mut text = format!("{error}");
    text.retain(|character| character != '\n' && character != '\t');
    // strip uuids so identical failures aggregate
    let mut out = String::new();
    for word in text.split(' ') {
        if word.len() >= 32 && word.chars().filter(|character| *character == '-').count() >= 4 {
            out.push_str("<id> ");
        } else {
            out.push_str(word);
            out.push(' ');
        }
    }
    out.chars().take(160).collect()
}

fn condition_marker_present(task: &Task) -> bool {
    task.metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|metadata| metadata.get("queued_recovery").is_some())
}

/// The snapshot count of the unsplit matrix (one test, one database) at
/// `2e75beaf`: every (workflow, state, variant, failure kind) cell. The cases
/// below partition those cells by failure kind.
const CONDITION_MATRIX_SNAPSHOTS: usize = 745;

fn condition_states(workflow: &str) -> Vec<&'static str> {
    if workflow == "std" {
        vec![
            "backlog",
            "todo",
            "planning",
            "in_progress",
            "review",
            "merging",
            "merge_failed",
        ]
    } else {
        vec![
            "backlog",
            "ready",
            "working",
            "review",
            "merging",
            "merge_failed",
        ]
    }
}

fn condition_variants(state: &str) -> Vec<&'static str> {
    match state {
        "review" => vec![
            "bare",
            "role_exec_done",
            "review_failed",
            "review_awaiting",
            "review_running_orphan",
            "review_passed",
            "rejections_max",
            "failed_json",
            "barrier_blocked",
            "blocked_retry_exhausted",
        ],
        "backlog" => vec!["bare", "failed_json"],
        _ => vec![
            "bare",
            "role_exec_done",
            "rejections_max",
            "failed_json",
            "barrier_blocked",
            "blocked_retry_exhausted",
        ],
    }
}

fn condition_variant_kinds(variant: &str) -> Vec<Option<FailureKind>> {
    match variant {
        "bare" | "role_exec_done" => CONDITION_KINDS.to_vec(),
        "review_failed" => vec![
            None,
            Some(FailureKind::ReviewBlocked),
            Some(FailureKind::ReviewNeedsOwner),
            Some(FailureKind::ReviewBudgetExhausted),
            Some(FailureKind::ReviewGateFailed),
            Some(FailureKind::ExecutorFailed),
        ],
        "barrier_blocked" => vec![None, Some(FailureKind::BeforeWorkHookFailed)],
        "rejections_max" => vec![
            None,
            Some(FailureKind::ReviewBudgetExhausted),
            Some(FailureKind::ExecutorFailed),
        ],
        _ => vec![None],
    }
}

/// Workflow, state, variant, failure kind.
type ConditionCell = (
    &'static str,
    &'static str,
    &'static str,
    Option<FailureKind>,
);

/// Every cell of the unsplit matrix, in its order.
fn condition_cells() -> Vec<ConditionCell> {
    let mut cells = Vec::new();
    for workflow in ["std", "auto"] {
        for state in condition_states(workflow) {
            for variant in condition_variants(state) {
                for kind in condition_variant_kinds(variant) {
                    cells.push((workflow, state, variant, kind));
                }
            }
        }
    }
    cells
}

fn condition_kind_name(kind: Option<FailureKind>) -> String {
    kind.map(|kind| kind.to_string())
        .unwrap_or_else(|| "none".to_owned())
}

#[derive(Clone, Copy, Default)]
struct ConditionTotals {
    snapshots: usize,
    accepted: usize,
    applied: usize,
    restored: usize,
    dead_ends: usize,
    command_errors: usize,
}

impl ConditionTotals {
    fn line(&self, head: &str) -> String {
        format!(
            "{head} snapshots={} accepted={} applied={} restored_with_error={} dead_ends={} command_errors={}",
            self.snapshots,
            self.accepted,
            self.applied,
            self.restored,
            self.dead_ends,
            self.command_errors
        )
    }
}

/// The parts this process has produced, by position in `CONDITION_KINDS`.
static CONDITION_PARTS: std::sync::Mutex<
    std::collections::BTreeMap<usize, (String, ConditionTotals)>,
> = std::sync::Mutex::new(std::collections::BTreeMap::new());

/// Writes this case's part (`db-matrix.parts/<nn>-<kind>.txt`) and rebuilds
/// the combined `db-matrix.txt` from the parts of this process, in the order
/// of `CONDITION_KINDS`. The last case to finish leaves the whole matrix
/// there; a filtered run leaves the cases it ran and says so on the last line.
fn condition_write_evidence(only: Option<FailureKind>, out: String, totals: ConditionTotals) {
    let index = CONDITION_KINDS
        .iter()
        .position(|kind| *kind == only)
        .expect("a case of CONDITION_KINDS");
    let dir = crate::task_actions::action_test_evidence_dir();
    let parts_dir = dir.join("db-matrix.parts");
    std::fs::create_dir_all(&parts_dir).unwrap();
    std::fs::write(
        parts_dir.join(format!("{index:02}-{}.txt", condition_kind_name(only))),
        format!("{out}{}\n", totals.line("MATRIX_PART")),
    )
    .unwrap();
    let mut parts = CONDITION_PARTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    parts.insert(index, (out, totals));
    let mut combined = String::new();
    let mut sum = ConditionTotals::default();
    for (part, totals) in parts.values() {
        combined.push_str(part);
        sum.snapshots += totals.snapshots;
        sum.accepted += totals.accepted;
        sum.applied += totals.applied;
        sum.restored += totals.restored;
        sum.dead_ends += totals.dead_ends;
        sum.command_errors += totals.command_errors;
    }
    let _ = writeln!(combined, "{}", sum.line("MATRIX_RESULT"));
    let complete = parts.len() == CONDITION_KINDS.len();
    if !complete {
        let _ = writeln!(
            combined,
            "MATRIX_PARTIAL cases={}/{}",
            parts.len(),
            CONDITION_KINDS.len()
        );
    }
    std::fs::write(dir.join("db-matrix.txt"), combined).unwrap();
    drop(parts);
    if complete {
        assert_eq!(
            sum.snapshots, CONDITION_MATRIX_SNAPSHOTS,
            "the cases together must run the whole matrix"
        );
    }
}

/// The cases cover the unsplit matrix exactly: each cell belongs to the one
/// case of its failure kind, and the cells add up to the old snapshot count.
#[test]
fn condition_matrix_cases_partition_the_matrix() {
    let cells = condition_cells();
    assert_eq!(cells.len(), CONDITION_MATRIX_SNAPSHOTS);
    let mut distinct = std::collections::HashSet::new();
    for cell in &cells {
        assert!(
            distinct.insert((cell.0, cell.1, cell.2, condition_kind_name(cell.3))),
            "{cell:?} appears twice"
        );
    }
    let mut covered = 0;
    for (index, kind) in CONDITION_KINDS.iter().enumerate() {
        assert!(
            !CONDITION_KINDS[..index].contains(kind),
            "{kind:?} has two cases"
        );
        let own = cells.iter().filter(|cell| cell.3 == *kind).count();
        assert!(own > 0, "the case of {kind:?} is empty");
        covered += own;
    }
    assert_eq!(covered, cells.len(), "a cell's kind has no case");
}

/// One failure kind's slice of the matrix, on its own database.
async fn condition_matrix_case(only: Option<FailureKind>) {
    let db = Arc::new(sqlite_db().await);
    let (mut snapshots, mut accepted, mut applied, mut restored, mut dead_ends, mut command_errors) =
        (0usize, 0usize, 0usize, 0usize, 0usize, 0usize);
    let mut out = String::new();
    let all_verbs = [
        TaskAction::Start,
        TaskAction::Hold { reason: None },
        TaskAction::Release { reason: None },
        TaskAction::retry(),
        TaskAction::SendBack {
            guidance: "Revise".to_owned(),
        },
        TaskAction::Approve {
            reason: None,
            override_checks: Some(false),
        },
        TaskAction::Restart { reason: None },
        TaskAction::Cancel { reason: None },
    ];
    for workflow in ["std", "auto"] {
        let (project, _, repo_dir) = seed_project_repo(&db).await;
        initialize_primary_repository(&repo_dir);
        // Settlement matrix isolates permanent outcomes; capacity/pause waits
        // are covered separately by the scenario tests.
        sqlx::query("UPDATE project SET settings = json_set(settings, '$.max_active_tasks', 0) WHERE id = ?")
            .bind(&project).execute(db.pool()).await.unwrap();
        let states = condition_states(workflow);
        if workflow != "std" {
            let definition =
                crate::workflow::default_autonomous_workflow::default_autonomous_workflow();
            sqlx::query("UPDATE project SET workflow_definition = ? WHERE id = ?")
                .bind(serde_json::to_string(&definition).unwrap())
                .bind(&project)
                .execute(db.pool())
                .await
                .unwrap();
        }
        let agent = seed_agent(&db).await;
        let row = AgentRepo::get_by_id(&*db, &agent).await.unwrap().unwrap();
        AgentRepo::update(
            &*db,
            db::UpdateAgent {
                id: row.id,
                expected_version: row.version,
                name: None,
                description: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: None,
                config_json: None,
                daemon_id: Some(None),
                max_concurrent_tasks: None,
                heartbeat_interval_seconds: None,
                max_missed_heartbeats: None,
                status: None,
                last_heartbeat_at: None,
                is_default: None,
                paused: None,
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let service = TaskService::new(Arc::clone(&db), Arc::new(EventBus::new(4096)))
            .with_workspace_root(repo_dir.path().join("workspaces"))
            .with_task_executor(Arc::new(ConditionPendingExecutor));
        for state in states {
            for variant in condition_variants(state) {
                let variant_kinds = condition_variant_kinds(variant)
                    .into_iter()
                    .filter(|kind| *kind == only);
                for kind in variant_kinds {
                    snapshots += 1;
                    let label = format!(
                        "{workflow}|{state}|{variant}|{}",
                        kind.map(|kind| kind.to_string())
                            .unwrap_or_else(|| "none".to_owned())
                    );
                    let task =
                        condition_fixture(&db, &project, &agent, workflow, state, kind, variant)
                            .await;
                    let offers = match service
                        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
                        .await
                    {
                        Ok(response) => response.available_actions,
                        Err(error) => {
                            let _ = writeln!(
                                out,
                                "CONDITION|OFFERS_ERR|{label}|{}",
                                condition_short(&error)
                            );
                            continue;
                        }
                    };
                    let offered: Vec<String> = offers
                        .iter()
                        .map(|offer| format!("{}[{}]", offer.action.verb(), offer.reason))
                        .collect();
                    let _ = writeln!(out, "CONDITION|OFFERS|{label}|{}", offered.join(","));
                    if offers.iter().all(|offer| offer.action.verb() == "cancel") {
                        let _ =
                            writeln!(out, "CONDITION|DEAD_INITIAL|{label}|{}", offered.join(","));
                    }
                    for action in &all_verbs {
                        if offers
                            .iter()
                            .any(|offer| offer.action.verb() == action.verb())
                        {
                            continue;
                        }
                        match service
                            .perform_task_action(task.id.clone(), action.clone(), task.version)
                            .await
                        {
                            Err(ServiceError::TaskActionUnavailable { .. }) => {}
                            Ok(_) => {
                                let _ = writeln!(
                                    out,
                                    "CONDITION|ABSENT_ACCEPTED|{label}|{}",
                                    action.verb()
                                );
                            }
                            Err(error) => {
                                let _ = writeln!(
                                    out,
                                    "CONDITION|ABSENT_OTHER_ERR|{label}|{}|{}",
                                    action.verb(),
                                    condition_short(&error)
                                );
                            }
                        }
                    }
                    let mut attempts: Vec<(String, TaskAction)> = Vec::new();
                    for offer in &offers {
                        if offer.action.verb() == "cancel" {
                            continue;
                        }
                        attempts.push((
                            format!("{}[{}]", offer.action.verb(), offer.reason),
                            offer.action.clone(),
                        ));
                        if let TaskAction::Retry {
                            reason: None,
                            fresh_session,
                            refresh_workspace,
                            reset_budget,
                            guidance,
                        } = &offer.action
                        {
                            for parameter in &offer.parameters {
                                match parameter.name.as_str() {
                                    "fresh_session" => {
                                        let flipped = !fresh_session.unwrap_or(false);
                                        attempts.push((
                                            format!(
                                                "retry[{}]+fresh_session={flipped}",
                                                offer.reason
                                            ),
                                            TaskAction::Retry {
                                                reason: None,
                                                fresh_session: Some(flipped),
                                                refresh_workspace: *refresh_workspace,
                                                reset_budget: *reset_budget,
                                                guidance: guidance.clone(),
                                            },
                                        ));
                                    }
                                    "refresh_workspace" => {
                                        attempts.push((
                                            format!(
                                                "retry[{}]+refresh_workspace=true",
                                                offer.reason
                                            ),
                                            TaskAction::Retry {
                                                reason: None,
                                                fresh_session: *fresh_session,
                                                refresh_workspace: Some(true),
                                                reset_budget: *reset_budget,
                                                guidance: guidance.clone(),
                                            },
                                        ));
                                    }
                                    "reset_budget" => {
                                        for value in
                                            parameter.boolean_values.clone().unwrap_or_default()
                                        {
                                            if Some(value) != *reset_budget {
                                                attempts.push((
                                                    format!(
                                                        "retry[{}]+reset_budget={value}",
                                                        offer.reason
                                                    ),
                                                    TaskAction::Retry {
                                                        reason: None,
                                                        fresh_session: *fresh_session,
                                                        refresh_workspace: *refresh_workspace,
                                                        reset_budget: Some(value),
                                                        guidance: guidance.clone(),
                                                    },
                                                ));
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        if let TaskAction::Approve {
                            override_checks, ..
                        } = &offer.action
                        {
                            for value in offer
                                .parameters
                                .iter()
                                .find(|parameter| parameter.name == "override")
                                .and_then(|parameter| parameter.boolean_values.clone())
                                .unwrap_or_default()
                            {
                                if Some(value) != *override_checks {
                                    attempts.push((
                                        format!("approve[{}]+override={value}", offer.reason),
                                        TaskAction::Approve {
                                            override_checks: Some(value),
                                            reason: None,
                                        },
                                    ));
                                }
                            }
                        }
                    }
                    for (tag, attempt) in &attempts {
                        let mut attempt = attempt.clone();
                        match &mut attempt {
                            TaskAction::Approve { reason, .. }
                            | TaskAction::Retry { reason, .. }
                            | TaskAction::Cancel { reason } => {
                                *reason = Some("Matrix operator reason".to_owned())
                            }
                            TaskAction::SendBack { guidance } => {
                                *guidance = "Matrix requested correction".to_owned()
                            }
                            _ => {}
                        }
                        eprintln!("MATRIX_PROGRESS {label} {tag}: command");
                        let copy = condition_fixture(
                            &db, &project, &agent, workflow, state, kind, variant,
                        )
                        .await;
                        let command = tokio::time::timeout(
                            Duration::from_secs(60),
                            service.perform_task_action(
                                copy.id.clone(),
                                attempt.clone(),
                                copy.version,
                            ),
                        )
                        .await;
                        let command_result = match command {
                            Err(_) => "CMD_TIMEOUT".to_owned(),
                            Ok(Ok(_)) => "ok".to_owned(),
                            Ok(Err(ServiceError::Db(
                                DbError::VersionConflict | DbError::TaskVersionConflict { .. },
                            ))) => "version_conflict".to_owned(),
                            Ok(Err(error)) => format!("CMD_ERR:{}", condition_short(&error)),
                        };
                        let mut dispatch_results = Vec::new();
                        for _ in 0..3 {
                            service.drain(&copy.id).await.unwrap();
                            let current = TaskRepo::get_by_id(&*db, &copy.id, false)
                                .await
                                .unwrap()
                                .unwrap();
                            if !condition_marker_present(&current) {
                                break;
                            }
                            eprintln!("MATRIX_PROGRESS {label} {tag}: dispatch");
                            let result = tokio::time::timeout(
                                Duration::from_secs(60),
                                service.dispatch_queued_recovery(&current),
                            )
                            .await;
                            dispatch_results.push(match result {
                                Err(_) => "DISPATCH_TIMEOUT".to_owned(),
                                Ok(Ok(true)) => "applied".to_owned(),
                                Ok(Ok(false)) => "not_applied".to_owned(),
                                Ok(Err(error)) => {
                                    format!("DISPATCH_ERR:{}", condition_short(&error))
                                }
                            });
                        }
                        let current = TaskRepo::get_by_id(&*db, &copy.id, false)
                            .await
                            .unwrap()
                            .unwrap();
                        let running = ExecutionRepo::list_running_by_task(&*db, &copy.id)
                            .await
                            .unwrap();
                        let marker = condition_marker_present(&current);
                        // The applied action's entry hook step is queued work
                        // in flight, like a running execution; offers wait
                        // (Cancel only) until it settles.
                        let hooks_pending = db::TaskStepRepo::entry_hooks_pending(&*db, &copy.id)
                            .await
                            .unwrap();
                        let after = service
                            .task_action_offers(&copy.id, &Actor::user(UserActionSource::Test))
                            .await
                            .map(|response| {
                                response
                                    .available_actions
                                    .iter()
                                    .map(|offer| {
                                        format!("{}[{}]", offer.action.verb(), offer.reason)
                                    })
                                    .collect::<Vec<_>>()
                                    .join(",")
                            })
                            .unwrap_or_else(|error| {
                                format!("OFFERS_ERR:{}", condition_short(&error))
                            });
                        let annotation_kind = current
                            .error_annotation
                            .as_deref()
                            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                            .and_then(|value| {
                                value.get("type").and_then(Value::as_str).map(str::to_owned)
                            })
                            .unwrap_or_else(|| "-".to_owned());
                        let terminal = matches!(current.status.as_str(), "done" | "cancelled");
                        let only_cancel = after
                            .split(',')
                            .all(|verb| verb.is_empty() || verb.starts_with("cancel["));
                        let mut flags = Vec::new();
                        if command_result == "ok" {
                            accepted += 1;
                        }
                        if command_result.starts_with("CMD_") {
                            command_errors += 1;
                            flags.push("PROPERTY_VIOLATION");
                        }
                        if dispatch_results
                            .iter()
                            .any(|result| result.starts_with("DISPATCH_"))
                        {
                            flags.push("DISPATCH_FAILED");
                        }
                        if marker && running.is_empty() {
                            flags.push("MARKER_STUCK");
                        }
                        if !terminal && running.is_empty() && !hooks_pending && only_cancel {
                            flags.push("DEAD_END");
                            if command_result == "ok" {
                                dead_ends += 1;
                            }
                        }
                        if command_result == "ok" && !marker {
                            if dispatch_results
                                .iter()
                                .any(|result| result.starts_with("DISPATCH_ERR:"))
                            {
                                assert!(
                                    current.error_annotation.is_some()
                                        || current.blocked_json.is_some()
                                        || current.failed_json.is_some(),
                                    "{label} {tag}: refusal lost its condition; dispatch={dispatch_results:?}; current={current:?}"
                                );
                                restored += 1;
                            } else {
                                applied += 1;
                            }
                        }
                        let _ = writeln!(
                            out,
                            "CONDITION|APPLY|{label}|{tag}|cmd={command_result}|dispatch={}|status={}|ann={annotation_kind}|marker={marker}|running={}|after={after}|{}",
                            dispatch_results.join(">"),
                            current.status,
                            running.len(),
                            flags.join("+"),
                        );
                        sqlx::query(
                            "UPDATE execution SET status = 'cancelled' WHERE status = 'running'",
                        )
                        .execute(db.pool())
                        .await
                        .unwrap();
                    }
                }
            }
        }
    }
    let expected = condition_cells()
        .iter()
        .filter(|cell| cell.3 == only)
        .count();
    assert_eq!(snapshots, expected, "the case must run each of its cells");
    condition_write_evidence(
        only,
        out,
        ConditionTotals {
            snapshots,
            accepted,
            applied,
            restored,
            dead_ends,
            command_errors,
        },
    );
    assert_eq!(
        command_errors, 0,
        "advertised parameter values must be accepted; see db-matrix.txt"
    );
    assert_eq!(
        dead_ends, 0,
        "accepted actions must not dead-end; see db-matrix.txt"
    );
    assert_eq!(
        accepted,
        applied + restored,
        "each accepted action must settle; see db-matrix.txt"
    );
}

/// `condition_offer_apply_dispatch_matrix` was one test that ran the whole
/// matrix serially on one database (about 300 s). Each failure kind is now a
/// case with its own database, so the cases run in parallel; the old name
/// still selects them all as a libtest filter.
macro_rules! condition_matrix_cases {
    ($($name:ident => $kind:expr,)+) => {
        const CONDITION_KINDS: &[Option<FailureKind>] = &[$($kind),+];
        mod condition_offer_apply_dispatch_matrix {
            use super::*;
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
                async fn $name() {
                    condition_matrix_case($kind).await;
                }
            )+
        }
    };
}

condition_matrix_cases! {
    none => None,
    merge_conflict => Some(FailureKind::MergeConflict),
    target_repo_dirty => Some(FailureKind::TargetRepoDirty),
    dirty_worktree => Some(FailureKind::DirtyWorktree),
    ci_failed => Some(FailureKind::CiFailed),
    review_gate_failed => Some(FailureKind::ReviewGateFailed),
    review_budget_exhausted => Some(FailureKind::ReviewBudgetExhausted),
    review_blocked => Some(FailureKind::ReviewBlocked),
    review_needs_owner => Some(FailureKind::ReviewNeedsOwner),
    environment_not_ready => Some(FailureKind::EnvironmentNotReady),
    retry_exhausted => Some(FailureKind::RetryExhausted),
    merge_fix_budget_exhausted => Some(FailureKind::MergeFixBudgetExhausted),
    workflow_guard_rejected => Some(FailureKind::WorkflowGuardRejected),
    internal_command_failed => Some(FailureKind::InternalCommandFailed),
    executor_failed => Some(FailureKind::ExecutorFailed),
    workspace_failed => Some(FailureKind::WorkspaceFailed),
    workspace_reset_required => Some(FailureKind::WorkspaceResetRequired),
    workspace_error => Some(FailureKind::WorkspaceError),
    before_work_hook_timeout => Some(FailureKind::BeforeWorkHookTimeout),
    before_work_hook_failed => Some(FailureKind::BeforeWorkHookFailed),
    max_turns_exceeded => Some(FailureKind::MaxTurnsExceeded),
    manual_stop => Some(FailureKind::ManualStop),
    recovery_required => Some(FailureKind::RecoveryRequired),
    executor_unavailable => Some(FailureKind::ExecutorUnavailable),
    unknown => Some(FailureKind::Unknown),
    dispatch_failed => Some(FailureKind::DispatchFailed),
    dependency_cancelled => Some(FailureKind::DependencyCancelled),
}

#[tokio::test]
async fn queued_role_retry_preserves_a_condition_when_dispatch_refuses() {
    let db = Arc::new(sqlite_db().await);
    let (project, _, repo) = seed_project_repo(&db).await;
    initialize_primary_repository(&repo);
    let agent = seed_agent(&db).await;
    let row = AgentRepo::get_by_id(&*db, &agent).await.unwrap().unwrap();
    AgentRepo::update(
        &*db,
        db::UpdateAgent {
            id: row.id,
            expected_version: row.version,
            name: None,
            description: None,
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: None,
            config_json: None,
            daemon_id: Some(None),
            max_concurrent_tasks: None,
            heartbeat_interval_seconds: None,
            max_missed_heartbeats: None,
            status: None,
            last_heartbeat_at: None,
            is_default: None,
            paused: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    let service = TaskService::new(db.clone(), Arc::new(EventBus::default()))
        .with_workspace_root(repo.path().join("workspaces"))
        .with_task_executor(Arc::new(ConditionPendingExecutor));
    let task = condition_fixture(
        &db,
        &project,
        &agent,
        "std",
        "planning",
        Some(FailureKind::ExecutorUnavailable),
        "bare",
    )
    .await;
    let action = service
        .task_action_offers(&task.id, &Actor::user(UserActionSource::Test))
        .await
        .unwrap()
        .available_actions
        .into_iter()
        .find(|offer| offer.action.verb() == "retry" && offer.reason == "role_retry")
        .unwrap()
        .action;
    service
        .perform_task_action(&task.id, action, task.version)
        .await
        .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    let outcome = service.dispatch_queued_recovery(&task).await;
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    if outcome.is_err() {
        assert!(
            condition_marker_present(&current)
                || current.error_annotation.is_some()
                || current.blocked_json.is_some()
                || current.failed_json.is_some(),
            "refusal lost condition: {outcome:?}, original={:?}, current={current:?}",
            task.metadata_json
        );
    }
}
