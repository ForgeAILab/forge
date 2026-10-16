//! The review contract driven end to end at crate level: one review attempt
//! as the contract sees it, without the workflow engine. `ContractDriver`
//! does only what surrounds the contract on the ordinary review path: it
//! records the entry checks on a running attempt, binds a reviewer execution
//! to that attempt, hands the reviewer the frozen prompt, and has Forge
//! evaluate the reply. Everything asserted here is the contract's behaviour
//! (`prepare_prompt`, `evaluate`, `review_source`).
use crate::{ReviewOutcome, ReviewWorkspace};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use db::{
    create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations, AgentRepo, AgentStatus,
    ClaimExecutionLease, CommentAuthorType, CreateAgent, CreateExecution, CreateProject,
    CreateRepo, CreateReview, CreateTask, CreateTaskComment, CreateTaskMedia,
    CreateTaskRoleAssignment, CreateWorkspace, DaemonRepo, DaemonStatus, ExecutionAdmission,
    ExecutionRepo, ExecutionStatus, ProjectRepo, RepoRepo, Review, ReviewConformanceRepo,
    ReviewRepo, ReviewStatus, SqliteDb, TaskCommentRepo, TaskMediaRepo, TaskRepo,
    TaskRoleAssignmentRepo, UpdateProject, UpsertDaemon, WorkspaceRepo, WorkspaceStatus,
};
use events::EventBus;
use executors::{
    ExecutionContext, ExecutionOutcome, LogEntry, LogKind, LogStream, LogWriter, TaskExecutor,
};
use serde_json::{json, Value};
use std::{path::Path, sync::Arc};
use tempfile::TempDir;
use uuid::Uuid;

const MAX_LOG_BYTES: u64 = 10 * 1024 * 1024;

/// What a test asks the driver for: where the reviewer logs, and which Agent
/// reviews.
struct DrivenReview {
    entry_triage: Option<api_types::StepCiTriage>,
    task_id: Uuid,
    executor_execution_id: Uuid,
    workspace_path: std::path::PathBuf,
    logs_path: String,
    auditor_agent_id: Option<String>,
}

/// The reviewer finished but Forge has no verdict: its reply was unusable,
/// its environment blocked it, or a check could not be evaluated.
#[derive(Debug)]
struct Unverified {
    execution_id: String,
    reason: String,
}

struct ContractDriver {
    db: Arc<SqliteDb>,
    reviewer: Arc<dyn TaskExecutor>,
}

impl ContractDriver {
    fn new(db: Arc<SqliteDb>, _event_bus: Arc<EventBus>, reviewer: Arc<dyn TaskExecutor>) -> Self {
        Self { db, reviewer }
    }

    async fn run(&self, req: DrivenReview) -> Result<(Review, ReviewOutcome), Unverified> {
        let db = &*self.db;
        let task_id = req.task_id.to_string();
        let candidate_id = req.executor_execution_id.to_string();
        let workspace_path = req.workspace_path.clone();
        let workspace: &dyn ReviewWorkspace = &workspace_path;
        let task = TaskRepo::get_by_id(db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        let project = ProjectRepo::get_by_id(db, &task.project_id)
            .await
            .unwrap()
            .unwrap();
        let candidate = ExecutionRepo::get_by_id(db, &candidate_id)
            .await
            .unwrap()
            .unwrap();
        let workspace_row =
            WorkspaceRepo::get_by_id(db, candidate.workspace_id.as_deref().unwrap())
                .await
                .unwrap()
                .unwrap();
        let repo = RepoRepo::get_by_id(db, &workspace_row.repo_id)
            .await
            .unwrap()
            .unwrap();
        let source = db
            .review_source(&task_id, Some(&candidate_id))
            .await
            .unwrap();
        let state_config = api_types::effective_review_config(&source).unwrap();
        let ci_steps: Vec<String> = state_config
            .get("ci_steps")
            .and_then(Value::as_array)
            .map(|steps| {
                steps
                    .iter()
                    .filter_map(|step| step.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        let review_prompt = state_config
            .get("review_prompt")
            .and_then(Value::as_str)
            .map(str::to_owned);

        // The entry checks, recorded on a running attempt as a review entry
        // records them.
        let now = now_rfc3339();
        let review = ReviewRepo::create(
            db,
            CreateReview {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                execution_id: candidate_id.clone(),
                attempt_number: 1,
                status: ReviewStatus::Running,
                step_results_json: json!({ "ci_steps": [] }).to_string(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("the review attempt opens");
        let environment = crate::contract::project_environment(db, &task_id)
            .await
            .unwrap();
        let mut ci_results = Vec::new();
        for (index, step) in ci_steps.iter().enumerate() {
            let started_at = now_rfc3339();
            let output = workspace
                .run(step, &environment.env, None)
                .await
                .expect("the entry check runs");
            let exit_code = output.exit_code.unwrap_or(1);
            if req.entry_triage.is_none() {
                assert_eq!(exit_code, 0, "entry check `{step}` passes before a review");
            }
            ci_results.push(json!({
                "index": index,
                "command": step,
                "exit_code": exit_code,
                "stderr_tail": output.stderr,
                "output_tail": format!("{}{}", output.stdout, output.stderr),
                "started_at": started_at,
                "finished_at": now_rfc3339(),
            }));
            if let Some(triage) = &req.entry_triage {
                ci_results.last_mut().unwrap()["triage"] = serde_json::to_value(triage).unwrap();
            }
        }
        sqlx::query("UPDATE review SET step_results_json = ?, updated_at = ? WHERE id = ?")
            .bind(json!({ "ci_steps": ci_results }).to_string())
            .bind(now_rfc3339())
            .bind(&review.id)
            .execute(db.pool())
            .await
            .unwrap();
        let review = ReviewRepo::get_by_id(db, &review.id)
            .await
            .unwrap()
            .unwrap();
        // Opening the attempt moved the Task's condition; admission is
        // checked against the Task as it is now.
        let task = TaskRepo::get_by_id(db, &task_id, false)
            .await
            .unwrap()
            .unwrap();

        // The reviewer execution, bound to the attempt and its candidate.
        let agent_id = req.auditor_agent_id.expect("a reviewing Agent is named");
        let agent = AgentRepo::get_by_id(db, &agent_id).await.unwrap().unwrap();
        let assignment = TaskRoleAssignmentRepo::get_by_task_and_role(db, &task_id, "reviewer")
            .await
            .unwrap();
        let execution_id = new_uuid_v4();
        let logs_path = Path::new(&req.logs_path)
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("{execution_id}.jsonl"))
            .display()
            .to_string();
        let snapshot = json!({
            "agent_id": agent.id,
            "executor_type": agent.executor_type,
            "config": {},
        });
        let now = Utc::now();
        let execution = ExecutionRepo::create_with_lease_and_admission(
            db,
            CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.clone(),
                agent_id: Some(agent.id.clone()),
                role: "reviewer".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: Some(candidate_id.clone()),
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: Some(logs_path.clone()),
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: Some(snapshot.to_string()),
                workspace_id: candidate.workspace_id.clone(),
                created_at: now.to_rfc3339(),
                updated_at: now.to_rfc3339(),
            },
            ClaimExecutionLease {
                execution_id: execution_id.clone(),
                expected_version: 1,
                owner: format!("contract-driver:{}", new_uuid_v4()),
                lease_expires_at: (now + ChronoDuration::minutes(10)).to_rfc3339(),
                hard_deadline_at: None,
                now: now.to_rfc3339(),
            },
            Some(ExecutionAdmission {
                purpose: None,
                expected_queued_recovery_id: None,
                expected_project_version: Some(project.version),
                expected_task_version: task.version,
                expected_task_status: task.status.clone(),
                expected_effective_role: Some("reviewer".to_owned()),
                expected_agent_version: Some(agent.version),
                expected_agent_max_concurrent_tasks: Some(agent.max_concurrent_tasks),
                expected_reviewer_parent_execution_id: Some(candidate_id.clone()),
                expected_latest_review_candidate_execution_id: Some(review.execution_id.clone()),
                expected_reviewer_id: Some(review.id.clone()),
                expected_reviewer_attempt_number: Some(review.attempt_number),
                expected_reviewer_status: Some(review.status.to_string()),
                expected_reviewer_updated_at: Some(review.updated_at.clone()),
                expected_reviewer_execution_id: review.reviewer_execution_id.clone(),
                expected_auditor_execution_id: review.auditor_execution_id.clone(),
                expected_assignment_id: assignment.as_ref().map(|a| a.id.clone()),
                expected_assignment_updated_at: assignment.as_ref().map(|a| a.updated_at.clone()),
                expected_workflow_definition: Some(project.workflow_definition.clone()),
            }),
        )
        .await
        .expect("the reviewer execution is admitted for the attempt");

        let diff = workspace.diff(&repo.default_branch).await.unwrap();
        // The base text the review contract is assembled around: the
        // Project's own prompt when it has one.
        let prompt = review_prompt.clone().unwrap_or_else(|| {
            format!(
                "Review this task implementation.\n\nTask title:\n{}\n\nTask description:\n{}\n\nGit diff:\n```diff\n{diff}\n```",
                task.title,
                task.description
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("(no description)"),
            )
        });
        let unverified = |reason: String| Unverified {
            execution_id: execution.id.clone(),
            reason,
        };
        let prompt = crate::contract::prepare_prompt(
            db,
            &execution.id,
            &task_id,
            workspace,
            true,
            agent.executor_type == "shell",
            prompt,
        )
        .await
        .map_err(unverified)?;
        let result = self
            .reviewer
            .execute(ExecutionContext {
                task_id: task_id.clone(),
                execution_id: execution.id.clone(),
                worktree_path: workspace.execution_path().await.unwrap(),
                description: prompt,
                agent_config: snapshot,
                logs_path: logs_path.clone(),
                heartbeat_interval_seconds: 30,
                max_turns: None,
                log_sender: None,
            })
            .await
            .expect("the reviewer runs");
        assert_eq!(result.status, ExecutionOutcome::Completed);
        sqlx::query("UPDATE execution SET status = 'completed', stopped_at = ?, updated_at = ? WHERE id = ?")
            .bind(now_rfc3339())
            .bind(now_rfc3339())
            .bind(&execution.id)
            .execute(db.pool())
            .await
            .unwrap();

        let reply = last_assistant_text(&logs_path).await;
        let conformance = crate::contract::evaluate(db, &execution.id, workspace, &reply)
            .await
            .map_err(unverified)?;
        match conformance.status {
            api_types::ConformanceStatus::Unverified => {
                return Err(unverified(
                    conformance
                        .reason
                        .unwrap_or_else(|| "unverified review".into()),
                ));
            }
            api_types::ConformanceStatus::Blocked => {
                return Err(unverified(format!(
                    "review blocked by its environment: {}",
                    conformance.reason.as_deref().unwrap_or("no reason given")
                )));
            }
            _ => {}
        }
        let (status, outcome, auditor) =
            if conformance.status == api_types::ConformanceStatus::Passed {
                (
                    ReviewStatus::Passed,
                    ReviewOutcome::Passed,
                    json!({ "verdict": "pass" }),
                )
            } else {
                let reason = conformance
                    .reason
                    .clone()
                    .unwrap_or_else(|| "review conformance failed".into());
                (
                    ReviewStatus::Failed,
                    ReviewOutcome::AuditorFailed {
                        reason: reason.clone(),
                    },
                    json!({ "verdict": "fail", "reason": reason }),
                )
            };
        // Settled as the review path settles it: under the Task's and the
        // attempt's current authority, projecting a pass onto the Task.
        let finished = now_rfc3339();
        let task = TaskRepo::get_by_id(db, &task_id, false)
            .await
            .unwrap()
            .unwrap();
        let review = ReviewRepo::get_by_id(db, &review.id)
            .await
            .unwrap()
            .unwrap();
        let projection = match status {
            ReviewStatus::Passed => Some(Some(finished.clone())),
            _ if task.review_passed_at.is_some() => Some(None),
            _ => None,
        };
        ReviewRepo::update_status_with_review_authority_and_task_projection(
            db,
            &review.id,
            status,
            json!({ "ci_steps": ci_results, "auditor": auditor, "conformance": conformance })
                .to_string(),
            Some(finished.clone()),
            &finished,
            task.version,
            &task.status,
            Some(project.version),
            Some(project.workflow_definition.as_str()),
            review.status.clone(),
            &review.updated_at,
            &review.execution_id,
            projection,
        )
        .await
        .expect("the verdict settles the attempt");
        Ok((
            ReviewRepo::get_by_id(db, &review.id)
                .await
                .unwrap()
                .unwrap(),
            outcome,
        ))
    }
}

/// The reviewer's final message: the text of its last assistant log entry.
async fn last_assistant_text(logs_path: &str) -> String {
    let contents = tokio::fs::read_to_string(logs_path)
        .await
        .unwrap_or_default();
    contents
        .lines()
        .filter_map(|line| serde_json::from_str::<LogEntry>(line).ok())
        .filter(|entry| entry.kind == LogKind::Assistant)
        .filter_map(|entry| {
            entry
                .payload
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .next_back()
        .unwrap_or_default()
}

struct SeededReview {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
    task_id: Uuid,
    executor_execution_id: Uuid,
    auditor_agent_id: String,
    workspace: TempDir,
    logs_path: String,
}

async fn seeded_review(ci_steps: Vec<&str>) -> SeededReview {
    let pool = create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let event_bus = Arc::new(EventBus::with_default_capacity());
    let workspace = tempfile::tempdir().expect("workspace creates");
    let logs_path = workspace.path().join("review.jsonl").display().to_string();

    let now = now_rfc3339();
    let daemon_id = Uuid::new_v4().to_string();
    let project_id = Uuid::new_v4().to_string();
    let repo_id = Uuid::new_v4().to_string();
    let agent_id = Uuid::new_v4().to_string();
    let task_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4().to_string();
    let executor_execution_id = Uuid::new_v4();

    DaemonRepo::upsert_by_machine_id(
        &*db,
        UpsertDaemon {
            max_concurrent_runs: None,
            id: daemon_id.clone(),
            machine_id: format!("machine-{daemon_id}"),
            hostname: "test-host".to_owned(),
            os: "linux".to_owned(),
            arch: "x86_64".to_owned(),
            agent_version: None,
            labels_json: "{}".to_owned(),
            status: DaemonStatus::Online,
            registration_token_hash: None,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("daemon creates");

    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            name: "Forge".to_owned(),
            settings: "{}".to_owned(),
            workflow_definition: "{}".to_string(),
            primary_repo_id: None,
            owner_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("project creates");

    RepoRepo::create(
        &*db,
        CreateRepo {
            id: repo_id.clone(),
            project_id: project_id.clone(),
            name: "forge".to_owned(),
            remote_url: Some("https://example.com/forge.git".to_owned()),
            local_path: None,
            default_branch: "main".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("repo creates");
    ProjectRepo::update_at_version(
        &*db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo_id.clone())),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        ProjectRepo::get_by_id(&*db, &project_id)
            .await
            .expect("fixture Project lookup")
            .expect("fixture Project exists")
            .version,
        None,
    )
    .await
    .expect("project primary repo updates");

    AgentRepo::create(
        &*db,
        CreateAgent {
            id: agent_id.clone(),
            name: "shell".to_owned(),
            description: None,
            executor_type: "shell".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: Some(daemon_id),
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: None,
            visibility: "global".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("agent creates");

    let task_state_config = serde_json::to_string(&json!({
        "review": {
            "ci_steps": ci_steps,
        },
    }))
    .expect("task state config serializes");

    TaskRepo::create(
        &*db,
        CreateTask {
            id: task_id.to_string(),
            project_id,
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Review me".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "review".to_string(),
            is_automation: false,
            priority: 0,
            task_state_config: Some(task_state_config),
            merge_config: None,
            plan: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("task creates");

    TaskRoleAssignmentRepo::assign(
        &*db,
        CreateTaskRoleAssignment {
            id: Uuid::new_v4().to_string(),
            task_id: task_id.to_string(),
            role_name: "reviewer".to_owned(),
            assignee_type: Some(db::AssigneeKind::Agent),
            assignee_id: Some(agent_id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("reviewer assignment creates");

    WorkspaceRepo::create(
        &*db,
        CreateWorkspace {
            id: workspace_id.clone(),
            task_id: task_id.to_string(),
            repo_id,
            worktree_path: workspace.path().display().to_string(),
            branch: format!("forge/{task_id}"),
            status: WorkspaceStatus::Ready,
            before_sha: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("workspace creates");

    ExecutionRepo::create(
        &*db,
        CreateExecution {
            id: executor_execution_id.to_string(),
            task_id: task_id.to_string(),
            agent_id: Some(agent_id.clone()),
            role: "executor".to_owned(),
            status: ExecutionStatus::Completed,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("executor execution creates");

    SeededReview {
        db,
        event_bus,
        task_id,
        executor_execution_id,
        auditor_agent_id: agent_id,
        workspace,
        logs_path,
    }
}

#[tokio::test]
async fn unbound_reviewer_contract_cannot_use_empty_or_legacy_ci_results() {
    let seed = seeded_review(Vec::new()).await;
    let workspace = WorkspaceRepo::get_by_task_id(&*seed.db, &seed.task_id.to_string())
        .await
        .expect("workspace lookup succeeds")
        .expect("seed workspace exists");
    let now = now_rfc3339();
    let reviewer = ExecutionRepo::create(
        &*seed.db,
        CreateExecution {
            id: new_uuid_v4(),
            task_id: seed.task_id.to_string(),
            agent_id: Some(seed.auditor_agent_id.clone()),
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: Some(seed.executor_execution_id.to_string()),
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace.id.clone()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("unbound reviewer creates");
    ReviewRepo::create(
        &*seed.db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: seed.task_id.to_string(),
            execution_id: seed.executor_execution_id.to_string(),
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: json!({"ci_steps": []}).to_string(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("legacy unbound Review creates");

    let error = crate::contract::admit(
        &seed.db,
        &reviewer.id,
        &seed.task_id.to_string(),
        seed.workspace.path(),
    )
    .await
    .expect_err("unbound reviewer must fail closed before CI admission");
    assert!(error.contains("no review attempt is bound"));
}

fn request(seed: &SeededReview) -> DrivenReview {
    DrivenReview {
        entry_triage: None,
        task_id: seed.task_id,
        executor_execution_id: seed.executor_execution_id,
        workspace_path: seed.workspace.path().to_path_buf(),
        logs_path: seed.logs_path.clone(),
        auditor_agent_id: None,
    }
}

#[tokio::test]
async fn review_source_includes_task_worklog_and_media_deliverables() {
    let seed = seeded_review(Vec::new()).await;
    let now = now_rfc3339();
    TaskCommentRepo::create_comment(
        &*seed.db,
        CreateTaskComment {
            id: Uuid::new_v4().to_string(),
            task_id: seed.task_id.to_string(),
            author_type: CommentAuthorType::Agent,
            author_id: Some(seed.auditor_agent_id.clone()),
            author_name: "researcher".to_owned(),
            content: "Compared the two parser designs; option B preserves streaming.".to_owned(),
            execution_id: Some(seed.executor_execution_id.to_string()),
            role: Some("researcher".to_owned()),
            worklog_kind: Some("validation".to_owned()),
            idempotency_key: Some("research-result".to_owned()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("worklog creates");
    TaskMediaRepo::create_media(
        &*seed.db,
        CreateTaskMedia {
            id: Uuid::new_v4().to_string(),
            task_id: seed.task_id.to_string(),
            display_filename: "comparison.json".to_owned(),
            content_type: "application/json".to_owned(),
            byte_size: 42,
            storage_key: format!("task-media/{}.json", Uuid::new_v4()),
            author_type: CommentAuthorType::Agent,
            author_id: Some(seed.auditor_agent_id.clone()),
            author_name: "researcher".to_owned(),
            created_at: now,
        },
    )
    .await
    .expect("Task evidence creates");

    let source = seed
        .db
        .review_source(&seed.task_id.to_string(), None)
        .await
        .expect("review source loads");
    assert_eq!(
        source["task_scope"]["evidence"]["worklog"][0]["kind"],
        "validation"
    );
    assert_eq!(
        source["task_scope"]["evidence"]["worklog"][0]["content"],
        "Compared the two parser designs; option B preserves streaming."
    );
    assert_eq!(
        source["task_scope"]["evidence"]["media"][0]["filename"],
        "comparison.json"
    );
    assert!(source["task_scope"]["evidence"]["media"][0]["asset_id"].is_string());
    let context = crate::contract::context_from_source(&source).expect("context normalizes");
    assert_eq!(
        context.task_scope["evidence"]["worklog"][0]["kind"],
        "validation"
    );
}

#[tokio::test]
async fn review_source_keeps_execution_workspace_repository_after_project_reselection() {
    let seed = seeded_review(Vec::new()).await;
    let task = TaskRepo::get_by_id(&*seed.db, &seed.task_id.to_string(), false)
        .await
        .expect("task lookup succeeds")
        .expect("seed task exists");
    let workspace = WorkspaceRepo::get_by_task_id(&*seed.db, &task.id)
        .await
        .expect("workspace lookup succeeds")
        .expect("seed workspace exists");
    let replacement_repo_id = Uuid::new_v4().to_string();
    let now = now_rfc3339();
    RepoRepo::create(
        &*seed.db,
        CreateRepo {
            id: replacement_repo_id.clone(),
            project_id: task.project_id.clone(),
            name: "replacement".to_owned(),
            remote_url: Some("https://example.com/replacement.git".to_owned()),
            local_path: None,
            default_branch: "trunk".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("replacement repo creates");
    let project = ProjectRepo::get_by_id(&*seed.db, &task.project_id)
        .await
        .expect("project lookup succeeds")
        .expect("seed project exists");
    ProjectRepo::update_at_version(
        &*seed.db,
        UpdateProject {
            id: project.id,
            name: None,
            settings: None,
            primary_repo_id: Some(Some(replacement_repo_id)),
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .expect("project repo selection updates");

    let source = seed
        .db
        .review_source(&task.id, Some(&seed.executor_execution_id.to_string()))
        .await
        .expect("review source loads");

    assert_eq!(source["repo_id"], workspace.repo_id);
    assert_eq!(source["task_scope"]["default_branch"], "main");
}

fn contract_from_prompt(prompt: &str) -> api_types::ReviewContract {
    let raw = if let Some(line) = prompt
        .lines()
        .find(|line| line.starts_with("export FORGE_REVIEW_CONTRACT="))
    {
        line.strip_prefix("export FORGE_REVIEW_CONTRACT='")
            .unwrap()
            .strip_suffix('\'')
            .unwrap()
            .replace("'\"'\"'", "'")
    } else {
        prompt
            .rsplit("Frozen review contract:\n")
            .next()
            .unwrap()
            .to_owned()
    };
    serde_json::from_str(&raw).unwrap()
}

fn valid_report(prompt: &str, path: &str) -> String {
    let contract = contract_from_prompt(prompt);
    format!(
        "Every requirement is implemented; see `{path}`.\n\n{}",
        json!({"result": "pass", "reason": format!("{} requirement(s) met", contract.context.requirements.len())})
    )
}

struct PassingAuditor;

#[async_trait]
impl TaskExecutor for PassingAuditor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> Result<executors::ExecutionResult, executors::ExecutorError> {
        let report = valid_report(&ctx.description, "README.md");
        let mut writer = LogWriter::new(&ctx.logs_path, ctx.execution_id, MAX_LOG_BYTES);
        writer
            .write(LogKind::Assistant, LogStream::Main, json!({"text": report}))
            .await?;
        Ok(executors::ExecutionResult {
            status: ExecutionOutcome::Completed,
            ..Default::default()
        })
    }
    async fn cancel(&self, _: &str) -> Result<(), executors::ExecutorError> {
        Ok(())
    }
}

struct CheckResultAwareAuditor;

#[async_trait]
impl TaskExecutor for CheckResultAwareAuditor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> Result<executors::ExecutionResult, executors::ExecutorError> {
        let contract = contract_from_prompt(&ctx.description);
        assert_eq!(contract.check_results.len(), 1);
        assert_eq!(contract.check_results[0].check_id, "ci:0");
        assert_eq!(contract.check_results[0].command, "true");
        assert_eq!(contract.check_results[0].exit_code, 0);
        let report = format!(
            "Forge's recorded check `ci:0` passed.\n\n{}",
            json!({"result": "pass", "reason": "Forge's recorded independent check passed"})
        );
        let mut writer = LogWriter::new(&ctx.logs_path, ctx.execution_id, MAX_LOG_BYTES);
        writer
            .write(LogKind::Assistant, LogStream::Main, json!({"text": report}))
            .await?;
        Ok(executors::ExecutionResult {
            status: ExecutionOutcome::Completed,
            ..Default::default()
        })
    }

    async fn cancel(&self, _: &str) -> Result<(), executors::ExecutorError> {
        Ok(())
    }
}

async fn canonical_spec_for_test(
    seed: &SeededReview,
    purpose: api_types::CheckPurpose,
) -> api_types::CheckSpec {
    let source = seed
        .db
        .review_source(
            &seed.task_id.to_string(),
            Some(&seed.executor_execution_id.to_string()),
        )
        .await
        .unwrap();
    let environment = crate::contract::project_environment(&seed.db, &seed.task_id.to_string())
        .await
        .unwrap();
    crate::check_spec::build_check_spec(
        purpose,
        &crate::check_spec::CheckSpecConfiguration {
            source: &source,
            entry_state_config: None,
            environment: &environment,
            hooks: &[],
            hook_test_index: None,
            single_environment_check: None,
            event: api_types::LifecycleEvent::BeforeWork,
            role: "reviewer",
            owner_is_daemon: false,
            canonical_policy: false,
            workspace: Some(crate::check_spec::CheckWorkspaceIdentity {
                workspace_id: "workspace",
                generation: 1,
            }),
            queue_head_bundle: None,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn project_default_ci_steps_run_before_the_reviewer() {
    // Live case: NK-1's Project set `default_review_config.ci_steps` and the
    // Task had no review config of its own. The contract required `ci:0`, the
    // runner only read the Task config, never ran the step, and every review
    // failed admission with "pre-review result for required check ci:0 is
    // unavailable".
    let seed = seeded_review(Vec::new()).await;
    sqlx::query("UPDATE task SET task_state_config = NULL WHERE id = ?")
        .bind(seed.task_id.to_string())
        .execute(seed.db.pool())
        .await
        .unwrap();
    sqlx::query(
        "UPDATE project SET settings = ? WHERE id = (SELECT project_id FROM task WHERE id = ?)",
    )
    .bind(json!({"default_review_config": {"ci_steps": ["true"]}}).to_string())
    .bind(seed.task_id.to_string())
    .execute(seed.db.pool())
    .await
    .unwrap();
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("README.md"), "candidate\n")
        .await
        .unwrap();
    git::commit_all(seed.workspace.path(), "candidate")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ContractDriver::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(CheckResultAwareAuditor),
    );

    let spec = canonical_spec_for_test(&seed, api_types::CheckPurpose::ReviewCi).await;
    let (review, outcome) = runner.run(req).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert_eq!(details.conformance.checks[0].check_id, "ci:0");
    assert_eq!(
        spec.commands
            .iter()
            .map(|c| c.shell_text.as_str())
            .collect::<Vec<_>>(),
        details
            .ci_steps
            .iter()
            .map(|s| s.command.as_str())
            .collect::<Vec<_>>()
    );
    assert!(spec.commands.iter().all(|c| c.timeout_seconds.is_none()));
}

#[tokio::test]
async fn reviewer_receives_and_can_cite_the_recorded_pre_review_ci_result() {
    let seed = seeded_review(vec!["true"]).await;
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("README.md"), "candidate\n")
        .await
        .unwrap();
    git::commit_all(seed.workspace.path(), "candidate")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ContractDriver::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(CheckResultAwareAuditor),
    );

    let (review, outcome) = runner.run(req).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert_eq!(details.conformance.checks[0].check_id, "ci:0");
    assert_eq!(details.conformance.checks[0].exit_code, 0);
}

#[tokio::test]
async fn clean_review_checkout_runs_setup_before_required_checks() {
    let seed = seeded_review(vec!["test -f node_modules/ready"]).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(
            json!({
                "review": {
                    "setup_steps": ["mkdir -p node_modules && touch node_modules/ready"],
                    "ci_steps": ["test -f node_modules/ready"]
                }
            })
            .to_string(),
        )
        .bind(seed.task_id.to_string())
        .execute(seed.db.pool())
        .await
        .expect("review setup config updates");
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("README.md"), "candidate\n")
        .await
        .unwrap();
    git::commit_all(seed.workspace.path(), "candidate")
        .await
        .unwrap();
    tokio::fs::create_dir(seed.workspace.path().join("node_modules"))
        .await
        .unwrap();
    tokio::fs::write(seed.workspace.path().join("node_modules/ready"), "ready")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ContractDriver::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(PassingAuditor),
    );

    let spec = canonical_spec_for_test(&seed, api_types::CheckPurpose::Conformance).await;
    let (review, outcome) = runner.run(req).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert_eq!(details.conformance.checks.len(), 2);
    assert_eq!(
        spec.commands
            .iter()
            .map(|c| (&c.id, &c.shell_text))
            .collect::<Vec<_>>(),
        details
            .conformance
            .checks
            .iter()
            .map(|c| (&c.check_id, &c.command))
            .collect::<Vec<_>>()
    );
    assert!(spec
        .commands
        .iter()
        .all(|c| c.timeout_seconds
            == Some(u64::from(crate::contract::DEFAULT_CHECK_TIMEOUT_SECONDS))));
    assert_eq!(details.conformance.checks[0].check_id, "setup:0");
    assert_eq!(details.conformance.checks[1].check_id, "ci:0");
    assert_eq!(details.conformance.checks[0].exit_code, 0);
    assert_eq!(details.conformance.checks[1].exit_code, 0);
}

#[tokio::test]
async fn a_check_that_outruns_its_limit_is_not_recorded_as_the_verdict() {
    let seed = seeded_review(vec!["sleep 5"]).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(json!({"review": {"ci_steps": ["sleep 5"], "check_timeout_seconds": 1}}).to_string())
        .bind(seed.task_id.to_string())
        .execute(seed.db.pool())
        .await
        .expect("review timeout config updates");
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("README.md"), "candidate\n")
        .await
        .unwrap();
    git::commit_all(seed.workspace.path(), "candidate")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ContractDriver::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(PassingAuditor),
    );

    let error = runner
        .run(req)
        .await
        .expect_err("timed-out check is not a verdict");

    let Unverified {
        execution_id,
        reason,
    } = error;
    assert!(crate::contract::is_check_timeout(&reason), "{reason}");
    assert!(reason.contains("`sleep 5` ran longer than 1s"), "{reason}");
    // Unrecorded, so the checks alone can be run again for the same reviewer.
    assert!(seed
        .db
        .review_conformance(&execution_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn checks_reuse_the_worktree_caches_and_leave_it_at_the_reviewed_commit() {
    let seed = seeded_review(vec!["test -f cache/warm", "touch stray.txt"]).await;
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join(".gitignore"), "cache/\n")
        .await
        .unwrap();
    tokio::fs::write(seed.workspace.path().join("README.md"), "candidate\n")
        .await
        .unwrap();
    git::commit_all(seed.workspace.path(), "candidate")
        .await
        .unwrap();
    // An ignored build cache the coder left behind, which a clean clone
    // would not have: the check only passes if it runs in the worktree.
    tokio::fs::create_dir(seed.workspace.path().join("cache"))
        .await
        .unwrap();
    tokio::fs::write(seed.workspace.path().join("cache/warm"), "warm")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ContractDriver::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(PassingAuditor),
    );

    let (review, outcome) = runner.run(req).await.unwrap();

    assert_eq!(
        outcome,
        ReviewOutcome::Passed,
        "{}",
        review.step_results_json
    );
    assert!(seed.workspace.path().join("cache/warm").exists());
    assert!(
        !seed.workspace.path().join("stray.txt").exists(),
        "untracked output of a check is removed so integration sees a clean tree"
    );
    assert!(git::is_worktree_clean(seed.workspace.path()).await.unwrap());
}

#[tokio::test]
async fn setup_that_rewrites_a_tracked_file_fails_the_candidate() {
    let seed = seeded_review(vec!["true"]).await;
    sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
        .bind(
            json!({
                "review": {
                    "setup_steps": ["echo regenerated > README.md"],
                    "ci_steps": ["true"]
                }
            })
            .to_string(),
        )
        .bind(seed.task_id.to_string())
        .execute(seed.db.pool())
        .await
        .expect("review setup config updates");
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("README.md"), "candidate\n")
        .await
        .unwrap();
    git::commit_all(seed.workspace.path(), "candidate")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ContractDriver::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(PassingAuditor),
    );

    let (review, outcome) = runner.run(req).await.unwrap();

    // A candidate that does not reproduce from its own commit is the coder's
    // defect, reported with the file, not a reviewer protocol failure.
    assert!(
        matches!(outcome, ReviewOutcome::AuditorFailed { .. }),
        "{outcome:?}"
    );
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Failed
    );
    let reason = details.conformance.reason.unwrap_or_default();
    assert!(
        reason.contains("modified tracked files: README.md"),
        "{reason}"
    );
    // The worktree goes back to the reviewed commit for the coder.
    assert_eq!(
        tokio::fs::read_to_string(seed.workspace.path().join("README.md"))
            .await
            .unwrap(),
        "candidate\n"
    );
}

// The check is explicit Project policy, never inferred/executed from Charter prose.
const RUST_PRODUCT_CHECK: &str = r#"set -eu
cargo metadata --offline --no-deps --format-version 1 > metadata.json
python3 - <<'PY'
import json
m=json.load(open('metadata.json'))
assert len(m['workspace_members']) == 1
p=m['packages'][0]
assert any('lib' in t['kind'] for t in p['targets'])
assert any('bin' in t['kind'] for t in p['targets'])
PY
mkdir -p tests
cat > tests/charter_boundary.rs <<'RS'
#[test] fn required_boundaries() {
  let rows = csvpeek::parse("value\n1\n");
  assert_eq!(rows, vec!["value", "1"]);
  assert_eq!(csvpeek::infer(&rows), "text");
  assert_eq!(csvpeek::schema(&rows), "column");
  assert_eq!(csvpeek::render(&rows), "value|1");
}
RS
cargo test --offline --test charter_boundary
test "$(cargo run --offline --quiet -- 'value,1')" = 'value|1'
"#;

async fn attach_rust_charter(seed: &SeededReview) {
    let task = TaskRepo::get_by_id(&*seed.db, &seed.task_id.to_string(), false)
        .await
        .unwrap()
        .unwrap();
    let charter: api_types::ProjectCharterContent = serde_json::from_value(json!({
        "identity":{"working_name":"CSVPeek","one_line_vision":"A tiny Rust CLI","maturity":"mvp"},
        "problem_and_people":{"problem_or_opportunity":"Inspect CSV"},
        "core_experience":{"primary_outcome":"Preview CSV"},
        "scope":{"required_deliverables":["One Rust crate with CLI, parsing, inference, schema and rendering boundaries"]},
        "success":{},"constraints_and_risks":{"technology":["Rust"]},"knowledge_ledger":{}
    })).unwrap();
    for sql in [
        "INSERT INTO user(id,email,password_hash,created_at,updated_at) VALUES ('owner','test@example.com','unused','now','now')",
        "UPDATE project SET owner_id='owner' WHERE id=?",
        "INSERT INTO project_charter(id,account_id,project_id,project_mode,maturity,created_at,updated_at) VALUES ('charter','owner',?,'compact','mvp','now','now')",
    ] {
        let mut query = sqlx::query(sql);
        if sql.contains('?') {
            query = query.bind(&task.project_id);
        }
        query.execute(seed.db.pool()).await.unwrap();
    }
    sqlx::query("INSERT INTO project_charter_revision(id,charter_id,revision,lifecycle,schema_version,render_version,content_json,rendered_view,author_type,content_digest,rendered_digest,created_at) VALUES ('charter-r1','charter',1,'approved','v1','v1',?,'','user',?,'render','now')")
        .bind(serde_json::to_string(&charter).unwrap()).bind(api_types::canonical_digest(&charter).unwrap()).execute(seed.db.pool()).await.unwrap();
    sqlx::query(
        "UPDATE project_charter SET current_approved_revision_id='charter-r1' WHERE id='charter'",
    )
    .execute(seed.db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE project SET current_charter_id='charter',current_charter_revision_id='charter-r1',charter_status='charter_backed',charter_setup_required=0 WHERE id=?")
        .bind(&task.project_id).execute(seed.db.pool()).await.unwrap();
    sqlx::query("INSERT INTO project_task_governance(task_id,project_id,charter_revision_id,runnable,created_at,updated_at) VALUES (?,?,'charter-r1',1,'now','now')")
        .bind(&task.id).bind(&task.project_id).execute(seed.db.pool()).await.unwrap();
    let config = json!({"review":{"ci_steps":[],"conformance_checks":[{"id":"rust-product","command":RUST_PRODUCT_CHECK,"requirement_ids":["charter-r1:/scope/required_deliverables/0"]}]}});
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(config.to_string())
        .bind(&task.id)
        .execute(seed.db.pool())
        .await
        .unwrap();
}

#[tokio::test]
async fn shell_governing_context_is_data_and_preserves_the_command() {
    let seed = seeded_review(vec![]).await;
    sqlx::query("UPDATE task SET title=? WHERE id=?")
        .bind("Quotes ' and $(printf injected) are Charter data")
        .bind(seed.task_id.to_string())
        .execute(seed.db.pool())
        .await
        .unwrap();
    let prompt = crate::contract::prepare_prompt(
        &seed.db,
        // The governing context is resolved through the execution being
        // prepared, so this has to be the Task's real executor attempt.
        &seed.executor_execution_id.to_string(),
        &seed.task_id.to_string(),
        seed.workspace.path(),
        false,
        true,
        "printf '%s' \"$FORGE_GOVERNING_CONTEXT\"".into(),
    )
    .await
    .unwrap();
    let output = tokio::process::Command::new("sh")
        .args(["-c", &prompt])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let actual: api_types::ReviewGoverningContext = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        actual,
        crate::contract::load_context(&seed.db, &seed.task_id.to_string(), None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn charter_check_rejects_wrong_product_and_empty_manifest_but_allows_rust_with_js_support() {
    for shape in ["go_js", "empty_rust", "rust", "rust_js_support"] {
        let seed = seeded_review(vec![]).await;
        attach_rust_charter(&seed).await;
        let path = seed.workspace.path();
        git::init(path).await.unwrap();
        tokio::fs::write(path.join("README.md"), "CSVPeek product\n")
            .await
            .unwrap();
        if shape == "go_js" || shape == "empty_rust" {
            tokio::fs::write(path.join("go.mod"), "module csvpeek\ngo 1.22\n")
                .await
                .unwrap();
            tokio::fs::write(path.join("main.go"), "package main\nfunc main() {}\n")
                .await
                .unwrap();
            tokio::fs::write(
                path.join("index.js"),
                "export const parse = s => s.split(',');\n",
            )
            .await
            .unwrap();
        }
        if shape != "go_js" {
            tokio::fs::create_dir(path.join("src")).await.unwrap();
            tokio::fs::write(
                path.join("Cargo.toml"),
                "[package]\nname='csvpeek'\nversion='0.1.0'\nedition='2021'\n",
            )
            .await
            .unwrap();
            let lib = if shape == "empty_rust" {
                "// empty shell\n"
            } else {
                "pub fn parse(s: &str) -> Vec<&str> {s.lines().collect()}\npub fn infer(_: &[&str])-> &'static str {\"text\"}\npub fn schema(_: &[&str])-> &'static str {\"column\"}\npub fn render(s: &[&str])-> String {s.join(\"|\")}\n"
            };
            tokio::fs::write(path.join("src/lib.rs"), lib)
                .await
                .unwrap();
            tokio::fs::write(path.join("src/main.rs"), "fn main(){println!(\"{}\", std::env::args().nth(1).unwrap().replace(',', \"|\"));}\n").await.unwrap();
        }
        if shape == "rust_js_support" {
            tokio::fs::write(
                path.join("package.json"),
                "{\"private\":true,\"description\":\"documentation harness\"}\n",
            )
            .await
            .unwrap();
        }
        git::commit_all(path, "candidate").await.unwrap();
        let logs = tempfile::tempdir().unwrap();
        let mut req = request(&seed);
        req.logs_path = logs.path().join("review.jsonl").display().to_string();
        req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
        let runner = ContractDriver::new(
            seed.db.clone(),
            seed.event_bus.clone(),
            Arc::new(PassingAuditor),
        );
        let (review, outcome) = runner.run(req).await.unwrap();
        let details: api_types::ReviewDetails =
            serde_json::from_str(&review.step_results_json).unwrap();
        let expected = if shape.starts_with("rust") {
            api_types::ConformanceStatus::Passed
        } else {
            api_types::ConformanceStatus::Failed
        };
        assert_eq!(
            details.conformance.status, expected,
            "{shape}: {outcome:?} {details:?}"
        );
        assert_eq!(details.conformance.checks.len(), 1);
        assert!(
            !path.join("tests/charter_boundary.rs").exists(),
            "checks must be isolated from the agent worktree"
        );
        use db::ReviewConformanceRepo;
        assert!(sqlx::query("DELETE FROM execution_review_assessment")
            .execute(seed.db.pool())
            .await
            .is_err());
        assert!(
            sqlx::query("UPDATE execution_review_contract SET source_digest='forged'")
                .execute(seed.db.pool())
                .await
                .is_err()
        );
        if expected == api_types::ConformanceStatus::Passed {
            seed.db
                .lock_review_integration(&seed.task_id.to_string())
                .await
                .unwrap()
                .release()
                .await
                .unwrap();
            sqlx::query("UPDATE task SET title='Changed acceptance',version=version+1 WHERE id=?")
                .bind(seed.task_id.to_string())
                .execute(seed.db.pool())
                .await
                .unwrap();
            assert!(
                seed.db
                    .lock_review_integration(&seed.task_id.to_string())
                    .await
                    .is_err(),
                "changed acceptance must not integrate"
            );
            assert!(
                ReviewRepo::update_status(
                    &*seed.db,
                    &review.id,
                    ReviewStatus::Passed,
                    review.step_results_json.clone(),
                    review.finished_at.clone(),
                    &now_rfc3339()
                )
                .await
                .is_err(),
                "stale acceptance cannot be republished"
            );
            assert_eq!(
                ReviewRepo::get_by_id(&*seed.db, &review.id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                ReviewStatus::Passed,
                "history stays intact"
            );
        } else {
            assert!(seed
                .db
                .lock_review_integration(&seed.task_id.to_string())
                .await
                .is_err());
        }
    }
}

#[tokio::test]
async fn review_source_excludes_evidence_recorded_after_the_reviewer_started() {
    let seed = seeded_review(Vec::new()).await;
    let task_id = seed.task_id.to_string();
    let workspace_id: String =
        sqlx::query_scalar("SELECT workspace_id FROM execution WHERE id = ?")
            .bind(seed.executor_execution_id.to_string())
            .fetch_one(seed.db.pool())
            .await
            .expect("executor workspace");
    let reviewer_execution_id = Uuid::new_v4().to_string();
    ExecutionRepo::create(
        &*seed.db,
        CreateExecution {
            id: reviewer_execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: Some(seed.auditor_agent_id.clone()),
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Running,
            stop_reason: None,
            stopped_by: None,
            resume_policy: None,
            stopped_at: None,
            parent_execution_id: None,
            agent_session_id: None,
            agent_message_id: None,
            last_activity_at: None,
            summary: None,
            logs_path: None,
            before_sha: None,
            after_sha: None,
            error: None,
            executor_config_snapshot_json: None,
            workspace_id: Some(workspace_id),
            created_at: "2026-09-25T12:00:00+00:00".to_owned(),
            updated_at: "2026-09-25T12:00:00+00:00".to_owned(),
        },
    )
    .await
    .expect("reviewer execution creates");

    for (key, execution_id, role, created_at) in [
        (
            "coder-validation",
            seed.executor_execution_id.to_string(),
            "coder",
            "2026-09-25T11:00:00+00:00",
        ),
        (
            "reviewer-report",
            reviewer_execution_id.clone(),
            "reviewer",
            "2026-09-25T12:05:00+00:00",
        ),
    ] {
        TaskCommentRepo::create_comment(
            &*seed.db,
            CreateTaskComment {
                id: Uuid::new_v4().to_string(),
                task_id: task_id.clone(),
                author_type: CommentAuthorType::Agent,
                author_id: Some(seed.auditor_agent_id.clone()),
                author_name: role.to_owned(),
                content: key.to_owned(),
                execution_id: Some(execution_id),
                role: Some(role.to_owned()),
                worklog_kind: Some("validation".to_owned()),
                idempotency_key: Some(key.to_owned()),
                created_at: created_at.to_owned(),
                updated_at: created_at.to_owned(),
            },
        )
        .await
        .expect("worklog creates");
    }
    TaskMediaRepo::create_media(
        &*seed.db,
        CreateTaskMedia {
            id: Uuid::new_v4().to_string(),
            task_id: task_id.clone(),
            display_filename: "review-run.log".to_owned(),
            content_type: "text/plain".to_owned(),
            byte_size: 12,
            storage_key: format!("task-media/{}.log", Uuid::new_v4()),
            author_type: CommentAuthorType::Agent,
            author_id: Some(seed.auditor_agent_id.clone()),
            author_name: "reviewer".to_owned(),
            created_at: "2026-09-25T12:06:00+00:00".to_owned(),
        },
    )
    .await
    .expect("reviewer evidence creates");

    let reviewed = seed
        .db
        .review_source(&task_id, Some(&reviewer_execution_id))
        .await
        .expect("review source loads");
    let evidence = &reviewed["task_scope"]["evidence"];
    assert_eq!(evidence["worklog"].as_array().map(Vec::len), Some(1));
    assert_eq!(evidence["worklog"][0]["content"], "coder-validation");
    assert_eq!(evidence["media"].as_array().map(Vec::len), Some(0));

    // Naming the candidate run instead keeps every record: the cutoff belongs
    // to a reviewing execution only.
    let candidate = seed
        .db
        .review_source(&task_id, Some(&seed.executor_execution_id.to_string()))
        .await
        .expect("candidate review source loads");
    assert_eq!(
        candidate["task_scope"]["evidence"]["worklog"]
            .as_array()
            .map(Vec::len),
        Some(2)
    );
    assert_eq!(
        candidate["task_scope"]["evidence"]["media"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
}

#[tokio::test]
async fn ci_triage_frozen_entry_failure_stays_nonblocking_during_conformance() {
    let seed = seeded_review(vec!["exit 1"]).await;
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("README.md"), "candidate\n")
        .await
        .unwrap();
    git::commit_all(seed.workspace.path(), "candidate")
        .await
        .unwrap();
    let logs = tempfile::tempdir().unwrap();
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    req.entry_triage = Some(api_types::StepCiTriage {
        mode: api_types::ReviewCiTriage::Full,
        base_status: api_types::CiBaseStatus::NotNeeded,
        base_commit: None,
        rerun_run_id: None,
        base_run_id: None,
        reason: None,
        class: Some(api_types::CiFailureClass::Flaky),
        failures: Vec::new(),
        blocking: false,
    });
    let driver = ContractDriver::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(PassingAuditor),
    );
    let (review, outcome) = driver.run(req).await.unwrap();
    assert_eq!(outcome, ReviewOutcome::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert_eq!(details.conformance.checks[0].exit_code, 1);
    assert!(
        !details.conformance.checks[0]
            .triage
            .as_ref()
            .unwrap()
            .blocking
    );
}
