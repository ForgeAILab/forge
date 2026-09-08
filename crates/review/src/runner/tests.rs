use super::*;
use crate::auditor::AuditorVerdict;
use async_trait::async_trait;
use db::{
    create_sqlite_pool, run_migrations, AgentRepo, AgentStatus, CommentAuthorType, CreateAgent,
    CreateProject, CreateRepo, CreateTask, CreateTaskComment, CreateTaskMedia, CreateWorkspace,
    DaemonRepo, DaemonStatus, ProjectRepo, RepoRepo, ReviewConformanceRepo, TaskCommentRepo,
    TaskMediaRepo, TaskRepo, UpdateProject, UpsertDaemon, WorkspaceRepo, WorkspaceStatus,
};
use serde_json::{json, Value};
use std::path::Path;
use tempfile::TempDir;

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
            remote_url: "https://example.com/forge.git".to_owned(),
            local_path: None,
            work_mode: db::WorkMode::DirectMerge,
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
            repo_id: Some(repo_id.clone()),
            parent_task_id: None,
            subtask_order: None,
            assignee_type: None,
            assignee_id: None,
            title: "Review me".to_owned(),
            description: None,
            task_type: "task".to_owned(),
            status: "in_progress".to_string(),
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

fn request(seed: &SeededReview) -> ReviewRequest {
    ReviewRequest {
        task_id: seed.task_id,
        executor_execution_id: seed.executor_execution_id,
        workspace_path: seed.workspace.path().to_path_buf(),
        ci_steps: Vec::new(),
        logs_path: seed.logs_path.clone(),
        auditor_agent_id: None,
        review_prompt: None,
        executor_thread_id: None,
        requires_user_approval: false,
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
        .review_source(&seed.task_id.to_string())
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
async fn reviewer_execution_is_owner_bound_at_creation() {
    let seed = seeded_review(Vec::new()).await;
    let runner = ReviewRunner::new(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(AdapterRegistry::new()),
    );
    let workspace = WorkspaceRepo::get_by_task_id(&*seed.db, &seed.task_id.to_string())
        .await
        .expect("workspace lookup succeeds")
        .expect("seed workspace exists");

    let (execution, owner) = runner
        .create_reviewer_execution(
            &seed.task_id.to_string(),
            &seed.executor_execution_id.to_string(),
            workspace.id,
            &request(&seed),
        )
        .await
        .expect("reviewer execution creates with lease");

    assert_eq!(execution.status, ExecutionStatus::Running);
    assert_eq!(execution.lease_owner.as_deref(), Some(owner.as_str()));
    assert!(execution.lease_expires_at.is_some());
    assert!(execution.hard_deadline_at.is_some());
}

#[test]
fn review_hard_deadline_uses_timeout_terminal_policy() {
    let policy = review_terminal_policy(&ReviewError::ExecutionHardDeadline {
        execution_id: "review-execution".to_owned(),
    });

    assert_eq!(policy.stop_reason, Some(db::StopReason::AgentTimeout));
    assert_eq!(
        policy.stopped_by.as_deref(),
        Some("system:heartbeat_monitor")
    );
    assert_eq!(policy.resume_policy, Some(db::ResumePolicy::Manual));
}

fn display_report(verdict: &str) -> String {
    json!({"contract_digest":"digest", "verdict":verdict, "requirements":[],
        "findings": if verdict == "fail" {json!([{"blocking":true,"expected":"persistence","actual":"missing","evidence":[]}])} else {json!([])}}).to_string()
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
    json!({"contract_digest":contract.digest,"verdict":"pass","requirements":contract.context.requirements.iter().map(|r|json!({
        "requirement_id":r.id,"disposition":"satisfied","rationale":"Fixture implements the required boundary", "evidence":[{"kind":"file","path":path,"commit_sha":contract.commit_sha,"start_line":1,"end_line":1}]
    })).collect::<Vec<_>>(),"findings":[]}).to_string()
}

struct MutatingAuditor;

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
        let report = json!({
            "contract_digest": contract.digest,
            "verdict": "pass",
            "requirements": contract.context.requirements.iter().map(|requirement| json!({
                "requirement_id": requirement.id,
                "disposition": "satisfied",
                "rationale": "Forge's recorded independent check passed",
                "evidence": [{"kind":"check","check_id":"ci:0"}]
            })).collect::<Vec<_>>(),
            "findings": []
        })
        .to_string();
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
    let runner = ReviewRunner::new_for_tests(
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
    let runner = ReviewRunner::new_for_tests(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(PassingAuditor),
    );

    let (review, outcome) = runner.run(req).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    let details: api_types::ReviewDetails =
        serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(
        details.conformance.status,
        api_types::ConformanceStatus::Passed
    );
    assert_eq!(details.conformance.checks.len(), 2);
    assert_eq!(details.conformance.checks[0].check_id, "setup:0");
    assert_eq!(details.conformance.checks[1].check_id, "ci:0");
    assert_eq!(details.conformance.checks[0].exit_code, 0);
    assert_eq!(details.conformance.checks[1].exit_code, 0);
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
        if sql.contains('?') { query = query.bind(&task.project_id); }
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
        "worker",
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
        crate::contract::load_context(&seed.db, &seed.task_id.to_string())
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
        sqlx::query("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES ('reviewer-role',?,'reviewer','agent',?,'now','now')")
            .bind(seed.task_id.to_string()).bind(&seed.auditor_agent_id).execute(seed.db.pool()).await.unwrap();
        let runner = ReviewRunner::new_for_tests(
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

#[async_trait]
impl TaskExecutor for MutatingAuditor {
    async fn execute(
        &self,
        ctx: ExecutionContext,
    ) -> Result<executors::ExecutionResult, executors::ExecutorError> {
        let worktree = Path::new(&ctx.worktree_path);
        tokio::fs::write(worktree.join("reviewer-created.txt"), "must be discarded\n").await?;
        let committed_sha = git::commit_all(worktree, "reviewer mutation")
            .await
            .map_err(|error| executors::ExecutorError::Other(error.to_string()))?;

        let mut writer = LogWriter::new(&ctx.logs_path, ctx.execution_id, MAX_LOG_BYTES);
        writer
            .write(
                LogKind::Assistant,
                LogStream::Main,
                json!({ "text": valid_report(&ctx.description, "baseline.txt") }),
            )
            .await?;

        Ok(executors::ExecutionResult {
            status: ExecutionOutcome::Completed,
            after_sha: Some(committed_sha),
            agent_session_id: Some("auditor-session".to_owned()),
            summary: Some("review passed".to_owned()),
            ..Default::default()
        })
    }

    async fn cancel(&self, _execution_id: &str) -> Result<(), executors::ExecutorError> {
        Ok(())
    }
}

#[tokio::test]
async fn auditor_worktree_mutations_are_discarded_before_review_completes() {
    let seed = seeded_review(Vec::new()).await;
    git::init(seed.workspace.path()).await.unwrap();
    tokio::fs::write(seed.workspace.path().join("baseline.txt"), "baseline\n")
        .await
        .unwrap();
    let original_sha = git::commit_all(seed.workspace.path(), "baseline")
        .await
        .unwrap();
    let logs = tempfile::tempdir().expect("logs tempdir creates");
    let mut req = request(&seed);
    req.logs_path = logs.path().join("review.jsonl").display().to_string();
    req.auditor_agent_id = Some(seed.auditor_agent_id.clone());
    let runner = ReviewRunner::new_for_tests(
        Arc::clone(&seed.db),
        Arc::clone(&seed.event_bus),
        Arc::new(MutatingAuditor),
    );

    let (_review, outcome) = runner.run(req).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    assert_eq!(
        git::get_current_sha(seed.workspace.path()).await.unwrap(),
        original_sha
    );
    assert!(!seed.workspace.path().join("reviewer-created.txt").exists());
    assert!(git::is_worktree_clean(seed.workspace.path()).await.unwrap());
}

async fn write_jsonl_log(path: &Path, entries: Vec<(LogKind, Value)>) {
    let mut lines = Vec::new();
    for (sequence, (kind, payload)) in entries.into_iter().enumerate() {
        let entry = LogEntry {
            schema_version: 1,
            sequence: sequence as u64,
            timestamp: "2026-04-15T00:00:00Z".to_owned(),
            execution_id: "auditor-exec".to_owned(),
            kind,
            stream: LogStream::Main,
            payload,
            truncated: false,
        };
        lines.push(serde_json::to_string(&entry).expect("entry serializes"));
    }
    tokio::fs::write(path, format!("{}\n", lines.join("\n")))
        .await
        .expect("log writes");
}

#[tokio::test]
async fn last_complete_assistant_message_is_the_report() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![
            (LogKind::Assistant, json!({ "text": "Verifying...\n" })),
            (
                LogKind::Assistant,
                json!({ "text": "All clear.\n===REVIEW: PASS===" }),
            ),
        ],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(message, "All clear.\n===REVIEW: PASS===");
}

#[tokio::test]
async fn shell_auditor_stdout_marker_parses_as_passed() {
    // A shell auditor has no assistant channel. Its verdict marker arrives as
    // ordinary stdout, and reading only assistant text made every shell
    // auditor fail as "structured review assessment missing" while its own log contained
    // the marker.
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(LogKind::Stdout, json!({ "line": display_report("pass") }))],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn shell_auditor_stdout_fail_marker_keeps_its_reason() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(LogKind::Stdout, json!({ "line": display_report("fail") }))],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        auditor::parse_verdict(&message),
        AuditorVerdict::Failed {
            reason: "Expected persistence; actual missing".to_owned()
        }
    );
}

#[tokio::test]
async fn stdout_lines_stay_separated_so_a_marker_cannot_be_glued_together() {
    // Two stdout lines must not concatenate into a marker neither one printed.
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![
            (LogKind::Stdout, json!({ "line": "===REVIEW: P" })),
            (LogKind::Stdout, json!({ "line": "ASS===" })),
        ],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        auditor::parse_verdict(&message),
        AuditorVerdict::Failed {
            reason: "structured review assessment missing".to_owned()
        }
    );
}

#[tokio::test]
async fn assistant_entry_with_pass_marker_parses_as_passed() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(
            LogKind::Assistant,
            json!({ "text": display_report("pass") }),
        )],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn claude_assistant_message_content_parses_as_passed() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(
            LogKind::Assistant,
            json!({
                "message": {
                    "content": [{
                        "type": "text",
                        "text": display_report("pass")
                    }]
                }
            }),
        )],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn claude_success_result_parses_as_passed() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![(
            LogKind::SessionInfo,
            json!({
                "subtype": "success",
                "result": display_report("pass")
            }),
        )],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(auditor::parse_verdict(&message), AuditorVerdict::Passed);
}

#[tokio::test]
async fn assistant_delta_entries_alone_do_not_count_for_verdict_text() {
    let tempdir = tempfile::tempdir().expect("tempdir creates");
    let logs_path = tempdir.path().join("auditor.jsonl");
    write_jsonl_log(
        &logs_path,
        vec![
            (LogKind::AssistantDelta, json!({ "delta": "===" })),
            (LogKind::AssistantDelta, json!({ "delta": "REVIEW: PASS" })),
            (LogKind::AssistantDelta, json!({ "delta": "===" })),
        ],
    )
    .await;

    let message = last_assistant_message(logs_path.to_str().unwrap())
        .await
        .unwrap();

    assert_eq!(
        auditor::parse_verdict(&message),
        AuditorVerdict::Failed {
            reason: "structured review assessment missing".to_owned()
        }
    );
}

#[tokio::test]
async fn codex_auditor_snapshot_carries_resume_thread_hint_for_codex_executor() {
    let now = now_rfc3339();
    let auditor_agent = Agent {
        id: "auditor-agent".to_owned(),
        name: "auditor".to_owned(),
        description: None,
        profile_id: "auditor-agent-profile".to_owned(),
        backend_kind: "cli".to_owned(),
        executor_type: "codex".to_owned(),
        provider: None,
        model: None,
        reasoning_effort: None,
        permission_policy: None,
        prompt_template: None,
        capabilities_json: "[]".to_owned(),
        tool_policy_json: "{}".to_owned(),
        config_json: "{}".to_owned(),
        credential_ref: None,
        daemon_id: Some("daemon".to_owned()),
        max_concurrent_tasks: 1,
        heartbeat_interval_seconds: 30,
        max_missed_heartbeats: 3,
        status: AgentStatus::Idle,
        last_heartbeat_at: None,
        is_default: false,
        paused: false,
        owner_id: None,
        visibility: "global".to_owned(),
        version: 1,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let executor_execution = Execution {
        id: "executor-exec".to_owned(),
        task_id: "task".to_owned(),
        agent_id: Some("executor-agent".to_owned()),
        role: "executor".to_owned(),
        status: ExecutionStatus::Completed,
        stop_reason: None,
        stopped_by: None,
        resume_policy: None,
        stopped_at: None,
        parent_execution_id: None,
        agent_session_id: Some("thread-123".to_owned()),
        agent_message_id: None,
        last_activity_at: None,
        prompt: None,
        summary: None,
        logs_path: None,
        before_sha: None,
        after_sha: None,
        error: None,
        executor_config_snapshot_json: None,
        workspace_id: Some("workspace".to_owned()),
        execution_version: 1,
        lease_owner: None,
        lease_expires_at: None,
        hard_deadline_at: None,
        last_heartbeat_at: None,
        last_progress_at: None,
        created_at: now.clone(),
        updated_at: now,
    };

    let extra_config =
        auditor_resume_thread_extra_config(&executor_execution, Some("codex"), &auditor_agent);
    let snapshot = build_auditor_config_snapshot(&auditor_agent, extra_config)
        .await
        .expect("snapshot builds");
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot parses");

    assert_eq!(
        snapshot["config"][RESUME_THREAD_ID_CONFIG_KEY],
        json!("thread-123")
    );
}

#[tokio::test]
async fn auditor_snapshot_carries_the_identity_the_embedded_runtime_requires() {
    let now = now_rfc3339();
    let auditor_agent = Agent {
        id: "auditor-agent".to_owned(),
        name: "auditor".to_owned(),
        description: None,
        profile_id: "auditor-agent-profile".to_owned(),
        backend_kind: "native".to_owned(),
        executor_type: "embedded".to_owned(),
        provider: Some("anthropic".to_owned()),
        model: Some("claude-sonnet-4".to_owned()),
        reasoning_effort: None,
        permission_policy: None,
        prompt_template: Some("You are a reviewer.".to_owned()),
        capabilities_json: "[]".to_owned(),
        tool_policy_json: "{}".to_owned(),
        config_json: "{}".to_owned(),
        credential_ref: None,
        daemon_id: None,
        max_concurrent_tasks: 1,
        heartbeat_interval_seconds: 30,
        max_missed_heartbeats: 3,
        status: AgentStatus::Idle,
        last_heartbeat_at: None,
        is_default: false,
        paused: false,
        owner_id: None,
        visibility: "global".to_owned(),
        version: 1,
        created_at: now.clone(),
        updated_at: now,
    };

    let snapshot = build_auditor_config_snapshot(&auditor_agent, None)
        .await
        .expect("snapshot builds");
    let snapshot: Value = serde_json::from_str(&snapshot).expect("snapshot parses");

    // A native backend resolves the profile — and through it the provider
    // credential — from these fields, and refuses a snapshot without them.
    assert_eq!(snapshot["agent_id"], json!("auditor-agent"));
    assert_eq!(snapshot["profile_id"], json!("auditor-agent-profile"));
    assert_eq!(snapshot["provider"], json!("anthropic"));
    assert_eq!(snapshot["model"], json!("claude-sonnet-4"));
    assert_eq!(snapshot["prompt_template"], json!("You are a reviewer."));
    // The runtime matches the claimed role against the `auditor` execution's
    // own role, and an auditor never writes to the delivered worktree.
    assert_eq!(snapshot[executors::TASK_ROLE_CONFIG_KEY], json!("reviewer"));
    assert!(executors::is_worktree_read_only(&snapshot));
}

#[tokio::test]
async fn empty_steps_auto_passes() {
    let seed = seeded_review(vec![]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (review, outcome) = runner.run(request(&seed)).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    assert_eq!(review.status, ReviewStatus::Passed);
    assert_eq!(review.step_results_json, "[]");
    assert!(review.finished_at.is_some());
}

#[tokio::test]
async fn discovery_review_does_not_run_inherited_implementation_ci_steps() {
    let seed = seeded_review(vec!["false"]).await;
    sqlx::query("UPDATE task SET task_type = 'discovery' WHERE id = ?")
        .bind(seed.task_id.to_string())
        .execute(seed.db.pool())
        .await
        .unwrap();
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (review, outcome) = runner.run(request(&seed)).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    assert_eq!(review.status, ReviewStatus::Passed);
    assert_eq!(review.step_results_json, "[]");
}

#[tokio::test]
async fn passing_rerun_waits_when_the_review_gate_requires_a_human() {
    let seed = seeded_review(vec![]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );
    let mut review_request = request(&seed);
    review_request.requires_user_approval = true;

    let (review, outcome) = runner.run(review_request).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::AwaitingHuman);
    assert_eq!(review.status, ReviewStatus::AwaitingHuman);
    assert!(review.finished_at.is_none());
    let task = TaskRepo::get_by_id(&*seed.db, &seed.task_id.to_string(), false)
        .await
        .unwrap()
        .unwrap();
    assert!(task.review_passed_at.is_none());
}

#[tokio::test]
async fn passing_step_records_pass() {
    let seed = seeded_review(vec!["true"]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (review, outcome) = runner.run(request(&seed)).await.unwrap();

    assert_eq!(outcome, ReviewOutcome::Passed);
    assert_eq!(review.status, ReviewStatus::Passed);
    let results: Vec<Value> = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["index"], 0);
    assert_eq!(results[0]["exit_code"], 0);
}

#[tokio::test]
async fn failing_step_records_fail() {
    let seed = seeded_review(vec!["true", "false", "echo never"]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (review, outcome) = runner.run(request(&seed)).await.unwrap();

    assert!(matches!(
        outcome,
        ReviewOutcome::CiFailed {
            ref failing_steps
        } if failing_steps.len() == 1 && failing_steps[0].index == 1
    ));
    assert_eq!(review.status, ReviewStatus::Failed);
    let results: Vec<Value> = serde_json::from_str(&review.step_results_json).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[1]["index"], 1);
    assert_ne!(results[1]["exit_code"], 0);
    assert!(!review.step_results_json.contains("echo never"));
}

#[tokio::test]
async fn attempt_numbers_increment() {
    let seed = seeded_review(vec![]).await;
    let runner = ReviewRunner::new(
        seed.db.clone(),
        seed.event_bus.clone(),
        Arc::new(AdapterRegistry::new()),
    );

    let (first, first_outcome) = runner.run(request(&seed)).await.unwrap();
    let (second, second_outcome) = runner.run(request(&seed)).await.unwrap();

    assert_eq!(first_outcome, ReviewOutcome::Passed);
    assert_eq!(second_outcome, ReviewOutcome::PassedCiOnly);
    assert_eq!(first.attempt_number, 1);
    assert_eq!(second.attempt_number, 2);
}
