//! Model-based workflow liveness test (refactor plan, Phase 4).
//!
//! A seeded generator drives random sequences of owner, executor, reviewer,
//! merge and crash actions through the real in-process stack: a file-backed
//! SQLite database, the real `TaskService`, Task-step worker, dispatcher,
//! review runner, merge service and crash recovery, a real temporary Git
//! repository, and a scripted executor in place of a provider CLI.
//!
//! It lives in the api test crate because that is where the fixtures it needs
//! already are: the router and the `/actions` offers endpoint (offers are
//! taken from what the API advertises, never guessed), the public
//! `TaskCondition` read model, daemon and Agent registration, and the
//! execution-setup helper.
//!
//! The run is turn based so that one seed always produces one history: no
//! background step worker or dispatcher loop runs. The model drains queued
//! Task steps and asks the dispatcher to reconcile until nothing changes
//! ("quiescence"), and every coder execution waits on a gate that only a model
//! action opens.
//!
//! The default run is deterministic: the fixed seeds and the corpus below,
//! nothing drawn from the clock, so it cannot turn an unrelated change red.
//! Random seeds are drawn only when `FORGE_MODEL_CASES=<n>` (n > 0) asks for
//! them; every seed is printed. Reproduce one case with
//! `FORGE_MODEL_SEED=<n>` (a comma list runs several). `FORGE_MODEL_LONG=1`
//! runs longer sequences (`FORGE_MODEL_STEPS` sets the length).
#![allow(dead_code)]
mod common;

use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use api::{build_router, AppState};
use api_types::{AgentResponse, DaemonRegisterResponse, DaemonResponse};
use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use db::TaskStepRepo;
use executors::{
    AvailabilityInfo, AvailabilityStatus, CodingExecutorAdapter, DiscoverContext,
    DiscoveredOptions, ExecutionContext, ExecutionOutcome, ExecutionResult, ExecutorError,
    ExecutorKind, LogKind, LogStream, LogWriter,
};
use serde_json::{json, Value};
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Seeded generator
// ---------------------------------------------------------------------------

/// splitmix64: small, seedable and good enough to pick actions.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Commit the Task's deliverable (and reconcile a handed-off conflict).
    Success,
    /// Commit the deliverable and an edit to a file every Task shares, so a
    /// second Task doing the same conflicts at merge.
    Conflict,
    /// Commit something else: the Task's required check fails in review.
    CiFail,
    /// The executor reports a failed run.
    Fail,
    /// The provider reports an exhausted quota.
    UsageLimit,
}

/// Tasks are named by their creation index, never by ID, so a sequence
/// replays on a fresh database. An index is taken modulo the number of Tasks
/// and an action that does not apply is skipped, so every subsequence of a
/// sequence is itself a valid sequence (which is what makes shrinking simple).
#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Create,
    CreateChild(usize),
    /// The first Task waits for the second.
    Depend(usize, usize),
    Claim(usize),
    Finish(usize, Outcome),
    /// Script the verdict of the Task's next agent reviews.
    Verdict(usize, bool),
    /// Apply the n-th offer the API advertises for the Task.
    Offer(usize, usize),
    Cancel(usize),
    /// Apply the offer with this verb, when the API advertises it.
    Take(usize, &'static str),
    PauseProject,
    ResumeProject,
    /// Save the Project unchanged: any edit moves its version.
    EditProject,
    /// Pause and resume the coder Agent.
    PauseAgent,
    ResumeAgent,
    /// Every retry, backoff and cooldown deadline passes.
    AdvanceClock,
    /// Restart on the same database after a quiescent state. Executions that
    /// were in flight lose their process.
    Crash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Step {
    action: Action,
    /// Crash after the action commits and before any Task step it queued
    /// runs, then restart and recover.
    crash_before_drain: bool,
}

fn step(action: Action) -> Step {
    Step {
        action,
        crash_before_drain: false,
    }
}

fn crash_after(action: Action) -> Step {
    Step {
        action,
        crash_before_drain: true,
    }
}

fn generate(seed: u64, steps: usize) -> Vec<Step> {
    let mut rng = Rng(seed);
    let mut sequence = vec![step(Action::Create)];
    let mut tasks = 1_u64;
    while sequence.len() < steps {
        // Half of the picks go to the newest Task: older ones settle.
        let task = if rng.below(2) == 0 {
            tasks - 1
        } else {
            rng.below(tasks)
        } as usize;
        let action = match rng.below(100) {
            0..=13 if tasks < 6 => {
                tasks += 1;
                Action::Create
            }
            // `CreateChild` is not generated yet: a long run with it enabled
            // reaches the open subtask findings pinned by the ignored tests
            // at the end of this file within a few dozen seeds.
            14 => Action::EditProject,
            18..=22 => Action::Depend(task, rng.below(tasks) as usize),
            23..=26 => Action::Claim(task),
            0..=57 => Action::Finish(
                task,
                match rng.below(10) {
                    0..=3 => Outcome::Success,
                    4 | 5 => Outcome::Conflict,
                    6 => Outcome::CiFail,
                    7 | 8 => Outcome::Fail,
                    _ => Outcome::UsageLimit,
                },
            ),
            58..=63 => Action::Verdict(task, rng.below(3) == 0),
            64..=81 => Action::Offer(task, rng.below(8) as usize),
            82..=84 => Action::Cancel(task),
            85 | 86 => Action::PauseProject,
            87 | 88 => Action::ResumeProject,
            89 => Action::PauseAgent,
            90 => Action::ResumeAgent,
            91..=95 => Action::AdvanceClock,
            _ => Action::Crash,
        };
        let crash_before_drain = action != Action::Crash && rng.below(10) == 0;
        sequence.push(Step {
            action,
            crash_before_drain,
        });
    }
    sequence
}

// ---------------------------------------------------------------------------
// Scripted executor
// ---------------------------------------------------------------------------

/// State the scripted executor shares with the model. One per boot: a crash
/// kills it, and its orphaned futures never return and never write.
struct Script {
    alive: AtomicBool,
    /// Task ID -> the coder execution waiting for a model action.
    waiting: Mutex<HashMap<String, (String, tokio::sync::oneshot::Sender<Outcome>)>>,
    shared: Arc<Shared>,
}

/// Survives restarts: it is the model's memory, not the server's.
#[derive(Default)]
struct Shared {
    verdicts: Mutex<HashMap<String, bool>>,
    index: Mutex<HashMap<String, usize>>,
    /// Subtask index -> its coordination root: a subtask's work is also the
    /// root's deliverable, which the root's aggregate review checks.
    parents: Mutex<HashMap<usize, usize>>,
    writes: AtomicU64,
}

struct ScriptedAdapter(Arc<Script>);

fn git(path: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .args([
            "-c",
            "user.email=model@forge.dev",
            "-c",
            "user.name=Forge Model",
        ])
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .map_err(|error| format!("git {args:?}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn deliverable(index: usize) -> String {
    format!("ok-{index}.txt")
}

impl ScriptedAdapter {
    fn commit(&self, ctx: &ExecutionContext, outcome: Outcome) -> Result<(), ExecutorError> {
        let worktree = Path::new(&ctx.worktree_path);
        let index = self
            .0
            .shared
            .index
            .lock()
            .unwrap()
            .get(&ctx.task_id)
            .copied()
            .unwrap_or(usize::MAX);
        let n = self.0.shared.writes.fetch_add(1, Ordering::SeqCst);
        let shared = worktree.join("shared.txt");
        let handed_off =
            std::fs::read_to_string(&shared).is_ok_and(|text| text.contains("<<<<<<<"));
        match outcome {
            Outcome::CiFail => {
                std::fs::write(worktree.join(format!("junk-{index}.txt")), format!("{n}\n"))?
            }
            _ => {
                std::fs::write(worktree.join(deliverable(index)), format!("{n}\n"))?;
                if let Some(root) = self.0.shared.parents.lock().unwrap().get(&index) {
                    std::fs::write(worktree.join(deliverable(*root)), format!("{n}\n"))?;
                }
            }
        }
        if outcome == Outcome::Conflict || handed_off {
            std::fs::write(&shared, format!("task {index} write {n}\n"))?;
        }
        git(worktree, &["add", "-A"]).map_err(ExecutorError::Other)?;
        git(
            worktree,
            &["commit", "-m", &format!("model task {index} write {n}")],
        )
        .map_err(ExecutorError::Other)?;
        Ok(())
    }

    async fn review(&self, ctx: &ExecutionContext) -> Result<(), ExecutorError> {
        let pass = self
            .0
            .shared
            .verdicts
            .lock()
            .unwrap()
            .get(&ctx.task_id)
            .copied()
            .unwrap_or(true);
        let text = if pass {
            common::passing_review_assessment(&ctx.description, Path::new(&ctx.worktree_path))
        } else {
            common::failing_review_assessment(&ctx.description, "the model scripted a rejection")
        };
        if let Some(parent) = Path::new(&ctx.logs_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        LogWriter::new(&ctx.logs_path, ctx.execution_id.clone(), 1024 * 1024)
            .write(LogKind::Assistant, LogStream::Main, json!({ "text": text }))
            .await?;
        Ok(())
    }
}

fn result(status: ExecutionOutcome, error: Option<&str>) -> ExecutionResult {
    ExecutionResult {
        status,
        agent_session_id: Some("model-session".to_owned()),
        summary: Some("scripted executor".to_owned()),
        error: error.map(str::to_owned),
        ..Default::default()
    }
}

#[async_trait::async_trait]
impl CodingExecutorAdapter for ScriptedAdapter {
    fn kind(&self) -> ExecutorKind {
        ExecutorKind::Codex
    }

    fn check_availability(&self) -> AvailabilityInfo {
        AvailabilityInfo {
            status: AvailabilityStatus::Authenticated,
            authenticated_at: None,
            config_path: None,
        }
    }

    async fn discover_options(
        &self,
        _ctx: DiscoverContext,
    ) -> Result<DiscoveredOptions, ExecutorError> {
        Ok(DiscoveredOptions::default())
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<ExecutionResult, ExecutorError> {
        if !self.0.alive.load(Ordering::SeqCst) {
            return std::future::pending().await;
        }
        if common::is_conformance_review_prompt(&ctx.description) {
            self.review(&ctx).await?;
            return Ok(result(ExecutionOutcome::Completed, None));
        }
        let (gate, opened) = tokio::sync::oneshot::channel();
        self.0
            .waiting
            .lock()
            .unwrap()
            .insert(ctx.task_id.clone(), (ctx.execution_id.clone(), gate));
        let outcome = opened.await;
        if !self.0.alive.load(Ordering::SeqCst) {
            // The process died with the server: nothing is reported.
            return std::future::pending().await;
        }
        match outcome {
            Err(_) => Ok(result(ExecutionOutcome::Cancelled, Some("cancelled"))),
            Ok(Outcome::Fail) => Ok(result(ExecutionOutcome::Failed, Some("scripted failure"))),
            Ok(Outcome::UsageLimit) => Err(ExecutorError::UsageExhausted {
                // The provider cooldown is wall-clock and in memory. The
                // model has no clock to advance, so the quota comes back at
                // once and the Task must then run again by itself.
                retry_after: Some(Duration::from_millis(1)),
                usage_reports: Vec::new(),
            }),
            Ok(outcome) => {
                self.commit(&ctx, outcome)?;
                Ok(result(ExecutionOutcome::Completed, None))
            }
        }
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), ExecutorError> {
        self.0
            .waiting
            .lock()
            .unwrap()
            .retain(|_, (waiting, _)| waiting != execution_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The real stack
// ---------------------------------------------------------------------------

struct Live {
    pool: sqlx::SqlitePool,
    runtime: Arc<services::ForgeRuntime>,
    state: AppState,
    app: Router,
    script: Arc<Script>,
}

struct World {
    dir: tempfile::TempDir,
    live: Option<Live>,
    shared: Arc<Shared>,
    project_id: String,
    coder_id: String,
    reviewer_id: String,
    project_paused: bool,
    agent_paused: bool,
    /// Random runs step around the open findings pinned by the ignored
    /// tests at the end of this file, so they keep finding other things:
    /// both former guards (cancelled dependencies, Project pause) are
    /// fixed and removed; the flag stays for the next one.
    avoid_open_findings: bool,
    tasks: Vec<String>,
    /// Model index -> the terminal state the Task was first seen in.
    settled: BTreeMap<usize, String>,
    trace: Vec<String>,
}

const QUIESCENCE_BOUND: usize = 400;
const QUIESCENCE_FLOOR: Duration = Duration::from_millis(60);
const SETTLE_ROUNDS: usize = 20;
const INITIAL: [&str; 2] = ["backlog", "todo"];
const TERMINAL: [&str; 2] = ["done", "cancelled"];

impl World {
    async fn boot(dir: &Path, shared: &Arc<Shared>) -> Live {
        let url = format!("sqlite:{}?mode=rwc", dir.join("forge.db").display());
        let pool = db::create_sqlite_pool(&url).await.expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        // A fresh `SqliteDb` per boot: its in-memory schedule generation and
        // step-executor reference die with the process, as in a real crash.
        let db = Arc::new(db::SqliteDb::new(pool.clone()));
        let script = Arc::new(Script {
            alive: AtomicBool::new(true),
            waiting: Mutex::default(),
            shared: Arc::clone(shared),
        });
        let mut registry = executors::AdapterRegistry::new();
        registry.register(Box::new(ScriptedAdapter(Arc::clone(&script))));
        let registry = Arc::new(registry);
        services::ensure_default_agents(db.as_ref(), &registry)
            .await
            .expect("default agents upsert");
        let event_bus = Arc::new(events::EventBus::new(1024));
        let workspaces = dir.join("workspaces");
        let mut config = config::ForgeConfig::with_data_dir(dir.join("data"));
        config.workspace.root = workspaces.clone();
        let runtime = Arc::new(
            services::ForgeRuntimeBuilder::from_config(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                config,
            )
            .with_adapter_registry(Arc::clone(&registry))
            .with_cleanup_scheduler(Arc::new(services::WorkspaceCleanupScheduler::new(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                workspaces.clone(),
            )))
            .with_merge_service(Arc::new(services::MergeService::new_for_test(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                workspaces,
            )))
            .with_review_runner(Arc::new(review::ReviewRunner::new(
                Arc::clone(&db),
                Arc::clone(&event_bus),
                Arc::clone(&registry),
            )))
            .with_workflows_dir(api::state::test_workflows_dir())
            .with_jwt_secret(api::state::test_jwt_secret())
            .with_bcrypt_cost(api::state::test_bcrypt_cost())
            .build(),
        );
        let state = AppState::from_runtime_arc(Arc::clone(&runtime), true);
        let app = build_router(state.clone(), dir.join("web"));
        Live {
            pool,
            runtime,
            state,
            app,
            script,
        }
    }

    async fn new() -> Self {
        let dir = tempfile::tempdir().expect("model directory creates");
        std::fs::create_dir_all(dir.path().join("web")).unwrap();
        std::fs::write(dir.path().join("web/index.html"), "<html></html>").unwrap();
        std::fs::create_dir_all(dir.path().join("workspaces")).unwrap();
        let repo = common::setup_git_repo(dir.path());
        std::fs::write(repo.join("shared.txt"), "base\n").unwrap();
        git(&repo, &["add", "-A"]).unwrap();
        git(&repo, &["commit", "-m", "shared file"]).unwrap();

        let shared = Arc::new(Shared::default());
        let live = Self::boot(dir.path(), &shared).await;
        sqlx::query(
            "INSERT INTO user(id,email,password_hash,display_name,is_admin,created_at,updated_at)
             VALUES ('test-user-id','test@example.com','$2b$04$placeholder',NULL,0,?,?)",
        )
        .bind(db::now_rfc3339())
        .bind(db::now_rfc3339())
        .execute(&live.pool)
        .await
        .expect("seed test user");
        let (project_id, repo_id) =
            common::create_project_and_repo(&live.app, "Model", &repo).await;
        let (coder_id, reviewer_id) =
            register_agents(&live.app, &dir.path().join("workspaces")).await;
        common::configure_execution_test_setup(
            &live.state.db,
            &project_id,
            &repo_id,
            &coder_id,
            &reviewer_id,
        )
        .await;
        // No planner: an unassigned planning gate is skipped, so the model
        // exercises the coder, review and merge loop. A planner needs a
        // scripted plan artifact, which this model does not write yet.
        let settings: String = sqlx::query_scalar("SELECT settings FROM project WHERE id = ?")
            .bind(&project_id)
            .fetch_one(&live.pool)
            .await
            .expect("project settings load");
        let mut settings: Value = serde_json::from_str(&settings).expect("settings parse");
        settings["default_role_assignments"]
            .as_array_mut()
            .expect("default roles are configured")
            .retain(|role| role["role_name"] != "planner");
        sqlx::query("UPDATE project SET settings = ? WHERE id = ?")
            .bind(settings.to_string())
            .bind(&project_id)
            .execute(&live.pool)
            .await
            .expect("project settings update");
        Self {
            dir,
            live: Some(live),
            shared,
            project_id,
            coder_id,
            reviewer_id,
            project_paused: false,
            agent_paused: false,
            avoid_open_findings: false,
            tasks: Vec::new(),
            settled: BTreeMap::new(),
            trace: Vec::new(),
        }
    }

    /// On a violation: every transition of every Task that is not settled.
    async fn record_history(&mut self) {
        let Some(live) = self.live.as_ref() else {
            return;
        };
        let mut lines = Vec::new();
        for (index, id) in self.tasks.iter().enumerate() {
            let rows: Vec<(String, String, String, String)> = sqlx::query_as(
                "SELECT from_state, to_state, triggered_by, trigger_reason FROM transition_log
                 WHERE task_id = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(&live.pool)
            .await
            .unwrap_or_default();
            if rows
                .last()
                .is_some_and(|row| TERMINAL.contains(&row.1.as_str()))
            {
                continue;
            }
            for (from, to, by, reason) in rows {
                let reason: String = reason.chars().take(160).collect();
                lines.push(format!(
                    "  history {index}: {from} -> {to} by {by}: {reason}"
                ));
            }
        }
        self.trace.extend(lines);
    }

    fn live(&self) -> &Live {
        self.live.as_ref().expect("the server is up")
    }

    /// Kill the server without a graceful shutdown and start a new one on
    /// the same database, the way `RuntimeSupervisor::start` does: crash
    /// recovery, then the dispatcher's startup reconciliation.
    async fn crash_and_restart(&mut self) -> Result<(), String> {
        let old = self.live.take().expect("the server is up");
        old.script.alive.store(false, Ordering::SeqCst);
        old.script.waiting.lock().unwrap().clear();
        old.runtime.task_dispatcher.stop();
        // Whatever the dead process still had in flight can no longer write.
        let _ = tokio::time::timeout(Duration::from_secs(5), old.pool.close()).await;
        drop(old);

        let live = Self::boot(self.dir.path(), &self.shared).await;
        live.runtime
            .crash_recovery
            .run()
            .await
            .map_err(|error| format!("crash recovery failed: {error}"))?;
        live.runtime
            .task_dispatcher
            .startup_reconcile()
            .await
            .map_err(|error| format!("startup reconciliation failed: {error}"))?;
        self.live = Some(live);
        Ok(())
    }

    async fn request(
        &self,
        method: Method,
        uri: &str,
        body: Option<Value>,
    ) -> Result<(StatusCode, Value), String> {
        let builder = Request::builder().method(method.clone()).uri(uri).header(
            header::AUTHORIZATION,
            format!("Bearer {}", common::test_jwt()),
        );
        let request = match body {
            Some(body) => builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string())),
            None => builder.body(Body::empty()),
        }
        .expect("request builds");
        let response = self
            .live()
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("router responds");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body reads");
        let value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        if status.is_server_error() {
            return Err(format!("{method} {uri} answered {status}: {value}"));
        }
        Ok((status, value))
    }

    async fn task(&self, index: usize) -> Result<Value, String> {
        let (status, task) = self
            .request(
                Method::GET,
                &format!("/api/v1/tasks/{}", self.tasks[index]),
                None,
            )
            .await?;
        if status != StatusCode::OK {
            return Err(format!("task {index} is unreadable: {status} {task}"));
        }
        Ok(task)
    }

    async fn offers(&self, index: usize) -> Result<(i64, Vec<Value>), String> {
        let (status, offers) = self
            .request(
                Method::GET,
                &format!("/api/v1/tasks/{}/actions", self.tasks[index]),
                None,
            )
            .await?;
        if status != StatusCode::OK {
            return Err(format!(
                "offers of task {index} are unreadable: {status} {offers}"
            ));
        }
        Ok((
            offers["version"].as_i64().unwrap_or_default(),
            offers["available_actions"]
                .as_array()
                .cloned()
                .unwrap_or_default(),
        ))
    }

    /// Invariant (b): an advertised offer is accepted when applied.
    async fn apply_offer(
        &self,
        index: usize,
        version: i64,
        offer: &Value,
    ) -> Result<String, String> {
        let mut action = offer["action"].clone();
        let verb = action["verb"].as_str().unwrap_or_default().to_owned();
        for parameter in offer["parameters"].as_array().into_iter().flatten() {
            let name = parameter["name"].as_str().unwrap_or_default();
            if parameter["required"] == true && action.get(name).is_none_or(Value::is_null) {
                action[name] = match parameter["boolean_values"].as_array() {
                    Some(values) => values.first().cloned().unwrap_or(Value::Bool(false)),
                    None => json!("the model says so"),
                };
            }
        }
        if verb == "send_back" && action["guidance"].as_str().is_none_or(str::is_empty) {
            action["guidance"] = json!("the model sends it back");
        }
        let (status, body) = self
            .request(
                Method::POST,
                &format!("/api/v1/tasks/{}/actions", self.tasks[index]),
                Some(json!({ "version": version, "action": action })),
            )
            .await?;
        if !status.is_success() {
            return Err(format!(
                "(b) offered action `{verb}` on task {index} was refused: {status} {body}; offer {offer}"
            ));
        }
        Ok(verb)
    }

    async fn create(&mut self, parent: Option<usize>) -> Result<String, String> {
        let index = self.tasks.len();
        let mut body = json!({
            "title": format!("model task {index}"),
            "description": "make the model deliverable",
            "role_assignments": [{
                "role_name": "reviewer", "assignee_type": "agent", "assignee_id": self.reviewer_id
            }],
            "review_config": {
                "ci_steps": [format!("test -f {}", deliverable(index))],
                "review_prompt": "Review the model deliverable."
            }
        });
        if let Some(parent) = parent {
            body["parent_task_id"] = json!(self.tasks[parent]);
            // A subtask's workflow has no reviewer role of its own.
            body.as_object_mut().unwrap().remove("role_assignments");
        }
        let (status, task) = self
            .request(
                Method::POST,
                &format!("/api/v1/projects/{}/tasks", self.project_id),
                Some(body),
            )
            .await?;
        if !status.is_success() {
            // Refusing a child of a Task that already moved on is legitimate.
            return Ok(format!("refused {status} {}", task["message"]));
        }
        let id = task["id"]
            .as_str()
            .expect("created Task has an ID")
            .to_owned();
        let pool = &self.live().pool;
        let stored: Option<String> =
            sqlx::query_scalar("SELECT task_state_config FROM task WHERE id = ?")
                .bind(&id)
                .fetch_one(pool)
                .await
                .map_err(|error| error.to_string())?;
        let mut state_config = stored
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
            .filter(Value::is_object)
            .unwrap_or_else(|| json!({}));
        if !state_config["review"].is_object() {
            state_config["review"] = json!({ "review_prompt": "Review the model deliverable." });
        }
        state_config["review"]["auditor_agent_id"] = json!(self.reviewer_id);
        sqlx::query("UPDATE task SET task_state_config = ? WHERE id = ?")
            .bind(state_config.to_string())
            .bind(&id)
            .execute(pool)
            .await
            .map_err(|error| error.to_string())?;
        self.shared.index.lock().unwrap().insert(id.clone(), index);
        if let Some(parent) = parent {
            self.shared.parents.lock().unwrap().insert(index, parent);
        }
        self.tasks.push(id);
        Ok("created".to_owned())
    }

    /// The model owns the clock. Backoff deadlines are wall-clock times in
    /// Task metadata; left alone, whether one has passed by the next step
    /// depends on how fast the machine is. So a deadline more than a second
    /// away is frozen (moved out of reach) as soon as it appears, and only
    /// `AdvanceClock` lets the frozen ones pass. Writes go through the Task
    /// repository, so the stored condition follows.
    async fn move_deadlines(&self, advance: bool) -> Result<usize, String> {
        const FROZEN: &str = "2099-01-01T00:00:00+00:00";
        let soon = (chrono::Utc::now() + chrono::Duration::seconds(1)).to_rfc3339();
        let mut moved = 0;
        for id in &self.tasks {
            let metadata: Option<String> =
                sqlx::query_scalar("SELECT metadata_json FROM task WHERE id = ?")
                    .bind(id)
                    .fetch_one(&self.live().pool)
                    .await
                    .map_err(|error| error.to_string())?;
            let Some(mut deferral) = metadata
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .map(|metadata| metadata["deferred_dispatch"].clone())
                .filter(Value::is_object)
            else {
                continue;
            };
            let deadline = deferral["not_before"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            let frozen = deadline.as_str() >= "2090";
            deferral["not_before"] = match (advance, frozen) {
                (true, true) => json!("1970-01-01T00:00:00+00:00"),
                (false, false) if deadline > soon => json!(FROZEN),
                _ => continue,
            };
            db::TaskRepo::mutate_metadata(
                &*self.live().state.db,
                id,
                None,
                vec![db::TaskMetadataMutation::Set {
                    key: "deferred_dispatch".to_owned(),
                    value: deferral,
                }],
                &db::now_rfc3339(),
            )
            .await
            .map_err(|error| format!("moving a deadline failed: {error}"))?;
            moved += 1;
        }
        Ok(moved)
    }

    /// The first Task at or after `from` (wrapping) for which `wanted` holds.
    fn first_from(&self, from: usize, wanted: impl Fn(usize) -> bool) -> Option<usize> {
        let count = self.tasks.len();
        (0..count)
            .map(|offset| (from + offset) % count)
            .find(|index| wanted(*index))
    }

    fn waiting(&self, index: usize) -> bool {
        self.live()
            .script
            .waiting
            .lock()
            .unwrap()
            .contains_key(&self.tasks[index])
    }

    async fn apply(&mut self, action: &Action) -> Result<String, String> {
        let count = self.tasks.len();
        let pick = |index: usize| (count > 0).then(|| index % count);
        match action {
            Action::Create => self.create(None).await,
            Action::CreateChild(parent) => match pick(*parent) {
                Some(parent) => self.create(Some(parent)).await,
                None => Ok("skipped".to_owned()),
            },
            Action::Depend(task, on) => {
                let (Some(task), Some(on)) = (pick(*task), pick(*on)) else {
                    return Ok("skipped".to_owned());
                };
                if task == on {
                    return Ok("skipped".to_owned());
                }
                let (status, _) = self
                    .request(
                        Method::POST,
                        &format!("/api/v1/tasks/{}/dependencies", self.tasks[task]),
                        Some(json!({ "depends_on_id": self.tasks[on] })),
                    )
                    .await?;
                Ok(format!("{status}"))
            }
            Action::Claim(task) => {
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                let (status, body) = self
                    .request(
                        Method::POST,
                        &format!("/api/v1/tasks/{}/claim", self.tasks[task]),
                        Some(json!({ "agent_id": self.coder_id, "overrides": null })),
                    )
                    .await?;
                Ok(if status.is_success() {
                    "claimed".to_owned()
                } else {
                    format!("refused {status} {}", body["error"]["code"])
                })
            }
            Action::Finish(task, outcome) => {
                // Finish the named Task's run, or the next one that has one.
                let Some(task) =
                    pick(*task).and_then(|task| self.first_from(task, |t| self.waiting(t)))
                else {
                    return Ok("skipped".to_owned());
                };
                let gate = self
                    .live()
                    .script
                    .waiting
                    .lock()
                    .unwrap()
                    .remove(&self.tasks[task]);
                match gate {
                    Some((_, gate)) => {
                        let _ = gate.send(*outcome);
                        Ok(format!("finished {task}"))
                    }
                    None => Ok("skipped".to_owned()),
                }
            }
            Action::Verdict(task, pass) => {
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                self.shared
                    .verdicts
                    .lock()
                    .unwrap()
                    .insert(self.tasks[task].clone(), *pass);
                Ok("scripted".to_owned())
            }
            Action::Offer(task, n) => {
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                let (version, offers) = self.offers(task).await?;
                if offers.is_empty() {
                    return Ok("no offers".to_owned());
                }
                self.apply_offer(task, version, &offers[n % offers.len()])
                    .await
            }
            Action::Cancel(task) | Action::Take(task, _) => {
                let verb = match action {
                    Action::Take(_, verb) => verb,
                    _ => "cancel",
                };
                let Some(task) = pick(*task) else {
                    return Ok("skipped".to_owned());
                };
                let (version, offers) = self.offers(task).await?;
                match offers.iter().find(|offer| offer["action"]["verb"] == verb) {
                    Some(offer) => self.apply_offer(task, version, offer).await,
                    None => Ok(format!("{verb} not offered")),
                }
            }
            Action::PauseProject | Action::ResumeProject => {
                let pause = *action == Action::PauseProject;
                let verb = if pause { "pause" } else { "resume" };
                let uri = format!("/api/v1/projects/{}/{verb}", self.project_id);
                let (status, body) = self.request(Method::POST, &uri, None).await?;
                if !status.is_success() {
                    return Err(format!("(b) project {verb} was refused: {status} {body}"));
                }
                self.project_paused = pause;
                Ok(verb.to_owned())
            }
            Action::EditProject => {
                let uri = format!("/api/v1/projects/{}", self.project_id);
                let (_, project) = self.request(Method::GET, &uri, None).await?;
                let version = project["version"].as_i64().unwrap_or_default();
                let body = json!({ "version": version, "name": project["name"] });
                let (status, saved) = self.request(Method::PATCH, &uri, Some(body)).await?;
                if !status.is_success() {
                    return Err(format!(
                        "(b) saving the Project was refused: {status} {saved}"
                    ));
                }
                if saved["version"].as_i64().unwrap_or_default() <= version {
                    return Err(format!(
                        "the model expects a Project edit to move its version: {version} -> {}",
                        saved["version"]
                    ));
                }
                Ok("saved".to_owned())
            }
            Action::PauseAgent | Action::ResumeAgent => {
                let pause = *action == Action::PauseAgent;
                let verb = if pause { "pause" } else { "resume" };
                let uri = format!("/api/v1/agents/{}/{verb}", self.coder_id);
                let (status, body) = self.request(Method::POST, &uri, None).await?;
                if !status.is_success() {
                    return Err(format!("(b) agent {verb} was refused: {status} {body}"));
                }
                self.agent_paused = pause;
                Ok(verb.to_owned())
            }
            Action::AdvanceClock => Ok(format!(
                "{} deadline(s) passed",
                self.move_deadlines(true).await?
            )),
            Action::Crash => {
                self.crash_and_restart().await?;
                Ok("restarted".to_owned())
            }
        }
    }

    async fn executions(&self, index: usize) -> Result<Vec<(String, String, String)>, String> {
        let (_, page) = self
            .request(
                Method::GET,
                &format!("/api/v1/tasks/{}/executions?limit=100", self.tasks[index]),
                None,
            )
            .await?;
        Ok(page["items"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|execution| {
                let text = |key: &str| execution[key].as_str().unwrap_or_default().to_owned();
                (text("id"), text("role"), text("status"))
            })
            .collect())
    }

    /// Everything that can change while the system still has work to do, and
    /// whether an execution is running that no model action can finish.
    async fn fingerprint(&self) -> Result<(String, Vec<String>), String> {
        let mut print = String::new();
        let mut unowned = Vec::new();
        for (index, id) in self.tasks.iter().enumerate() {
            let (status, version): (String, i64) =
                sqlx::query_as("SELECT status, version FROM task WHERE id = ?")
                    .bind(id)
                    .fetch_one(&self.live().pool)
                    .await
                    .map_err(|error| error.to_string())?;
            let pending = self
                .live()
                .state
                .db
                .pending_steps(id)
                .await
                .map_err(|error| error.to_string())?;
            print.push_str(&format!("{index}:{status}@{version}+{pending}["));
            for (execution, role, status) in self.executions(index).await? {
                print.push_str(&format!("{role}={status},"));
                let gated = self
                    .live()
                    .script
                    .waiting
                    .lock()
                    .unwrap()
                    .get(id)
                    .is_some_and(|(waiting, _)| *waiting == execution);
                if matches!(status.as_str(), "running" | "pending" | "queued") && !gated {
                    unowned.push(format!("task {index} {role} execution is {status}"));
                }
            }
            print.push_str("] ");
        }
        Ok((print, unowned))
    }

    /// Invariant (d): run the step worker and the dispatcher until nothing
    /// changes. Not settling within the bound is a livelock.
    async fn quiesce(&self) -> Result<(), String> {
        let mut last = String::new();
        let mut stable = 0;
        let mut unowned = Vec::new();
        let mut history = Vec::new();
        let mut changed = std::time::Instant::now();
        for round in 0..QUIESCENCE_BOUND {
            self.move_deadlines(false).await?;
            for (index, id) in self.tasks.iter().enumerate() {
                self.live()
                    .state
                    .task_service
                    .drain(id)
                    .await
                    .map_err(|error| {
                        format!("draining the steps of task {index} failed: {error}")
                    })?;
            }
            self.live()
                .runtime
                .task_dispatcher
                .check_once()
                .await
                .map_err(|error| format!("dispatcher pass failed: {error}"))?;
            tokio::time::sleep(Duration::from_millis(if round < 20 { 5 } else { 25 })).await;
            let frozen = self.move_deadlines(false).await?;
            let (mut print, running) = self.fingerprint().await?;
            if frozen > 0 {
                print.push_str(&format!("froze {frozen} at round {round}"));
            }
            unowned = running;
            if print == last {
                stable += 1;
                // Work a finished run leaves to its spawned task (recording
                // the failure, scheduling the retry) has no row to watch
                // while it is pending, so "unchanged" also has to last: three
                // rounds pass in microseconds of scheduler time on a loaded
                // runner.
                if stable >= 3 && unowned.is_empty() && changed.elapsed() >= QUIESCENCE_FLOOR {
                    return Ok(());
                }
            } else {
                stable = 0;
                changed = std::time::Instant::now();
                history.push(print.clone());
                last = print;
            }
        }
        if stable >= 3 {
            return Err(format!(
                "(a) an execution stays in flight with no live owner and nothing reclaims it: {unowned:?}; state {last}"
            ));
        }
        let tail = history.len().saturating_sub(6);
        Err(format!(
            "(d) livelock: no quiescence after {QUIESCENCE_BOUND} rounds; last states {:#?}",
            &history[tail..]
        ))
    }

    /// The invariants that hold in every committed state.
    async fn check_committed(&mut self) -> Result<(), String> {
        // (e) the condition invariant the code already asserts.
        let violations = self
            .live()
            .state
            .db
            .task_condition_violations()
            .await
            .map_err(|error| error.to_string())?;
        if !violations.is_empty() {
            let indexes: Vec<_> = violations
                .iter()
                .map(|id| self.tasks.iter().position(|task| task == id))
                .collect();
            return Err(format!(
                "(e) stored condition disagrees with durable state for tasks {indexes:?}"
            ));
        }
        // (f) terminal Tasks stay terminal.
        for index in 0..self.tasks.len() {
            let status: String = sqlx::query_scalar("SELECT status FROM task WHERE id = ?")
                .bind(&self.tasks[index])
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
            match self.settled.get(&index) {
                Some(settled) if *settled != status => {
                    return Err(format!(
                        "(f) task {index} left terminal state {settled} for {status}"
                    ));
                }
                None if TERMINAL.contains(&status.as_str()) => {
                    self.settled.insert(index, status);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// The invariants of a quiescent state. Returns one line per Task.
    async fn check_quiescent(&mut self) -> Result<String, String> {
        self.check_committed().await?;
        // (a) the dispatcher's own "owned or parked" assertion.
        self.live()
            .runtime
            .task_dispatcher
            .sweep_and_assert()
            .await
            .map_err(|error| format!("(a) {error}"))?;
        let mut line = String::new();
        for index in 0..self.tasks.len() {
            let task = self.task(index).await?;
            let status = task["status"].as_str().unwrap_or_default().to_owned();
            let kind = task["condition"]["kind"].as_str().unwrap_or("?").to_owned();
            let reason = ["primary", "failure", "reason"]
                .iter()
                .find_map(|key| task["condition"][key]["kind"].as_str())
                .unwrap_or("-")
                .to_owned();
            let details = &task["condition"]["details"];
            let cause = details["diagnostic"]["blocking_reason"]
                .as_str()
                .or(details["failure_kind"].as_str())
                .map(|cause| format!("({cause})"))
                .unwrap_or_default();
            line.push_str(&format!("{index}:{status}/{kind}/{reason}{cause} "));
            if TERMINAL.contains(&status.as_str()) {
                if kind != "settled" {
                    return Err(format!(
                        "(e) terminal task {index} ({status}) has condition {}",
                        task["condition"]
                    ));
                }
                continue;
            }
            let (_, offers) = self.offers(index).await?;
            let verbs: Vec<&str> = offers
                .iter()
                .filter_map(|offer| offer["action"]["verb"].as_str())
                .collect();
            self.check_exit(index, &task, &status, &kind, &reason, &verbs)
                .await?;
        }
        Ok(line)
    }

    /// Invariants (a) and (c): a Task that is not terminal has a live owner,
    /// or waits for something that exists and can clear the wait.
    async fn check_exit(
        &self,
        index: usize,
        task: &Value,
        status: &str,
        kind: &str,
        reason: &str,
        verbs: &[&str],
    ) -> Result<(), String> {
        let offered = |wanted: &[&str]| wanted.iter().any(|verb| verbs.contains(verb));
        let wedge = |why: &str| {
            Err(format!(
                "(c) task {index} in `{status}` is {kind}/{reason} and {why}; offers {verbs:?}; condition {}",
                task["condition"]
            ))
        };
        if self.waiting(index) {
            return Ok(()); // a live coder run: the model can finish it.
        }
        let unsettled = |ids: Vec<String>| async move {
            let mut open = false;
            for id in ids {
                let status: Option<String> =
                    sqlx::query_scalar("SELECT status FROM task WHERE id = ?")
                        .bind(id)
                        .fetch_optional(&self.live().pool)
                        .await
                        .map_err(|error| error.to_string())?;
                open |= status.is_some_and(|status| !TERMINAL.contains(&status.as_str()));
            }
            Ok::<bool, String>(open)
        };
        if task["condition"]["details"]["diagnostic"]["blocking_reason"] == "dependency_cancelled" {
            // By design this park offers cancellation only; its other exit
            // is removing the cancelled dependency, which the settlement
            // drive takes and which must then let the Task continue.
            return if offered(&["cancel"]) {
                Ok(())
            } else {
                wedge("`cancel` is not offered for a cancelled dependency")
            };
        }
        match (kind, reason) {
            ("clear" | "entering" | "running" | "deferred", _) => {
                // Quiescence: nothing is queued for this Task and it has no
                // live execution. It may only be waiting for something that
                // exists: capacity another run holds, an open child, a pause
                // the owner can lift, or (a backoff) the retry it offers.
                let children: Vec<String> =
                    sqlx::query_scalar("SELECT id FROM task WHERE parent_task_id = ?")
                        .bind(&self.tasks[index])
                        .fetch_all(&self.live().pool)
                        .await
                        .map_err(|error| error.to_string())?;
                // Another Task's run excuses only a Task still waiting for
                // its first slot. A started Task with no run of its own must
                // show why (a deferral, a park), whatever the others do.
                let capacity_held = INITIAL.contains(&status)
                    && (0..self.tasks.len()).any(|other| self.waiting(other));
                if capacity_held
                    || self.project_paused
                    || self.agent_paused
                    || unsettled(children).await?
                    || kind == "deferred"
                // a deadline `AdvanceClock` lets pass.
                {
                    return Ok(());
                }
                wedge("has no queued step, no live execution, no park and nothing it could be waiting for")
            }
            (_, "held") => {
                if offered(&["release"]) {
                    Ok(())
                } else {
                    wedge("`release` is not offered")
                }
            }
            (_, "dependencies") => {
                let dependencies: Vec<String> = sqlx::query_scalar(
                    "SELECT depends_on_id FROM task_dependency WHERE task_id = ?",
                )
                .bind(&self.tasks[index])
                .fetch_all(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if unsettled(dependencies).await? || offered(&["cancel", "retry", "start"]) {
                    Ok(())
                } else {
                    wedge("every dependency is settled and nothing is offered")
                }
            }
            (_, "children") => {
                let children: Vec<String> =
                    sqlx::query_scalar("SELECT id FROM task WHERE parent_task_id = ?")
                        .bind(&self.tasks[index])
                        .fetch_all(&self.live().pool)
                        .await
                        .map_err(|error| error.to_string())?;
                if unsettled(children).await? || offered(&["retry", "restart", "approve"]) {
                    Ok(())
                } else {
                    wedge("every child is settled and nothing is offered")
                }
            }
            (_, "capacity") => {
                let mut busy = false;
                for other in 0..self.tasks.len() {
                    busy |= other != index && self.waiting(other);
                }
                if busy {
                    Ok(())
                } else {
                    wedge("no execution holds the capacity it waits for")
                }
            }
            // Deadline parks: the scheduler re-reads them at a wall-clock
            // deadline the model does not advance. Owner action is the exit
            // the model can check.
            (
                _,
                "failure" | "agent_timeout" | "budget_exhausted" | "entry_blocked"
                | "human_decision" | "human_work" | "execution_stopped" | "review_checks"
                | "dispatch_refusal" | "integration" | "retry_deadline" | "review_grace",
            ) => {
                if offered(&[
                    "retry",
                    "restart",
                    "approve",
                    "send_back",
                    "start",
                    "release",
                ]) {
                    Ok(())
                } else {
                    wedge("no recovery action is offered")
                }
            }
            _ => wedge("the model knows no exit for this reason"),
        }
    }

    /// Liveness, end to end. Every pause is lifted, every run succeeds and
    /// every review passes; a Task parked for its owner gets the way forward
    /// the API offers, a Task with a cancelled dependency loses it, and
    /// everything else must move by itself. Every Task must then settle.
    async fn drive_to_settlement(&mut self) -> Result<(), String> {
        self.shared.verdicts.lock().unwrap().clear();
        if self.project_paused {
            self.apply(&Action::ResumeProject).await?;
        }
        if self.agent_paused {
            self.apply(&Action::ResumeAgent).await?;
        }
        self.quiesce().await?;
        let mut last = self.check_quiescent().await?;
        self.trace.push(format!("pauses lifted: {last}"));
        let mut unchanged = 0;
        let mut attempts: HashMap<usize, usize> = HashMap::new();
        for round in 0..SETTLE_ROUNDS {
            let mut open = 0;
            let mut moves = 0;
            let mut taken = Vec::new();
            let mut deferred = false;
            for index in 0..self.tasks.len() {
                let task = self.task(index).await?;
                let status = task["status"].as_str().unwrap_or_default().to_owned();
                if TERMINAL.contains(&status.as_str()) {
                    continue;
                }
                open += 1;
                if self.waiting(index) {
                    self.apply(&Action::Finish(index, Outcome::Success)).await?;
                    moves += 1;
                    continue;
                }
                let cancelled = task["condition"]["details"]["diagnostic"]["hook"]
                    ["cancelled_dependency_ids"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                if !cancelled.is_empty() {
                    for dependency in cancelled {
                        let (status, body) = self
                            .request(
                                Method::DELETE,
                                &format!(
                                    "/api/v1/tasks/{}/dependencies/{}",
                                    self.tasks[index],
                                    dependency.as_str().unwrap_or_default()
                                ),
                                None,
                            )
                            .await?;
                        if !status.is_success() {
                            return Err(format!(
                                "(c) removing the cancelled dependency of task {index} was refused: {status} {body}"
                            ));
                        }
                    }
                    moves += 1;
                    continue;
                }
                let kind = task["condition"]["kind"].as_str().unwrap_or_default();
                let reason = ["primary", "failure"]
                    .iter()
                    .find_map(|key| task["condition"][key]["kind"].as_str())
                    .unwrap_or_default();
                if kind == "deferred" {
                    deferred = true; // waits for a deadline, not for the owner.
                    continue;
                }
                // These wait for other Tasks, not for the owner.
                if !matches!(kind, "parked" | "failed")
                    || matches!(reason, "dependencies" | "children" | "capacity")
                {
                    continue;
                }
                // The owner's ways forward, least intrusive first. When one
                // was accepted and changed nothing, the next round takes the
                // next one: a park is a wedge only when none of them helps.
                let (version, offers) = self.offers(index).await?;
                let forward: Vec<&Value> = ["release", "retry", "send_back", "restart", "approve"]
                    .iter()
                    .filter_map(|verb| offers.iter().find(|offer| offer["action"]["verb"] == *verb))
                    .collect();
                if !forward.is_empty() {
                    let turn = attempts.entry(index).or_insert(0);
                    let offer = forward[*turn % forward.len()];
                    *turn += 1;
                    let verb = self.apply_offer(index, version, offer).await?;
                    taken.push(format!("{verb}({index})"));
                    moves += 1;
                }
            }
            if open == 0 {
                return Ok(());
            }
            if deferred && self.move_deadlines(true).await? > 0 {
                taken.push("clock".to_owned());
                moves += 1;
            }
            if moves == 0 {
                return Err(format!(
                    "(c) wedge: with every pause lifted, {open} Task(s) stay unsettled and nothing can move them: no run is live, no owner action is offered for a park, and the scheduler does nothing: {last}"
                ));
            }
            self.quiesce().await?;
            let state = self.check_quiescent().await?;
            self.trace
                .push(format!("settle {round} {taken:?}: {state}"));
            // Invariant (b), second half: an accepted offer changes state.
            if state == last {
                unchanged += 1;
            } else {
                unchanged = 0;
                attempts.clear();
            }
            last = state;
            // Twice through every way forward an owner can be offered.
            if unchanged >= 10 {
                let mut conditions = String::new();
                for index in 0..self.tasks.len() {
                    let task = self.task(index).await?;
                    if !TERMINAL.contains(&task["status"].as_str().unwrap_or_default()) {
                        conditions.push_str(&format!("\n  task {index}: {}", task["condition"]));
                    }
                }
                return Err(format!(
                    "(b) wedge: every offered way forward is accepted and changes nothing, round after round (last taken: {taken:?}): {last}{conditions}"
                ));
            }
        }
        Err(format!(
            "(d) no settlement after {SETTLE_ROUNDS} rounds of successful runs, passing reviews and offered recoveries: {last}"
        ))
    }
}

async fn register_agents(app: &Router, workspaces: &Path) -> (String, String) {
    let registration: DaemonRegisterResponse = common::json_request(
        app,
        Method::POST,
        "/api/v1/daemons/register",
        json!({
            "machine_id": services::embedded_daemon::embedded_machine_id(),
            "hostname": "model-host",
            "os": "linux",
            "arch": "x86_64",
            "agent_version": "model"
        }),
        StatusCode::OK,
    )
    .await;
    let _: DaemonResponse = common::json_request_with_bearer(
        app,
        Method::POST,
        &format!("/api/v1/daemons/{}/report", registration.daemon_id),
        &registration.registration_token,
        json!({
            "detected_clis": [{ "kind": "codex", "availability": "authenticated", "path": "/bin/codex" }],
            "runtimes": [{ "kind": "local", "workspace_root": workspaces.to_string_lossy(), "status": "ready" }]
        }),
        StatusCode::OK,
    )
    .await;
    let mut ids = Vec::new();
    for name in ["model-coder", "model-reviewer"] {
        let agent: AgentResponse = common::json_request_with_bearer(
            app,
            Method::POST,
            "/api/v1/agents",
            &common::admin_jwt(),
            json!({ "name": name, "executor_type": "codex", "daemon_id": registration.daemon_id }),
            StatusCode::OK,
        )
        .await;
        ids.push(agent.id);
    }
    (ids.remove(0), ids.remove(0))
}

// ---------------------------------------------------------------------------
// Running, shrinking and reporting
// ---------------------------------------------------------------------------

struct Failure {
    step: Option<usize>,
    violation: String,
    trace: Vec<String>,
}

/// Run one sequence on a fresh stack. `Ok` carries the trace: one line per
/// step with every Task's state, which is what "deterministic" compares.
async fn run(sequence: &[Step], avoid_open_findings: bool) -> Result<Vec<String>, Failure> {
    let mut world = World::new().await;
    world.avoid_open_findings = avoid_open_findings;
    let mut at = None;
    let outcome = async {
        for (number, step) in sequence.iter().enumerate() {
            at = Some(number);
            let applied = world.apply(&step.action).await?;
            world.check_committed().await?;
            if step.crash_before_drain {
                world.crash_and_restart().await?;
                world.check_committed().await?;
            }
            world.quiesce().await?;
            let state = world.check_quiescent().await?;
            world.trace.push(format!(
                "{number} {:?}{} -> {applied}: {state}",
                step.action,
                if step.crash_before_drain {
                    " +crash"
                } else {
                    ""
                }
            ));
        }
        at = None;
        world.drive_to_settlement().await
    }
    .await;
    if outcome.is_err() {
        world.record_history().await;
    }
    // Leave nothing running behind the temporary directory.
    if let Some(live) = world.live.take() {
        live.script.alive.store(false, Ordering::SeqCst);
        live.script.waiting.lock().unwrap().clear();
        live.runtime.task_dispatcher.stop();
        let _ = tokio::time::timeout(Duration::from_secs(5), live.pool.close()).await;
    }
    match outcome {
        Ok(()) => Ok(world.trace),
        Err(violation) => Err(Failure {
            step: at,
            violation,
            trace: world.trace,
        }),
    }
}

/// The invariant letter of a violation: a shrunk case must break the same one.
fn class(violation: &str) -> &str {
    violation.get(..3).unwrap_or(violation)
}

/// Simple delta shrink: drop one step at a time, from the end, and keep the
/// drop when the same invariant still breaks.
async fn shrink(sequence: Vec<Step>, failure: Failure, avoid: bool) -> (Vec<Step>, Failure) {
    let wanted = class(&failure.violation).to_owned();
    let (mut sequence, mut failure) = (sequence, failure);
    // Steps after the failing one never ran.
    if let Some(step) = failure.step {
        sequence.truncate(step + 1);
    }
    let mut replays = 0;
    let mut index = sequence.len();
    while index > 0 && replays < 40 {
        index -= 1;
        let mut candidate = sequence.clone();
        candidate.remove(index);
        replays += 1;
        if let Err(smaller) = run(&candidate, avoid).await {
            if class(&smaller.violation) == wanted {
                sequence = candidate;
                failure = smaller;
            }
        }
    }
    (sequence, failure)
}

fn report(name: &str, sequence: &[Step], failure: &Failure) -> String {
    format!(
        "\n=== model violation ({name}) ===\n{}\nfailing step: {:?}\nminimized sequence ({} steps):\n{}\ntrace:\n{}\n",
        failure.violation,
        failure.step,
        sequence.len(),
        sequence
            .iter()
            .map(|step| format!(
                "  {:?}{}",
                step.action,
                if step.crash_before_drain { " +crash" } else { "" }
            ))
            .collect::<Vec<_>>()
            .join("\n"),
        failure.trace.join("\n"),
    )
}

fn digest(trace: &[String]) -> u64 {
    // FNV-1a over the trace: equal digests mean equal histories.
    trace
        .iter()
        .flat_map(|line| line.bytes().chain(*b"\n"))
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

fn env_number(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

async fn run_cases(cases: Vec<(String, Vec<Step>)>) {
    run_all(cases, false).await;
}

async fn run_all(cases: Vec<(String, Vec<Step>)>, avoid: bool) {
    let mut passed = Vec::new();
    let mut failed = Vec::new();
    // Cases are independent stacks; a few at a time keeps the wall time low.
    for batch in cases.chunks(4) {
        let outcomes =
            futures_util::future::join_all(batch.iter().map(|(_, sequence)| async move {
                let started = std::time::Instant::now();
                (run(sequence, avoid).await, started.elapsed())
            }))
            .await;
        for ((name, sequence), (outcome, took)) in batch.iter().zip(outcomes) {
            match outcome {
                Ok(trace) => {
                    if std::env::var("FORGE_MODEL_TRACE").is_ok() {
                        println!("--- {name}\n{}", trace.join("\n"));
                    }
                    passed.push(format!(
                        "{name} digest {:016x} ({} ms)",
                        digest(&trace),
                        took.as_millis()
                    ));
                }
                Err(failure) => failed.push((name.clone(), sequence.clone(), failure)),
            }
        }
    }
    passed.sort();
    for line in &passed {
        println!("model case ok: {line}");
    }
    let mut reports = String::new();
    for (name, sequence, failure) in failed {
        let (sequence, failure) = shrink(sequence, failure, avoid).await;
        reports.push_str(&report(&name, &sequence, &failure));
    }
    assert!(reports.is_empty(), "{reports}");
}

/// Seeds every run covers, so a regression in one of them is a stable failure.
const FIXED_SEEDS: [u64; 5] = [1, 2, 3, 5, 8];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_sequences_keep_every_task_live() {
    let long = std::env::var("FORGE_MODEL_LONG").is_ok_and(|value| value != "0");
    let steps = env_number("FORGE_MODEL_STEPS").unwrap_or(if long { 40 } else { 16 }) as usize;
    let chosen: Vec<u64> = std::env::var("FORGE_MODEL_SEED")
        .map(|list| {
            list.split(',')
                .filter_map(|seed| seed.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default();
    let seeds = if chosen.is_empty() {
        // Random seeds only on request: see the header.
        let extra = env_number("FORGE_MODEL_CASES").unwrap_or(0);
        let mut clock = Rng(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64));
        let mut seeds = FIXED_SEEDS.to_vec();
        seeds.extend((0..extra).map(|_| clock.next() % 1_000_000_000));
        seeds
    } else {
        chosen
    };
    println!("model seeds (reproduce one with FORGE_MODEL_SEED=<n>, steps {steps}): {seeds:?}");
    run_all(
        seeds
            .into_iter()
            .map(|seed| (format!("FORGE_MODEL_SEED={seed}"), generate(seed, steps)))
            .collect(),
        true,
    )
    .await;
}

/// Sequences for wedges this project has shipped, as far as the action set
/// can express them. Each must end with every Task settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_wedges_stay_fixed() {
    use Action::{Cancel, Claim, Crash, Create, Depend, Finish};
    run_cases(vec![
        // v0.13.10: the merge landed but the cascade stopped short and the
        // Task stayed in `merging`. The plain path must reach `done`.
        (
            "merge cascade reaches done".to_owned(),
            vec![
                step(Create),
                step(Claim(0)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        // 3.2 C2b: a merge intent orphaned by a crash. The coder's result is
        // committed, the server dies before the review / merge steps run.
        (
            "crash before the review and merge steps".to_owned(),
            vec![
                step(Create),
                step(Claim(0)),
                crash_after(Finish(0, Outcome::Success)),
            ],
        ),
        // A crash with a coder run in flight: recovery must reclaim it.
        (
            "crash with a run in flight".to_owned(),
            vec![step(Create), step(Claim(0)), step(Crash)],
        ),
        // Review-refresh livelock (stress pass 3) and conflict handoff: two
        // Tasks edit the same file, the second conflicts at merge, is handed
        // back, reconciled and must merge after a fresh review.
        (
            "second merge conflicts and is handed back".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Claim(0)),
                step(Claim(1)),
                step(Finish(0, Outcome::Conflict)),
                step(Finish(1, Outcome::Conflict)),
            ],
        ),
        // Cancelled-dependency stranding: the dependant must keep a way out.
        (
            "dependency cancelled under a waiting Task".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Depend(1, 0)),
                step(Cancel(0)),
            ],
        ),
        // Found by a random run of this model and fixed with it: linking
        // the same dependency twice answered 500.
        (
            "the same dependency twice".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Depend(1, 0)),
                step(Depend(1, 0)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        // A dependant of a finished Task is released.
        (
            "dependency completes under a waiting Task".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Depend(1, 0)),
                step(Claim(0)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        // A failed review and a failed check go back to the coder and recover.
        (
            "review rejection and check failure recover".to_owned(),
            vec![
                step(Create),
                step(Action::Verdict(0, false)),
                step(Claim(0)),
                step(Finish(0, Outcome::CiFail)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
    ])
    .await;
}

/// Pause with no resume: every hold the API offers must offer its release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_task_always_offers_its_release() {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        world.apply(&Action::Create).await?;
        world.quiesce().await?;
        let mut held = 0;
        for stage in ["initial", "claimed"] {
            let (version, offers) = world.offers(0).await?;
            if let Some(hold) = offers
                .iter()
                .find(|offer| offer["action"]["verb"] == "hold")
            {
                held += 1;
                world.apply_offer(0, version, hold).await?;
                world.quiesce().await?;
                world.check_quiescent().await?;
                let (version, offers) = world.offers(0).await?;
                let release = offers
                    .iter()
                    .find(|offer| offer["action"]["verb"] == "release")
                    .ok_or_else(|| {
                        format!("(c) {stage}: a held Task offers no release: {offers:?}")
                    })?;
                world.apply_offer(0, version, release).await?;
                world.quiesce().await?;
                world.check_quiescent().await?;
            }
            world.apply(&Action::Claim(0)).await?;
            world.quiesce().await?;
        }
        if held == 0 {
            return Err("`hold` was never offered, so this test checked nothing".to_owned());
        }
        world.drive_to_settlement().await
    }
    .await;
    if let Err(violation) = outcome {
        panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
    }
}

/// Found by a random run of this model and fixed with it; minimized below.
///
/// `start` was offered on a Task waiting in `todo` (for capacity, or because
/// its coder was paused) for the Agent of the planning gate's role, which
/// with no planner assigned fell back to any available Agent, while the
/// claim it then made entered `in_progress` for the coder role. The claim
/// was refused ("role 'coder' is assigned to a different agent"), the Task
/// was parked as `recovery_required`, and the `retry` then offered restored
/// the same park every time.
///
/// The offer, the queued action and its replay now resolve the role the way
/// the claim does (`services::workflow::action_role`). So `start` on a Task
/// queued for capacity is accepted and waits for that capacity, and `start`
/// is not offered while the Agent it would run on is paused: resuming the
/// Agent is that Task's exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_on_a_queued_task_does_not_park_it_as_failed() {
    use Action::{Create, PauseAgent, Take};
    for (sequence, expected) in [
        (vec![Create, Create, Create, Take(2, "start")], "start"),
        (
            vec![PauseAgent, Create, Take(0, "start")],
            "start not offered",
        ),
    ] {
        let mut world = World::new().await;
        let outcome: Result<(), String> = async {
            let mut applied = String::new();
            for action in &sequence {
                applied = world.apply(action).await?;
                world.quiesce().await?;
                let state = world.check_quiescent().await?;
                world
                    .trace
                    .push(format!("{action:?} -> {applied}: {state}"));
            }
            if applied != expected {
                return Err(format!(
                    "taking `start` answered `{applied}`, expected `{expected}`"
                ));
            }
            let started = world.tasks.len() - 1;
            let task = world.task(started).await?;
            if task["status"] != "todo" || task["condition"]["kind"] == "parked" {
                return Err(format!(
                    "(b) `start` on a queued Task must leave it queued, not parked or moved: {} {}",
                    task["status"], task["condition"]
                ));
            }
            world.drive_to_settlement().await
        }
        .await;
        if let Err(violation) = outcome {
            panic!(
                "{violation}\nsequence {sequence:?}\ntrace:\n{}",
                world.trace.join("\n")
            );
        }
    }
}

/// Found by this model, fixed. A held Task used to lose its hold when one of
/// its dependencies was cancelled: the `dependency_cancelled` blocker
/// (`services/src/task_service/dependencies.rs`) overwrote it, and removing
/// the cancelled dependency then left the Task in `in_progress` with a
/// `clear` condition, no run and no park. The blocker now carries the
/// condition it displaces and removing the dependency restores it: the Task
/// is held again and offers `release`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_task_survives_a_cancelled_dependency() {
    use Action::{Cancel, Create, Depend, Take};
    run_cases(vec![(
        "held dependant, dependency cancelled".to_owned(),
        vec![
            step(Create),
            step(Create),
            step(Depend(0, 1)),
            step(Take(0, "hold")),
            step(Cancel(1)),
        ],
    )])
    .await;
}

/// Found by a random run of this model, fixed; minimized below. A dependency
/// is cancelled while its dependant is already parked for another reason
/// (here: an owner approval with no implementation candidate). The typed
/// `dependency_cancelled` blocker used to be skipped, so `send_back`, `retry`
/// and `restart` stayed on offer, each accepted and then refused by the
/// dependency gate into an untyped `recovery_required: "dependency gate"`
/// park. The Task now shows the typed blocker naming the dependency, offers
/// cancellation only, and gets its original park back when the dependency is
/// removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_task_names_its_cancelled_dependency() {
    use Action::{Cancel, Create, Depend, Take};
    run_cases(vec![(
        "parked dependant, dependency cancelled".to_owned(),
        vec![
            step(Create),
            step(Take(0, "approve")),
            step(Create),
            step(Depend(0, 1)),
            step(Cancel(1)),
        ],
    )])
    .await;
}

/// Found by a random run of this model and fixed with it; minimized below.
///
/// A coder run that failed after the Project's version changed (a pause, a
/// resume, any settings edit) was dropped: the Task stayed in `in_progress`
/// with a `clear` condition, no retry deadline, no blocker and no park, and
/// nothing relaunched it.
///
/// The failure path and the dispatcher's healer both refused to act on a run
/// dispatched under an older Project version. That fence is for completions,
/// which advance the workflow. A failure is now retried (or blocks the Task)
/// under the current Project whichever version dispatched the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_fails_after_a_project_pause_is_retried() {
    use Action::{Create, EditProject, Finish, PauseProject, ResumeProject};
    let mut cases = Vec::new();
    for outcome in [Outcome::Fail, Outcome::UsageLimit] {
        for (name, edits) in [
            ("while the Project is paused", vec![PauseProject]),
            (
                "after the Project was paused and resumed",
                vec![PauseProject, ResumeProject],
            ),
            ("after the Project was edited", vec![EditProject]),
        ] {
            let mut sequence = vec![step(Create)];
            sequence.extend(edits.into_iter().map(step));
            sequence.push(step(Finish(0, outcome)));
            cases.push((format!("run ends {outcome:?} {name}"), sequence));
        }
    }
    // The failed run must leave a pending retry behind, not only settle
    // because the final drive pokes the Task.
    for (name, sequence) in &cases {
        if name.contains("paused") && !name.contains("resumed") {
            continue; // a paused Project legitimately waits.
        }
        let trace = run(sequence, false)
            .await
            .unwrap_or_else(|failure| panic!("{}", report(name, sequence, &failure)));
        let after_failure = &trace[sequence.len() - 1];
        assert!(
            after_failure.contains("0:in_progress/deferred/")
                || after_failure.contains("0:in_progress/running/"),
            "{name}: the failed run left no pending retry and no new run: {after_failure}\n{}",
            trace.join("\n")
        );
    }
    run_cases(cases).await;
}

/// OPEN, UNTRIAGED: found by a random run once the generator stopped stepping
/// around failures after a Project pause (`FORGE_MODEL_SEED=45233331
/// FORGE_MODEL_LONG=1`); minimized below. The coder Agent is paused, the
/// running Task is held, the server crashes before the hold's steps run, and
/// the Task is released. Once the Agent resumes the Task stays in
/// `in_progress` with a `clear` condition and no run; `retry` is offered.
/// The same crash under a paused Agent without the hold recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "open stall: hold, crash and release under a paused Agent leave the Task idle once the Agent resumes"]
async fn a_task_released_after_a_crash_under_a_paused_agent_runs_again() {
    use Action::{Create, PauseAgent, Take};
    run_cases(vec![(
        "hold, crash and release under a paused Agent".to_owned(),
        vec![
            step(Create),
            step(PauseAgent),
            crash_after(Take(0, "hold")),
            step(Take(0, "release")),
        ],
    )])
    .await;
}

/// Found by this model, fixed. A subtask used to be accepted under a parent
/// that was already done, cancelled, in review or integrating; it then sat in
/// `todo` with a `clear` condition, was never scheduled and offered only
/// `start` and `cancel`. Creating it is now refused with
/// `SUBTASK_PARENT_CLOSED`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subtask_of_a_settled_parent_is_scheduled_or_refused() {
    use Action::{Create, CreateChild, Finish};
    run_cases(vec![(
        "subtask under a finished parent".to_owned(),
        vec![
            step(Create),
            step(Finish(0, Outcome::Success)),
            step(CreateChild(0)),
        ],
    )])
    .await;
}

/// OPEN WEDGE found by a random run with `CreateChild` generated; minimized
/// below. A coordination root whose aggregate review check fails ends in
/// `review` parked on `review retry budget exhausted`. The offered `retry`
/// is accepted, moves the root to `in_progress` and parks it on "invalid
/// operation: coordination root <id> is not in its aggregate review state";
/// the offered `restart` puts it back in `review` on the exhausted budget.
/// The two alternate for ever and no offer leads anywhere else.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "open wedge: a coordination root that fails aggregate review loops between retry and restart"]
async fn a_coordination_root_that_fails_aggregate_review_can_recover() {
    use Action::{Create, CreateChild, Finish};
    run_cases(vec![(
        "root fails aggregate review".to_owned(),
        vec![
            step(Create),
            step(Create),
            crash_after(Finish(1, Outcome::Fail)),
            step(CreateChild(1)),
            step(Finish(2, Outcome::CiFail)),
        ],
    )])
    .await;
}

/// OPEN finding from a random run with `CreateChild` generated; minimized
/// below. `cancel` is offered for a coordination root and refused with
/// `400 validation_error`: "the Task is settling a completed execution's plan
/// artifact; retry after publication". An offered action must be accepted
/// (invariant (b)).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "open: an offered cancel on a coordination root is refused while a plan artifact settles"]
async fn an_offered_cancel_is_accepted_while_a_plan_artifact_settles() {
    use Action::{Create, CreateChild, Finish, Offer, Verdict};
    run_cases(vec![(
        "cancel offered and refused on a root".to_owned(),
        vec![
            step(Create),
            step(Finish(0, Outcome::Success)),
            step(Create),
            step(Create),
            crash_after(Verdict(3, false)),
            step(CreateChild(2)),
            step(Finish(4, Outcome::Success)),
            step(Finish(4, Outcome::Success)),
            step(Offer(2, 0)),
        ],
    )])
    .await;
}

/// OPEN STALL found by a random run with `CreateChild` generated. A subtask
/// created under a held parent is accepted and never dispatched while the
/// hold lasts (`task_hierarchy::coordination_root_allows_child_dispatch`),
/// which is right, but it says nothing: it sits in `todo` with a `clear`
/// condition and offers `start` and `cancel`. Its exit is releasing the
/// parent; the subtask should show that it waits for its parent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "open stall: a subtask of a held parent waits with a clear condition"]
async fn a_subtask_of_a_held_parent_shows_why_it_waits() {
    use Action::{Create, CreateChild, Take};
    run_cases(vec![(
        "subtask under a held parent".to_owned(),
        vec![step(Create), step(Take(0, "hold")), step(CreateChild(0))],
    )])
    .await;
}
