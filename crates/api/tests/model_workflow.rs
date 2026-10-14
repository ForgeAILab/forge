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
//! `FORGE_MODEL_NO_SHRINK=1` reports a failure as found, without replaying
//! shorter sequences (each replay of a stall costs its full patience).
//! `FORGE_MODEL_CASE=<text>` runs only the cases whose name contains it,
//! `FORGE_MODEL_LOG=<level>` prints the stack's log with timestamps, and
//! `FORGE_MODEL_LOCK_STORM=<ms>` adds a second writer to every model database.
//!
//! The server's run cap is pinned (`MODEL_RUN_CAP`, or `FORGE_MODEL_RUN_CAP=<n>`)
//! instead of derived from the host's cores, so a 4-core CI runner and a
//! workstation walk the same histories. It is the automatic cap of a small
//! machine: runs, and review checks, do wait for a slot in these sequences.
//! `sequences_stay_live_at_run_cap_1` and `sequences_stay_live_without_a_pinned_run_cap`
//! walk a few of the same sequences with one slot and with the host's own cap:
//! a machine limited to one run is a supported setting, and with it a Task's
//! whole life (run, review check, reviewer, merge) happens in that one slot.
#![allow(dead_code)]
mod common;

use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock, Weak,
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
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
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
    /// Build the breakdown before the first dispatcher pass.
    CreateLeadRoot,
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
    /// Make the scripted lead the planner of the Task (a coordination root).
    AssignLead(usize),
    RemoveLead(usize),
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
    let mut sequence = vec![step(Action::CreateLeadRoot), step(Action::AssignLead(0))];
    let mut tasks = 3_u64;
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
            14 => Action::EditProject,
            15 if tasks < 6 => {
                tasks += 1;
                Action::CreateChild(0)
            }
            16 => Action::AssignLead(0),
            17 => Action::RemoveLead(0),
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
    db: Arc<db::SqliteDb>,
    service: OnceLock<Weak<services::TaskService>>,
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
    lead_operations: Mutex<Vec<String>>,
    lead_operation_errors: Mutex<Vec<String>>,
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

// ---------------------------------------------------------------------------
// Scripted lead (plan 3.10): `Action::AssignLead` and the planner runs of a
// root's lead. The lead must be an Agent with a native profile; the model's
// one is native by `backend_kind` and runs through the scripted adapter (its
// profile names the adapter's executor type), so no provider is needed.
// ---------------------------------------------------------------------------

/// System text of the step-check prompt (`planner.step_check.v1`).
const STEP_CHECK_PROMPT_MARK: &str = "Step-check doctrine:";
/// System text of the lead's prompt on its root (`planner.coordination.v1`).
const LEAD_PROMPT_MARK: &str = "You are the lead of this Forge coordination root";
const LEAD_AGENT_ID: &str = "model-lead";

impl ScriptedAdapter {
    async fn replan_operations(&self, ctx: &ExecutionContext) -> Result<(), ExecutorError> {
        let db = &self.0.db;
        let replanning: bool = sqlx::query_scalar(
            "SELECT COALESCE((SELECT bridge_kind='replan' FROM transition_log WHERE task_id=? AND to_state='planning' ORDER BY rowid DESC LIMIT 1), 0)"
        ).bind(&ctx.task_id).fetch_one(db.pool()).await
            .map_err(|e| ExecutorError::Other(e.to_string()))?;
        if !replanning {
            return Ok(());
        }
        let service = self
            .0
            .service
            .get()
            .and_then(Weak::upgrade)
            .expect("live model service");
        let read = lead_call(
            db,
            &service,
            LEAD_AGENT_ID,
            &ctx.task_id,
            "lead.read",
            json!({}),
            &format!("{}:read", ctx.execution_id),
        )
        .await
        .map_err(|e| ExecutorError::Other(format!("model lead read: {e:?}")))?;
        let children = read["subtasks"].as_array().expect("lead reads subtasks");
        let mut calls = vec![(
            "lead.subtask",
            json!({"action":"create", "title":"model lead fix", "description":"finish the root deliverable"}),
        )];
        if let Some(child) = children.iter().find(|c| c["lead_decides_park"] == true) {
            calls.push(("lead.recover", json!({"task_id":child["task_id"], "version":child["version"], "action":{"verb":"retry"}})));
        }
        if let Some(child) = children
            .iter()
            .find(|c| c["started"] == false && c["status"] == "todo")
        {
            calls.push(("lead.subtask", json!({"action":"cancel", "task_id":child["task_id"], "version":child["version"], "reason":"replaced by the lead fix"})));
        }
        for (n, (operation, payload)) in calls.into_iter().enumerate() {
            let result = lead_call(
                db,
                &service,
                LEAD_AGENT_ID,
                &ctx.task_id,
                operation,
                payload,
                &format!("{}:op:{n}", ctx.execution_id),
            )
            .await;
            let value = match result {
                Ok(value) => value,
                Err(forge_agent_host::AgentHostError::StructuredOutcome(outcome))
                    if outcome.code == api_types::OutcomeCode::ValidationError
                        && outcome
                            .details
                            .as_ref()
                            .is_some_and(|details| details["refusal"] == "lead_limit_reached") =>
                {
                    continue
                }
                Err(error) => {
                    return Err(ExecutorError::Other(format!(
                        "model lead operation {operation}: {error:?}"
                    )))
                }
            };
            self.0.shared.lead_operations.lock().unwrap().push(format!(
                "{operation}:{}",
                value["action"].as_str().unwrap_or("retry")
            ));
            if operation == "lead.subtask" && value["action"] == "create" {
                let id = value["task_id"].as_str().expect("created id").to_owned();
                let mut index = self.0.shared.index.lock().unwrap();
                let next = index.len();
                let root = index[&ctx.task_id];
                let child_index = *index.entry(id).or_insert(next);
                self.0
                    .shared
                    .parents
                    .lock()
                    .unwrap()
                    .insert(child_index, root);
            }
        }
        Ok(())
    }

    /// The lead's step check of a subtask: a result block with the verdict
    /// the model scripted for that subtask (`Action::Verdict`; pass when
    /// nothing is scripted).
    async fn step_check(&self, ctx: &ExecutionContext) -> Result<(), ExecutorError> {
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
            "The step leaves its deliverable.\n{\"result\":\"pass\",\"reason\":\"the model scripted a pass\"}"
        } else {
            "The step is not accepted.\n{\"result\":\"fail\",\"reason\":\"the model scripted a return\"}"
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

/// The native Task session of `agent` on `task_id`, created once.
async fn session(db: &db::SqliteDb, agent: &str, task_id: &str) -> String {
    let runtime = format!("runtime:{agent}:{task_id}");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM agent_session WHERE runtime_session_id=?)")
            .bind(&runtime)
            .fetch_one(db.pool())
            .await
            .unwrap();
    if exists {
        return runtime;
    }
    let (profile, project): (String, String) = sqlx::query_as(
        "SELECT i.selected_profile_id, t.project_id FROM agent_identity i, task t WHERE i.id=? AND t.id=?",
    )
    .bind(agent)
    .bind(task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let now = db::now_rfc3339();
    let scope = db::new_uuid_v4();
    db::AgentContextScopeRepo::create_context_scope(
        db,
        db::CreateAgentContextScope {
            id: scope.clone(),
            identity_id: agent.to_owned(),
            scope_type: "task".to_owned(),
            scope_id: task_id.to_owned(),
            project_id: Some(project),
            task_id: Some(task_id.to_owned()),
            task_role: Some("planner".to_owned()),
            workspace_access: "task_read".to_owned(),
            authority_json: "{}".to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
            workspace_path: None,
        },
    )
    .await
    .expect("context scope creates");
    db::AgentSessionRepo::create_agent_session(
        db,
        db::CreateAgentSession {
            id: db::new_uuid_v4(),
            identity_id: agent.to_owned(),
            profile_id: profile,
            context_scope_id: scope,
            backend_kind: "native".to_owned(),
            runtime_session_id: Some(runtime.clone()),
            status: "running".to_owned(),
            capabilities_json: "{}".to_owned(),
            connection_status: "healthy".to_owned(),
            predecessor_session_id: None,
            last_activity_at: Some(now.clone()),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("agent session creates");
    runtime
}

fn task_scope(task_id: &str) -> CanonicalScope {
    CanonicalScope {
        scope_type: CanonicalScopeType::Task,
        scope_id: task_id.to_owned(),
        workspace_access: WorkspaceAccess::TaskRead,
    }
}

/// One lead operation, as the native host issues it for the run of `agent`
/// on `task_id`.
async fn lead_call(
    db: &Arc<db::SqliteDb>,
    service: &Arc<services::TaskService>,
    agent: &str,
    task_id: &str,
    operation: &str,
    payload: Value,
    key: &str,
) -> std::result::Result<Value, forge_agent_host::AgentHostError> {
    let runtime = session(db, agent, task_id).await;
    let provider = services::CoordinationToolProvider::new(Arc::clone(db));
    provider.set_task_service(Arc::clone(service));
    ForgeToolProvider::propose(
        &provider,
        agent,
        &task_scope(task_id),
        &runtime,
        operation,
        json!({"operation": operation, "payload": payload, "dedupe_key": key, "correlation_id": key}),
    )
    .await
    // The host wraps a command's value in its outcome envelope.
    .map(|value| match value.get("result") {
        Some(result) if result.is_object() => result.clone(),
        _ => value,
    })
}

impl World {
    /// The scripted lead, created on first use.
    async fn lead_agent(&self) -> Result<String, String> {
        let db = &*self.live().state.db;
        let err = |error: db::DbError| error.to_string();
        if db::AgentRepo::get_by_id(db, LEAD_AGENT_ID)
            .await
            .map_err(err)?
            .is_some()
        {
            return Ok(LEAD_AGENT_ID.to_owned());
        }
        let daemon_id: Option<String> = sqlx::query_scalar(
            "SELECT p.daemon_id FROM agent_profile p JOIN agent_identity i ON i.selected_profile_id = p.id WHERE i.id = ?",
        )
        .bind(&self.coder_id)
        .fetch_one(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let now = db::now_rfc3339();
        let agent = db::AgentRepo::create_identity_with_profile(
            db,
            db::CreateAgentIdentity {
                id: LEAD_AGENT_ID.to_owned(),
                name: "model-lead".to_owned(),
                description: None,
                max_concurrent_tasks: 2,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: Some(now.clone()),
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                account_permission_ceiling: r#"["read_task","read_memory","task_read"]"#.to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
            db::CreateAgentProfile {
                id: db::new_uuid_v4(),
                identity_id: LEAD_AGENT_ID.to_owned(),
                backend_kind: "native".to_owned(),
                executor_type: "codex".to_owned(),
                provider: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                tool_policy_json: r#"["read_task","read_memory","task_read"]"#.to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .map_err(err)?;
        db::AgentConnectionHealthRepo::upsert_connection_health(
            db,
            db::UpsertAgentConnectionHealth {
                profile_id: agent.profile_id.clone(),
                status: "healthy".to_owned(),
                capability_status_json: "{}".to_owned(),
                checked_at: Some(now.clone()),
                error_code: None,
                updated_at: now,
            },
        )
        .await
        .map_err(err)?;
        Ok(LEAD_AGENT_ID.to_owned())
    }

    /// `Action::AssignLead`: the lead becomes the planner of the Task. A
    /// refusal (the Task is a subtask, or cannot have a lead) is an answer.
    async fn assign_lead(&mut self, index: usize) -> Result<String, String> {
        let lead = self.lead_agent().await?;
        let (status, body) = self
            .request(
                Method::PUT,
                &format!("/api/v1/tasks/{}/roles/planner", self.tasks[index]),
                Some(json!({ "assignee_type": "agent", "assignee_id": lead })),
            )
            .await?;
        Ok(if status.is_success() {
            "lead assigned".to_owned()
        } else {
            format!("refused {status} {}", body["code"])
        })
    }

    /// One model step outside `run`: the action, then every invariant of a
    /// committed and of a quiescent state.
    async fn lead_step(&mut self, action: Action) -> Result<String, String> {
        let result: Result<String, String> = async {
            let applied = self.apply(&action).await?;
            self.check_committed().await?;
            self.quiesce().await?;
            let state = self.check_quiescent().await?;
            self.trace.push(format!("{action:?} -> {applied}: {state}"));
            Ok(applied)
        }
        .await;
        result.map_err(|error| {
            format!(
                "{error}\nfailing action: {action:?}\ntrace: {:#?}",
                self.trace
            )
        })
    }

    async fn lead_count(&self, sql: &str) -> Result<i64, String> {
        sqlx::query_scalar(sql)
            .fetch_one(&self.live().pool)
            .await
            .map_err(|error| error.to_string())
    }
}

/// A coordination root with a lead, end to end on the real stack: the lead
/// plans (the root rests in `planning` with its run live, its subtasks parked
/// on it), then checks every finished step; one step is returned once and
/// then accepted. Every invariant runs after every action, so (i), (j) of
/// stage 1 and (m), (n) of stage 3 are checked on states that have a lead, a
/// planning root, running step checks and a charged `gate:lead_check`.
async fn root_with_a_lead() -> Result<(), String> {
    let mut world = World::new().await;
    world.create(None).await?;
    for _ in 0..2 {
        let child = world.create(Some(0)).await?;
        if child.starts_with("refused") {
            return Err(format!("the subtask was {child}"));
        }
    }
    let assigned = world.lead_step(Action::AssignLead(0)).await?;
    if assigned != "lead assigned" {
        return Err(format!("the lead was {assigned}"));
    }
    // The lead plans: its run is live on the root, nothing else runs.
    let root = world.task(0).await?;
    let planning = root["status"] == "planning";
    if planning {
        if !world.waiting(0) {
            return Err(format!(
                "the root plans and its lead does not run: {}",
                root["condition"]
            ));
        }
        let parked = world
            .lead_count(
                "SELECT COUNT(*) FROM task c WHERE c.parent_task_id IS NOT NULL
                   AND json_extract(c.condition_json, '$.primary.kind') = 'parent'
                   AND json_extract(c.condition_json, '$.primary.cause') = 'planning'",
            )
            .await?;
        if parked != 2 {
            return Err(format!(
                "(j) {parked} of 2 subtasks are parked on the planning root"
            ));
        }
        world.lead_step(Action::Finish(0, Outcome::Success)).await?;
    }
    // The first step is returned once, then accepted.
    world.lead_step(Action::Verdict(1, false)).await?;
    if !world.waiting(1) {
        return Err(format!(
            "the first subtask does not run: {:#?}",
            world.trace
        ));
    }
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    // The step rests in the gate with the lead's check live: (m), (h).
    if world.task(1).await?["status"] != "lead_check" || !world.waiting(1) {
        return Err(format!(
            "(m) the finished step is not in the gate with its check running: {}; trace {:#?}",
            world.task(1).await?,
            world.trace
        ));
    }
    let live = world
        .lead_count(
            "SELECT COUNT(*) FROM execution e JOIN task c ON c.id = e.task_id
             WHERE c.status = 'lead_check' AND e.role = 'planner' AND e.status = 'running'
               AND e.agent_id = 'model-lead'",
        )
        .await?;
    if live != 1 {
        return Err(format!(
            "(m) {live} step checks run under the lead, 1 expected"
        ));
    }
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    let returned = world
        .lead_count(
            "SELECT COALESCE(MAX(spent), 0) FROM task_budget WHERE kind = 'gate:lead_check'",
        )
        .await?;
    if returned != 1 || world.task(1).await?["status"] != "in_progress" {
        return Err(format!(
            "(n) the lead's `fail` did not return the step once: spent {returned}, task {}; trace {:#?}",
            world.task(1).await?,
            world.trace
        ));
    }
    world.lead_step(Action::Verdict(1, true)).await?;
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    world.lead_step(Action::Finish(1, Outcome::Success)).await?;
    if world.task(1).await?["status"] != "done" {
        return Err(format!(
            "the lead's `pass` did not finish the step: {}; trace {:#?}",
            world.task(1).await?,
            world.trace
        ));
    }
    // The second step and the root's review and merge: every waiting run
    // (the worker, the lead's check) is finished by the settlement drive.
    world.drive_to_settlement().await?;
    for index in 0..world.tasks.len() {
        let task = world.task(index).await?;
        if task["status"] != "done" {
            return Err(format!(
                "task {index} ended {}: {}; trace {:#?}",
                task["status"], task["condition"], world.trace
            ));
        }
    }
    let checks = world
        .lead_count(
            "SELECT COUNT(*) FROM execution e JOIN task c ON c.id = e.task_id
             WHERE c.parent_task_id IS NOT NULL AND e.role = 'planner' AND e.status = 'completed'",
        )
        .await?;
    if checks != 3 {
        return Err(format!(
            "(m) 3 step checks were expected (fail, pass, pass), {checks} ran"
        ));
    }
    let skipped = world
        .lead_count("SELECT COUNT(*) FROM transition_log WHERE from_state = 'lead_check' AND bridge_kind = 'gate_skipped'")
        .await?;
    if skipped != 0 {
        return Err(format!(
            "{skipped} step(s) passed the gate unchecked under a lead"
        ));
    }
    if !planning {
        return Err(format!(
            "(i), (j) the root never rested in planning under its lead; trace {:#?}",
            world.trace
        ));
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_root_with_a_lead_plans_and_has_every_step_checked() {
    log_to_stderr();
    if let Err(error) = root_with_a_lead().await {
        panic!("{error}");
    }
}

/// Exhaust the real step gate, then the replanning lead creates a fix,
/// recovers the parked step and cancels the untouched suffix via native tools.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replanning_lead_uses_real_operations() {
    let mut world = World::new().await;
    world.create(None).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.create(Some(0)).await.unwrap();
    for action in [
        Action::AssignLead(0),
        Action::Finish(0, Outcome::Success),
        Action::Verdict(1, false),
    ] {
        world.lead_step(action).await.unwrap();
    }
    for _ in 0..3 {
        world
            .lead_step(Action::Finish(1, Outcome::Success))
            .await
            .unwrap();
        if world.waiting(1) {
            world
                .lead_step(Action::Finish(1, Outcome::Success))
                .await
                .unwrap();
        }
    }
    assert_eq!(
        world.task(0).await.unwrap()["status"],
        "planning",
        "{:?}",
        world.trace
    );
    assert_eq!(
        world
            .lead_count("SELECT SUM(spent) FROM task_budget WHERE kind='replan'")
            .await
            .unwrap(),
        1
    );
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    assert_eq!(
        *world.shared.lead_operations.lock().unwrap(),
        [
            "lead.subtask:create",
            "lead.recover:retry",
            "lead.subtask:cancel"
        ]
    );
    assert_eq!(world.tasks.len(), 4);
    assert_eq!(world.task(2).await.unwrap()["status"], "cancelled");
    assert_eq!(world.task(0).await.unwrap()["status"], "in_progress");
    world.lead_step(Action::Verdict(1, true)).await.unwrap();
    world.drive_to_settlement().await.unwrap();
    assert_eq!(world.task(0).await.unwrap()["status"], "done");
    assert_eq!(world.task(3).await.unwrap()["status"], "done");
}

/// Different submitted steps consume one root's full re-plan window. Each
/// allowed re-plan performs native lead operations; the sixth trigger parks
/// the root instead of admitting another planning run or another debit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replanning_lead_operations_stop_at_the_root_budget() {
    let mut world = World::new().await;
    world.create(None).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.lead_step(Action::AssignLead(0)).await.unwrap();
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    let mut child = 1;
    for round in 1..=6 {
        world
            .lead_step(Action::Verdict(child, false))
            .await
            .unwrap();
        // Five actions from a worker (or four from a recovered check)
        // exhaust the gate. Stop before Finish's fallback could open the
        // root's planning run instead of this child's run.
        for _ in 0..6 {
            let root = world.task(0).await.unwrap();
            if root["status"] == "planning"
                || root["condition"]["primary"]["budget_kind"] == "replan"
            {
                break;
            }
            assert!(world.waiting(child), "the submitted step owns the next run");
            world
                .lead_step(Action::Finish(child, Outcome::Success))
                .await
                .unwrap();
        }
        let root = world.task(0).await.unwrap();
        if round == 6 {
            assert_eq!(root["status"], "in_progress");
            assert_eq!(
                root["condition"]["primary"]["kind"], "budget_exhausted",
                "{root}"
            );
            assert_eq!(
                root["condition"]["primary"]["budget_kind"], "replan",
                "{root}"
            );
            assert!(!world.waiting(0));
            break;
        }
        assert_eq!(root["status"], "planning", "{root}");
        world
            .lead_step(Action::Finish(0, Outcome::Success))
            .await
            .unwrap();
        assert_eq!(world.task(0).await.unwrap()["status"], "in_progress");
        let next = world.tasks.len() - 1;
        world.lead_step(Action::Verdict(child, true)).await.unwrap();
        // Retry validates the existing work first. If the product instead
        // needs an implementation run, finish that and its check too.
        for _ in 0..2 {
            if world.task(child).await.unwrap()["status"] == "done" {
                break;
            }
            assert!(world.waiting(child));
            world
                .lead_step(Action::Finish(child, Outcome::Success))
                .await
                .unwrap();
        }
        assert_eq!(world.task(child).await.unwrap()["status"], "done");
        child = next;
    }
    assert_eq!(
        world
            .lead_count("SELECT SUM(spent) FROM task_budget WHERE kind='replan'")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        world
            .lead_count("SELECT COUNT(*) FROM task_budget_charge WHERE kind='replan'")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        world
            .lead_count("SELECT COUNT(*) FROM transition_log WHERE bridge_kind='replan'")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        world
            .shared
            .lead_operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| op.as_str() == "lead.subtask:create")
            .count(),
        5
    );
    world.check_quiescent().await.unwrap();
}

/// A lead retries the exhausted gate, which validates the existing work.
/// Another failing verdict must stay within the gate's current window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_validation_after_lead_recovery_respects_the_step_budget() {
    let mut world = World::new().await;
    world.create(None).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.lead_step(Action::AssignLead(0)).await.unwrap();
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    world.lead_step(Action::Verdict(1, false)).await.unwrap();
    for _ in 0..3 {
        world
            .lead_step(Action::Finish(1, Outcome::Success))
            .await
            .unwrap();
        if world.waiting(1) {
            world
                .lead_step(Action::Finish(1, Outcome::Success))
                .await
                .unwrap();
        }
    }
    assert_eq!(world.task(0).await.unwrap()["status"], "planning");
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    assert!(world.waiting(1), "recovery starts the validation check");
    // Keep the failed verdict instead of passing the recovered validation.
    world
        .lead_step(Action::Finish(1, Outcome::Success))
        .await
        .unwrap();
    world.check_quiescent().await.unwrap();
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
        // A run of the scripted lead waits for a model action like any other
        // run, so the model sees the states in which it is live.
        let step_check = ctx.description.contains(STEP_CHECK_PROMPT_MARK);
        let lead_run = step_check || ctx.description.contains(LEAD_PROMPT_MARK);
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
            // The step check reports a verdict. On a re-plan the lead uses
            // the native tool boundary to change the breakdown.
            Ok(_) if lead_run => {
                if step_check {
                    self.step_check(&ctx).await?;
                } else {
                    let operations = self.replan_operations(&ctx).await;
                    // A crash can close this process's pool while its native
                    // tool awaits a read. The dead process reports neither a
                    // tool failure nor a completion to the surviving oracle.
                    if !self.0.alive.load(Ordering::SeqCst) {
                        return std::future::pending().await;
                    }
                    if let Err(error) = operations {
                        self.0
                            .shared
                            .lead_operation_errors
                            .lock()
                            .unwrap()
                            .push(error.to_string());
                        return Err(error);
                    }
                }
                Ok(result(ExecutionOutcome::Completed, None))
            }
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

/// The durable check worker of one boot. Review-entry CI is a check it
/// executes; it dies with its server.
struct Checks {
    stop: tokio::sync::watch::Sender<bool>,
    job: tokio::task::JoinHandle<()>,
}
impl Checks {
    fn stop(&self) {
        let _ = self.stop.send(true);
        self.job.abort();
    }
}
impl Drop for Checks {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Live {
    checks: Checks,
    pool: sqlx::SqlitePool,
    runtime: Arc<services::ForgeRuntime>,
    state: AppState,
    app: Router,
    script: Arc<Script>,
}

/// `FORGE_MODEL_LOCK_STORM=<ms>`: another writer on the model's database,
/// taking the write lock for that long and leaving it for as long, for the
/// whole run. A transaction that reads before it writes without having begun
/// immediate is refused under it almost every time, where it otherwise is
/// refused only when a second writer happens to be there; everything that
/// takes the lock up front merely waits.
struct LockStorm(tokio::task::JoinHandle<()>);
impl Drop for LockStorm {
    fn drop(&mut self) {
        self.0.abort();
    }
}
fn lock_storm(dir: &Path) -> Option<LockStorm> {
    let hold = Duration::from_millis(env_number("FORGE_MODEL_LOCK_STORM")?);
    let path = dir.join("forge.db");
    Some(LockStorm(tokio::spawn(async move {
        use sqlx::Connection;
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .busy_timeout(Duration::from_secs(30));
        let mut connection = sqlx::SqliteConnection::connect_with(&options)
            .await
            .expect("the lock storm connects");
        for statement in [
            "CREATE TABLE IF NOT EXISTS model_lock_storm(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)",
            "INSERT OR IGNORE INTO model_lock_storm VALUES (1, 0)",
        ] {
            sqlx::query(statement)
                .execute(&mut connection)
                .await
                .expect("the lock storm table creates");
        }
        loop {
            // A committed change each round: a reader's snapshot goes stale too.
            if sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut connection)
                .await
                .is_ok()
            {
                let _ = sqlx::query("UPDATE model_lock_storm SET n = n + 1")
                    .execute(&mut connection)
                    .await;
                tokio::time::sleep(hold).await;
                let _ = sqlx::query("COMMIT").execute(&mut connection).await;
            }
            tokio::time::sleep(hold).await;
        }
    })))
}

// ---------------------------------------------------------------------------
// Hard deadlines
// ---------------------------------------------------------------------------

/// The longest one wait of the model may take: a drain, a dispatcher pass, a
/// request, a start. Each of them takes well under a second when it works.
const WAIT_LIMIT: Duration = Duration::from_secs(120);
/// The longest a pre-queue fixture test may take, start to settlement.
const TEST_LIMIT: Duration = Duration::from_secs(300);

/// What a wait that never returns is reported from: where the model is, and
/// enough of the stack to read its state without the `World` (which the
/// stuck future borrows) and without the stack's own pool (which may be what
/// is stuck).
#[derive(Default)]
struct Probe {
    name: String,
    waits: Mutex<Vec<(String, std::time::Instant)>>,
    database: Mutex<Option<PathBuf>>,
    script: Mutex<Option<Arc<Script>>>,
}

impl Probe {
    fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            ..Self::default()
        })
    }

    /// Every table that says who owes a Task its next move, read on a
    /// connection of its own, and the in-process state no table has.
    async fn dump(&self) -> String {
        let mut out = format!("==== STATE DUMP {} ====\n", self.name);
        for (what, since) in self.waits.lock().unwrap().iter() {
            out.push_str(&format!("waiting {:?} in: {what}\n", since.elapsed()));
        }
        out.push_str(&format!("run cap {:?}\n", run_cap()));
        if let Some(script) = self.script.lock().unwrap().as_ref() {
            out.push_str(&format!(
                "scripted runs waiting for the model (task -> execution): {:?}\n",
                script
                    .waiting
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(task, (execution, _))| (task.clone(), execution.clone()))
                    .collect::<Vec<_>>()
            ));
        }
        out.push_str(&format!(
            "owner checkout locks of the whole test process (key, held, holders and waiters): {:?}\n",
            db::integration_owner_locks()
        ));
        let Some(database) = self.database.lock().unwrap().clone() else {
            return out + "no database yet\n";
        };
        match tokio::time::timeout(Duration::from_secs(30), Self::tables(&database)).await {
            Ok(tables) => out.push_str(&tables),
            Err(_) => out.push_str("the tables could not be read within 30 s\n"),
        }
        out
    }

    async fn tables(database: &Path) -> String {
        use sqlx::{Column, Connection, Row, ValueRef};
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(database)
            .read_only(true)
            .busy_timeout(Duration::from_secs(2));
        let mut connection = match sqlx::SqliteConnection::connect_with(&options).await {
            Ok(connection) => connection,
            Err(error) => return format!("the database could not be opened: {error}\n"),
        };
        let mut out = String::new();
        for (title, query) in [
            (
                "machine slots",
                "SELECT (SELECT COUNT(*) FROM execution WHERE status='running') AS running_executions,
                        (SELECT COUNT(*) FROM execution WHERE status IN ('pending','queued')) AS pending_executions,
                        (SELECT COUNT(*) FROM check_run WHERE admitted_at IS NOT NULL AND state IN ('running','cancelling','cleaning','uncertain')) AS admitted_checks,
                        (SELECT COUNT(*) FROM check_run WHERE state='queued') AS queued_checks,
                        (SELECT COUNT(*) FROM task_schedule_wait WHERE daemon_id='*') AS run_slot_waiters,
                        strftime('%Y-%m-%dT%H:%M:%fZ','now') AS now",
            ),
            ("task", "SELECT * FROM task ORDER BY created_at, rowid"),
            ("task_step (open, failed, or the last 12)", "SELECT * FROM task_step WHERE status NOT IN ('done','completed','cancelled','superseded') OR last_error IS NOT NULL OR rowid IN (SELECT rowid FROM task_step ORDER BY rowid DESC LIMIT 12) ORDER BY rowid"),
            ("integration_queue", "SELECT * FROM integration_queue"),
            ("integration_attempt", "SELECT * FROM integration_attempt ORDER BY created_at, rowid"),
            ("check_run", "SELECT * FROM check_run ORDER BY rowid"),
            ("check_consumer", "SELECT * FROM check_consumer ORDER BY rowid"),
            ("execution", "SELECT * FROM execution ORDER BY created_at, rowid"),
            ("task_schedule_wait", "SELECT * FROM task_schedule_wait"),
            ("task_schedule_park", "SELECT * FROM task_schedule_park"),
            ("task_schedule_dirty", "SELECT * FROM task_schedule_dirty"),
            ("project_schedule_dirty", "SELECT * FROM project_schedule_dirty"),
            ("task_slot_wait_age", "SELECT * FROM task_slot_wait_age"),
            ("task_budget", "SELECT * FROM task_budget"),
            ("workspace_lease", "SELECT * FROM workspace_lease"),
            ("worker_health", "SELECT * FROM worker_health"),
            ("worker_item_failure", "SELECT * FROM worker_item_failure ORDER BY rowid DESC LIMIT 20"),
            ("worker_dead_letter", "SELECT * FROM worker_dead_letter ORDER BY rowid DESC LIMIT 20"),
            ("transition_log (last 30)", "SELECT task_id, from_state, to_state, triggered_by, bridge_kind, substr(trigger_reason,1,160) AS reason, created_at FROM transition_log ORDER BY created_at DESC, rowid DESC LIMIT 30"),
        ] {
            out.push_str(&format!("-- {title}\n"));
            let rows = match sqlx::query(query).fetch_all(&mut connection).await {
                Ok(rows) => rows,
                Err(error) => {
                    out.push_str(&format!("   unreadable: {error}\n"));
                    continue;
                }
            };
            for row in rows {
                let mut line = String::new();
                for (at, column) in row.columns().iter().enumerate() {
                    if row.try_get_raw(at).map_or(true, |value| value.is_null()) {
                        continue;
                    }
                    let value = row
                        .try_get::<String, _>(at)
                        .or_else(|_| row.try_get::<i64, _>(at).map(|n| n.to_string()))
                        .or_else(|_| row.try_get::<f64, _>(at).map(|n| n.to_string()))
                        .unwrap_or_else(|_| "<blob>".to_owned());
                    if value.is_empty() || value == "{}" || value == "[]" {
                        continue;
                    }
                    let short: String = value.chars().take(400).collect();
                    line.push_str(&format!("{}={short} | ", column.name()));
                }
                out.push_str(&format!("   {line}\n"));
            }
        }
        let _ = connection.close().await;
        out
    }

    /// The dump, also straight to the process's stderr: libtest shows a
    /// test's captured output only once the whole run ends.
    async fn report(&self, what: &str) -> String {
        use std::io::Write;
        let report = format!("HANG {}: {what}\n{}", self.name, self.dump().await);
        let _ = writeln!(std::io::stderr(), "{report}");
        report
    }

    /// The backstop for a test thread that blocks (a lock, a child process)
    /// where no timer of its own runtime can fire: another thread reports the
    /// state and ends the test process.
    fn watchdog(self: &Arc<Self>, limit: Duration) -> std::sync::mpsc::Sender<()> {
        let (done, wait) = std::sync::mpsc::channel::<()>();
        let probe = Arc::clone(self);
        std::thread::spawn(move || {
            if wait.recv_timeout(limit) != Err(std::sync::mpsc::RecvTimeoutError::Timeout) {
                return;
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the watchdog's runtime builds");
            runtime.block_on(probe.report(&format!(
                "the test did not end within {limit:?} and its own deadline did not fire; ending the test process"
            )));
            std::process::exit(101);
        });
        done
    }
}

/// `wait`, or a panic with the state dump when it has not returned within
/// `WAIT_LIMIT`. Nothing the model awaits may wait longer than that.
async fn bounded<T>(probe: &Probe, what: &str, wait: impl Future<Output = T>) -> T {
    probe
        .waits
        .lock()
        .unwrap()
        .push((what.to_owned(), std::time::Instant::now()));
    match tokio::time::timeout(WAIT_LIMIT, wait).await {
        Ok(value) => {
            probe.waits.lock().unwrap().pop();
            value
        }
        Err(_) => panic!(
            "{}",
            probe
                .report(&format!("`{what}` did not return within {WAIT_LIMIT:?}"))
                .await
        ),
    }
}

struct World {
    dir: tempfile::TempDir,
    probe: Arc<Probe>,
    _storm: Option<LockStorm>,
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

/// Rounds a livelock is given before it is reported, and the least wall time
/// it is given as well: a loaded runner makes every round slow, and work a
/// run leaves to a spawned task is measured in time, not in rounds.
const QUIESCENCE_BOUND: usize = 400;
const QUIESCENCE_PATIENCE: Duration = Duration::from_secs(90);
/// How long the state must stay unchanged to count as settled. Work that a
/// finished run hands to a spawned task has no row to watch while it is
/// pending; on a loaded 4-core runner (eight tests, four workers each) that
/// task can wait tens of milliseconds for a thread.
const QUIESCENCE_FLOOR: Duration = Duration::from_millis(250);
/// The server's run cap in every model stack: what a 4-core machine resolves
/// on its own (`config::automatic_run_cap_for_cores`).
const MODEL_RUN_CAP: u32 = 2;
tokio::task_local! {
    /// The cap a test pins for the stacks it boots; `None` leaves the cap to
    /// the host (`max_concurrent_runs` unset).
    static PINNED_RUN_CAP: Option<u32>;
}

/// A test's own pin first, then `FORGE_MODEL_RUN_CAP`, then `MODEL_RUN_CAP`.
fn run_cap() -> Option<u32> {
    PINNED_RUN_CAP.try_with(|cap| *cap).unwrap_or_else(|_| {
        Some(env_number("FORGE_MODEL_RUN_CAP").map_or(MODEL_RUN_CAP, |cap| cap as u32))
    })
}

const BUSY_RETRIES: usize = 8;
const SETTLE_ROUNDS: usize = 20;
const INITIAL: [&str; 2] = ["backlog", "todo"];
const TERMINAL: [&str; 2] = ["done", "cancelled"];

// Model-only observations for invariant (l). Whether a Task is a
// lead-managed root is read inside the transaction that writes each `replan`
// charge and each planning entry over the `replan` bridge, so a later change
// of the assignment cannot rewrite what was true when the re-plan was decided.
//
// The product decides a re-plan once, at its charge (`charge_replan`), and
// the entry that carries the same receipt is that decision's second half:
// the owner's removal of the lead may land between the two commands, and the
// product keeps the bridge on purpose (services test
// `a_rejection_decided_as_a_replan_keeps_its_bridge_when_the_lead_is_removed`).
// So an entry is authorized by a lead at its own commit, or by the charge of
// its own receipt having been written under a lead. Every other charge or
// entry on a Task that is not a lead-managed root is a violation, and stays
// one after the lead is assigned again.
const MODEL_LEAD_ROOT: &str = "t.parent_task_id IS NULL AND EXISTS (SELECT 1 FROM task c WHERE c.parent_task_id = t.id AND c.deleted_at IS NULL) AND EXISTS (SELECT 1 FROM task_role_assignment a WHERE a.task_id = t.id AND a.role_name = 'planner' AND a.assignee_type = 'agent' AND a.assignee_id IS NOT NULL)";

async fn install_replan_audit(pool: &sqlx::SqlitePool) {
    sqlx::query("CREATE TABLE IF NOT EXISTS model_replan_authority (task_id TEXT NOT NULL, kind TEXT NOT NULL, receipt TEXT NOT NULL, eligible INTEGER NOT NULL)")
        .execute(pool).await.unwrap();
    let lead_root_now =
        format!("COALESCE((SELECT ({MODEL_LEAD_ROOT}) FROM task t WHERE t.id=NEW.task_id),0)");
    let charged_under_a_lead = "EXISTS(SELECT 1 FROM model_replan_authority a WHERE a.task_id=NEW.task_id AND a.kind='charge' AND a.eligible=1 AND a.receipt=json_extract(NEW.bridge_payload,'$.receipt'))";
    for (kind, table, filter, receipt, eligible) in [
        (
            "charge",
            "task_budget_charge",
            "NEW.kind = 'replan'",
            "NEW.step_id",
            lead_root_now.clone(),
        ),
        (
            "entry",
            "transition_log",
            "NEW.bridge_kind = 'replan'",
            "COALESCE(json_extract(NEW.bridge_payload,'$.receipt'), NEW.id)",
            format!("({lead_root_now} OR {charged_under_a_lead})"),
        ),
    ] {
        sqlx::raw_sql(&format!(
            "CREATE TRIGGER IF NOT EXISTS model_replan_{kind} AFTER INSERT ON {table}
             WHEN {filter} BEGIN INSERT INTO model_replan_authority(task_id,kind,receipt,eligible)
             VALUES(NEW.task_id, '{kind}', {receipt}, {eligible}); END"
        ))
        .execute(pool)
        .await
        .unwrap();
    }
}

/// The audit of (l) on rows written directly: what a product that re-plans
/// a root with no lead would write is caught as a charge alone, as an entry
/// alone, and as both; the history of an authorized re-plan is not.
#[tokio::test]
async fn replan_authority_audit_retains_history_and_catches_later_illegal_effects() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    install_replan_audit(&pool).await;
    let entry = |id: &str, receipt: &str| {
        format!("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload) VALUES('{id}','root','in_progress','planning','system:workflow','model','now','replan','{{\"receipt\":\"{receipt}\"}}');")
    };
    let charge = |receipt: &str| {
        format!("INSERT INTO task_budget_charge(task_id,kind,window_id,step_id) VALUES('root','replan','w','{receipt}');")
    };
    let flagged = || async {
        sqlx::query_scalar::<_, String>(
            "SELECT kind || ' ' || receipt FROM model_replan_authority WHERE eligible=0 ORDER BY rowid",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
    };
    sqlx::raw_sql(&format!("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p','now','now');
        INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('root','p','root','planning','now','now');
        INSERT INTO task(id,project_id,parent_task_id,title,status,created_at,updated_at) VALUES('child','p','root','child','todo','now','now');
        INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('a','root','planner','agent','lead','now','now');
        INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES('root','replan','w',2);
        {} {} {}
        DELETE FROM task_role_assignment WHERE id='a';
        {}",
        charge("with-lead"), entry("e1", "with-lead"), charge("decided-with-lead"),
        // The designed race: decided and charged under the lead, entered
        // after the owner removed it.
        entry("e2", "decided-with-lead")))
        .execute(&pool).await.unwrap();
    assert_eq!(
        flagged().await,
        Vec::<String>::new(),
        "an authorized re-plan stays authorized after the lead is removed"
    );
    // A product that re-plans a root with no lead: the charge alone, the
    // entry alone (no charge, and a receipt charged without a lead), both.
    sqlx::raw_sql(&format!(
        "{} {} {}",
        charge("no-lead"),
        entry("e3", "never-charged"),
        entry("e4", "no-lead")
    ))
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        flagged().await,
        ["charge no-lead", "entry never-charged", "entry no-lead"],
        "every effect decided without a lead is a violation"
    );
    // Assigning a lead again does not launder them.
    sqlx::raw_sql("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('b','root','planner','agent','lead','now','now');")
        .execute(&pool).await.unwrap();
    assert_eq!(flagged().await.len(), 3);
    pool.close().await;
}

impl World {
    async fn boot(dir: &Path, shared: &Arc<Shared>, probe: &Arc<Probe>) -> Live {
        *probe.database.lock().unwrap() = Some(dir.join("forge.db"));
        bounded(
            probe,
            "booting the server",
            Self::boot_unbounded(dir, shared, probe),
        )
        .await
    }

    async fn boot_unbounded(dir: &Path, shared: &Arc<Shared>, probe: &Arc<Probe>) -> Live {
        let url = format!("sqlite:{}?mode=rwc", dir.join("forge.db").display());
        let pool = db::create_sqlite_pool(&url).await.expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        install_replan_audit(&pool).await;
        // A fresh `SqliteDb` per boot: its in-memory schedule generation and
        // step-executor reference die with the process, as in a real crash.
        let db = Arc::new(db::SqliteDb::new(pool.clone()));
        let script = Arc::new(Script {
            alive: AtomicBool::new(true),
            waiting: Mutex::default(),
            shared: Arc::clone(shared),
            db: Arc::clone(&db),
            service: OnceLock::new(),
        });
        let mut registry = executors::AdapterRegistry::new();
        registry.register(Box::new(ScriptedAdapter(Arc::clone(&script))));
        *probe.script.lock().unwrap() = Some(Arc::clone(&script));
        let registry = Arc::new(registry);
        services::ensure_default_agents(db.as_ref(), &registry)
            .await
            .expect("default agents upsert");
        let event_bus = Arc::new(events::EventBus::new(1024));
        let workspaces = dir.join("workspaces");
        let mut config = config::ForgeConfig::with_data_dir(dir.join("data"));
        config.workspace.root = workspaces.clone();
        config.server.max_concurrent_runs = run_cap();
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
                workspaces,
            )))
            .with_workflows_dir(api::state::test_workflows_dir())
            .with_jwt_secret(api::state::test_jwt_secret())
            .with_bcrypt_cost(api::state::test_bcrypt_cost())
            .build(),
        );
        let _ = script.service.set(Arc::downgrade(&runtime.task_service));
        let state = AppState::from_runtime_arc(Arc::clone(&runtime), true);
        let app = build_router(state.clone(), dir.join("web"));
        let (stop, signal) = tokio::sync::watch::channel(false);
        let checks = Checks {
            stop,
            job: state
                .task_service
                .check_worker()
                .expect("the runtime composes the check worker")
                .start(&state.operator_status_service.periodic_workers(), signal),
        };
        Live {
            checks,
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
        let probe = Probe::new("model");
        let live = Self::boot(dir.path(), &shared, &probe).await;
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
            _storm: lock_storm(dir.path()),
            dir,
            probe,
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
        let mut old = self.live.take().expect("the server is up");
        old.script.alive.store(false, Ordering::SeqCst);
        old.script.waiting.lock().unwrap().clear();
        old.runtime.task_dispatcher.stop();
        old.checks.stop();
        // Whatever the dead process still had in flight can no longer write:
        // its check worker is gone before the pool closes, and the pool is
        // closed down to its last connection. `Pool::close` alone can leave
        // one open, and a connection of the dead process that still held the
        // write lock would make the new server wait out its busy timeout.
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut old.checks.job).await;
        if tokio::time::timeout(Duration::from_secs(10), db::close_sqlite_pool(&old.pool))
            .await
            .is_err()
        {
            eprintln!(
                "model: the crashed server still holds {} database connection(s)",
                old.pool.size()
            );
        }
        drop(old);

        let live = Self::boot(self.dir.path(), &self.shared, &self.probe).await;
        bounded(&self.probe, "starting after a crash", Self::start(&live)).await?;
        self.live = Some(live);
        Ok(())
    }

    /// What `RuntimeSupervisor::start` does before its workers run, call for
    /// call and in its order: crash recovery, the integration import, the
    /// dispatcher's startup reconciliation.
    async fn start(
        live: &Live,
    ) -> Result<services::integration_steps::import::ImportReport, String> {
        live.runtime
            .crash_recovery
            .run()
            .await
            .map_err(|error| format!("crash recovery failed: {error}"))?;
        let report = services::integration_steps::import::import_at_start(&live.state.db)
            .await
            .map_err(|error| format!("integration import failed: {error}"))?;
        live.runtime
            .task_dispatcher
            .startup_reconcile()
            .await
            .map_err(|error| format!("startup reconciliation failed: {error}"))?;
        Ok(report)
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
        let (status, bytes) = bounded(&self.probe, &format!("{method} {uri}"), async {
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
            (status, bytes)
        })
        .await;
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
            // The code, not the message: the message names the parent by
            // its random id, and the trace has to be the same on every run.
            return Ok(format!("refused {status} {}", task["code"]));
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
        if advance {
            // The retry time of a merge the queue worker parked.
            for id in &self.tasks {
                let waits: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM integration_attempt WHERE task_ref = ? AND current = 1
                     AND state = 'parked' AND julianday(available_at) > julianday('now'))",
                )
                .bind(id)
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if waits
                    && bounded(
                        &self.probe,
                        "expediting a parked attempt",
                        self.live().state.db.expedite_parked_integration_attempt(
                            id,
                            &db::now_rfc3339(),
                            false,
                        ),
                    )
                    .await
                    .map_err(|error| error.to_string())?
                {
                    moved += 1;
                }
            }
        }
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
            // A Task with a step still queued (always, right after a crash)
            // refuses a direct write as busy: run its steps and try again.
            // The deadline is re-read on the next pass, so a write the steps
            // made obsolete is harmless.
            let mut attempt = 0;
            loop {
                let now = db::now_rfc3339();
                let written = bounded(
                    &self.probe,
                    "moving a deadline",
                    db::TaskRepo::mutate_metadata(
                        &*self.live().state.db,
                        id,
                        None,
                        vec![db::TaskMetadataMutation::Set {
                            key: "deferred_dispatch".to_owned(),
                            value: deferral.clone(),
                        }],
                        &now,
                    ),
                )
                .await;
                match written {
                    Ok(_) => break,
                    Err(db::DbError::TaskBusy { .. }) if attempt < BUSY_RETRIES => {
                        attempt += 1;
                        self.drain(id)
                            .await
                            .map_err(|error| format!("draining a busy Task failed: {error}"))?;
                    }
                    Err(error) => return Err(format!("moving a deadline failed: {error}")),
                }
            }
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
            Action::CreateLeadRoot => {
                let root = self.tasks.len();
                self.create(None).await?;
                self.create(Some(root)).await?;
                self.create(Some(root)).await?;
                self.assign_lead(root).await
            }
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
            Action::AssignLead(task) => match pick(*task) {
                Some(task) => self.assign_lead(task).await,
                None => Ok("skipped".to_owned()),
            },
            Action::RemoveLead(task) => match pick(*task) {
                Some(task) => {
                    let (status, body) = self
                        .request(
                            Method::DELETE,
                            &format!("/api/v1/tasks/{}/roles/planner", self.tasks[task]),
                            None,
                        )
                        .await?;
                    Ok(if status.is_success() {
                        "lead removed".to_owned()
                    } else {
                        format!("refused {status} {}", body["code"])
                    })
                }
                None => Ok("skipped".to_owned()),
            },
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
            // A hooks step suspended on review-entry CI is not a pending
            // step, but the check worker still owes this Task a result.
            let awaiting: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status = 'suspended'",
            )
            .bind(id)
            .fetch_one(&self.live().pool)
            .await
            .map_err(|error| error.to_string())?;
            if awaiting > 0 {
                // A check queued behind a full machine is owed nothing until
                // a slot frees: it waits for a run the model can finish, like
                // a Task parked on capacity. With no such run, the slot it
                // waits for is held by nothing the model can end.
                let slot = self
                    .live()
                    .state
                    .db
                    .awaited_check_waits_for_slot(id)
                    .await
                    .map_err(|error| error.to_string())?;
                let held = (0..self.tasks.len()).any(|other| other != index && self.waiting(other));
                if !slot {
                    unowned.push(format!("task {index} waits for a check result"));
                } else if !held {
                    unowned.push(format!(
                        "task {index} waits for a check slot that no execution holds"
                    ));
                }
            }
            print.push_str(&format!("{index}:{status}@{version}+{pending}~{awaiting}["));
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

    /// The step worker's drain of one Task, under the model's hard deadline.
    async fn drain(&self, id: &str) -> Result<(), services::ServiceError> {
        bounded(
            &self.probe,
            &format!("draining the steps of Task {id}"),
            self.live().state.task_service.drain(id),
        )
        .await
        .map(|_| ())
    }

    /// One dispatcher pass, under the model's hard deadline.
    async fn dispatcher_pass(&self) -> Result<(), String> {
        bounded(
            &self.probe,
            "a dispatcher pass",
            self.live().runtime.task_dispatcher.check_once(),
        )
        .await
        .map(|_| ())
        .map_err(|error| format!("dispatcher pass failed: {error}"))
    }

    /// Invariant (d): run the step worker and the dispatcher until nothing
    /// changes. Not settling within the bound is a livelock.
    async fn quiesce(&mut self) -> Result<(), String> {
        let mut last = String::new();
        let mut stable = 0;
        let mut unowned = Vec::new();
        let mut history = Vec::new();
        let mut changed = std::time::Instant::now();
        let started = changed;
        for round in 0.. {
            if round >= QUIESCENCE_BOUND && started.elapsed() >= QUIESCENCE_PATIENCE {
                break;
            }
            self.sync_lead_tasks();
            self.move_deadlines(false).await?;
            for (index, id) in self.tasks.iter().enumerate() {
                self.drain(id).await.map_err(|error| {
                    format!("draining the steps of task {index} failed: {error}")
                })?;
            }
            self.dispatcher_pass().await?;
            tokio::time::sleep(Duration::from_millis(if round < 20 { 5 } else { 25 })).await;
            let frozen = self.move_deadlines(false).await?;
            let (mut print, running) =
                bounded(&self.probe, "reading the fingerprint", self.fingerprint()).await?;
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

    fn sync_lead_tasks(&mut self) {
        let mut indexed: Vec<_> = self
            .shared
            .index
            .lock()
            .unwrap()
            .iter()
            .map(|(id, index)| (*index, id.clone()))
            .collect();
        indexed.sort_by_key(|(index, _)| *index);
        self.tasks = indexed.into_iter().map(|(_, id)| id).collect();
    }

    /// The invariants that hold in every committed state.
    async fn check_committed(&mut self) -> Result<(), String> {
        if let Some(error) = self.shared.lead_operation_errors.lock().unwrap().first() {
            return Err(format!(
                "scripted lead operation failed through the native entry: {error}"
            ));
        }
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
        self.check_roots(false).await?;
        self.check_replans(false).await?;
        self.check_step_checks(false).await?;
        Ok(())
    }

    /// The step-check invariants of plan 3.10 stage 3 (`lead_check`).
    ///
    /// Real here: the no-lead rule. Every subtask the model finishes now
    /// passes through `lead_check`, and under a root without a lead it must
    /// never rest there (the gate is skipped in the entry's own step, and the
    /// dispatcher passes a subtask it finds there). With the skip target
    /// mutated back to "the first implementation state" this fails on the
    /// first finished subtask.
    ///
    /// (m) and (n) bind where a root has a lead: `Action::AssignLead` and
    /// the scripted lead exist, and `a_root_with_a_lead_plans_and_has_every_
    /// step_checked` runs every invariant on states with a live step check, a
    /// returned step and a charged `gate:lead_check`. Random sequences also
    /// create coordination roots, assign/remove leads and run their operations.
    /// The operation corpus exhausts the check budget and recovers the step.
    async fn check_step_checks(&self, quiescent: bool) -> Result<(), String> {
        let pool = &self.live().pool;
        let rows = |sql: &'static str| async move {
            sqlx::query_scalar::<_, String>(sql)
                .fetch_all(pool)
                .await
                .map_err(|error| error.to_string())
        };
        // (n) the returns of a step are bounded by the gate's limit.
        let overspent = rows(
            "SELECT task_id || ' spent ' || spent FROM task_budget
             WHERE kind = 'gate:lead_check' AND spent > 2",
        )
        .await?;
        if !overspent.is_empty() {
            return Err(format!(
                "(n) step checks returned past their limit: {overspent:?}"
            ));
        }
        // A step check is a planner run and nothing else: no other role ever
        // runs on a subtask in the gate.
        let foreign = rows(
            "SELECT c.id || ' runs ' || e.role FROM task c JOIN execution e ON e.task_id = c.id
             WHERE c.parent_task_id IS NOT NULL AND c.deleted_at IS NULL
               AND c.status = 'lead_check' AND e.status = 'running' AND e.role != 'planner'",
        )
        .await?;
        if !foreign.is_empty() {
            return Err(format!(
                "a subtask in the step check runs another role: {foreign:?}"
            ));
        }
        if !quiescent {
            return Ok(());
        }
        const LEAD: &str = "EXISTS (SELECT 1 FROM task_role_assignment a WHERE a.task_id = r.id AND a.role_name = 'planner' AND a.assignee_type = 'agent' AND a.assignee_id IS NOT NULL)";
        // No lead: a subtask never rests in the gate. A hold, a failure or a
        // wait on its root is a rest with an owner; anything else is not: a
        // clear condition, and equally a park that asks somebody to assign
        // the role (the form a broken skip takes: mutation-tested by removing
        // the gate's skip hook and the dispatcher's `gate_skip` fact).
        let resting: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT c.id || ' ' || COALESCE(c.condition_json, '') FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.deleted_at IS NULL AND c.status = 'lead_check'
               AND NOT {LEAD}
               AND r.status IN ('in_progress', 'merge_failed')
               AND COALESCE(json_extract(c.condition_json, '$.kind'), 'clear') != 'failed'
               AND COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') NOT IN ('held', 'failure')
               AND json_extract(r.condition_json, '$.kind') = 'clear'
               AND NOT EXISTS (SELECT 1 FROM project p WHERE p.id = c.project_id AND p.paused_at IS NOT NULL)"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !resting.is_empty() {
            return Err(format!(
                "subtasks of a root without a lead rest in the step check: {resting:?}"
            ));
        }
        // (m) under a lead, a subtask in the gate has its check running or a
        // typed park.
        let unowned: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT c.id FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.deleted_at IS NULL AND c.status = 'lead_check'
               AND {LEAD}
               AND json_extract(c.condition_json, '$.kind') = 'clear'
               AND NOT EXISTS (SELECT 1 FROM execution e WHERE e.task_id = c.id AND e.role = 'planner' AND e.status = 'running')"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !unowned.is_empty() {
            return Err(format!(
                "(m) subtasks in the step check under a lead with no check and no park: {unowned:?}"
            ));
        }
        Ok(())
    }

    /// The re-plan invariants of plan 3.10 stage 2.
    ///
    /// (l) in every committed state: only a coordination root with a lead
    /// ever spends `replan` or enters planning over the `replan` bridge;
    /// what is spent is within the limit, each charge has one receipt, no
    /// two planning entries share a receipt, and at most one charge is ahead
    /// of its entry (the crash window between the charge and the entry).
    /// (o) at quiescence: a root in aggregate review, merging or a terminal
    /// state has no subtask left to run, so a re-plan can add work only
    /// before the root returns to implementation.
    ///
    /// Both halves of (l), and (o), bind on the operation corpus: a real
    /// re-plan charges its receipt, and the lead creates, recovers and cancels
    /// subtasks before the root returns to implementation and aggregate review.
    /// Random sequences additionally assign and remove the lead.
    async fn check_replans(&self, quiescent: bool) -> Result<(), String> {
        let pool = &self.live().pool;
        let rows = |sql: &'static str| async move {
            sqlx::query_scalar::<_, String>(sql)
                .fetch_all(pool)
                .await
                .map_err(|error| error.to_string())
        };
        let strangers = rows(
            "SELECT task_id || ' ' || kind || ' ' || receipt FROM model_replan_authority WHERE eligible=0"
        ).await?;
        if !strangers.is_empty() {
            return Err(format!(
                "(l) Tasks that are not lead-managed roots re-planned: {strangers:?}"
            ));
        }
        let uncharged = rows(
            "SELECT l.task_id || ' ' || l.id FROM transition_log l WHERE l.bridge_kind='replan'
             AND NOT EXISTS(SELECT 1 FROM task_budget_charge c WHERE c.task_id=l.task_id AND c.kind='replan'
                            AND c.step_id=json_extract(l.bridge_payload,'$.receipt'))"
        ).await?;
        if !uncharged.is_empty() {
            return Err(format!(
                "(l) re-plan entries lack their charge: {uncharged:?}"
            ));
        }
        let overspent = rows(
            "SELECT b.task_id || ' spent ' || b.spent FROM task_budget b WHERE b.kind = 'replan'
               AND (b.spent > 5
                    OR b.spent != (SELECT COUNT(*) FROM task_budget_charge c
                                   WHERE c.task_id = b.task_id AND c.kind = 'replan' AND c.window_id = b.window_id)
                    OR (SELECT COUNT(*) FROM task_budget_charge c
                        WHERE c.task_id = b.task_id AND c.kind = 'replan' AND c.window_id = b.window_id
                          AND NOT EXISTS (SELECT 1 FROM transition_log l WHERE l.task_id = b.task_id AND l.bridge_kind = 'replan'
                                          AND json_extract(l.bridge_payload, '$.receipt') = c.step_id)) > 1)",
        )
        .await?;
        if !overspent.is_empty() {
            return Err(format!(
                "(l) re-plan charges disagree with their receipts or entries: {overspent:?}"
            ));
        }
        let repeated = rows(
            "SELECT l.task_id || ' ' || json_extract(l.bridge_payload, '$.receipt') FROM transition_log l
             WHERE l.bridge_kind = 'replan' GROUP BY l.task_id, json_extract(l.bridge_payload, '$.receipt')
             HAVING COUNT(*) > 1",
        )
        .await?;
        if !repeated.is_empty() {
            return Err(format!(
                "(l) one re-plan trigger entered planning twice: {repeated:?}"
            ));
        }
        if !quiescent {
            return Ok(());
        }
        let open = rows(
            "SELECT r.id || ' in ' || r.status || ' with ' || c.id || ' in ' || c.status
             FROM task r JOIN task c ON c.parent_task_id = r.id
             WHERE r.parent_task_id IS NULL AND r.deleted_at IS NULL AND c.deleted_at IS NULL
               AND r.status IN ('review', 'merging', 'done', 'cancelled')
               AND c.status NOT IN ('done', 'cancelled')",
        )
        .await?;
        if !open.is_empty() {
            return Err(format!(
                "(o) roots past implementation with a subtask left to run: {open:?}"
            ));
        }
        Ok(())
    }

    /// The coordination-root invariants of plan 3.10 stage 1, for roots with
    /// and without a planner (the lead).
    ///
    /// (k) holds in every committed state. (g), (h), (i) and (j) are checked
    /// where nothing is left for the dispatcher to do: a root restored from
    /// an older release may hold a running subtask in `todo` until its first
    /// pass, and a stopping execution is still `running` until it reports.
    ///
    /// What is real and what is vacuous here. (g) is real: with the product
    /// mutated back to the old rule (a subtask of a `todo` root dispatches
    /// and the root is not kicked off) `a_coordination_root_merges_through_
    /// the_queue` fails on it. The no-lead halves of (h) and (k) are real for
    /// the same roots. (i), (j) and the lead halves of (h) and (k) bind where
    /// a root has a lead: `a_root_with_a_lead_plans_and_has_every_step_
    /// checked` (`Action::AssignLead`, the scripted lead) checks them on a
    /// root that rests in `planning` with its lead's run live. They also bind
    /// during the operation corpus's re-plan and in random sequences, which
    /// create coordination roots and assign/remove leads.
    async fn check_roots(&self, quiescent: bool) -> Result<(), String> {
        let pool = &self.live().pool;
        let rows = |sql: &'static str| async move {
            sqlx::query_scalar::<_, String>(sql)
                .fetch_all(pool)
                .await
                .map_err(|error| error.to_string())
        };
        const ROOT: &str = "r.parent_task_id IS NULL AND r.deleted_at IS NULL AND EXISTS (SELECT 1 FROM task c WHERE c.parent_task_id = r.id AND c.deleted_at IS NULL)";
        const LEAD: &str = "EXISTS (SELECT 1 FROM task_role_assignment a WHERE a.task_id = r.id AND a.role_name = 'planner' AND a.assignee_type = 'agent' AND a.assignee_id IS NOT NULL)";
        // (k) a root without a planner never runs one.
        let ran: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT r.id FROM task r WHERE {ROOT} AND NOT {LEAD} AND EXISTS (SELECT 1 FROM execution e WHERE e.task_id = r.id AND e.role = 'planner' AND e.status = 'running')"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !ran.is_empty() {
            return Err(format!("(k) roots without a planner run one: {ran:?}"));
        }
        if !quiescent {
            return Ok(());
        }
        // (k) ... and never rests in `planning`.
        let resting: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT r.id FROM task r WHERE {ROOT} AND NOT {LEAD} AND r.status = 'planning'
               AND json_extract(r.condition_json, '$.kind') = 'clear'"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !resting.is_empty() {
            return Err(format!(
                "(k) roots without a planner rest in planning: {resting:?}"
            ));
        }
        // (g) no subtask runs while its root is not in an implementation state.
        let early = rows(
            "SELECT c.id || ' under ' || r.status FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.status NOT IN ('in_progress', 'merge_failed')
               AND EXISTS (SELECT 1 FROM execution e WHERE e.task_id = c.id AND e.status = 'running')",
        )
        .await?;
        if !early.is_empty() {
            return Err(format!(
                "(g) subtasks run while their root does not implement: {early:?}"
            ));
        }
        // (h) one running execution between a root and its subtasks: the
        // planner on the root counts.
        let crowded = rows(
            "SELECT r.id FROM task r WHERE r.parent_task_id IS NULL AND r.deleted_at IS NULL
               AND (SELECT COUNT(*) FROM execution e JOIN task t ON t.id = e.task_id
                    WHERE e.status = 'running' AND (t.id = r.id OR t.parent_task_id = r.id)
                      AND EXISTS (SELECT 1 FROM task c WHERE c.parent_task_id = r.id)) > 1",
        )
        .await?;
        if !crowded.is_empty() {
            return Err(format!(
                "(h) roots with more than one running execution in their tree: {crowded:?}"
            ));
        }
        // (i) a root that plans for its lead is never parked on its children.
        let stranded: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT r.id FROM task r WHERE {ROOT} AND {LEAD} AND r.status = 'planning'
               AND json_extract(r.condition_json, '$.primary.kind') = 'children'"
        ))
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
        if !stranded.is_empty() {
            return Err(format!(
                "(i) planning roots parked on their children: {stranded:?}"
            ));
        }
        // (j) a subtask of a planning root is held by a typed park.
        let unheld = rows(
            "SELECT c.id || ' ' || COALESCE(c.condition_json, '') FROM task c JOIN task r ON r.id = c.parent_task_id
             WHERE c.deleted_at IS NULL AND r.deleted_at IS NULL AND r.status = 'planning'
               AND c.status IN ('todo', 'in_progress')
               AND NOT (COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') = 'parent'
                        AND json_extract(c.condition_json, '$.primary.cause') IN ('planning', 'held', 'blocked'))
               AND COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') NOT IN ('held', 'failure', 'budget_exhausted', 'agent_timeout')
               AND NOT (EXISTS (SELECT 1 FROM project p WHERE p.id = c.project_id AND p.paused_at IS NOT NULL)
                        AND COALESCE(json_extract(c.condition_json, '$.primary.kind'), '') = 'project_paused')",
        )
        .await?;
        if !unheld.is_empty() {
            return Err(format!(
                "(j) subtasks of a planning root without a parent park: {unheld:?}"
            ));
        }
        Ok(())
    }

    /// The invariants of a quiescent state. Returns one line per Task.
    async fn check_quiescent(&mut self) -> Result<String, String> {
        self.check_committed().await?;
        self.check_roots(true).await?;
        self.check_replans(true).await?;
        self.check_step_checks(true).await?;
        // (a) the dispatcher's own "owned or parked" assertion.
        bounded(
            &self.probe,
            "the dispatcher's sweep and assertion",
            self.live().runtime.task_dispatcher.sweep_and_assert(),
        )
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
            // Project pause owns no Task action: its exit is Resume Project.
            // Require the live Project fact, so a stale pause park is a wedge.
            (_, "project_paused") => {
                let paused: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM project WHERE id=? AND paused_at IS NOT NULL)",
                )
                .bind(&self.project_id)
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if paused {
                    Ok(())
                } else {
                    wedge("the Project it waits for is not paused")
                }
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
            // A subtask its parent does not let run: the exit is on the
            // parent, which must exist, be open and (when it is held) offer
            // its release.
            (_, "parent") => {
                let parent = task["condition"]["primary"]["parent_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                let cause = task["condition"]["primary"]["cause"]
                    .as_str()
                    .unwrap_or_default();
                let Some(parent_index) = self.tasks.iter().position(|id| *id == parent) else {
                    return wedge("the parent it names is not a Task of this Project");
                };
                if !unsettled(vec![parent]).await? {
                    return wedge("the parent it waits for is settled");
                }
                if cause == "held" {
                    let (_, offers) = self.offers(parent_index).await?;
                    if !offers
                        .iter()
                        .any(|offer| offer["action"]["verb"] == "release")
                    {
                        return wedge("its held parent offers no release");
                    }
                }
                if verbs.contains(&"start") {
                    return wedge("`start` is offered although the parent does not let it run");
                }
                Ok(())
            }
            // The Agent that would run the Task is paused or unreachable:
            // the exit is on the Agent, which the model can resume.
            (_, "agent") => {
                let agent = task["condition"]["primary"]["agent_id"]
                    .as_str()
                    .unwrap_or_default();
                if agent != self.coder_id && agent != self.reviewer_id {
                    return wedge("the Agent it names is not one of this Project's");
                }
                if self.agent_paused || self.project_paused {
                    Ok(())
                } else {
                    wedge("the Agent it waits for is not paused")
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
            // Review-entry CI queued behind a full machine: the exit is a run
            // ending, which frees the slot the check worker then gives it.
            (_, "check") if task["condition"]["primary"]["wait"]["phase"] == "slot" => {
                if (0..self.tasks.len()).any(|other| other != index && self.waiting(other)) {
                    Ok(())
                } else {
                    wedge("no execution holds the slot its check waits for")
                }
            }
            // In the merge queue (`waiting`, `owned`, `applied`): the queue
            // worker owns the Task and no owner action is needed, so none
            // being offered is not a wedge. It is one when nothing can move
            // the queue: its head check waits for a machine slot no run
            // holds, or the queue has no live head and cannot be claimed.
            (_, "integration")
                if matches!(
                    task["condition"]["primary"]["reason"]["kind"].as_str(),
                    Some("waiting" | "owned" | "applied")
                ) =>
            {
                let slot_wait = task["condition"]["additional"]
                    .as_array()
                    .is_some_and(|reasons| {
                        reasons
                            .iter()
                            .any(|r| r["kind"] == "check" && r["wait"]["phase"] == "slot")
                    });
                if slot_wait {
                    return if (0..self.tasks.len())
                        .any(|other| other != index && self.waiting(other))
                    {
                        Ok(())
                    } else {
                        wedge("no execution holds the slot its head check waits for")
                    };
                }
                let moving: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM integration_attempt a JOIN integration_queue q ON q.id=a.queue_id \
                     WHERE a.task_ref = ? AND a.current = 1 \
                       AND ((q.head_attempt_id IS NOT NULL AND julianday(q.lease_until) > julianday('now')) \
                         OR (q.head_attempt_id IS NULL AND q.state = 'open' AND a.state = 'queued')))",
                )
                .bind(&self.tasks[index])
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                // A paused Project stops the queue for its Tasks; the exit
                // is the resume, which the model can do.
                if moving || self.project_paused {
                    Ok(())
                } else {
                    wedge("the merge queue that owns it has no live head and cannot be claimed")
                }
            }
            // An accepted action refused admission by an unfinished
            // dependency waits for that Task; `cancel` stays on offer.
            (_, "dispatch_refusal")
                if unsettled(
                    sqlx::query_scalar(
                        "SELECT depends_on_id FROM task_dependency WHERE task_id = ?",
                    )
                    .bind(&self.tasks[index])
                    .fetch_all(&self.live().pool)
                    .await
                    .map_err(|error| error.to_string())?,
                )
                .await?
                    && offered(&["cancel"]) =>
            {
                Ok(())
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
        let mut reopened: std::collections::HashSet<usize> = Default::default();
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
                    || matches!(
                        reason,
                        "dependencies" | "children" | "capacity" | "parent" | "agent"
                    )
                    || (reason == "check"
                        && task["condition"]["primary"]["wait"]["phase"] == "slot")
                {
                    continue;
                }
                // The owner's ways forward, least intrusive first. When one
                // was accepted and changed nothing, the next round takes the
                // next one: a park is a wedge only when none of them helps.
                let (version, offers) = self.offers(index).await?;
                // Reopening a parent for corrective work is taken once per
                // parent: it changes state every time (the parent leaves
                // review, held, and comes back), so the rotation below would
                // never get past it to the owner's override.
                let forward: Vec<Value> = ["release", "retry", "send_back", "restart", "approve"]
                    .iter()
                    .filter_map(|verb| offers.iter().find(|offer| offer["action"]["verb"] == *verb))
                    .filter(|offer| {
                        offer["reason"] != "root_review_reopen" || !reopened.contains(&index)
                    })
                    // Corrective work in this harness is supplied through
                    // subtasks. Prefer that offered route to claiming that a
                    // manual edit of the root's checkout has been completed.
                    .cloned()
                    .collect();
                if !forward.is_empty() {
                    let turn = attempts.entry(index).or_insert(0);
                    let offer = &forward[*turn % forward.len()];
                    *turn += 1;
                    let verb = self.apply_offer(index, version, offer).await?;
                    taken.push(format!("{verb}({index})"));
                    moves += 1;
                    if offer["reason"] == "root_review_reopen" {
                        // What the reopening is for: the parent must now
                        // accept a corrective subtask.
                        reopened.insert(index);
                        let created = self.create(Some(index)).await?;
                        if created.starts_with("refused") {
                            return Err(format!(
                                "(b) task {index} was sent back for corrective work and refuses a subtask: {created}"
                            ));
                        }
                        taken.push(format!("corrective child of {index}"));
                    }
                }
            }
            if open == 0 {
                self.trace.push(format!("settled: {last}"));
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
    let operations = world.shared.lead_operations.lock().unwrap().clone();
    if !operations.is_empty() {
        world.trace.push(format!("lead operations: {operations:?}"));
    }
    // Leave nothing running behind the temporary directory.
    if let Some(live) = world.live.take() {
        live.script.alive.store(false, Ordering::SeqCst);
        live.script.waiting.lock().unwrap().clear();
        live.runtime.task_dispatcher.stop();
        live.checks.stop();
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
    let mut index = if std::env::var("FORGE_MODEL_NO_SHRINK").is_ok() {
        0
    } else {
        sequence.len()
    };
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
    // FNV-1a over action observations and the final settlement. Intermediate
    // settlement rounds observe asynchronous slot/check timing, not model
    // actions; retain them in the diagnostic trace without hashing them.
    trace
        .iter()
        .filter(|line| !line.starts_with("settle "))
        .flat_map(|line| line.bytes().chain(*b"\n"))
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

#[test]
fn settlement_scheduling_does_not_change_the_action_trace_digest() {
    let base = vec![
        "0 Create -> task running".to_owned(),
        "pauses lifted: task running".to_owned(),
    ];
    let mut isolated = base.clone();
    isolated.extend(
        [
            "settle 0 []: task review/check",
            "settle 1 []: task done",
            "settled: task done",
        ]
        .map(str::to_owned),
    );
    let mut concurrent = base;
    concurrent.extend(["settle 0 []: task done", "settled: task done"].map(str::to_owned));
    assert_eq!(digest(&isolated), digest(&concurrent));
    concurrent[0] = "0 Create -> task parked".to_owned();
    assert_ne!(digest(&isolated), digest(&concurrent));
    concurrent[0] = isolated[0].clone();
    *concurrent.last_mut().unwrap() = "settled: task cancelled".to_owned();
    assert_ne!(digest(&isolated), digest(&concurrent));
}

fn env_number(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

async fn run_cases(cases: Vec<(String, Vec<Step>)>) {
    run_all(cases, false).await;
}

/// `FORGE_MODEL_LOG=<level>` prints the stack's own log at that level
/// (`warn`, `info`, `debug`) with timestamps: what waited for the database,
/// who held it, what a worker gave up on.
fn log_to_stderr() {
    if let Some(level) = std::env::var("FORGE_MODEL_LOG")
        .ok()
        .and_then(|level| level.parse::<tracing::Level>().ok())
    {
        let _ = tracing_subscriber::fmt()
            .with_max_level(level)
            .with_writer(std::io::stderr)
            .try_init();
    }
}

async fn run_all(mut cases: Vec<(String, Vec<Step>)>, avoid: bool) {
    log_to_stderr();
    // `FORGE_MODEL_CASE=<text>` runs only the cases whose name contains it.
    if let Ok(only) = std::env::var("FORGE_MODEL_CASE") {
        cases.retain(|(name, _)| name.contains(&only));
    }
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
const FIXED_SEEDS: [u64; 16] = [
    1, 2, 3, 5, 8, 909358760, 213412165, 974622785, 637240885, 277839211, 580812352, 610875980,
    207044633, 749274245, 263753536, 625967944,
];

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

/// The sequences the extra caps walk: every Task lifecycle shape (plain
/// success, a failed run retried, a review sent back, two Tasks for one slot,
/// a crash in flight) and two generated seeds. Fewer than the default cap's
/// set, to keep the target's runtime in bounds.
fn cap_matrix_cases() -> Vec<(String, Vec<Step>)> {
    use Action::{Claim, Create, Finish, Verdict};
    let mut cases = vec![
        (
            "one Task runs, is checked, reviewed and merged".to_owned(),
            vec![step(Create), step(Finish(0, Outcome::Success))],
        ),
        (
            "a failed run is retried".to_owned(),
            vec![step(Create), step(Finish(0, Outcome::Fail))],
        ),
        (
            "a usage limit is waited out".to_owned(),
            vec![
                step(Create),
                step(Claim(0)),
                step(Finish(0, Outcome::UsageLimit)),
            ],
        ),
        (
            "review sends the Task back".to_owned(),
            vec![
                step(Create),
                step(Verdict(0, false)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        (
            "three Tasks share the slots".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Create),
                step(Finish(0, Outcome::Success)),
                step(Finish(1, Outcome::Fail)),
            ],
        ),
        (
            "crash with a run in flight".to_owned(),
            vec![step(Create), crash_after(Create)],
        ),
    ];
    cases.extend([3, 5].map(|seed| (format!("FORGE_MODEL_SEED={seed}"), generate(seed, 16))));
    cases
}

/// A machine with `max_concurrent_runs: 1`. Before the fix a single Task
/// parked on a machine slot that its own running execution held
/// (`placement::machine_precheck::task_blocked` counted it), and nothing but
/// another Task's dispatch retired that wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_stay_live_at_run_cap_1() {
    PINNED_RUN_CAP
        .scope(Some(1), run_cases(cap_matrix_cases()))
        .await;
}

/// With one slot the whole lifecycle still happens, each stage in that slot:
/// the coder run, the review-entry check, the reviewer run and the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_slot_carries_a_task_from_run_to_merge() {
    PINNED_RUN_CAP
        .scope(Some(1), async {
            let mut world = World::new().await;
            let outcome: Result<(), String> = async {
                for action in [Action::Create, Action::Finish(0, Outcome::Success)] {
                    let applied = world.apply(&action).await?;
                    world.quiesce().await?;
                    let state = world.check_quiescent().await?;
                    world
                        .trace
                        .push(format!("{action:?} -> {applied}: {state}"));
                }
                world.drive_to_settlement().await?;
                let task = world.task(0).await?;
                if task["status"] != "done" {
                    return Err(format!("the Task ended in {}", task["status"]));
                }
                let pool = &world.live.as_ref().expect("the stack is up").pool;
                let checks: Vec<(String,)> = sqlx::query_as("SELECT state FROM check_run")
                    .fetch_all(pool)
                    .await
                    .map_err(|error| error.to_string())?;
                if checks.is_empty() || checks.iter().any(|(state,)| state != "succeeded") {
                    return Err(format!("review-entry check runs: {checks:?}"));
                }
                let runs: Vec<(String, String)> =
                    sqlx::query_as("SELECT role, status FROM execution ORDER BY created_at, rowid")
                        .fetch_all(pool)
                        .await
                        .map_err(|error| error.to_string())?;
                for role in ["coder", "reviewer"] {
                    if !runs
                        .iter()
                        .any(|(ran, status)| ran == role && status == "completed")
                    {
                        return Err(format!("no completed {role} run in {runs:?}"));
                    }
                }
                Ok(())
            }
            .await;
            if let Err(violation) = outcome {
                panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
            }
        })
        .await;
}

/// How a pause lands on a Task whose review-entry check is requested and not
/// yet finished (its hooks step is suspended).
#[derive(Clone, Copy, PartialEq, Debug)]
enum PauseKind {
    Project,
    /// Both of the Project's Agents: the reviewer is the check's follow-up.
    Agents,
    /// The Task itself: `hold` is the owner's Task-level pause.
    TaskHold,
}

impl World {
    /// The states of the check runs the Task asked for, oldest first.
    /// The states of the check RUNS a Task's consumers were answered by, one
    /// entry per run. Two consumers of one run (the review entry and the
    /// queue head that reuses its passed result) are one run.
    async fn check_runs(&self, index: usize) -> Result<Vec<String>, String> {
        sqlx::query_scalar(
            "SELECT r.state FROM check_run r
             WHERE r.id IN (SELECT c.run_id FROM check_consumer c WHERE c.task_id = ?)
             ORDER BY r.created_at, r.rowid",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())
    }

    /// The queue head of an unchanged commit and target is a recorded
    /// consumer of the review entry's passed run: it never has a run of its
    /// own, never a delivery that is applied a second time, and there is at
    /// most one of it per merging entry.
    async fn head_consumers_reuse_the_entry_run(&self, index: usize) -> Result<i64, String> {
        /// Origin, run, result, applied at.
        type Consumer = (String, Option<String>, Option<String>, Option<String>);
        let rows: Vec<Consumer> = sqlx::query_as(
            "SELECT origin, run_id, result_id, applied_at FROM check_consumer
             WHERE task_id = ? ORDER BY created_at, rowid",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let passed: Vec<(String, String)> = sqlx::query_as(
            "SELECT c.run_id, c.result_id FROM check_consumer c
             JOIN check_result r ON r.id = c.result_id
             WHERE c.task_id = ? AND c.origin = 'entry' AND r.outcome = 'pass'",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let mut heads = 0;
        for (origin, run, result, applied) in &rows {
            if origin == "entry" {
                continue;
            }
            heads += 1;
            let reused = matches!((run, result), (Some(run), Some(result))
                if passed.iter().any(|(r, s)| r == run && s == result));
            // A reuse is created answered: the `settle` step that records it
            // applies it, and no delivery step is spent on it.
            if origin != "integration" || !reused || applied.is_none() {
                return Err(format!(
                    "a consumer beside the review entry is not a reuse of its passed run: {rows:?}"
                ));
            }
        }
        Ok(heads)
    }

    async fn pause_agents(&mut self, pause: bool) -> Result<(), String> {
        let verb = if pause { "pause" } else { "resume" };
        for agent in [self.coder_id.clone(), self.reviewer_id.clone()] {
            let uri = format!("/api/v1/agents/{agent}/{verb}");
            let (status, body) = self.request(Method::POST, &uri, None).await?;
            if !status.is_success() {
                return Err(format!("agent {verb} was refused: {status} {body}"));
            }
        }
        Ok(())
    }
}

/// A pause while the Task's hooks step is suspended on its review-entry
/// check (here: queued behind a full machine). The check is machine work
/// already asked for:
/// - **Project pause, Agent pause**: the check runs to its end and its result
///   is applied; the pause only holds what comes after it (the reviewer run).
///   Resuming continues from the applied result: the check is not run again.
/// - **Task hold**: not on offer in this state. A Task waiting for its entry
///   check offers `cancel` only (which abandons the wait and stops the run);
///   there is no Task-level pause to resume from, and the check finishes.
async fn pause_while_the_entry_check_is_pending(
    kind: PauseKind,
    keep_one_slot: bool,
) -> Result<Vec<String>, String> {
    // Pinned: the case starts with two Tasks running side by side.
    PINNED_RUN_CAP
        .scope(
            Some(MODEL_RUN_CAP),
            pause_with_two_slots_then_one(kind, keep_one_slot),
        )
        .await
}

async fn pause_with_two_slots_then_one(
    kind: PauseKind,
    keep_one_slot: bool,
) -> Result<Vec<String>, String> {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        // Two Tasks run. The machine then has one slot, which the second
        // Task's run holds: the first Task's entry check is requested and
        // waits for it, its hooks step suspended.
        for action in [Action::Create, Action::Create] {
            world.apply(&action).await?;
            world.quiesce().await?;
        }
        let cap = &world.live().state.db.server_run_cap;
        cap.set(Some(1), 1, &cap.embedded_machine_id());
        world.apply(&Action::Finish(0, Outcome::Success)).await?;
        world.quiesce().await?;
        let task = world.task(0).await?;
        if task["status"] != "review" || world.check_runs(0).await? != ["queued"] {
            return Err(format!(
                "expected a Task in review with one queued check, got {} {:?}",
                task["status"],
                world.check_runs(0).await?
            ));
        }
        match kind {
            PauseKind::Project => {
                world.apply(&Action::PauseProject).await?;
            }
            PauseKind::Agents => world.pause_agents(true).await?,
            PauseKind::TaskHold => {
                let (_, offers) = world.offers(0).await?;
                let verbs: Vec<&str> = offers
                    .iter()
                    .filter_map(|offer| offer["action"]["verb"].as_str())
                    .collect();
                if verbs != ["cancel"] {
                    return Err(format!(
                        "a Task waiting for its entry check offers {verbs:?}, expected only cancel"
                    ));
                }
            }
        }
        world.quiesce().await?;
        // The slot frees: the check that was asked for before the pause runs.
        world.apply(&Action::Finish(1, Outcome::Success)).await?;
        let settled = tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                world.quiesce().await?;
                let runs = world.check_runs(0).await?;
                if runs
                    .iter()
                    .all(|state| ["succeeded", "failed", "cancelled"].contains(&state.as_str()))
                {
                    return Ok::<_, String>(runs);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| "the check did not finish under the pause".to_owned())??;
        world.quiesce().await?;
        let paused = world.task(0).await?;
        world.trace.push(format!(
            "{kind:?} paused: checks {settled:?}, task {} {}",
            paused["status"], paused["condition"]
        ));
        let reviewer_runs: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM execution WHERE role = 'reviewer' AND task_id = ?",
        )
        .bind(&world.tasks[0])
        .fetch_one(&world.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        match kind {
            PauseKind::Project | PauseKind::Agents => {
                if settled != ["succeeded"] {
                    return Err(format!("the check did not run to its end: {settled:?}"));
                }
                let applied: i64 = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin='entry' AND applied_at IS NOT NULL",
                )
                .bind(&world.tasks[0])
                .fetch_one(&world.live().pool)
                .await
                .map_err(|error| error.to_string())?;
                if applied != 1 {
                    return Err("the finished check's result was not applied".to_owned());
                }
                if paused["status"] != "review" || reviewer_runs != 0 {
                    return Err(format!(
                        "the pause did not hold the reviewer run: {} with {reviewer_runs} reviewer run(s)",
                        paused["status"]
                    ));
                }
            }
            PauseKind::TaskHold => {
                if settled != ["succeeded"] {
                    return Err(format!("the check did not run to its end: {settled:?}"));
                }
            }
        }
        if !keep_one_slot {
            // The pause is what this case is about. With one slot, the two
            // Tasks' follow-up runs would also race for it after the resume
            // (see the ignored case at the end of this file).
            let cap = &world.live().state.db.server_run_cap;
            cap.set(Some(MODEL_RUN_CAP), MODEL_RUN_CAP, &cap.embedded_machine_id());
        }
        match kind {
            PauseKind::Project => {
                world.apply(&Action::ResumeProject).await?;
            }
            PauseKind::Agents => world.pause_agents(false).await?,
            PauseKind::TaskHold => {}
        }
        world.quiesce().await?;
        world.drive_to_settlement().await?;
        for index in 0..2 {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!("task {index} ended in {}", task["status"]));
            }
        }
        let runs = world.check_runs(0).await?;
        let heads = world.head_consumers_reuse_the_entry_run(0).await?;
        if runs != ["succeeded"] || heads > 1 {
            return Err(format!(
                "check runs after resume: {runs:?} with {heads} head reuse(s), expected one run"
            ));
        }
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Ok(world.trace),
        Err(violation) => Err(format!("{violation}\ntrace:\n{}", world.trace.join("\n"))),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_project_pause_lets_the_pending_entry_check_finish_and_resume_does_not_repeat_it() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::Project, false).await
    {
        panic!("{violation}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_agent_pause_lets_the_pending_entry_check_finish_and_resume_does_not_repeat_it() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::Agents, false).await {
        panic!("{violation}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_waiting_for_its_pending_entry_check_offers_cancel_and_no_hold() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::TaskHold, false).await
    {
        panic!("{violation}");
    }
}

/// The other state of that wait: the machine had a slot and the entry check
/// runs. The Task waits for its check all the same and offers the same, also
/// when its reviewer or its Project is paused meanwhile: the check wait is
/// what parks the Task, so no pause puts a `hold` on offer beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_whose_entry_check_runs_offers_cancel_and_no_hold() {
    for pause in [None, Some(PauseKind::Agents), Some(PauseKind::Project)] {
        let mut world = World::new().await;
        let outcome: Result<(), String> = async {
            world.apply(&Action::Create).await?;
            world.quiesce().await?;
            // A check that runs until this test has looked at it: it ends
            // when the gate file appears, not after a time the look has to
            // fit into. (It also ends when the model directory is removed,
            // so a failed case leaves no process behind.)
            let gate = world.dir.path().join("entry-check-may-end");
            sqlx::query("UPDATE task SET task_state_config=json_set(task_state_config,'$.review.ci_steps',json_array(?)) WHERE id=?")
                .bind(format!(
                    "until [ -e '{gate}' ] || [ ! -d '{dir}' ]; do sleep 0.05; done; test -f {file}",
                    gate = gate.display(),
                    dir = world.dir.path().display(),
                    file = deliverable(0)
                ))
                .bind(&world.tasks[0])
                .execute(&world.live().pool)
                .await
                .map_err(|error| error.to_string())?;
            world.apply(&Action::Finish(0, Outcome::Success)).await?;
            // Not `quiesce`: that waits for the check to end. The Task's
            // steps run beside this look.
            let drain = |world: &World| {
                let service = world.live().state.task_service.clone();
                let id = world.tasks[0].clone();
                tokio::spawn(async move { service.drain(&id).await.map(|_| ()) })
            };
            let mut steps = drain(&world);
            let mut looked = 0;
            loop {
                if steps.is_finished() {
                    steps = drain(&world);
                }
                let task = world.task(0).await?;
                let runs = world.check_runs(0).await?;
                if task["status"] == "review" && runs == ["running"] {
                    break;
                }
                looked += 1;
                if looked > 400 || runs.iter().any(|state| state != "queued" && state != "running")
                {
                    return Err(format!(
                        "expected a Task in review with one running check, got {} {runs:?} {}",
                        task["status"], task["condition"]
                    ));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            match pause {
                Some(PauseKind::Project) => {
                    world.apply(&Action::PauseProject).await?;
                }
                Some(PauseKind::Agents) => world.pause_agents(true).await?,
                _ => {}
            }
            world
                .live()
                .runtime
                .task_dispatcher
                .check_once()
                .await
                .map_err(|error| error.to_string())?;
            let task = world.task(0).await?;
            let (_, offers) = world.offers(0).await?;
            let verbs: Vec<&str> = offers
                .iter()
                .filter_map(|offer| offer["action"]["verb"].as_str())
                .collect();
            world.trace.push(format!(
                "entry check running, pause {pause:?}: offers {verbs:?}, condition {}",
                task["condition"]
            ));
            if world.check_runs(0).await? != ["running"] {
                return Err("the check ended before the offers were read".to_owned());
            }
            if verbs != ["cancel"] {
                return Err(format!(
                    "a Task whose entry check runs (pause {pause:?}) offers {verbs:?}, expected only cancel"
                ));
            }
            std::fs::write(&gate, b"").map_err(|error| error.to_string())?;
            let _ = steps.await;
            // `drive_to_settlement` resumes the Project.
            if pause == Some(PauseKind::Agents) {
                world.pause_agents(false).await?;
            }
            world.drive_to_settlement().await
        }
        .await;
        if let Err(violation) = outcome {
            panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
        }
    }
}

/// Where a hold command finds the entry check of the Task it holds.
#[derive(Clone, Copy, Debug, PartialEq)]
enum HoldFinds {
    /// The check runs and the hold is sent at once: whichever of the orders
    /// below the machine produces.
    TheRunInFlight,
    /// The check runs and has a long way to go: the check worker stops it
    /// before the release.
    ARunItStops,
    /// The same, and the release is sent the moment the check worker begins
    /// to stop the run.
    ARunBeingStopped,
    /// The check runs, and the release follows the hold at once: before the
    /// check worker's next look, unless the machine is very slow.
    ARunItsReleaseJoins,
    /// The run has ended and its result is stored, but no Task step has
    /// read it yet.
    AFinishedRun,
    /// The check is asked for and the step that asked has not begun to wait
    /// for it: the hold stops that step in the middle.
    AStepThatJustAsked,
    /// The same window, and the owner command is a Cancel.
    AStepThatJustAskedCancelled,
}

/// A hold COMMAND during the entry check. It is not on offer (see above) and
/// is accepted all the same, because a hold may stop hooks in flight: the
/// hooks step is superseded, its check request withdrawn, the attempt closed.
/// The release runs the review entry again by itself. Before, the released
/// Task had no check, and its reviewer was given the review.
///
/// The hold withdraws the request; it does not wait for the run. The check
/// worker stops a run nobody waits for at its next look (within a second),
/// so one of two things is true at the release, and both are right:
/// - the run was stopped: the entry asks again and a second run answers it;
/// - the run was not stopped (it had ended, or the release asked before the
///   worker looked): its result answers the entry, and nothing runs twice.
///
/// Either way the Task stays in `review` while it is held and no reviewer
/// runs under the hold; one run of the check passes, one review attempt
/// passes on it, and every attempt before that one is closed.
async fn hold_during_the_entry_check(finds: HoldFinds) -> Result<Vec<String>, String> {
    use HoldFinds::*;
    const REVIEWER_RUNS: &str =
        "SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer'";
    // Results the check worker delivered to a waiting step.
    const DELIVERED: &str = "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin='entry' AND applied_at IS NOT NULL";
    const CONSUMERS: &str =
        "SELECT c.origin || ':' || COALESCE(r.state,'-') || ':' || COALESCE(x.outcome,'-')
                || CASE WHEN c.cancelled_at IS NOT NULL THEN ':withdrawn' ELSE '' END
                || CASE WHEN c.applied_at IS NOT NULL THEN ':delivered' ELSE '' END
         FROM check_consumer c LEFT JOIN check_run r ON r.id = c.run_id
         LEFT JOIN check_result x ON x.id = c.result_id
         WHERE c.task_id = ? ORDER BY c.created_at, c.rowid";
    const ENTRY_RUNS: &str = "SELECT r.state FROM check_run r WHERE r.id IN
           (SELECT c.run_id FROM check_consumer c WHERE c.task_id = ? AND c.origin = 'entry')
         ORDER BY r.created_at, r.rowid";
    const REVIEWS: &str =
        "SELECT attempt_number || ':' || status FROM review WHERE task_id = ? ORDER BY attempt_number";
    const STEPS: &str =
        "SELECT kind || ':' || status FROM task_step WHERE task_id = ? ORDER BY seq";
    const SET_CHECK: &str = "UPDATE task SET task_state_config=json_set(task_state_config,'$.review.ci_steps',json_array(?)) WHERE id=?";
    // The check worker alone runs the check in these (no drain beside it):
    // the check waits for the one slot, which a second Task's run holds,
    // until that run ends. The model has no step worker, so no Task step
    // runs meanwhile.
    let slot_seam = !matches!(finds, TheRunInFlight | ARunItStops);
    let just_asked = matches!(finds, AStepThatJustAsked | AStepThatJustAskedCancelled);
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        for _ in 0..if slot_seam { 2 } else { 1 } {
            world.apply(&Action::Create).await?;
            world.quiesce().await?;
        }
        let pool = world.live().pool.clone();
        let task_id = world.tasks[0].clone();
        let sql = |error: sqlx::Error| error.to_string();
        let count = |sql: &'static str| {
            let (pool, id) = (pool.clone(), task_id.clone());
            async move {
                sqlx::query_scalar::<_, i64>(sql)
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .map_err(|error| error.to_string())
            }
        };
        let list = |sql: &'static str| {
            let (pool, id) = (pool.clone(), task_id.clone());
            async move {
                sqlx::query_scalar::<_, String>(sql)
                    .bind(id)
                    .fetch_all(&pool)
                    .await
                    .map_err(|error| error.to_string())
            }
        };
        // A check that is still running when the hold arrives. The one the
        // check worker must stop runs long the first time only: its second
        // run is the one the release waits for.
        let first = world.dir.path().join("entry-check-ran");
        let command = match finds {
            TheRunInFlight | ARunItsReleaseJoins => {
                Some(format!("sleep 4 && test -f {}", deliverable(0)))
            }
            ARunItStops | ARunBeingStopped => Some(format!(
                "if [ ! -e '{0}' ]; then : > '{0}'; sleep 60; fi; test -f {1}",
                first.display(),
                deliverable(0)
            )),
            AFinishedRun | AStepThatJustAsked | AStepThatJustAskedCancelled => None,
        };
        if let Some(command) = command {
            sqlx::query(SET_CHECK)
                .bind(command)
                .bind(&task_id)
                .execute(&pool)
                .await
                .map_err(sql)?;
        }
        let mut steps = None;
        if slot_seam {
            let cap = &world.live().state.db.server_run_cap;
            cap.set(Some(1), 1, &cap.embedded_machine_id());
            if just_asked {
                // The real hooks step asks for the check (the run waits for
                // the one slot) and is stopped right there, before it
                // suspends: it holds its claim, and the drain that carries
                // it stays alive for the owner command to preempt.
                use services::workflow::actions::review_entry_halt as halt;
                let reached = halt::arm(&task_id, halt::AFTER_REQUEST_BEFORE_SUSPEND);
                world.apply(&Action::Finish(0, Outcome::Success)).await?;
                let drain = |world: &World| {
                    let service = world.live().state.task_service.clone();
                    let id = world.tasks[0].clone();
                    tokio::spawn(async move { service.drain(&id).await.map(|_| ()) })
                };
                let mut running = drain(&world);
                let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
                let asked = reached.notified();
                tokio::pin!(asked);
                loop {
                    tokio::select! {
                        _ = &mut asked => break,
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                    if running.is_finished() {
                        running = drain(&world);
                    }
                    if tokio::time::Instant::now() >= deadline {
                        halt::disarm(&task_id);
                        running.abort();
                        let task = world.task(0).await?;
                        return Err(format!(
                            "the entry never asked for its check: {} steps {:?}",
                            task["status"],
                            list(STEPS).await?
                        ));
                    }
                }
                let claimed = list(
                    "SELECT kind || ':' || status FROM task_step WHERE task_id = ? AND status IN ('claimed','suspended')",
                )
                .await?;
                if world.check_runs(0).await? != ["queued"] || claimed != ["hooks:claimed"] {
                    halt::disarm(&task_id);
                    running.abort();
                    return Err(format!(
                        "expected a queued check and a claimed hooks step: {:?} {claimed:?}",
                        world.check_runs(0).await?
                    ));
                }
                steps = Some(running);
            } else {
                world.apply(&Action::Finish(0, Outcome::Success)).await?;
                world.quiesce().await?;
                if world.check_runs(0).await? != ["queued"] {
                    return Err(format!(
                        "expected one queued check: {:?}",
                        world.check_runs(0).await?
                    ));
                }
                // The slot frees and the check worker runs the check.
                world.apply(&Action::Finish(1, Outcome::Success)).await?;
                let reached = if finds == AFinishedRun {
                    "SELECT COUNT(*) FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                     WHERE c.task_id = ? AND r.state = 'succeeded' AND c.delivery_step_id IS NOT NULL"
                } else {
                    "SELECT COUNT(*) FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                     WHERE c.task_id = ? AND r.state = 'running' AND r.dispatch_json IS NOT NULL"
                };
                tokio::time::timeout(Duration::from_secs(60), async {
                    while count(reached).await? != 1 {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok::<_, String>(())
                })
                .await
                .map_err(|_| format!("the check did not get there: {finds:?}"))??;
                if count("SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status = 'suspended'")
                    .await?
                    != 1
                    || count(DELIVERED).await? != 0
                {
                    return Err("expected a suspended hooks step and an unread result".to_owned());
                }
            }
        } else {
            world.apply(&Action::Finish(0, Outcome::Success)).await?;
            // Not `quiesce`: that waits for the check to end.
            let drain = |world: &World| {
                let service = world.live().state.task_service.clone();
                let id = world.tasks[0].clone();
                tokio::spawn(async move { service.drain(&id).await.map(|_| ()) })
            };
            let mut running = drain(&world);
            let mut looked = 0;
            loop {
                if running.is_finished() {
                    running = drain(&world);
                }
                let task = world.task(0).await?;
                let runs = world.check_runs(0).await?;
                if task["status"] == "review" && runs == ["running"] {
                    break;
                }
                looked += 1;
                if looked > 400 {
                    return Err(format!("no running check: {} {runs:?}", task["status"]));
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            steps = Some(running);
        }
        let (version, offers) = world.offers(0).await?;
        if finds == AStepThatJustAskedCancelled {
            let cancel = offers
                .iter()
                .find(|offer| offer["action"]["verb"] == "cancel")
                .ok_or_else(|| format!("no cancel is offered: {offers:?}"))?;
            world.apply_offer(0, version, cancel).await?;
        } else {
            let (status, body) = world
                .request(
                    Method::POST,
                    &format!("/api/v1/tasks/{task_id}/actions"),
                    Some(json!({ "version": version, "action": { "verb": "hold" } })),
                )
                .await?;
            if !status.is_success() {
                return Err(format!("the hold command was refused: {status} {body}"));
            }
        }
        if just_asked {
            services::workflow::actions::review_entry_halt::disarm(&task_id);
        }
        if let Some(steps) = steps {
            let _ = steps.await;
        }
        // What the check worker has to do for the case, the hold standing.
        let stopping: &[&str] = match finds {
            ARunItStops => &["cancelled"],
            ARunBeingStopped => &["cancelling", "cancelled"],
            _ => &[],
        };
        if !stopping.is_empty() {
            let stopped = tokio::time::timeout(Duration::from_secs(45), async {
                loop {
                    let runs = world.check_runs(0).await?;
                    if runs.len() == 1 && stopping.contains(&runs[0].as_str()) {
                        return Ok::<_, String>(());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await;
            match stopped {
                Ok(done) => done?,
                Err(_) => {
                    return Err(format!(
                        "the withdrawn check was not stopped: runs {:?} consumers {:?} reviews {:?} steps {:?}",
                        world.check_runs(0).await?,
                        list(CONSUMERS).await?,
                        list(REVIEWS).await?,
                        list("SELECT kind || ':' || status || ':' || COALESCE(awaited_consumer_id,'-') || ':' || COALESCE(last_error,'-') FROM task_step WHERE task_id = ? ORDER BY seq").await?
                    ))
                }
            }
        }
        if just_asked {
            // The request was withdrawn with the step that made it, its
            // queued run cancelled with it, and its attempt closed, before
            // the slot frees.
            let (consumers, reviews) = (list(CONSUMERS).await?, list(REVIEWS).await?);
            if consumers != ["entry:cancelled:-:withdrawn"] || reviews != ["1:cancelled"] {
                return Err(format!(
                    "the request of the stopped step was not withdrawn: consumers {consumers:?} reviews {reviews:?}"
                ));
            }
            // The withdrawal cancelled the queued run nobody waits for, in
            // the same write and while the machine is still full: no slot
            // has to free for it.
            let runs = world.check_runs(0).await?;
            if runs != ["cancelled"] {
                return Err(format!(
                    "the run nobody waits for was not cancelled at the withdrawal: runs {runs:?} consumers {:?}",
                    list(CONSUMERS).await?
                ));
            }
            // The slot frees under the hold: nothing is left to run in it.
            world.apply(&Action::Finish(1, Outcome::Success)).await?;
            let runs = world.check_runs(0).await?;
            if runs != ["cancelled"] {
                return Err(format!("the cancelled run changed after the slot freed: {runs:?}"));
            }
        }
        if slot_seam {
            // The hold is what these cases are about, not the one slot.
            let cap = &world.live().state.db.server_run_cap;
            cap.set(Some(MODEL_RUN_CAP), MODEL_RUN_CAP, &cap.embedded_machine_id());
        }
        // The hold has done all it does when its command is answered; the
        // two cases that race the check worker do not give it time.
        if !matches!(finds, ARunBeingStopped | ARunItsReleaseJoins) {
            world.quiesce().await?;
        }
        if finds == AStepThatJustAskedCancelled {
            // Nothing is left of the entry: the Task is cancelled, the
            // request withdrawn, its run stopped, the attempt closed, and no
            // step waits or was delivered to.
            world.quiesce().await?;
            let task = world.task(0).await?;
            let (runs, reviews, consumers, steps) = (
                world.check_runs(0).await?,
                list(REVIEWS).await?,
                list(CONSUMERS).await?,
                list(STEPS).await?,
            );
            world.trace.push(format!(
                "{finds:?}: {} runs {runs:?} consumers {consumers:?} reviews {reviews:?} steps {steps:?}",
                task["status"]
            ));
            let live = count(
                "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status IN ('pending','claimed','suspended')",
            )
            .await?;
            if task["status"] != "cancelled"
                || runs != ["cancelled"]
                || reviews != ["1:cancelled"]
                // No result row: the queued run was never claimed.
                || consumers != ["entry:cancelled:-:withdrawn"]
                || count(DELIVERED).await? != 0
                || count(REVIEWER_RUNS).await? != 0
                || live != 0
            {
                return Err(format!(
                    "after the cancel: {} runs {runs:?} reviews {reviews:?} consumers {consumers:?}, {live} live step(s)",
                    task["status"]
                ));
            }
            return Ok(());
        }
        let held = world.task(0).await?;
        let runs = world.check_runs(0).await?;
        let reviews = list(REVIEWS).await?;
        let delivered = count(DELIVERED).await?;
        world.trace.push(format!(
            "{finds:?} held: {} runs {runs:?} consumers {:?} reviews {reviews:?} steps {:?} {}",
            held["status"],
            list(CONSUMERS).await?,
            list(STEPS).await?,
            held["condition"]
        ));
        if held["status"] != "review" || held["condition"]["primary"]["kind"] != "held" {
            return Err(format!(
                "the held Task is not held in review: {} {}",
                held["status"], held["condition"]
            ));
        }
        if count(REVIEWER_RUNS).await? != 0 {
            return Err("a reviewer ran while the Task was held".to_owned());
        }
        // The step that waited for the check is stopped, its request
        // withdrawn before any result was delivered, and its attempt closed.
        // A step stopped after it asked and before it began to wait is no
        // different: the request is recorded on the step when it is made.
        let closed = reviews == ["1:cancelled"] && delivered == 0;
        if !closed {
            return Err(format!(
                "under the hold: review attempts {reviews:?}, {delivered} result(s) delivered"
            ));
        }
        if !match finds {
            ARunItStops | AStepThatJustAsked => runs == ["cancelled"],
            AFinishedRun => runs == ["succeeded"],
            _ => runs.len() == 1,
        } {
            return Err(format!("check runs under the hold: {runs:?}"));
        }
        let (version, offers) = world.offers(0).await?;
        let release = offers
            .iter()
            .find(|offer| offer["action"]["verb"] == "release")
            .ok_or_else(|| format!("the held Task offers no release: {offers:?}"))?;
        world.apply_offer(0, version, release).await?;
        world.quiesce().await?;
        let runs = list(ENTRY_RUNS).await?;
        let released = world.task(0).await?;
        let delivered = count(DELIVERED).await?;
        world.trace.push(format!(
            "{finds:?} released: {} entry runs {runs:?} consumers {:?} reviews {:?} steps {:?} {}",
            released["status"],
            list(CONSUMERS).await?,
            list(REVIEWS).await?,
            list(STEPS).await?,
            released["condition"]
        ));
        // One run passed. A second one exists only when the first was
        // stopped, and never when the first was left to end.
        let again = runs == ["cancelled", "succeeded"];
        let kept = runs == ["succeeded"];
        if !match finds {
            ARunItStops => again && delivered == 1,
            // The entry's own step reads a result that exists when it asks:
            // nothing is delivered.
            AFinishedRun => kept && delivered == 0,
            AStepThatJustAsked => again && delivered == 1,
            AStepThatJustAskedCancelled => false,
            TheRunInFlight | ARunBeingStopped | ARunItsReleaseJoins => again || kept,
        } {
            return Err(format!(
                "the entry check after the release: {runs:?}, {delivered} result(s) delivered, {} {}",
                released["status"], released["condition"]
            ));
        }
        world.drive_to_settlement().await?;
        let done = world.task(0).await?;
        let reviews = list(REVIEWS).await?;
        world.trace.push(format!(
            "{finds:?} settled: {} reviews {reviews:?}",
            done["status"]
        ));
        // The attempt the hold stopped is closed, and one attempt passed on
        // the result. (An entry that joined a run in the middle of being
        // stopped is answered "cancelled": it closes its attempt too and
        // asks again.)
        let (last, before) = reviews.split_last().ok_or("no review attempt")?;
        if done["status"] != "done"
            || !last.ends_with(":passed")
            || before.is_empty()
            || before.iter().any(|attempt| !attempt.ends_with(":cancelled"))
        {
            return Err(format!(
                "the released Task ended in {} with review attempts {reviews:?}",
                done["status"]
            ));
        }
        // The owner's rule, for every check this Task asked for: one commit,
        // target and configuration passes in one run.
        let repeated = count(
            "SELECT COUNT(*) - COUNT(DISTINCT r.identity_key) FROM check_run r
             WHERE r.state = 'succeeded'
               AND r.id IN (SELECT c.run_id FROM check_consumer c WHERE c.task_id = ?)",
        )
        .await?;
        if repeated != 0 {
            return Err(format!("{repeated} check(s) ran to a pass a second time"));
        }
        Ok(())
    }
    .await;
    match outcome {
        Ok(()) => Ok(world.trace),
        Err(violation) => Err(format!("{violation}\ntrace:\n{}", world.trace.join("\n"))),
    }
}

/// The cases that start with two Tasks running side by side pin the cap.
async fn hold_during_the_entry_check_or_panic(finds: HoldFinds) {
    let case = hold_during_the_entry_check(finds);
    if let Err(violation) = PINNED_RUN_CAP.scope(Some(MODEL_RUN_CAP), case).await {
        panic!("{violation}");
    }
}

/// As the owner does it: whichever order the machine produces. (It asserted
/// exactly two runs, and failed in CI when the release asked before the check
/// worker had looked at the run.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_command_during_the_entry_check_is_resumed_by_its_release() {
    if let Err(violation) = hold_during_the_entry_check(HoldFinds::TheRunInFlight).await {
        panic!("{violation}");
    }
}

/// The first order, every run: the check worker stops the run nobody waits
/// for, and the release runs the check again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_that_stops_the_running_entry_check_runs_it_again_at_its_release() {
    if let Err(violation) = hold_during_the_entry_check(HoldFinds::ARunItStops).await {
        panic!("{violation}");
    }
}

/// The second order, every run: the run ended before the hold. Its result is
/// not applied under the hold; the release's entry is answered by it, and
/// the same commit and configuration do not run a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_after_the_entry_check_ended_keeps_its_result_for_the_release() {
    hold_during_the_entry_check_or_panic(HoldFinds::AFinishedRun).await;
}

/// The second order as CI met it: the release follows its hold before the
/// check worker has looked at the run. The run was never stopped, the entry
/// joins it, and its result is the one answer. (On a machine too slow for
/// that, this is the first order.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_right_after_the_hold_joins_the_entry_check_that_still_runs() {
    hold_during_the_entry_check_or_panic(HoldFinds::ARunItsReleaseJoins).await;
}

/// Between the two: the release arrives while the run is being stopped. The
/// entry never takes that run's cancellation for an answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_release_while_the_entry_check_is_being_stopped_still_gets_one_pass() {
    hold_during_the_entry_check_or_panic(HoldFinds::ARunBeingStopped).await;
}

/// The hold stops the hooks step after it asked for the check and before it
/// began to wait. The request is recorded on the step with the request
/// itself, so the hold withdraws it as it does for a step that waits: the
/// run nobody waits for is stopped, no result is delivered to a step that is
/// gone, the attempt is closed under the hold, and the release asks again.
/// (The step is stopped at that exact point, in the real hook.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hold_that_stops_the_step_that_just_asked_withdraws_the_entry_check_it_asked_for() {
    hold_during_the_entry_check_or_panic(HoldFinds::AStepThatJustAsked).await;
}

/// The same window under a Cancel: nothing is left of the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancel_that_stops_the_step_that_just_asked_leaves_nothing_of_the_entry_check() {
    hold_during_the_entry_check_or_panic(HoldFinds::AStepThatJustAskedCancelled).await;
}

/// A held Task in `review` and a system re-entry of its review (recovery, a
/// queued command). The re-entry is refused while the hold stands and changes
/// nothing. With the real dispatcher nothing has to ask again after the
/// release: the dispatcher gives the review its reviewer run and the Task
/// goes on to its end by itself. (A Task that waits for its entry CHECK
/// cannot be held at all: see the two cases above.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_task_in_review_refuses_a_system_re_entry_and_goes_on_by_itself_after_release() {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        world.apply(&Action::Create).await?;
        world.quiesce().await?;
        world.apply(&Action::Finish(0, Outcome::Success)).await?;
        // The reviewer is paused: the Task waits in `review` for it, its
        // entry check applied, and that wait is one the owner can hold.
        world.pause_agents(true).await?;
        world.quiesce().await?;
        let facts = |world: &World| {
            let pool = world.live().pool.clone();
            let id = world.tasks[0].clone();
            async move {
                let count = |sql: &'static str| {
                    let (pool, id) = (pool.clone(), id.clone());
                    async move {
                        sqlx::query_scalar::<_, i64>(sql)
                            .bind(id)
                            .fetch_one(&pool)
                            .await
                            .map_err(|error| error.to_string())
                    }
                };
                Ok::<_, String>((
                    count("SELECT COUNT(*) FROM review WHERE task_id = ?").await?,
                    // Review-entry requests. The queue head's recorded reuse
                    // of the entry's passed run (origin `integration`, written
                    // when the released Task merges) is not an entry that ran
                    // again; that it has no run of its own is asserted below.
                    count("SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin = 'entry'").await?,
                    count("SELECT COUNT(*) FROM task_step WHERE task_id = ? AND kind = 'hooks'")
                        .await?,
                    count("SELECT status_epoch FROM task WHERE id = ?").await?,
                    count("SELECT COUNT(*) FROM execution WHERE task_id = ? AND role = 'reviewer'").await?,
                ))
            }
        };
        let task = world.task(0).await?;
        let reviewing = facts(&world).await?;
        if task["status"] != "review" || reviewing.4 != 0 {
            return Err(format!(
                "expected a Task in review waiting for its paused reviewer, got {} {reviewing:?} {}",
                task["status"], task["condition"]
            ));
        }
        let (version, offers) = world.offers(0).await?;
        let hold = offers
            .iter()
            .find(|offer| offer["action"]["verb"] == "hold")
            .ok_or_else(|| {
                format!(
                    "a Task waiting in review for its reviewer offers no hold: {offers:?} {}",
                    task["condition"]
                )
            })?;
        world.apply_offer(0, version, hold).await?;
        world.quiesce().await?;
        let held = facts(&world).await?;
        let id = world.tasks[0]
            .parse()
            .map_err(|_| "task id".to_owned())?;
        let refused = world
            .live()
            .state
            .task_service
            .rerun_review(
                id,
                api_types::Actor::system(api_types::SystemComponent::CrashRecovery),
            )
            .await;
        if !matches!(refused, Err(services::ServiceError::Conflict(_))) {
            return Err(format!(
                "a system re-entry ran on a held Task: {:?}",
                refused.map(|task| task.status)
            ));
        }
        world.quiesce().await?;
        if facts(&world).await? != held {
            return Err(format!(
                "the refused re-entry changed the held Task: {held:?} -> {:?}",
                facts(&world).await?
            ));
        }
        let (version, offers) = world.offers(0).await?;
        let release = offers
            .iter()
            .find(|offer| offer["action"]["verb"] == "release")
            .ok_or_else(|| format!("the held Task offers no release: {offers:?}"))?;
        world.apply_offer(0, version, release).await?;
        world.pause_agents(false).await?;
        world.quiesce().await?;
        // Nothing asked again and no offer was applied: the dispatcher gave
        // the review its reviewer run, on the check that was applied before
        // the hold (no second check, no second attempt).
        let released = facts(&world).await?;
        let task = world.task(0).await?;
        world.trace.push(format!(
            "released: {} {released:?} {}",
            task["status"], task["condition"]
        ));
        if released.4 < 1 {
            return Err(format!(
                "after the release nothing continues the review: {} {released:?} {}",
                task["status"], task["condition"]
            ));
        }
        if (released.0, released.1) != (held.0, held.1) {
            return Err(format!(
                "the release ran the entry again: {held:?} -> {released:?}"
            ));
        }
        let runs = world.check_runs(0).await?;
        let heads = world.head_consumers_reuse_the_entry_run(0).await?;
        if runs != ["succeeded"] || heads > 1 {
            return Err(format!(
                "the release ran a check again: runs {runs:?}, head reuses {heads}"
            ));
        }
        world.drive_to_settlement().await
    }
    .await;
    if let Err(violation) = outcome {
        panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
    }
}

/// Where the server dies while a Task's hooks step waits for its review-entry
/// check. The process is dropped and a new one is built on the same database.
#[derive(Clone, Copy, PartialEq, Debug)]
enum RestartPoint {
    /// The check was requested; the step had not yet been suspended.
    AfterRequestBeforeSuspend,
    /// The result's delivery step is enqueued and has not run.
    AfterDeliveryBeforeWake,
    /// The delivery woke the hooks step and the runtime stopped as that
    /// step, claimed, was about to read the result.
    AfterWakeBeforeSettle,
    /// The delivery step is dead-lettered: nothing wakes the hooks step but
    /// its own deadline.
    DeliveryDeadLettered,
}

async fn restart_while_the_entry_check_is_awaited(point: RestartPoint) {
    // Pinned: the case starts with two Tasks running side by side.
    PINNED_RUN_CAP
        .scope(Some(MODEL_RUN_CAP), restart_with_two_slots_then_one(point))
        .await;
}

async fn restart_with_two_slots_then_one(point: RestartPoint) {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        // As in the pause tests: the first Task's check waits for the one
        // slot, which the second Task's run holds.
        for action in [Action::Create, Action::Create] {
            world.apply(&action).await?;
            world.quiesce().await?;
        }
        let cap = &world.live().state.db.server_run_cap;
        cap.set(Some(1), 1, &cap.embedded_machine_id());
        world.apply(&Action::Finish(0, Outcome::Success)).await?;
        world.quiesce().await?;
        if world.check_runs(0).await? != ["queued"] {
            return Err(format!("expected one queued check: {:?}", world.check_runs(0).await?));
        }
        let task_id = world.tasks[0].clone();
        let pool = world.live().pool.clone();
        let sql = |error: sqlx::Error| error.to_string();
        if point == RestartPoint::AfterRequestBeforeSuspend {
            // The consumer and its run are stored; the step still holds the
            // claim of a process that is gone.
            let changed = sqlx::query(
                "UPDATE task_step SET status='claimed', claimed_by='dead-process',
                        lease_until='2000-01-01T00:00:00+00:00', attempts=attempts+1,
                        suspended_until=NULL
                 WHERE task_id = ? AND status = 'suspended'",
            )
            .bind(&task_id)
            .execute(&pool)
            .await
            .map_err(sql)?
            .rows_affected();
            if changed != 1 {
                return Err("no suspended hooks step to rewind".to_owned());
            }
        } else {
            // The slot frees; the check worker runs the check and enqueues
            // its delivery. No Task step runs: the model has no step worker.
            world.apply(&Action::Finish(1, Outcome::Success)).await?;
            let delivery: String = tokio::time::timeout(Duration::from_secs(60), async {
                loop {
                    let step: Option<String> = sqlx::query_scalar(
                        "SELECT c.delivery_step_id FROM check_consumer c JOIN check_run r ON r.id = c.run_id
                         WHERE c.task_id = ? AND r.state = 'succeeded' AND c.delivery_step_id IS NOT NULL",
                    )
                    .bind(&task_id)
                    .fetch_optional(&pool)
                    .await
                    .map_err(sql)?;
                    if let Some(step) = step {
                        return Ok::<_, String>(step);
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .map_err(|_| "the check result was not delivered".to_owned())??;
            let pending: Option<String> =
                sqlx::query_scalar("SELECT status FROM task_step WHERE id = ?")
                    .bind(&delivery)
                    .fetch_optional(&pool)
                    .await
                    .map_err(sql)?;
            let suspended: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_step WHERE task_id = ? AND status = 'suspended'",
            )
            .bind(&task_id)
            .fetch_one(&pool)
            .await
            .map_err(sql)?;
            if pending.as_deref() != Some("pending") || suspended != 1 {
                return Err(format!(
                    "expected an unrun delivery and a suspended step, got {pending:?} / {suspended}"
                ));
            }
            match point {
                RestartPoint::AfterWakeBeforeSettle => {
                    // The real runtime runs the delivery step and claims the
                    // woken hooks step, and is stopped right there: the step
                    // parks at the armed point and the drain that carries it
                    // is dropped, as a process that died holding the claim.
                    use services::workflow::actions::review_entry_halt as halt;
                    let reached = halt::arm(&task_id, halt::AFTER_WAKE_BEFORE_SETTLE);
                    let service = world.live().state.task_service.clone();
                    let id = task_id.clone();
                    let running = tokio::spawn(async move { service.drain(&id).await });
                    let stopped =
                        tokio::time::timeout(Duration::from_secs(60), reached.notified()).await;
                    running.abort();
                    let _ = running.await;
                    halt::disarm(&task_id);
                    if stopped.is_err() {
                        return Err("the woken hooks step never ran".to_owned());
                    }
                    let delivered: Option<String> =
                        sqlx::query_scalar("SELECT status FROM task_step WHERE id = ?")
                            .bind(&delivery)
                            .fetch_optional(&pool)
                            .await
                            .map_err(sql)?;
                    let applied: i64 = sqlx::query_scalar(
                        "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin='entry' AND applied_at IS NOT NULL",
                    )
                    .bind(&task_id)
                    .fetch_one(&pool)
                    .await
                    .map_err(sql)?;
                    let claimed: Vec<(String, String)> = sqlx::query_as(
                        "SELECT kind, status FROM task_step WHERE task_id = ? AND status IN ('claimed','pending','suspended')",
                    )
                    .bind(&task_id)
                    .fetch_all(&pool)
                    .await
                    .map_err(sql)?;
                    if delivered.as_deref() != Some("done")
                        || applied != 1
                        || claimed != [("hooks".to_owned(), "claimed".to_owned())]
                    {
                        return Err(format!(
                            "expected a finished delivery and a claimed hooks step, got {delivered:?} / {applied} / {claimed:?}"
                        ));
                    }
                    let reviews: Vec<String> =
                        sqlx::query_scalar("SELECT status FROM review WHERE task_id = ?")
                            .bind(&task_id)
                            .fetch_all(&pool)
                            .await
                            .map_err(sql)?;
                    if reviews != ["running"] {
                        return Err(format!("the attempt settled before the stop: {reviews:?}"));
                    }
                    // Time passes: the dead process's claim runs out.
                    sqlx::query(
                        "UPDATE task_step SET lease_until='2000-01-01T00:00:00+00:00' WHERE task_id = ? AND status = 'claimed'",
                    )
                    .bind(&task_id)
                    .execute(&pool)
                    .await
                    .map_err(sql)?;
                }
                RestartPoint::DeliveryDeadLettered => {
                    sqlx::query(
                        "UPDATE task_step SET status='failed', last_error='dead-lettered', completed_at=?, updated_at=? WHERE id = ?",
                    )
                    .bind(db::now_rfc3339())
                    .bind(db::now_rfc3339())
                    .bind(&delivery)
                    .execute(&pool)
                    .await
                    .map_err(sql)?;
                    // Only the step's own deadline is left; it has passed.
                    sqlx::query(
                        "UPDATE task_step SET suspended_until='2000-01-01T00:00:00+00:00' WHERE task_id = ? AND status = 'suspended'",
                    )
                    .bind(&task_id)
                    .execute(&pool)
                    .await
                    .map_err(sql)?;
                }
                _ => {}
            }
        }
        drop(pool);
        world.crash_and_restart().await?;
        world.quiesce().await?;
        world.drive_to_settlement().await?;
        for index in 0..2 {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!("task {index} ended in {}", task["status"]));
            }
        }
        // One request, one run, one review attempt that passed: the restart
        // repeated nothing.
        // The Task merged on an unchanged commit and target, so its queue
        // head is a second CONSUMER of that one run (a recorded reuse), never
        // a second run and never a second review-entry request.
        let runs = world.check_runs(0).await?;
        let consumers: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM check_consumer WHERE task_id = ? AND origin = 'entry'",
        )
        .bind(&task_id)
        .fetch_one(&world.live().pool)
        .await
        .map_err(sql)?;
        let heads = world.head_consumers_reuse_the_entry_run(0).await?;
        if runs != ["succeeded"] || consumers != 1 || heads > 1 {
            return Err(format!(
                "check runs {runs:?}, entry requests {consumers}, head reuses {heads}; expected one run, one request, at most one reuse"
            ));
        }
        let reviews: Vec<String> =
            sqlx::query_scalar("SELECT status FROM review WHERE task_id = ? ORDER BY attempt_number")
                .bind(&task_id)
                .fetch_all(&world.live().pool)
                .await
                .map_err(sql)?;
        if reviews != ["passed"] {
            return Err(format!("review attempts {reviews:?}, expected one that passed"));
        }
        Ok(())
    }
    .await;
    if let Err(violation) = outcome {
        world.record_history().await;
        panic!("{point:?}: {violation}\ntrace:\n{}", world.trace.join("\n"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_after_the_check_request_before_the_suspend_asks_once() {
    restart_while_the_entry_check_is_awaited(RestartPoint::AfterRequestBeforeSuspend).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_after_the_check_delivery_before_the_wake_resumes_the_step() {
    restart_while_the_entry_check_is_awaited(RestartPoint::AfterDeliveryBeforeWake).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_after_the_wake_before_the_settle_settles_from_the_stored_result() {
    restart_while_the_entry_check_is_awaited(RestartPoint::AfterWakeBeforeSettle).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_lettered_check_delivery_is_read_by_the_step_at_its_deadline() {
    restart_while_the_entry_check_is_awaited(RestartPoint::DeliveryDeadLettered).await;
}

/// One slot. After the Project is resumed, the first Task's reviewer run
/// takes the slot and the second Task (whose coder run ended under the pause)
/// is refused a run slot. Its `machine_capacity` wait and its
/// `task_schedule_wait` row (`daemon_id = '*'`) are written after the
/// dispatcher read the machine as full. When the reviewer run ended between
/// that read and those writes, the mark for "waiters on a run slot" found no
/// waiter: the Task stayed `in_progress`, parked on `capacity` (scope
/// `machine`), with the machine idle, no step pending and nothing due.
///
/// A Task that starts waiting for a run slot is now read once more after its
/// wait row exists (`SqliteDb::schedule_wait`), and every such row carries a
/// recheck deadline. This case failed in about two of three runs of the
/// whole target on a loaded machine.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_slot_wait_registered_after_the_release_is_still_woken() {
    if let Err(violation) = pause_while_the_entry_check_is_pending(PauseKind::Project, true).await {
        panic!("{violation}");
    }
}

/// No pinned cap: the server resolves its own from the host's cores.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_stay_live_without_a_pinned_run_cap() {
    PINNED_RUN_CAP
        .scope(None, run_cases(cap_matrix_cases()))
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
    // Pinned: the third Task must queue behind the Agent's two runs. With one
    // machine slot it waits for the machine instead and `start` is not offered.
    PINNED_RUN_CAP
        .scope(Some(MODEL_RUN_CAP), start_on_a_queued_task())
        .await;
}

async fn start_on_a_queued_task() {
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
            // Waiting for its paused Agent is a visible wait, not a park the
            // refused claim left behind.
            let agent_wait = task["condition"]["primary"]["kind"] == "agent";
            if task["status"] != "todo" || (task["condition"]["kind"] == "parked" && !agent_wait) {
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

/// The adjacent gap: the dependency is unfinished, not cancelled. An accepted
/// `send_back` or `retry` used to be refused by the dependency gate into the
/// same untyped `recovery_required: "dependency gate"` park, which offered
/// the refused action again. The action now stays accepted behind the
/// dependency wait and runs when the dependency finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_action_waits_for_an_unfinished_dependency() {
    use Action::{Create, Depend, Finish, Take};
    let sequence = vec![
        step(Create),
        step(Take(0, "approve")),
        step(Create),
        step(Depend(0, 1)),
        step(Take(0, "send_back")),
        step(Take(0, "retry")),
        step(Finish(1, Outcome::Success)),
    ];
    let name = "parked dependant, dependency unfinished";
    let trace = run(&sequence, false)
        .await
        .unwrap_or_else(|failure| panic!("{}", report(name, &sequence, &failure)));
    assert!(
        !trace.iter().any(|line| line.contains("recovery_required")),
        "{name}: the wait became an untyped park\n{}",
        trace.join("\n")
    );
    run_cases(vec![(name.to_owned(), sequence)]).await;
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

/// Found by a random run of this model (`FORGE_MODEL_SEED=45233331
/// FORGE_MODEL_LONG=1`) and fixed with it; minimized below. The coder Agent
/// is paused, the running Task is held and then released. The hold stopped
/// the run with a `manual` resume policy, and a release taken while no Agent
/// could run the Task (`release_to_dispatch_queue`,
/// `services/src/task_service/actions.rs`) cleared the hold but left that
/// policy, so the dispatcher's stopped-run park
/// (`task_dispatcher/snapshot.rs`, `stopped_execution`) kept the Task in
/// `in_progress` with a `clear` condition and no run once the Agent resumed.
/// The release is the owner's decision on that run: it now marks it
/// resumable and the Task runs again when its Agent can take it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

/// Reached by `FORGE_MODEL_SEED=974622785` on a slow CI runner and fixed;
/// the sequence below is that seed's without its crash. The crash only chose
/// the history: on the runner the lead's planning run was committed before
/// the crash, so the root went on to coordinate and subtask 2 started; on a
/// fast host the crash wins and the seed never gets there.
///
/// The owner approves the running subtask twice. A user acts under the
/// Project workflow, so the first approval moves the subtask to `review`
/// (a state the inherited subtask workflow does not have), where it parks
/// with nothing to review; the second passes that gate into `merging`. A
/// subtask does not merge: the `run_merge` entry answered `Skipped`, and the
/// dispatcher's admission sweep takes root Tasks only, so the subtask rested
/// in `merging` with a `clear` condition, no step and no park. It now ends
/// `done` there (its work lands with the root's merge), from the entry and
/// from any later dispatcher pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subtask_approved_into_the_merge_state_finishes() {
    use Action::{AdvanceClock, AssignLead, CreateLeadRoot, Finish, Take};
    let name = "the owner approves a subtask through review into merging";
    let sequence = vec![
        step(CreateLeadRoot),
        step(AssignLead(0)),
        step(Finish(2, Outcome::UsageLimit)),
        step(Take(1, "cancel")),
        step(AdvanceClock),
        step(Finish(2, Outcome::Success)),
        step(Take(2, "approve")),
        step(Take(2, "approve")),
    ];
    let trace = run(&sequence, false)
        .await
        .unwrap_or_else(|failure| panic!("{}", report(name, &sequence, &failure)));
    // The sequence must walk the history it names, or it proves nothing.
    assert!(
        trace[5].contains("2:in_progress/running/")
            && trace[6].contains("-> approve:")
            && trace[6].contains("2:review/parked/failure(review_candidate_missing)")
            && trace[7].contains("-> approve:"),
        "{name}: the subtask did not reach its merge state\n{}",
        trace.join("\n")
    );
    assert!(
        trace[7].contains(" 2:done/settled/"),
        "{name}: the subtask did not finish\n{}",
        trace.join("\n")
    );
}

/// Reached by `FORGE_MODEL_LONG=1 FORGE_MODEL_SEED=951857209` and fixed;
/// minimized below. Task 3's review is rejected (scripted) while other Tasks
/// hold the coder's runs; it goes back to `in_progress` and stayed there with
/// a `clear` condition, no run, no deferral and no park. It was waiting for a
/// free run of its busy Agent: the dispatcher's capacity park
/// (`Reason::Capacity`, `task_dispatcher/next_step.rs`, `agent_full`) was a
/// scheduling decision nothing stored. For a Task that already started it is
/// now the visible schedule park `AgentCapacity`: the Task is `parked` on the
/// typed reason `capacity` with scope `agent`, and runs when a run ends. The
/// capacity park is code the parent, Agent and dependency waits did not
/// touch, so the stall predates them (read, not re-run on the older build).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_sent_back_by_review_under_a_busy_agent_shows_why_it_waits() {
    use Action::{Create, Finish, Offer, Verdict};
    run_cases(vec![(
        "review rejection while the coder is busy".to_owned(),
        vec![
            step(Create),
            step(Offer(0, 0)),
            step(Create),
            step(Create),
            step(Finish(2, Outcome::Fail)),
            step(Finish(2, Outcome::Success)),
            step(Create),
            step(Create),
            step(Create),
            step(Verdict(3, false)),
            step(Finish(1, Outcome::Conflict)),
        ],
    )])
    .await;
}

/// The case above, as CI minimized it when it failed there (next/v0.14
/// `a3a08e50`, a 4-core runner: run cap 2): "task step drain timed out
/// waiting for a check". Task 1 passes review, loses the merge race to a
/// moved target, is rebased and enters `review` a second time. By then the
/// dispatcher has given the freed run slot to the next Task, so the second
/// entry's CI run is queued behind a full machine. That is a wait the Task
/// states (`check`, phase `slot`) and the check worker ends when a run does;
/// it was the drain helper that kept sweeping for a slot nothing in a drain
/// can free (`TaskStepWorker::drain`, `services/src/worker_runtime/queue.rs`).
/// The drain now returns on a stated slot wait, and the settle phase proves
/// the second entry's CI runs and the Task merges once a run ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_rebased_after_merge_contention_runs_its_review_entry_again() {
    use Action::{Create, Finish, Verdict};
    run_cases(vec![(
        "second review entry after a mechanical rebase".to_owned(),
        vec![
            step(Create),
            step(Create),
            step(Create),
            step(Finish(2, Outcome::Fail)),
            step(Finish(2, Outcome::Success)),
            step(Create),
            step(Create),
            step(Create),
            step(Verdict(3, false)),
            step(Finish(1, Outcome::Conflict)),
        ],
    )])
    .await;
}

/// The check of a rebased commit, directly. Two Tasks branch from the same
/// target. The first merges; the second passed its review on the old target,
/// and the merge queue rebases it because the target moved. A check runs on
/// the rebased commit and settles before it merges (the queue's head check,
/// or the entry check of a second review when the review cannot carry), and
/// the Task merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_second_review_entry_after_a_rebase_requests_runs_and_settles_its_check() {
    // Pinned: both Tasks run side by side and branch from the same target.
    PINNED_RUN_CAP
        .scope(Some(MODEL_RUN_CAP), second_review_entry_after_a_rebase())
        .await;
}

async fn second_review_entry_after_a_rebase() {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        for action in [
            Action::Create,
            Action::Create,
            Action::Finish(0, Outcome::Success),
            Action::Finish(1, Outcome::Success),
        ] {
            let applied = world.apply(&action).await?;
            world.quiesce().await?;
            let state = world.check_quiescent().await?;
            world
                .trace
                .push(format!("{action:?} -> {applied}: {state}"));
        }
        world.drive_to_settlement().await?;
        let pool = world.live().pool.clone();
        let sql = |error: sqlx::Error| error.to_string();
        for index in 0..2 {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!("task {index} ended in {}", task["status"]));
            }
        }
        // Whichever Task merged second is the one the queue rebased: it stays
        // in `merging` (a clean rebase no longer bounces through
        // `merge_failed` and `review`), and the commit that landed is not the
        // one its review passed.
        let rebased: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT task_ref, integrated_sha FROM integration_attempt
             WHERE integrated_sha IS NOT NULL AND original_candidate_sha IS NOT NULL
               AND original_candidate_sha <> integrated_sha",
        )
        .fetch_all(&pool)
        .await
        .map_err(sql)?;
        let [(task_id, landed)] = rebased.as_slice() else {
            return Err(format!(
                "expected exactly one Task to be rebased by the merge queue, got {rebased:?}"
            ));
        };
        // Every check the Task asked for ran, finished and its verdict was
        // used, and one of them ran on the rebased commit that landed: the
        // entry check of a second review entry, or the queue's head check.
        //
        // A verdict is used in one of two orders. The delivery step applies
        // it (`applied_at`) and wakes the waiting entry step. Or the entry
        // step reads the stored result itself first (the run ended before
        // the step looked, or the step's wake deadline came before the
        // delivery sweep): it settles the Review and the Task leaves the
        // status entry, so the delivery that follows is superseded as stale
        // and `applied_at` stays empty. What shows the verdict was used then
        // is the Review that asked for the check: passed, with the check's
        // commands recorded on it.
        let checks: Vec<(String, String, String, Option<String>, i64, bool)> = sqlx::query_as(
            "SELECT c.origin, r.commit_sha, r.state, c.applied_at, c.status_epoch,
                    c.origin = 'entry' AND EXISTS(
                        SELECT 1 FROM review v
                        WHERE v.task_id = c.task_id AND v.status = 'passed'
                          AND c.request_key LIKE '%|' || v.id
                          AND json_array_length(v.step_results_json, '$.ci_steps') > 0)
             FROM check_consumer c JOIN check_run r ON r.id = c.run_id
             WHERE c.task_id = ? ORDER BY c.created_at, c.rowid",
        )
        .bind(task_id)
        .fetch_all(&pool)
        .await
        .map_err(sql)?;
        if checks
            .iter()
            .any(|(origin, _, state, applied, _, read_by_entry)| {
                // In either order the entry's Review carries the result.
                state != "succeeded"
                    || if origin == "entry" {
                        !read_by_entry
                    } else {
                        applied.is_none()
                    }
            })
        {
            return Err(format!("a check did not run and settle: {checks:?}"));
        }
        if !checks.iter().any(|(_, commit, ..)| commit == landed) {
            return Err(format!(
                "no check ran on the rebased commit {landed} before it merged: {checks:?}"
            ));
        }
        let reviews: Vec<String> = sqlx::query_scalar(
            "SELECT status FROM review WHERE task_id = ? ORDER BY attempt_number",
        )
        .bind(task_id)
        .fetch_all(&pool)
        .await
        .map_err(sql)?;
        if reviews.is_empty() || reviews.iter().any(|status| status != "passed") {
            return Err(format!(
                "review attempts {reviews:?}; expected every one passed"
            ));
        }
        Ok(())
    }
    .await;
    if let Err(violation) = outcome {
        world.record_history().await;
        panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
    }
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

/// Found by a random run with `CreateChild` generated and fixed; minimized
/// below. A coordination root whose aggregate review check fails ends in
/// `review` parked on `review retry budget exhausted`. It used to have no
/// exit but `cancel`:
///
/// - `retry` moved the root to `in_progress`, parked it on "coordination root
///   <id> is not in its aggregate review state"
///   (`services/src/task_service/execution/guards.rs`), and `restart` put it
///   back, for ever. `retry` on a root is now review-only: it resets the
///   budget and re-runs the aggregate review in place
///   (`reset_task_retry_budget`, `services/src/task_service/execution/recovery.rs`).
/// - The owner's `approve` (override) moved the root to `merging`, where
///   integration sent it back: "fresh conformance review required before
///   integration". A manual pass was not review authority while a reviewer
///   Agent was assigned, for a root or for any other Task. It now is
///   (`lock_review_integration`, `db/src/review_conformance.rs`).
/// - A corrective subtask could not be added (`SUBTASK_PARENT_CLOSED` in
///   `review`) and `send_back` was not offered on an exhausted budget. It is
///   now (`root_review_reopen`, `services/src/task_actions.rs`): the root
///   goes back to its working state with a fresh budget, held, and accepts
///   the subtask; its release runs the subtask and the review after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

/// Found by a random run with `CreateChild` generated and fixed with it;
/// minimized below. `cancel` was offered for a coordination root and refused
/// with `400 validation_error`: "the Task is settling a completed execution's
/// plan artifact; retry after publication". The refusal was
/// `ensure_plan_publication_transition_authority`
/// (`services/src/task_service/execution.rs`) in `cancel_task_with_options`
/// and in the subtask cancel cascade
/// (`services/src/workflow/actions/common.rs`), and the claim outlived
/// quiescence, so waiting did not clear it. Cancel now abandons the claim
/// (the prior plan is restored, staged files are removed) and proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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

/// Found by a random run with `CreateChild` generated and fixed with it. A
/// subtask created under a held parent is accepted and never dispatched while
/// the hold lasts (`task_hierarchy::coordination_root_allows_child_dispatch`),
/// which is right, but it said nothing: it sat in `todo` with a `clear`
/// condition and offered `start` and `cancel`. The dispatcher's park for it
/// (`services/src/task_dispatcher/next_step.rs`) was a scheduling decision
/// nothing stored. It is now the visible schedule park `ParentWait`: the
/// subtask is `parked` on the typed reason `parent`, naming the parent and
/// the cause, and offers no `start`. The same run found that releasing the
/// held parent queued a role the root may not run and put the hold back;
/// the release now clears the hold and the subtask runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subtask_of_a_held_parent_shows_why_it_waits() {
    use Action::{Create, CreateChild, Take};
    run_cases(vec![(
        "subtask under a held parent".to_owned(),
        vec![step(Create), step(Take(0, "hold")), step(CreateChild(0))],
    )])
    .await;
}

// ===========================================================================
// Upgrade: databases and repositories the last pre-queue build really wrote
// (`tests/fixtures/pre_queue/README.md`), restored and started on this build.
// ===========================================================================

/// The machine id the fixtures' daemon row carries (scrubbed when packed).
const PRE_QUEUE_MACHINE: &str = "embedded:fixture-host:macos:aarch64";

/// Replace `from` by `to` in every text file under `dir`: the database dump,
/// the manifest and Git's worktree link files. Git objects are not text.
fn reroot(dir: &Path, from: &str, to: &str) -> Result<(), String> {
    for entry in std::fs::read_dir(dir).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        let meta = std::fs::symlink_metadata(&path).map_err(|error| error.to_string())?;
        if meta.is_dir() {
            reroot(&path, from, to)?;
        } else if meta.is_file() && meta.len() < 16 * 1024 * 1024 {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            if text.contains(from) {
                std::fs::write(&path, text.replace(from, to)).map_err(|error| error.to_string())?;
            }
        }
    }
    Ok(())
}

/// What a start must leave alone the second time: every Task's state,
/// condition, offers, attempts and history, and every branch.
#[derive(Debug, PartialEq, Eq)]
struct UpgradePrint {
    tasks: Vec<String>,
    refs: String,
}

impl World {
    /// Restore the fixture into a fresh directory, re-rooted there, and start
    /// this build on it. Returns the fixture's manifest and the first
    /// start's import report.
    async fn restore(
        name: &str,
        shadow: bool,
        probe: Arc<Probe>,
    ) -> Result<
        (
            Self,
            Value,
            services::integration_steps::import::ImportReport,
        ),
        String,
    > {
        let io = |error: std::io::Error| error.to_string();
        let dir = tempfile::tempdir().map_err(io)?;
        let unpack = tempfile::tempdir().map_err(io)?;
        let archive =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/pre_queue/pre_queue.tar.xz");
        let tar = Command::new("tar")
            .arg("-xJf")
            .arg(&archive)
            .arg("-C")
            .arg(unpack.path())
            .arg(name)
            .output()
            .map_err(io)?;
        if !tar.status.success() {
            return Err(format!(
                "unpacking {name}: {}",
                String::from_utf8_lossy(&tar.stderr)
            ));
        }
        for entry in std::fs::read_dir(unpack.path().join(name)).map_err(io)? {
            let entry = entry.map_err(io)?;
            std::fs::rename(entry.path(), dir.path().join(entry.file_name())).map_err(io)?;
        }
        let manifest: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.path().join("manifest.json")).map_err(io)?,
        )
        .map_err(|error| error.to_string())?;
        let old_root = manifest["root"]
            .as_str()
            .ok_or("the manifest names its root")?;
        reroot(dir.path(), old_root, &dir.path().display().to_string())?;
        reroot(
            dir.path(),
            PRE_QUEUE_MACHINE,
            &services::embedded_daemon::embedded_machine_id(),
        )?;
        let dump = std::fs::read_to_string(dir.path().join("db.sql")).map_err(io)?;
        if dump.contains(old_root) {
            return Err(format!("{name}: the dump still names {old_root}"));
        }
        {
            use sqlx::Connection;
            let options = sqlx::sqlite::SqliteConnectOptions::new()
                .filename(dir.path().join("forge.db"))
                .foreign_keys(false)
                .create_if_missing(true);
            let mut connection = sqlx::SqliteConnection::connect_with(&options)
                .await
                .map_err(|error| error.to_string())?;
            sqlx::raw_sql(&dump)
                .execute(&mut connection)
                .await
                .map_err(|error| format!("loading the dump of {name}: {error}"))?;
            if !shadow {
                // An install that never ran the passive queue stages (the
                // last release) has no attempt and no queue row at all.
                sqlx::raw_sql("DELETE FROM integration_attempt; DELETE FROM integration_queue;")
                    .execute(&mut connection)
                    .await
                    .map_err(|error| format!("removing the shadow rows of {name}: {error}"))?;
            }
            connection
                .close()
                .await
                .map_err(|error| error.to_string())?;
        }
        std::fs::remove_file(dir.path().join("db.sql")).map_err(io)?;
        std::fs::create_dir_all(dir.path().join("web")).map_err(io)?;
        std::fs::write(dir.path().join("web/index.html"), "<html></html>").map_err(io)?;

        let shared = Arc::new(Shared::default());
        let mut tasks = Vec::new();
        for task in manifest["tasks"].as_array().into_iter().flatten() {
            let id = task["id"].as_str().unwrap_or_default().to_owned();
            shared.index.lock().unwrap().insert(id.clone(), tasks.len());
            tasks.push(id);
        }
        // Later than every write of the run that made the fixture.
        shared.writes.store(1000, Ordering::SeqCst);
        let live = Self::boot(dir.path(), &shared, &probe).await;
        let agent = |name: &'static str| {
            let pool = live.pool.clone();
            async move {
                sqlx::query_scalar::<_, String>("SELECT id FROM agent_identity WHERE name = ?")
                    .bind(name)
                    .fetch_one(&pool)
                    .await
                    .map_err(|error| format!("{name}: {error}"))
            }
        };
        let (coder_id, reviewer_id) = (agent("model-coder").await?, agent("model-reviewer").await?);
        let report = bounded(&probe, "the first start on the fixture", Self::start(&live)).await?;
        let world = Self {
            _storm: lock_storm(dir.path()),
            dir,
            probe,
            live: Some(live),
            shared,
            project_id: manifest["project_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            coder_id,
            reviewer_id,
            project_paused: manifest["project_paused"] == true,
            agent_paused: false,
            avoid_open_findings: false,
            tasks,
            settled: BTreeMap::new(),
            trace: Vec::new(),
        };
        Ok((world, manifest, report))
    }

    fn main_sha(&self) -> Result<String, String> {
        git(
            &self.dir.path().join("repo"),
            &["rev-parse", "refs/heads/main"],
        )
    }

    /// Whether `commit` is on main.
    fn on_main(&self, commit: &str) -> bool {
        git(
            &self.dir.path().join("repo"),
            &["merge-base", "--is-ancestor", commit, "refs/heads/main"],
        )
        .is_ok()
    }

    async fn upgrade_print(&self) -> Result<UpgradePrint, String> {
        let sql = |error: sqlx::Error| error.to_string();
        let pool = &self.live().pool;
        let mut tasks = Vec::new();
        for (index, id) in self.tasks.iter().enumerate() {
            let task = self.task(index).await?;
            let (_, offers) = self.offers(index).await?;
            let verbs: Vec<String> = offers
                .iter()
                .map(|offer| format!("{}:{}", offer["action"]["verb"], offer["reason"]))
                .collect();
            let attempts: Vec<(String, String, i64, Option<String>)> = sqlx::query_as(
                "SELECT state, admission_key, current, failure_message FROM integration_attempt
                 WHERE task_ref = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .map_err(sql)?;
            let history: Vec<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT from_state, to_state, bridge_kind FROM transition_log
                 WHERE task_id = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .map_err(sql)?;
            let reviews: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM review WHERE task_id = ?")
                .bind(id)
                .fetch_one(pool)
                .await
                .map_err(sql)?;
            let budgets: Vec<(String, i64)> = sqlx::query_as(
                "SELECT kind, spent FROM task_budget WHERE task_id = ? ORDER BY kind",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            tasks.push(format!(
                "{index}: {} condition {} / {} offers {verbs:?} attempts {attempts:?} history {history:?} reviews {reviews} budgets {budgets:?}",
                task["status"], task["condition"]["primary"], task["condition"]["additional"],
            ));
        }
        Ok(UpgradePrint {
            tasks,
            refs: git(
                &self.dir.path().join("repo"),
                &[
                    "for-each-ref",
                    "--format=%(refname) %(objectname)",
                    "refs/heads",
                ],
            )?,
        })
    }

    /// `rounds` passes of the step worker and the dispatcher (its sweep and
    /// the integration reconciler in it) over a state with nothing to do
    /// commit nothing: SQLite's `data_version`, read on a connection of its
    /// own, moves with every commit of any other connection.
    async fn idle_passes_write_nothing(&self, rounds: usize) -> Result<(), String> {
        use sqlx::Connection;
        let sql = |error: sqlx::Error| error.to_string();
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(self.dir.path().join("forge.db"))
            .read_only(true);
        let mut watch = sqlx::SqliteConnection::connect_with(&options)
            .await
            .map_err(sql)?;
        let counts = |pool: sqlx::SqlitePool| async move {
            let mut out = Vec::new();
            for table in [
                "task_step",
                "domain_event",
                "transition_log",
                "integration_attempt",
            ] {
                let n: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
                    .fetch_one(&pool)
                    .await
                    .unwrap_or(-1);
                out.push(format!("{table}={n}"));
            }
            let moved: (i64, i64) = sqlx::query_as(
                "SELECT (SELECT COALESCE(SUM(version),0) FROM task), (SELECT COALESCE(SUM(revision),0) FROM integration_attempt)",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or((-1, -1));
            format!(
                "{out:?} task versions {} attempt revisions {}",
                moved.0, moved.1
            )
        };
        // Let the check worker's own start-up writes pass first.
        let mut before: i64 = -1;
        for _ in 0..40 {
            let now: i64 = sqlx::query_scalar("PRAGMA data_version")
                .fetch_one(&mut watch)
                .await
                .map_err(sql)?;
            if now == before {
                break;
            }
            before = now;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let rows = counts(self.live().pool.clone()).await;
        for _ in 0..rounds {
            for id in &self.tasks {
                self.drain(id)
                    .await
                    .map_err(|error| format!("draining an idle Task failed: {error}"))?;
            }
            self.dispatcher_pass().await?;
        }
        let after: i64 = sqlx::query_scalar("PRAGMA data_version")
            .fetch_one(&mut watch)
            .await
            .map_err(sql)?;
        watch.close().await.map_err(sql)?;
        if after != before {
            return Err(format!(
                "{rounds} idle passes committed {} write(s): rows before {rows}, after {}",
                after - before,
                counts(self.live().pool.clone()).await
            ));
        }
        Ok(())
    }

    /// One Task in one line: where it is and what its owner is told.
    async fn upgrade_row(&self, index: usize) -> Result<String, String> {
        let task = self.task(index).await?;
        let (epoch, version): (i64, i64) =
            sqlx::query_as("SELECT status_epoch, version FROM task WHERE id = ?")
                .bind(&self.tasks[index])
                .fetch_one(&self.live().pool)
                .await
                .map_err(|error| error.to_string())?;
        let (_, offers) = self.offers(index).await?;
        let offers: Vec<String> = offers
            .iter()
            .map(|offer| {
                format!(
                    "{}({})",
                    offer["action"]["verb"].as_str().unwrap_or_default(),
                    offer["reason"].as_str().unwrap_or_default()
                )
            })
            .collect();
        let attempts: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT state, failure_message FROM integration_attempt WHERE task_ref = ? AND current = 1",
        )
        .bind(&self.tasks[index])
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let reason = |reason: &Value| match reason["kind"].as_str() {
            Some("integration") => format!(
                "integration:{}{}",
                reason["reason"]["kind"].as_str().unwrap_or_default(),
                reason["reason"]["cause"]
                    .as_str()
                    .map(|cause| format!("/{cause}"))
                    .unwrap_or_default()
            ),
            Some("failure") => format!(
                "failure:{}",
                reason["failure_kind"].as_str().unwrap_or_default()
            ),
            Some(kind) => kind.to_owned(),
            None => "-".to_owned(),
        };
        let condition = &task["condition"];
        let additional: Vec<String> = condition["additional"]
            .as_array()
            .into_iter()
            .flatten()
            .map(reason)
            .collect();
        Ok(format!(
            "{}@{epoch} v{version} {} primary {} additional {additional:?} offers {offers:?} attempt {attempts:?}",
            task["status"].as_str().unwrap_or_default(),
            condition["kind"].as_str().unwrap_or_default(),
            reason(&condition["primary"]),
        ))
    }

    /// The rows behind a failure: each Task's own columns, its attempts with
    /// their journals, its open steps and the trace.
    #[allow(clippy::type_complexity)]
    async fn upgrade_rows(&self) -> String {
        let pool = &self.live().pool;
        let mut out = String::new();
        for id in &self.tasks {
            let task: Vec<(String, i64, i64, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
                "SELECT status,version,status_epoch,blocked_json,failed_json,error_annotation,metadata_json,condition_json FROM task WHERE id = ?",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            out.push_str(&format!("ROW task {id}: {task:?}\n"));
            let attempts: Vec<(String, String, String, i64, Option<String>, Option<String>, Option<String>, String, Option<String>)> = sqlx::query_as(
                "SELECT id,state,admission_key,current,failure_kind,failure_message,available_at,operation_receipts_json,import_source_json FROM integration_attempt WHERE task_ref = ? ORDER BY created_at, rowid",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            for attempt in attempts {
                out.push_str(&format!("ROW   attempt {attempt:?}\n"));
            }
            let steps: Vec<(String, String, String, String, Option<String>)> = sqlx::query_as(
                "SELECT kind,status,expected_status,causation_key,last_error FROM task_step WHERE task_id = ? AND (status NOT IN ('done','completed','cancelled','superseded') OR last_error IS NOT NULL) ORDER BY seq",
            )
            .bind(id)
            .fetch_all(pool)
            .await
            .unwrap_or_default();
            for step in steps {
                out.push_str(&format!("ROW   step {step:?}\n"));
            }
        }
        format!("{out}TRACE {:#?}", self.trace)
    }

    /// The typed-statement refusal round 2 found on rows written with plain
    /// SQL, on any step of any Task.
    async fn refused_statements(&self) -> Result<Vec<String>, String> {
        sqlx::query_scalar(
            "SELECT task_id || ' ' || kind || ': ' || last_error FROM task_step
             WHERE last_error LIKE '%differs from its legacy and owner witnesses%'",
        )
        .fetch_all(&self.live().pool)
        .await
        .map_err(|error| error.to_string())
    }
}

/// What design section 5 says the upgrade does with the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Upgraded {
    /// The merge had landed: it is reported, once, and never repeated.
    Reported,
    /// Nothing had landed: the queue merges it once, with no owner action.
    Merges,
    /// A coder repair is the Task's own flow; it merges after the repair.
    Repaired,
    /// The Task waits with a typed reason and an offered action.
    Parked,
}

/// One fixture test under its hard deadline. The body is not `Send` as a
/// whole, so it is awaited here: the deadline fires at any await, and where
/// the body blocks its thread (a lock, a child process) the watchdog thread
/// reports and ends the process a minute later.
async fn pre_queue_upgrade(
    name: &'static str,
    shadow: bool,
    expected: Upgraded,
    take: Option<&'static str>,
) -> Result<(), String> {
    let variant = if shadow { "shadow" } else { "bare" };
    let probe = Probe::new(format!(
        "pre-queue {name} {variant}{}",
        take.map(|verb| format!(" {verb}")).unwrap_or_default()
    ));
    let _watchdog = probe.watchdog(TEST_LIMIT + Duration::from_secs(60));
    let body = pre_queue_upgrade_unbounded(name, shadow, expected, take, Arc::clone(&probe));
    match tokio::time::timeout(TEST_LIMIT, body).await {
        Ok(outcome) => outcome,
        Err(_) => panic!(
            "{}",
            probe
                .report(&format!("the test did not end within {TEST_LIMIT:?}"))
                .await
        ),
    }
}

async fn pre_queue_upgrade_unbounded(
    name: &'static str,
    shadow: bool,
    expected: Upgraded,
    take: Option<&'static str>,
    probe: Arc<Probe>,
) -> Result<(), String> {
    let (mut world, manifest, report) = World::restore(name, shadow, probe).await?;
    let variant = if shadow { "shadow" } else { "bare" };
    match pre_queue_upgraded(
        name, variant, expected, take, &mut world, &manifest, &report,
    )
    .await
    {
        Ok(()) => Ok(()),
        Err(error) => Err(format!("{error}\n{}", world.upgrade_rows().await)),
    }
}

#[allow(clippy::too_many_arguments)]
async fn pre_queue_upgraded(
    name: &str,
    variant: &str,
    expected: Upgraded,
    take: Option<&'static str>,
    world: &mut World,
    manifest: &Value,
    report: &services::integration_steps::import::ImportReport,
) -> Result<(), String> {
    let old_main = manifest["main"].as_str().unwrap_or_default().to_owned();
    eprintln!(
        "UPGRADE {name}: first start imported {} quarantined {} needs_fact {} unreadable {} continuations {}",
        report.imported, report.quarantined, report.needs_fact, report.unreadable, report.continuations
    );
    if report.unreadable > 0 {
        return Err(format!(
            "{name}: {} Task(s) were unreadable to the importer",
            report.unreadable
        ));
    }
    let clock = std::time::Instant::now();
    world.quiesce().await?;
    eprintln!(
        "UPGRADE {name}: first quiescence after {:?}",
        clock.elapsed()
    );
    // A merge the queue worker put off (the old build's rebase or lock was
    // still on disk) is retried at its own time, with no owner action.
    for _ in 0..4 {
        if world.project_paused || world.move_deadlines(true).await? == 0 {
            break;
        }
        world.quiesce().await?;
    }
    eprintln!(
        "UPGRADE {name}: retry deadlines passed after {:?}",
        clock.elapsed()
    );
    let line = world.check_quiescent().await?;
    let first = world.upgrade_print().await?;
    eprintln!(
        "UPGRADE {name}: after the first start: {line}\n  {}",
        first.tasks.join("\n  ")
    );
    let refused = world.refused_statements().await?;
    if !refused.is_empty() {
        return Err(format!(
            "{name}: the typed integration statement was refused: {refused:?}"
        ));
    }
    let unknown: Vec<String> = sqlx::query_scalar(
        "SELECT kind || ' ' || expected_status || ': ' || last_error FROM task_step
         WHERE last_error LIKE '%unknown action%'",
    )
    .fetch_all(&world.live().pool)
    .await
    .map_err(|error| error.to_string())?;
    if !unknown.is_empty() {
        return Err(format!(
            "{name}: a step of the old build named a hook this build does not have: {unknown:?}"
        ));
    }

    // A second start changes nothing. A run in flight loses its process on
    // a restart, so this is asked only of a state with no run.
    let running = (0..world.tasks.len()).any(|index| world.waiting(index));
    if !running {
        world.crash_and_restart().await?;
        world.quiesce().await?;
        world.check_quiescent().await?;
        let second = world.upgrade_print().await?;
        if first != second {
            return Err(format!(
                "{name}: a second start changed the state:\n first {first:#?}\n second {second:#?}"
            ));
        }
    }

    eprintln!(
        "UPGRADE {name}: second start checked after {:?}",
        clock.elapsed()
    );
    let last = world.tasks.len() - 1;
    let subject = world.task(last).await?;
    let before: i64 = manifest["tasks"][last]["transitions"]
        .as_array()
        .map_or(0, |rows| rows.len() as i64);
    let moves = |world: &World| {
        let (pool, id) = (world.live().pool.clone(), world.tasks[last].clone());
        async move {
            let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT from_state, to_state, bridge_kind FROM transition_log
                 WHERE task_id = ? ORDER BY created_at, rowid LIMIT -1 OFFSET ?",
            )
            .bind(id)
            .bind(before)
            .fetch_all(&pool)
            .await
            .map_err(|error| error.to_string())?;
            Ok::<String, String>(
                rows.iter()
                    .map(|(from, to, bridge)| match bridge {
                        Some(bridge) => format!("{from}>{to}[{bridge}]"),
                        None => format!("{from}>{to}"),
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            )
        }
    };
    let runs = manifest["tasks"][last]["executions"]
        .as_array()
        .map_or(0, Vec::len);
    eprintln!(
        "TABLE {name} {variant} start: was {}@{} | now {} | moves [{}] | main {}",
        manifest["tasks"][last]["status"]
            .as_str()
            .unwrap_or_default(),
        manifest["tasks"][last]["epoch"],
        world.upgrade_row(last).await?,
        moves(world).await?,
        if world.main_sha()? == old_main {
            "unchanged"
        } else {
            "advanced"
        },
    );
    let status = subject["status"].as_str().unwrap_or_default().to_owned();
    let main_after_start = world.main_sha()?;
    match expected {
        Upgraded::Reported => {
            if status != "done" || main_after_start != old_main {
                return Err(format!(
                    "{name}: a landed merge is reported once and not repeated; the Task is {status}, main {old_main} -> {main_after_start}"
                ));
            }
        }
        Upgraded::Merges => {
            if status != "done" || main_after_start == old_main {
                return Err(format!(
                    "{name}: the Task merges with no owner action; it is {status} with condition {}, main {old_main} -> {main_after_start}",
                    subject["condition"]
                ));
            }
            // Done means merged: main moved to a commit that carries the
            // Task's own branch tip.
            let branch = format!("refs/heads/task/{}", &world.tasks[last][..8]);
            let tip = git(&world.dir.path().join("repo"), &["rev-parse", &branch])?;
            if !world.on_main(&tip) {
                return Err(format!(
                    "{name}: the Task is done and its branch {tip} is not on main {main_after_start}"
                ));
            }
        }
        Upgraded::Repaired => {
            if TERMINAL.contains(&status.as_str()) {
                return Err(format!(
                    "{name}: the Task was settled without its repair: {status}"
                ));
            }
        }
        Upgraded::Parked => {
            let condition = &subject["condition"];
            let integration = condition["primary"]["kind"] == "integration"
                || condition["additional"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .any(|reason| reason["kind"] == "integration");
            let (_, offers) = world.offers(last).await?;
            if status == "done" || main_after_start != old_main {
                return Err(format!(
                    "{name}: a stopped Task merged with its stop in place: {status}, main {old_main} -> {main_after_start}"
                ));
            }
            if !integration {
                return Err(format!(
                    "{name}: the parked Task does not state its integration reason: {condition}"
                ));
            }
            if offers.is_empty() && !world.project_paused {
                return Err(format!(
                    "{name}: the parked Task offers no action: {condition}"
                ));
            }
        }
    }

    // A stopped Task is not worked on: the reconciler, the gate reads and
    // the dispatcher's sweep commit nothing while it waits.
    if expected == Upgraded::Parked && take.is_none() {
        world
            .idle_passes_write_nothing(10)
            .await
            .map_err(|error| format!("{name}: {error}"))?;
    }
    // What the fixture left on disk for the owner to clean up.
    if name == "p19_target_dirty" {
        git(&world.dir.path().join("repo"), &["checkout", "--", "."])?;
    }
    // One owner command through the real action endpoint, traced.
    let main_before_command = world.main_sha()?;
    if let Some(verb) = take {
        let was = world.upgrade_row(last).await?;
        let taken = world.apply(&Action::Take(last, verb)).await?;
        if taken != verb {
            return Err(format!(
                "{name}: `{verb}` on the Task: {taken}; it is {was}"
            ));
        }
        world.quiesce().await?;
        eprintln!(
            "COMMAND {name} {verb}: before {was} | after {} | moves [{}]",
            world.upgrade_row(last).await?,
            moves(world).await?,
        );
    }
    // The offered actions, and the runs they need, get every Task merged.
    world.drive_to_settlement().await?;
    let new_runs: Vec<(String, String)> = sqlx::query_as(
        "SELECT role, status FROM execution WHERE task_id = ? ORDER BY created_at, rowid LIMIT -1 OFFSET ?",
    )
    .bind(&world.tasks[last])
    .bind(runs as i64)
    .fetch_all(&world.live().pool)
    .await
    .map_err(|error| error.to_string())?;
    eprintln!(
        "TABLE {name} {variant} settled{}: {} | moves [{}] | new runs {new_runs:?} | owner actions {:?}",
        take.map(|verb| format!(" after {verb}")).unwrap_or_default(),
        world.upgrade_row(last).await?,
        moves(world).await?,
        world
            .trace
            .iter()
            .filter_map(|line| line.split_once(": ").map(|(taken, _)| taken.to_owned()))
            .collect::<Vec<_>>(),
    );
    for index in 0..world.tasks.len() {
        let task = world.task(index).await?;
        if take == Some("cancel") && index == last {
            if task["status"] != "cancelled" {
                return Err(format!(
                    "{name}: the cancelled Task ended {}",
                    task["status"]
                ));
            }
            continue;
        }
        // The owner's override at the merge gate declares the Task done
        // without a merge (as before the queue): nothing may merge it after.
        if take == Some("approve") && index == last {
            let live: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM integration_attempt WHERE task_ref = ? AND current = 1
                 AND state NOT IN ('completed','cancelled','superseded')",
            )
            .bind(&world.tasks[index])
            .fetch_one(&world.live().pool)
            .await
            .map_err(|error| error.to_string())?;
            if task["status"] != "done" || world.main_sha()? != main_before_command || live != 0 {
                return Err(format!(
                    "{name}: an approved Task is done with nothing merged and no live attempt; it is {}, main {main_before_command} -> {}, live attempts {live}",
                    task["status"],
                    world.main_sha()?
                ));
            }
            continue;
        }
        if task["status"] != "done" {
            return Err(format!(
                "{name}: task {index} ended {} and not done: {}; trace {:#?}",
                task["status"], task["condition"], world.trace
            ));
        }
        let branch = format!("refs/heads/task/{}", &world.tasks[index][..8]);
        let merged: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM transition_log WHERE task_id = ? AND to_state = 'done'",
        )
        .bind(&world.tasks[index])
        .fetch_one(&world.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        if merged != 1 {
            return Err(format!(
                "{name}: task {index} was reported done {merged} times"
            ));
        }
        if let Ok(tip) = git(&world.dir.path().join("repo"), &["rev-parse", &branch]) {
            if !world.on_main(&tip) {
                return Err(format!(
                    "{name}: task {index} is done and its branch {tip} is not on main"
                ));
            }
        }
    }
    if expected == Upgraded::Reported && world.main_sha()? != old_main {
        return Err(format!(
            "{name}: main moved after a landed merge was reported"
        ));
    }
    // One commit per Task on main: nothing merged twice.
    let subjects = git(
        &world.dir.path().join("repo"),
        &["log", "--format=%s", "refs/heads/main"],
    )?;
    let mut seen = std::collections::BTreeSet::new();
    for subject in subjects.lines() {
        if !seen.insert(subject.to_owned()) {
            return Err(format!(
                "{name}: main carries `{subject}` twice:\n{subjects}"
            ));
        }
    }
    eprintln!("UPGRADE {name}: settled after {:?}", clock.elapsed());
    // An attempt left parked behind a Task that was cancelled or approved is
    // cancelled by the worker's sweep at its retry time.
    if world.move_deadlines(true).await? > 0 {
        world.quiesce().await?;
    }
    // And a start after the end changes nothing either: also not for a
    // cancelled Task, whose Cancel ended the attempt parked behind it in its
    // own transaction (no import of the old build's merge intent again).
    let settled = world.upgrade_print().await?;
    world.crash_and_restart().await?;
    world.quiesce().await?;
    let again = world.upgrade_print().await?;
    if settled != again {
        return Err(format!(
            "{name}: a start after settlement changed the state:\n before {settled:#?}\n after {again:#?}"
        ));
    }
    eprintln!("UPGRADE {name}: settled: {}", settled.tasks.join("\n  "));
    // One writer per merge across the upgrade: at no instant did more than
    // one of the old merge's hooks step, the import continuation and the
    // queue worker's driver hold a Task's merge.
    use services::integration_worker::holders::{merge_holders, MergeHolder};
    let holders_db = db::SqliteDb::new(world.live.as_ref().expect("the server is up").pool.clone());
    for task in &world.tasks {
        let (most, overlap, seen) = merge_holders(&holders_db, task);
        eprintln!("UPGRADE {name}: merge holders of {task}: {seen:?}, at most {most} at once");
        if most > 1 {
            return Err(format!(
                "{name}: two writers held the merge of {task} at once: {overlap:?} (seen {seen:?})"
            ));
        }
    }
    // The two shapes where the old merge's own step was still live at the
    // upgrade: that step and the worker both acted, one after the other.
    if take.is_none()
        && matches!(
            name,
            "p05_rebase_target_no_outcome" | "p02_merge_intent_landed_no_outcome"
        )
    {
        let (_, _, seen) = merge_holders(&holders_db, &world.tasks[world.tasks.len() - 1]);
        for expected in [MergeHolder::LegacyHooksStep, MergeHolder::WorkerDriver] {
            if !seen.contains(&expected) {
                return Err(format!(
                    "{name} ({variant}): {expected:?} never held the merge; seen {seen:?}"
                ));
            }
        }
    }
    Ok(())
}

/// Each fixture twice: as the last pre-queue build left it (with the passive
/// "shadow" attempt that build recorded for a merge), and with every attempt
/// and queue row removed, which is what an install that never ran the passive
/// stages has (the importer classifies those Tasks from their own rows).
macro_rules! pre_queue {
    ($($test:ident, $bare:ident: $name:literal => $expected:ident;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $test() {
            log_to_stderr();
            if let Err(error) = pre_queue_upgrade($name, true, Upgraded::$expected, None).await {
                panic!("{error}");
            }
        }
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $bare() {
            log_to_stderr();
            if let Err(error) = pre_queue_upgrade($name, false, Upgraded::$expected, None).await {
                panic!("{error}");
            }
        }
    )*};
}

/// One owner command, through the real action endpoint, on a Task the
/// upgrade left stopped in its merge state: what it does to the Task's status
/// and epoch, and that the Task then merges (or, cancelled, stays cancelled).
macro_rules! pre_queue_command {
    ($($test:ident: $name:literal, $verb:literal;)*) => {$(
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $test() {
            log_to_stderr();
            if let Err(error) =
                pre_queue_upgrade($name, true, Upgraded::Parked, Some($verb)).await
            {
                panic!("{error}");
            }
        }
    )*};
}

pre_queue_command! {
    pre_queue_command_hold_under_a_paused_project: "p10_project_paused_at_merge_gate", "hold";
    pre_queue_command_release_a_held_task: "p13_held_in_merging", "release";
    pre_queue_command_restart_a_held_task: "p13_held_in_merging", "restart";
    pre_queue_command_approve_a_held_task: "p13_held_in_merging", "approve";
    pre_queue_command_cancel_a_held_task: "p13_held_in_merging", "cancel";
    pre_queue_command_retry_a_failed_merge: "p19_target_dirty", "retry";
    pre_queue_command_approve_a_failed_merge: "p19_target_dirty", "approve";
}

pre_queue! {
    pre_queue_p01_outcome_done_task_merging, pre_queue_bare_p01_outcome_done_task_merging: "p01_outcome_done_task_merging" => Reported;
    pre_queue_p02_merge_intent_landed_no_outcome, pre_queue_bare_p02_merge_intent_landed_no_outcome: "p02_merge_intent_landed_no_outcome" => Reported;
    pre_queue_p02_merge_intent_not_landed, pre_queue_bare_p02_merge_intent_not_landed: "p02_merge_intent_not_landed" => Merges;
    pre_queue_p04_pending_hooks, pre_queue_bare_p04_pending_hooks: "p04_pending_hooks" => Merges;
    pre_queue_p05_rebase_target_no_outcome, pre_queue_bare_p05_rebase_target_no_outcome: "p05_rebase_target_no_outcome" => Merges;
    pre_queue_p06_clean_rebase_in_merge_failed, pre_queue_bare_p06_clean_rebase_in_merge_failed: "p06_clean_rebase_in_merge_failed" => Merges;
    pre_queue_p06_clean_rebase_in_review, pre_queue_bare_p06_clean_rebase_in_review: "p06_clean_rebase_in_review" => Merges;
    pre_queue_p07_conflict_coder_repairing, pre_queue_bare_p07_conflict_coder_repairing: "p07_conflict_coder_repairing" => Repaired;
    pre_queue_p07_conflict_handoff_entry_pending, pre_queue_bare_p07_conflict_handoff_entry_pending: "p07_conflict_handoff_entry_pending" => Repaired;
    pre_queue_p09_back_at_merge_gate_after_bounce, pre_queue_bare_p09_back_at_merge_gate_after_bounce: "p09_back_at_merge_gate_after_bounce" => Merges;
    pre_queue_p10_project_paused_at_merge_gate, pre_queue_bare_p10_project_paused_at_merge_gate: "p10_project_paused_at_merge_gate" => Parked;
    pre_queue_p11_project_resumed_not_continued, pre_queue_bare_p11_project_resumed_not_continued: "p11_project_resumed_not_continued" => Merges;
    pre_queue_p13_held_in_merging_project_paused, pre_queue_bare_p13_held_in_merging_project_paused: "p13_held_in_merging_project_paused" => Parked;
    pre_queue_p13_held_in_merging, pre_queue_bare_p13_held_in_merging: "p13_held_in_merging" => Parked;
    pre_queue_p14_merge_failed_on_a_locked_index, pre_queue_bare_p14_merge_failed_on_a_locked_index: "p14_merge_failed_on_a_locked_index" => Repaired;
    pre_queue_p19_dirty_worktree, pre_queue_bare_p19_dirty_worktree: "p19_dirty_worktree" => Repaired;
    pre_queue_p19_target_dirty, pre_queue_bare_p19_target_dirty: "p19_target_dirty" => Parked;
}

/// A coordination root and its subtask: the subtask's work lands on the
/// root's branch, and the root merges through the queue like any Task (one
/// completed attempt, its branch on main), with no owner action.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_coordination_root_merges_through_the_queue() {
    log_to_stderr();
    let run = async {
        let mut world = World::new().await;
        world.create(None).await?;
        let child = world.create(Some(0)).await?;
        if child.starts_with("refused") {
            return Err(format!("the subtask was {child}"));
        }
        world.quiesce().await?;
        world.drive_to_settlement().await?;
        let owner: Vec<&String> = world
            .trace
            .iter()
            .filter(|line| line.contains("(0)") || line.contains("(1)"))
            .collect();
        if !owner.is_empty() {
            return Err(format!("an owner action was needed: {owner:#?}"));
        }
        for index in 0..world.tasks.len() {
            let task = world.task(index).await?;
            if task["status"] != "done" {
                return Err(format!(
                    "task {index} ended {}: {}; trace {:#?}",
                    task["status"], task["condition"], world.trace
                ));
            }
        }
        let attempts: Vec<(String, String)> = sqlx::query_as(
            "SELECT task_ref, state FROM integration_attempt ORDER BY created_at, rowid",
        )
        .fetch_all(&world.live().pool)
        .await
        .map_err(|error| error.to_string())?;
        let of = |index: usize| -> Vec<&str> {
            attempts
                .iter()
                .filter(|(task, _)| task == &world.tasks[index])
                .map(|(_, state)| state.as_str())
                .collect()
        };
        if of(0).last() != Some(&"completed") {
            return Err(format!(
                "the root has no completed attempt: root {:?}, subtask {:?}",
                of(0),
                of(1)
            ));
        }
        if !of(1).is_empty() {
            return Err(format!("a subtask was in the queue: {:?}", of(1)));
        }
        let tip = git(
            &world.dir.path().join("repo"),
            &[
                "rev-parse",
                &format!("refs/heads/task/{}", &world.tasks[0][..8]),
            ],
        )?;
        if !world.on_main(&tip) {
            return Err(format!("the root's branch {tip} is not on main"));
        }
        for index in 0..2 {
            let file = world.dir.path().join("repo").join(deliverable(index));
            let tracked = git(
                &world.dir.path().join("repo"),
                &[
                    "cat-file",
                    "-e",
                    &format!("refs/heads/main:{}", deliverable(index)),
                ],
            );
            if tracked.is_err() {
                return Err(format!("main lacks {}", file.display()));
            }
        }
        Ok(())
    };
    if let Err(error) = run.await {
        panic!("{error}");
    }
}
