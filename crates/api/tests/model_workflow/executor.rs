//! Scripted model executor outcomes, commits, lead operations and step checks.

use super::*;

// ---------------------------------------------------------------------------
// Scripted executor
// ---------------------------------------------------------------------------

/// State the scripted executor shares with the model. One per boot: a crash
/// kills it, and its orphaned futures never return and never write.
pub(super) struct Script {
    pub(super) alive: AtomicBool,
    /// Task ID -> the coder execution waiting for a model action.
    pub(super) waiting: Mutex<HashMap<String, (String, tokio::sync::oneshot::Sender<Outcome>)>>,
    pub(super) shared: Arc<Shared>,
    pub(super) db: Arc<db::SqliteDb>,
    pub(super) service: OnceLock<Weak<services::TaskService>>,
}

/// Survives restarts: it is the model's memory, not the server's.
#[derive(Default)]
pub(super) struct Shared {
    pub(super) verdicts: Mutex<HashMap<String, bool>>,
    pub(super) index: Mutex<HashMap<String, usize>>,
    /// Subtask index -> its coordination root: a subtask's work is also the
    /// root's deliverable, which the root's aggregate review checks.
    pub(super) parents: Mutex<HashMap<usize, usize>>,
    pub(super) writes: AtomicU64,
    pub(super) lead_operations: Mutex<Vec<String>>,
    pub(super) lead_operation_errors: Mutex<Vec<String>>,
}

pub(super) struct ScriptedAdapter(pub(super) Arc<Script>);

pub(super) fn git(path: &Path, args: &[&str]) -> Result<String, String> {
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

pub(super) fn deliverable(index: usize) -> String {
    format!("ok-{index}.txt")
}

impl ScriptedAdapter {
    pub(super) fn commit(
        &self,
        ctx: &ExecutionContext,
        outcome: Outcome,
    ) -> Result<(), ExecutorError> {
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

    pub(super) async fn review(&self, ctx: &ExecutionContext) -> Result<(), ExecutorError> {
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
pub(super) const STEP_CHECK_PROMPT_MARK: &str = "Step-check doctrine:";

/// System text of the lead's prompt on its root (`planner.coordination.v1`).
pub(super) const LEAD_PROMPT_MARK: &str = "You are the lead of this Forge coordination root";

pub(super) const LEAD_AGENT_ID: &str = "model-lead";

impl ScriptedAdapter {
    pub(super) async fn replan_operations(
        &self,
        ctx: &ExecutionContext,
    ) -> Result<(), ExecutorError> {
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
    pub(super) async fn step_check(&self, ctx: &ExecutionContext) -> Result<(), ExecutorError> {
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

pub(super) fn result(status: ExecutionOutcome, error: Option<&str>) -> ExecutionResult {
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
