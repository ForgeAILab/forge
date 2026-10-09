//! Server owner gate and attempt receipt sink. No worker or Task projection.
use crate::{
    integration_effects::{self, EffectOwner, EffectWorkspace},
    Result, ServiceError,
};
use api_types::WorkspaceOwnerOperationOutcome;
use db::{
    IntegrationEffectAdmission, IntegrationEffectRequest, IntegrationOperationKind,
    IntegrationOperationState, IntegrationOwnerFence, SqliteDb,
};
use serde_json::json;
use std::{path::Path, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerEffectRefusal {
    StaleFence,
    ForeignOwner,
    WitnessMismatch,
    ReconciliationRequired,
    RequestConflict,
}
impl From<db::IntegrationEffectRefusal> for OwnerEffectRefusal {
    fn from(v: db::IntegrationEffectRefusal) -> Self {
        use db::IntegrationEffectRefusal as R;
        match v {
            R::StaleFence => Self::StaleFence,
            R::ForeignOwner => Self::ForeignOwner,
            R::ReconciliationRequired => Self::ReconciliationRequired,
            R::RequestConflict => Self::RequestConflict,
        }
    }
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OwnerRebaseReceipt {
    NotPerformed {},
    Completed {
        outcome: WorkspaceOwnerOperationOutcome,
    },
    Refused {
        reason: OwnerEffectRefusal,
    },
    Cancelled {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    TimedOut {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    Infrastructure {
        message: String,
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
}

pub struct ServerRebaseRequest<'a> {
    pub fence: &'a IntegrationOwnerFence,
    pub workspace: &'a EffectWorkspace,
    pub target_branch: &'a str,
    pub expected_head_sha: &'a str,
    pub expected_target_sha: &'a str,
    pub handoff_conflicts: bool,
    pub deadline: Duration,
    pub cancel: &'a CancellationToken,
}

pub struct ServerIntegrationOwner {
    db: Arc<SqliteDb>,
}
impl ServerIntegrationOwner {
    pub fn new(db: Arc<SqliteDb>) -> Self {
        Self { db }
    }
    /// Reconcile under the same physical-owner lock before admitting another
    /// effect. A restarted owner can prove an exact reviewed merge; an unknown
    /// run never becomes permission to repeat it.
    pub async fn reconcile_outstanding(&self) -> Result<()> {
        self.reconcile_checkout(None).await
    }

    async fn reconcile_checkout(&self, current: Option<&IntegrationEffectRequest>) -> Result<()> {
        for request in self
            .db
            .outstanding_integration_effects("server", None)
            .await?
        {
            if current.is_some_and(|current| {
                current.fence.target_owner["location_id"]
                    != request.fence.target_owner["location_id"]
                    || (matches!(
                        request.kind,
                        IntegrationOperationKind::Rebase | IntegrationOperationKind::Check
                    ) && request.witness["workspace"]["handle"]
                        != current.witness["workspace"]["handle"])
            }) {
                continue;
            }
            let Some(guard) = self.db.lock_integration_reconciliation(&request).await? else {
                continue;
            };
            if !guard.started {
                guard
                    .record(
                        json!({"kind":"not_performed"}),
                        IntegrationOperationState::Failed,
                    )
                    .await?;
                continue;
            }
            let result = self.server_reconciliation_result(&request).await;
            let (result, state) = match result {
                Ok(Some(result)) => (result, IntegrationOperationState::Succeeded),
                Ok(None) | Err(_) => (
                    json!({"kind":"infrastructure","message":"owner effect has no exact completion proof","head_sha":null,"rebase_in_progress":true}),
                    IntegrationOperationState::Uncertain,
                ),
            };
            guard.record(result, state).await?;
        }
        Ok(())
    }

    async fn server_reconciliation_result(
        &self,
        request: &IntegrationEffectRequest,
    ) -> Result<Option<serde_json::Value>> {
        let legacy = request.fence.lease_owner.starts_with("task-step:");
        if legacy && request.kind == IntegrationOperationKind::Rebase {
            let path = request.witness["workspace"]["handle"]
                .as_str()
                .unwrap_or_default();
            let target = request.witness["expected_target_sha"]
                .as_str()
                .unwrap_or_default();
            if path.is_empty() || target.is_empty() {
                return Ok(None);
            }
            let proof = tokio::time::timeout(Duration::from_secs(2), async {
                let path = Path::new(path);
                if git::detect_rebase_in_progress(path).await?
                    || !git::command_output(path, &["merge-base", "--is-ancestor", target, "HEAD"])
                        .await?
                        .status
                        .success()
                {
                    return Ok::<_, ServiceError>(None);
                }
                let paths = if request.witness["handoff_conflicts"] == true {
                    git::paths_adding_conflict_markers(path, target, "HEAD").await?
                } else {
                    Vec::new()
                };
                let outcome = if paths.is_empty() {
                    WorkspaceOwnerOperationOutcome::Rebased
                } else {
                    WorkspaceOwnerOperationOutcome::Conflict {
                        details: "resumed committed conflict handoff".into(),
                        conflict_paths: paths,
                    }
                };
                Ok(Some(json!(OwnerRebaseReceipt::Completed { outcome })))
            })
            .await;
            return match proof {
                Ok(result) => result,
                Err(_) => Ok(None),
            };
        }
        let path: Option<String> =
            sqlx::query_scalar("SELECT path FROM repo_location WHERE id=? AND owner_kind='server'")
                .bind(request.fence.target_owner["location_id"].as_str())
                .fetch_optional(self.db.pool())
                .await
                .map_err(db::DbError::from)?;
        let Some(path) = path else {
            return Ok(None);
        };
        let path = Path::new(&path);
        if !matches!(
            request.kind,
            IntegrationOperationKind::FastForward | IntegrationOperationKind::Merge
        ) || request.witness["reviewed"].is_null()
        {
            return Ok(None);
        }
        let candidate = request.witness["expected_head_sha"]
            .as_str()
            .unwrap_or_default();
        let branch = request.witness["target_branch"]
            .as_str()
            .unwrap_or_default();
        let before = request.witness["expected_target_sha"]
            .as_str()
            .unwrap_or_default();
        if candidate.is_empty() || before.is_empty() || branch.is_empty() {
            return Ok(None);
        }
        let proof = tokio::time::timeout(Duration::from_secs(2), async {
            if legacy {
                let target = integration_effects::merge::target_tip(path, branch).await?;
                return Ok::<_, ServiceError>(
                    git::command_output(path, &["merge-base", "--is-ancestor", candidate, &target])
                        .await?
                        .status
                        .success(),
                );
            }
            Ok::<_, ServiceError>(
                git::get_current_sha(path).await? == candidate
                    && integration_effects::merge::target_tip(path, branch).await? == candidate
                    && git::command_output(
                        path,
                        &["merge-base", "--is-ancestor", before, candidate],
                    )
                    .await?
                    .status
                    .success(),
            )
        })
        .await;
        if !matches!(proof, Ok(Ok(true))) {
            return Ok(None);
        }
        Ok(Some(json!(OwnerMergeReceipt::Completed {
            outcome: crate::MergeOutcome::Done {
                before_sha: before.to_owned(),
                after_sha: candidate.to_owned(),
                branch: branch.to_owned()
            }
        })))
    }

    /// Today's recorder keeps its phased read/authority ordering. Bind its
    /// existing facts to the current Task-step fence, not a queue claim.
    pub(crate) async fn admit_task_step(
        &self,
        workspace: &EffectWorkspace,
        kind: IntegrationOperationKind,
        witness: serde_json::Value,
    ) -> Result<Option<IntegrationEffectAdmission>> {
        let Some(request) = self
            .db
            .task_step_integration_request(&workspace.workspace_id, kind, witness)
            .await?
        else {
            return Ok(None);
        };
        self.reconcile_checkout(Some(&request)).await?;
        Ok(Some(
            match self.db.begin_integration_effect(request).await? {
                IntegrationEffectAdmission::Started(mut guard) => {
                    if let Some(refusal) = guard.start().await? {
                        return Ok(Some(IntegrationEffectAdmission::Refused(refusal)));
                    }
                    if guard.server_workspace_paths().await?.is_none() {
                        let receipt = guard
                            .record(
                                json!({"kind":"refused","reason":"witness_mismatch"}),
                                IntegrationOperationState::Failed,
                            )
                            .await?;
                        IntegrationEffectAdmission::Replay(receipt)
                    } else {
                        IntegrationEffectAdmission::Started(guard)
                    }
                }
                admission => admission,
            },
        ))
    }

    pub async fn rebase(&self, input: ServerRebaseRequest<'_>) -> Result<OwnerRebaseReceipt> {
        // Owner mismatch is refused before any Git (including fact queries).
        if input.workspace.owner != EffectOwner::Server
            || input.fence.target_owner["owner_kind"] != "server"
        {
            return Ok(OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            });
        }
        let request = IntegrationEffectRequest {
            fence: input.fence.clone(),
            kind: IntegrationOperationKind::Rebase,
            witness: json!({"workspace":input.workspace,"target_branch":input.target_branch,"expected_head_sha":input.expected_head_sha,"expected_target_sha":input.expected_target_sha,"handoff_conflicts":input.handoff_conflicts,"deadline_nanos":input.deadline.as_nanos().to_string()}),
        };
        self.reconcile_checkout(Some(&request)).await?;
        let mut guard = match self.db.begin_integration_effect(request).await? {
            IntegrationEffectAdmission::Replay(r) => {
                return serde_json::from_value(r.result).map_err(|e| {
                    ServiceError::invalid_operation(format!("invalid owner rebase receipt: {e}"))
                })
            }
            IntegrationEffectAdmission::Refused(r) => {
                return Ok(OwnerRebaseReceipt::Refused { reason: r.into() })
            }
            IntegrationEffectAdmission::Started(g) => g,
        };
        if let Some(refusal) = guard.start().await? {
            return Ok(OwnerRebaseReceipt::Refused {
                reason: refusal.into(),
            });
        }
        let Some((path, target_path)) = guard.server_workspace_paths().await? else {
            let outcome = OwnerRebaseReceipt::Refused {
                reason: OwnerEffectRefusal::WitnessMismatch,
            };
            guard.record(json!(outcome), rebase_state(&outcome)).await?;
            return Ok(outcome);
        };
        let operation = async {
            match verify_objects(
                &path,
                &target_path,
                input.target_branch,
                input.expected_head_sha,
                input.expected_target_sha,
            )
            .await
            {
                Err(error) => interrupted(&path, false, Some(error.to_string())).await,
                Ok(false) => OwnerRebaseReceipt::Refused {
                    reason: OwnerEffectRefusal::WitnessMismatch,
                },
                Ok(true) => {
                    let effect = integration_effects::rebase::RebaseEffectInput {
                        workspace: input.workspace,
                        worktree_path: &path,
                        target_branch: input.expected_target_sha,
                        handoff_conflicts: input.handoff_conflicts,
                        expected_head_sha: Some(input.expected_head_sha),
                        expected_target_sha: Some(input.expected_target_sha),
                        deadline: None,
                    };
                    match integration_effects::rebase::rebase(&effect).await {
                        Ok(outcome) => OwnerRebaseReceipt::Completed { outcome },
                        Err(error) => interrupted(&path, false, Some(error.to_string())).await,
                    }
                }
            }
        };
        let outcome = tokio::select! {
            biased;
            _ = input.cancel.cancelled() => interrupted(&path, false, None).await,
            result = tokio::time::timeout(input.deadline, operation) => match result {
                Ok(outcome) => outcome,
                Err(_) => interrupted(&path, true, None).await,
            }
        };
        guard.record(json!(outcome), rebase_state(&outcome)).await?;
        Ok(outcome)
    }
}
async fn verify_objects(
    path: &Path,
    target_path: &Path,
    target: &str,
    head: &str,
    target_sha: &str,
) -> Result<bool> {
    if head.is_empty() || target_sha.is_empty() {
        return Ok(false);
    }
    Ok(git::get_current_sha(path).await? == head
        && integration_effects::merge::target_tip(target_path, target).await? == target_sha
        && integration_effects::merge::target_tip(path, target).await? == target_sha)
}
async fn interrupted(path: &Path, timed_out: bool, message: Option<String>) -> OwnerRebaseReceipt {
    let (head_sha, rebase_in_progress) = tokio::time::timeout(Duration::from_secs(2), async {
        (
            git::get_current_sha(path).await.ok(),
            git::detect_rebase_in_progress(path).await.unwrap_or(true),
        )
    })
    .await
    .unwrap_or((None, true));
    match message {
        Some(message) => OwnerRebaseReceipt::Infrastructure {
            message: integration_effects::check::tail_bytes(&message, 4096),
            head_sha,
            rebase_in_progress,
        },
        None if timed_out => OwnerRebaseReceipt::TimedOut {
            head_sha,
            rebase_in_progress,
        },
        None => OwnerRebaseReceipt::Cancelled {
            head_sha,
            rebase_in_progress,
        },
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OwnerMergeReceipt {
    NotPerformed {},
    Cancelled {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    TimedOut {
        head_sha: Option<String>,
        rebase_in_progress: bool,
    },
    Completed {
        outcome: crate::MergeOutcome,
    },
    Refused {
        reason: OwnerEffectRefusal,
    },
    Infrastructure {
        message: String,
    },
}
pub struct ServerMergeRequest<'a> {
    pub fence: &'a IntegrationOwnerFence,
    pub workspace: &'a EffectWorkspace,
    pub target_branch: &'a str,
    pub task_branch: &'a str,
    pub expected_head_sha: &'a str,
    pub expected_target_sha: &'a str,
    pub reviewed: Option<integration_effects::merge::ReviewedMergeObject>,
    pub cancel: &'a CancellationToken,
    pub deadline: Option<Duration>,
}
impl ServerIntegrationOwner {
    /// The caller supplies its frozen review permit. The owner gate protects
    /// physical identity and objects, and grants no new review/Task authority.
    pub async fn merge(&self, input: ServerMergeRequest<'_>) -> Result<OwnerMergeReceipt> {
        if input.workspace.owner != EffectOwner::Server
            || input.fence.target_owner["owner_kind"] != "server"
        {
            return Ok(OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::ForeignOwner,
            });
        }
        let kind = if input.reviewed.is_some() {
            IntegrationOperationKind::FastForward
        } else {
            IntegrationOperationKind::Merge
        };
        let request = IntegrationEffectRequest {
            fence: input.fence.clone(),
            kind,
            witness: json!({"workspace":input.workspace,"target_branch":input.target_branch,"task_branch":input.task_branch,"expected_head_sha":input.expected_head_sha,"expected_target_sha":input.expected_target_sha,"reviewed":input.reviewed,"deadline_nanos":input.deadline.map(|duration| duration.as_nanos().to_string())}),
        };
        self.reconcile_checkout(Some(&request)).await?;
        let mut guard = match self.db.begin_integration_effect(request).await? {
            IntegrationEffectAdmission::Replay(r) => {
                return serde_json::from_value(r.result).map_err(|e| {
                    ServiceError::invalid_operation(format!("invalid owner merge receipt: {e}"))
                })
            }
            IntegrationEffectAdmission::Refused(r) => {
                return Ok(OwnerMergeReceipt::Refused { reason: r.into() })
            }
            IntegrationEffectAdmission::Started(g) => g,
        };
        if let Some(refusal) = guard.start().await? {
            return Ok(OwnerMergeReceipt::Refused {
                reason: refusal.into(),
            });
        }
        let outcome = match guard.server_workspace_paths().await? {
            None => OwnerMergeReceipt::Refused {
                reason: OwnerEffectRefusal::WitnessMismatch,
            },
            Some((path, target)) => {
                let effect = integration_effects::merge::MergeEffectInput {
                    workspace: input.workspace,
                    worktree_path: &path,
                    repo_path: &target,
                    target_branch: input.target_branch,
                    task_branch: input.task_branch,
                    diagnostic_entity_id: &input.fence.attempt_id,
                    before_sha: input.expected_target_sha,
                    expected_head_sha: input.expected_head_sha,
                    observed_target_sha: input.expected_target_sha,
                    reviewed: input.reviewed,
                };
                let deadline = async {
                    match input.deadline {
                        Some(duration) => tokio::time::sleep(duration).await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    biased;
                    _ = input.cancel.cancelled() => merge_interrupted(&target, false).await,
                    _ = deadline => merge_interrupted(&target, true).await,
                    result = server_merge(&effect) => match result {
                        Ok(Some(outcome)) => OwnerMergeReceipt::Completed { outcome },
                        Ok(None) => OwnerMergeReceipt::Refused { reason: OwnerEffectRefusal::WitnessMismatch },
                        Err(e) => OwnerMergeReceipt::Infrastructure { message: integration_effects::check::tail_bytes(&e.to_string(), 4096) },
                    }
                }
            }
        };
        let state = match &outcome {
            OwnerMergeReceipt::Completed { .. } => IntegrationOperationState::Succeeded,
            OwnerMergeReceipt::Refused { .. }
            | OwnerMergeReceipt::NotPerformed { .. }
            | OwnerMergeReceipt::Cancelled { .. }
            | OwnerMergeReceipt::TimedOut { .. } => IntegrationOperationState::Failed,
            OwnerMergeReceipt::Infrastructure { .. } => IntegrationOperationState::Uncertain,
        };
        guard.record(json!(outcome), state).await?;
        Ok(outcome)
    }
}
async fn merge_interrupted(path: &Path, timed_out: bool) -> OwnerMergeReceipt {
    let (head_sha, rebase_in_progress) = tokio::time::timeout(Duration::from_secs(2), async {
        (
            git::get_current_sha(path).await.ok(),
            git::detect_rebase_in_progress(path).await.unwrap_or(true),
        )
    })
    .await
    .unwrap_or((None, true));
    if timed_out {
        OwnerMergeReceipt::TimedOut {
            head_sha,
            rebase_in_progress,
        }
    } else {
        OwnerMergeReceipt::Cancelled {
            head_sha,
            rebase_in_progress,
        }
    }
}

async fn server_merge(
    input: &integration_effects::merge::MergeEffectInput<'_>,
) -> Result<Option<crate::MergeOutcome>> {
    use integration_effects::merge::*;
    if input.expected_head_sha.is_empty()
        || input.observed_target_sha.is_empty()
        || git::get_current_sha(input.worktree_path).await? != input.expected_head_sha
        || target_tip(input.repo_path, input.target_branch).await? != input.observed_target_sha
    {
        return Ok(None);
    }
    if let Some(outcome) = merge_cleanliness(input.worktree_path, input.repo_path).await? {
        return Ok(Some(outcome));
    }
    let already = match validate_merge_candidate(input).await? {
        MergeCandidateOutcome::Ready { already_merged } => already_merged,
        MergeCandidateOutcome::Refused(outcome) => return Ok(Some(outcome)),
    };
    let applied = apply_merge(input, already).await?;
    Ok(Some(merge_result(input, already, applied).await?))
}

fn rebase_state(outcome: &OwnerRebaseReceipt) -> IntegrationOperationState {
    match outcome {
        OwnerRebaseReceipt::Completed { .. } => IntegrationOperationState::Succeeded,
        OwnerRebaseReceipt::Infrastructure { .. } => IntegrationOperationState::Uncertain,
        _ => IntegrationOperationState::Failed,
    }
}

/// Validate owner facts before persistence or journal acknowledgement.
pub(crate) fn validate_attempt_receipt(receipt: &db::IntegrationEffectReceipt) -> Result<()> {
    let invalid = || {
        ServiceError::invalid_operation(
            "owner attempt receipt contradicts its frozen request or state",
        )
    };
    let completed = receipt.result["kind"] == "completed";
    if (receipt.operation_state == IntegrationOperationState::Succeeded) != completed {
        return Err(invalid());
    }
    match receipt.request.kind {
        IntegrationOperationKind::Merge | IntegrationOperationKind::FastForward => {
            let outcome: OwnerMergeReceipt =
                serde_json::from_value(receipt.result.clone()).map_err(|_| invalid())?;
            if let OwnerMergeReceipt::Completed {
                outcome:
                    crate::MergeOutcome::Done {
                        after_sha, branch, ..
                    },
            } = outcome
            {
                let witness = &receipt.request.witness;
                if witness["target_branch"].as_str() != Some(&branch) {
                    return Err(invalid());
                }
                let reviewed = witness["reviewed"]["commit_sha"].as_str();
                let expected = if receipt.request.kind == IntegrationOperationKind::FastForward {
                    witness["expected_head_sha"].as_str()
                } else {
                    reviewed
                };
                if expected.is_some_and(|head| head != after_sha) {
                    return Err(invalid());
                }
            }
        }
        IntegrationOperationKind::Rebase => {
            let outcome: OwnerRebaseReceipt =
                serde_json::from_value(receipt.result.clone()).map_err(|_| invalid())?;
            if matches!(
                outcome,
                OwnerRebaseReceipt::Completed {
                    outcome: WorkspaceOwnerOperationOutcome::Applied
                        | WorkspaceOwnerOperationOutcome::ReviewCheckout { .. }
                }
            ) {
                return Err(invalid());
            }
        }
        IntegrationOperationKind::Check => {}
        IntegrationOperationKind::Reconcile => return Err(invalid()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{IntegrationAttempt, IntegrationQueueRepo};
    struct Fixture {
        _temp: tempfile::TempDir,
        db: Arc<SqliteDb>,
        owner: ServerIntegrationOwner,
        workspace: EffectWorkspace,
        fence: IntegrationOwnerFence,
        repo: std::path::PathBuf,
        tree: std::path::PathBuf,
        head: String,
        target: String,
        cancel: CancellationToken,
    }
    async fn git_at(path: &Path, args: &[&str]) -> String {
        let out = tokio::process::Command::new("git")
            .args(args)
            .current_dir(path)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .output()
            .await
            .unwrap();
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    }
    async fn fixture() -> Fixture {
        fixture_with_storage(false).await
    }
    async fn fixture_with_storage(durable: bool) -> Fixture {
        let temp = tempfile::tempdir().unwrap();
        let repo = temp.path().join("repo");
        std::fs::create_dir(&repo).unwrap();
        git::init(&repo).await.unwrap();
        std::fs::write(repo.join("base"), "base\n").unwrap();
        git::commit_all(&repo, "initial").await.unwrap();
        let target = git::get_current_sha(&repo).await.unwrap();
        let tree = temp.path().join("tree");
        git_at(
            &repo,
            &["worktree", "add", "-b", "candidate", tree.to_str().unwrap()],
        )
        .await;
        std::fs::write(tree.join("candidate"), "candidate\n").unwrap();
        git::commit_all(&tree, "candidate").await.unwrap();
        let head = git::get_current_sha(&tree).await.unwrap();
        let url = if durable {
            format!(
                "sqlite://{}?mode=rwc",
                temp.path().join("owner.sqlite").display()
            )
        } else {
            "sqlite::memory:".to_owned()
        };
        let pool = db::create_sqlite_pool(&url).await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let now = db::now_rfc3339();
        sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo(id,project_id,name,local_path,default_branch,created_at,updated_at) VALUES('r','p','r',?,'main',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','merging',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'candidate','ready',?,?)").bind(tree.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('pl','w','t','server','l',?,1,'ready','scheduler','{}',?,?)").bind(tree.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
        let q = db
            .create_or_get_integration_queue("r", "main")
            .await
            .unwrap();
        let a = db
            .admit_integration_attempt(IntegrationAttempt::new(
                Some(q.id.clone()),
                "t".into(),
                "p".into(),
                "owner-test".into(),
                "merging".into(),
                0,
                1,
            ))
            .await
            .unwrap();
        let q = db.integration_queue(&q.id).await.unwrap().unwrap();
        db.claim_integration_queue(
            &q.id,
            q.revision,
            "test-owner",
            &now,
            "2099-01-01T00:00:00Z",
        )
        .await
        .unwrap();
        let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
        let workspace = EffectWorkspace {
            workspace_id: "w".into(),
            placement_id: "pl".into(),
            generation: 1,
            owner: EffectOwner::Server,
            handle: tree.to_string_lossy().into_owned(),
        };
        let owner = ServerIntegrationOwner::new(db.clone());
        Fixture {
            _temp: temp,
            db,
            owner,
            workspace,
            fence,
            repo,
            tree,
            head,
            target,
            cancel: CancellationToken::new(),
        }
    }
    fn merge_request(f: &Fixture) -> ServerMergeRequest<'_> {
        ServerMergeRequest {
            cancel: &f.cancel,
            deadline: None,
            fence: &f.fence,
            workspace: &f.workspace,
            target_branch: "main",
            task_branch: "candidate",
            expected_head_sha: &f.head,
            expected_target_sha: &f.target,
            reviewed: Some(integration_effects::merge::ReviewedMergeObject {
                commit_sha: f.head.clone(),
                base_sha: f.target.clone(),
            }),
        }
    }
    #[test]
    fn owner_receipt_validation_refuses_wrong_object_and_false_success() {
        let request = IntegrationEffectRequest {
            fence: IntegrationOwnerFence {
                queue_id: "q".into(),
                attempt_id: "a".into(),
                generation: 1,
                lease_owner: "worker".into(),
                target_owner: json!({}),
            },
            kind: IntegrationOperationKind::FastForward,
            witness: json!({"target_branch":"main","expected_head_sha":"approved"}),
        };
        let mut receipt = db::IntegrationEffectReceipt {
            request,
            result: json!({"kind":"completed","outcome":crate::MergeOutcome::Done { before_sha: "before".into(), after_sha: "foreign".into(), branch: "main".into() }}),
            operation_state: IntegrationOperationState::Succeeded,
            recorded_at: db::now_rfc3339(),
        };
        assert!(validate_attempt_receipt(&receipt).is_err());
        receipt.result["outcome"]["Done"]["after_sha"] = json!("approved");
        validate_attempt_receipt(&receipt).unwrap();
        receipt.operation_state = IntegrationOperationState::Failed;
        assert!(validate_attempt_receipt(&receipt).is_err());
    }

    #[tokio::test]
    async fn lost_merge_reply_replays_one_receipt_without_git_changes() {
        let f = fixture().await;
        let result = f.owner.merge(merge_request(&f)).await.unwrap();
        assert!(matches!(
            result,
            OwnerMergeReceipt::Completed {
                outcome: crate::MergeOutcome::Done { .. }
            }
        ));
        let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
        let objects = git_at(&f.repo, &["count-objects", "-v"]).await;
        let reconnect = ServerIntegrationOwner::new(f.db.clone());
        let replay = reconnect.merge(merge_request(&f)).await.unwrap();
        assert_eq!(json!(result), json!(replay));
        assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
        assert_eq!(git_at(&f.repo, &["count-objects", "-v"]).await, objects);
        let n: i64 = sqlx::query_scalar(
            "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
        )
        .bind(&f.fence.attempt_id)
        .fetch_one(f.db.pool())
        .await
        .unwrap();
        assert_eq!(n, 1);
        assert_eq!(
            f.db.integration_attempt(&f.fence.attempt_id)
                .await
                .unwrap()
                .unwrap()
                .current_operation_state,
            Some(IntegrationOperationState::Succeeded)
        );
    }
    #[tokio::test]
    async fn restart_reconciles_outstanding_intent_before_a_new_effect() {
        let mut f = fixture_with_storage(true).await;
        let request = IntegrationEffectRequest {
            fence: f.fence.clone(),
            kind: IntegrationOperationKind::FastForward,
            witness: json!({"workspace":f.workspace,"target_branch":"main","task_branch":"candidate","expected_head_sha":f.head,"expected_target_sha":f.target,"reviewed":{"commit_sha":f.head,"base_sha":f.target},"deadline_nanos":null}),
        };
        let IntegrationEffectAdmission::Started(mut guard) =
            f.db.begin_integration_effect(request.clone())
                .await
                .unwrap()
        else {
            panic!("not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        git::checkout_branch(&f.repo, "main").await.unwrap();
        git_at(&f.repo, &["merge", "--ff-only", &f.head]).await;
        // Crash after Git but before receipt commit; reopen durable storage.
        drop(guard);
        f.db.pool().close().await;
        let url = format!(
            "sqlite://{}?mode=rwc",
            f._temp.path().join("owner.sqlite").display()
        );
        f.db = Arc::new(SqliteDb::new(db::create_sqlite_pool(&url).await.unwrap()));
        f.owner = ServerIntegrationOwner::new(f.db.clone());
        let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
        let result = f.owner.merge(merge_request(&f)).await.unwrap();
        assert!(matches!(
            result,
            OwnerMergeReceipt::Completed {
                outcome: crate::MergeOutcome::Done { .. }
            }
        ));
        assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
        let attempt =
            f.db.integration_attempt(&f.fence.attempt_id)
                .await
                .unwrap()
                .unwrap();
        assert!(attempt.effect_intent_json.is_none());
        assert_eq!(
            attempt.current_operation_state,
            Some(IntegrationOperationState::Succeeded)
        );
    }

    #[tokio::test]
    async fn stale_and_foreign_server_owner_are_refused_before_git_and_witness_is_checked() {
        for case in ["stale", "foreign", "placement", "head", "target"] {
            let mut f = fixture().await;
            let before = git_at(&f.repo, &["reflog", "--all"]).await;
            match case {
                "stale" => f.fence.generation += 1,
                "foreign" => f.fence.target_owner["generation"] = json!(999),
                "placement" => f.workspace.generation += 1,
                "head" => f.head = "bad-head".into(),
                "target" => f.target = "bad-target".into(),
                _ => unreachable!(),
            }
            if matches!(case, "stale" | "foreign" | "placement") {
                std::fs::rename(f.repo.join(".git"), f.repo.join("hidden-git")).unwrap();
            }
            let outcome = f.owner.merge(merge_request(&f)).await.unwrap();
            assert!(
                matches!(outcome, OwnerMergeReceipt::Refused { .. }),
                "case {case}: {outcome:?}"
            );
            if matches!(case, "stale" | "foreign" | "placement") {
                std::fs::rename(f.repo.join("hidden-git"), f.repo.join(".git")).unwrap();
            }
            assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, before);
        }
    }
    #[tokio::test]
    async fn duplicate_rebase_returns_receipt_and_preserves_objects_and_reflog() {
        let mut f = fixture().await;
        std::fs::write(f.repo.join("target"), "target\n").unwrap();
        git::commit_all(&f.repo, "advance target").await.unwrap();
        f.target = git::get_current_sha(&f.repo).await.unwrap();
        let cancel = CancellationToken::new();
        let request = || ServerRebaseRequest {
            fence: &f.fence,
            workspace: &f.workspace,
            target_branch: "main",
            expected_head_sha: &f.head,
            expected_target_sha: &f.target,
            handoff_conflicts: false,
            deadline: Duration::from_secs(5),
            cancel: &cancel,
        };
        let first = f.owner.rebase(request()).await.unwrap();
        assert!(matches!(first, OwnerRebaseReceipt::Completed { .. }));
        assert_ne!(git::get_current_sha(&f.tree).await.unwrap(), f.head);
        let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
        let objects = git_at(&f.repo, &["count-objects", "-v"]).await;
        let second = f.owner.rebase(request()).await.unwrap();
        assert_eq!(json!(first), json!(second));
        assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
        assert_eq!(git_at(&f.repo, &["count-objects", "-v"]).await, objects);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_mid_rebase_kills_git_and_hook_and_retains_recovery_facts() {
        use std::os::unix::fs::PermissionsExt;
        let mut f = fixture().await;
        std::fs::write(f.repo.join("target"), "target\n").unwrap();
        git::commit_all(&f.repo, "advance target").await.unwrap();
        f.target = git::get_current_sha(&f.repo).await.unwrap();
        let hook = f.repo.join(".git/hooks/pre-rebase");
        std::fs::write(
            &hook,
            "#!/bin/sh\necho $$ > hook.pid\necho $PPID > git.pid\nexec sleep 60\n",
        )
        .unwrap();
        std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755)).unwrap();
        let cancel = CancellationToken::new();
        let operation = f.owner.rebase(ServerRebaseRequest {
            fence: &f.fence,
            workspace: &f.workspace,
            target_branch: "main",
            expected_head_sha: &f.head,
            expected_target_sha: &f.target,
            handoff_conflicts: false,
            deadline: Duration::from_secs(20),
            cancel: &cancel,
        });
        let cancel_midway = async {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !f.tree.join("git.pid").exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            tokio::time::timeout(Duration::from_millis(250), async {
                let mut tx = db::begin_immediate(f.db.pool()).await.unwrap();
                sqlx::query("UPDATE repo SET name='writer during Git' WHERE id='r'")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            })
            .await
            .expect("Git effect held the SQLite writer");
            cancel.cancel();
        };
        let (outcome, _) = tokio::join!(operation, cancel_midway);
        let outcome = outcome.unwrap();
        assert!(matches!(outcome, OwnerRebaseReceipt::Cancelled { .. }));
        for file in ["git.pid", "hook.pid"] {
            let pid = std::fs::read_to_string(f.tree.join(file)).unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let alive = std::process::Command::new("kill")
                        .args(["-0", pid.trim()])
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .status()
                        .unwrap()
                        .success();
                    #[cfg(target_os = "linux")]
                    let alive = alive
                        && !(file == "hook.pid"
                            && std::fs::read_to_string(format!("/proc/{}/stat", pid.trim()))
                                .is_ok_and(|stat| {
                                    stat.rsplit_once(") ")
                                        .is_some_and(|(_, tail)| tail.starts_with('Z'))
                                }));
                    if !alive {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        assert_eq!(git::get_current_sha(&f.tree).await.unwrap(), f.head);
        assert!(!git::detect_rebase_in_progress(&f.tree).await.unwrap());
        let raw: String =
            sqlx::query_scalar("SELECT effect_receipts_json FROM integration_attempt WHERE id=?")
                .bind(&f.fence.attempt_id)
                .fetch_one(f.db.pool())
                .await
                .unwrap();
        assert!(raw.contains("cancelled"));
    }
}
