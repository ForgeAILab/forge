//! The operations of a coordination root's lead (plan 3.10 stage 4).
//!
//! Authority is decided in two places and nowhere else. The registry admits
//! the operation by the principal fact (`Principal::TaskAgent::lead_root_id`,
//! loaded from the running execution). [`TaskService::bind_lead`] is the one
//! command boundary: every operation below goes through it, and it repeats
//! the workflow's side of "this run leads this root now" and checks that the
//! target is the root or one of its subtasks. No operation checks scope
//! itself and none can skip the boundary: the bound value is the only way to
//! obtain the target.
//!
//! The effects are the existing commands: the adaptive Task command (create,
//! reorder), `update_task`, the Task action executor (cancel, recover), role
//! reassignment, and Task comments. What a lead can repeat has a machine
//! bound, held on the ROOT in the budget ledger so that nothing the lead does
//! to a subtask resets it (`db::budget::LEAD_*`).
use super::*;
use crate::LeadRefusal;
use api_types::{Actor, TaskAction};
use serde_json::{json, Value};

/// The running execution a lead operation is issued by. Every field is
/// server-derived: the identity and Task from the session scope, the
/// execution from the one running run of that session, the root from the
/// admitted principal.
#[derive(Debug, Clone)]
pub struct LeadRun {
    pub identity_id: String,
    pub execution_id: String,
    /// The Task the run executes on. A lead run executes on its root: a
    /// step check on a subtask is not one and is refused at the boundary.
    pub task_id: String,
    pub root_id: String,
}

/// A lead run bound to its root, and the target it may act on.
pub(crate) struct LeadBinding {
    pub run: LeadRun,
    pub root: Task,
    pub role: String,
}

/// Hand-off notes kept per subtask; the newest are the ones a run reads.
pub(crate) const MAX_HANDOFF_NOTES_PER_SUBTASK: i64 = 20;
/// Hand-off notes put into one implementation prompt.
pub(crate) const HANDOFF_NOTES_IN_PROMPT: usize = 5;
const READ_SECTION_BYTES: usize = 32 * 1024;
const READ_TEXT_CHARS: usize = 2000;
const READ_CHILDREN: usize = 100;

fn refused(refusal: LeadRefusal, message: impl Into<String>) -> ServiceError {
    ServiceError::LeadOperationRefused {
        refusal,
        message: message.into(),
    }
}

fn clip(text: &str, chars: usize) -> (String, bool) {
    if text.chars().count() <= chars {
        (text.to_owned(), false)
    } else {
        (text.chars().take(chars).collect(), true)
    }
}

/// Whether a subtask ever had an implementation run admitted. Such a subtask
/// has work in the shared workspace: the lead may recover it, reassign it or
/// leave it a note, and may not rewrite, reorder or drop it. Every run on a
/// subtask that is not its root's step check counts, whatever the Project's
/// workflow calls its implementation role (and an owner's interactive
/// session too: it had the workspace).
pub(crate) async fn subtask_started(db: &SqliteDb, task_id: &str) -> Result<bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM execution WHERE task_id = ? AND role <> ?)")
        .bind(task_id)
        .bind(db::inherited_subtask::STEP_CHECK_ROLE)
        .fetch_one(db.pool())
        .await
        .map_err(|error| db::DbError::from(error).into())
}

/// End the wait a lead's question put on `task`, whichever way it ends: the
/// owner answered (`marker` is the question's comment id), or the root left
/// its planning state by another route (`None`: whatever question is open).
/// Its Attention item is resolved with it. A Task that does not wait for a
/// lead's question is returned unchanged.
pub(crate) async fn clear_lead_input_wait(
    db: &SqliteDb,
    task: &Task,
    marker: Option<&str>,
) -> Result<Task> {
    let metadata: Value = task
        .metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or(Value::Null);
    if metadata["awaiting_human_reason"] != db::LEAD_INPUT_WAIT {
        return Ok(task.clone());
    }
    let Some(current) = metadata["awaiting_human_marker_id"].as_str() else {
        return Ok(task.clone());
    };
    if marker.is_some_and(|marker| marker != current) {
        return Ok(task.clone());
    }
    let updated = TaskRepo::mutate_metadata(
        db,
        &task.id,
        None,
        vec![db::TaskMetadataMutation::CompareAndMutate {
            key: "awaiting_human_marker_id".to_owned(),
            expected: json!(current),
            mutations: [
                "awaiting_human",
                "awaiting_human_reason",
                "awaiting_human_marker_id",
            ]
            .into_iter()
            // The asking run stays marked while the root plans (its
            // completion is never an Accept); it goes when the root left.
            .chain(marker.is_none().then_some("lead_input_execution_id"))
            .map(|key| db::TaskMetadataMutation::Remove {
                key: key.to_owned(),
            })
            .collect(),
        }],
        &now_rfc3339(),
    )
    .await?;
    let now = now_rfc3339();
    sqlx::query(
        "UPDATE attention_projection SET status='resolved', resolved_at=?, updated_at=?, version=version+1
         WHERE dedupe_key=? AND status!='resolved'",
    )
    .bind(&now)
    .bind(&now)
    .bind(lead_input_dedupe_key(&task.id, current))
    .execute(db.pool())
    .await
    .map_err(db::DbError::from)?;
    Ok(updated)
}

pub(crate) fn lead_input_dedupe_key(root_id: &str, comment_id: &str) -> String {
    format!("lead-input:{root_id}:{comment_id}")
}

/// Whether this execution ended by asking the owner a question. The
/// planning gate never accepts such a run, also after the answer arrived:
/// the root stays in its planning state and the lead runs again with the
/// answer in its input. The marker stays until the root leaves planning.
pub(crate) fn lead_asked_input(task: &Task, execution_id: &str) -> bool {
    task.metadata_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .is_some_and(|metadata| metadata["lead_input_execution_id"] == execution_id)
}

/// The owner's answer to a lead's question: a comment on the root (the
/// lead's next run reads it), the wait cleared, the root woken. No transition
/// happens, so no re-plan is charged.
pub(crate) async fn answer_lead_input(
    db: &SqliteDb,
    root_id: &str,
    comment_id: &str,
    answer: &str,
    author_name: &str,
) -> Result<Task> {
    let root = TaskRepo::get_by_id(db, root_id, false)
        .await?
        .ok_or_else(|| ServiceError::not_found("task", root_id.to_owned()))?;
    let now = now_rfc3339();
    TaskCommentRepo::create_comment(
        db,
        CreateTaskComment {
            id: new_uuid_v4(),
            task_id: root.id.clone(),
            author_type: CommentAuthorType::User,
            author_id: None,
            author_name: author_name.to_owned(),
            content: format!("Answer to the lead's question: {}", answer.trim()),
            execution_id: None,
            role: None,
            worklog_kind: None,
            idempotency_key: Some(format!("lead-input-answer:{comment_id}")),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await?;
    let root = clear_lead_input_wait(db, &root, Some(comment_id)).await?;
    crate::wake_task_dispatch(db, &root.id, "the owner answered the lead").await?;
    Ok(root)
}

impl TaskService {
    /// THE command boundary of every lead operation. `run` must be a running
    /// execution of the root's lead on the root, in the root's planning
    /// state, and `target`, when given, must be that root or one of its
    /// subtasks in the same Project. A step check on a subtask is not such a
    /// run. Anything else is a typed refusal and nothing is written.
    pub(crate) async fn bind_lead(
        &self,
        run: &LeadRun,
        target: Option<&str>,
    ) -> Result<(LeadBinding, Option<Task>)> {
        let not_leading = |why: &str| {
            refused(
                LeadRefusal::NotLeading,
                format!(
                    "this run does not lead coordination root {}: {why}",
                    run.root_id
                ),
            )
        };
        let execution = ExecutionRepo::get_by_id(&*self.db, &run.execution_id)
            .await?
            .filter(|execution| {
                execution.status == db::ExecutionStatus::Running
                    && execution.task_id == run.task_id
                    && execution.agent_id.as_deref() == Some(run.identity_id.as_str())
            })
            .ok_or_else(|| not_leading("its execution is not running"))?;
        let (root, workflow) =
            crate::task_hierarchy::coordination_root_context(&self.db, &run.root_id).await?;
        if root.parent_task_id.is_some() {
            return Err(not_leading("that Task is not a root"));
        }
        let policy = crate::task_hierarchy::RootRolePolicy::for_workflow(&workflow);
        let assignments = TaskRoleAssignmentRepo::list_by_task(&*self.db, &root.id).await?;
        let lead = policy
            .lead(&root.id, &assignments)
            .filter(|lead| lead.assignee_id.as_deref() == Some(run.identity_id.as_str()))
            .ok_or_else(|| not_leading("another Agent, or nobody, is its lead now"))?;
        if execution.role != lead.role_name {
            return Err(not_leading("its execution does not run the lead's role"));
        }
        if run.task_id != root.id {
            return Err(not_leading(
                "it does not run on the root (a step check returns a verdict and changes nothing)",
            ));
        }
        if policy.planning_state() != Some(root.status.as_str()) {
            return Err(not_leading("the root is not in its planning state"));
        }
        let target = match target {
            Some(id) => {
                Some(crate::task_hierarchy::ensure_in_lead_subtree(&self.db, &root, id).await?)
            }
            None => None,
        };
        Ok((
            LeadBinding {
                run: run.clone(),
                role: execution.role,
                root,
            },
            target,
        ))
    }

    /// A subtask of the bound root (never the root itself).
    fn lead_subtask_target(binding: &LeadBinding, target: Option<Task>) -> Result<Task> {
        target
            .filter(|task| task.id != binding.root.id)
            .ok_or_else(|| {
                refused(
                    LeadRefusal::WrongTarget,
                    "this operation takes a subtask of the root, not the root itself",
                )
            })
    }

    async fn ensure_subtask_not_started(&self, task: &Task) -> Result<()> {
        if subtask_started(&self.db, &task.id).await? {
            return Err(refused(
                LeadRefusal::SubtaskStarted,
                format!(
                    "subtask {} already had an implementation run; use lead.recover, lead.subtask assign or lead.handoff",
                    task.id
                ),
            ));
        }
        Ok(())
    }

    /// A lead never makes itself a worker: its runs read the repository, and
    /// a worker's run writes it. The owner may give one Agent both roles; a
    /// lead's own operation may not.
    fn ensure_not_self_assignment(run: &LeadRun, agent_id: &str) -> Result<()> {
        if agent_id == run.identity_id {
            return Err(refused(
                LeadRefusal::SelfAssignment,
                "a lead does not assign itself as a worker; name another Agent that is usable in this Project",
            ));
        }
        Ok(())
    }

    fn ensure_version(task: &Task, version: i64) -> Result<()> {
        if task.version != version {
            return Err(db::DbError::TaskVersionConflict {
                expected: version,
                actual: task.version,
            }
            .into());
        }
        Ok(())
    }

    /// One comment with the lead's execution and role as provenance.
    async fn lead_comment(
        &self,
        binding: &LeadBinding,
        task_id: &str,
        kind: &str,
        content: &str,
        key: &str,
    ) -> Result<TaskComment> {
        let now = now_rfc3339();
        let comment = TaskCommentRepo::create_comment(
            &*self.db,
            CreateTaskComment {
                id: new_uuid_v4(),
                task_id: task_id.to_owned(),
                author_type: CommentAuthorType::Agent,
                author_id: Some(binding.run.identity_id.clone()),
                author_name: "lead".to_owned(),
                content: content.to_owned(),
                execution_id: Some(binding.run.execution_id.clone()),
                role: Some(binding.role.clone()),
                worklog_kind: Some(kind.to_owned()),
                idempotency_key: Some(format!("lead:{kind}:{key}")),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?;
        if comment.content != content || comment.worklog_kind.as_deref() != Some(kind) {
            return Err(db::DbError::IdempotencyConflict.into());
        }
        self.publish(ForgeEvent {
            event_type: "comment.created".to_owned(),
            entity_id: comment.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::CommentCreated {
                task_id: task_id.to_owned(),
                comment_id: comment.id.clone(),
                author_type: "agent".to_owned(),
                author_name: comment.author_name.clone(),
            },
        });
        Ok(comment)
    }

    /// Charge one unit of a lead bound on the root. `Ok(false)`: this very
    /// request was charged before (a retry of the same key). A spent bound
    /// is a typed refusal.
    async fn charge_lead_bound(
        &self,
        root: &Task,
        charges: &[(String, i64)],
        key: &str,
        what: &str,
    ) -> Result<bool> {
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let mut fresh = false;
        for (budget, limit) in charges {
            let charge = db::budget::charge(&mut tx, &root.id, budget, *limit, key).await?;
            if charge.charged {
                fresh = true;
                continue;
            }
            let recorded: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM task_budget_charge c JOIN task_budget b USING(task_id,kind) WHERE c.task_id=? AND c.kind=? AND c.window_id=b.window_id AND c.step_id=?)",
            )
            .bind(&root.id)
            .bind(budget)
            .bind(key)
            .fetch_one(&mut *tx)
            .await
            .map_err(db::DbError::from)?;
            if !recorded {
                // Dropping the transaction takes back the charges made above.
                return Err(refused(
                    LeadRefusal::LimitReached,
                    format!(
                        "{what}: the limit of {limit} is reached for this root; leave the rest to the owner or the Project Agent (a Retry with a new budget on the root opens a new window)"
                    ),
                ));
            }
        }
        tx.commit().await.map_err(db::DbError::from)?;
        Ok(fresh)
    }

    /// The bounded read of the lead's subtree.
    pub async fn lead_read(&self, run: &LeadRun, task_id: Option<&str>) -> Result<Value> {
        let (binding, target) = self.bind_lead(run, task_id).await?;
        let root = &binding.root;
        let children = crate::task_hierarchy::ordered_children(&self.db, &root.id).await?;
        let mut truncated = children.len() > READ_CHILDREN;
        let mut rows = Vec::new();
        let mut bytes = 0usize;
        for child in children.iter().take(READ_CHILDREN) {
            let (description, clipped) =
                clip(child.description.as_deref().unwrap_or_default(), 400);
            let notes: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM task_comment WHERE task_id=? AND worklog_kind=?",
            )
            .bind(&child.id)
            .bind(db::COMMENT_KIND_HANDOFF)
            .fetch_one(self.db.pool())
            .await
            .map_err(db::DbError::from)?;
            let row = json!({
                "task_id": child.id,
                "version": child.version,
                "order": child.subtask_order,
                "title": child.title,
                "description": description,
                "description_truncated": clipped,
                "status": child.status,
                "condition": child.condition.public(),
                "started": subtask_started(&self.db, &child.id).await?,
                "lead_decides_park": crate::task_hierarchy::subtask_decision_park(child).is_some(),
                "handoff_notes": notes,
            });
            bytes += row.to_string().len();
            if bytes > READ_SECTION_BYTES {
                truncated = true;
                break;
            }
            rows.push(row);
        }
        let mut out = json!({
            "operation": "lead.read",
            "root": {"task_id": root.id, "version": root.version, "status": root.status, "title": root.title},
            "subtasks": rows,
            "truncated": truncated,
        });
        if let Some(task) = target.filter(|task| task.id != root.id) {
            let (description, clipped) = clip(
                task.description.as_deref().unwrap_or_default(),
                READ_TEXT_CHARS * 8,
            );
            let executions: Vec<(String, String, Option<String>, String)> = sqlx::query_as(
                "SELECT role, status, error, created_at FROM execution WHERE task_id=? ORDER BY created_at DESC, id DESC LIMIT 5",
            )
            .bind(&task.id)
            .fetch_all(self.db.pool())
            .await
            .map_err(db::DbError::from)?;
            let comments: Vec<(String, Option<String>, String, String)> = sqlx::query_as(
                "SELECT author_name, worklog_kind, content, created_at FROM task_comment WHERE task_id=? ORDER BY created_at DESC, id DESC LIMIT 10",
            )
            .bind(&task.id)
            .fetch_all(self.db.pool())
            .await
            .map_err(db::DbError::from)?;
            let offers = self
                .task_action_offers(&task.id, &Actor::agent(&run.identity_id))
                .await?;
            out["subtask"] = json!({
                "task_id": task.id,
                "version": task.version,
                "status": task.status,
                "description": description,
                "description_truncated": clipped,
                "condition": task.condition.public(),
                "recovery_actions": offers.available_actions.iter().map(|offer| json!({"action": offer.action, "reason": offer.reason})).collect::<Vec<_>>(),
                "executions": executions.into_iter().map(|(role, status, error, at)| json!({"role": role, "status": status, "error": error.map(|error| clip(&error, 500).0), "created_at": at})).collect::<Vec<_>>(),
                "comments": comments.into_iter().map(|(author, kind, content, at)| json!({"author": author, "kind": kind, "content": clip(&content, READ_TEXT_CHARS).0, "created_at": at})).collect::<Vec<_>>(),
            });
        }
        Ok(out)
    }

    /// Append one subtask to the root. The lead is the command's actor and
    /// `key` its idempotency key: the same key never creates a second one.
    pub async fn lead_create_subtask(
        &self,
        run: &LeadRun,
        title: String,
        description: Option<String>,
        assignee_id: Option<String>,
        key: &str,
    ) -> Result<Value> {
        let (binding, _) = self.bind_lead(run, None).await?;
        let root = &binding.root;
        validate_required("title", &title)?;
        // An exact retry answers with what the first call created, whatever
        // changed since (the adaptive receipt is keyed on the lead and key).
        if let Some(receipt) = db::CommandReceiptRepo::get_command_receipt_by_identity(
            &*self.db,
            db::inherited_subtask::TASK_LEAD_PRINCIPAL,
            &run.identity_id,
            "project",
            &root.project_id,
            super::adaptive::TASK_ADAPTIVE_COMMAND,
            &Self::lead_command_key(&binding, key),
        )
        .await?
        {
            let outcome: Value = serde_json::from_str(&receipt.outcome_json).unwrap_or(Value::Null);
            let created = outcome["tasks"]
                .as_array()
                .and_then(|tasks| tasks.first())
                .filter(|task| task["title"] == title.as_str())
                .ok_or(db::DbError::IdempotencyConflict)?;
            return Ok(json!({
                "operation": "lead.subtask", "action": "create", "replayed": true,
                "task_id": created["id"], "version": created["version"], "status": created["status"],
            }));
        }
        let (_, workflow) =
            crate::task_hierarchy::coordination_root_context(&self.db, &root.id).await?;
        crate::task_hierarchy::ensure_parent_accepts_subtasks(root, &workflow)?;
        if let Some(agent_id) = assignee_id.as_deref() {
            validate_required("assignee_id", agent_id)?;
            Self::ensure_not_self_assignment(run, agent_id)?;
            crate::ensure_execution_role_principal(
                &self.db,
                &root.project_id,
                crate::workflow::default_roles::CODER,
                agent_id,
            )
            .await?;
        }
        self.charge_lead_bound(
            root,
            &[
                (
                    db::budget::LEAD_SUBTASKS_KEY.to_owned(),
                    db::budget::LEAD_SUBTASKS_LIMIT,
                ),
                (
                    db::budget::lead_subtasks_run_key(&run.execution_id),
                    db::budget::LEAD_SUBTASKS_PER_RUN_LIMIT,
                ),
            ],
            key,
            "subtasks created by the lead",
        )
        .await?;
        // The charge restated the root's condition; read its version again.
        let root = TaskRepo::get_by_id(&*self.db, &root.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", root.id.clone()))?;
        let board_revision = TaskBoardRepo::board_revision(&*self.db, &root.project_id).await?;
        let result = self
            .execute_adaptive_task_command(self.lead_adaptive_command(
                &binding,
                &root,
                board_revision,
                AdaptiveTaskOperation::Split {
                    items: vec![AdaptiveTaskChild {
                        title,
                        description,
                        assignee_id,
                    }],
                },
                "The root's lead added a subtask",
                key,
            ))
            .await?;
        crate::wake_task_dispatch(&self.db, &root.id, "the lead added a subtask").await?;
        let created = result
            .tasks
            .first()
            .ok_or_else(|| ServiceError::invalid_operation("no subtask was created"))?;
        Ok(json!({
            "operation": "lead.subtask", "action": "create", "replayed": result.replayed,
            "task_id": created.id, "version": created.version, "status": created.status,
        }))
    }

    /// Command receipts are unique per Project, operation and key, so the
    /// lead's key is scoped to its root and identity.
    fn lead_command_key(binding: &LeadBinding, key: &str) -> String {
        format!("lead:{}:{}:{key}", binding.root.id, binding.run.identity_id)
    }

    fn lead_adaptive_command(
        &self,
        binding: &LeadBinding,
        root: &Task,
        board_revision: i64,
        operation: AdaptiveTaskOperation,
        rationale: &str,
        key: &str,
    ) -> AdaptiveTaskCommand {
        AdaptiveTaskCommand {
            project_id: root.project_id.clone(),
            source_task_id: root.id.clone(),
            expected_task_version: root.version,
            expected_board_revision: board_revision,
            operation,
            rationale: rationale.to_owned(),
            // The lead is the command's actor, as a lead: the receipt and
            // the event carry it, and the database repeats the lead check
            // under the writer transaction (`task_lead` principal).
            actor_type: db::inherited_subtask::TASK_LEAD_PRINCIPAL.to_owned(),
            actor_id: binding.run.identity_id.clone(),
            policy_result: "allowed".to_owned(),
            policy_revision: None,
            policy_digest: None,
            requested_permission: Some("task_read".to_owned()),
            idempotency_key: Self::lead_command_key(binding, key),
            correlation_id: binding.run.execution_id.clone(),
            causation_id: None,
            causation_depth: 0,
        }
    }

    /// Change the title or description of a subtask that never started.
    pub async fn lead_update_subtask(
        &self,
        run: &LeadRun,
        task_id: &str,
        version: i64,
        title: Option<String>,
        description: Option<String>,
        key: &str,
    ) -> Result<Value> {
        let (binding, target) = self.bind_lead(run, Some(task_id)).await?;
        let task = Self::lead_subtask_target(&binding, target)?;
        self.ensure_subtask_not_started(&task).await?;
        if title.is_none() && description.is_none() {
            return Err(ServiceError::invalid_operation(
                "update needs a title or a description",
            ));
        }
        Self::ensure_version(&task, version)?;
        let what = match (&title, &description) {
            (Some(_), Some(_)) => "title and description",
            (Some(_), None) => "title",
            _ => "description",
        };
        let updated = self
            .update_task(
                task.id.clone(),
                api_types::UpdateTaskRequest {
                    title,
                    description,
                    priority: None,
                    merge_config: None,
                    plan: None,
                    task_state_config: None,
                    review_requirement_ids: None,
                    parent_task_id: None,
                    version,
                },
            )
            .await?;
        self.lead_comment(
            &binding,
            &task.id,
            "decision",
            &format!("The lead changed this subtask's {what}."),
            key,
        )
        .await?;
        Ok(json!({
            "operation": "lead.subtask", "action": "update",
            "task_id": updated.id, "version": updated.version, "status": updated.status,
        }))
    }

    /// Cancel a subtask that never started. The lead is the action's actor.
    pub async fn lead_cancel_subtask(
        &self,
        run: &LeadRun,
        task_id: &str,
        version: i64,
        reason: String,
    ) -> Result<Value> {
        let (binding, target) = self.bind_lead(run, Some(task_id)).await?;
        let task = Self::lead_subtask_target(&binding, target)?;
        self.ensure_subtask_not_started(&task).await?;
        Self::ensure_version(&task, version)?;
        let result = self
            .perform_task_action_as(
                task.id.clone(),
                TaskAction::Cancel {
                    reason: Some(reason),
                },
                version,
                Actor::agent(&run.identity_id),
            )
            .await?;
        crate::wake_task_dispatch(&self.db, &binding.root.id, "the lead cancelled a subtask")
            .await?;
        Ok(json!({
            "operation": "lead.subtask", "action": "cancel",
            "task_id": result.task.id, "version": result.task.version, "status": result.task.status,
        }))
    }

    /// Reorder the subtasks that have not started. The sequence command owns
    /// the rule (the terminal prefix and the current subtask stay; only the
    /// untouched `todo` suffix moves) and requires exactly the root's
    /// subtasks, so a foreign id is refused there.
    pub async fn lead_reorder_subtasks(
        &self,
        run: &LeadRun,
        ordered_task_ids: Vec<String>,
        key: &str,
    ) -> Result<Value> {
        let (binding, _) = self.bind_lead(run, None).await?;
        for id in &ordered_task_ids {
            crate::task_hierarchy::ensure_in_lead_subtree(&self.db, &binding.root, id).await?;
        }
        let board_revision =
            TaskBoardRepo::board_revision(&*self.db, &binding.root.project_id).await?;
        let result = self
            .execute_adaptive_task_command(self.lead_adaptive_command(
                &binding,
                &binding.root,
                board_revision,
                AdaptiveTaskOperation::Sequence { ordered_task_ids },
                "The root's lead reordered the subtasks that have not started",
                key,
            ))
            .await?;
        Ok(json!({
            "operation": "lead.subtask", "action": "reorder", "replayed": result.replayed,
            "ordered_task_ids": result.tasks.iter().map(|task| task.id.clone()).collect::<Vec<_>>(),
        }))
    }

    /// Assign the worker of a subtask, or the root's default worker.
    pub async fn lead_assign_worker(
        &self,
        run: &LeadRun,
        task_id: Option<&str>,
        agent_id: &str,
    ) -> Result<Value> {
        let (binding, target) = self.bind_lead(run, task_id).await?;
        let task = target.unwrap_or_else(|| binding.root.clone());
        validate_required("agent_id", agent_id)?;
        Self::ensure_not_self_assignment(run, agent_id)?;
        let role = crate::workflow::default_roles::CODER;
        crate::ensure_execution_role_principal(&self.db, &task.project_id, role, agent_id).await?;
        let now = now_rfc3339();
        let assignment = self
            .reassign_role(
                CreateTaskRoleAssignment {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    role_name: role.to_owned(),
                    assignee_type: Some(AssigneeKind::Agent),
                    assignee_id: Some(agent_id.to_owned()),
                    created_at: now.clone(),
                    updated_at: now,
                },
                false,
                false,
            )
            .await?;
        Ok(json!({
            "operation": "lead.subtask", "action": "assign",
            "task_id": assignment.task_id, "role": assignment.role_name,
            "agent_id": assignment.assignee_id,
            "default_worker": task.id == binding.root.id,
        }))
    }

    /// Apply one recovery action the Task action system offers the lead on a
    /// parked subtask. Bounded per subtask on the root's ledger.
    pub async fn lead_recover_subtask(
        &self,
        run: &LeadRun,
        task_id: &str,
        action: TaskAction,
        version: i64,
        key: &str,
    ) -> Result<Value> {
        let (binding, target) = self.bind_lead(run, Some(task_id)).await?;
        let task = Self::lead_subtask_target(&binding, target)?;
        let actor = Actor::agent(&run.identity_id);
        let bound = db::budget::lead_recover_key(&task.id);
        // A retry of the same request after it was applied.
        if task.version != version
            && db::budget::receipt_recorded(self.db.pool(), &binding.root.id, &bound, key).await?
        {
            return Ok(json!({
                "operation": "lead.recover", "replayed": true,
                "task_id": task.id, "version": task.version, "status": task.status,
            }));
        }
        let offers = self.task_action_offers(&task.id, &actor).await?;
        Self::ensure_version(&task, version)?;
        let offered = offers.available_actions.iter().any(|offer| {
            offer.action.verb() == action.verb()
                && offer.authority.contains(&api_types::ActionAuthority::Lead)
                && operation_registry::task_proposals::action_parameters_allowed(
                    offer, &action, false,
                )
        });
        if !offered {
            let verbs = offers
                .available_actions
                .iter()
                .filter(|offer| offer.authority.contains(&api_types::ActionAuthority::Lead))
                .map(|offer| offer.action.verb())
                .collect::<Vec<_>>();
            return Err(refused(
                LeadRefusal::ActionNotOffered,
                format!(
                    "`{}` is not offered to the lead on subtask {} now; offered: [{}]. The lead recovers a subtask that is parked on its work; any other park is the owner's or the Project Agent's.",
                    action.verb(),
                    task.id,
                    verbs.join(", ")
                ),
            ));
        }
        self.charge_lead_bound(
            &binding.root,
            &[(bound, db::budget::LEAD_RECOVER_LIMIT)],
            key,
            &format!("recoveries of subtask {}", task.id),
        )
        .await?;
        let result = self
            .perform_task_action_as(task.id.clone(), action, version, actor)
            .await?;
        crate::wake_task_dispatch(&self.db, &binding.root.id, "the lead recovered a subtask")
            .await?;
        Ok(json!({
            "operation": "lead.recover", "replayed": false,
            "task_id": result.task.id, "version": result.task.version, "status": result.task.status,
        }))
    }

    /// Leave a note the subtask's next implementation run reads first.
    pub async fn lead_handoff(
        &self,
        run: &LeadRun,
        task_id: &str,
        note: &str,
        key: &str,
    ) -> Result<Value> {
        let (binding, target) = self.bind_lead(run, Some(task_id)).await?;
        let task = Self::lead_subtask_target(&binding, target)?;
        let note = note.trim();
        if note.is_empty() || note.chars().count() > operation_registry::task_lead::MAX_NOTE_CHARS {
            return Err(ServiceError::invalid_operation(format!(
                "a hand-off note is 1 to {} characters",
                operation_registry::task_lead::MAX_NOTE_CHARS
            )));
        }
        let (existing, replay): (i64, i64) = sqlx::query_as(
            "SELECT COUNT(*), COALESCE(SUM(idempotency_key = ?), 0) FROM task_comment WHERE task_id=? AND worklog_kind=?",
        )
        .bind(format!("lead:{}:{key}", db::COMMENT_KIND_HANDOFF))
        .bind(&task.id)
        .bind(db::COMMENT_KIND_HANDOFF)
        .fetch_one(self.db.pool())
        .await
        .map_err(db::DbError::from)?;
        if replay == 0 && existing >= MAX_HANDOFF_NOTES_PER_SUBTASK {
            return Err(refused(
                LeadRefusal::LimitReached,
                format!(
                    "subtask {} already has {MAX_HANDOFF_NOTES_PER_SUBTASK} hand-off notes; its next run reads the newest {HANDOFF_NOTES_IN_PROMPT}",
                    task.id
                ),
            ));
        }
        let comment = self
            .lead_comment(&binding, &task.id, db::COMMENT_KIND_HANDOFF, note, key)
            .await?;
        Ok(json!({
            "operation": "lead.handoff", "task_id": task.id, "comment_id": comment.id,
            "execution_id": comment.execution_id, "role": comment.role,
        }))
    }

    /// Ask the owner one question. The question is a `request_input` comment
    /// on the root, the owner gets one Attention item for it, and the root
    /// waits in its planning state (`human_decision { lead_input }`): the
    /// planning gate does not accept this run, and the lead is dispatched
    /// again only when the answer cleared the wait. The comment, the item and
    /// the wait are ONE transaction: a question is never shown without the
    /// root waiting for its answer (the run would be accepted and the answer
    /// lost), and the root never waits on a question nobody is shown.
    pub async fn lead_request_input(
        &self,
        run: &LeadRun,
        question: &str,
        key: &str,
    ) -> Result<Value> {
        let (binding, _) = self.bind_lead(run, None).await?;
        let question = question.trim();
        if question.is_empty()
            || question.chars().count() > operation_registry::task_lead::MAX_NOTE_CHARS
        {
            return Err(ServiceError::invalid_operation(format!(
                "a question is 1 to {} characters",
                operation_registry::task_lead::MAX_NOTE_CHARS
            )));
        }
        let root = &binding.root;
        let now = now_rfc3339();
        let mut tx = db::begin_immediate(self.db.pool()).await?;
        let comment = self
            .db
            .create_task_comment_in_tx(
                &mut tx,
                &CreateTaskComment {
                    id: new_uuid_v4(),
                    task_id: root.id.clone(),
                    author_type: CommentAuthorType::Agent,
                    author_id: Some(run.identity_id.clone()),
                    author_name: "lead".to_owned(),
                    content: question.to_owned(),
                    execution_id: Some(run.execution_id.clone()),
                    role: Some(binding.role.clone()),
                    worklog_kind: Some(db::COMMENT_KIND_REQUEST_INPUT.to_owned()),
                    idempotency_key: Some(format!(
                        "lead:{}:{key}",
                        db::COMMENT_KIND_REQUEST_INPUT
                    )),
                    created_at: now.clone(),
                    updated_at: now.clone(),
                },
            )
            .await?;
        if comment.content != question {
            return Err(db::DbError::IdempotencyConflict.into());
        }
        let dedupe = lead_input_dedupe_key(&root.id, &comment.id);
        let asked: Option<String> =
            sqlx::query_scalar("SELECT status FROM attention_projection WHERE dedupe_key=?")
                .bind(&dedupe)
                .fetch_optional(&mut *tx)
                .await
                .map_err(db::DbError::from)?;
        let waiting = json!({
            "operation": "lead.request_input", "task_id": root.id, "comment_id": comment.id,
            "waiting": true,
            "next": "End this run now. The root waits for the owner's answer and the lead runs again with it.",
        });
        if asked.is_some() {
            // A retry of a question that was asked (and maybe answered
            // since): nothing is written twice and no wait is opened again.
            return Ok(waiting);
        }
        if !self
            .db
            .open_lead_input_wait_in_tx(&mut tx, &root.id, &comment.id, &run.execution_id, &now)
            .await?
        {
            // Dropping the transaction takes the comment back.
            return Err(refused(
                LeadRefusal::InputPending,
                "this root already waits for a person; end the run, it starts again with the answer",
            ));
        }
        let event = db::DomainEventRepo::append_event_in_tx(
            &*self.db,
            &mut tx,
            &db::CreateDomainEvent {
                id: new_uuid_v4(),
                event_type: "task.lead_input_requested".to_owned(),
                entity_type: "task".to_owned(),
                entity_id: root.id.clone(),
                actor_type: "agent".to_owned(),
                actor_id: Some(run.identity_id.clone()),
                scope_type: "project".to_owned(),
                scope_id: root.project_id.clone(),
                correlation_id: run.execution_id.clone(),
                causation_id: None,
                causation_depth: 0,
                dedupe_key: Some(dedupe.clone()),
                payload_json: json!({"task_id": root.id, "comment_id": comment.id}).to_string(),
                created_at: now.clone(),
            },
        )
        .await?;
        self.db
            .insert_attention_in_tx(
                &mut tx,
                db::CreateAttentionProjection {
                    id: new_uuid_v4(),
                    attention_type: "human_input_required".to_owned(),
                    scope_type: "project".to_owned(),
                    scope_id: root.project_id.clone(),
                    identity_id: Some(run.identity_id.clone()),
                    source_event_id: event.id,
                    priority: 95,
                    status: "open".to_owned(),
                    summary: clip(question.lines().next().unwrap_or(question), 160).0,
                    details_json: json!({
                        "task_id": root.id,
                        "comment_id": comment.id,
                        "question": question,
                        "execution_id": run.execution_id,
                    })
                    .to_string(),
                    dedupe_key: dedupe,
                    occurred_at: now.clone(),
                    updated_at: now,
                    acknowledged_at: None,
                    snoozed_until: None,
                    resolved_at: None,
                    updated_by_user_id: None,
                    recommended_action: crate::attention_service::ANSWER_LEAD_INPUT.to_owned(),
                    source_sequence: Some(event.sequence),
                },
            )
            .await?;
        tx.commit().await.map_err(db::DbError::from)?;
        self.publish(ForgeEvent {
            event_type: "comment.created".to_owned(),
            entity_id: comment.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::CommentCreated {
                task_id: root.id.clone(),
                comment_id: comment.id.clone(),
                author_type: "agent".to_owned(),
                author_name: comment.author_name.clone(),
            },
        });
        Ok(waiting)
    }
}
