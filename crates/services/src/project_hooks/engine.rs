use api_types::{ProjectHookAction, ProjectHookRule};
use chrono::{DateTime, Utc};
use db::{
    new_uuid_v4, now_rfc3339, CommentAuthorType, CreateNotification, CreateProjectHookRun,
    CreateTaskComment, Notification, Project, ProjectHookRun, ProjectHookRunRepo,
    ProjectHookRunStatus, Task, TaskComment, TaskRepo, UpdateProjectHookRun,
};
use events::{event_timestamp, EventContext, ForgeEvent, PROJECT_HOOK_RUN_CHANGED_EVENT};
use sqlx::{Sqlite, Transaction};

use crate::{
    project_hooks::{
        actions::{
            dispatch_agent::{self, DispatchPreparation},
            task_type_to_string, ActionContext, ActionOutcome,
        },
        triggers::TriggerMatch,
        ProjectHookService,
    },
    task_service::PreparedProjectHookTask,
    worker_runtime::{consumer_error_kind, WorkerErrorKind},
    Result,
};

pub struct ProjectHookEngine<'a> {
    service: &'a ProjectHookService,
}

pub struct PreparedHook {
    project: Project,
    rule: ProjectHookRule,
    input: CreateProjectHookRun,
    action: PreparedAction,
}
enum PreparedAction {
    Notify(CreateNotification),
    Comment(CreateTaskComment),
    CreateTask(Box<PreparedProjectHookTask>),
    External {
        task: Box<PreparedProjectHookTask>,
        agent_id: String,
        prompt: String,
    },
    Skipped(String),
    Failed(String),
}
pub struct CommittedHook {
    run: ProjectHookRun,
    notification: Option<Notification>,
    comment: Option<TaskComment>,
    task: Option<Task>,
    external: bool,
}

impl<'a> ProjectHookEngine<'a> {
    pub fn new(service: &'a ProjectHookService) -> Self {
        Self { service }
    }

    pub async fn run(
        &self,
        project: &Project,
        rule: ProjectHookRule,
        trigger_match: TriggerMatch,
    ) -> Result<()> {
        let prepared = self.prepare(project, rule, trigger_match).await?;
        let mut tx = db::begin_immediate(self.service.db.pool()).await?;
        let committed = self.commit(&mut tx, &prepared).await?;
        tx.commit().await?;
        if let Some(committed) = committed {
            self.after_commit(&prepared, &committed).await?;
        }
        Ok(())
    }

    pub async fn prepare(
        &self,
        project: &Project,
        rule: ProjectHookRule,
        trigger_match: TriggerMatch,
    ) -> Result<PreparedHook> {
        let now = now_rfc3339();
        let input = CreateProjectHookRun {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            rule_id: rule.id.clone(),
            trigger_type: trigger_match.trigger_type.clone(),
            dedupe_key: trigger_match.dedupe_key.clone(),
            // Running is the durable started marker, committed BEFORE external dispatch.
            status: ProjectHookRunStatus::Running,
            source_task_id: trigger_match.source_task_id.clone(),
            source_execution_id: trigger_match.source_execution_id.clone(),
            automation_task_id: None,
            execution_id: None,
            agent_id: None,
            reason: trigger_match.reason.clone(),
            created_at: now.clone(),
            updated_at: now,
            completed_at: None,
        };
        let action = match self
            .prepare_action(project, &rule, &trigger_match, &input)
            .await
        {
            Ok(action) => action,
            Err(error) if consumer_error_kind(&error) == WorkerErrorKind::Terminal => {
                PreparedAction::Failed(error.to_string())
            }
            Err(error) => return Err(error),
        };
        Ok(PreparedHook {
            project: project.clone(),
            rule,
            input,
            action,
        })
    }

    async fn prepare_action(
        &self,
        project: &Project,
        rule: &ProjectHookRule,
        trigger_match: &TriggerMatch,
        run: &CreateProjectHookRun,
    ) -> Result<PreparedAction> {
        match &rule.action {
            ProjectHookAction::Notify {
                title,
                message,
                severity,
            } => {
                let body = format!(
                    "{}\n\nTrigger: {}\nDedupe key: {}\nHook run: {}\nRule: {}{}",
                    message,
                    trigger_match.trigger_type,
                    trigger_match.dedupe_key,
                    run.id,
                    rule.id,
                    severity
                        .as_ref()
                        .map(|s| format!("\nSeverity: {s}"))
                        .unwrap_or_default()
                );
                Ok(PreparedAction::Notify(CreateNotification {
                    id: new_uuid_v4(),
                    project_id: project.id.clone(),
                    task_id: trigger_match.source_task_id.clone(),
                    event_type: "project_hook.notify".to_owned(),
                    title: title.clone(),
                    body: Some(body),
                    read: false,
                    created_at: now_rfc3339(),
                }))
            }
            ProjectHookAction::AddComment {
                target_task_id,
                content,
            } => {
                let task_id = target_task_id
                    .as_ref()
                    .or(trigger_match.source_task_id.as_ref())
                    .ok_or_else(|| {
                        crate::ServiceError::invalid_operation(
                            "add_comment requires target_task_id or trigger source task",
                        )
                    })?;
                let now = now_rfc3339();
                Ok(PreparedAction::Comment(CreateTaskComment {
                    id: new_uuid_v4(),
                    task_id: task_id.clone(),
                    author_type: CommentAuthorType::System,
                    author_id: None,
                    author_name: "Forge".to_owned(),
                    content: format!(
                        "{}\n\nProject hook run: {}\nRule: {}",
                        content, run.id, rule.id
                    ),
                    execution_id: None,
                    role: None,
                    worklog_kind: None,
                    idempotency_key: None,
                    created_at: now.clone(),
                    updated_at: now,
                }))
            }
            ProjectHookAction::CreateTask {
                title,
                description,
                task_type,
                priority,
            } => {
                let mut parts = description
                    .as_deref()
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .map(|text| vec![text.to_owned()])
                    .unwrap_or_default();
                parts.push(format!("Project hook run: {}", run.id));
                parts.push(format!("Rule: {}", rule.id));
                Ok(PreparedAction::CreateTask(Box::new(
                    self.service
                        .task_service
                        .prepare_project_hook_task(
                            project,
                            title.clone(),
                            parts.join("\n\n"),
                            task_type
                                .map(task_type_to_string)
                                .unwrap_or_else(|| "task".to_owned()),
                            priority.unwrap_or(0),
                            false,
                        )
                        .await?,
                )))
            }
            ProjectHookAction::DispatchAgent {
                agent_id,
                prompt,
                follow_up,
            } => {
                let context = ActionContext {
                    service: self.service,
                    project,
                    rule_id: &rule.id,
                    run,
                    trigger_match,
                };
                Ok(
                    match dispatch_agent::prepare(
                        &context,
                        agent_id,
                        prompt.as_deref(),
                        follow_up.as_ref(),
                    )
                    .await?
                    {
                        DispatchPreparation::Ready {
                            task,
                            agent_id,
                            prompt,
                        } => PreparedAction::External {
                            task,
                            agent_id,
                            prompt,
                        },
                        DispatchPreparation::Skipped(reason) => PreparedAction::Skipped(reason),
                    },
                )
            }
        }
    }

    pub async fn still_matches(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        prepared: &PreparedHook,
    ) -> Result<bool> {
        let version: Option<i64> = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
            .bind(&prepared.project.id)
            .fetch_optional(&mut **tx)
            .await?;
        if version != Some(prepared.project.version) {
            return Err(db::DbError::VersionConflict.into());
        }
        super::triggers::all_work_completed::all_work_completed_in_tx(tx, &prepared.project).await
    }

    pub async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        prepared: &PreparedHook,
    ) -> Result<Option<CommittedHook>> {
        let Some(run) = self
            .service
            .db
            .claim_project_hook_run_in_tx(
                tx,
                prepared.input.clone(),
                i64::from(prepared.rule.max_concurrent_runs),
                &format!("max_concurrent_runs reached for rule {}", prepared.rule.id),
            )
            .await?
        else {
            return Ok(None);
        };
        let mut committed = CommittedHook {
            run,
            notification: None,
            comment: None,
            task: None,
            external: false,
        };
        if committed.run.status == ProjectHookRunStatus::Skipped {
            return Ok(Some(committed));
        }
        let cooldown = self
            .cooldown_skip_reason(tx, &prepared.project.id, &prepared.rule)
            .await?;
        let outcome = if let Some(reason) = cooldown {
            Some(ActionOutcome::skipped(reason))
        } else {
            match &prepared.action {
                PreparedAction::Notify(input) => {
                    let notification = self.service.db.create_notification_in_tx(tx, input).await?;
                    let outcome = ActionOutcome::completed(format!(
                        "notification {} created",
                        notification.id
                    ));
                    committed.notification = Some(notification);
                    Some(outcome)
                }
                PreparedAction::Comment(input) => {
                    let task =
                        TaskRepo::get_by_id_in_tx(&*self.service.db, tx, &input.task_id, false)
                            .await?;
                    match task {
                        Some(task) if task.project_id == prepared.project.id => {
                            committed.comment =
                                Some(self.service.db.create_task_comment_in_tx(tx, input).await?);
                            Some(ActionOutcome::completed(format!(
                                "comment added to task {}",
                                input.task_id
                            )))
                        }
                        Some(_) => Some(failed_outcome(
                            crate::ServiceError::invalid_operation(format!(
                                "task {} does not belong to project {}",
                                input.task_id, prepared.project.id
                            ))
                            .to_string(),
                        )),
                        None => Some(failed_outcome(
                            crate::ServiceError::not_found("task", input.task_id.clone())
                                .to_string(),
                        )),
                    }
                }
                PreparedAction::CreateTask(input) => {
                    let task = self
                        .service
                        .task_service
                        .commit_project_hook_task(tx, input)
                        .await?;
                    let outcome = ActionOutcome::completed(format!("created task {}", task.id));
                    committed.task = Some(task);
                    Some(outcome)
                }
                PreparedAction::External { task, agent_id, .. } => {
                    let task = self
                        .service
                        .task_service
                        .commit_project_hook_task(tx, task)
                        .await?;
                    let now = now_rfc3339();
                    committed.run = self
                        .service
                        .db
                        .update_project_hook_run_in_tx(
                            tx,
                            UpdateProjectHookRun {
                                id: committed.run.id.clone(),
                                status: ProjectHookRunStatus::Running,
                                automation_task_id: Some(Some(task.id.clone())),
                                execution_id: None,
                                agent_id: Some(Some(agent_id.clone())),
                                reason: None,
                                updated_at: now,
                                completed_at: None,
                            },
                        )
                        .await?;
                    committed.task = Some(task);
                    committed.external = true;
                    None
                }
                PreparedAction::Skipped(reason) => Some(ActionOutcome::skipped(reason.clone())),
                PreparedAction::Failed(reason) => Some(failed_outcome(reason.clone())),
            }
        };
        if let Some(outcome) = outcome {
            committed.run = self
                .service
                .db
                .update_project_hook_run_in_tx(tx, status_update(&committed.run.id, outcome))
                .await?;
        }
        Ok(Some(committed))
    }

    pub async fn after_commit(
        &self,
        prepared: &PreparedHook,
        committed: &CommittedHook,
    ) -> Result<()> {
        self.publish_run_changed(&committed.run);
        if let Some(notification) = &committed.notification {
            self.service
                .notification_service
                .publish_created(notification);
        }
        if let Some(comment) = &committed.comment {
            self.service
                .task_service
                .after_project_hook_comment(comment)
                .await;
        }
        if let Some(task) = &committed.task {
            self.service.event_bus.publish(ForgeEvent {
                event_type: "task.created".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::TaskCreated {
                    project_id: task.project_id.clone(),
                    title: task.title.clone(),
                },
            });
        }
        if committed.external {
            let PreparedAction::External {
                agent_id, prompt, ..
            } = &prepared.action
            else {
                unreachable!("external commit has dispatch preparation");
            };
            let task_id = committed
                .run
                .automation_task_id
                .clone()
                .expect("dispatch task committed");
            let launch = self
                .service
                .task_service
                .launch_execution(
                    task_id.clone(),
                    agent_id.clone(),
                    Some(prompt.clone()),
                    None,
                )
                .await;
            let outcome = match launch {
                Ok(result) => ActionOutcome {
                    status: ProjectHookRunStatus::Dispatched,
                    automation_task_id: Some(task_id),
                    execution_id: Some(result.execution.id),
                    agent_id: Some(agent_id.clone()),
                    reason: Some("agent dispatched".to_owned()),
                },
                Err(error) => ActionOutcome {
                    status: ProjectHookRunStatus::Failed,
                    automation_task_id: Some(task_id),
                    execution_id: None,
                    agent_id: Some(agent_id.clone()),
                    reason: Some(format!(
                        "automation task created but execution launch failed: {error}"
                    )),
                },
            };
            let run = ProjectHookRunRepo::update_status(
                &*self.service.db,
                status_update(&committed.run.id, outcome),
            )
            .await?;
            self.publish_run_changed(&run);
        }
        Ok(())
    }

    async fn cooldown_skip_reason(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        project_id: &str,
        rule: &ProjectHookRule,
    ) -> Result<Option<String>> {
        let Some(seconds) = rule.cooldown_seconds else {
            return Ok(None);
        };
        // Preserve the old 100-recent-runs bound and completed/dispatched/skipped policy.
        let runs: Vec<(String, String, Option<String>, String)> = sqlx::query_as("SELECT id, rule_id, COALESCE(completed_at, updated_at), status FROM project_hook_run WHERE project_id = ? ORDER BY created_at DESC, id DESC LIMIT 100").bind(project_id).fetch_all(&mut **tx).await?;
        for (id, rule_id, timestamp, status) in runs {
            if rule_id != rule.id
                || !matches!(status.as_str(), "completed" | "dispatched" | "skipped")
            {
                continue;
            }
            if timestamp
                .and_then(|text| DateTime::parse_from_rfc3339(&text).ok())
                .is_some_and(|timestamp| {
                    Utc::now()
                        .signed_duration_since(timestamp.with_timezone(&Utc))
                        .num_seconds()
                        < i64::try_from(seconds).unwrap_or(i64::MAX)
                })
            {
                return Ok(Some(format!(
                    "rule {} is inside cooldown after run {}",
                    rule.id, id
                )));
            }
        }
        Ok(None)
    }

    fn publish_run_changed(&self, run: &ProjectHookRun) {
        self.service.event_bus.publish(ForgeEvent {
            event_type: PROJECT_HOOK_RUN_CHANGED_EVENT.to_owned(),
            entity_id: run.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ProjectHookRunChanged {
                project_id: run.project_id.clone(),
                run_id: run.id.clone(),
                rule_id: run.rule_id.clone(),
                trigger_type: run.trigger_type.clone(),
                dedupe_key: run.dedupe_key.clone(),
                status: run.status.to_string(),
                source_task_id: run.source_task_id.clone(),
                automation_task_id: run.automation_task_id.clone(),
                execution_id: run.execution_id.clone(),
                agent_id: run.agent_id.clone(),
                reason: run.reason.clone(),
            },
        });
    }
}

fn failed_outcome(reason: String) -> ActionOutcome {
    ActionOutcome {
        status: ProjectHookRunStatus::Failed,
        automation_task_id: None,
        execution_id: None,
        agent_id: None,
        reason: Some(reason),
    }
}
fn status_update(id: &str, outcome: ActionOutcome) -> UpdateProjectHookRun {
    let now = now_rfc3339();
    UpdateProjectHookRun {
        id: id.to_owned(),
        status: outcome.status,
        automation_task_id: Some(outcome.automation_task_id),
        execution_id: Some(outcome.execution_id),
        agent_id: Some(outcome.agent_id),
        reason: Some(outcome.reason),
        updated_at: now.clone(),
        completed_at: Some(Some(now)),
    }
}
