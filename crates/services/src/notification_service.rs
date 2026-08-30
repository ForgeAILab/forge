use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, CreateNotification, Notification, NotificationRepo, ReviewRepo,
    SqliteDb, TaskRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};

pub struct NotificationService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

impl NotificationService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    pub fn start(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut rx = self.event_bus.subscribe();
            loop {
                let Ok(event) = rx.recv().await else {
                    break;
                };
                if let Err(error) = self.handle_event(event).await {
                    tracing::warn!(%error, "notification service failed to handle event");
                }
            }
        })
    }

    pub async fn create_project_hook_notification(
        &self,
        project_id: String,
        task_id: Option<String>,
        title: String,
        body: Option<String>,
    ) -> crate::Result<Notification> {
        self.create_and_publish(
            project_id,
            task_id,
            "project_hook.notify".to_owned(),
            title,
            body,
        )
        .await
    }

    async fn handle_event(&self, event: ForgeEvent) -> crate::Result<()> {
        match event.context {
            EventContext::TaskStatusChanged {
                project_id,
                new_status,
                ..
            } if new_status == crate::workflow::default_states::DONE => {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, true).await?
                else {
                    return Ok(());
                };
                self.create_and_publish(
                    project_id,
                    Some(task.id),
                    "task.done".to_owned(),
                    task.title,
                    None,
                )
                .await?;
            }
            EventContext::TaskMoved(payload)
                if payload.old_status != payload.new_status
                    && payload.new_status == crate::workflow::default_states::DONE =>
            {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, true).await?
                else {
                    return Ok(());
                };
                self.create_and_publish(
                    payload.project_id,
                    Some(task.id),
                    "task.done".to_owned(),
                    task.title,
                    None,
                )
                .await?;
            }
            EventContext::TaskBlocked {
                project_id, reason, ..
            } => {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, true).await?
                else {
                    return Ok(());
                };
                self.create_and_publish(
                    project_id,
                    Some(task.id),
                    "task.blocked".to_owned(),
                    task.title,
                    Some(reason),
                )
                .await?;
            }
            EventContext::ProjectAutonomyStalled {
                project_id,
                open_incidents,
                reason,
            } => {
                // No task id: the stall is the Project's, not any one Task's.
                self.create_and_publish(
                    project_id,
                    None,
                    "project.autonomy_stalled".to_owned(),
                    format!("Project Agent stopped: {open_incidents} open incident(s) unanswered"),
                    Some(reason),
                )
                .await?;
            }
            EventContext::TaskFailed {
                project_id, reason, ..
            } => {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, true).await?
                else {
                    return Ok(());
                };
                self.create_and_publish(
                    project_id,
                    Some(task.id),
                    "task.failed".to_owned(),
                    task.title,
                    Some(reason),
                )
                .await?;
            }
            // TaskRecovered context is shared by task.recovered (manual recovery
            // required), task.execution_resumed, and task.recovery_action; only
            // the first needs the user's attention. Shutdown recoveries are
            // auto-resumed at the next startup, so they are not notified either.
            EventContext::TaskRecovered { project_id, reason }
                if event.event_type == "task.recovered" && reason != "shutdown" =>
            {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &event.entity_id, true).await?
                else {
                    return Ok(());
                };
                self.create_and_publish(
                    project_id,
                    Some(task.id),
                    "task.recovery_required".to_owned(),
                    task.title,
                    Some(recovery_reason_message(&reason)),
                )
                .await?;
            }
            EventContext::ReviewPassed { task_id, .. } => {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &task_id, true).await? else {
                    return Ok(());
                };
                self.create_and_publish(
                    task.project_id,
                    Some(task.id),
                    "review.passed".to_owned(),
                    format!("Review passed: {}", task.title),
                    None,
                )
                .await?;
            }
            EventContext::ReviewFailed {
                task_id, review_id, ..
            } => {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &task_id, true).await? else {
                    return Ok(());
                };
                let reason = ReviewRepo::get_by_id(&*self.db, &review_id)
                    .await?
                    .and_then(|review| extract_review_failure_reason(&review.step_results_json));
                self.create_and_publish(
                    task.project_id,
                    Some(task.id),
                    "review.failed".to_owned(),
                    format!("Review failed: {}", task.title),
                    reason,
                )
                .await?;
            }
            EventContext::MergeFailed { task_id, reason } => {
                let Some(task) = TaskRepo::get_by_id(&*self.db, &task_id, true).await? else {
                    return Ok(());
                };
                self.create_and_publish(
                    task.project_id,
                    Some(task.id),
                    "merge.failed".to_owned(),
                    task.title,
                    Some(reason),
                )
                .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn create_and_publish(
        &self,
        project_id: String,
        task_id: Option<String>,
        event_type: String,
        title: String,
        body: Option<String>,
    ) -> crate::Result<Notification> {
        let notification = NotificationRepo::create(
            &*self.db,
            CreateNotification {
                id: new_uuid_v4(),
                project_id,
                task_id,
                event_type: event_type.clone(),
                title: title.clone(),
                body,
                read: false,
                created_at: now_rfc3339(),
            },
        )
        .await?;

        self.event_bus.publish(ForgeEvent {
            event_type: "notification.created".to_owned(),
            entity_id: notification.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::NotificationCreated {
                notification_id: notification.id.clone(),
                project_id: notification.project_id.clone(),
                task_id: notification.task_id.clone(),
                event_type,
                title,
            },
        });
        Ok(notification)
    }
}

fn recovery_reason_message(reason: &str) -> String {
    match reason {
        "crash_recovery" => "Needs manual recovery after a server restart".to_owned(),
        "agent_timeout" => "Needs manual recovery after an agent heartbeat timeout".to_owned(),
        other => format!("Needs manual recovery: {other}"),
    }
}

fn extract_review_failure_reason(step_results_json: &str) -> Option<String> {
    let details = serde_json::from_str::<serde_json::Value>(step_results_json).ok()?;
    details
        .get("auditor")
        .and_then(|auditor| auditor.get("reason"))
        .and_then(|reason| reason.as_str())
        .map(|reason| reason.to_owned())
}
