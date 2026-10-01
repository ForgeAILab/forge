use std::sync::Arc;

use db::{
    new_uuid_v4, now_rfc3339, CreateNotification, Notification, NotificationRepo, ReviewRepo,
    SqliteDb, TaskRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use tokio::sync::{broadcast, watch};

pub struct NotificationService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

impl NotificationService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    pub fn start_with_shutdown(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        let receiver = self.event_bus.subscribe();
        tokio::spawn(self.run(receiver, shutdown))
    }

    async fn run(
        self: Arc<Self>,
        mut receiver: broadcast::Receiver<ForgeEvent>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        if *shutdown.borrow_and_update() {
            return;
        }

        loop {
            tokio::select! {
                result = receiver.recv() => {
                    let event = match result {
                        Ok(event) => event,
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            tracing::warn!(skipped_events = skipped, "notification service event receiver lagged");
                            continue;
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                    };
                    if let Err(error) = self.handle_event(event).await {
                        tracing::warn!(%error, "notification service failed to handle event");
                    }
                }
                result = shutdown.changed() => {
                    if result.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use db::{create_sqlite_pool, run_migrations, CreateProject, ProjectRepo};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn worker_robustness_notification_receiver_continues_after_lag() {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
        let event_bus = Arc::new(EventBus::new(1));
        let service = Arc::new(NotificationService::new(
            Arc::clone(&db),
            Arc::clone(&event_bus),
        ));
        let project = ProjectRepo::create(
            &*db,
            CreateProject {
                id: new_uuid_v4(),
                name: "Notifications".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let receiver = event_bus.subscribe();
        for entity_id in ["first", "second"] {
            event_bus.publish(ForgeEvent {
                event_type: "test.event".to_owned(),
                entity_id: entity_id.to_owned(),
                timestamp: event_timestamp(),
                context: EventContext::Empty {},
            });
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(Arc::clone(&service).run(receiver, shutdown_rx));

        event_bus.publish(ForgeEvent {
            event_type: "project.autonomy_stalled".to_owned(),
            entity_id: project.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::ProjectAutonomyStalled {
                project_id: project.id.clone(),
                open_incidents: 1,
                reason: "needs attention".to_owned(),
            },
        });

        timeout(Duration::from_secs(10), async {
            loop {
                let count = sqlx::query_scalar::<_, i64>(
                    "SELECT COUNT(*) FROM notification
                     WHERE project_id = ? AND event_type = 'project.autonomy_stalled'",
                )
                .bind(&project.id)
                .fetch_one(db.pool())
                .await
                .unwrap();
                if count == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("post-lag event is delivered");
        shutdown_tx.send(true).unwrap();
        handle.await.unwrap();
    }
}
