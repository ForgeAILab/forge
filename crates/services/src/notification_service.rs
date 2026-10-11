use std::sync::Arc;

use async_trait::async_trait;
use db::{
    new_uuid_v4, now_rfc3339, CreateNotification, DomainEvent, Notification, ReviewRepo, SqliteDb,
    TaskRepo,
};
use events::{event_timestamp, EventBus, EventContext, ForgeEvent};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{Sqlite, Transaction};
use tokio::sync::watch;

use crate::worker_runtime::{
    consumer_error, Outcome, Subscription, Worker, WorkerError, WorkerRuntime,
};

pub(crate) const CONSUMER_NAME: &str = "notifications";

#[derive(Clone)]
pub struct NotificationService {
    db: Arc<SqliteDb>,
    event_bus: Arc<EventBus>,
}

/// Immutable notification content; only the committed row is published.
#[derive(Deserialize)]
pub struct NotificationRequest {
    project_id: String,
    task_id: Option<String>,
    event_type: String,
    title: String,
    body: Option<String>,
}

impl NotificationRequest {
    fn input(&self) -> CreateNotification {
        CreateNotification {
            id: new_uuid_v4(),
            project_id: self.project_id.clone(),
            task_id: self.task_id.clone(),
            event_type: self.event_type.clone(),
            title: self.title.clone(),
            body: self.body.clone(),
            read: false,
            created_at: now_rfc3339(),
        }
    }
}

impl NotificationService {
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        Self { db, event_bus }
    }

    pub fn start_with_shutdown(
        self: Arc<Self>,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        Arc::new(WorkerRuntime::new(Arc::clone(&self.db), self)).start(shutdown)
    }

    async fn prepare(&self, event: &DomainEvent) -> crate::Result<Option<NotificationRequest>> {
        let payload: Value = serde_json::from_str(&event.payload_json).map_err(|_| {
            crate::ServiceError::invalid_operation("invalid notification event payload")
        })?;
        if event.event_type == "notification.requested" {
            let request: NotificationRequest = serde_json::from_value(payload).map_err(|_| {
                crate::ServiceError::invalid_operation("invalid notification request")
            })?;
            // Preserve the old missing-Task skip after a source is removed;
            // a deleted Project likewise has no inbox to receive this request.
            let project_exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM project WHERE id = ?)")
                    .bind(&request.project_id)
                    .fetch_one(self.db.pool())
                    .await?;
            if !project_exists {
                return Ok(None);
            }
            if let Some(task_id) = request.task_id.as_deref() {
                if TaskRepo::get_by_id(&*self.db, task_id, true)
                    .await?
                    .is_none()
                {
                    return Ok(None);
                }
            }
            return Ok(Some(request));
        }
        let (kind, task_id, body) = match event.event_type.as_str() {
            "task.transitioned"
                if payload["from_state"] != payload["to_state"]
                    && payload["to_state"] == crate::workflow::default_states::DONE =>
            {
                ("task.done", event.entity_id.as_str(), None)
            }
            "task.status_changed"
                if payload["from_status"] != payload["to_status"]
                    && payload["to_status"] == crate::workflow::default_states::DONE =>
            {
                ("task.done", event.entity_id.as_str(), None)
            }
            "review.status_changed"
                if event.actor_type == "review_runner"
                    && payload["manual_override"] != true
                    && payload["finished"] == true
                    && (payload["status"] == "passed" || payload["status"] == "failed") =>
            {
                let Some(task_id) = payload["task_id"].as_str() else {
                    return Ok(None);
                };
                let passed = payload["status"] == "passed";
                let reason = if passed {
                    None
                } else {
                    ReviewRepo::get_by_id(&*self.db, &event.entity_id)
                        .await?
                        .and_then(|review| extract_review_failure_reason(&review.step_results_json))
                };
                (
                    if passed {
                        "review.passed"
                    } else {
                        "review.failed"
                    },
                    task_id,
                    reason,
                )
            }
            _ => return Ok(None),
        };
        let Some(task) = TaskRepo::get_by_id(&*self.db, task_id, true).await? else {
            return Ok(None);
        };
        let title = match kind {
            "review.passed" => format!("Review passed: {}", task.title),
            "review.failed" => format!("Review failed: {}", task.title),
            _ => task.title,
        };
        Ok(Some(NotificationRequest {
            project_id: task.project_id,
            task_id: Some(task.id),
            event_type: kind.to_owned(),
            title,
            body,
        }))
    }

    pub(crate) fn publish_created(&self, notification: &Notification) {
        self.event_bus.publish(ForgeEvent {
            event_type: "notification.created".to_owned(),
            entity_id: notification.id.clone(),
            timestamp: event_timestamp(),
            context: EventContext::NotificationCreated {
                notification_id: notification.id.clone(),
                project_id: notification.project_id.clone(),
                task_id: notification.task_id.clone(),
                event_type: notification.event_type.clone(),
                title: notification.title.clone(),
            },
        });
    }
}

#[async_trait]
impl Worker<Notification> for NotificationService {
    type Prepared = NotificationRequest;
    fn name(&self) -> &str {
        CONSUMER_NAME
    }
    fn subscription(&self) -> Subscription {
        Subscription::Exact(vec![
            "task.transitioned".into(),
            "task.status_changed".into(),
            "review.status_changed".into(),
            "notification.requested".into(),
        ])
    }
    async fn handle(
        &self,
        event: &DomainEvent,
    ) -> std::result::Result<Outcome<Self::Prepared>, WorkerError> {
        self.prepare(event)
            .await
            .map(|request| request.map_or(Outcome::Skip, Outcome::Done))
            .map_err(consumer_error)
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        _: &DomainEvent,
        prepared: &Self::Prepared,
    ) -> std::result::Result<Notification, WorkerError> {
        self.db
            .create_notification_in_tx(tx, &prepared.input())
            .await
            .map_err(|error| WorkerError::database("notification insert", error))
    }
    async fn after_commit(
        &self,
        _: &DomainEvent,
        _: &Self::Prepared,
        notification: &Notification,
    ) -> std::result::Result<(), WorkerError> {
        self.publish_created(notification);
        Ok(())
    }
}

fn extract_review_failure_reason(step_results_json: &str) -> Option<String> {
    let details = serde_json::from_str::<Value>(step_results_json).ok()?;
    details
        .get("auditor")
        .and_then(|auditor| auditor.get("reason"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use db::{
        create_sqlite_pool, run_migrations, CreateDomainEvent, CreateProject, CreateTask,
        DomainEventRepo, NotificationRepo, ProjectRepo,
    };
    use serde_json::json;

    async fn fixture() -> (Arc<SqliteDb>, Arc<NotificationService>, db::Task) {
        let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
        run_migrations(&pool).await.unwrap();
        let db = Arc::new(SqliteDb::new(pool));
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
        let task = TaskRepo::create(
            &*db,
            CreateTask {
                id: new_uuid_v4(),
                project_id: project.id,
                parent_task_id: None,
                subtask_order: None,
                assignee_type: None,
                assignee_id: None,
                title: "Task title".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "review".to_owned(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now_rfc3339(),
                updated_at: now_rfc3339(),
            },
        )
        .await
        .unwrap();
        let service = Arc::new(NotificationService::new(
            Arc::clone(&db),
            Arc::new(EventBus::new(1)),
        ));
        (db, service, task)
    }

    async fn append(
        db: &SqliteDb,
        task: &db::Task,
        event_type: &str,
        payload: Value,
    ) -> DomainEvent {
        let id = new_uuid_v4();
        db.append_event(CreateDomainEvent {
            id: id.clone(),
            event_type: event_type.to_owned(),
            entity_type: "task".to_owned(),
            entity_id: task.id.clone(),
            actor_type: "system".to_owned(),
            actor_id: None,
            scope_type: "task".to_owned(),
            scope_id: task.id.clone(),
            correlation_id: id,
            causation_id: None,
            causation_depth: 0,
            dedupe_key: None,
            payload_json: payload.to_string(),
            created_at: now_rfc3339(),
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn durable_notifications_preserve_the_old_trigger_list_and_bodies() {
        let (db, service, task) = fixture().await;
        // Automatic and manual moves have one authoritative transition each.
        for actor in ["system", "user:test"] {
            db.append_event(db::CreateDomainEvent::task_transition(
                new_uuid_v4(),
                &task.id,
                &task.project_id,
                "review",
                "done",
                Some("complete"),
                actor,
                "complete",
                false,
                now_rfc3339(),
                json!({}),
            ))
            .await
            .unwrap();
        }
        let long_reason = "needs owner input ".repeat(100);
        sqlx::query("UPDATE task SET blocked_json = ?, version = version + 1 WHERE id = ?")
            .bind(json!({"reason": long_reason, "kind": "review_blocked"}).to_string())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        sqlx::query("UPDATE task SET failed_json = ?, blocked_json = NULL, version = version + 1 WHERE id = ?").bind(json!({"reason": "hard failure", "kind": "hook_failed"}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
        for reason in ["crash_recovery", "agent_timeout", "shutdown"] {
            sqlx::query("UPDATE task SET error_annotation = ?, failed_json = NULL, version = version + 1 WHERE id = ?").bind(json!({"type": "recovery_required", "blocking_reason": reason}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
        }
        sqlx::query("UPDATE task SET error_annotation = ?, version = version + 1 WHERE id = ?").bind(json!({"type": "merge_conflict", "message": "conflicts in file.rs", "detected_at": now_rfc3339()}).to_string()).bind(&task.id).execute(db.pool()).await.unwrap();
        for (index, status) in [db::ReviewStatus::Passed, db::ReviewStatus::Failed]
            .into_iter()
            .enumerate()
        {
            let execution_id = new_uuid_v4();
            sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES (?, ?, 'reviewer', 'completed', ?, ?)").bind(&execution_id).bind(&task.id).bind(now_rfc3339()).bind(now_rfc3339()).execute(db.pool()).await.unwrap();
            let review = ReviewRepo::create(
                &*db,
                db::CreateReview {
                    id: new_uuid_v4(),
                    task_id: task.id.clone(),
                    execution_id,
                    attempt_number: index as i64 + 1,
                    status: db::ReviewStatus::Running,
                    step_results_json: "{}".to_owned(),
                    started_at: now_rfc3339(),
                    created_at: now_rfc3339(),
                    updated_at: now_rfc3339(),
                },
            )
            .await
            .unwrap();
            ReviewRepo::update_status(
                &*db,
                &review.id,
                status,
                json!({"auditor": {"reason": "Missing tests"}}).to_string(),
                Some(now_rfc3339()),
                &now_rfc3339(),
            )
            .await
            .unwrap();
        }
        append(&db, &task, "notification.requested", json!({"project_id": task.project_id, "task_id": null, "event_type": "project.autonomy_stalled", "title": "Project Agent stopped: 1 open incident(s) unanswered", "body": "the Project Agent's hourly wake budget is exhausted"})).await;
        // Per-attempt outcomes, resumed executions and shutdown stay silent.
        for event_type in [
            "execution.failed",
            "execution.cancelled",
            "task.execution_resumed",
            "task.recovery_action",
        ] {
            append(&db, &task, event_type, json!({"reason": "crash_recovery"})).await;
        }
        append(
            &db,
            &task,
            "task.transitioned",
            json!({"from_state": "done", "to_state": "done"}),
        )
        .await;
        append(
            &db,
            &task,
            "review.status_changed",
            json!({"task_id": task.id, "status": "running", "finished": false}),
        )
        .await;
        let mut bus_receiver = service.event_bus.subscribe();
        for _ in 0..20 {
            service.event_bus.publish(ForgeEvent {
                event_type: "noise".to_owned(),
                entity_id: task.id.clone(),
                timestamp: event_timestamp(),
                context: EventContext::Empty {},
            });
        }
        assert!(matches!(
            bus_receiver.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_))
        ));
        WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service))
            .run_once(100)
            .await
            .unwrap();
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(
            "SELECT event_type, title, body FROM notification ORDER BY event_type, body",
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        let expected = [
            "merge.failed",
            "project.autonomy_stalled",
            "review.failed",
            "review.passed",
            "task.blocked",
            "task.done",
            "task.done",
            "task.failed",
            "task.recovery_required",
            "task.recovery_required",
        ];
        assert_eq!(
            rows.iter().map(|row| row.0.as_str()).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            rows.iter()
                .find(|row| row.0 == "task.blocked")
                .unwrap()
                .2
                .as_deref(),
            Some(long_reason.as_str())
        );
        assert_eq!(
            rows.iter().find(|row| row.0 == "review.passed").unwrap().1,
            "Review passed: Task title"
        );
        assert_eq!(
            rows.iter()
                .find(|row| row.0 == "review.failed")
                .unwrap()
                .2
                .as_deref(),
            Some("Missing tests")
        );
        // Re-instantiation resumes the cursor; no second row for any outcome.
        WorkerRuntime::new(Arc::clone(&db), service)
            .run_once(100)
            .await
            .unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM notification")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 10);
    }

    #[tokio::test]
    async fn notification_and_cursor_survive_crash_before_after_commit() {
        let (db, service, task) = fixture().await;
        let event = append(&db, &task, "notification.requested", json!({"project_id": task.project_id, "task_id": task.id, "event_type": "task.blocked", "title": task.title, "body": "owner input"})).await;
        let Outcome::Done(prepared) = service.handle(&event).await.unwrap() else {
            panic!("request matches");
        };
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        service.commit(&mut tx, &event, &prepared).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(NotificationRepo::unread_count(&*db, None).await.unwrap(), 0);
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        service.commit(&mut tx, &event, &prepared).await.unwrap();
        db.advance_domain_event_cursor_in_tx(
            &mut tx,
            CONSUMER_NAME,
            0,
            event.sequence,
            &now_rfc3339(),
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            WorkerRuntime::new(Arc::clone(&db), service)
                .run_once(100)
                .await
                .unwrap(),
            0
        );
        assert_eq!(NotificationRepo::unread_count(&*db, None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn upgrade_head_seed_prevents_historical_notification_delivery() {
        let (db, service, task) = fixture().await;
        let mut tx = db::begin_immediate(db.pool()).await.unwrap();
        for index in 0..3000 {
            let id = format!("old-notification-{index}");
            db.append_event_in_tx(&mut tx, &CreateDomainEvent { id: id.clone(), event_type: "notification.requested".to_owned(), entity_type: "task".to_owned(), entity_id: task.id.clone(), actor_type: "system".to_owned(), actor_id: None, scope_type: "task".to_owned(), scope_id: task.id.clone(), correlation_id: id, causation_id: None, causation_depth: 0, dedupe_key: None, payload_json: json!({"project_id": task.project_id, "task_id": task.id, "event_type": "task.blocked", "title": "old", "body": null}).to_string(), created_at: now_rfc3339() }).await.unwrap();
        }
        sqlx::query("DELETE FROM event_consumer_cursor WHERE consumer_name IN ('project-hooks', 'notifications')").execute(&mut *tx).await.unwrap();
        sqlx::raw_sql(
            include_str!("../../db/migrations/V202610030200__notify_hooks_consumers.sql")
                .split("-- These mutations")
                .next()
                .unwrap(),
        )
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            WorkerRuntime::new(Arc::clone(&db), Arc::clone(&service))
                .run_once(100)
                .await
                .unwrap(),
            0
        );
        assert_eq!(NotificationRepo::unread_count(&*db, None).await.unwrap(), 0);
        append(&db, &task, "notification.requested", json!({"project_id": task.project_id, "task_id": task.id, "event_type": "task.blocked", "title": "new", "body": null})).await;
        assert_eq!(
            WorkerRuntime::new(Arc::clone(&db), service)
                .run_once(100)
                .await
                .unwrap(),
            1
        );
    }
    #[tokio::test]
    async fn deleted_task_request_does_not_block_following_delivery() {
        let (db, service, task) = fixture().await;
        append(&db, &task, "notification.requested", json!({"project_id": task.project_id, "task_id": task.id, "event_type": "task.blocked", "title": "Removed Task", "body": null})).await;
        sqlx::query("DELETE FROM task WHERE id = ?")
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        append(&db, &task, "notification.requested", json!({"project_id": task.project_id, "task_id": null, "event_type": "project.autonomy_stalled", "title": "Retained Project", "body": null})).await;
        assert_eq!(
            WorkerRuntime::new(Arc::clone(&db), service)
                .run_once(100)
                .await
                .unwrap(),
            1
        );
        assert_eq!(NotificationRepo::unread_count(&*db, None).await.unwrap(), 1);
        let quarantines: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM worker_dead_letter WHERE worker_name = 'notifications'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(quarantines, 0);
    }
}

#[cfg(test)]
mod parity_tests;
