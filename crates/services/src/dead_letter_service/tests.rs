use super::*;
use crate::worker_runtime::{Outcome, RetryPolicy, Subscription, WorkerError};
use async_trait::async_trait;
use db::{create_sqlite_pool, run_migrations, CreateDomainEvent, DomainEvent, DomainEventRepo};
use sqlx::{Sqlite, Transaction};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

struct Consumer {
    name: &'static str,
    fail: AtomicBool,
    hold: AtomicBool,
    entered: Notify,
    release: Notify,
    seen: AtomicUsize,
    published: AtomicUsize,
}
impl Consumer {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            fail: AtomicBool::new(false),
            hold: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
            seen: AtomicUsize::new(0),
            published: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl Worker for Consumer {
    type Prepared = ();
    fn name(&self) -> &str {
        self.name
    }
    fn subscription(&self) -> Subscription {
        Subscription::Exact(vec!["test.event".into()])
    }
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy {
            max_attempts: 1,
            ..Default::default()
        }
    }
    async fn handle(&self, _: &DomainEvent) -> std::result::Result<Outcome<()>, WorkerError> {
        self.seen.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        if self.hold.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(Outcome::Done(()))
    }
    async fn commit(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        event: &DomainEvent,
        _: &(),
    ) -> std::result::Result<(), WorkerError> {
        sqlx::query(
            "INSERT INTO replay_effect (consumer, sequence) VALUES (?, ?) ON CONFLICT DO NOTHING",
        )
        .bind(self.name)
        .bind(event.sequence)
        .execute(&mut **tx)
        .await
        .map_err(|e| WorkerError::new(e.to_string()))?;
        if self.fail.load(Ordering::SeqCst) {
            return Err(WorkerError::new("new commit error"));
        }
        Ok(())
    }
    async fn after_commit(
        &self,
        _: &DomainEvent,
        _: &(),
        _: &(),
    ) -> std::result::Result<(), WorkerError> {
        self.published.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn admin() -> DeadLetterActor<'static> {
    DeadLetterActor {
        user_id: "admin-1",
        is_admin: true,
    }
}
async fn fixture() -> (Arc<SqliteDb>, Arc<Consumer>, DeadLetterService, String) {
    let pool = create_sqlite_pool("sqlite::memory:").await.unwrap();
    run_migrations(&pool).await.unwrap();
    sqlx::query("CREATE TABLE replay_effect (consumer TEXT, sequence INTEGER, PRIMARY KEY (consumer, sequence))").execute(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let worker = Arc::new(Consumer::new("original"));
    db.append_event(CreateDomainEvent {
        id: db::new_uuid_v4(),
        event_type: "test.event".into(),
        entity_type: "test".into(),
        entity_id: "test".into(),
        actor_type: "system".into(),
        actor_id: None,
        scope_type: "system".into(),
        scope_id: "test".into(),
        correlation_id: "test".into(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: None,
        payload_json: r#"{"secret":"do not expose"}"#.into(),
        created_at: db::now_rfc3339(),
    })
    .await
    .unwrap();
    worker.fail.store(true, Ordering::SeqCst);
    assert_eq!(
        WorkerRuntime::new(db.clone(), worker.clone())
            .run_once(1)
            .await
            .unwrap(),
        1
    );
    worker.fail.store(false, Ordering::SeqCst);
    let id: String = sqlx::query_scalar("SELECT id FROM worker_dead_letter")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let mut service = DeadLetterService::new(db.clone());
    service.register(worker.clone());
    (db, worker, service, id)
}
async fn effect_count(db: &SqliteDb) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM replay_effect")
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn replay_commits_effect_and_resolution_once_without_touching_cursors_or_other_consumers() {
    let (db, worker, mut service, id) = fixture().await;
    let other = Arc::new(Consumer::new("other"));
    service.register(other.clone());
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence = 100, updated_at = '2000-01-01T00:00:00Z'",
    )
    .execute(db.pool())
    .await
    .unwrap();
    let result = service.replay(admin(), &id).await.unwrap();
    assert_eq!(result.outcome, "replayed");
    assert_eq!(result.dead_letter.summary.attempts, 2);
    assert_eq!(result.dead_letter.resolved_by.as_deref(), Some("admin-1"));
    assert!(result.dead_letter.resolved_at.is_some());
    assert_eq!(effect_count(&db).await, 1);
    assert_eq!(worker.published.load(Ordering::SeqCst), 1);
    assert_eq!(other.seen.load(Ordering::SeqCst), 0);
    let cursor = db.get_consumer_cursor("original").await.unwrap().unwrap();
    assert_eq!(cursor.last_sequence, 100);
    assert_eq!(cursor.updated_at, "2000-01-01T00:00:00Z");
    assert!(matches!(
        service.replay(admin(), &id).await,
        Err(ServiceError::Db(db::DbError::VersionConflict))
    ));
    assert_eq!(effect_count(&db).await, 1);
    assert_eq!(
        db.worker_dead_letter_history("original").await.unwrap().0,
        0
    );
    assert!(db
        .worker_dead_letter_issues(&["original"], "1900")
        .await
        .unwrap()
        .is_empty());
    let listed = service
        .list(admin(), None, DeadLetterState::Resolved, None, 50)
        .await
        .unwrap();
    assert_eq!(listed.items.len(), 1);
}

#[tokio::test]
async fn replay_failure_rolls_back_effect_and_audits_new_attempt_without_automatic_retry() {
    let (db, worker, service, id) = fixture().await;
    worker.fail.store(true, Ordering::SeqCst);
    let result = service.replay(admin(), &id).await.unwrap();
    assert_eq!(result.outcome, "replay_failed");
    assert!(result.dead_letter.resolved_at.is_none());
    assert_eq!(result.dead_letter.summary.attempts, 2);
    assert_eq!(result.dead_letter.summary.reason, "new commit error");
    assert_eq!(effect_count(&db).await, 0);
    assert_eq!(
        db.worker_dead_letter_history("original").await.unwrap().0,
        1
    );
    let action: (String, String, String) =
        sqlx::query_as("SELECT actor_id, outcome, reason FROM worker_dead_letter_action")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        action,
        (
            "admin-1".into(),
            "replay_failed".into(),
            "new commit error".into()
        )
    );
    let seen = worker.seen.load(Ordering::SeqCst);
    assert_eq!(
        WorkerRuntime::new(db.clone(), worker.clone())
            .run_once(1)
            .await
            .unwrap(),
        0
    );
    assert_eq!(worker.seen.load(Ordering::SeqCst), seen);
    worker.fail.store(false, Ordering::SeqCst);
    service.replay(admin(), &id).await.unwrap();
    assert_eq!(effect_count(&db).await, 1);
}

#[tokio::test]
async fn dismiss_audits_reason_and_excludes_resolved_from_status() {
    let (db, worker, service, id) = fixture().await;
    let result = service
        .dismiss(admin(), &id, Some("obsolete event"))
        .await
        .unwrap();
    assert_eq!(result.outcome, "dismissed");
    assert_eq!(
        result.dead_letter.resolution_reason.as_deref(),
        Some("obsolete event")
    );
    assert_eq!(result.dead_letter.summary.attempts, 1);
    assert_eq!(effect_count(&db).await, 0);
    assert_eq!(worker.seen.load(Ordering::SeqCst), 1);
    assert_eq!(
        db.worker_dead_letter_history("original").await.unwrap().0,
        0
    );
    assert!(matches!(
        service.dismiss(admin(), &id, None).await,
        Err(ServiceError::Db(db::DbError::VersionConflict))
    ));
    assert!(matches!(
        service.replay(admin(), "unknown").await,
        Err(ServiceError::Db(db::DbError::NotFound))
    ));
}

#[tokio::test]
async fn dismiss_wins_race_during_replay_preparation_and_replay_has_no_effect() {
    let (db, worker, service, id) = fixture().await;
    worker.hold.store(true, Ordering::SeqCst);
    worker.entered.notified().await;
    let service = Arc::new(service);
    let replay_service = service.clone();
    let replay_id = id.clone();
    let replay = tokio::spawn(async move { replay_service.replay(admin(), &replay_id).await });
    worker.entered.notified().await;
    service.dismiss(admin(), &id, None).await.unwrap();
    worker.release.notify_one();
    assert!(matches!(
        replay.await.unwrap(),
        Err(ServiceError::Db(db::DbError::VersionConflict))
    ));
    assert_eq!(effect_count(&db).await, 0);
    let actions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter_action")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(actions, 1);
}

#[tokio::test]
async fn concurrent_replays_have_one_winner_and_one_effect() {
    let (db, worker, service, id) = fixture().await;
    worker.hold.store(true, Ordering::SeqCst);
    worker.entered.notified().await;
    let service = Arc::new(service);
    let a = {
        let service = service.clone();
        let id = id.clone();
        tokio::spawn(async move { service.replay(admin(), &id).await })
    };
    worker.entered.notified().await;
    let b = {
        let service = service.clone();
        let id = id.clone();
        tokio::spawn(async move { service.replay(admin(), &id).await })
    };
    worker.entered.notified().await;
    worker.release.notify_waiters();
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    let loser = if a.is_err() { a } else { b };
    assert!(matches!(
        loser,
        Err(ServiceError::Db(db::DbError::VersionConflict))
    ));
    assert_eq!(effect_count(&db).await, 1);
}

#[tokio::test]
async fn service_actions_require_admin_before_lookup() {
    let (_, _, service, id) = fixture().await;
    let user = || DeadLetterActor {
        user_id: "user",
        is_admin: false,
    };
    assert!(matches!(
        service.replay(user(), &id).await,
        Err(ServiceError::AuthorizationDenied { .. })
    ));
    assert!(matches!(
        service.dismiss(user(), &id, None).await,
        Err(ServiceError::AuthorizationDenied { .. })
    ));
    assert!(matches!(
        service
            .list(user(), None, DeadLetterState::Open, None, 50)
            .await,
        Err(ServiceError::AuthorizationDenied { .. })
    ));
}

#[tokio::test]
async fn unregistered_consumer_replay_is_an_audited_failure() {
    let (db, _, _, id) = fixture().await;
    let service = DeadLetterService::new(db.clone());
    let result = service.replay(admin(), &id).await.unwrap();
    assert_eq!(result.outcome, "replay_failed");
    assert_eq!(result.dead_letter.summary.attempts, 2);
    assert_eq!(
        result.dead_letter.summary.reason,
        "dead letter has no registered event consumer"
    );
    assert!(result.dead_letter.resolved_at.is_none());
    assert_eq!(effect_count(&db).await, 0);
}
