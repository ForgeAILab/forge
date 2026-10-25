//! Attention projection, wake policy, and consumer health cases.

use super::*;

async fn health_service() -> AttentionService {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    AttentionService::new(Arc::clone(&db), crate::test_support::noop_wake_turns())
}

async fn append_projection_event(service: &AttentionService, id: &str, kind: &str) -> DomainEvent {
    service
        .db
        .append_event(CreateDomainEvent {
            id: id.into(),
            event_type: kind.into(),
            entity_type: "task".into(),
            entity_id: id.into(),
            actor_type: "system".into(),
            actor_id: None,
            scope_type: "project".into(),
            scope_id: "projection-test".into(),
            correlation_id: id.into(),
            causation_id: None,
            causation_depth: 0,
            dedupe_key: Some(id.into()),
            payload_json: "{}".into(),
            created_at: now_rfc3339(),
        })
        .await
        .unwrap()
}
/// m2: the Task condition's `material_blocker` against the real incident
/// digest inputs. Each blocker shape is written to a Task, its
/// `task.interruption_changed` event is projected by the Attention
/// service, and the incident it materializes (Task context, failure
/// class, action offers and all) must keep its canonical digest when the
/// interruption and intervention flag are replaced by the condition's
/// material blocker. Stage 4 hashes through this digest, not the struct.
#[tokio::test]
async fn integration_progress_is_quiet_and_owner_blockers_have_stable_incidents() {
    let service = health_service().await;
    let db = &service.db;
    let now = now_rfc3339();
    db::ProjectRepo::create(
        &**db,
        db::CreateProject {
            id: "integration".into(),
            owner_id: None,
            name: "Integration".into(),
            primary_repo_id: None,
            updated_at: now.clone(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let task = db::TaskRepo::create(
        &**db,
        db::CreateTask {
            id: "integration-task".into(),
            project_id: "integration".into(),
            parent_task_id: None,
            assignee_type: None,
            assignee_id: None,
            title: "Integration".into(),
            description: None,
            task_type: "task".into(),
            status: "merging".into(),
            is_automation: false,
            priority: 0,
            task_state_config: None,
            merge_config: None,
            subtask_order: None,
            plan: None,
            updated_at: now.clone(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    let baseline: i64 = sqlx::query_scalar("SELECT count(*) FROM attention_projection")
        .fetch_one(db.pool())
        .await
        .unwrap();
    // The condition is driven through the statement seam under a claimed
    // Task step, as stage B's consumer will. A statement writes no event
    // and no version (asserted below), and `task.interruption_changed`
    // is deduplicated by Task version, so the consumer's own commit is
    // modelled by the version bump and the event appended after each
    // statement: emitting that event is a stage D requirement.
    use db::TaskStepRepo as _;
    let step_id = db::new_uuid_v4();
    db.enqueue_step(&db::EnqueueTaskStep {
        id: step_id.clone(),
        task_id: task.id.clone(),
        kind: "command".into(),
        payload_json: "{}".into(),
        causation_step_id: None,
        causation_key: step_id.clone(),
        chain_id: step_id,
        chain_position: 1,
        expected_status: task.status.clone(),
        expected_version: task.version,
        expected_epoch: None,
        lane: "fast".into(),
        available_at: now_rfc3339(),
    })
    .await
    .unwrap();
    let step = db
        .claim_step(
            "attention-tests",
            Some(&task.id),
            &db::task_writer::lease_deadline(),
        )
        .await
        .unwrap()
        .unwrap();
    let task_id = task.id.clone();
    let state = |statement: db::ConditionStatement| {
        let (db, service, step, task_id) = (db.clone(), &service, step.clone(), task_id.clone());
        async move {
            let events = |db: Arc<db::SqliteDb>| async move {
                sqlx::query_scalar::<_, i64>("SELECT count(*) FROM domain_event")
                    .fetch_one(db.pool())
                    .await
                    .unwrap()
            };
            let before = events(db.clone()).await;
            db::task_writer::in_task_step(step, async {
                let mut tx = db::begin_immediate(db.pool()).await.unwrap();
                db.state_integration_condition_in_tx(&mut tx, &task_id, &statement)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
            })
            .await;
            assert_eq!(
                events(db.clone()).await,
                before,
                "a statement writes no event"
            );
            sqlx::query("UPDATE task SET version=version+1 WHERE id=?")
                .bind(&task_id)
                .execute(db.pool())
                .await
                .unwrap();
            let task = db::TaskRepo::get_by_id(&*db, &task_id, false)
                .await
                .unwrap()
                .unwrap();
            db::DomainEventRepo::append_event(
                &*db,
                CreateDomainEvent::task_interruption_changed(&task),
            )
            .await
            .unwrap();
            service.project_once(100).await.unwrap();
            task
        }
    };
    let integration =
        |reason: api_types::IntegrationReason| db::ConditionStatement::Integration { reason };
    let cleared = |attempt: &str| db::ConditionStatement::IntegrationCleared {
        attempt_id: api_types::IntegrationAttemptId::new(attempt),
    };
    for condition in crate::task_actions::tests::integration_conditions()
        .into_iter()
        .filter(|c| !db::material_blocker(c).requires_intervention)
    {
        let reason = condition.integration_reason().unwrap().clone();
        let stated = state(integration(reason.clone())).await;
        assert_eq!(stated.condition.integration_wait(), Some(&reason));
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM attention_projection")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            baseline,
            "{reason:?} raises no incident"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM domain_event WHERE event_type='attention.wake_requested'"
            )
            .fetch_one(db.pool())
            .await
            .unwrap(),
            0
        );
    }
    let mut digests = Vec::new();
    let mut owner = "attempt";
    for (attempt, message) in [
        ("attempt", "target dirty"),
        ("replacement", "target dirty"),
        ("replacement", "different owner failure"),
        ("replacement", "different owner failure"),
    ] {
        if attempt != owner {
            // Another attempt takes the row only once its owner is cleared.
            state(cleared(owner)).await;
            owner = attempt;
        }
        let stated = state(integration(api_types::IntegrationReason::Deferred {
            attempt_id: api_types::IntegrationAttemptId::new(attempt),
            cause: api_types::IntegrationDeferralCause::TargetDirty,
            owner_id: Some("owner".into()),
            message: message.into(),
        }))
        .await;
        assert!(db::material_blocker(&stated.condition).requires_intervention);
        let raw: String = sqlx::query_scalar("SELECT id FROM attention_projection WHERE json_extract(details_json,'$.entity_id')=? ORDER BY updated_at DESC LIMIT 1").bind(&task.id).fetch_one(db.pool()).await.unwrap();
        let attention = db::AttentionRepo::get_attention(&**db, &raw)
            .await
            .unwrap()
            .unwrap();
        digests.push(db::canonical_attention_incident_digest(&attention));
    }
    assert_eq!(digests[0], digests[1], "the attempt is not material");
    assert_ne!(
        digests[1], digests[2],
        "a changed owner failure changes the digest once"
    );
    assert_eq!(
        digests[2], digests[3],
        "unchanged failure stays the same incident"
    );
}

#[tokio::test]
async fn material_blocker_keeps_the_real_incident_digest() {
    let service = health_service().await;
    let db = &service.db;
    let now = now_rfc3339();
    db::ProjectRepo::create(
        &**db,
        db::CreateProject {
            id: "material".into(),
            owner_id: None,
            name: "Material".into(),
            primary_repo_id: None,
            updated_at: now.clone(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: now.clone(),
        },
    )
    .await
    .unwrap();
    service.project_once(1).await.unwrap(); // one-time worker initialization
    let text = |value: Value| Some(value.to_string());
    let mut shapes = vec![
        (
            text(json!({"type":"manual_stop","blocking_reason":"held"})),
            None,
            None,
        ),
        (
            text(
                json!({"type":"review_needs_owner","blocking_reason":"finding","blocked_execution_id":"e"}),
            ),
            text(json!({"kind":"review_needs_owner","reason":"owner","execution_id":"e"})),
            None,
        ),
        (
            text(json!({"type":"merge_conflict","message":"advisory"})),
            None,
            None,
        ),
        (
            text(json!({"type":"retry_exhausted","message":"spent"})),
            text(json!({"kind":"retry_exhausted","reason":"spent"})),
            None,
        ),
        (
            None,
            None,
            text(json!({"kind":"executor_failed","reason":"failure","execution_id":"e"})),
        ),
        (
            text(json!({"type":"unknown","message":"retained"})),
            None,
            None,
        ),
        (
            None,
            text(json!({"kind":"manual_stop","reason":"manual"})),
            None,
        ),
        (
            text(json!({"type":"workspace_error","message":"m".repeat(10000)})),
            None,
            None,
        ),
        // Malformed and non-object values in each column.
        (Some("not json".into()), None, None),
        (Some("[1,2]".into()), None, None),
        (None, Some("not json".into()), None),
        (None, Some("42".into()), None),
        (None, None, Some("{".into())),
        (None, None, Some("[]".into())),
        (
            Some("not json".into()),
            text(json!({"kind":"workspace_error","reason":"real","execution_id":"e"})),
            None,
        ),
    ];
    for kind in db::LEGACY_BLOCKING_ANNOTATION_KINDS {
        let annotation = json!({"type":kind,"message":"detail","blocking_reason":"reason","blocked_by":"owner","blocked_execution_id":"execution"});
        let interruption = json!({"kind":kind,"reason":"reason","execution_id":"execution","details":{"key":"detail"}});
        shapes.push((text(annotation.clone()), None, None));
        shapes.push((text(annotation.clone()), text(interruption.clone()), None));
        shapes.push((
            text(annotation),
            text(interruption.clone()),
            text(interruption),
        ));
    }
    let mut incidents = 0;
    for (index, (annotation, blocked, failed)) in shapes.into_iter().enumerate() {
        let id = format!("material-{index}");
        let mut task = db::TaskRepo::create(
            &**db,
            db::CreateTask {
                id: id.clone(),
                project_id: "material".into(),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: id.clone(),
                description: None,
                task_type: "task".into(),
                status: "in_progress".into(),
                is_automation: false,
                priority: 0,
                task_state_config: None,
                merge_config: None,
                subtask_order: None,
                plan: None,
                updated_at: now.clone(),
                created_at: now.clone(),
            },
        )
        .await
        .unwrap();
        // The writer's own event, appended with the legacy columns.
        let mut transaction = db::begin_immediate(db.pool()).await.unwrap();
        sqlx::query("UPDATE task SET error_annotation=?,blocked_json=?,failed_json=? WHERE id=?")
            .bind(&annotation)
            .bind(&blocked)
            .bind(&failed)
            .bind(&id)
            .execute(&mut *transaction)
            .await
            .unwrap();
        db.sync_condition_in_tx(&mut transaction, &id)
            .await
            .unwrap();
        task.error_annotation = annotation;
        task.blocked_json = blocked;
        task.failed_json = failed;
        task.condition = db::task_condition::decode(
            &sqlx::query_scalar::<_, String>("SELECT condition_json FROM task WHERE id=?")
                .bind(&id)
                .fetch_one(&mut *transaction)
                .await
                .unwrap(),
        )
        .unwrap();
        let event = CreateDomainEvent::task_interruption_changed(&task);
        db::DomainEventRepo::append_event_in_tx(&**db, &mut transaction, &event)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        service.project_once(100).await.unwrap();

        let material = db::material_blocker(&db.task_condition(&id).await.unwrap());
        let payload: Value = serde_json::from_str(&event.payload_json).unwrap();
        assert_eq!(
            json!(material.requires_intervention),
            payload["material_blocker"]["requires_intervention"],
            "{id}"
        );
        let incident: Option<String> = sqlx::query_scalar(
            "SELECT id FROM attention_projection WHERE json_extract(details_json,'$.entity_id')=?",
        )
        .bind(&id)
        .fetch_optional(db.pool())
        .await
        .unwrap();
        let Some(incident) = incident else {
            assert!(
                !material.requires_intervention,
                "{id}: a blocker that needs intervention has an incident"
            );
            continue;
        };
        incidents += 1;
        let mut attention = db::AttentionRepo::get_attention(&**db, &incident)
            .await
            .unwrap()
            .unwrap();
        let real = db::canonical_attention_incident_digest(&attention);
        let mut details: Value = serde_json::from_str(&attention.details_json).unwrap();
        // The real details carry far more than the interruption.
        assert!(details["task"].is_object(), "{id}: {details}");
        assert!(details.get("failure_class").is_some());
        let mut stated = details["interruption"].clone();
        db::strip_attention_delivery_metadata(&mut stated);
        assert_eq!(json!(material.interruption), stated, "{id}");
        details["interruption"] = json!(material.interruption);
        details["recovery"]["requires_intervention"] = json!(material.requires_intervention);
        attention.details_json = details.to_string();
        assert_eq!(
            real,
            db::canonical_attention_incident_digest(&attention),
            "{id}: the material blocker re-arms a consumed incident"
        );
        // A different blocker does change it: the digest is not vacuous.
        details["interruption"] = json!({"source":"blocked","kind":"another","reason":"cause"});
        attention.details_json = details.to_string();
        assert_ne!(real, db::canonical_attention_incident_digest(&attention));
    }
    assert!(
        incidents >= 20,
        "only {incidents} shapes raised an incident"
    );
}

#[tokio::test]
async fn runtime_upgrade_preserves_cursor_ignores_legacy_lease_and_projects_once() {
    let service = health_service().await;
    let old = append_projection_event(&service, "old-incident", "validation.failed").await;
    let next = append_projection_event(&service, "new-incident", "validation.failed").await;
    sqlx::query("INSERT INTO event_consumer_cursor (consumer_name, last_sequence, updated_at) VALUES (?, ?, ?)")
            .bind(CONSUMER_NAME).bind(old.sequence).bind(now_rfc3339()).execute(service.db.pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../db/tests/fixtures/event_delivery.sql"
    ))
    .execute(service.db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO event_processing_lease (consumer_name, event_sequence, lease_owner, leased_until, attempts, updated_at) VALUES (?, ?, 'legacy', '2999-01-01T00:00:00Z', 1, ?)")
            .bind(CONSUMER_NAME).bind(next.sequence).bind(now_rfc3339()).execute(service.db.pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../db/migrations/V202610020700__retire_event_delivery_leases.sql"
    ))
    .execute(service.db.pool())
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TRIGGER fail_after_projection BEFORE INSERT ON domain_event WHEN NEW.event_type LIKE 'agent.wake.%' BEGIN SELECT RAISE(ABORT, 'test failure after projection write'); END;")
            .execute(service.db.pool()).await.unwrap();
    assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
    assert_eq!(
        service
            .consumer_cursor()
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        old.sequence
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection")
        .fetch_one(service.db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER fail_after_projection")
        .execute(service.db.pool())
        .await
        .unwrap();
    sqlx::query("UPDATE worker_health SET retry_not_before = '2000-01-01T00:00:00Z'")
        .execute(service.db.pool())
        .await
        .unwrap();
    assert_eq!(service.project_once(100).await.unwrap().processed_events, 1);
    assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
    let keys: Vec<String> = sqlx::query_scalar("SELECT source_event_id FROM attention_projection")
        .fetch_all(service.db.pool())
        .await
        .unwrap();
    assert_eq!(keys, vec![next.id]);
    assert_eq!(
        service
            .consumer_cursor()
            .await
            .unwrap()
            .unwrap()
            .last_sequence,
        next.sequence
    );
    let health = service.consumer_health().await.unwrap().unwrap();
    assert!(health.last_success_at.is_some());
    assert!(health.last_error_code.is_none());
}
#[tokio::test]
async fn runtime_unprojected_event_and_idle_cycle_write_nothing() {
    let service = health_service().await;
    service.project_once(1).await.unwrap(); // one-time worker initialization
    sqlx::raw_sql("CREATE TABLE writes (name TEXT); CREATE TRIGGER observed_health AFTER UPDATE ON worker_health BEGIN INSERT INTO writes VALUES ('health'); END;
            CREATE TRIGGER observed_cursor AFTER UPDATE ON event_consumer_cursor BEGIN INSERT INTO writes VALUES ('cursor'); END;")
            .execute(service.db.pool()).await.unwrap();
    append_projection_event(&service, "irrelevant", "ignored").await;
    for _ in 0..4 {
        assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
    }
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM writes")
        .fetch_one(service.db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}
#[tokio::test]
async fn runtime_commit_check_quarantines_once_and_next_incident_proceeds() {
    let service = health_service().await;
    let poison = append_projection_event(&service, "poison", "validation.failed").await;
    let good = append_projection_event(&service, "good", "validation.failed").await;
    let Outcome::Done(prepared) = service.prepare_projection(&poison).await.unwrap() else {
        panic!("projected");
    };
    let mut tx = db::begin_immediate(service.db.pool()).await.unwrap();
    service
        .commit_projection(&mut tx, &poison, &prepared)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    // Force exact semantic dedupe conflict at commit, after the first write.
    sqlx::query("UPDATE domain_event SET payload_json = '{}' WHERE event_type LIKE 'agent.wake.%'")
        .execute(service.db.pool())
        .await
        .unwrap();
    // Remove the fixture's linked setup disposition before corrupting its source.
    sqlx::query("DELETE FROM agent_wake_disposition_current")
        .execute(service.db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM agent_wake_disposition")
        .execute(service.db.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM attention_projection")
        .execute(service.db.pool())
        .await
        .unwrap();
    let run = service.project_once(100).await.unwrap();
    assert_eq!(run.processed_events, 2); // terminal acknowledgement and good projection
    assert_eq!(service.project_once(100).await.unwrap().processed_events, 0);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM worker_dead_letter WHERE worker_name = ?")
            .bind(CONSUMER_NAME)
            .fetch_one(service.db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    let sources: Vec<String> =
        sqlx::query_scalar("SELECT source_event_id FROM attention_projection")
            .fetch_all(service.db.pool())
            .await
            .unwrap();
    assert_eq!(sources, vec![good.id]);
}

#[tokio::test]
async fn consumer_health_ignores_resolved_quarantines() {
    let service = health_service().await;
    service.project_once(1).await.unwrap();
    let mut tx = db::begin_immediate(service.db.pool()).await.unwrap();
    db::WorkerHealth::new(service.db.clone(), CONSUMER_NAME)
        .dead_letter_in_tx(
            &mut tx,
            db::WorkItem {
                source_key: "12",
                item_type: "validation.failed",
            },
            db::FailureState {
                attempts: 8,
                first_failed_at: &now_rfc3339(),
            },
            "projection rejection",
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let health = service.consumer_health().await.unwrap().unwrap();
    assert_eq!(
        health.last_error_message.as_deref(),
        Some("projection rejection")
    );
    let id: String = sqlx::query_scalar("SELECT id FROM worker_dead_letter")
        .fetch_one(service.db.pool())
        .await
        .unwrap();
    crate::dead_letter_service::DeadLetterService::new(service.db.clone())
        .dismiss(
            crate::dead_letter_service::DeadLetterActor {
                user_id: "admin",
                is_admin: true,
            },
            &id,
            None,
        )
        .await
        .unwrap();
    let health = service.consumer_health().await.unwrap().unwrap();
    assert!(health.last_error_code.is_none());
    assert!(health.last_error_message.is_none());
}

fn event(event_type: &str, payload_json: &str) -> DomainEvent {
    DomainEvent {
        sequence: 1,
        id: "event-1".to_owned(),
        event_type: event_type.to_owned(),
        entity_type: "task".to_owned(),
        entity_id: "task-1".to_owned(),
        actor_type: "system".to_owned(),
        actor_id: None,
        scope_type: "project".to_owned(),
        scope_id: "project-1".to_owned(),
        correlation_id: "corr-1".to_owned(),
        causation_id: None,
        causation_depth: 0,
        dedupe_key: None,
        payload_json: payload_json.to_owned(),
        created_at: "2026-01-01T00:00:00Z".to_owned(),
    }
}

fn user_event(event_type: &str, entity_type: &str, payload_json: &str) -> DomainEvent {
    DomainEvent {
        entity_type: entity_type.to_owned(),
        entity_id: "entity-1".to_owned(),
        actor_type: "user".to_owned(),
        actor_id: Some("user-1".to_owned()),
        ..event(event_type, payload_json)
    }
}

#[test]
fn user_decisions_wake_the_agent_as_decision_recorded() {
    assert_eq!(
        classify_event(&user_event(
            "milestone.definition.transitioned",
            "milestone",
            r#"{"revision_id":"rev-1","lifecycle":"approved"}"#
        )),
        Some("decision_recorded")
    );
    assert_eq!(
        classify_event(&user_event(
            "milestone.definition.transitioned",
            "milestone",
            r#"{"revision_id":"rev-1","lifecycle":"rejected"}"#
        )),
        Some("decision_recorded")
    );
    // Proposing is the Agent asking, not the user deciding.
    assert_eq!(
        classify_event(&user_event(
            "milestone.definition.transitioned",
            "milestone",
            r#"{"revision_id":"rev-1","lifecycle":"proposed"}"#
        )),
        None
    );
    assert_eq!(
        classify_event(&user_event(
            "project.decision.approved",
            "project_decision",
            "{}"
        )),
        Some("decision_recorded")
    );
    assert_eq!(
        classify_event(&user_event(
            "project.decision.candidate_rejected",
            "project_decision_candidate",
            "{}"
        )),
        Some("decision_recorded")
    );
    assert_eq!(
        classify_event(&user_event(
            "project.document.approved",
            "project_document_approval",
            "{}"
        )),
        Some("decision_recorded")
    );
    assert_eq!(
        category_metadata("decision_recorded").2,
        "continue_from_decision"
    );
}

#[test]
fn an_agents_own_approval_is_not_a_user_decision() {
    let agent_event = DomainEvent {
        actor_type: "agent".to_owned(),
        actor_id: Some("identity-1".to_owned()),
        ..user_event(
            "milestone.definition.transitioned",
            "milestone",
            r#"{"revision_id":"rev-1","lifecycle":"approved"}"#,
        )
    };
    assert_eq!(classify_event(&agent_event), None);
    assert_eq!(
        classify_event(&event("project.decision.approved", "{}")),
        None
    );
}

#[test]
fn category_split_keeps_every_bucket_and_matches_ten() {
    let split = |total| {
        (
            category_budget(total, "blocker"),
            category_budget(total, "delivery"),
            category_budget(total, "decision"),
        )
    };
    assert_eq!(split(10), (4, 4, 2));
    assert_eq!(split(5), (2, 2, 1));
    assert_eq!(split(7), (3, 2, 2));
    for total in MIN_SPLIT_WAKE_BUDGET..=40 {
        let (blocker, delivery, decision) = split(total);
        assert!(blocker >= 1 && delivery >= 1 && decision >= 1, "{total}");
        assert_eq!(blocker + delivery + decision, total, "{total}");
    }
}

#[test]
fn incident_keys_name_their_category() {
    assert_eq!(
        incident_key_category("attention:decision_recorded:project:p1:milestone:m1"),
        Some("decision_recorded")
    );
    assert_eq!(incident_key_category("attention::project:p1"), None);
    assert_eq!(incident_key_category("wake:decision_recorded"), None);
}

#[test]
fn review_park_attention_uses_the_typed_cause_and_replaces_other_blockers() {
    for reason in [
        "repeated finding F1",
        "only non-blocking findings remain",
        "no change after repair round",
    ] {
        let payload = json!({"requires_intervention":true,"interruption":{"kind":"review_needs_owner","reason":reason}}).to_string();
        let park = event("task.interruption_changed", &payload);
        assert_eq!(classify_event(&park), Some("review_needs_owner"));
        assert_eq!(resolution_categories(&park), vec!["execution_failed"]);
    }
    let failure = event(
        "task.interruption_changed",
        r#"{"requires_intervention":true,"interruption":{"kind":"executor_failed","reason":"no change after repair round"}}"#,
    );
    assert_eq!(classify_event(&failure), Some("execution_failed"));
    assert_eq!(resolution_categories(&failure), vec!["review_needs_owner"]);
    // Integration deferrals reuse the diagnostic failure kind but retain
    // their existing recovery category, rather than becoming review parks.
    let integration = event(
        "task.interruption_changed",
        r#"{"requires_intervention":true,"interruption":{"source":"integration","kind":"review_needs_owner","reason":"integration requires the owner"}}"#,
    );
    assert_eq!(classify_event(&integration), Some("execution_failed"));
    let clear = event(
        "task.interruption_changed",
        r#"{"requires_intervention":false}"#,
    );
    assert_eq!(
        resolution_categories(&clear),
        vec!["execution_failed", "review_needs_owner"]
    );
}

#[test]
fn task_transition_rules_are_deterministic() {
    assert_eq!(
        classify_event(&event("task.transitioned", r#"{"to_state":"blocked"}"#)),
        None
    );
    assert_eq!(
        classify_event(&event("task.transitioned", r#"{"to_state":"review"}"#)),
        None
    );
    assert_eq!(
        classify_event(&event("task.transitioned", r#"{"to_state":"done"}"#)),
        None
    );
    assert!(
        resolution_categories(&event("task.transitioned", r#"{"to_state":"done"}"#)).is_empty()
    );
    assert_eq!(
        classify_event(&event("project_release.candidate_requested", "{}")),
        Some("human_input_required")
    );
    assert_eq!(
        classify_event(&event("execution.failed", r#"{"error":"boom"}"#)),
        None
    );
    assert_eq!(
        classify_event(&event(
            "execution.cancelled",
            r#"{"stop_reason":"user_cancelled"}"#
        )),
        None
    );
    assert_eq!(
        classify_event(&event(
            "task.interruption_changed",
            r#"{"requires_intervention":true}"#
        )),
        Some("execution_failed")
    );
    assert_eq!(
        classify_event(&event(
            "task.interruption_changed",
            r#"{"requires_intervention":false}"#
        )),
        None
    );
    assert_eq!(
        resolution_categories(&event(
            "task.interruption_changed",
            r#"{"requires_intervention":false}"#
        )),
        vec!["execution_failed", "review_needs_owner"]
    );
    assert_eq!(
        classify_event(&event(
            "execution.progress_warning",
            r#"{"execution_id":"execution-1"}"#
        )),
        Some("progress_warning")
    );
    assert_eq!(
        classify_event(&event(
            "execution.stalled",
            r#"{"execution_id":"execution-1"}"#
        )),
        Some("run_stalled")
    );
    assert_eq!(classify_event(&event("execution.completed", "{}")), None);
    assert_eq!(
        resolution_categories(&event("execution.completed", "{}")),
        vec!["progress_warning", "execution_failed"]
    );
    assert_eq!(
        resolution_categories(&event(
            "execution.failed",
            r#"{"execution_id":"execution-1"}"#
        )),
        vec!["progress_warning"]
    );
    assert_eq!(
        resolution_categories(&event(
            "execution.progressed",
            r#"{"execution_id":"execution-1"}"#
        )),
        vec!["progress_warning"]
    );
}

#[test]
fn progress_warning_incident_is_deduped_by_execution_episode() {
    let first = event(
        "execution.progress_warning",
        r#"{"execution_id":"execution-1","episode_id":"episode-1"}"#,
    );
    let mut replay = first.clone();
    replay.id = "event-2".to_owned();
    replay.sequence = 2;
    assert_eq!(
        attention_incident_key("progress_warning", &first, "project", "project-1"),
        attention_incident_key("progress_warning", &replay, "project", "project-1")
    );

    let mut next_execution = replay;
    next_execution.id = "event-3".to_owned();
    next_execution.sequence = 3;
    next_execution.payload_json =
        r#"{"execution_id":"execution-2","episode_id":"episode-2"}"#.to_owned();
    assert_ne!(
        attention_incident_key("progress_warning", &first, "project", "project-1"),
        attention_incident_key("progress_warning", &next_execution, "project", "project-1")
    );
}

#[test]
fn progress_warning_projection_maps_to_typed_attention_category() {
    let item = AttentionProjection {
        id: "attention-1".to_owned(),
        attention_type: "progress_warning".to_owned(),
        scope_type: "project".to_owned(),
        scope_id: "project-1".to_owned(),
        identity_id: None,
        source_event_id: "event-1".to_owned(),
        priority: 65,
        status: "open".to_owned(),
        summary: "Execution is waiting for semantic progress".to_owned(),
        details_json: "{}".to_owned(),
        dedupe_key: "attention:progress_warning:project:project-1:execution:execution-1".to_owned(),
        occurred_at: "2026-01-01T00:00:00Z".to_owned(),
        updated_at: "2026-01-01T00:00:00Z".to_owned(),
        version: 1,
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: None,
        updated_by_user_id: None,
        recommended_action: "inspect_run".to_owned(),
        source_sequence: Some(1),
    };
    assert_eq!(
        attention_item(item).unwrap().category,
        AttentionCategory::ProgressWarning
    );
}

#[test]
fn summaries_and_diagnostics_are_bounded_and_redacted() {
    let unicode = "é".repeat(200);
    assert!(bounded_text(unicode).chars().count() <= MAX_ATTENTION_SUMMARY_LEN);
    let error = ServiceError::Domain("authorization token=secret-value".to_owned());
    let message = bounded_error_message(&error);
    assert!(!message.contains("secret-value"));
    assert!(message.len() <= MAX_ATTENTION_SUMMARY_LEN);
}
