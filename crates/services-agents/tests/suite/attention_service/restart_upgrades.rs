//! Attention service restart upgrades tests.

use super::*;
use crate::test_services as services;

/// A real stage-three-shaped condition/event, an already open incident and a
/// configured responder cross the migration without a new incident or wake.
#[tokio::test]
async fn condition_reader_upgrade_keeps_open_incident_and_wake_identity() {
    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "Condition cutover").await;
    let task = project_task(&db, &project_id, "Unchanged blocker").await;
    let reason = "owner must repair the workspace";
    let interrupted =
        serde_json::json!({"kind":"workspace_error","reason":reason,"created_at":now_rfc3339()})
            .to_string();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET blocked_json=? WHERE id=?")
        .bind(&interrupted)
        .bind(&task.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, &task.id).await.unwrap();
    // The typed presentation did not exist on the stage-three base.
    let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let mut old: serde_json::Value = serde_json::from_str(&raw).unwrap();
    old["evidence"]
        .as_object_mut()
        .unwrap()
        .remove("presentation");
    sqlx::query("UPDATE task SET condition_json=? WHERE id=?")
        .bind(old.to_string())
        .bind(&task.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    append_attention_event(&db,"task.interruption_changed",&task.id,&project_id,serde_json::json!({"task_id":task.id,"task_version":task.version,"task_status":task.status,"requires_intervention":true,"interruption":{"source":"blocked","kind":"workspace_error","reason":reason,"execution_id":"historical-reporter"}})).await;
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    let incident_id: String = sqlx::query_scalar("SELECT id FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?").bind(&project_id).fetch_one(db.pool()).await.unwrap();
    // The incident row as `777b5f1f` stored it. That server copied the
    // event's `interruption` into the incident as written, reporting
    // execution included; this build strips delivery metadata on the way in.
    // That one field is the only part of the stored incident the two builds
    // write differently for this event, so put it back as the old server
    // left it and take the digest from that row.
    sqlx::query("UPDATE attention_projection SET details_json=json_set(details_json,'$.interruption',json(?)) WHERE id=?")
        .bind(serde_json::json!({"source":"blocked","kind":"workspace_error","reason":reason,"execution_id":"historical-reporter"}).to_string())
        .bind(&incident_id)
        .execute(db.pool())
        .await
        .unwrap();
    let stored_details: String =
        sqlx::query_scalar("SELECT details_json FROM attention_projection WHERE id=?")
            .bind(&incident_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert!(stored_details.contains("historical-reporter"));
    let before = AttentionRepo::get_attention(&*db, &incident_id)
        .await
        .unwrap()
        .unwrap();
    let digest = db::canonical_attention_incident_digest(&before);
    let wakes_before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        wakes_before, 1,
        "the responder is configured and the old blocker admitted a wake"
    );
    // Reinstall just the predecessor's projection trigger/index, then replay
    // the new migration through the production runner. Task and event data stay.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("DELETE FROM _migration WHERE version=202610070507")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DROP INDEX idx_task_condition_retry_project")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    run_migrations(db.pool()).await.unwrap();
    let condition = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap()
        .condition;
    assert!(db::material_blocker(&condition).requires_intervention);
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    DomainEventRepo::append_event(&*db, CreateDomainEvent::task_interruption_changed(&current))
        .await
        .unwrap();
    service.project_once(100).await.unwrap();
    let after = AttentionRepo::get_attention(&*db, &incident_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(digest, db::canonical_attention_incident_digest(&after));
    assert_eq!(before.id, after.id);
    let incidents: i64 = sqlx::query_scalar("SELECT count(*) FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?").bind(&project_id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(incidents, 1);
    let wakes_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        wakes_after, wakes_before,
        "migration must not re-arm a consumed incident"
    );
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    TaskRepo::update(&*db,db::UpdateTask {id:task.id.clone(),expected_version:current.version,title:None,description:None,priority:None,merge_config:None,plan:None,error_annotation:Some(None),blocked_json:Some(Some(serde_json::json!({"kind":"workspace_error","reason":"a different workspace blocker","created_at":now_rfc3339()}).to_string())),failed_json:Some(None),task_state_config:None,parent_task_id:None,updated_at:now_rfc3339()}).await.unwrap();
    service.project_once(100).await.unwrap();
    let changed = AttentionRepo::get_attention(&*db, &incident_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(digest, db::canonical_attention_incident_digest(&changed));
    let material_events: i64 = sqlx::query_scalar("SELECT count(*) FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id=?").bind(&task.id).fetch_one(db.pool()).await.unwrap();
    assert_eq!(
        material_events, 3,
        "one historical event, one unchanged typed replay, exactly one real blocker change"
    );
}

/// Plan 3.1 stage 5a, Attention continuity. The starting state is what the
/// deployed build (`9d9b228f`) leaves in the database, not what this build
/// would write:
///
/// - the `task.interruption_changed` event in that build's payload shape
///   (`crates/db/src/repository.rs:976-982` there: no `condition`, no
///   `material_blocker`);
/// - the open incident row with the `details_json` that build's projector
///   stored (`crates/services/src/attention_service.rs:2562-2584` there),
///   hard-coded below member by member;
/// - the wake rows and their stored incident digest. `db::sqlite::agent_wake`
///   is unchanged since that commit, and the digest stored there is checked
///   against that build's digest algorithm (`crates/db/src/models.rs:926-1003`
///   there), restated here so it does not share code with this build.
///
/// This build then starts and runs its first projection and wake sweep.
#[tokio::test]
async fn incident_stored_by_the_deployed_build_survives_this_builds_first_pass() {
    use serde_json::{json, Value};
    use sha2::{Digest, Sha256};

    fn deployed_strip(value: &mut Value) {
        match value {
            Value::Object(object) => {
                for key in [
                    "source_event_id",
                    "source_event_type",
                    "source_sequence",
                    "task_version",
                    "created_at",
                    "updated_at",
                    "detected_at",
                    "last_checked_at",
                    "next_check_at",
                    "execution_id",
                ] {
                    object.remove(key);
                }
                object.values_mut().for_each(deployed_strip);
            }
            Value::Array(array) => array.iter_mut().for_each(deployed_strip),
            _ => {}
        }
    }
    fn sha(value: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(value.as_bytes());
        hasher.update([0]);
        format!("sha256:{}", hex::encode(hasher.finalize()))
    }
    /// `canonical_attention_incident_digest` as `9d9b228f` computes it.
    async fn deployed_digest(db: &SqliteDb, incident_id: &str) -> String {
        let (kind, scope_type, scope_id, status, details, action): (String, String, String, String, String, String) =
            sqlx::query_as("SELECT attention_type,scope_type,scope_id,status,details_json,recommended_action FROM attention_projection WHERE id=?")
                .bind(incident_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let bounded: String = details.chars().take(8_192).collect();
        let canonical = serde_json::from_str::<Value>(&bounded)
            .map(|mut value| {
                deployed_strip(&mut value);
                if let Some(task) = value.get_mut("task").and_then(|v| v.as_object_mut()) {
                    task.remove("task_title");
                }
                if let Some(recovery) = value.get_mut("recovery").and_then(|v| v.as_object_mut()) {
                    recovery.remove("actions");
                }
                value.to_string()
            })
            .unwrap_or(bounded);
        let take = |value: &str| value.chars().take(256).collect::<String>();
        sha(&format!(
            "type={};scope_type={};scope_id={};status={};details_digest={};recommended_action={}",
            take(&kind),
            take(&scope_type),
            take(&scope_id),
            take(&status),
            sha(&canonical),
            take(&action),
        ))
    }
    async fn count(db: &SqliteDb, sql: &str, bind: &str) -> i64 {
        sqlx::query_scalar(sql)
            .bind(bind)
            .fetch_one(db.pool())
            .await
            .unwrap()
    }
    const INCIDENTS: &str = "SELECT count(*) FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?";
    const WAKES: &str =
        "SELECT count(*) FROM domain_event WHERE event_type='agent.wake.admitted' AND scope_id=?";
    const BLOCKERS: &str = "SELECT count(*) FROM agent_wake_blocker WHERE attention_id=?";

    let db = database().await;
    let identity_id = new_uuid_v4();
    identity(&db, &identity_id).await;
    let project_id = configured_project(&db, &identity_id, "Deployed incident").await;
    let task = project_task(&db, &project_id, "Unchanged blocker").await;
    let reason = "owner must repair the workspace";

    // The Task as the deployed build blocked it. Its condition is what the
    // upgrade migration restates from those fields.
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET blocked_json=? WHERE id=?")
        .bind(
            json!({"kind":"workspace_error","reason":reason,"created_at":now_rfc3339()})
                .to_string(),
        )
        .bind(&task.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    db.sync_condition_in_tx(&mut tx, &task.id).await.unwrap();
    tx.commit().await.unwrap();

    // The event as the deployed build wrote it.
    let deployed_interruption = json!({"source":"blocked","kind":"workspace_error","reason":reason,"execution_id":"historical-reporter"});
    append_attention_event(
        &db,
        "task.interruption_changed",
        &task.id,
        &project_id,
        json!({
            "task_id": task.id,
            "task_version": task.version,
            "task_status": task.status,
            "requires_intervention": true,
            "interruption": deployed_interruption,
        }),
    )
    .await;
    let (event_id, event_sequence): (String, i64) = sqlx::query_as(
        "SELECT id,sequence FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id=?",
    )
    .bind(&task.id)
    .fetch_one(db.pool())
    .await
    .unwrap();

    // The wake rows, written by code that is unchanged since the deployed
    // build. The incident row this pass also writes is replaced below.
    AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    )
    .project_once(100)
    .await
    .unwrap();
    let incident_id: String = sqlx::query_scalar(
        "SELECT id FROM attention_projection WHERE attention_type='execution_failed' AND scope_id=?",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    let written: Value = serde_json::from_str(
        &sqlx::query_scalar::<_, String>(
            "SELECT details_json FROM attention_projection WHERE id=?",
        )
        .bind(&incident_id)
        .fetch_one(db.pool())
        .await
        .unwrap(),
    )
    .unwrap();

    // The incident row the deployed build stored for that event. The offers
    // are illustrative, not captured from a deployed database: both builds
    // remove `recovery.actions` before taking the digest.
    let deployed_actions = json!([{
        "action": {"verb": "retry", "reason": null, "fresh_session": null, "refresh_workspace": null, "reset_budget": null, "guidance": null},
        "accepts": ["guidance"],
        "allowed_actors": ["owner", "project_agent"],
        "reason": "blocked",
        "label": "Retry Task",
    }]);
    let deployed_details = json!({
        "source_event_id": event_id,
        "source_event_type": "task.interruption_changed",
        "source_sequence": event_sequence,
        "entity_type": "task",
        "entity_id": task.id,
        "scope_type": written["scope_type"],
        "scope_id": written["scope_id"],
        "task": {
            "task_title": "Unchanged blocker",
            "task_status": task.status,
            "task_version": task.version,
        },
        "failure_class": null,
        "retry_decision": null,
        "retry_action": null,
        "decision": null,
        "role": null,
        "stop_reason": null,
        "error": null,
        "interruption": deployed_interruption,
        "recovery": {
            "requires_intervention": true,
            "actions": deployed_actions,
            "automatic_retry": false,
        },
    });
    // Guard on the hard-coded row: this build writes the same document
    // except for the two members the builds are known to write differently
    // (the interruption it strips, and the offers it computes).
    let mut comparable = written.clone();
    comparable["interruption"] = deployed_interruption.clone();
    comparable["recovery"]["actions"] = deployed_actions.clone();
    comparable["task"]["task_version"] = json!(task.version);
    assert_eq!(
        comparable, deployed_details,
        "the hard-coded deployed incident row"
    );
    let deployed_details = deployed_details.to_string();
    sqlx::query("UPDATE attention_projection SET details_json=? WHERE id=?")
        .bind(&deployed_details)
        .bind(&incident_id)
        .execute(db.pool())
        .await
        .unwrap();

    // The digest the wake rows store is the deployed build's digest of that row.
    let digest = deployed_digest(&db, &incident_id).await;
    let latest: Option<String> = sqlx::query_scalar(
        "SELECT incident_digest FROM agent_wake_attention_latest WHERE attention_id=?",
    )
    .bind(&incident_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        latest.as_deref(),
        Some(digest.as_str()),
        "stored wake digest"
    );
    let stored_blockers: Vec<String> =
        sqlx::query_scalar("SELECT incident_digest FROM agent_wake_blocker WHERE attention_id=?")
            .bind(&incident_id)
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(
        stored_blockers,
        vec![digest.clone()],
        "stored blocker digest"
    );
    let wakes = count(&db, WAKES, &project_id).await;
    assert_eq!(
        wakes, 1,
        "the deployed build admitted one wake for this blocker"
    );

    // This build starts: its first projection pass and wake sweep.
    let service = AttentionService::new(
        Arc::clone(&db),
        Arc::new(services::WakeTurnConsumer::new(Arc::clone(&db))),
    );
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(
        count(&db, INCIDENTS, &project_id).await,
        1,
        "no new incident"
    );
    assert_eq!(count(&db, WAKES, &project_id).await, wakes, "no wake");
    assert_eq!(count(&db, BLOCKERS, &incident_id).await, 1);
    assert_eq!(
        deployed_digest(&db, &incident_id).await,
        digest,
        "same digest"
    );
    let stored: String =
        sqlx::query_scalar("SELECT details_json FROM attention_projection WHERE id=?")
            .bind(&incident_id)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        stored, deployed_details,
        "the first pass leaves the stored incident alone"
    );

    // The first event this build writes about the same, unchanged blocker.
    let current = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    DomainEventRepo::append_event(&*db, CreateDomainEvent::task_interruption_changed(&current))
        .await
        .unwrap();
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(
        count(&db, INCIDENTS, &project_id).await,
        1,
        "no new incident"
    );
    assert_eq!(count(&db, WAKES, &project_id).await, wakes, "no wake");
    assert_eq!(count(&db, BLOCKERS, &incident_id).await, 1);
    assert_eq!(
        deployed_digest(&db, &incident_id).await,
        digest,
        "same digest"
    );

    // A real blocker change, through the real writer: one new event, the one
    // incident restated with a new digest, and never more than one wake.
    // (In this fixture the first wake's turn is still open and no second
    // wake is admitted, also when swept past the cooldown.)
    TaskRepo::update(
        &*db,
        db::UpdateTask {
            id: task.id.clone(),
            expected_version: current.version,
            title: None,
            description: None,
            priority: None,
            merge_config: None,
            plan: None,
            error_annotation: Some(None),
            blocked_json: Some(Some(
                json!({"kind":"workspace_error","reason":"a different workspace blocker","created_at":now_rfc3339()})
                    .to_string(),
            )),
            failed_json: Some(None),
            task_state_config: None,
            parent_task_id: None,
            updated_at: now_rfc3339(),
        },
    )
    .await
    .unwrap();
    service.project_once(100).await.unwrap();
    service.sweep_once_at(&now_rfc3339()).await.unwrap();
    assert_eq!(
        count(&db, INCIDENTS, &project_id).await,
        1,
        "one incident, restated"
    );
    let changed = deployed_digest(&db, &incident_id).await;
    assert_ne!(changed, digest, "a real change is a new blocker");
    let events = count(
        &db,
        "SELECT count(*) FROM domain_event WHERE event_type='task.interruption_changed' AND entity_id=?",
        &task.id,
    )
    .await;
    assert_eq!(
        events, 3,
        "deployed event, unchanged restatement, one real change"
    );
    // The admitted wake holds a cooldown; sweep past it, twice.
    for minutes in [10, 20] {
        service
            .sweep_once_at(&(Utc::now() + Duration::minutes(minutes)).to_rfc3339())
            .await
            .unwrap();
    }
    let new_wakes = count(&db, WAKES, &project_id).await - wakes;
    let new_blockers = count(&db, BLOCKERS, &incident_id).await - 1;
    println!("after a real change: wakes +{new_wakes}, blockers +{new_blockers}");
    assert!(
        new_wakes <= 1 && new_blockers <= 1,
        "at most one wake for one change"
    );
}

#[tokio::test]
async fn restart_wake_imported_merges_are_write_free_and_a_real_change_wakes_once() {
    let db = database().await;
    let project_id = "restart-merge-project".to_owned();
    let at = "2026-01-01T00:00:00Z";
    let owner = "restart-owner";
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: owner.into(),
            email: "restart@example.test".into(),
            password_hash: "test".into(),
            display_name: None,
            is_admin: false,
            created_at: at.into(),
            updated_at: at.into(),
        },
    )
    .await
    .unwrap();
    ProjectRepo::create(
        &*db,
        CreateProject {
            id: project_id.clone(),
            owner_id: Some(owner.into()),
            name: "Imported merges".into(),
            primary_repo_id: None,
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            created_at: at.into(),
            updated_at: at.into(),
        },
    )
    .await
    .unwrap();
    let mut task_ids = Vec::new();
    for (index, title) in ["merge_held", "merge_paused"].into_iter().enumerate() {
        let task = project_task(&db, &project_id, title).await;
        let attempt = format!("imported-{title}");
        sqlx::query("INSERT INTO integration_attempt (id,task_id,task_ref,project_ref,queue_seq,current,admission_key,expected_status,expected_epoch,observed_task_version,enqueued_at,state,failure_kind,failure_message,import_source_json,created_at,updated_at) VALUES (?,?,?,?,1,0,?,'merging',0,1,?,'quarantined','owner_required','missing execution or pinned workspace','{}',?,?)")
            .bind(&attempt).bind(&task.id).bind(&task.id).bind(&project_id).bind(&attempt)
            .bind(at).bind(at).bind(at).execute(db.pool()).await.unwrap();
        let interruption = serde_json::json!({"source":"integration", "cause":"owner_required", "kind":"review_needs_owner", "owner_id":null, "reason":"missing execution or pinned workspace"});
        let reason = serde_json::json!({"kind":"deferred", "attempt_id":attempt, "cause":"owner_required", "owner_id":null, "message":"missing execution or pinned workspace"});
        let mut evidence =
            serde_json::to_value(db::task_condition::ConditionEvidence::default()).unwrap();
        evidence["material"] =
            serde_json::json!({"requires_intervention":true,"interruption":interruption});
        evidence["witnesses"] = serde_json::json!([{"kind":"integration", "reason":reason}]);
        let condition = serde_json::json!({"kind":"parked", "primary":{"kind":"integration", "reason":reason}, "additional":[], "resume":{"kind":"integration", "attempt_id":attempt}, "since":null, "evidence":evidence});
        db::task_condition::decode(&condition.to_string()).unwrap();
        sqlx::query("UPDATE task SET status='merging', condition_json=? WHERE id=?")
            .bind(condition.to_string())
            .bind(&task.id)
            .execute(db.pool())
            .await
            .unwrap();
        append_attention_event_at(
            &db,
            "task.interruption_changed",
            &task.id,
            &project_id,
            serde_json::json!({"requires_intervention":true, "interruption":interruption}),
            &format!("2026-01-01T00:00:0{index}Z"),
        )
        .await;
        task_ids.push(task.id);
    }
    let new_service = || {
        Arc::new(AttentionService::new(
            db.clone(),
            Arc::new(services::WakeTurnConsumer::new(db.clone())),
        ))
    };
    restart_wake_cycle(&db, new_service()).await;
    let decisions: Vec<(String, String)> = sqlx::query_as("SELECT attention_id,incident_digest FROM agent_wake_disposition ORDER BY source_event_sequence")
        .fetch_all(db.pool()).await.unwrap();
    assert_eq!(decisions.len(), 2);
    for (id, digest) in &decisions {
        assert_eq!(
            *digest,
            services::wake_attention_incident_digest(&db.get_attention(id).await.unwrap().unwrap())
        );
    }
    let cursors: Vec<i64> = sqlx::query_scalar("SELECT last_sequence FROM event_consumer_cursor")
        .fetch_all(db.pool())
        .await
        .unwrap();
    let head: i64 = sqlx::query_scalar("SELECT MAX(sequence) FROM domain_event")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(cursors, vec![head; 7]);
    let before = restart_wake_snapshot(&db).await;
    restart_wake_observe_writes(&db).await;
    // Reconstruct twice: neither a second nor a third start may write anything.
    for _ in 0..2 {
        restart_wake_cycle(&db, new_service()).await;
        let events: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            events, 2,
            "an unchanged startup must not add a batch-shaped setup decision"
        );
        assert_eq!(
            before,
            restart_wake_snapshot(&db).await,
            "all columns, including timestamps and cursor versions"
        );
        let writes: Vec<(String, String)> = sqlx::query_as("SELECT * FROM restart_wake_writes")
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert!(
            writes.is_empty(),
            "even same-value writes are forbidden: {writes:?}"
        );
    }
    // Real setup changes after restart admit the outstanding blocker batch once.
    let responder = "restart-responder";
    identity(&db, responder).await;
    sqlx::query("UPDATE agent_identity SET owner_id=? WHERE id=?")
        .bind(owner)
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    let version: i64 = sqlx::query_scalar("SELECT version FROM project_agent_binding WHERE project_id=? AND state='agent_setup_required'")
        .bind(&project_id).fetch_one(db.pool()).await.unwrap();
    services::AgentChatService::new(db.clone())
        .set_project_binding(services::SetProjectAgentBindingInput {
            actor_user_id: owner.into(),
            project_id: project_id.clone(),
            identity_id: Some(responder.into()),
            state: "active".into(),
            autonomy_policy_json: "{}".into(),
            permission_ceiling_json: "{}".into(),
            subscriptions_json: "[]".into(),
            wake_budget: 10,
            expected_version: Some(version),
            replacement_reason: None,
        })
        .await
        .unwrap();
    restart_wake_cycle(&db, new_service()).await;
    restart_wake_cycle(&db, new_service()).await;
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let turns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        (admitted, turns),
        (1, 1),
        "a real setup change wakes the two blockers exactly once"
    );
    // A completed turn consumes the old digest. Resolve the other batch member
    // so a material change to this Task must be the source of the next wake.
    sqlx::query("UPDATE agent_chat_turn_job SET status='succeeded',version=version+1")
        .execute(db.pool())
        .await
        .unwrap();
    let other: String = sqlx::query_scalar(
        "SELECT id FROM attention_projection WHERE json_extract(details_json,'$.entity_id')=?",
    )
    .bind(&task_ids[1])
    .fetch_one(db.pool())
    .await
    .unwrap();
    let other = db.get_attention(&other).await.unwrap().unwrap();
    db.update_attention_lifecycle(UpdateAttentionLifecycle {
        id: other.id,
        expected_version: other.version,
        status: "resolved".into(),
        acknowledged_at: None,
        snoozed_until: None,
        resolved_at: Some(Some(now_rfc3339())),
        updated_by_user_id: None,
        updated_at: now_rfc3339(),
    })
    .await
    .unwrap();
    let task = TaskRepo::get_by_id(&*db, &task_ids[0], false)
        .await
        .unwrap()
        .unwrap();
    let mut condition = serde_json::to_value(&task.condition).unwrap();
    let message = "the imported merge now requires a different owner repair";
    condition["primary"]["reason"]["message"] = serde_json::json!(message);
    condition["evidence"]["witnesses"][0]["reason"]["message"] = serde_json::json!(message);
    condition["evidence"]["material"]["interruption"]["reason"] = serde_json::json!(message);
    db::task_condition::decode(&condition.to_string()).unwrap();
    sqlx::query("UPDATE task SET condition_json=?,version=version+1 WHERE id=?")
        .bind(condition.to_string())
        .bind(&task.id)
        .execute(db.pool())
        .await
        .unwrap();
    let changed = TaskRepo::get_by_id(&*db, &task.id, false)
        .await
        .unwrap()
        .unwrap();
    db.append_event(CreateDomainEvent::task_interruption_changed(&changed))
        .await
        .unwrap();
    let service = new_service();
    // Project first, then advance admission time beyond the existing cooldown.
    services::worker_runtime::WorkerRuntime::new(db.clone(), service.clone())
        .run_once(100)
        .await
        .unwrap();
    assert_eq!(
        service.sweep_once_at("2030-01-01T00:00:00Z").await.unwrap(),
        1
    );
    restart_wake_cycle(&db, new_service()).await;
    restart_wake_cycle(&db, new_service()).await;
    let admitted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let turns: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agent_chat_turn_job")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        (admitted, turns),
        (2, 2),
        "a material blocker change after restart adds exactly one wake and turn"
    );
}

#[tokio::test]
async fn restart_wake_latest_upsert_changes_only_material_values() {
    let db = database().await;
    append_attention_event(
        &db,
        "validation.failed",
        "latest-task",
        "latest-project",
        serde_json::json!({}),
    )
    .await;
    let service = AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    );
    service.project_once(100).await.unwrap();
    let id: String = sqlx::query_scalar("SELECT id FROM attention_projection")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let mut tx = db::begin_immediate(db.pool()).await.unwrap();
    // Equal NULL identities must be compared as equal. Timestamps are not material.
    for (index, (digest, disposition, reason, identity, expected)) in [
        ("digest", "setup_required", "missing", None, 1),
        ("digest", "setup_required", "missing", None, 0),
        ("changed", "setup_required", "missing", None, 1),
        ("changed", "deferred", "missing", None, 1),
        ("changed", "deferred", "cooldown", None, 1),
        ("changed", "deferred", "cooldown", Some("identity"), 1),
        ("changed", "deferred", "cooldown", Some("identity"), 0),
        ("changed", "deferred", "cooldown", None, 1),
    ]
    .into_iter()
    .enumerate()
    {
        let timestamp = format!("2030-01-01T00:00:0{index}Z");
        let before: String = sqlx::query_scalar(
            "SELECT updated_at FROM agent_wake_attention_latest WHERE attention_id=?",
        )
        .bind(&id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(
            db.record_latest_attention_wake_in_tx(
                &mut tx,
                db::RecordLatestAttentionWakeInTxInput {
                    attention_id: &id,
                    existing_attention_id: &id,
                    incident_digest: digest,
                    disposition,
                    reason,
                    identity_id: identity,
                    updated_at: &timestamp,
                }
            )
            .await
            .unwrap(),
            expected
        );
        let after: String = sqlx::query_scalar(
            "SELECT updated_at FROM agent_wake_attention_latest WHERE attention_id=?",
        )
        .bind(&id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        assert_eq!(after, if expected == 0 { before } else { timestamp });
    }
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn restart_wake_checkpoint_failure_rolls_back_decision_and_disposition() {
    use services::worker_runtime::WorkerRuntime;
    let db = database().await;
    let service = Arc::new(AttentionService::new(
        db.clone(),
        Arc::new(services::WakeTurnConsumer::new(db.clone())),
    ));
    let runtime = WorkerRuntime::new(db.clone(), service);
    runtime.run_once(100).await.unwrap(); // initialize the checkpoint before fault injection
    append_attention_event(
        &db,
        "validation.failed",
        "atomic-task",
        "atomic-project",
        serde_json::json!({}),
    )
    .await;
    let before = restart_wake_snapshot(&db).await;
    sqlx::raw_sql("CREATE TRIGGER fail_wake_checkpoint BEFORE UPDATE ON event_consumer_cursor WHEN NEW.consumer_name='attention_projection' BEGIN SELECT RAISE(ABORT, 'checkpoint fault'); END;")
        .execute(db.pool()).await.unwrap();
    assert!(runtime.run_once(100).await.is_err());
    assert_eq!(
        before,
        restart_wake_snapshot(&db).await,
        "the audit event, both disposition rows and latest decision roll back with the cursor"
    );
    let incidents: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM attention_projection")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(incidents, 0);
    sqlx::query("DROP TRIGGER fail_wake_checkpoint")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(runtime.run_once(100).await.unwrap(), 1);
    let counts: (i64, i64, i64, i64) = sqlx::query_as("SELECT (SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'), (SELECT COUNT(*) FROM agent_wake_disposition), (SELECT COUNT(*) FROM agent_wake_disposition_current), (SELECT COUNT(*) FROM agent_wake_attention_latest)")
        .fetch_one(db.pool()).await.unwrap();
    assert_eq!(counts, (1, 1, 1, 1));
    let source: i64 = sqlx::query_scalar(
        "SELECT sequence FROM domain_event WHERE event_type='validation.failed'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    let cursor = db
        .get_consumer_cursor("attention_projection")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.last_sequence, source);
}

/// A responder that is bound but cannot run (its Agent is paused) is a real
/// setup change: each open blocker records one new setup decision naming the
/// bound Agent, a restart records nothing more, and unpausing wakes once.
#[tokio::test]
async fn restart_wake_bound_but_paused_responder_records_one_setup_decision_per_blocker() {
    let db = database().await;
    let (project_id, owner, _) = restart_wake_two_merge_blockers(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let setup = "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'";
    assert_eq!(restart_wake_count(&db, setup).await, 2);

    let responder = "restart-paused-responder";
    restart_wake_bind(&db, &project_id, &owner, responder).await;
    sqlx::query("UPDATE agent_identity SET paused=1 WHERE id=?")
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(
        restart_wake_count(&db, setup).await,
        4,
        "the changed responder setup records one decision per open blocker"
    );
    assert_eq!(
        restart_wake_count(&db, "SELECT COUNT(*) FROM agent_wake_disposition").await,
        4
    );
    let latest: Vec<Option<String>> =
        sqlx::query_scalar("SELECT identity_id FROM agent_wake_attention_latest ORDER BY 1")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert_eq!(latest, vec![Some(responder.to_owned()); 2]);

    let before = restart_wake_snapshot(&db).await;
    restart_wake_observe_writes(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(before, restart_wake_snapshot(&db).await);
    let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
    assert_eq!(writes, 0, "an unchanged restart writes nothing");

    sqlx::query("UPDATE agent_identity SET paused=0 WHERE id=?")
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(
        restart_wake_count(
            &db,
            "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'"
        )
        .await,
        1,
        "the runnable responder is woken exactly once for both blockers"
    );
}

/// The deployed release's startup sweep stored one extra setup decision under
/// a batch digest. After the upgrade that row is inert: the first start writes
/// nothing, and the next real change still wakes exactly once.
#[tokio::test]
async fn restart_wake_stored_batch_digest_decision_is_inert_after_upgrade() {
    use sha2::{Digest, Sha256};
    let db = database().await;
    let (project_id, owner, _) = restart_wake_two_merge_blockers(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    // Seed what the deployed sweep added: a copy of the first member's setup
    // decision keyed and digested by the batch (both member digests).
    let members: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT e.id, d.incident_digest, e.dedupe_key FROM domain_event e JOIN agent_wake_disposition d ON d.source_event_id=e.id ORDER BY e.sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(members.len(), 2);
    let mut hasher = Sha256::new();
    hasher.update(format!("{}:{}", members[0].1, members[1].1).as_bytes());
    hasher.update([0]);
    let batch = format!("sha256:{}", hex::encode(hasher.finalize()));
    let event_id = new_uuid_v4();
    sqlx::query("INSERT INTO domain_event (id,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,dedupe_key,payload_json,created_at) SELECT ?,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,?,json_set(payload_json,'$.incident_digest',?),'2026-01-02T00:00:00Z' FROM domain_event WHERE id=?")
        .bind(&event_id).bind(format!("agent-wake-decision:agent-wake-setup_required:{batch}")).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    let disposition_id = new_uuid_v4();
    sqlx::query("INSERT INTO agent_wake_disposition (id,consumer_name,source_event_id,source_event_sequence,attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,incident_digest,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at) SELECT ?,consumer_name,?,(SELECT sequence FROM domain_event WHERE id=?),attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,?,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at FROM agent_wake_disposition WHERE source_event_id=?")
        .bind(&disposition_id).bind(&event_id).bind(&event_id).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_wake_disposition_current (consumer_name,source_event_id,disposition_id,attempt_number,updated_at) SELECT consumer_name,source_event_id,id,attempt_number,updated_at FROM agent_wake_disposition WHERE id=?")
        .bind(&disposition_id).execute(db.pool()).await.unwrap();
    // The deployed consumers had already read past the stored decision.
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence=(SELECT MAX(sequence) FROM domain_event)",
    )
    .execute(db.pool())
    .await
    .unwrap();

    let before = restart_wake_snapshot(&db).await;
    restart_wake_observe_writes(&db).await;
    for _ in 0..2 {
        restart_wake_cycle(&db, restart_wake_service(&db)).await;
        assert_eq!(before, restart_wake_snapshot(&db).await);
        let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
        assert_eq!(
            writes, 0,
            "the first start after the upgrade writes nothing"
        );
    }

    restart_wake_bind(&db, &project_id, &owner, "restart-upgrade-responder").await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    assert_eq!(
        restart_wake_count(
            &db,
            "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.admitted'"
        )
        .await,
        1
    );
}

/// With a bound responder that cannot run, the deployed release stored only
/// the batch decision (projection deferred the blockers to the sweep). The
/// first start after the upgrade records each blocker's own decision once;
/// later starts write nothing.
#[tokio::test]
async fn restart_wake_unready_responder_batch_decision_is_replaced_once_after_upgrade() {
    use sha2::{Digest, Sha256};
    let db = database().await;
    // Bind a paused responder before the blockers are projected.
    let (project_id, owner, _) = restart_wake_two_merge_blockers(&db).await;
    let responder = "restart-unready-responder";
    restart_wake_bind(&db, &project_id, &owner, responder).await;
    sqlx::query("UPDATE agent_identity SET paused=1 WHERE id=?")
        .bind(responder)
        .execute(db.pool())
        .await
        .unwrap();
    // Projection defers bound blockers; the next start's sweep decides them.
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let members: Vec<(String, String)> = sqlx::query_as(
        "SELECT e.id, d.incident_digest FROM domain_event e JOIN agent_wake_disposition d ON d.source_event_id=e.id WHERE e.event_type='agent.wake.setup_required' ORDER BY e.sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let decided: Vec<(String, String)> = sqlx::query_as(
        "SELECT event_type, payload_json FROM domain_event WHERE event_type LIKE 'agent.wake.%' ORDER BY sequence",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        members.len(),
        2,
        "only the sweep decided these blockers: {decided:?}"
    );
    // Replace the per-blocker decisions with the deployed batch decision.
    let mut hasher = Sha256::new();
    hasher.update(format!("{}:{}", members[0].1, members[1].1).as_bytes());
    hasher.update([0]);
    let batch = format!("sha256:{}", hex::encode(hasher.finalize()));
    let event_id = new_uuid_v4();
    sqlx::query("INSERT INTO domain_event (id,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,dedupe_key,payload_json,created_at) SELECT ?,event_type,entity_type,entity_id,actor_type,actor_id,scope_type,scope_id,correlation_id,causation_id,causation_depth,?,json_set(payload_json,'$.incident_digest',?),created_at FROM domain_event WHERE id=?")
        .bind(&event_id).bind(format!("agent-wake-decision:agent-wake-setup_required:{batch}")).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    let disposition_id = new_uuid_v4();
    sqlx::query("INSERT INTO agent_wake_disposition (id,consumer_name,source_event_id,source_event_sequence,attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,incident_digest,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at) SELECT ?,consumer_name,?,(SELECT sequence FROM domain_event WHERE id=?),attempt_number,max_attempts,disposition,reason,turn_job_id,attention_id,retry_at,incident_key,?,binding_id,binding_version,profile_id,profile_version,provenance_json,parent_disposition_id,created_at,updated_at FROM agent_wake_disposition WHERE source_event_id=?")
        .bind(&disposition_id).bind(&event_id).bind(&event_id).bind(&batch).bind(&members[0].0)
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_wake_disposition_current (consumer_name,source_event_id,disposition_id,attempt_number,updated_at) SELECT consumer_name,source_event_id,id,attempt_number,updated_at FROM agent_wake_disposition WHERE id=?")
        .bind(&disposition_id).execute(db.pool()).await.unwrap();
    for (id, _) in &members {
        for sql in [
            "DELETE FROM agent_wake_disposition_current WHERE source_event_id=?",
            "DELETE FROM agent_wake_disposition WHERE source_event_id=?",
            "DELETE FROM domain_event WHERE id=?",
        ] {
            sqlx::query(sql).bind(id).execute(db.pool()).await.unwrap();
        }
    }
    sqlx::query(
        "UPDATE event_consumer_cursor SET last_sequence=(SELECT MAX(sequence) FROM domain_event)",
    )
    .execute(db.pool())
    .await
    .unwrap();
    restart_wake_observe_writes(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let writes: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name, operation FROM restart_wake_writes WHERE table_name <> 'event_consumer_cursor' ORDER BY 1, 2",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    let expected = [
        ("agent_wake_disposition", "INSERT"),
        ("agent_wake_disposition", "INSERT"),
        ("agent_wake_disposition_current", "INSERT"),
        ("agent_wake_disposition_current", "INSERT"),
        ("domain_event", "INSERT"),
        ("domain_event", "INSERT"),
    ]
    .map(|(t, o)| (t.to_owned(), o.to_owned()));
    assert_eq!(writes, expected, "once: one decision per blocker");
    sqlx::query("DELETE FROM restart_wake_writes")
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
    assert_eq!(writes, 0, "not forever");
}

/// A blocker whose Task went away while Forge was down is settled by the
/// startup sweep, even when it is not the first member of the Project's
/// blockers; the other blocker's decision is left untouched.
#[tokio::test]
async fn restart_wake_blocker_closed_during_downtime_is_settled_once() {
    let db = database().await;
    let (_, _, task_ids) = restart_wake_two_merge_blockers(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    sqlx::query("UPDATE task SET deleted_at=? WHERE id=?")
        .bind(now_rfc3339())
        .bind(&task_ids[1])
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_observe_writes(&db).await;
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let statuses: Vec<(String, String)> = sqlx::query_as(
        "SELECT json_extract(details_json,'$.entity_id'), status FROM attention_projection ORDER BY occurred_at",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        statuses,
        vec![
            (task_ids[0].clone(), "open".to_owned()),
            (task_ids[1].clone(), "resolved".to_owned())
        ]
    );
    let suppressed: Vec<String> = sqlx::query_scalar(
        "SELECT json_extract(payload_json,'$.reason') FROM domain_event WHERE event_type='agent.wake.suppressed'",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(suppressed, vec!["resolved_incident".to_owned()]);
    let setup = "SELECT COUNT(*) FROM domain_event WHERE event_type='agent.wake.setup_required'";
    assert_eq!(restart_wake_count(&db, setup).await, 2);
    sqlx::query("DELETE FROM restart_wake_writes")
        .execute(db.pool())
        .await
        .unwrap();
    restart_wake_cycle(&db, restart_wake_service(&db)).await;
    let writes: i64 = restart_wake_count(&db, "SELECT COUNT(*) FROM restart_wake_writes").await;
    assert_eq!(writes, 0);
}

/// The latest-decision upsert compares every stored value except the
/// timestamp. A new column must be added to that comparison too.
#[tokio::test]
async fn restart_wake_latest_guard_covers_every_column() {
    use sqlx::Row;
    let db = database().await;
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(agent_wake_attention_latest)")
        .fetch_all(db.pool())
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    assert_eq!(
        columns,
        [
            "attention_id",
            "incident_digest",
            "disposition",
            "reason",
            "identity_id",
            "updated_at"
        ],
        "update the guard in record_latest_attention_wake_in_tx for a new column"
    );
}
