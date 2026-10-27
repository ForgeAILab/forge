use super::ConditionRefusal;
use serde_json::json;

fn refusal(project_wait: Option<&str>, message: &str) -> ConditionRefusal {
    let mut value = json!({
        "blocker_digest": "sha256:unchanged",
        "capability": "project_capacity",
        "recorded_at": "2026-10-25T00:00:00Z",
        "safe_message": message,
        "task_version": 1,
    });
    if let Some(wait) = project_wait {
        value["project_wait"] = json!(wait);
    }
    serde_json::from_value(value).unwrap()
}

#[test]
fn refusal_serialization_keeps_main_field_order() {
    let refusal = refusal(None, "project_at_capacity: waiting for a slot");
    assert_eq!(
        serde_json::to_string(&refusal).unwrap(),
        r#"{"task_version":1,"capability":"project_capacity","blocker_digest":"sha256:unchanged","recorded_at":"2026-10-25T00:00:00Z","safe_message":"project_at_capacity: waiting for a slot"}"#
    );
    let disk = ConditionRefusal {
        capability: "machine_capacity".into(),
        capacity_scope: Some("disk".into()),
        ..refusal
    };
    assert_eq!(
        serde_json::to_string(&disk).unwrap(),
        r#"{"task_version":1,"capability":"machine_capacity","blocker_digest":"sha256:unchanged","recorded_at":"2026-10-25T00:00:00Z","safe_message":"project_at_capacity: waiting for a slot","capacity_scope":"disk"}"#
    );
}

#[test]
fn typed_project_at_capacity_overrides_owner_prefix() {
    let refusal = refusal(
        Some("at_capacity"),
        "project_waiting_on_owner: waiting for a slot",
    );
    assert_eq!(
        refusal.capacity_reason(),
        ("project_at_capacity", "waiting for a slot")
    );
}

#[test]
fn typed_project_waiting_on_owner_overrides_capacity_prefix() {
    let refusal = refusal(
        Some("waiting_on_owner"),
        "project_at_capacity: waiting on the owner",
    );
    assert_eq!(
        refusal.capacity_reason(),
        ("project_waiting_on_owner", "waiting on the owner")
    );
}

#[test]
fn typed_project_wait_is_independent_of_message_wording() {
    for (wait, reason) in [
        ("at_capacity", "project_at_capacity"),
        ("waiting_on_owner", "project_waiting_on_owner"),
    ] {
        for message in ["New wording", "another_prefix: New wording"] {
            let refusal = refusal(Some(wait), message);
            assert_eq!(refusal.capacity_reason(), (reason, "New wording"));
        }
    }
}

#[test]
fn pre_upgrade_project_wait_uses_the_original_prefix_rule() {
    for (message, reason, display) in [
        (
            "project_at_capacity: waiting for a slot",
            "project_at_capacity",
            "waiting for a slot",
        ),
        (
            "project_waiting_on_owner: 10 parked tasks waiting on the owner",
            "project_waiting_on_owner",
            "10 parked tasks waiting on the owner",
        ),
        (
            "project_waiting_on_owner",
            "project_at_capacity",
            "project_waiting_on_owner",
        ),
        (
            "project_waiting_on_owner:missing space",
            "project_at_capacity",
            "project_waiting_on_owner:missing space",
        ),
        ("other: waiting", "project_at_capacity", "waiting"),
    ] {
        let refusal = refusal(None, message);
        assert_eq!(refusal.capacity_reason(), (reason, display));
        assert!(serde_json::to_value(refusal)
            .unwrap()
            .get("project_wait")
            .is_none());
    }
}

#[tokio::test]
async fn literal_pre_upgrade_rows_keep_public_projection_and_attention_digest() {
    use crate::{CreateDomainEvent, TaskRepo};

    for row in pre_upgrade_rows() {
        let db = load_pre_upgrade_row(&row).await;
        let id = row["id"].as_str().unwrap();
        let condition_json = row["condition_json"].as_str().unwrap();
        let metadata_json = row["metadata_json"].as_str().unwrap();
        let task = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        let refusal = task.condition.read().refusal.unwrap();
        assert_eq!(refusal.project_wait, None);
        if refusal.capability == "coder" {
            assert_eq!(refusal.safe_message, "no coder agent");
        } else {
            assert_eq!(
                refusal.capacity_reason(),
                (
                    row["reason"].as_str().unwrap(),
                    row["message"].as_str().unwrap()
                )
            );
        }
        assert_eq!(
            serde_json::to_value(task.condition.public()).unwrap(),
            row["public_condition"]
        );
        assert_eq!(task.condition.read().operator_reason, None);
        let event = CreateDomainEvent::task_interruption_changed(&task);
        assert_eq!(event.dedupe_key, Some(format!("task-interruption-update:{id}:1:52a0d1ac4568bf4775c96f9c143cda7e9f03d01566c0839e8c74c3327a89f401")));
        assert_eq!(
            serde_json::to_value(event.dedupe_key).unwrap(),
            row["attention_dedupe_key"]
        );
        let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(id)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(
            raw, condition_json,
            "reading the upgraded row never rewrites it"
        );
        assert_eq!(task.metadata_json.as_deref(), Some(metadata_json));
    }
}

fn pre_upgrade_rows() -> Vec<serde_json::Value> {
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/pre_upgrade_project_waits.json")).unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(
        rows.iter()
            .map(|row| row["reason"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "project_at_capacity",
            "project_waiting_on_owner",
            "machine_capacity",
            "disk_pressure",
            "dispatch_parked"
        ]
    );
    rows
}

async fn load_pre_upgrade_row(row: &serde_json::Value) -> crate::SqliteDb {
    use crate::task_condition::tests::{db, task};

    let db = db().await;
    let id = row["id"].as_str().unwrap();
    task(&db, id).await;
    // Recreate main's creation fact: the initial entry witness uses this time.
    // Keep the fixture's stored JSON bytes untouched, including refusal order.
    sqlx::query(
        "UPDATE task SET condition_json=?, metadata_json=?, version=?, created_at=? WHERE id=?",
    )
    .bind(row["condition_json"].as_str().unwrap())
    .bind(row["metadata_json"].as_str().unwrap())
    .bind(row["version"].as_i64().unwrap())
    .bind(row["created_at"].as_str().unwrap())
    .bind(id)
    .execute(db.pool())
    .await
    .unwrap();
    db
}

#[tokio::test]
async fn literal_pre_upgrade_rows_get_one_digest_repair_and_keep_their_cache_body() {
    let mut observations = Vec::new();
    for row in pre_upgrade_rows() {
        let db = load_pre_upgrade_row(&row).await;
        let id = row["id"].as_str().unwrap().to_owned();
        let before: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id='p'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let repaired = db
            .check_task_conditions_of(std::slice::from_ref(&id))
            .await
            .unwrap();
        let after: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id='p'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        let (condition, metadata): (String, String) =
            sqlx::query_as("SELECT condition_json, metadata_json FROM task WHERE id=?")
                .bind(&id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        observations.push((
            repaired,
            after - before,
            crate::task_condition::test_support::without_channel_digest(&condition)
                == row["condition_json"].as_str().unwrap(),
            metadata == row["metadata_json"].as_str().unwrap(),
        ));
        let current = crate::TaskRepo::get_by_id(&db, &id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(current
            .condition
            .channels_match(&crate::ConditionInput::from(&current)));
        assert_eq!(db.check_task_conditions_of(&[id]).await.unwrap(), 0);
        let unchanged: i64 = sqlx::query_scalar("SELECT list_revision FROM project WHERE id='p'")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(unchanged, after, "the second check writes nothing");
    }
    assert_eq!(observations, vec![(1, 1, true, true); 5],
        "main-written Project, machine, disk and role rows gain only a digest, with one cache invalidation");
}
