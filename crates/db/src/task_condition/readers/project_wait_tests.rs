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
    use crate::task_condition::tests::{db, task};
    use crate::{CreateDomainEvent, TaskRepo};

    let db = db().await;
    let rows: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/pre_upgrade_project_waits.json")).unwrap();
    for (index, row) in rows.iter().enumerate() {
        let id = format!("pre-upgrade-{index}");
        task(&db, &id).await;
        let condition_json = row["condition_json"].as_str().unwrap();
        let metadata_json = row["metadata_json"].as_str().unwrap();
        sqlx::query("UPDATE task SET condition_json=?, metadata_json=?, version=1 WHERE id=?")
            .bind(condition_json)
            .bind(metadata_json)
            .bind(&id)
            .execute(db.pool())
            .await
            .unwrap();
        let task = TaskRepo::get_by_id(&db, &id, false).await.unwrap().unwrap();
        let refusal = task.condition.read().refusal.unwrap();
        assert_eq!(refusal.project_wait, None);
        assert_eq!(
            refusal.capacity_reason(),
            (
                row["reason"].as_str().unwrap(),
                row["message"].as_str().unwrap()
            )
        );
        assert_eq!(
            serde_json::to_value(task.condition.public()).unwrap(),
            json!({
                "kind":"parked", "primary":{"kind":"capacity","scope":"project"},
                "additional":[], "resume":{"kind":"reconcile"}, "since":null,
                "details":{"failure_kind":null,"diagnostic":null,"interruption":null,
                    "failed":false,"blocked":false,"human_wait":false,"entry_wait":false}
            })
        );
        assert_eq!(task.condition.read().operator_reason, None);
        let event = CreateDomainEvent::task_interruption_changed(&task);
        assert_eq!(event.dedupe_key, Some(format!("task-interruption-update:{id}:1:52a0d1ac4568bf4775c96f9c143cda7e9f03d01566c0839e8c74c3327a89f401")));
        let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(&id)
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
