//! Tests for crash replay in lead operations.

use super::*;
/// A trigger aborts the command transaction after the separately committed
/// charge. Rebuilding the service removes all in-memory state before replay.
/// Both operation orders are exercised, plus a restart after each command.
#[tokio::test]
async fn lead_create_and_recover_replay_once_across_charge_command_crashes_in_both_orders() {
    for create_first in [true, false] {
        let mut ops = ops_fixture(false).await;
        let (root, child, _) = root_with_parked_child(&ops.lead, true).await;
        until(&ops.lead, "the root plans", async || {
            running(&ops.lead, &root.id, PLANNER).await
        })
        .await;
        let original_count = children(&ops.lead, &root.id).await.len();
        let planning_execution: String = sqlx::query_scalar(
            "SELECT id FROM execution WHERE task_id=? AND role='planner' AND status='running'",
        )
        .bind(&root.id)
        .fetch_one(ops.lead.db.pool())
        .await
        .unwrap();
        let parked = reload(&ops.lead.db, &child.id).await;
        let operations = if create_first {
            ["create", "recover"]
        } else {
            ["recover", "create"]
        };
        for operation in operations {
            let (name, payload, budget, trigger) = if operation == "create" {
                ("lead.subtask", json!({"action":"create", "title":"crash-safe fix", "description":"finish it"}), db::budget::LEAD_SUBTASKS_KEY.to_owned(),
                 format!("CREATE TRIGGER fail_lead_command BEFORE INSERT ON task WHEN NEW.parent_task_id='{}' BEGIN SELECT RAISE(FAIL,'injected crash before create command'); END", root.id))
            } else {
                ("lead.recover", json!({"task_id":child.id, "version":parked.version, "action":{"verb":"retry"}}), db::budget::lead_recover_key(&child.id),
                 format!("CREATE TRIGGER fail_lead_command BEFORE UPDATE ON task WHEN OLD.id='{}' BEGIN SELECT RAISE(FAIL,'injected crash before recover command'); END", child.id))
            };
            sqlx::query(&trigger)
                .execute(ops.lead.db.pool())
                .await
                .unwrap();
            let failure = ops
                .call(&root.id, name, payload.clone(), operation)
                .await
                .unwrap_err();
            assert!(
                failure.contains("InternalFailure"),
                "the injected command failure crosses the native envelope: {failure}"
            );
            assert_eq!(
                spent(&ops.lead, &root.id, &budget).await,
                1,
                "charge committed before command"
            );
            assert_eq!(
                children(&ops.lead, &root.id).await.len(),
                if operation == "recover" && create_first {
                    original_count + 1
                } else {
                    original_count
                }
            );
            if operation == "recover" {
                assert_eq!(
                    reload(&ops.lead.db, &child.id).await.version,
                    parked.version
                );
            }
            sqlx::query("DROP TRIGGER fail_lead_command")
                .execute(ops.lead.db.pool())
                .await
                .unwrap();
            restart_operations(&mut ops);
            let first = ops
                .call(&root.id, name, payload.clone(), operation)
                .await
                .unwrap();
            restart_operations(&mut ops);
            let replay = ops.call(&root.id, name, payload, operation).await.unwrap();
            assert_eq!(first["task_id"], replay["task_id"]);
            assert_eq!(replay["replayed"], true);
            assert_eq!(
                spent(&ops.lead, &root.id, &budget).await,
                1,
                "replay is not charged twice"
            );
            if operation == "create" {
                assert_eq!(
                    children(&ops.lead, &root.id).await.len(),
                    original_count + 1,
                    "created once"
                );
                assert_eq!(
                    spent(
                        &ops.lead,
                        &root.id,
                        &db::budget::lead_subtasks_run_key(&planning_execution)
                    )
                    .await,
                    1,
                    "the per-run creation debit is also once"
                );
            } else {
                let recovered = reload(&ops.lead.db, &child.id).await;
                assert!(crate::task_hierarchy::subtask_decision_park(&recovered).is_none());
                let version = recovered.version;
                ops.call(&root.id, name, json!({"task_id":child.id, "version":parked.version, "action":{"verb":"retry"}}), operation).await.unwrap();
                assert_eq!(
                    reload(&ops.lead.db, &child.id).await.version,
                    version,
                    "command applied once"
                );
            }
        }
        assert_eq!(
            children(&ops.lead, &root.id).await.len(),
            original_count + 1
        );
        assert_eq!(
            spent(
                &ops.lead,
                &root.id,
                &db::budget::lead_recover_key(&child.id)
            )
            .await,
            1
        );
    }
}

/// The question wait starts when the tool commits, while the asking run can
/// still be live. Hold stops that run; release must retain the question and
/// its answer must launch exactly one new lead run.
#[tokio::test]
async fn holding_a_live_asking_run_then_releasing_keeps_the_question_and_runs_once_after_answer() {
    let mut ops = ops_fixture(false).await;
    let (root, child) = root_with_child(&ops.lead, true).await;
    until(&ops.lead, "the asking lead is running", async || {
        running(&ops.lead, &root.id, PLANNER).await
    })
    .await;
    let asking = tokio::time::timeout(Duration::from_secs(5), ops.lead.rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        asking.task_id, root.id,
        "hold happens after the lead actually launches"
    );
    ops.call(
        &root.id,
        "lead.request_input",
        json!({"question":"A or B?"}),
        "held-question",
    )
    .await
    .unwrap();
    let (item,version):(String,i64)=sqlx::query_as("SELECT id,version FROM attention_projection WHERE json_extract(details_json,'$.task_id')=? AND recommended_action='answer_lead_input' AND status='open'")
        .bind(&root.id).fetch_one(ops.lead.db.pool()).await.unwrap();
    let waiting = reload(&ops.lead.db, &root.id).await;
    ops.service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Hold { reason: None },
            waiting.version,
        )
        .await
        .unwrap();
    passes(&ops.lead, 4).await;
    assert_eq!(
        executions(&ops.lead.db, &root.id).await,
        [(PLANNER.to_owned(), "cancelled".to_owned())]
    );
    assert_eq!(question_items(&ops.lead, "open").await, 1);
    let held = reload(&ops.lead.db, &root.id).await;
    ops.service
        .perform_task_action(
            &root.id,
            api_types::TaskAction::Release { reason: None },
            held.version,
        )
        .await
        .unwrap();
    passes(&ops.lead, 5).await;
    assert_eq!(
        executions(&ops.lead.db, &root.id).await.len(),
        1,
        "release still waits for the answer"
    );
    assert!(park(&ops.lead.db, &root.id).await.contains("HumanWork"));
    assert!(executions(&ops.lead.db, &child.id).await.is_empty());
    crate::attention_service::AttentionService::new(
        Arc::clone(&ops.lead.db),
        crate::test_support::noop_wake_turns(),
    )
    .resolve_with_answer("owner", &item, version, Some("B"))
    .await
    .expect("the held question is still answerable");
    until(&ops.lead, "the answered lead runs again", async || {
        running(&ops.lead, &root.id, PLANNER).await
    })
    .await;
    passes(&ops.lead, 5).await;
    assert_eq!(
        executions(&ops.lead.db, &root.id).await,
        [
            (PLANNER.to_owned(), "cancelled".to_owned()),
            (PLANNER.to_owned(), "running".to_owned())
        ]
    );
    assert_eq!(spent(&ops.lead, &root.id, "replan").await, 0);
    let answered = tokio::time::timeout(Duration::from_secs(5), ops.lead.rx.recv())
        .await
        .expect("the answer launches the new planning run")
        .expect("answered lead context");
    assert_eq!(answered.task_id, root.id);
    let mut inputs = vec![asking.description, answered.description];
    while let Ok(ctx) = ops.lead.rx.try_recv() {
        if ctx.task_id == root.id {
            inputs.push(ctx.description);
        }
    }
    assert_eq!(inputs.len(), 2);
    assert!(inputs[1].contains("Answer to the lead's question: B"));
}
