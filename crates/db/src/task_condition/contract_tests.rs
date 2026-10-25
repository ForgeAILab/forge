use super::tests::{claim, db, task};
use super::*;
use crate::{Conditioned, ProjectRepo, TaskMutation, TaskRepo};
use serde_json::json;

const AT: &str = "2026-10-25T07:00:00Z";

fn park(owner: ConditionReasonOwner, reason: ParkReason, marker: &str) -> ConditionStatement {
    ConditionStatement::Edit {
        edit: Box::new(ConditionOwnerEdit {
            owner,
            expected: None,
            replacement: Some(ConditionReplacement {
                identity: ConditionIdentity::Marker {
                    epoch: 0,
                    marker_id: marker.into(),
                },
                condition: StatedCondition::Parked {
                    reason,
                    resume: ConditionContinuation::Reconcile,
                    since: None,
                    presentation: Default::default(),
                },
            }),
        }),
    }
}

#[tokio::test]
async fn diagnostic_dedup_cap_rollback_and_project_cascade() {
    let db = db().await;
    task(&db, "text").await;
    let owner = DiagnosticOwner::Reason {
        reason: ParkReason::Failure {
            failure_kind: api_types::FailureKind::ExecutorFailed,
        },
    };
    let original = "é".repeat(DIAGNOSTIC_TEXT_LIMIT);
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    let id = db
        .record_task_diagnostic_text_in_tx(
            &mut tx,
            "text",
            RecordTaskDiagnosticText {
                kind: "failure",
                source: "executor",
                owner: &owner,
                text: &original,
            },
        )
        .await
        .unwrap();
    let again = db
        .record_task_diagnostic_text_in_tx(
            &mut tx,
            "text",
            RecordTaskDiagnosticText {
                kind: "failure",
                source: "executor",
                owner: &owner,
                text: &original,
            },
        )
        .await
        .unwrap();
    assert_eq!(id, again);
    tx.commit().await.unwrap();
    let row = db.task_diagnostic_text("text", &id).await.unwrap().unwrap();
    assert_eq!(row.byte_length, original.len() as i64);
    assert_eq!(row.digest, diagnostics::digest(original.as_bytes()));
    assert_eq!(row.occurrence_count, 2);
    assert!(row.text.len() <= DIAGNOSTIC_TEXT_LIMIT);
    assert!(row.text.ends_with(PRESENTATION_TRUNCATION_MARKER));
    assert_eq!(row.owner, owner);
    assert!(row.last_seen >= row.created_at);
    assert!(db
        .task_diagnostic_text("other", &id)
        .await
        .unwrap()
        .is_none());
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    db.record_task_diagnostic_text_in_tx(
        &mut tx,
        "text",
        RecordTaskDiagnosticText {
            kind: "failure",
            source: "executor",
            owner: &owner,
            text: "rolled back",
        },
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        db.task_diagnostic_texts("text", None, 100)
            .await
            .unwrap()
            .items
            .len(),
        1
    );
    ProjectRepo::delete(&db, "p").await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM task_diagnostic_text")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn real_hold_retains_200kb_text() {
    let db = db().await;
    task(&db, "long").await;
    let text = "é".repeat(100_000);
    super::statement_tests::hold(&db, "long", &text).await;
    let condition = db.task_condition("long").await.unwrap();
    assert!(condition.read().interruption.unwrap().reason.len() <= PRESENTATION_TEXT_LIMIT);
    let texts = db.task_diagnostic_texts("long", None, 100).await.unwrap();
    let detail = texts
        .items
        .iter()
        .find(|text| text.source == "hold")
        .unwrap();
    assert_eq!(detail.text, text);
    assert_eq!(detail.byte_length, 200_000);
    assert_eq!(detail.source, "hold");
}

#[tokio::test]
async fn owned_clear_is_identity_fenced_and_preserves_foreign_blockers() {
    let db = db().await;
    task(&db, "owners").await;
    test_support::set_task_condition(
        &db,
        "owners",
        park(
            ConditionReasonOwner::Hold,
            ParkReason::Held {
                actor: "user".into(),
            },
            "hold",
        ),
    )
    .await
    .unwrap();
    test_support::set_task_condition(
        &db,
        "owners",
        park(
            ConditionReasonOwner::Environment,
            ParkReason::Environment {
                wait_kind: ConditionEnvironmentKind::EnvironmentNotReady,
                source: ConditionSource {
                    field: LegacyConditionField::ConditionJson,
                    key: None,
                },
            },
            "probe-2",
        ),
    )
    .await
    .unwrap();
    let clear = |marker: &str| ConditionStatement::Edit {
        edit: Box::new(ConditionOwnerEdit {
            owner: ConditionReasonOwner::Environment,
            expected: Some(ConditionIdentity::Marker {
                epoch: 0,
                marker_id: marker.into(),
            }),
            replacement: None,
        }),
    };
    let before = db.task_condition("owners").await.unwrap();
    assert!(matches!(
        test_support::set_task_condition(&db, "owners", clear("probe-1")).await,
        Err(DbError::VersionConflict)
    ));
    assert_eq!(db.task_condition("owners").await.unwrap(), before);
    let after = test_support::set_task_condition(&db, "owners", clear("probe-2"))
        .await
        .unwrap();
    assert!(after
        .reasons()
        .any(|r| matches!(r, ParkReason::Held { actor } if actor == "user")));
    assert!(!after
        .reasons()
        .any(|r| matches!(r, ParkReason::Environment { .. })));
}

#[tokio::test]
async fn deployed_sql_and_update_payloads_still_decode_and_replay() {
    // Verbatim field shapes from 1991b306: task_mutation.rs and repository.rs.
    const SQL: &str = r#"{"Sql":{"task_id":"historic","query":"UPDATE task SET blocked_json=?, updated_at=? WHERE id=?","arguments":["{\"kind\":\"executor_failed\",\"reason\":\"historic full reason\",\"created_at\":\"2026-10-01T00:00:00Z\"}","2026-10-01T00:00:00Z","historic"]}}"#;
    const UPDATE: &str = r#"{"TaskUpdate":{"input":{"id":"historic","expected_version":1,"title":"accepted before upgrade","description":{"present":false,"value":null},"priority":null,"merge_config":{"present":false,"value":null},"plan":{"present":false,"value":null},"error_annotation":{"present":false,"value":null},"blocked_json":{"present":true,"value":null},"failed_json":{"present":false,"value":null},"task_state_config":{"present":false,"value":null},"parent_task_id":{"present":false,"value":null},"updated_at":"2026-10-01T00:00:00Z"}}}"#;
    let db = db().await;
    task(&db, "historic").await;
    let step = claim(&db, "historic").await;
    crate::task_writer::in_task_step(step, async {
        let sql: TaskMutation = serde_json::from_str(SQL).unwrap();
        assert_eq!(sql.apply(&db).await.unwrap(), json!(1));
        let row = TaskRepo::get_by_id(&db, "historic", false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            row.condition.read().blocked_message.as_deref(),
            Some("historic full reason")
        );
        let update: TaskMutation = serde_json::from_str(UPDATE).unwrap();
        let result: Task = serde_json::from_value(update.apply(&db).await.unwrap()).unwrap();
        assert_eq!(result.title, "accepted before upgrade");
        assert!(result.blocked_json.is_none());
        assert!(matches!(result.condition, TaskCondition::Clear { .. }));
        // TaskArchive receipts and queued rollback commands from the same deployed build.
        const ARCHIVE: &str = r#"{"id":"historic","project_id":"p","parent_task_id":null,"assignee_type":null,"assignee_id":null,"title":"old result","description":null,"task_type":"task","status":"todo","is_automation":false,"priority":0,"board_position":0.0,"subtask_order":null,"task_state_config":null,"merge_config":null,"metadata_json":null,"plan":null,"error_annotation":null,"blocked_json":"{\"kind\":\"workflow_loop\",\"reason\":\"old rollback block\",\"created_at\":\"2026-10-01T00:00:00Z\"}","failed_json":null,"entry_barrier_json":null,"review_passed_at":null,"archived_at":null,"deleted_at":null,"version":1,"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-01T00:00:00Z"}"#;
        const RESTORE: &str = r#"{"TaskRestoreQueuedRecovery":{"input":{"task_id":"historic","expected_version":1,"queued_recovery_id":"restore-history","failed_json":null,"error_annotation":null,"blocked_json":"{\"kind\":\"workflow_loop\",\"reason\":\"old rollback block\",\"created_at\":\"2026-10-01T00:00:00Z\"}","updated_at":"2026-10-01T00:00:00Z"}}}"#;
        let archived: Task = serde_json::from_str(ARCHIVE).unwrap();
        assert_eq!(archived.condition.read().blocked_message.as_deref(), Some("old rollback block"));
        TaskRepo::mutate_metadata(&db, "historic", None, vec![crate::TaskMetadataMutation::Set {
            key: "queued_recovery".into(), value: json!({"id":"restore-history"}),
        }], AT).await.unwrap();
        let restore: TaskMutation = serde_json::from_str(RESTORE).unwrap();
        let restored: Task = serde_json::from_value(restore.apply(&db).await.unwrap()).unwrap();
        assert_eq!(restored.blocked_json, archived.blocked_json);
        assert_eq!(restored.condition.read().blocked_message.as_deref(), Some("old rollback block"));
        let envelope = Conditioned {
            input: sql,
            condition: ConditionStatement::Preserve,
        };
        assert_eq!(
            serde_json::from_value::<Conditioned<TaskMutation>>(
                serde_json::to_value(envelope).unwrap()
            )
            .unwrap()
            .condition,
            ConditionStatement::Preserve
        );
    })
    .await;
}

#[tokio::test]
async fn corrupt_idle_rebuilds_with_raw_text_and_live_step_self_heals() {
    let db = db().await;
    task(&db, "broken-idle").await;
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='broken-idle'")
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(&["broken-idle".into()])
        .await
        .unwrap();
    let parked = db.task_condition("broken-idle").await.unwrap();
    assert!(matches!(parked, TaskCondition::Clear { .. }));
    let details = db
        .task_diagnostic_texts("broken-idle", None, 100)
        .await
        .unwrap()
        .items;
    assert_eq!(details.len(), 1);
    assert_eq!(details[0].text, "{}");
    assert_eq!(
        db.check_task_conditions_of(&["broken-idle".into()])
            .await
            .unwrap(),
        0
    );
    super::statement_tests::hold(&db, "broken-idle", "new explicit intent").await;
    assert!(!recovery::unreadable(
        &db.task_condition("broken-idle").await.unwrap()
    ));
    task(&db, "broken-step").await;
    let step = super::producer_tests::enqueue(&db, "broken-step", "hooks", "todo", "{}").await;
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='broken-step'")
        .execute(db.pool())
        .await
        .unwrap();
    db.check_task_conditions_of(&["broken-step".into()])
        .await
        .unwrap();
    assert!(
        matches!(db.task_condition("broken-step").await.unwrap(), TaskCondition::Entering { step_id, .. } if step_id == step)
    );
    assert_eq!(
        db.task_diagnostic_texts("broken-step", None, 100)
            .await
            .unwrap()
            .items[0]
            .text,
        "{}"
    );
}

#[tokio::test]
async fn real_failure_writer_keeps_200kb_text_outside_condition() {
    let db = db().await;
    let t = task(&db, "long-failure").await;
    let step = claim(&db, &t.id).await;
    let text = "f".repeat(200_000);
    crate::task_writer::in_task_step(step, async {
        let input = crate::UpdateTaskStatus {
            id: t.id.clone(),
            expected_version: t.version,
            status: t.status.clone(),
            assignee_id: None,
            error_annotation: Some(None),
            blocked_json: Some(None),
            failed_json: Some(Some(
                json!({"kind":"executor_failed","reason":text,"created_at":AT,"execution_id":null})
                    .to_string(),
            )),
            updated_at: AT.into(),
        };
        let condition = ConditionStatement::Edit {
            edit: Box::new(ConditionOwnerEdit {
                owner: ConditionReasonOwner::Diagnostic,
                expected: None,
                replacement: Some(ConditionReplacement {
                    identity: ConditionIdentity::Entry {
                        epoch: 0,
                        transition_id: None,
                    },
                    condition: StatedCondition::Failed {
                        reason: ParkReason::Failure {
                            failure_kind: api_types::FailureKind::ExecutorFailed,
                        },
                        resume: ConditionContinuation::Reconcile,
                        since: Some(AT.into()),
                        presentation: ConditionPresentation {
                            failure_message: Some(text.clone()),
                            interruption: Some(ConditionInterruption {
                                kind: api_types::FailureKind::ExecutorFailed,
                                reason: text.clone(),
                                at: AT.into(),
                                source: None,
                                execution_id: None,
                            }),
                            ..Default::default()
                        },
                    },
                }),
            }),
        };
        let row: Task = db
            .apply_conditioned_task_mutation(
                &t.id,
                Conditioned {
                    input: TaskMutation::TaskUpdateStatus { input },
                    condition,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            row.condition.read().failure_message.unwrap().len(),
            PRESENTATION_TEXT_LIMIT
        );
        let texts = db.task_diagnostic_texts(&t.id, None, 100).await.unwrap();
        let detail = texts
            .items
            .iter()
            .find(|text| text.source == "diagnostic")
            .unwrap();
        assert_eq!(detail.text, text);
        assert_eq!(detail.byte_length, 200_000);
        let mut connection = db.pool().acquire().await.unwrap();
        let facts = ConditionFacts::load(&mut connection, &t.id).await.unwrap();
        let expected = facts.condition(&LegacyConditionInput::from(&row));
        assert_eq!(row.condition.typed(), expected.typed());
    })
    .await;
}

#[tokio::test]
async fn stated_sql_queue_and_inline_paths_use_intent_not_sql_classification() {
    use crate::{
        task_writer::{TaskQuery, TaskQueryResult},
        TaskStepRepo,
    };
    let db = db().await;
    for queued in [false, true] {
        let id = if queued {
            "typed-queued"
        } else {
            "typed-inline"
        };
        task(&db, id).await;
        let statement = park(
            ConditionReasonOwner::Hold,
            ParkReason::Held {
                actor: "operator".into(),
            },
            "hold-id",
        );
        if queued {
            let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
            let result = TaskQuery::stated(
                &db,
                id,
                "UPDATE task SET title=? WHERE id=?",
                ConditionChange::Legacy,
                statement,
            )
            .bind("typed title")
            .bind(id)
            .execute_in_tx(&mut tx)
            .await
            .unwrap();
            assert!(matches!(result, TaskQueryResult::Queued { .. }));
            tx.commit().await.unwrap();
            let step = db
                .claim_step("typed-sql", Some(id), &crate::task_writer::lease_deadline())
                .await
                .unwrap()
                .unwrap();
            let payload: TaskMutation = serde_json::from_str(&step.payload_json).unwrap();
            assert!(matches!(payload, TaskMutation::StatedSql { .. }));
            crate::task_writer::in_task_step(step, async {
                assert_eq!(payload.apply(&db).await.unwrap(), json!(1));
            })
            .await;
        } else {
            let step = claim(&db, id).await;
            crate::task_writer::in_task_step(step, async {
                assert_eq!(
                    TaskQuery::stated(
                        &db,
                        id,
                        "UPDATE task SET title=? WHERE id=?",
                        ConditionChange::Legacy,
                        statement
                    )
                    .bind("typed title")
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap(),
                    1
                );
            })
            .await;
        }
        let row = TaskRepo::get_by_id(&db, id, false).await.unwrap().unwrap();
        assert_eq!(row.title, "typed title");
        assert!(row
            .condition
            .reasons()
            .any(|r| matches!(r, ParkReason::Held { actor } if actor == "operator")));
        assert!(
            row.error_annotation.is_none(),
            "the typed intent was never inferred from a legacy field"
        );
    }
}

#[tokio::test]
async fn inline_cas_and_epoch_rejection_roll_back_condition_legacy_and_text() {
    let db = db().await;
    let t = task(&db, "atomic").await;
    let step = claim(&db, &t.id).await;
    crate::task_writer::in_task_step(step, async {
        for (version, epoch) in [(t.version - 1, 0), (t.version, 99)] {
            let mut statement = park(
                ConditionReasonOwner::Diagnostic,
                ParkReason::Failure {
                    failure_kind: api_types::FailureKind::ExecutorFailed,
                },
                "failure",
            );
            if let ConditionStatement::Edit { edit } = &mut statement {
                let replacement = edit.replacement.as_mut().unwrap();
                replacement.identity = ConditionIdentity::Entry {
                    epoch,
                    transition_id: None,
                };
                if let StatedCondition::Parked { presentation, .. } = &mut replacement.condition {
                    presentation.failure_message =
                        Some("full diagnostic that must roll back".into());
                }
            }
            let input = crate::UpdateTaskStatus {
                id: t.id.clone(),
                expected_version: version,
                status: t.status.clone(),
                assignee_id: None,
                error_annotation: Some(Some("new legacy value".into())),
                blocked_json: None,
                failed_json: None,
                updated_at: AT.into(),
            };
            let result: Result<Task> = db
                .apply_conditioned_task_mutation(
                    &t.id,
                    Conditioned {
                        input: TaskMutation::TaskUpdateStatus { input },
                        condition: statement,
                    },
                )
                .await;
            assert!(matches!(result, Err(DbError::VersionConflict)));
            let row = TaskRepo::get_by_id(&db, &t.id, false)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.version, t.version);
            assert!(row.error_annotation.is_none());
            assert_eq!(row.condition, t.condition);
            assert!(db
                .task_diagnostic_texts(&t.id, None, 100)
                .await
                .unwrap()
                .items
                .is_empty());
        }
    })
    .await;
}

#[test]
fn unknown_typed_owner_is_newer_and_saved_condition_contains_no_legacy_bodies() {
    let mut value = serde_json::to_value(TaskCondition::default()).unwrap();
    value["evidence"]["ownership"] = json!([{"owner":"future_owner","identity":{"witness":"entry","epoch":0,"transition_id":null},"reason":null}]);
    assert!(matches!(
        classify(value.to_string().as_bytes()),
        Stored::Newer
    ));
    let mut condition = TaskCondition::default();
    condition.evidence_mut().error_annotation = Some("private old annotation".into());
    condition
        .evidence_mut()
        .metadata
        .insert("queued_recovery".into(), "private old command".into());
    let saved = SavedTaskCondition::capture(&condition);
    let encoded = serde_json::to_string(&saved).unwrap();
    assert!(!encoded.contains("private old"));
    let decoded: SavedTaskCondition = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, saved);
}

#[tokio::test]
async fn corrupt_running_review_role_and_integration_owners_self_heal() {
    use crate::{ExecutionRepo, IntegrationQueueRepo, ReviewRepo, TaskRoleAssignmentRepo};
    let db = db().await;
    for (id, state) in [
        ("live-run", "in_progress"),
        ("review-owner", "review"),
        ("role-owner", "planning"),
        ("queue-owner", "merging"),
    ] {
        let t = task(&db, id).await;
        let step = claim(&db, id).await;
        crate::task_writer::in_task_step(step, async {
            let t = TaskRepo::update_status(
                &db,
                crate::UpdateTaskStatus {
                    id: id.into(),
                    expected_version: t.version,
                    status: state.into(),
                    assignee_id: None,
                    error_annotation: None,
                    blocked_json: None,
                    failed_json: None,
                    updated_at: crate::now_rfc3339(),
                },
            )
            .await
            .unwrap();
            if id == "live-run" || id == "review-owner" {
                let execution = crate::CreateExecution {
                    id: format!("execution-{id}"),
                    task_id: id.into(),
                    agent_id: None,
                    role: "coder".into(),
                    status: if id == "live-run" {
                        crate::ExecutionStatus::Running
                    } else {
                        crate::ExecutionStatus::Completed
                    },
                    stop_reason: None,
                    stopped_by: None,
                    resume_policy: None,
                    stopped_at: None,
                    parent_execution_id: None,
                    agent_session_id: None,
                    agent_message_id: None,
                    last_activity_at: None,
                    summary: None,
                    logs_path: None,
                    before_sha: None,
                    after_sha: None,
                    error: None,
                    executor_config_snapshot_json: None,
                    workspace_id: None,
                    created_at: crate::now_rfc3339(),
                    updated_at: crate::now_rfc3339(),
                };
                ExecutionRepo::create(&db, execution).await.unwrap();
            }
            if id == "review-owner" {
                ReviewRepo::create(
                    &db,
                    crate::CreateReview {
                        id: "review".into(),
                        task_id: id.into(),
                        execution_id: format!("execution-{id}"),
                        attempt_number: 1,
                        status: crate::ReviewStatus::AwaitingHuman,
                        step_results_json: "{}".into(),
                        started_at: crate::now_rfc3339(),
                        created_at: crate::now_rfc3339(),
                        updated_at: crate::now_rfc3339(),
                    },
                )
                .await
                .unwrap();
            }
            if id == "role-owner" {
                TaskRoleAssignmentRepo::assign(
                    &db,
                    crate::CreateTaskRoleAssignment {
                        id: "role".into(),
                        task_id: id.into(),
                        role_name: "planner".into(),
                        assignee_type: Some(crate::AssigneeKind::User),
                        assignee_id: Some("human".into()),
                        created_at: crate::now_rfc3339(),
                        updated_at: crate::now_rfc3339(),
                    },
                )
                .await
                .unwrap();
            }
            if id == "queue-owner" {
                crate::RepoRepo::create(
                    &db,
                    crate::CreateRepo {
                        id: "queue-repo".into(),
                        project_id: "p".into(),
                        name: "repo".into(),
                        remote_url: None,
                        local_path: None,
                        default_branch: "main".into(),
                        created_at: crate::now_rfc3339(),
                        updated_at: crate::now_rfc3339(),
                    },
                )
                .await
                .unwrap();
                let queue = db
                    .create_or_get_integration_queue("queue-repo", "main")
                    .await
                    .unwrap();
                let epoch: i64 = sqlx::query_scalar("SELECT status_epoch FROM task WHERE id=?")
                    .bind(id)
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
                db.admit_integration_attempt(crate::IntegrationAttempt::new(
                    Some(queue.id),
                    id.into(),
                    "p".into(),
                    "recovery-attempt".into(),
                    state.into(),
                    epoch,
                    t.version,
                ))
                .await
                .unwrap();
            }
        })
        .await;
        sqlx::query("UPDATE task SET condition_json='{}' WHERE id=?")
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
        state_with_policy(&db, id, None).await;
        let condition = db.task_condition(id).await.unwrap();
        assert!(!recovery::unreadable(&condition), "{id}: {condition:?}");
        match id {
            "live-run" => assert!(
                matches!(condition, TaskCondition::Running { execution_id, .. } if execution_id == "execution-live-run")
            ),
            "review-owner" | "role-owner" => assert!(condition
                .reasons()
                .any(|r| matches!(r, ParkReason::HumanDecision { .. }))),
            "queue-owner" => assert!(matches!(
                condition.integration_reason(),
                Some(IntegrationReason::Waiting { .. })
            )),
            _ => unreachable!(),
        }
        let texts = db.task_diagnostic_texts(id, None, 100).await.unwrap().items;
        assert!(texts
            .iter()
            .any(|d| d.kind == "unreadable_condition" && d.text == "{}"));
    }
}

#[tokio::test]
async fn unicode_attention_material_keeps_its_identity() {
    let db = db().await;
    task(&db, "unicode-material").await;
    let message = "😀".repeat(512);
    let material = MaterialBlocker {
        requires_intervention: true,
        interruption: Some(json!({"source":"failed","kind":"executor_failed","reason":message})),
    };
    let condition = TaskCondition::Failed {
        failure: ParkReason::Failure {
            failure_kind: api_types::FailureKind::ExecutorFailed,
        },
        additional: Vec::new(),
        resume: ConditionContinuation::Reconcile,
        since: None,
        evidence: ConditionEvidence {
            material: Some(material.clone()),
            presentation: Some(ConditionRead {
                failure_message: Some(message),
                hard_failure: true,
                ..Default::default()
            }),
            ..Default::default()
        },
    };
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    set_condition(&mut tx, "unicode-material", &encode(&condition), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let stored = db.task_condition("unicode-material").await.unwrap();
    assert_eq!(
        material_blocker(&stored),
        material,
        "private Attention digest input retains its historical 512-character bound"
    );
}

#[tokio::test]
async fn typed_restore_refuses_a_changed_condition_even_without_version_bump() {
    let db = db().await;
    let task = task(&db, "restore-fence").await;
    let saved = SavedTaskCondition::capture(&task.condition);
    test_support::set_task_condition(
        &db,
        &task.id,
        park(
            ConditionReasonOwner::Hold,
            ParkReason::Held {
                actor: "first".into(),
            },
            "first",
        ),
    )
    .await
    .unwrap();
    let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let restore = ConditionStatement::Restore {
        saved: Box::new(saved),
        expected_epoch: 0,
        expected_condition_digest: diagnostics::digest(raw.as_bytes()),
    };
    test_support::set_task_condition(
        &db,
        &task.id,
        park(
            ConditionReasonOwner::Hold,
            ParkReason::Held {
                actor: "successor".into(),
            },
            "second",
        ),
    )
    .await
    .unwrap();
    let version: i64 = sqlx::query_scalar("SELECT version FROM task WHERE id=?")
        .bind(&task.id)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(version, task.version);
    assert!(matches!(
        test_support::set_task_condition(&db, &task.id, restore).await,
        Err(DbError::VersionConflict)
    ));
    assert!(db
        .task_condition(&task.id)
        .await
        .unwrap()
        .reasons()
        .any(|r| matches!(r, ParkReason::Held { actor } if actor == "successor")));
}

#[tokio::test]
async fn budget_owned_clear_preserves_another_budget_kind() {
    let db = db().await;
    task(&db, "budgets").await;
    for kind in [crate::budget::Kind::Review, crate::budget::Kind::Execution] {
        test_support::set_task_condition(
            &db,
            "budgets",
            park(
                ConditionReasonOwner::Budget { kind },
                ParkReason::BudgetExhausted {
                    budget_kind: Some(kind),
                    source: ConditionSource {
                        field: LegacyConditionField::ConditionJson,
                        key: None,
                    },
                },
                kind.key(),
            ),
        )
        .await
        .unwrap();
    }
    let after = test_support::set_task_condition(
        &db,
        "budgets",
        ConditionStatement::Edit {
            edit: Box::new(ConditionOwnerEdit {
                owner: ConditionReasonOwner::Budget {
                    kind: crate::budget::Kind::Review,
                },
                expected: Some(ConditionIdentity::Marker {
                    epoch: 0,
                    marker_id: crate::budget::Kind::Review.key().into(),
                }),
                replacement: None,
            }),
        },
    )
    .await
    .unwrap();
    assert!(!after.reasons().any(|r| matches!(
        r,
        ParkReason::BudgetExhausted {
            budget_kind: Some(crate::budget::Kind::Review),
            ..
        }
    )));
    assert!(after.reasons().any(|r| matches!(
        r,
        ParkReason::BudgetExhausted {
            budget_kind: Some(crate::budget::Kind::Execution),
            ..
        }
    )));
}

#[tokio::test]
async fn foreign_hold_keeps_its_view_and_owned_clear_reveals_the_other_diagnosis() {
    let db = db().await;
    task(&db, "views").await;
    super::statement_tests::hold(&db, "views", "wait for measurements").await;
    let statement = ConditionStatement::Edit {
        edit: Box::new(ConditionOwnerEdit {
            owner: ConditionReasonOwner::Diagnostic,
            expected: None,
            replacement: Some(ConditionReplacement {
                identity: ConditionIdentity::Marker {
                    epoch: 0,
                    marker_id: "failure".into(),
                },
                condition: StatedCondition::Parked {
                    reason: ParkReason::Failure {
                        failure_kind: api_types::FailureKind::WorkspaceResetRequired,
                    },
                    resume: ConditionContinuation::Reconcile,
                    since: Some(AT.into()),
                    presentation: ConditionPresentation {
                        diagnostic: Some(ConditionDiagnostic {
                            kind: api_types::FailureKind::WorkspaceResetRequired,
                            blocking_reason: "workspace_reset".into(),
                            minimal_operator: true,
                            message: Some("workspace needs repair".into()),
                            actor: Some("owner".into()),
                            at: Some(AT.into()),
                            execution_id: None,
                        }),
                        ..Default::default()
                    },
                },
            }),
        }),
    };
    let held = test_support::set_task_condition(&db, "views", statement)
        .await
        .unwrap();
    assert!(matches!(
        held,
        TaskCondition::Parked {
            primary: ParkReason::Held { .. },
            ..
        }
    ));
    assert_eq!(
        held.read().diagnostic.unwrap().blocking_reason,
        "wait for measurements"
    );
    assert!(!material_blocker(&held).requires_intervention);
    let after = test_support::set_task_condition(
        &db,
        "views",
        ConditionStatement::Edit {
            edit: Box::new(ConditionOwnerEdit {
                owner: ConditionReasonOwner::Hold,
                expected: None,
                replacement: None,
            }),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        after,
        TaskCondition::Parked {
            primary: ParkReason::Failure {
                failure_kind: api_types::FailureKind::WorkspaceResetRequired
            },
            ..
        }
    ));
    assert_eq!(
        after.read().diagnostic.unwrap().message.as_deref(),
        Some("workspace needs repair")
    );
    assert!(material_blocker(&after).requires_intervention);
    assert!(db
        .task_diagnostic_texts("views", None, 100)
        .await
        .unwrap()
        .items
        .iter()
        .any(|d| d.text == "wait for measurements"));
}

#[tokio::test]
async fn preserve_cannot_guess_clear_over_corrupt_idle_storage() {
    let db = db().await;
    task(&db, "preserve-corrupt").await;
    sqlx::query("UPDATE task SET condition_json='{}' WHERE id='preserve-corrupt'")
        .execute(db.pool())
        .await
        .unwrap();
    let condition =
        state_with_policy(&db, "preserve-corrupt", Some(&ConditionStatement::Preserve)).await;
    assert!(recovery::unreadable(&condition));
    let texts = db
        .task_diagnostic_texts("preserve-corrupt", None, 100)
        .await
        .unwrap()
        .items;
    assert!(texts.iter().any(|d| d.text == "{}"));
}

#[tokio::test]
async fn owned_retry_keeps_its_deadline_across_a_hold_and_records_full_text() {
    let db = db().await;
    task(&db, "timer").await;
    const UNTIL: &str = "2030-01-01T00:00:00Z";
    let text = "r".repeat(200_000);
    let deferred = |owner| ConditionStatement::Edit {
        edit: Box::new(ConditionOwnerEdit {
            owner,
            expected: None,
            replacement: Some(ConditionReplacement {
                identity: ConditionIdentity::Marker {
                    epoch: 0,
                    marker_id: "retry".into(),
                },
                condition: StatedCondition::Deferred {
                    reason: RetryCause::ExecutionFailure,
                    until: Some(UNTIL.into()),
                    resume: ConditionContinuation::Dispatch {
                        target_state: "in_progress".into(),
                    },
                    presentation: ConditionPresentation {
                        retry: Some(ConditionRetry {
                            reason: text.clone(),
                            not_before: UNTIL.into(),
                            target_state: "in_progress".into(),
                        }),
                        ..Default::default()
                    },
                },
            }),
        }),
    };
    test_support::set_task_condition(&db, "timer", deferred(ConditionReasonOwner::Retry))
        .await
        .unwrap();
    test_support::set_task_condition(
        &db,
        "timer",
        park(
            ConditionReasonOwner::Hold,
            ParkReason::Held {
                actor: "owner".into(),
            },
            "hold",
        ),
    )
    .await
    .unwrap();
    let released = test_support::set_task_condition(
        &db,
        "timer",
        ConditionStatement::Edit {
            edit: Box::new(ConditionOwnerEdit {
                owner: ConditionReasonOwner::Hold,
                expected: None,
                replacement: None,
            }),
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(&released, TaskCondition::Deferred { until: Some(until), reason: RetryCause::ExecutionFailure, .. } if until == UNTIL)
    );
    assert_eq!(released.read().retry.unwrap().not_before, UNTIL);
    assert!(db
        .task_diagnostic_texts("timer", None, 100)
        .await
        .unwrap()
        .items
        .iter()
        .any(|d| d.text == text));
    test_support::set_task_condition(
        &db,
        "timer",
        ConditionStatement::Edit {
            edit: Box::new(ConditionOwnerEdit {
                owner: ConditionReasonOwner::Retry,
                expected: None,
                replacement: None,
            }),
        },
    )
    .await
    .unwrap();
    test_support::set_task_condition(&db, "timer", deferred(ConditionReasonOwner::Environment))
        .await
        .unwrap();
    let cleared = test_support::set_task_condition(
        &db,
        "timer",
        ConditionStatement::Edit {
            edit: Box::new(ConditionOwnerEdit {
                owner: ConditionReasonOwner::Environment,
                expected: None,
                replacement: None,
            }),
        },
    )
    .await
    .unwrap();
    assert!(matches!(cleared, TaskCondition::Clear { .. }));
    assert!(!cleared.read().retry_recorded);
    assert!(cleared.read().retry.is_none());
}

async fn state_with_policy(
    db: &SqliteDb,
    id: &str,
    statement: Option<&ConditionStatement>,
) -> TaskCondition {
    let mut tx = crate::begin_immediate(db.pool()).await.unwrap();
    let produced = producers::derive_with_policy(
        &mut tx,
        id,
        ConditionChange::Full,
        None,
        statement,
        recovery::UnreadablePolicy::DurableOwners,
    )
    .await
    .unwrap()
    .unwrap();
    set_condition(&mut tx, id, &produced.encoded, Some(produced.version))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.task_condition(id).await.unwrap()
}
