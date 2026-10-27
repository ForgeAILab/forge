//! Review contracts repository tests.
use super::*;

#[tokio::test]
async fn review_digest_settings_and_audit_edits_preserve_passed_contract() {
    let (db, project_id, task_id, contract) = seed_review_digest_contract(2).await;
    let project = ProjectRepo::get_by_id(&db, &project_id)
        .await
        .unwrap()
        .unwrap();
    ProjectRepo::update_at_version(
        &db,
        UpdateProject {
            id: project_id.clone(),
            name: None,
            settings: Some(
                serde_json::json!({
                    "max_active_tasks": 7,
                    "default_review_config": {"ci_steps": ["true"]},
                    "environment": {"env": {"TOKEN": "rotated-secret"}, "recheck_interval_seconds": 1200},
                })
                .to_string(),
            ),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
        project.version,
        None,
    )
    .await
    .unwrap();
    // Non-review workflow configuration and Task state config are not authority.
    sqlx::query("UPDATE project SET workflow_definition = json_set(workflow_definition, '$.states[1].config', json('{\"unrelated\":true}')) WHERE id = ?")
        .bind(&project_id).execute(db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET task_state_config = '{\"working\":{\"unrelated\":true}}', priority = 10 WHERE id = ?")
        .bind(&task_id).execute(db.pool()).await.unwrap();
    db.record_review_conformance(
        &db.review_conformance(&contract.execution_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .await
    .unwrap();
    assert_review_digest_integrates(&db, &task_id).await;
    // The same passed-contract verifier also authorizes carry.
    let now = now_rfc3339();
    ReviewRepo::create(
        &db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id: contract.execution_id.clone(),
            attempt_number: 2,
            status: ReviewStatus::Running,
            step_results_json: "{}".into(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        db.review_carry_base(&task_id).await.unwrap().contract,
        contract
    );
}

#[tokio::test]
async fn review_digest_flow_settings_preserve_passed_contract() {
    for (key, value) in [
        ("max_active_tasks", serde_json::json!(7)),
        (
            "environment.recheck_interval_seconds",
            serde_json::json!(1200),
        ),
    ] {
        let (db, project_id, task_id, contract) = seed_review_digest_contract(2).await;
        let project = ProjectRepo::get_by_id(&db, &project_id)
            .await
            .unwrap()
            .unwrap();
        let mut settings: serde_json::Value = serde_json::from_str(&project.settings).unwrap();
        if key == "max_active_tasks" {
            settings[key] = value;
        } else {
            settings["environment"]["recheck_interval_seconds"] = value;
        }
        ProjectRepo::update_at_version(
            &db,
            UpdateProject {
                id: project_id,
                name: None,
                settings: Some(settings.to_string()),
                primary_repo_id: None,
                paused_at: None,
                updated_at: now_rfc3339(),
            },
            project.version,
            None,
        )
        .await
        .unwrap();
        assert_review_digest_integrates(&db, &task_id).await;
        // Carry verifies the previous attempt when a new Review is running.
        let now = now_rfc3339();
        ReviewRepo::create(
            &db,
            CreateReview {
                id: new_uuid_v4(),
                task_id: task_id.clone(),
                execution_id: contract.execution_id.clone(),
                attempt_number: 2,
                status: ReviewStatus::Running,
                step_results_json: "{}".into(),
                started_at: now.clone(),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            db.review_carry_base(&task_id).await.unwrap().contract,
            contract,
            "{key}"
        );
    }
}

#[tokio::test]
async fn review_digest_governing_edits_invalidate_passed_contract() {
    for change in [
        "ci_steps",
        "description",
        "plan",
        "requirement_ids",
        "review_state",
        "env_name",
    ] {
        let (db, project_id, task_id, contract) = seed_review_digest_contract(2).await;
        let (sql, id) = match change {
            "ci_steps" => ("UPDATE project SET settings = json_set(settings, '$.default_review_config.ci_steps', json('[\"false\"]')) WHERE id = ?", &project_id),
            "review_state" => ("UPDATE project SET workflow_definition = json_set(workflow_definition, '$.states[0].config.review_prompt', 'Changed acceptance instruction') WHERE id = ?", &project_id),
            "env_name" => ("UPDATE project SET settings = json_set(settings, '$.environment.env.NEW_TOKEN', 'secret') WHERE id = ?", &project_id),
            "description" => ("UPDATE task SET description = 'Changed acceptance criteria' WHERE id = ?", &task_id),
            "plan" => ("UPDATE task SET plan = 'Changed required plan' WHERE id = ?", &task_id),
            "requirement_ids" => ("UPDATE task SET task_state_config = '{\"review\":{\"requirement_ids\":[\"new-requirement\"]}}' WHERE id = ?", &task_id),
            _ => unreachable!(),
        };
        sqlx::query(sql).bind(id).execute(db.pool()).await.unwrap();
        assert!(
            matches!(
                db.lock_review_integration(&task_id).await,
                Err(DbError::Check(_))
            ),
            "{change}"
        );
        let conformance = db
            .review_conformance(&contract.execution_id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                db.record_review_conformance(&conformance).await,
                Err(DbError::Check(_))
            ),
            "{change}"
        );
    }
}

#[tokio::test]
async fn review_digest_unversioned_contract_preserves_v1_verification() {
    let (db, project_id, task_id, contract) = seed_review_digest_contract(1).await;
    let raw: String = sqlx::query_scalar(
        "SELECT contract_json FROM execution_review_contract WHERE execution_id = ?",
    )
    .bind(&contract.execution_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(!raw.contains("source_digest_version"));
    let restored = db
        .review_contract(&contract.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.context.source_digest_version, None);
    assert_eq!(serde_json::to_string(&restored).unwrap(), raw);
    let mut digest_input = serde_json::from_str::<serde_json::Value>(&raw).unwrap();
    digest_input["digest"] = serde_json::json!("");
    assert_eq!(
        api_types::canonical_digest(&digest_input).unwrap(),
        restored.digest
    );
    assert_review_digest_integrates(&db, &task_id).await;
    sqlx::query(
        "UPDATE project SET settings = json_set(settings, '$.max_active_tasks', 7) WHERE id = ?",
    )
    .bind(&project_id)
    .execute(db.pool())
    .await
    .unwrap();
    assert!(matches!(
        db.lock_review_integration(&task_id).await,
        Err(DbError::Check(_))
    ));
    let conformance = db
        .review_conformance(&contract.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        db.record_review_conformance(&conformance).await,
        Err(DbError::Check(_))
    ));
}

#[test]
fn review_digest_scopes_authority_and_rejects_unknown_versions() {
    let source = serde_json::json!({
        "charter": {"id": "r1", "content_digest": "charter-digest"},
        "task_charter_revision_id": "r1",
        "task_scope": {"title": "Task", "description": "Acceptance", "plan": "Plan", "config": {}, "allocations": {}},
        "documents": [{"id": "doc-r1", "content_digest": "document-digest"}],
        "workflow": {"states": [{"name": "review", "role": "reviewer", "config": {}}]},
        "project_settings": {"default_review_config": {
            "setup_steps": [], "ci_steps": [], "check_timeout_seconds": 1800,
            "conformance_checks": [], "requirement_ids": [], "requirement_allocations": {},
        }},
    });
    let digest = api_types::review_source_digest(&source, 2).unwrap();
    for (pointer, value) in [
        ("/charter/id", serde_json::json!("r2")),
        (
            "/charter/content_digest",
            serde_json::json!("new requirement digest"),
        ),
        (
            "/documents/0/content_digest",
            serde_json::json!("new acceptance digest"),
        ),
        (
            "/task_scope/allocations",
            serde_json::json!({"req": {"task_id": "other-task"}}),
        ),
        ("/task_scope/title", serde_json::json!("New title")),
        (
            "/project_settings/default_review_config/setup_steps",
            serde_json::json!(["install"]),
        ),
        (
            "/project_settings/default_review_config/check_timeout_seconds",
            serde_json::json!(60),
        ),
        (
            "/project_settings/default_review_config/conformance_checks",
            serde_json::json!([{"id": "check", "command": "test", "requirement_ids": ["req"]}]),
        ),
    ] {
        let mut changed = source.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        assert_ne!(
            api_types::review_source_digest(&changed, 2).unwrap(),
            digest,
            "{pointer}"
        );
    }
    let mut audit = source.clone();
    audit["reviewer_assignment"] = serde_json::json!({"id": "new-reviewer"});
    audit["task_scope"]["evidence"] =
        serde_json::json!({"worklog": ["new report"], "media": ["new attachment"]});
    audit["task_scope"]["merge_config"] = serde_json::json!("changed target");
    assert_eq!(api_types::review_source_digest(&audit, 2).unwrap(), digest);
    // Fingerprint effective commands: overridden Project defaults have no effect.
    let mut overridden = source.clone();
    overridden["task_scope"]["config"]["review"] = serde_json::json!({"ci_steps": ["true"]});
    let override_digest = api_types::review_source_digest(&overridden, 2).unwrap();
    overridden["project_settings"]["default_review_config"]["ci_steps"] =
        serde_json::json!(["false"]);
    assert_eq!(
        api_types::review_source_digest(&overridden, 2).unwrap(),
        override_digest
    );
    assert!(api_types::review_source_digest(&source, 99).is_err());
}

#[tokio::test]
async fn review_task_authority_settlement_is_atomic_with_task_cas() {
    let db = sqlite_db().await;
    let (project_id, _repo_id, _agent_id) = seed_project_repo_agent(&db).await;
    let task_id = seed_task(&db, &project_id, None, "review".to_owned(), "review task").await;
    let now = now_rfc3339();
    let execution_id = new_uuid_v4();
    ExecutionRepo::create(
        &db,
        CreateExecution {
            id: execution_id.clone(),
            task_id: task_id.clone(),
            agent_id: None,
            role: "reviewer".to_owned(),
            status: ExecutionStatus::Completed,
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
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review execution creates");
    let review = ReviewRepo::create(
        &db,
        CreateReview {
            id: new_uuid_v4(),
            task_id: task_id.clone(),
            execution_id,
            attempt_number: 1,
            status: ReviewStatus::Running,
            step_results_json: "{}".to_owned(),
            started_at: now.clone(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("review creates");
    let task = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task loads")
        .expect("task exists");

    let stale = ReviewRepo::update_status_with_task_authority(
        &db,
        &review.id,
        ReviewStatus::Passed,
        "{}".to_owned(),
        Some(now.clone()),
        &now,
        task.version + 1,
        Some(now.clone()),
        crate::ReviewEventOrigin::Runner,
    )
    .await;
    assert!(matches!(stale, Err(DbError::VersionConflict)));
    assert_eq!(
        ReviewRepo::get_by_id(&db, &review.id)
            .await
            .expect("review reloads")
            .expect("review exists")
            .status,
        ReviewStatus::Running
    );
    let task_after_stale = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads")
        .expect("task exists");
    assert_eq!(task_after_stale.version, task.version);
    assert!(task_after_stale.review_passed_at.is_none());
    assert!(!DomainEventRepo::list_events_after(&db, 0, 100)
        .await
        .expect("events list after stale settlement")
        .iter()
        .any(|event| event.entity_id == review.id && event.event_type == "review.status_changed"));

    let (settled_review, settled_task) = ReviewRepo::update_status_with_task_authority(
        &db,
        &review.id,
        ReviewStatus::Passed,
        "{}".to_owned(),
        Some(now.clone()),
        &now,
        task.version,
        Some(now.clone()),
        crate::ReviewEventOrigin::Runner,
    )
    .await
    .expect("review and task authority settle together");
    assert_eq!(settled_review.status, ReviewStatus::Passed);
    assert_eq!(settled_task.version, task.version + 1);
    assert_eq!(settled_task.review_passed_at.as_deref(), Some(now.as_str()));

    let replay = ReviewRepo::update_status_with_task_authority(
        &db,
        &review.id,
        ReviewStatus::Passed,
        "{}".to_owned(),
        Some(now.clone()),
        &now,
        settled_task.version,
        Some(now.clone()),
        crate::ReviewEventOrigin::Runner,
    )
    .await;
    assert!(matches!(replay, Err(DbError::InvalidTransition)));
    let task_after_replay = TaskRepo::get_by_id(&db, &task_id, false)
        .await
        .expect("task reloads after replay")
        .expect("task exists after replay");
    assert_eq!(task_after_replay.version, settled_task.version);
    assert_eq!(
        task_after_replay.review_passed_at,
        settled_task.review_passed_at
    );
    assert_eq!(
        DomainEventRepo::list_events_after(&db, 0, 100)
            .await
            .expect("events list after replay")
            .iter()
            .filter(
                |event| event.entity_id == review.id && event.event_type == "review.status_changed"
            )
            .count(),
        1
    );
}

/// A v0.13.12 server froze assessments as `{result, reason, report}`: no
/// routing fields and no findings. Insert those exact bytes (not the current
/// writer's output) and replay them through the current reader and writer.
#[tokio::test]
async fn review_findings_v0_13_12_conformance_round_trip_is_byte_identical() {
    let (db, _, _, seeded) = seed_review_digest_contract(2).await;
    let now = now_rfc3339();
    let mut contract = seeded.clone();
    contract.execution_id = new_uuid_v4();
    contract.digest = String::new();
    contract.digest = api_types::canonical_digest(&contract).unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,?,'reviewer','completed',?,?)")
        .bind(&contract.execution_id).bind(&contract.context.task_id).bind(&now).bind(&now)
        .execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO execution_review_contract VALUES(?,?,?,?,?,?)")
        .bind(&contract.execution_id)
        .bind(&contract.context.task_id)
        .bind(&contract.digest)
        .bind(&contract.context.source_digest)
        .bind(serde_json::to_string(&contract).unwrap())
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    let historical = format!(
        "{{\"status\":\"failed\",\"contract\":{},\"assessment\":{{\"result\":\"fail\",\"reason\":\"missing test\",\"report\":\"## Review\\n\\nNo test.\"}},\"checks\":[],\"reason\":\"missing test\"}}",
        serde_json::to_string(&contract).unwrap()
    );
    sqlx::query("INSERT INTO execution_review_assessment VALUES(?,?,?)")
        .bind(&contract.execution_id)
        .bind(&historical)
        .bind(&now)
        .execute(db.pool())
        .await
        .unwrap();
    let before: String = sqlx::query_scalar(
        "SELECT conformance_json FROM execution_review_assessment WHERE execution_id=?",
    )
    .bind(&contract.execution_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(before, historical);
    let assessment = db
        .review_conformance(&contract.execution_id)
        .await
        .unwrap()
        .unwrap();
    assert!(assessment.assessment.as_ref().unwrap().findings.is_empty());
    assert!(assessment.assessment.as_ref().unwrap().resolved.is_empty());
    assert_eq!(serde_json::to_string(&assessment).unwrap(), historical);
    db.record_review_conformance(&assessment).await.unwrap();
    let after: String = sqlx::query_scalar(
        "SELECT conformance_json FROM execution_review_assessment WHERE execution_id=?",
    )
    .bind(&contract.execution_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(after, before);
    let mut changed = assessment;
    changed
        .assessment
        .as_mut()
        .unwrap()
        .resolved
        .push("F1".into());
    assert!(
        matches!(db.record_review_conformance(&changed).await,Err(DbError::Check(message)) if message=="assessment is already frozen")
    );
}

#[tokio::test]
async fn review_frozen_long_form_assessment_replays_without_rewriting_bytes() {
    let (db, assessment, historical) = long_form_review_assessment().await;
    assert_ne!(serde_json::to_string(&assessment).unwrap(), historical);
    db.record_review_conformance(&assessment).await.unwrap();
    let stored: String = sqlx::query_scalar(
        "SELECT conformance_json FROM execution_review_assessment WHERE execution_id = ?",
    )
    .bind(&assessment.contract.as_ref().unwrap().execution_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(stored, historical);
}

#[tokio::test]
async fn review_frozen_long_form_assessment_refuses_changed_content() {
    let (db, assessment, historical) = long_form_review_assessment().await;
    for field in ["verdict", "reason", "report", "finding"] {
        let mut changed = assessment.clone();
        let report = changed.assessment.as_mut().unwrap();
        match field {
            "verdict" => report.result = api_types::ReviewResult::Pass,
            "reason" => report.reason = "different reason".into(),
            "report" => report.report = "different report".into(),
            "finding" => report.findings.push(api_types::ReviewFinding {
                requirement_ids: Vec::new(),
                kind: None,
                same_cause_as: None,
                id: None,
                summary: "different finding".into(),
                severity: api_types::ReviewFindingSeverity::Blocking,
                r#where: None,
                expected: None,
                observed: None,
            }),
            _ => unreachable!(),
        }
        assert!(
            matches!(db.record_review_conformance(&changed).await, Err(DbError::Check(message)) if message == "assessment is already frozen"),
            "changed {field} must be refused"
        );
        let stored: String = sqlx::query_scalar(
            "SELECT conformance_json FROM execution_review_assessment WHERE execution_id = ?",
        )
        .bind(&assessment.contract.as_ref().unwrap().execution_id)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(stored, historical, "changed {field} rewrote the frozen row");
    }
}
