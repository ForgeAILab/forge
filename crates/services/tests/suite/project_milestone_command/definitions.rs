//! Milestone definition, revision, primary selection, replay, and charter binding.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn milestone_service_is_race_safe_materializes_checks_and_replays_frozen_state() {
    let db = fixture().await;
    let service = ProjectMilestoneCommandService::new(Arc::clone(&db));
    let define = definition_command(DefinitionCommandInput {
        project_version: 1,
        milestone_id: None,
        milestone_version: 1,
        base_revision_id: None,
        lifecycle: MilestoneDefinitionLifecycle::Proposed,
        key: "service-define-race",
        name: "Service milestone",
        check_id: Some("service-check"),
    });
    let service_a = service.clone();
    let service_b = service.clone();
    let (first, second) = tokio::join!(
        service_a.define_milestone(define.clone(), None),
        service_b.define_milestone(define.clone(), None),
    );
    let first = first.expect("first service define");
    let second = second.expect("racing service define replays");
    assert_eq!(second, first);
    let milestone_key: String = sqlx::query_scalar(
        "SELECT milestone_key FROM project_milestone WHERE id = ? AND project_id = ?",
    )
    .bind(&first.milestone_id)
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("allocated milestone key");
    assert_eq!(milestone_key, "M002");
    let materialized_check: (String, String, i64) = sqlx::query_as(
        "SELECT definition_revision_id, source_kind, version
         FROM project_milestone_check WHERE id = 'service-check'",
    )
    .fetch_one(db.pool())
    .await
    .expect("proposed acceptance check materializes");
    assert_eq!(materialized_check.0, first.id);
    assert_eq!(materialized_check.1, "manual");
    assert_eq!(materialized_check.2, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt
             WHERE scope_id = ? AND operation = ? AND idempotency_key = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .bind("service-define-race")
        .fetch_one(db.pool())
        .await
        .expect("define receipt count"),
        1
    );

    let primary = ProjectPrimaryMilestoneCommand {
        project_id: PROJECT_ID.to_owned(),
        primary_milestone_id: Some(first.milestone_id.clone()),
        expected_project_version: 2,
        idempotency_key: "service-primary".to_owned(),
        authorization: user_authorization("project.milestone.primary.set", "service-primary"),
    };
    let frozen = service
        .set_primary_milestone(primary.clone(), None)
        .await
        .expect("primary milestone command");
    sqlx::query("UPDATE project SET name = 'Later name', version = version + 1 WHERE id = ?")
        .bind(PROJECT_ID)
        .execute(db.pool())
        .await
        .expect("later Project mutation");
    let replay = service
        .set_primary_milestone(primary, None)
        .await
        .expect("primary milestone frozen replay");
    assert_eq!(replay, frozen);
    assert_ne!(
        ProjectRepo::get_by_id(&*db, PROJECT_ID)
            .await
            .expect("Project lookup")
            .expect("Project")
            .name,
        frozen.name
    );
    assert_eq!(
        service
            .define_milestone(define.clone(), None)
            .await
            .expect("define replay after mutable Project changes"),
        first
    );
    let mut changed = define;
    changed.content.outcome = "changed outcome".to_owned();
    changed.rendered_view =
        api_types::canonical_json(&changed.content).expect("changed canonical content");
    assert!(matches!(
        service.define_milestone(changed, None).await,
        Err(ServiceError::Db(DbError::IdempotencyConflict))
    ));

    let project_version: i64 = sqlx::query_scalar("SELECT version FROM project WHERE id = ?")
        .bind(PROJECT_ID)
        .fetch_one(db.pool())
        .await
        .expect("current Project version");
    let draft = service
        .define_milestone(
            definition_command(DefinitionCommandInput {
                project_version,
                milestone_id: None,
                milestone_version: 1,
                base_revision_id: None,
                lifecycle: MilestoneDefinitionLifecycle::Draft,
                key: "service-draft",
                name: "Draft milestone",
                check_id: Some("draft-check"),
            }),
            None,
        )
        .await
        .expect("draft define");
    assert_eq!(draft.lifecycle, "draft");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_milestone_check WHERE id = 'draft-check'",
        )
        .fetch_one(db.pool())
        .await
        .expect("draft check count"),
        0
    );
    let proposed = service
        .revise_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 0,
                milestone_id: Some(&draft.milestone_id),
                milestone_version: 2,
                base_revision_id: Some(&draft.id),
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                key: "service-draft-propose",
                name: "Draft milestone proposed",
                check_id: Some("draft-check"),
            }),
            None,
        )
        .await
        .expect("draft-first milestone can be proposed");
    assert_eq!(
        proposed.base_revision_id.as_deref(),
        Some(draft.id.as_str())
    );
    assert_eq!(proposed.lifecycle, "proposed");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT definition_revision_id FROM project_milestone_check
             WHERE id = 'draft-check'",
        )
        .fetch_one(db.pool())
        .await
        .expect("proposed draft check materializes"),
        proposed.id
    );
}

#[tokio::test]
async fn milestone_define_revise_and_primary_share_receipts_and_replay_contract() {
    let db = fixture().await;

    let define_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": "direct-milestone",
        "revision_id": "direct-milestone-revision",
    });
    let define_receipt = receipt(ReceiptInput {
        operation: PROJECT_MILESTONE_OPERATION,
        principal_type: "user",
        principal_id: USER_ID,
        key: "direct-define",
        digest: "digest-direct-define",
        correlation_id: "direct-define-correlation",
        outcome: &define_outcome,
        execution_id: None,
    });
    let define = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: "direct-milestone".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 2,
            milestone_key: "M002".to_owned(),
            display_label: Some("Direct milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: milestone_revision(
            "direct-milestone-revision",
            "direct-milestone",
            1,
            0,
            None,
            "proposed",
        ),
        allocate_project_sequence: false,
        check_definitions: Vec::new(),
        command_receipt: Some(define_receipt),
        action_execution: None,
    };
    let created = ProjectOrchestrationRepo::create_project_milestone_command(&*db, define)
        .await
        .expect("direct milestone define");
    assert_eq!(created.revision, 1);

    let revise_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": "direct-milestone",
        "revision_id": "direct-milestone-revision-2",
    });
    let revise = db::AppendProjectMilestoneRevisionCommand {
        revision: milestone_revision(
            "direct-milestone-revision-2",
            "direct-milestone",
            2,
            1,
            Some("direct-milestone-revision"),
            "proposed",
        ),
        check_definitions: Vec::new(),
        command_receipt: Some(receipt(ReceiptInput {
            operation: PROJECT_MILESTONE_OPERATION,
            principal_type: "user",
            principal_id: USER_ID,
            key: "direct-revise",
            digest: "digest-direct-revise",
            correlation_id: "direct-revise-correlation",
            outcome: &revise_outcome,
            execution_id: None,
        })),
        action_execution: None,
    };
    let revised = ProjectOrchestrationRepo::append_project_milestone_revision_command(&*db, revise)
        .await
        .expect("direct milestone revise");
    assert_eq!(revised.revision, 2);

    let primary_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "primary_milestone_id": "direct-milestone",
    });
    let primary = db::SetPrimaryProjectMilestoneCommand {
        project_id: PROJECT_ID.to_owned(),
        primary_milestone_id: Some("direct-milestone".to_owned()),
        expected_project_version: 2,
        principal_type: "user".to_owned(),
        principal_id: USER_ID.to_owned(),
        authorization_basis: "explicit user authorization".to_owned(),
        authorization_action: "project.milestone.primary.set".to_owned(),
        authorization_occurred_at: db::now_rfc3339(),
        explicit_event: "primary-set".to_owned(),
        idempotency_key: "direct-primary".to_owned(),
        updated_at: NOW.to_owned(),
        command_receipt: Some(receipt(ReceiptInput {
            operation: PROJECT_MILESTONE_OPERATION,
            principal_type: "user",
            principal_id: USER_ID,
            key: "direct-primary",
            digest: "digest-direct-primary",
            correlation_id: "direct-primary-correlation",
            outcome: &primary_outcome,
            execution_id: None,
        })),
        action_execution: None,
    };
    let project =
        ProjectOrchestrationRepo::set_primary_project_milestone_command(&*db, primary.clone())
            .await
            .expect("direct set primary");
    assert_eq!(
        project.primary_milestone_id.as_deref(),
        Some("direct-milestone")
    );
    let replay = ProjectOrchestrationRepo::set_primary_project_milestone_command(&*db, primary)
        .await
        .expect("direct set primary replay");
    assert_eq!(replay, project);

    let stored = CommandReceiptRepo::get_command_receipt(
        &*db,
        "user",
        USER_ID,
        "project",
        PROJECT_ID,
        PROJECT_MILESTONE_OPERATION,
        "direct-primary",
        "digest-direct-primary",
    )
    .await
    .expect("primary receipt lookup")
    .expect("primary receipt");
    let event = DomainEventRepo::get_event(&*db, &stored.event_id)
        .await
        .expect("primary event lookup")
        .expect("primary event");
    assert_eq!(event.actor_type, "user");
    assert_eq!(event.actor_id.as_deref(), Some(USER_ID));
    assert_eq!(event.correlation_id, stored.correlation_id);
    assert_eq!(event.causation_id, stored.causation_id);
    assert_eq!(event.causation_depth, stored.causation_depth);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM command_receipt WHERE scope_id = ? AND operation = ?",
        )
        .bind(PROJECT_ID)
        .bind(PROJECT_MILESTONE_OPERATION)
        .fetch_one(db.pool())
        .await
        .expect("milestone receipt count"),
        3
    );
}

#[tokio::test]
async fn native_milestone_and_readiness_release_commands_replay_after_state_changes() {
    let db = fixture().await;

    let milestone_action = action(
        &db,
        "native-milestone-action",
        PROJECT_MILESTONE_OPERATION,
        "native-milestone-correlation",
    )
    .await;
    let define_outcome = json!({
        "operation": PROJECT_MILESTONE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": "native-milestone",
        "revision_id": "native-milestone-revision",
    });
    let define_key = "native-milestone-define";
    let define_execution_id = "native-milestone-execution";
    let define_receipt = receipt(ReceiptInput {
        operation: PROJECT_MILESTONE_OPERATION,
        principal_type: "agent",
        principal_id: AGENT_ID,
        key: define_key,
        digest: "digest-native-milestone",
        correlation_id: "native-milestone-correlation",
        outcome: &define_outcome,
        execution_id: Some(define_execution_id),
    });
    let define_execution = action_execution(
        &milestone_action,
        define_execution_id,
        define_key,
        &define_outcome,
    );
    let define_input = CreateProjectMilestoneCommand {
        milestone: CreateProjectMilestone {
            id: "native-milestone".to_owned(),
            project_id: PROJECT_ID.to_owned(),
            expected_project_version: 1,
            milestone_sequence: 2,
            milestone_key: "M002".to_owned(),
            display_label: Some("Native milestone".to_owned()),
            created_at: NOW.to_owned(),
            updated_at: NOW.to_owned(),
        },
        revision: milestone_revision(
            "native-milestone-revision",
            "native-milestone",
            1,
            0,
            None,
            "proposed",
        ),
        allocate_project_sequence: false,
        check_definitions: Vec::new(),
        command_receipt: Some(define_receipt.clone()),
        action_execution: Some(define_execution.clone()),
    };
    let defined =
        ProjectOrchestrationRepo::create_project_milestone_command(&*db, define_input.clone())
            .await
            .expect("native milestone define");
    let replay = ProjectOrchestrationRepo::create_project_milestone_command(&*db, define_input)
        .await
        .expect("native milestone replay");
    assert_eq!(replay, defined);

    let stored = CommandReceiptRepo::get_command_receipt(
        &*db,
        "agent",
        AGENT_ID,
        "project",
        PROJECT_ID,
        PROJECT_MILESTONE_OPERATION,
        define_key,
        "digest-native-milestone",
    )
    .await
    .expect("native milestone receipt lookup")
    .expect("native milestone receipt");
    assert_eq!(
        stored.agent_action_execution_id.as_deref(),
        Some(define_execution_id)
    );
    let event = DomainEventRepo::get_event(&*db, &stored.event_id)
        .await
        .expect("native milestone event lookup")
        .expect("native milestone event");
    assert_eq!(event.actor_type, "agent");
    assert_eq!(event.actor_id.as_deref(), Some(AGENT_ID));
    assert_eq!(event.correlation_id, "native-milestone-correlation");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_action_execution WHERE id = ?",)
            .bind(define_execution_id)
            .fetch_one(db.pool())
            .await
            .expect("native execution count"),
        1
    );

    sqlx::query("UPDATE project_milestone SET version = 99 WHERE id = ?")
        .bind("native-milestone")
        .execute(db.pool())
        .await
        .expect("mutable milestone state changes");
    let mut changed_digest = define_input_for_conflict(&defined, &stored);
    changed_digest.input_digest = "different-digest".to_owned();
    let conflict = ProjectOrchestrationRepo::create_project_milestone_command(
        &*db,
        changed_digest.into_command(),
    )
    .await;
    assert!(matches!(conflict, Err(DbError::IdempotencyConflict)));

    let mut changed_principal = define_input_for_conflict(&defined, &stored);
    changed_principal.principal_id_for_test = Some("other-agent".to_owned());
    let conflict = ProjectOrchestrationRepo::create_project_milestone_command(
        &*db,
        changed_principal.into_command(),
    )
    .await;
    assert!(matches!(conflict, Err(DbError::IdempotencyConflict)));

    // A direct user readiness command is the existing user-side materializer
    // contract.  The native release request below proves the optional action
    // execution linkage on the request/event materialization boundary.
    let readiness_key = "direct-readiness";
    let readiness_outcome = json!({
        "operation": PROJECT_READINESS_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": MILESTONE_ID,
        "readiness_snapshot_id": "readiness-command-snapshot",
    });
    let mut readiness = readiness_snapshot("readiness-command-snapshot", readiness_key);
    readiness.snapshot.event_watermark = sqlx::query_scalar(
        "SELECT COALESCE(
            (SELECT id FROM domain_event
             WHERE scope_type = 'project' AND scope_id = ?
               AND event_type != 'milestone.readiness.evaluated'
             ORDER BY sequence DESC LIMIT 1),
            'none')",
    )
    .bind(PROJECT_ID)
    .fetch_one(db.pool())
    .await
    .expect("readiness source watermark");
    readiness.command_receipt = Some(receipt(ReceiptInput {
        operation: PROJECT_READINESS_OPERATION,
        principal_type: "user",
        principal_id: USER_ID,
        key: readiness_key,
        digest: "digest-direct-readiness",
        correlation_id: "direct-readiness-correlation",
        outcome: &readiness_outcome,
        execution_id: None,
    }));
    let snapshot = ProjectOrchestrationRepo::create_project_readiness_snapshot_command(
        &*db,
        readiness.clone(),
    )
    .await
    .expect("direct readiness command");
    assert_eq!(snapshot.outcome, "ready");
    sqlx::query("UPDATE project_milestone SET version = 77 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("readiness mutable state changes");
    let snapshot_replay =
        ProjectOrchestrationRepo::create_project_readiness_snapshot_command(&*db, readiness)
            .await
            .expect("direct readiness replay");
    assert_eq!(snapshot_replay, snapshot);

    let release_action = action(
        &db,
        "native-release-action",
        PROJECT_RELEASE_OPERATION,
        "native-release-correlation",
    )
    .await;
    let release_key = "native-release-request";
    let release_event_id = "native-release-event";
    let release_outcome = json!({
        "operation": PROJECT_RELEASE_OPERATION,
        "project_id": PROJECT_ID,
        "milestone_id": MILESTONE_ID,
        "readiness_snapshot_id": "readiness-command-snapshot",
        "candidate_event_id": release_event_id,
        "status": "pending_user_release_approval",
    });
    // Restore only the expected version needed by the release request.  The
    // request contract still verifies the ready lifecycle and snapshot digest.
    sqlx::query(
        "UPDATE project_milestone SET version = 2, lifecycle = 'ready_for_release'
         WHERE id = ?",
    )
    .bind(MILESTONE_ID)
    .execute(db.pool())
    .await
    .expect("ready milestone state");
    let release_receipt = receipt(ReceiptInput {
        operation: PROJECT_RELEASE_OPERATION,
        principal_type: "agent",
        principal_id: AGENT_ID,
        key: release_key,
        digest: "digest-native-release",
        correlation_id: "native-release-correlation",
        outcome: &release_outcome,
        execution_id: Some("native-release-execution"),
    });
    let release_execution = action_execution(
        &release_action,
        "native-release-execution",
        release_key,
        &release_outcome,
    );
    let release = CreateProjectReleaseRequestCommand {
        request: CreateProjectReleaseRequest {
            event_id: release_event_id.to_owned(),
            project_id: PROJECT_ID.to_owned(),
            milestone_id: MILESTONE_ID.to_owned(),
            expected_milestone_version: 2,
            readiness_snapshot_id: "readiness-command-snapshot".to_owned(),
            readiness_digest: "readiness-digest-1".to_owned(),
            status: "pending_user_release_approval".to_owned(),
            idempotency_key: release_key.to_owned(),
            created_at: NOW.to_owned(),
        },
        command_receipt: Some(release_receipt.clone()),
        action_execution: Some(release_execution.clone()),
    };
    let request =
        ProjectOrchestrationRepo::create_project_release_request_command(&*db, release.clone())
            .await
            .expect("native release request");
    assert_eq!(request.status, "pending_user_release_approval");
    let request_replay =
        ProjectOrchestrationRepo::create_project_release_request_command(&*db, release)
            .await
            .expect("native release request replay");
    assert_eq!(request_replay, request);
    let stored_release = CommandReceiptRepo::get_command_receipt(
        &*db,
        "agent",
        AGENT_ID,
        "project",
        PROJECT_ID,
        PROJECT_RELEASE_OPERATION,
        release_key,
        "digest-native-release",
    )
    .await
    .expect("release receipt lookup")
    .expect("release receipt");
    assert_eq!(
        stored_release.agent_action_execution_id.as_deref(),
        Some("native-release-execution")
    );
    let release_event = DomainEventRepo::get_event(&*db, &stored_release.event_id)
        .await
        .expect("release event lookup")
        .expect("release event");
    assert_eq!(
        release_event.event_type,
        "project_release.candidate_requested"
    );
    assert_eq!(release_event.actor_type, "agent");
    assert_eq!(release_event.actor_id.as_deref(), Some(AGENT_ID));
    assert_eq!(release_event.correlation_id, "native-release-correlation");

    sqlx::query("UPDATE project_milestone SET version = 100 WHERE id = ?")
        .bind(MILESTONE_ID)
        .execute(db.pool())
        .await
        .expect("release mutable state changes");
    let mut changed_release = release_input_for_conflict(&request, &stored_release);
    changed_release.input_digest = "different-release-digest".to_owned();
    let conflict = ProjectOrchestrationRepo::create_project_release_request_command(
        &*db,
        changed_release.into_command(),
    )
    .await;
    assert!(matches!(conflict, Err(DbError::IdempotencyConflict)));
}

#[tokio::test]
async fn verbatim_revise_returns_current_revision_and_keeps_approval_standing() {
    let db = fixture().await;
    let service = ProjectMilestoneCommandService::new(Arc::clone(&db));
    let defined = service
        .define_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 1,
                milestone_id: None,
                milestone_version: 1,
                base_revision_id: None,
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                key: "verbatim-define",
                name: "Verbatim milestone",
                check_id: Some("verbatim-check"),
            }),
            None,
        )
        .await
        .expect("define milestone");
    sqlx::query("UPDATE project_milestone_revision SET lifecycle = 'approved' WHERE id = ?")
        .bind(&defined.id)
        .execute(db.pool())
        .await
        .expect("mark revision approved");

    // Identical content re-authored as a new proposal must not mint a
    // revision that revokes the standing approval.
    let noop = service
        .revise_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 0,
                milestone_id: Some(&defined.milestone_id),
                milestone_version: 1,
                base_revision_id: Some(&defined.id),
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                key: "verbatim-noop",
                name: "Verbatim milestone",
                check_id: Some("verbatim-check"),
            }),
            None,
        )
        .await
        .expect("verbatim revise no-ops");
    assert_eq!(noop.id, defined.id);
    assert_eq!(noop.lifecycle, "approved");
    let revision_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?",
    )
    .bind(&defined.milestone_id)
    .fetch_one(db.pool())
    .await
    .expect("revision count after verbatim revise");
    assert_eq!(revision_count, 1);
    let current_pointer: Option<String> = sqlx::query_scalar(
        "SELECT current_definition_revision_id FROM project_milestone WHERE id = ?",
    )
    .bind(&defined.milestone_id)
    .fetch_one(db.pool())
    .await
    .expect("current pointer after verbatim revise");
    assert_eq!(current_pointer.as_deref(), Some(defined.id.as_str()));

    // An actual content change still appends a new proposed revision.
    let milestone_version: i64 =
        sqlx::query_scalar("SELECT version FROM project_milestone WHERE id = ?")
            .bind(&defined.milestone_id)
            .fetch_one(db.pool())
            .await
            .expect("milestone version before changed revise");
    let changed = service
        .revise_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 0,
                milestone_id: Some(&defined.milestone_id),
                milestone_version,
                base_revision_id: Some(&defined.id),
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                key: "verbatim-changed",
                name: "Verbatim milestone changed",
                check_id: Some("verbatim-check"),
            }),
            None,
        )
        .await
        .expect("changed revise appends");
    assert_ne!(changed.id, defined.id);
    assert_eq!(changed.lifecycle, "proposed");

    // Re-proposing the now-current proposal verbatim is also a no-op.
    let duplicate_proposal = service
        .revise_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 0,
                milestone_id: Some(&defined.milestone_id),
                milestone_version: milestone_version + 1,
                base_revision_id: Some(&changed.id),
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                key: "verbatim-duplicate-proposal",
                name: "Verbatim milestone changed",
                check_id: Some("verbatim-check"),
            }),
            None,
        )
        .await
        .expect("duplicate proposal no-ops");
    assert_eq!(duplicate_proposal.id, changed.id);
    let final_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM project_milestone_revision WHERE milestone_id = ?",
    )
    .bind(&defined.milestone_id)
    .fetch_one(db.pool())
    .await
    .expect("final revision count");
    assert_eq!(final_count, 2);
}

#[tokio::test]
async fn definitions_bind_the_current_charter_when_the_author_omits_it() {
    let db = fixture().await;
    let service = ProjectMilestoneCommandService::new(Arc::clone(&db));

    // Before the Project is Charter-backed there is nothing to bind: this is
    // the shape of a definition authored while Charter setup was pending.
    let legacy = service
        .define_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 1,
                milestone_id: None,
                milestone_version: 1,
                base_revision_id: None,
                lifecycle: MilestoneDefinitionLifecycle::Draft,
                key: "unbound-define",
                name: "Unbound milestone",
                check_id: Some("unbound-check"),
            }),
            None,
        )
        .await
        .expect("define without a Charter");
    assert_eq!(charter_binding(&db, &legacy.id).await, None);

    sqlx::query(
        "UPDATE project SET current_charter_id = ?, current_charter_revision_id = ?,
                current_charter_version = 1 WHERE id = ?",
    )
    .bind(CHARTER_ID)
    .bind(CHARTER_REVISION_ID)
    .bind(PROJECT_ID)
    .execute(db.pool())
    .await
    .expect("Project becomes Charter-backed");

    // Readiness checks every validation against the definition's Charter and
    // validations are recorded under the Project's Charter, so an omitted
    // binding is filled rather than left to make the milestone unreleasable.
    let bound = service
        .define_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 0,
                milestone_id: None,
                milestone_version: 1,
                base_revision_id: None,
                lifecycle: MilestoneDefinitionLifecycle::Draft,
                key: "bound-define",
                name: "Bound milestone",
                check_id: Some("bound-check"),
            }),
            None,
        )
        .await
        .expect("define binds the current Charter");
    assert_eq!(
        charter_binding(&db, &bound.id).await.as_deref(),
        Some(CHARTER_REVISION_ID)
    );

    // An unbound definition has nothing to inherit; its next revision repairs it.
    let repaired = service
        .revise_milestone(
            definition_command(DefinitionCommandInput {
                project_version: 0,
                milestone_id: Some(&legacy.milestone_id),
                milestone_version: 2,
                base_revision_id: Some(&legacy.id),
                lifecycle: MilestoneDefinitionLifecycle::Proposed,
                key: "unbound-revise",
                name: "Unbound milestone, revised",
                check_id: Some("unbound-check"),
            }),
            None,
        )
        .await
        .expect("revise binds the current Charter");
    assert_ne!(repaired.id, legacy.id);
    assert_eq!(
        charter_binding(&db, &repaired.id).await.as_deref(),
        Some(CHARTER_REVISION_ID)
    );
}
