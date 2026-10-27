use super::*;

#[tokio::test]
async fn document_read_pages_keep_scope_keysets_and_immutable_revision_rows() {
    let db = database().await;
    project(&db, "read-project").await;
    project(&db, "other-project").await;
    for (scope, id) in [
        ("read-project", "doc-a"),
        ("read-project", "doc-b"),
        ("read-project", "doc-c"),
        ("other-project", "doc-z"),
    ] {
        ProjectOrchestrationRepo::create_project_document(
            &db,
            document_command(scope, id, "unused", None).document,
        )
        .await
        .unwrap();
    }
    let page = db
        .project_document_id_page("read-project", None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2, "fetch limit + 1");
    assert_eq!(page[0].id().unwrap(), "doc-c");
    assert_eq!(page[1].id().unwrap(), "doc-b");
    let timestamp = "2026-08-20T00:00:00.000Z";
    let page = db
        .project_document_id_page("read-project", Some((timestamp.into(), "doc-b".into())), 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id().unwrap(), "doc-a");
    assert_eq!(
        db.project_document_count_before("read-project", timestamp, "doc-b")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        db.project_document_count_before("read-project", timestamp, "doc-a")
            .await
            .unwrap(),
        0
    );

    let revision1 = ProjectOrchestrationRepo::create_project_document_revision(
        &db,
        document_command("read-project", "doc-a", "revision-1", None).revision,
    )
    .await
    .unwrap();
    let mut next = document_command("read-project", "doc-a", "revision-2", None).revision;
    next.expected_document_version = 2;
    next.base_revision = 1;
    next.base_revision_id = Some(revision1.id.clone());
    let revision2 = ProjectOrchestrationRepo::create_project_document_revision(&db, next)
        .await
        .unwrap();
    let page = db
        .project_document_revision_page("doc-a", None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    let mut records = page.into_iter().map(|row| row.into_revision().unwrap());
    assert_eq!(records.next().unwrap(), revision2);
    assert_eq!(records.next().unwrap(), revision1);
    let page = db
        .project_document_revision_page("doc-a", Some((2, "revision-2".into())), 1)
        .await
        .unwrap();
    assert_eq!(
        page.into_iter().next().unwrap().into_revision().unwrap(),
        revision1
    );
    assert!(db
        .project_document_revision_page("doc-z", None, 1)
        .await
        .unwrap()
        .is_empty());
    let freshness = db
        .overview_document_freshness("read-project")
        .await
        .unwrap();
    assert_eq!(freshness.len(), 3);
    let doc = freshness
        .iter()
        .find(|row| row.id().unwrap() == "doc-a")
        .unwrap();
    assert_eq!(
        doc.current_draft_revision_id().unwrap().as_deref(),
        Some("revision-2")
    );
    assert_eq!(
        doc.working_digest().unwrap().as_deref(),
        Some("content-digest")
    );
    assert!(db
        .approved_project_document_pointers("read-project")
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn decision_reads_keep_full_project_replacements_and_page_boundaries() {
    let db = database().await;
    project(&db, "read-project").await;
    project(&db, "other-project").await;
    for (scope, id) in [
        ("read-project", "candidate-a"),
        ("read-project", "candidate-b"),
        ("other-project", "candidate-z"),
    ] {
        sqlx::query(
            "INSERT INTO project_decision_candidate
            (id, project_id, lifecycle, question, context_json, options_json,
             selected_outcome, rationale, principal_type, principal_id, source_refs_json,
             expected_project_version, created_at, updated_at)
            VALUES (?, ?, 'proposed', 'Choose?', '{}', '[\"a\",\"b\"]', 'a', 'reason',
                    'user', 'user-1', '[]', 1, 'now', 'now')",
        )
        .bind(id)
        .bind(scope)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let page = db
        .project_decision_candidate_page("read-project", None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    let candidate = page.into_iter().next().unwrap().into_candidate().unwrap();
    assert_eq!(candidate.id, "candidate-b");
    assert_eq!(
        Some(candidate),
        ProjectOrchestrationRepo::get_project_decision_candidate(&db, "candidate-b")
            .await
            .unwrap()
    );
    let page = db
        .project_decision_candidate_page(
            "read-project",
            Some(("now".into(), "candidate-b".into())),
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        page.into_iter()
            .next()
            .unwrap()
            .into_candidate()
            .unwrap()
            .id,
        "candidate-a"
    );
    let pending = db.overview_pending_decisions("read-project").await.unwrap();
    assert_eq!(
        pending
            .iter()
            .map(|row| row.id().unwrap())
            .collect::<Vec<_>>(),
        ["candidate-a", "candidate-b"]
    );

    for (scope, id, state, supersedes) in [
        ("read-project", "decision-a", "active", None),
        (
            "read-project",
            "decision-b",
            "invalidated",
            Some("decision-a"),
        ),
        ("other-project", "decision-z", "active", None),
    ] {
        sqlx::query(
            "INSERT INTO project_decision
            (id, project_id, state, decision_class, question, selected_outcome, rationale,
             principal_type, principal_id, authority_basis, authorization_action,
             explicit_event, authorization_occurred_at, supersedes_decision_id, created_at)
            VALUES (?, ?, ?, 'project_implementation', 'Choose?', 'a', 'reason',
                    'user', 'user-1', 'explicit', 'approve', 'event', 'now', ?, 'now')",
        )
        .bind(id)
        .bind(scope)
        .bind(state)
        .bind(supersedes)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let page = db
        .project_decision_page("read-project", None, 1)
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(
        page.into_iter().next().unwrap().into_decision().unwrap().id,
        "decision-b"
    );
    let page = db
        .project_decision_page("read-project", Some(("now".into(), "decision-b".into())), 1)
        .await
        .unwrap();
    let decision = page.into_iter().next().unwrap().into_decision().unwrap();
    assert_eq!(decision.id, "decision-a");
    assert_eq!(
        Some(decision),
        db.project_decision_detail_source("read-project", "decision-a")
            .await
            .unwrap()
            .map(|(record, replacement)| {
                assert_eq!(replacement.as_deref(), Some("invalidated"));
                record
            })
    );
    assert!(db
        .project_decision_detail_source("other-project", "decision-a")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        db.project_decision_replacement_state("read-project", "decision-a")
            .await
            .unwrap()
            .as_deref(),
        Some("invalidated")
    );
    assert!(db
        .project_decision_replacement_state("other-project", "decision-a")
        .await
        .unwrap()
        .is_none());
    let decisions = db.overview_decisions("read-project").await.unwrap();
    assert_eq!(
        decisions
            .iter()
            .map(|row| row.id().unwrap())
            .collect::<Vec<_>>(),
        ["decision-b", "decision-a"]
    );
    let replacements = db
        .overview_decision_replacements("read-project")
        .await
        .unwrap();
    assert_eq!(replacements.len(), 1);
    assert_eq!(
        replacements[0].supersedes_decision_id().unwrap().as_deref(),
        Some("decision-a")
    );
    assert_eq!(replacements[0].state().unwrap(), "invalidated");
    sqlx::query("ALTER TABLE project_decision RENAME COLUMN created_at TO unavailable_created_at")
        .execute(db.pool())
        .await
        .unwrap();
    assert!(
        db.project_decision_detail_source("read-project", "missing-decision")
            .await
            .unwrap()
            .is_none(),
        "a missing record must return before querying its successor"
    );
}

#[tokio::test]
async fn overview_and_charter_reads_handle_missing_canonical_sources() {
    let db = database().await;
    project(&db, "read-project").await;
    assert!(db
        .overview_current_charter("missing-revision", "missing-charter", "read-project")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .overview_active_milestones("read-project")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .overview_milestone_revision("missing-revision", "missing-milestone")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .overview_acceptance_checks("read-project", "missing-milestone", "missing-revision")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .overview_milestone_tasks("read-project", "missing-milestone")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .overview_project_tasks("read-project")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .overview_milestone_checks("read-project", "missing-milestone")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .overview_project_checks("read-project")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .overview_evidence("read-project")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .overview_latest_readiness("read-project", "missing-milestone")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .overview_releases("read-project")
        .await
        .unwrap()
        .is_empty());
    let expected_watermark: String = sqlx::query_scalar(
        "SELECT id FROM domain_event WHERE scope_type = 'project' AND scope_id = ? ORDER BY sequence DESC LIMIT 1",
    )
    .bind("read-project")
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        db.overview_event_watermark("read-project")
            .await
            .unwrap()
            .unwrap(),
        expected_watermark
    );
    assert!(db
        .overview_event_watermark("missing-project")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        db.overview_failed_task_count("read-project").await.unwrap(),
        0
    );
    assert!(!db
        .overview_reconciliation_required("read-project")
        .await
        .unwrap());
    assert!(db
        .current_project_milestone_pointers("read-project")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .active_project_binding_pointers("read-project")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .latest_project_charter_id("read-project", "missing-account")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .latest_project_charter_approval_id("missing-charter")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .project_charter_approval_id_by_key("missing-key")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .project_charter_genesis_session_id("missing-charter")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .current_project_operating_skill_revision_id("missing-skill")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .current_project_operating_skill_revision_id("forge.project.orchestration/v1")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn charter_reads_keep_account_scope_and_distinguish_null_session_from_missing_row() {
    let db = database().await;
    db::UserRepo::create_user(
        &db,
        &db::User {
            id: "read-account".into(),
            email: "read@example.test".into(),
            password_hash: "hash".into(),
            display_name: None,
            is_admin: false,
            created_at: "now".into(),
            updated_at: "now".into(),
        },
    )
    .await
    .unwrap();
    project(&db, "read-project").await;
    sqlx::query("UPDATE project SET owner_id = 'read-account' WHERE id = 'read-project'")
        .execute(db.pool())
        .await
        .unwrap();
    ProjectOrchestrationRepo::create_project_charter(
        &db,
        db::CreateProjectCharter {
            id: "charter".into(),
            account_id: "read-account".into(),
            genesis_session_id: None,
            project_mode: "compact".into(),
            maturity: "mvp".into(),
            created_at: "now".into(),
            updated_at: "now".into(),
        },
    )
    .await
    .unwrap();
    sqlx::query("UPDATE project_charter SET project_id = 'read-project' WHERE id = 'charter'")
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(
        db.latest_project_charter_id("read-project", "read-account")
            .await
            .unwrap()
            .as_deref(),
        Some("charter")
    );
    assert!(db
        .latest_project_charter_id("read-project", "other-account")
        .await
        .unwrap()
        .is_none());
    assert!(db
        .latest_project_charter_id("other-project", "read-account")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        db.project_charter_genesis_session_id("charter")
            .await
            .unwrap(),
        Some(None)
    );
    assert_eq!(
        db.project_charter_genesis_session_id("missing-charter")
            .await
            .unwrap(),
        None
    );
}
