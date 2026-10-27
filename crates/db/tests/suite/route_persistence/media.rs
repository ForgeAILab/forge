use super::*;
use db::{CommentAuthorType, CreateTaskMedia, ProjectMediaAttachment, TaskMediaRepo};

async fn asset(db: &SqliteDb, id: &str, project_id: &str, created_at: &str, deleted: bool) {
    sqlx::query("INSERT INTO media_asset (id, project_id, display_filename, content_type, byte_size, storage_key, created_at, updated_at, deleted_at) VALUES (?, ?, 'proof.png', 'image/png', 4, ?, ?, ?, ?)")
        .bind(id).bind(project_id).bind(format!("media/{id}.png")).bind(created_at).bind(created_at)
        .bind(deleted.then_some(LATER)).execute(db.pool()).await.unwrap();
}

#[tokio::test]
async fn project_media_pages_preserve_keysets_deletion_and_project_scope() {
    let db = database().await;
    project(&db, "project", None).await;
    project(&db, "other", None).await;
    for (id, project_id, created_at, deleted) in [
        ("a", "project", NOW, false),
        ("b", "project", NOW, false),
        ("c", "project", LATER, false),
        ("deleted", "project", NOW, true),
        ("foreign", "other", NOW, false),
    ] {
        asset(&db, id, project_id, created_at, deleted).await;
    }
    assert_eq!(
        asset_page(&db, "project", None, 1).await.unwrap(),
        (vec!["a".into()], true)
    );
    assert_eq!(
        asset_page(&db, "project", Some((NOW.into(), "a".into())), 1)
            .await
            .unwrap(),
        (vec!["b".into()], true)
    );
    assert_eq!(
        asset_page(&db, "project", Some((NOW.into(), "b".into())), 1)
            .await
            .unwrap(),
        (vec!["c".into()], false)
    );
    assert_eq!(
        asset_page(&db, "project", Some((LATER.into(), "c".into())), 1)
            .await
            .unwrap(),
        (vec![], false)
    );
    assert_eq!(
        asset_page(&db, "missing", None, 1).await.unwrap(),
        (vec![], false)
    );
}

#[tokio::test]
async fn task_media_provenance_is_scoped_to_live_project_attachments() {
    let db = database().await;
    project(&db, "project", None).await;
    task(&db, "task", "project", "todo").await;
    TaskMediaRepo::create_media(
        &db,
        CreateTaskMedia {
            id: "task-media".into(),
            task_id: "task".into(),
            display_filename: "proof.png".into(),
            content_type: "image/png".into(),
            byte_size: 4,
            storage_key: "task/task-media__proof.png".into(),
            author_type: CommentAuthorType::User,
            author_id: None,
            author_name: "test".into(),
            created_at: NOW.into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        db.project_media_task_media_ids("project", "task-media")
            .await
            .unwrap(),
        ["task-media"]
    );
    assert!(db
        .project_media_task_media_ids("other", "task-media")
        .await
        .unwrap()
        .is_empty());
    assert!(db
        .project_media_task_media_ids("project", "missing")
        .await
        .unwrap()
        .is_empty());
    TaskMediaRepo::soft_delete_media(&db, "task-media", LATER)
        .await
        .unwrap();
    assert!(db
        .project_media_task_media_ids("project", "task-media")
        .await
        .unwrap()
        .is_empty());
}

async fn attachment(
    db: &SqliteDb,
    id: &str,
    kind: &str,
    milestone: &str,
    created_at: &str,
    deleted: bool,
) {
    sqlx::query("INSERT INTO project_media_attachment (
        id, project_id, asset_id, attachment_kind, milestone_id, source_task_id,
        source_execution_id, source_validation_id, source_task_version, source_context_digest,
        source_definition_revision_id, acceptance_check_ids_json, caption, evidence_kind,
        checksum, availability, project_url, author_type, author_id, authorization_json,
        version, created_at, deleted_at, updated_at)
        VALUES (?, 'project', 'asset', ?, ?, 'task', 'execution', 'validation', 7, 'context',
        'definition', '[\"check\"]', 'proof', 'screenshot', 'checksum', 'redacted',
        '/api/v1/projects/project/media/asset', 'user', 'author', '{\"action\":\"attach\"}', 4, ?, ?, ?)")
        .bind(id).bind(kind).bind(milestone).bind(created_at).bind(deleted.then_some(LATER)).bind(created_at)
        .execute(db.pool()).await.unwrap();
}

#[tokio::test]
async fn evidence_pages_and_lookup_keep_scope_filters_and_decode_all_provenance() {
    let db = database().await;
    project(&db, "project", None).await;
    project(&db, "other", None).await;
    task(&db, "task", "project", "todo").await;
    sqlx::query("INSERT INTO execution (id, task_id, role, status, created_at, updated_at) VALUES ('execution', 'task', 'coder', 'completed', ?, ?)")
        .bind(NOW).bind(LATER).execute(db.pool()).await.unwrap();
    asset(&db, "asset", "project", NOW, false).await;
    for (id, sequence) in [("milestone", 1), ("other-milestone", 2)] {
        sqlx::query("INSERT INTO project_milestone (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at) VALUES (?, 'project', ?, ?, 'planned', ?, ?)")
            .bind(id).bind(sequence).bind(format!("M{sequence:03}")).bind(NOW).bind(NOW).execute(db.pool()).await.unwrap();
    }
    attachment(&db, "a", "evidence", "milestone", NOW, false).await;
    attachment(&db, "b", "evidence", "milestone", NOW, false).await;
    attachment(&db, "c", "evidence", "milestone", LATER, false).await;
    attachment(&db, "deleted", "evidence", "milestone", NOW, true).await;
    attachment(&db, "wrong-kind", "project", "milestone", NOW, false).await;
    attachment(
        &db,
        "wrong-milestone",
        "evidence",
        "other-milestone",
        NOW,
        false,
    )
    .await;
    let expected = ProjectMediaAttachment {
        id: "a".into(),
        project_id: "project".into(),
        asset_id: "asset".into(),
        attachment_kind: "evidence".into(),
        task_media_id: None,
        task_id: None,
        milestone_id: Some("milestone".into()),
        milestone_check_id: None,
        source_task_id: Some("task".into()),
        source_execution_id: Some("execution".into()),
        source_validation_id: Some("validation".into()),
        source_task_version: Some(7),
        source_context_digest: Some("context".into()),
        source_definition_revision_id: Some("definition".into()),
        acceptance_check_ids_json: "[\"check\"]".into(),
        caption: Some("proof".into()),
        evidence_kind: Some("screenshot".into()),
        checksum: Some("checksum".into()),
        availability: "redacted".into(),
        project_url: Some("/api/v1/projects/project/media/asset".into()),
        author_type: "user".into(),
        author_id: Some("author".into()),
        authorization_json: "{\"action\":\"attach\"}".into(),
        version: 4,
        created_at: NOW.into(),
        deleted_at: None,
        updated_at: NOW.into(),
    };
    let (first, has_more) = evidence_page(&db, "project", "milestone", None, 1)
        .await
        .unwrap();
    assert!(has_more);
    assert_eq!(first.as_slice(), std::slice::from_ref(&expected));
    assert_eq!(
        db.project_evidence_by_id("project", "milestone", "a")
            .await
            .unwrap(),
        Some(expected)
    );
    let (second, has_more) = evidence_page(
        &db,
        "project",
        "milestone",
        Some((NOW.into(), "a".into())),
        1,
    )
    .await
    .unwrap();
    assert!(has_more);
    assert_eq!(second[0].id, "b");
    let (third, has_more) = evidence_page(
        &db,
        "project",
        "milestone",
        Some((NOW.into(), "b".into())),
        1,
    )
    .await
    .unwrap();
    assert!(!has_more);
    assert_eq!(third[0].id, "c");
    assert_eq!(
        evidence_page(&db, "other", "milestone", None, 1)
            .await
            .unwrap(),
        (vec![], false)
    );
    for (project, milestone, id) in [
        ("other", "milestone", "a"),
        ("project", "other-milestone", "a"),
        ("project", "milestone", "deleted"),
        ("project", "milestone", "wrong-kind"),
        ("project", "milestone", "missing"),
    ] {
        assert!(db
            .project_evidence_by_id(project, milestone, id)
            .await
            .unwrap()
            .is_none());
    }

    sqlx::query("UPDATE project_media_attachment SET caption = x'80' WHERE id = 'c'")
        .execute(db.pool())
        .await
        .unwrap();
    let (rows, has_more) = db
        .project_evidence_page("project", "milestone", None, 2)
        .await
        .unwrap();
    assert!(has_more);
    assert_eq!(rows.len(), 2);
    for row in rows {
        row.into_attachment()
            .expect("the corrupt sentinel is not decoded");
    }
    let (rows, has_more) = db
        .project_evidence_page("project", "milestone", None, 3)
        .await
        .unwrap();
    assert!(!has_more);
    let mut rows = rows.into_iter();
    rows.next().unwrap().into_attachment().unwrap();
    rows.next().unwrap().into_attachment().unwrap();
    assert!(rows.next().unwrap().into_attachment().is_err());
}

async fn asset_page(
    db: &SqliteDb,
    project: &str,
    cursor: Option<(String, String)>,
    limit: i64,
) -> db::Result<(Vec<String>, bool)> {
    let (rows, has_more) = db
        .project_media_asset_id_page(project, cursor, limit)
        .await?;
    let ids = rows
        .into_iter()
        .map(|row| row.asset_id())
        .collect::<db::Result<_>>()?;
    Ok((ids, has_more))
}

async fn evidence_page(
    db: &SqliteDb,
    project: &str,
    milestone: &str,
    cursor: Option<(String, String)>,
    limit: i64,
) -> db::Result<(Vec<ProjectMediaAttachment>, bool)> {
    let (rows, has_more) = db
        .project_evidence_page(project, milestone, cursor, limit)
        .await?;
    let attachments = rows
        .into_iter()
        .map(|row| row.into_attachment())
        .collect::<db::Result<_>>()?;
    Ok((attachments, has_more))
}
