use db::{
    create_sqlite_pool, now_rfc3339, run_migrations, BeginProjectMediaUpload, CommentAuthorType,
    CreateProjectMediaAsset, CreateProjectMediaAttachment, CreateProjectMediaAttachmentMutation,
    CreateProjectReleaseMediaPin, CreateTaskMedia, ProjectMediaTombstone, SharedMediaRepo,
    SqliteDb, TaskMediaRepo,
};
use sqlx::SqlitePool;

async fn fixture() -> (SqliteDb, String, String) {
    let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
    run_migrations(&pool).await.expect("migrations");
    let db = SqliteDb::new(pool);
    let project_id = "project-media".to_owned();
    let task_id = "task-media".to_owned();
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO user (id, email, password_hash, display_name, created_at, updated_at)
         VALUES ('user-media', 'media@example.test', 'test', 'Media Tester', ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("user");
    sqlx::query(
        "INSERT INTO project (id, name, owner_id, settings, created_at, updated_at)
         VALUES (?, ?, 'user-media', '{}', ?, ?)",
    )
    .bind(&project_id)
    .bind("Media Project")
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("project");
    sqlx::query(
        "INSERT INTO repo (id, project_id, name, remote_url, local_path, work_mode, default_branch, created_at, updated_at)
         VALUES ('repo-media', ?, 'Media Repo', '/tmp/media-repo', '/tmp/media-repo', 'direct_merge', 'main', ?, ?)",
    )
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("repo");
    sqlx::query(
        "INSERT INTO task (id, project_id, repo_id, title, task_type, status, created_at, updated_at)
         VALUES (?, ?, 'repo-media', 'Media Task', 'task', 'todo', ?, ?)",
    )
    .bind(&task_id)
    .bind(&project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("task");
    (db, project_id, task_id)
}

async fn upload(db: &SqliteDb, task_id: &str, id: &str) -> db::TaskMedia {
    let media = TaskMediaRepo::create_media(
        db,
        CreateTaskMedia {
            id: id.to_owned(),
            task_id: task_id.to_owned(),
            display_filename: "proof.png".to_owned(),
            content_type: "image/png".to_owned(),
            byte_size: 4,
            storage_key: format!("{task_id}/{id}__proof.png"),
            author_type: CommentAuthorType::User,
            author_id: Some("user-media".to_owned()),
            author_name: "Tester".to_owned(),
            created_at: now_rfc3339(),
        },
    )
    .await
    .expect("media");
    // The focused repository tests model a legacy row whose unchanged bytes
    // have already gone through checksum reconciliation. Production callers
    // perform that same CAS only after hashing the stored bytes.
    SharedMediaRepo::set_media_asset_checksum(
        db,
        &media.id,
        media.byte_size,
        "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
        &now_rfc3339(),
    )
    .await
    .expect("legacy media checksum reconciliation");
    media
}

async fn create_release(db: &SqliteDb, project_id: &str) {
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at)
         VALUES ('milestone-media', ?, 1, 'M001', 'released', ?, ?)",
    )
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone");
    sqlx::query(
        "INSERT INTO project_milestone_revision
         (id, milestone_id, revision, base_revision, lifecycle, outcome,
          included_scope_json, excluded_scope_json, document_revisions_json,
          task_selection_json, dependencies_json, risks_json, acceptance_checks_json,
          evidence_requirements_json, known_issues_json, change_summary, schema_version,
          render_version, rendered_view, content_digest, rendered_digest,
          author_type, source_refs_json, created_at)
         VALUES ('milestone-media-r1', 'milestone-media', 1, 0, 'approved', 'media proof',
                 '[]', '[]', '[]', '[]', '[]', '[]', '[]', '[]', '[]', '',
                 'test', 'test', 'media milestone', 'digest', 'rendered', 'user', '[]', ?)",
    )
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("milestone revision");
    sqlx::query(
        "INSERT INTO project_charter
         (id, account_id, project_id, project_mode, maturity, lifecycle, created_at, updated_at)
         VALUES ('charter-media', 'user-media', ?, 'standard', 'mvp', 'attached', ?, ?)",
    )
    .bind(project_id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter");
    sqlx::query(
        "INSERT INTO project_charter_revision
         (id, charter_id, revision, lifecycle, schema_version, render_version,
          content_json, rendered_view, author_type, author_id, content_digest,
          rendered_digest, created_at)
         VALUES ('charter-media-r1', 'charter-media', 1, 'approved', 'test', 'test',
                 '{}', 'media charter', 'user', 'user-media', 'charter-digest',
                 'charter-rendered-digest', ?)",
    )
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("charter revision");
    sqlx::query(
        "UPDATE project_charter
         SET current_approved_revision_id = ?
         WHERE id = 'charter-media'",
    )
    .bind("charter-media-r1")
    .execute(db.pool())
    .await
    .expect("charter pointer");
    sqlx::query(
        "INSERT INTO project_readiness_snapshot
         (id, project_id, milestone_id, definition_revision_id,
          event_watermark, outcome,
          computing_policy_revision, readiness_digest, principal_type, principal_id,
          authorization_basis, authorization_action, authorization_occurred_at,
          expected_milestone_version, explicit_event,
          idempotency_key, created_at)
         VALUES ('readiness-media', ?, 'milestone-media', 'milestone-media-r1',
                 'event-watermark-media', 'ready',
                 'test', 'readiness-digest', 'user', 'user-media', 'test',
                 'project.milestone.readiness.evaluate', '2026-08-13T00:00:00Z', 1,
                 'event-media', 'idem-readiness-media', ?)",
    )
    .bind(project_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("readiness");
    sqlx::query(
        "INSERT INTO project_release
         (id, project_id, milestone_id, release_sequence, release_revision,
          release_identifier, milestone_revision_id, readiness_snapshot_id,
          readiness_digest,
          releasing_principal_type, releasing_principal_id,
          authorization_basis, authorization_action, authorization_occurred_at,
          explicit_event, schema_version, snapshot_digest,
          idempotency_key, created_at)
         VALUES ('release-media', ?, 'milestone-media', 1, 1, 'M001-r1',
                 'milestone-media-r1', 'readiness-media', 'readiness-digest',
                 'user', 'user-media', 'test', 'project.release.create',
                 '2026-08-13T00:00:00Z', 'event-release-media', 'test',
                 'snapshot-digest', 'idem-release-media', ?)",
    )
    .bind(project_id)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("release");
}

#[tokio::test]
async fn unreferenced_task_media_is_claimed_and_gc_is_idempotent() {
    let (db, _project_id, task_id) = fixture().await;
    let media = upload(&db, &task_id, "asset-gc").await;
    let deleted_at = now_rfc3339();
    TaskMediaRepo::soft_delete_media(&db, &media.id, &deleted_at)
        .await
        .expect("soft delete");

    let asset = SharedMediaRepo::get_media_asset(&db, &media.id)
        .await
        .expect("asset")
        .expect("asset row");
    assert_eq!(asset.gc_state, "gc_candidate");
    let candidates = SharedMediaRepo::claim_media_gc_candidates(
        &db,
        &deleted_at,
        "worker-gc-1",
        "9999-12-31T00:00:00Z",
        10,
    )
    .await
    .expect("claim");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].gc_state, "gc_queued");
    assert_eq!(candidates[0].gc_lease_owner.as_deref(), Some("worker-gc-1"));
    assert_eq!(
        candidates[0].gc_lease_expires_at.as_deref(),
        Some("9999-12-31T00:00:00Z")
    );
    let claimed_version = candidates[0].version;

    let completed = SharedMediaRepo::complete_media_gc(
        &db,
        &media.id,
        "worker-gc-1",
        claimed_version,
        &deleted_at,
    )
    .await
    .expect("complete")
    .expect("completed asset");
    assert_eq!(completed.gc_state, "deleted");
    assert_eq!(completed.availability, "purged");
    assert!(SharedMediaRepo::claim_media_gc_candidates(
        &db,
        &deleted_at,
        "worker-gc-1",
        "9999-12-31T00:00:00Z",
        10,
    )
    .await
    .expect("replay claim")
    .is_empty());
    assert!(matches!(
        SharedMediaRepo::complete_media_gc(
            &db,
            &media.id,
            "worker-gc-1",
            claimed_version,
            &deleted_at,
        )
        .await,
        Err(db::DbError::VersionConflict)
    ));
}

#[tokio::test]
async fn active_project_evidence_blocks_task_media_gc_until_removed() {
    let (db, project_id, task_id) = fixture().await;
    let media = upload(&db, &task_id, "asset-evidence").await;
    let now = now_rfc3339();
    let attached = SharedMediaRepo::create_project_media_attachment(
        &db,
        CreateProjectMediaAttachment {
            id: "evidence-attachment".to_owned(),
            project_id: project_id.clone(),
            asset_id: media.id.clone(),
            attachment_kind: "evidence".to_owned(),
            task_media_id: None,
            task_id: None,
            milestone_id: None,
            milestone_check_id: None,
            source_task_id: Some(task_id.clone()),
            source_execution_id: None,
            source_validation_id: None,
            source_task_version: None,
            source_context_digest: None,
            source_definition_revision_id: None,
            acceptance_check_ids_json: "[\"check-1\"]".to_owned(),
            caption: Some("proof".to_owned()),
            evidence_kind: Some("screenshot".to_owned()),
            checksum: Some(
                "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08".to_owned(),
            ),
            availability: "available".to_owned(),
            project_url: Some(format!("/api/v1/projects/{project_id}/media/{}", media.id)),
            author_type: "user".to_owned(),
            author_id: Some("user-media".to_owned()),
            authorization_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("evidence attachment");
    assert_eq!(attached.source_task_version, Some(1));
    assert!(attached.source_context_digest.is_some());

    TaskMediaRepo::soft_delete_media(&db, &media.id, &now)
        .await
        .expect("soft delete task media");
    let asset = SharedMediaRepo::reconcile_media_asset(&db, &media.id, &now)
        .await
        .expect("reconcile")
        .expect("asset");
    assert_eq!(asset.gc_state, "referenced");
    assert!(SharedMediaRepo::claim_media_gc_candidates(
        &db,
        &now,
        "worker-evidence",
        "9999-12-31T00:00:00Z",
        10,
    )
    .await
    .expect("claim with evidence")
    .is_empty());

    SharedMediaRepo::soft_delete_project_media_attachment(&db, "evidence-attachment", &now)
        .await
        .expect("remove evidence");
    assert_eq!(
        SharedMediaRepo::get_media_asset(&db, &media.id)
            .await
            .expect("asset")
            .expect("asset row")
            .gc_state,
        "gc_candidate"
    );
}

#[tokio::test]
async fn immutable_release_pin_keeps_asset_referenced_after_task_cleanup() {
    let (db, project_id, task_id) = fixture().await;
    create_release(&db, &project_id).await;
    let media = upload(&db, &task_id, "asset-pinned").await;
    let now = now_rfc3339();
    let project_url = format!("/api/v1/projects/{project_id}/media/{}", media.id);
    SharedMediaRepo::create_project_media_attachment(
        &db,
        CreateProjectMediaAttachment {
            id: "pinned-evidence".to_owned(),
            project_id: project_id.clone(),
            asset_id: media.id.clone(),
            attachment_kind: "evidence".to_owned(),
            task_media_id: None,
            task_id: None,
            milestone_id: None,
            milestone_check_id: None,
            source_task_id: Some(task_id.clone()),
            source_execution_id: None,
            source_validation_id: None,
            source_task_version: None,
            source_context_digest: None,
            source_definition_revision_id: None,
            acceptance_check_ids_json: "[]".to_owned(),
            caption: Some("release proof".to_owned()),
            evidence_kind: Some("screenshot".to_owned()),
            checksum: Some(
                "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08".to_owned(),
            ),
            availability: "available".to_owned(),
            project_url: Some(project_url.clone()),
            author_type: "user".to_owned(),
            author_id: Some("user-media".to_owned()),
            authorization_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("pinned evidence attachment");
    let pin = SharedMediaRepo::create_project_release_media_pin(
        &db,
        CreateProjectReleaseMediaPin {
            id: "pin-media".to_owned(),
            project_id: project_id.clone(),
            release_id: "release-media".to_owned(),
            asset_id: media.id.clone(),
            attachment_id: None,
            legacy_task_media_id: Some(media.id.clone()),
            asset_checksum: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
                .to_owned(),
            attachment_digest: "attachment-digest".to_owned(),
            availability: "available".to_owned(),
            pin_digest: "pin-digest".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("pin");
    assert_eq!(pin.asset_id, media.id);

    // A release-pin retry with a NULL attachment identity must return the
    // original row (SQLite's ordinary UNIQUE constraint treats NULLs as
    // distinct).  A conflicting immutable payload is rejected instead of
    // silently returning that unrelated original.
    let replay = SharedMediaRepo::create_project_release_media_pin(
        &db,
        CreateProjectReleaseMediaPin {
            id: "pin-media-retry".to_owned(),
            project_id: project_id.clone(),
            release_id: "release-media".to_owned(),
            asset_id: media.id.clone(),
            attachment_id: None,
            legacy_task_media_id: Some(media.id.clone()),
            asset_checksum: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
                .to_owned(),
            attachment_digest: "attachment-digest".to_owned(),
            availability: "available".to_owned(),
            pin_digest: "pin-digest".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("idempotent NULL-attachment pin replay");
    assert_eq!(replay.id, pin.id);
    let duplicate_null_pin = sqlx::query(
        "INSERT INTO project_release_media_pin
            (id, project_id, release_id, asset_id, asset_checksum, attachment_digest,
             availability, pin_digest, created_at)
         VALUES ('pin-media-raw-duplicate', ?, 'release-media', ?,
                 '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
                 'attachment-digest', 'available', 'pin-digest', ?)",
    )
    .bind(&project_id)
    .bind(&media.id)
    .bind(&now)
    .execute(db.pool())
    .await;
    assert!(
        duplicate_null_pin.is_err(),
        "NULL attachment identity must be unique in SQLite"
    );
    let conflict = SharedMediaRepo::create_project_release_media_pin(
        &db,
        CreateProjectReleaseMediaPin {
            id: "pin-media-conflict".to_owned(),
            project_id: project_id.clone(),
            release_id: "release-media".to_owned(),
            asset_id: media.id.clone(),
            attachment_id: None,
            legacy_task_media_id: Some(media.id.clone()),
            asset_checksum: "different-checksum".to_owned(),
            attachment_digest: "attachment-digest".to_owned(),
            availability: "available".to_owned(),
            pin_digest: "pin-digest".to_owned(),
            created_at: now.clone(),
        },
    )
    .await;
    assert!(matches!(conflict, Err(db::DbError::Check(_))));

    TaskMediaRepo::soft_delete_media(&db, &media.id, &now)
        .await
        .expect("soft delete task media");
    assert!(TaskMediaRepo::get_media_by_id(&db, &media.id, false)
        .await
        .expect("task media lookup")
        .is_none());
    let retained_project_url: String = sqlx::query_scalar(
        "SELECT project_url FROM project_media_attachment
         WHERE id = 'pinned-evidence' AND deleted_at IS NULL",
    )
    .fetch_one(db.pool())
    .await
    .expect("retained Project evidence URL");
    assert_eq!(retained_project_url, project_url);
    SharedMediaRepo::soft_delete_project_media_attachment(&db, "pinned-evidence", &now)
        .await
        .expect("remove non-pinned evidence attachment");
    let asset = SharedMediaRepo::get_media_asset(&db, &media.id)
        .await
        .expect("asset")
        .expect("asset row");
    assert_eq!(asset.gc_state, "referenced");
    assert_eq!(
        SharedMediaRepo::list_project_release_media_pins(&db, "release-media")
            .await
            .expect("pins")
            .len(),
        1
    );
    assert!(SharedMediaRepo::claim_media_gc_candidates(
        &db,
        &now,
        "worker-pinned",
        "9999-12-31T00:00:00Z",
        10,
    )
    .await
    .expect("claim pinned")
    .is_empty());
}

#[tokio::test]
async fn project_upload_staging_is_replay_safe_and_version_bound() {
    let (db, project_id, _task_id) = fixture().await;
    let now = now_rfc3339();
    let pending = SharedMediaRepo::begin_project_media_upload(
        &db,
        BeginProjectMediaUpload {
            project_id: project_id.clone(),
            idempotency_key: "upload-replay".to_owned(),
            mutation_fingerprint: "fingerprint-1".to_owned(),
            expected_project_version: 1,
            asset_id: "asset-project-upload".to_owned(),
            final_storage_key: "projects/project-media/asset-project-upload__proof.png".to_owned(),
            staging_storage_key:
                "pending/projects/project-media/asset-project-upload__proof.png.uploading"
                    .to_owned(),
            display_filename: "proof.png".to_owned(),
            content_type: "image/png".to_owned(),
            byte_size: 8,
            checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("pending upload");
    assert_eq!(pending.status, "pending");

    let quarantined = SharedMediaRepo::create_project_media_asset(
        &db,
        CreateProjectMediaAsset {
            id: pending.asset_id.clone(),
            project_id: project_id.clone(),
            display_filename: pending.display_filename.clone(),
            content_type: pending.content_type.clone(),
            byte_size: pending.byte_size,
            storage_key: pending.final_storage_key.clone(),
            checksum: pending.checksum.clone(),
            idempotency_key: pending.idempotency_key.clone(),
            mutation_fingerprint: pending.mutation_fingerprint.clone(),
            expected_project_version: pending.expected_project_version,
            actor_type: "user".to_owned(),
            actor_id: Some("user-media".to_owned()),
            authorization_event_id: "auth-upload".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("metadata commit");
    assert_eq!(quarantined.availability, "quarantined");
    let available =
        SharedMediaRepo::finalize_project_media_upload(&db, &project_id, &pending.asset_id, &now)
            .await
            .expect("finalize");
    assert_eq!(available.availability, "available");

    let replay = SharedMediaRepo::begin_project_media_upload(
        &db,
        BeginProjectMediaUpload {
            project_id: project_id.clone(),
            idempotency_key: pending.idempotency_key.clone(),
            mutation_fingerprint: pending.mutation_fingerprint.clone(),
            expected_project_version: pending.expected_project_version,
            asset_id: "a-new-server-id-is-ignored".to_owned(),
            final_storage_key: "a-new-storage-key-is-ignored".to_owned(),
            staging_storage_key: "a-new-staging-key-is-ignored".to_owned(),
            display_filename: pending.display_filename.clone(),
            content_type: pending.content_type.clone(),
            byte_size: pending.byte_size,
            checksum: pending.checksum.clone(),
            created_at: now.clone(),
        },
    )
    .await
    .expect("exact replay");
    assert_eq!(replay.status, "finalized");
    assert_eq!(replay.asset_id, pending.asset_id);

    let conflict = SharedMediaRepo::begin_project_media_upload(
        &db,
        BeginProjectMediaUpload {
            project_id,
            idempotency_key: pending.idempotency_key,
            mutation_fingerprint: "different-fingerprint".to_owned(),
            expected_project_version: 1,
            asset_id: "asset-conflict".to_owned(),
            final_storage_key: "conflict".to_owned(),
            staging_storage_key: "conflict-staging".to_owned(),
            display_filename: pending.display_filename,
            content_type: pending.content_type,
            byte_size: pending.byte_size,
            checksum: pending.checksum,
            created_at: now,
        },
    )
    .await;
    assert!(matches!(conflict, Err(db::DbError::IdempotencyConflict)));
}

#[tokio::test]
async fn project_media_tombstone_replay_compares_complete_user_receipt() {
    let (db, project_id, task_id) = fixture().await;
    let media = upload(&db, &task_id, "asset-tombstone-replay").await;
    let media_asset = SharedMediaRepo::get_media_asset(&db, &media.id)
        .await
        .expect("shared media asset")
        .expect("shared media asset row");
    let created_at = now_rfc3339();
    let original = ProjectMediaTombstone {
        asset_id: media.id.clone(),
        project_id: project_id.clone(),
        expected_version: media_asset.version,
        idempotency_key: "media-tombstone-replay".to_owned(),
        mutation_fingerprint: "media-tombstone-request".to_owned(),
        target_availability: "redacted".to_owned(),
        principal_type: "user".to_owned(),
        principal_id: "user-media".to_owned(),
        authorization_basis: "explicit_user_authorization".to_owned(),
        authorization_action: "project.media.redact".to_owned(),
        authorization_occurred_at: "2026-08-13T00:00:00Z".to_owned(),
        authorization_event_id: "media-tombstone-event".to_owned(),
        authorization_json: serde_json::json!({
            "principal": {"kind": "user", "id": "user-media"},
            "authorization_basis": "explicit_user_authorization",
            "action": "project.media.redact",
            "event_id": "media-tombstone-event",
            "occurred_at": "2026-08-13T00:00:00Z"
        })
        .to_string(),
        reason: "remove the bounded proof from active media".to_owned(),
        created_at,
    };
    let first = SharedMediaRepo::tombstone_project_media_asset(&db, original.clone())
        .await
        .expect("media tombstone");
    assert_eq!(first.id, media.id);
    assert_eq!(first.availability, "redacted");
    let replay = SharedMediaRepo::tombstone_project_media_asset(&db, original.clone())
        .await
        .expect("exact media tombstone replay");
    assert_eq!(replay, first);
    let replay_projection = SharedMediaRepo::replay_project_media_tombstone(&db, original.clone())
        .await
        .expect("read-only media tombstone replay")
        .expect("committed media tombstone receipt");
    assert_eq!(replay_projection, first);

    // The route resolves a receipt before current authorization validation;
    // even a malformed changed authority must therefore conflict rather than
    // become a new 400/403 mutation attempt.
    let mut malformed_authority = original.clone();
    malformed_authority.authorization_occurred_at = "not-rfc3339".to_owned();
    let conflict = SharedMediaRepo::replay_project_media_tombstone(&db, malformed_authority).await;
    assert!(matches!(conflict, Err(db::DbError::IdempotencyConflict)));

    // The repository receives the canonical mutation fingerprint from the
    // API, but it must still compare each persisted authority field directly.
    // Keeping the fingerprint/key constant here isolates the receipt fields.
    for (label, altered) in [
        ("tombstone action", {
            let mut value = original.clone();
            value.authorization_action = "project.media.purge".to_owned();
            value
        }),
        ("tombstone occurred_at", {
            let mut value = original.clone();
            value.authorization_occurred_at = "2026-08-13T00:00:01Z".to_owned();
            value
        }),
        ("tombstone basis", {
            let mut value = original.clone();
            value.authorization_basis = "altered_basis".to_owned();
            value
        }),
        ("tombstone event", {
            let mut value = original.clone();
            value.authorization_event_id = "altered-tombstone-event".to_owned();
            value
        }),
        ("tombstone principal", {
            let mut value = original.clone();
            value.principal_id = "different-user".to_owned();
            value
        }),
        ("tombstone target asset", {
            let mut value = original.clone();
            value.asset_id = "different-media-asset".to_owned();
            value
        }),
        ("tombstone target version", {
            let mut value = original.clone();
            value.expected_version += 1;
            value
        }),
        ("tombstone target disposition", {
            let mut value = original.clone();
            value.target_availability = "purged".to_owned();
            value
        }),
    ] {
        let conflict = SharedMediaRepo::tombstone_project_media_asset(&db, altered).await;
        assert!(
            matches!(conflict, Err(db::DbError::IdempotencyConflict)),
            "{label} must conflict on replay, got {conflict:?}"
        );
    }
    let tombstone_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM media_asset_tombstone WHERE idempotency_key = ?")
            .bind(&original.idempotency_key)
            .fetch_one(db.pool())
            .await
            .expect("one immutable tombstone row");
    assert_eq!(tombstone_count, 1);
}

#[tokio::test]
async fn project_evidence_composite_rejects_cross_project_asset() {
    let (db, project_id, task_id) = fixture().await;
    let media = upload(&db, &task_id, "asset-cross-project").await;
    let now = now_rfc3339();
    sqlx::query(
        "INSERT INTO project (id, name, settings, created_at, updated_at)
         VALUES ('project-media-other', 'Other', '{}', ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("other project");
    sqlx::query(
        "INSERT INTO project_milestone
         (id, project_id, milestone_sequence, milestone_key, lifecycle, created_at, updated_at)
         VALUES ('milestone-media-other', 'project-media-other', 1, 'M001', 'active', ?, ?)",
    )
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await
    .expect("other milestone");

    let result = SharedMediaRepo::create_project_media_attachment_mutation(
        &db,
        CreateProjectMediaAttachmentMutation {
            attachment: CreateProjectMediaAttachment {
                id: "cross-project-evidence".to_owned(),
                project_id: "project-media-other".to_owned(),
                asset_id: media.id,
                attachment_kind: "evidence".to_owned(),
                task_media_id: None,
                task_id: None,
                milestone_id: Some("milestone-media-other".to_owned()),
                milestone_check_id: None,
                source_task_id: None,
                source_execution_id: None,
                source_validation_id: None,
                source_task_version: None,
                source_context_digest: None,
                source_definition_revision_id: None,
                acceptance_check_ids_json: "[]".to_owned(),
                caption: Some("cross project".to_owned()),
                evidence_kind: Some("screenshot".to_owned()),
                checksum: Some(
                    "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08".to_owned(),
                ),
                availability: "available".to_owned(),
                project_url: None,
                author_type: "user".to_owned(),
                author_id: Some("user-media".to_owned()),
                authorization_json: "{}".to_owned(),
                created_at: now.clone(),
            },
            expected_milestone_version: 1,
            idempotency_key: "cross-project-evidence".to_owned(),
            mutation_fingerprint: "cross-project-fingerprint".to_owned(),
            authorization_event_id: "auth-cross-project".to_owned(),
            command_receipt: None,
            action_execution: None,
        },
    )
    .await;
    assert!(matches!(
        result,
        Err(db::DbError::NotFound) | Err(db::DbError::Check(_))
    ));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM project_media_attachment WHERE id = 'cross-project-evidence'",
        )
        .fetch_one(db.pool())
        .await
        .expect("attachment count"),
        0
    );
    let _ = project_id;
}

#[tokio::test]
async fn queued_gc_is_restartable_and_rejects_new_typed_references() {
    let (db, project_id, task_id) = fixture().await;
    create_release(&db, &project_id).await;
    let media = upload(&db, &task_id, "asset-restart").await;
    let now = now_rfc3339();
    TaskMediaRepo::soft_delete_media(&db, &media.id, &now)
        .await
        .expect("soft delete");
    let first_claim = SharedMediaRepo::claim_media_gc_candidates(
        &db,
        &now,
        "worker-restart-1",
        "9999-12-31T00:00:00Z",
        1,
    )
    .await
    .expect("claim");
    assert_eq!(first_claim.len(), 1);
    let first_claim_version = first_claim[0].version;

    let error = SharedMediaRepo::create_project_media_attachment(
        &db,
        CreateProjectMediaAttachment {
            id: "late-attachment".to_owned(),
            project_id: project_id.clone(),
            asset_id: media.id.clone(),
            attachment_kind: "evidence".to_owned(),
            task_media_id: None,
            task_id: None,
            milestone_id: None,
            milestone_check_id: None,
            source_task_id: Some(task_id),
            source_execution_id: None,
            source_validation_id: None,
            source_task_version: None,
            source_context_digest: None,
            source_definition_revision_id: None,
            acceptance_check_ids_json: "[]".to_owned(),
            caption: Some("late".to_owned()),
            evidence_kind: Some("screenshot".to_owned()),
            checksum: None,
            availability: "available".to_owned(),
            project_url: None,
            author_type: "user".to_owned(),
            author_id: Some("user-media".to_owned()),
            authorization_json: "{}".to_owned(),
            created_at: now.clone(),
        },
    )
    .await
    .expect_err("queued asset cannot be reattached");
    assert!(matches!(error, db::DbError::Check(_)));

    // The same interleaving must be rejected at the schema boundary, even if
    // a caller bypasses the typed repository and inserts directly through
    // SQLite while a worker owns the persisted lease.
    let raw_attachment = sqlx::query(
        "INSERT INTO project_media_attachment
            (id, project_id, asset_id, attachment_kind, acceptance_check_ids_json,
             availability, author_type, authorization_json, version, created_at, updated_at)
         VALUES ('late-attachment-sql', ?, ?, 'evidence', '[]', 'available',
                 'user', '{}', 1, ?, ?)",
    )
    .bind(&project_id)
    .bind(&media.id)
    .bind(&now)
    .bind(&now)
    .execute(db.pool())
    .await;
    assert!(
        raw_attachment.is_err(),
        "SQL attachment trigger must reject queued asset"
    );

    let raw_pin = sqlx::query(
        "INSERT INTO project_release_media_pin
            (id, project_id, release_id, asset_id, legacy_task_media_id,
             availability, pin_digest, created_at)
         VALUES ('late-pin-sql', ?, 'release-media', ?, ?, 'available', 'late', ?)",
    )
    .bind(&project_id)
    .bind(&media.id)
    .bind(&media.id)
    .bind(&now)
    .execute(db.pool())
    .await;
    assert!(
        raw_pin.is_err(),
        "SQL release-pin trigger must reject queued asset"
    );

    // A worker restart sees the queued row again, and reconciliation remains
    // idempotent even when the first worker never finalized it.
    // Simulate an expired persisted lease without replaying state.  A new
    // worker can reclaim it only because the lease expiry is now in the past.
    sqlx::query(
        "UPDATE media_asset
         SET gc_lease_expires_at = '2000-01-01T00:00:00Z'
         WHERE id = ?",
    )
    .bind(&media.id)
    .execute(db.pool())
    .await
    .expect("expire worker lease");
    let restarted = SqliteDb::new(db.pool().clone());
    let candidates = SharedMediaRepo::claim_media_gc_candidates(
        &restarted,
        &now,
        "worker-restart-2",
        "9999-12-31T00:00:00Z",
        1,
    )
    .await
    .expect("restarted claim");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].id, media.id);
    assert!(matches!(
        SharedMediaRepo::complete_media_gc(
            &restarted,
            &media.id,
            "worker-restart-1",
            first_claim_version,
            &now,
        )
        .await,
        Err(db::DbError::VersionConflict)
    ));
    SharedMediaRepo::complete_media_gc(
        &restarted,
        &media.id,
        "worker-restart-2",
        candidates[0].version,
        &now,
    )
    .await
    .expect("restarted finalize")
    .expect("restarted tombstone");

    let unreconciled = SharedMediaRepo::list_purged_media_assets(&restarted, 32)
        .await
        .expect("purged reconciliation list");
    assert_eq!(unreconciled.len(), 1);
    assert_eq!(unreconciled[0].id, media.id);
    SharedMediaRepo::mark_purged_media_asset_reconciled(&restarted, &media.id, &now)
        .await
        .expect("purge reconciliation marker");
    assert!(SharedMediaRepo::list_purged_media_assets(&restarted, 32)
        .await
        .expect("advanced purge reconciliation list")
        .is_empty());
}

// Keep the import in this integration test explicit: it makes accidental use
// of a non-migrated connection obvious if the fixture is ever simplified.
#[allow(dead_code)]
fn _pool_type(_: &SqlitePool) {}
