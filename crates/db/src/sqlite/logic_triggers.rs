use crate::{new_uuid_v4, CreateTaskMedia, Result};

pub(super) async fn bootstrap_account_main_chat_in_tx(
    connection: &mut sqlx::SqliteConnection,
    account_id: &str,
    created_at: &str,
    updated_at: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_chat (
            id, kind, account_id, project_id, status,
            instruction_revision, message_count, version, created_at, updated_at
         )
         SELECT ?, 'account_main', ?, NULL, 'agent_setup_required', 0, 0, 1, ?, ?
         WHERE NOT EXISTS (
            SELECT 1 FROM agent_chat
            WHERE kind = 'account_main' AND account_id = ?
         )",
    )
    .bind(new_uuid_v4())
    .bind(account_id)
    .bind(created_at)
    .bind(updated_at)
    .bind(account_id)
    .execute(connection)
    .await?;
    Ok(())
}

pub(super) async fn bootstrap_project_chat_and_binding_in_tx(
    connection: &mut sqlx::SqliteConnection,
    project_id: &str,
    created_at: &str,
    updated_at: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO agent_chat (
            id, kind, account_id, project_id, status,
            instruction_revision, message_count, version, created_at, updated_at
         )
         SELECT ?, 'project', NULL, ?, 'agent_setup_required', 0, 0, 1, ?, ?
         WHERE NOT EXISTS (
            SELECT 1 FROM agent_chat
            WHERE kind = 'project' AND project_id = ?
         )",
    )
    .bind(new_uuid_v4())
    .bind(project_id)
    .bind(created_at)
    .bind(updated_at)
    .bind(project_id)
    .execute(&mut *connection)
    .await?;

    sqlx::query(
        "INSERT INTO project_agent_binding (
            id, project_id, identity_id, profile_id, state,
            autonomy_policy_json, permission_ceiling_json, subscriptions_json,
            wake_budget, version, created_at, updated_at
         )
         SELECT ?, ?, NULL, NULL, 'agent_setup_required', '{}', '{}', '[]', 10, 1, ?, ?
         WHERE NOT EXISTS (
            SELECT 1 FROM project_agent_binding
            WHERE project_id = ? AND state IN ('active', 'agent_setup_required')
         )",
    )
    .bind(new_uuid_v4())
    .bind(project_id)
    .bind(created_at)
    .bind(updated_at)
    .bind(project_id)
    .execute(connection)
    .await?;
    Ok(())
}

pub(super) async fn rearm_attention_wake_on_reopen_in_tx(
    connection: &mut sqlx::SqliteConnection,
    attention_id: &str,
    previous_status: &str,
    current_status: &str,
) -> Result<()> {
    if previous_status != "resolved" || current_status == "resolved" {
        return Ok(());
    }

    sqlx::query(
        "DELETE FROM agent_wake_blocker
         WHERE attention_id = ?
           AND turn_job_id IN (
             SELECT id FROM agent_chat_turn_job
             WHERE status IN ('succeeded', 'failed', 'cancelled')
           )",
    )
    .bind(attention_id)
    .execute(&mut *connection)
    .await?;
    sqlx::query("DELETE FROM agent_wake_attention_latest WHERE attention_id = ?")
        .bind(attention_id)
        .execute(connection)
        .await?;
    Ok(())
}

pub(super) async fn assign_topic_rotation_origin_in_tx(
    connection: &mut sqlx::SqliteConnection,
    chat_id: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE agent_chat_topic_rotation
         SET origin_turn_id = (
             SELECT id FROM agent_chat_turn_job
             WHERE chat_id = ? AND status IN ('leased', 'awaiting_input')
             ORDER BY created_at DESC LIMIT 1
         )
         WHERE chat_id = ?",
    )
    .bind(chat_id)
    .bind(chat_id)
    .execute(connection)
    .await?;
    Ok(())
}

pub(super) async fn enqueue_native_topic_rotation_in_tx(
    connection: &mut sqlx::SqliteConnection,
    chat_id: Option<&str>,
    label: &str,
    cause: &str,
    created_at: &str,
) -> Result<bool> {
    let Some(chat_id) = chat_id else {
        return Ok(false);
    };
    let inserted = sqlx::query(
        "INSERT OR IGNORE INTO agent_chat_topic_rotation (
            chat_id, id, label, cause, created_at
         )
         SELECT ?, ?, ?, ?, ?
         WHERE EXISTS (
             SELECT 1
             FROM agent_chat c
             JOIN agent_identity i ON i.id = COALESCE(
                 (SELECT b.identity_id FROM account_main_agent_binding b
                  WHERE b.account_id = c.account_id AND b.state = 'active'),
                 (SELECT b.identity_id FROM project_agent_binding b
                  WHERE b.project_id = c.project_id AND b.state = 'active')
             )
             JOIN agent_profile p ON p.id = i.selected_profile_id
             WHERE c.id = ? AND p.backend_kind = 'native'
         )",
    )
    .bind(chat_id)
    .bind(new_uuid_v4())
    .bind(label)
    .bind(cause)
    .bind(created_at)
    .bind(chat_id)
    .execute(&mut *connection)
    .await?
    .rows_affected()
        > 0;
    if inserted {
        assign_topic_rotation_origin_in_tx(connection, chat_id).await?;
    }
    Ok(inserted)
}

pub(super) async fn enqueue_idle_topic_rotation_in_tx(
    connection: &mut sqlx::SqliteConnection,
    message_id: &str,
    chat_id: &str,
    author_type: &str,
    created_at: &str,
) -> Result<bool> {
    if author_type != "user" {
        return Ok(false);
    }
    let idle: Option<i64> = sqlx::query_scalar(
        "SELECT julianday(?) - (
             SELECT MAX(julianday(created_at)) FROM agent_chat_message
             WHERE chat_id = ? AND id <> ? AND author_type IN ('user', 'agent')
         ) >= 8.0 / 24.0",
    )
    .bind(created_at)
    .bind(chat_id)
    .bind(message_id)
    .fetch_one(&mut *connection)
    .await?;
    if idle != Some(1) {
        return Ok(false);
    }
    enqueue_native_topic_rotation_in_tx(connection, Some(chat_id), "After idle", "idle", created_at)
        .await
}

pub(super) async fn project_task_media_in_tx(
    connection: &mut sqlx::SqliteConnection,
    input: &CreateTaskMedia,
) -> Result<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO media_asset (
            id, project_id, legacy_task_media_id, display_filename, content_type,
            byte_size, storage_key, checksum, availability, gc_state,
            created_at, updated_at
         )
         SELECT ?, t.project_id, ?, ?, ?, ?, ?, NULL, 'available', 'referenced', ?, ?
         FROM task t WHERE t.id = ?",
    )
    .bind(&input.id)
    .bind(&input.id)
    .bind(&input.display_filename)
    .bind(&input.content_type)
    .bind(input.byte_size)
    .bind(&input.storage_key)
    .bind(&input.created_at)
    .bind(&input.created_at)
    .bind(&input.task_id)
    .execute(&mut *connection)
    .await?;

    sqlx::query(
        "UPDATE task_media
         SET asset_id = COALESCE(
             asset_id,
             (SELECT id FROM media_asset WHERE storage_key = ? ORDER BY id LIMIT 1)
         )
         WHERE id = ?",
    )
    .bind(&input.storage_key)
    .bind(&input.id)
    .execute(&mut *connection)
    .await?;

    let attachment = sqlx::query(
        "INSERT OR IGNORE INTO project_media_attachment (
            id, project_id, asset_id, attachment_kind, task_media_id, task_id,
            acceptance_check_ids_json, evidence_kind, availability,
            project_url, author_type, author_id, authorization_json,
            version, created_at, updated_at
         )
         SELECT
            'task-media-attachment:' || ?, t.project_id, tm.asset_id, 'task', ?, ?, '[]',
            CASE
                WHEN ? LIKE 'image/%' THEN 'screenshot'
                WHEN ? LIKE 'video/%' THEN 'walkthrough_video'
                ELSE 'other'
            END,
            'available',
            '/api/v1/projects/' || t.project_id || '/media/' || tm.asset_id,
            ?, ?, '{}', 1, ?, ?
         FROM task_media tm
         JOIN task t ON t.id = tm.task_id
         WHERE tm.id = ? AND tm.asset_id IS NOT NULL",
    )
    .bind(&input.id)
    .bind(&input.id)
    .bind(&input.task_id)
    .bind(&input.content_type)
    .bind(&input.content_type)
    .bind(input.author_type.to_string())
    .bind(input.author_id.as_deref())
    .bind(&input.created_at)
    .bind(&input.created_at)
    .bind(&input.id)
    .execute(&mut *connection)
    .await?;
    if attachment.rows_affected() > 0 {
        let asset_id: String = sqlx::query_scalar("SELECT asset_id FROM task_media WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut *connection)
            .await?;
        super::shared_media::reconcile_media_asset_in_tx(connection, &asset_id, &input.created_at)
            .await?;
    }
    Ok(())
}
