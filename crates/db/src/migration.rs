use crate::{new_uuid_v4, now_rfc3339, DbError, Result};
use include_dir::{include_dir, Dir};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqlitePool};
use std::{
    fs,
    path::{Path, PathBuf},
};

// Embed every migration .sql file into the binary at compile time so a released
// binary has no filesystem dependency on the source tree.
// Keep this module's source revisioned when adding a migration: include_dir's
// directory dependency is intentionally compile-time and older Cargo versions
// do not always notice a newly-created file under the directory (or a changed
// migration after the initial build).
// Embedded migration bundle revision: V125.
static MIGRATIONS_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/migrations");

#[derive(Debug, Clone, PartialEq, Eq)]
struct Migration {
    version: i64,
    name: String,
    path: PathBuf,
}

pub async fn run_migrations(pool: &SqlitePool) -> Result<()> {
    ensure_migration_table(pool).await?;

    let mut migrations: Vec<(Migration, String)> = MIGRATIONS_DIR
        .files()
        .filter(|file| file.path().extension().and_then(|ext| ext.to_str()) == Some("sql"))
        .map(|file| {
            let migration = parse_migration_path(file.path().to_path_buf())?;
            let sql = file
                .contents_utf8()
                .ok_or_else(|| DbError::InvalidMigrationFilename {
                    path: file.path().to_path_buf(),
                })?
                .to_string();
            Ok::<_, DbError>((migration, sql))
        })
        .collect::<Result<_>>()?;

    migrations.sort_by_key(|(migration, _)| migration.version);

    for (migration, sql) in migrations {
        if is_applied(pool, migration.version).await? {
            continue;
        }
        apply_migration_sql(pool, &migration, &sql).await?;
    }

    reconcile_project_admission_bindings(pool).await?;

    Ok(())
}

/// Complete the one historical binding shape that SQL cannot safely repair:
/// V071's public replacement primitive could create an active Charter-backed
/// row without the V076 authority fields, and SQLite has no SHA-256 function
/// with which to freeze the selected identity's current Profile policy. This
/// bounded startup reconciliation derives only canonical rows, preserves the
/// selected identity/settings/Chat/Charter/handoff, and records a replacement
/// binding plus durable event. Ambiguous rows are made setup-required instead
/// of fabricating authority.
async fn reconcile_project_admission_bindings(pool: &SqlitePool) -> Result<()> {
    let candidates = sqlx::query(
        "SELECT b.id, b.project_id, b.identity_id, b.version,
                b.autonomy_policy_json, b.permission_ceiling_json,
                b.subscriptions_json, b.wake_budget,
                p.owner_id, p.current_charter_id, p.current_charter_revision_id,
                receipt.id AS admission_receipt_id,
                approval.id AS charter_approval_id,
                identity.selected_profile_id, identity.paused, identity.archived_at,
                profile.tool_policy_json,
                skill.current_revision_id AS operating_skill_revision_id
         FROM project_agent_binding b
         JOIN project p ON p.id = b.project_id
         LEFT JOIN project_admission_receipt receipt ON receipt.project_id = p.id
         LEFT JOIN project_charter_approval approval
           ON approval.consumed_project_id = p.id
          AND approval.charter_id = p.current_charter_id
          AND approval.revision_id = p.current_charter_revision_id
          AND approval.lifecycle = 'consumed'
         LEFT JOIN agent_identity identity
           ON identity.id = b.identity_id AND identity.owner_id = p.owner_id
         LEFT JOIN agent_profile profile
           ON profile.id = identity.selected_profile_id
          AND profile.identity_id = identity.id
         LEFT JOIN operating_skill skill
           ON skill.skill_key = 'forge.project.orchestration/v1'
          AND skill.lifecycle = 'active'
         WHERE b.state = 'active'
           AND p.charter_status = 'charter_backed'
           AND p.charter_setup_required = 0
           AND (b.charter_setup_required != 0
                OR b.admission_receipt_id IS NULL
                OR b.charter_approval_id IS NULL
                OR b.operating_skill_revision_id IS NULL
                OR length(trim(b.policy_revision)) = 0
                OR length(trim(b.policy_digest)) = 0
                OR b.charter_id IS NOT p.current_charter_id
                OR b.charter_revision_id IS NOT p.current_charter_revision_id)",
    )
    .fetch_all(pool)
    .await?;

    for candidate in candidates {
        let binding_id: String = candidate.try_get("id")?;
        let project_id: String = candidate.try_get("project_id")?;
        let identity_id: Option<String> = candidate.try_get("identity_id")?;
        let expected_version: i64 = candidate.try_get("version")?;
        let owner_id: Option<String> = candidate.try_get("owner_id")?;
        let charter_id: Option<String> = candidate.try_get("current_charter_id")?;
        let charter_revision_id: Option<String> =
            candidate.try_get("current_charter_revision_id")?;
        let admission_receipt_id: Option<String> = candidate.try_get("admission_receipt_id")?;
        let charter_approval_id: Option<String> = candidate.try_get("charter_approval_id")?;
        let selected_profile_id: Option<String> = candidate.try_get("selected_profile_id")?;
        let identity_paused: Option<i64> = candidate.try_get("paused")?;
        let identity_archived_at: Option<String> = candidate.try_get("archived_at")?;
        let tool_policy_json: Option<String> = candidate.try_get("tool_policy_json")?;
        let operating_skill_revision_id: Option<String> =
            candidate.try_get("operating_skill_revision_id")?;

        let safe = identity_id.is_some()
            && owner_id.is_some()
            && charter_id.is_some()
            && charter_revision_id.is_some()
            && admission_receipt_id.is_some()
            && charter_approval_id.is_some()
            && selected_profile_id.is_some()
            && identity_paused == Some(0)
            && identity_archived_at.is_none()
            && tool_policy_json.is_some()
            && operating_skill_revision_id.is_some();
        let now = now_rfc3339();
        if !safe {
            let mut tx = crate::begin_immediate(pool).await?;
            sqlx::query(
                "UPDATE project_agent_binding
                 SET charter_setup_required = 1, updated_at = ?
                 WHERE id = ? AND project_id = ? AND state = 'active' AND version = ?",
            )
            .bind(&now)
            .bind(&binding_id)
            .bind(&project_id)
            .bind(expected_version)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                "UPDATE agent_chat SET status = 'agent_setup_required',
                     version = version + 1, updated_at = ?
                 WHERE kind = 'project' AND project_id = ? AND status = 'ready'",
            )
            .bind(&now)
            .bind(&project_id)
            .execute(&mut *tx)
            .await?;
            let event_id = new_uuid_v4();
            sqlx::query(
                "INSERT OR IGNORE INTO domain_event (
                    id, event_type, entity_type, entity_id, actor_type, actor_id,
                    scope_type, scope_id, correlation_id, causation_id,
                    causation_depth, dedupe_key, payload_json, created_at
                 ) VALUES (?, 'project.agent_binding.repair_required',
                           'project_agent_binding', ?, 'system', NULL,
                           'project', ?, ?, NULL, 0, ?, ?, ?)",
            )
            .bind(&event_id)
            .bind(&binding_id)
            .bind(&project_id)
            .bind(&event_id)
            .bind(format!(
                "project-binding-admission-repair-required:{binding_id}"
            ))
            .bind(
                serde_json::json!({
                    "project_id": project_id,
                    "binding_id": binding_id,
                    "status": "agent_setup_required",
                    "reason": "admission authority could not be derived uniquely"
                })
                .to_string(),
            )
            .bind(&now)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            continue;
        }

        let identity_id = identity_id.expect("safe binding identity");
        let profile_id = selected_profile_id.expect("safe selected Profile");
        let charter_id = charter_id.expect("safe current Charter");
        let charter_revision_id = charter_revision_id.expect("safe current Charter revision");
        let admission_receipt_id = admission_receipt_id.expect("safe admission receipt");
        let charter_approval_id = charter_approval_id.expect("safe Charter approval");
        let operating_skill_revision_id =
            operating_skill_revision_id.expect("safe operating skill revision");
        let tool_policy_json = tool_policy_json.expect("safe Profile policy");
        let mut digest = Sha256::new();
        digest.update(b"forge.project-agent-policy/v1\0");
        digest.update(tool_policy_json.as_bytes());
        let policy_digest = hex::encode(digest.finalize());
        let replacement_id = new_uuid_v4();
        let autonomy_policy_json: String = candidate.try_get("autonomy_policy_json")?;
        let permission_ceiling_json: String = candidate.try_get("permission_ceiling_json")?;
        let subscriptions_json: String = candidate.try_get("subscriptions_json")?;
        let wake_budget: i64 = candidate.try_get("wake_budget")?;

        let mut tx = crate::begin_immediate(pool).await?;
        let replaced = sqlx::query(
            "UPDATE project_agent_binding
             SET state = 'replaced', replaced_by_binding_id = NULL,
                 replacement_reason = 'Project admission authority reconciliation',
                 version = version + 1, updated_at = ?
             WHERE id = ? AND project_id = ? AND state = 'active' AND version = ?",
        )
        .bind(&now)
        .bind(&binding_id)
        .bind(&project_id)
        .bind(expected_version)
        .execute(&mut *tx)
        .await?;
        if replaced.rows_affected() != 1 {
            tx.rollback().await?;
            continue;
        }
        sqlx::query(
            "INSERT INTO project_agent_binding (
                id, project_id, identity_id, profile_id, state,
                autonomy_policy_json, permission_ceiling_json, subscriptions_json,
                wake_budget, version, operating_skill_revision_id,
                policy_revision, policy_digest, charter_id, charter_revision_id,
                charter_setup_required, admission_receipt_id, charter_approval_id,
                created_at, updated_at
             ) VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?,
                       'forge.project-agent-policy/v1', ?, ?, ?, 0, ?, ?, ?, ?)",
        )
        .bind(&replacement_id)
        .bind(&project_id)
        .bind(&identity_id)
        .bind(&profile_id)
        .bind(&autonomy_policy_json)
        .bind(&permission_ceiling_json)
        .bind(&subscriptions_json)
        .bind(wake_budget)
        .bind(expected_version + 1)
        .bind(&operating_skill_revision_id)
        .bind(&policy_digest)
        .bind(&charter_id)
        .bind(&charter_revision_id)
        .bind(&admission_receipt_id)
        .bind(&charter_approval_id)
        .bind(&now)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE project_agent_binding SET replaced_by_binding_id = ?
             WHERE id = ? AND project_id = ? AND state = 'replaced'",
        )
        .bind(&replacement_id)
        .bind(&binding_id)
        .bind(&project_id)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "UPDATE agent_chat SET status = 'ready', version = version + 1, updated_at = ?
             WHERE kind = 'project' AND project_id = ? AND status = 'agent_setup_required'",
        )
        .bind(&now)
        .bind(&project_id)
        .execute(&mut *tx)
        .await?;
        let event_id = new_uuid_v4();
        sqlx::query(
            "INSERT INTO domain_event (
                id, event_type, entity_type, entity_id, actor_type, actor_id,
                scope_type, scope_id, correlation_id, causation_id,
                causation_depth, dedupe_key, payload_json, created_at
             ) VALUES (?, 'project.agent_binding.repaired',
                       'project_agent_binding', ?, 'system', NULL,
                       'project', ?, ?, NULL, 0, ?, ?, ?)",
        )
        .bind(&event_id)
        .bind(&replacement_id)
        .bind(&project_id)
        .bind(&event_id)
        .bind(format!("project-binding-admission-repair:{binding_id}"))
        .bind(
            serde_json::json!({
                "project_id": project_id,
                "replaced_binding_id": binding_id,
                "binding_id": replacement_id,
                "admission_receipt_id": admission_receipt_id,
                "charter_approval_id": charter_approval_id,
                "status": "repaired"
            })
            .to_string(),
        )
        .bind(&now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }

    Ok(())
}

pub async fn run_migrations_from(pool: &SqlitePool, migration_dir: impl AsRef<Path>) -> Result<()> {
    ensure_migration_table(pool).await?;

    let migrations = discover_migrations(migration_dir.as_ref())?;
    for migration in migrations {
        if is_applied(pool, migration.version).await? {
            continue;
        }
        apply_migration(pool, &migration).await?;
    }

    Ok(())
}

async fn ensure_migration_table(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS _migration (
            version     INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            applied_at  TEXT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

fn discover_migrations(migration_dir: &Path) -> Result<Vec<Migration>> {
    let entries = fs::read_dir(migration_dir).map_err(|source| DbError::ReadMigrationDir {
        path: migration_dir.to_path_buf(),
        source,
    })?;
    let mut migrations = Vec::new();

    for entry in entries {
        let path = entry
            .map_err(|source| DbError::ReadMigrationDir {
                path: migration_dir.to_path_buf(),
                source,
            })?
            .path();

        if path.extension().and_then(|extension| extension.to_str()) != Some("sql") {
            continue;
        }

        migrations.push(parse_migration_path(path)?);
    }

    migrations.sort_by_key(|migration| migration.version);
    Ok(migrations)
}

fn parse_migration_path(path: PathBuf) -> Result<Migration> {
    let filename = path
        .file_name()
        .and_then(|filename| filename.to_str())
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?;
    let stem = filename
        .strip_suffix(".sql")
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?;
    let (version_part, name) = stem
        .split_once("__")
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?;
    let version = version_part
        .strip_prefix('V')
        .ok_or_else(|| DbError::InvalidMigrationFilename { path: path.clone() })?
        .parse::<i64>()
        .map_err(|source| DbError::InvalidMigrationVersion {
            path: path.clone(),
            source,
        })?;

    Ok(Migration {
        version,
        name: name.to_owned(),
        path,
    })
}

async fn is_applied(pool: &SqlitePool, version: i64) -> Result<bool> {
    let applied = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _migration WHERE version = ?")
        .bind(version)
        .fetch_one(pool)
        .await?;
    Ok(applied > 0)
}

async fn apply_migration(pool: &SqlitePool, migration: &Migration) -> Result<()> {
    let sql = fs::read_to_string(&migration.path).map_err(|source| DbError::ReadMigrationFile {
        path: migration.path.clone(),
        source,
    })?;
    apply_migration_sql(pool, migration, &sql).await
}

async fn apply_migration_sql(pool: &SqlitePool, migration: &Migration, sql: &str) -> Result<()> {
    if migration_requires_direct_connection(sql) {
        let mut connection = pool.acquire().await?;
        sqlx::raw_sql(sql).execute(&mut *connection).await?;
        sqlx::query("INSERT INTO _migration (version, name, applied_at) VALUES (?, ?, ?)")
            .bind(migration.version)
            .bind(&migration.name)
            .bind(now_rfc3339())
            .execute(&mut *connection)
            .await?;
    } else {
        let mut transaction = crate::begin_immediate(pool).await?;

        sqlx::raw_sql(sql).execute(&mut *transaction).await?;
        sqlx::query("INSERT INTO _migration (version, name, applied_at) VALUES (?, ?, ?)")
            .bind(migration.version)
            .bind(&migration.name)
            .bind(now_rfc3339())
            .execute(&mut *transaction)
            .await?;

        transaction.commit().await?;
    }

    Ok(())
}

fn migration_requires_direct_connection(sql: &str) -> bool {
    // SQLite ignores `PRAGMA foreign_keys = ...` inside an open transaction, so
    // migrations that rebuild referenced tables must run directly on a single
    // connection instead of inside the default transaction wrapper.
    sql.to_ascii_lowercase().contains("pragma foreign_keys")
}
