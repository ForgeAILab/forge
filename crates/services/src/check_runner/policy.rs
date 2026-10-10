//! What a request states about the environment its commands will see.
//!
//! The canonical CI policy is the only one whose result may be reused, so its
//! identity names everything a command can read besides the commit: each
//! declared key (the owner's `PATH` and `HOME`, by value; each Project value,
//! by an opaque revision) and the owner's attested environment. The owner
//! recomputes all of it before it runs and refuses when any part moved, so a
//! stored result was produced by exactly the environment its identity names.
use crate::{Result, ServiceError};
use api_types::*;
use std::collections::BTreeMap;

/// The `system_setting` key of the salt below. Never listed or writable
/// through the admin settings API.
pub const VALUE_REVISION_SALT_KEY: &str = "check_value_revision_salt";

/// A Project value may be a secret, so the identity never holds it or a
/// public hash of it. Its revision is keyed with a random salt this server
/// created once and keeps in its own database: equal values give equal
/// revisions for as long as the installation lives, across restarts, so a
/// request stated before a restart is still verifiable by the owner after it.
pub async fn value_revision(db: &db::SqliteDb, key: &str, value: &str) -> Result<String> {
    let read = || {
        sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key=?")
            .bind(VALUE_REVISION_SALT_KEY)
            .fetch_optional(db.pool())
    };
    let salt = match read().await.map_err(db::DbError::from)? {
        Some(salt) => salt,
        None => {
            // First use. Two first users race on the key: one row wins and
            // both read it back.
            sqlx::query("INSERT OR IGNORE INTO system_setting(key,value,updated_at) VALUES(?,?,?)")
                .bind(VALUE_REVISION_SALT_KEY)
                .bind(format!("{}{}", db::new_uuid_v4(), db::new_uuid_v4()))
                .bind(db::now_rfc3339())
                .execute(db.pool())
                .await
                .map_err(db::DbError::from)?;
            read()
                .await
                .map_err(db::DbError::from)?
                .ok_or_else(|| ServiceError::invalid_operation("check value salt was not stored"))?
        }
    };
    canonical_digest_with_schema(
        "forge.check-value-revision/1",
        &serde_json::json!({ "salt": salt, "key": key, "value": value }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

/// The digest input of `spec` as its owner will execute it. `project` is the
/// Project environment in force now.
pub async fn digest_input(
    db: &db::SqliteDb,
    spec: CheckSpec,
    project: &BTreeMap<String, String>,
) -> Result<CheckDigestInput> {
    let canonical = spec.execution_policy == CANONICAL_CI_POLICY;
    let owner = if canonical {
        Some(check_executor::canonical_environment().await)
    } else {
        None
    };
    let mut environment = BTreeMap::new();
    for key in spec.commands.iter().flat_map(|c| c.environment_keys.iter()) {
        let value = match (owner, project.get(key)) {
            // The frozen policies inherit the owner's whole environment:
            // nothing about it is controlled, and nothing is reused.
            (None, _) => CheckEnvironmentValue::Volatile,
            (Some(_), Some(value)) => {
                CheckEnvironmentValue::SecretRevision(value_revision(db, key, value).await?)
            }
            (Some(owner), None) => CheckEnvironmentValue::ControlledValue(
                owner.values.get(key).cloned().ok_or_else(|| {
                    ServiceError::invalid_operation(format!(
                        "check declares environment key {key} that neither the Project nor the owner provides"
                    ))
                })?,
            ),
        };
        environment.insert(key.clone(), value);
    }
    let environment_identity = if canonical {
        check_executor::canonical_inputs()
            .await
            .and_then(|inputs| inputs.identity())
            .map_err(ServiceError::invalid_operation)?
    } else {
        CheckEnvironmentIdentity::NotAttested
    };
    Ok(CheckDigestInput {
        spec,
        environment,
        environment_identity,
        execution_revision: CheckExecutionRevision {
            number: 0,
            audit_ref: None,
        },
    })
}
