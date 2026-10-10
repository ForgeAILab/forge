//! What a request states about the environment its commands will see.
//!
//! The canonical CI policy is the only one whose result may be reused. Its
//! steps inherit the owner's environment exactly as the frozen policy's do,
//! so its identity names that environment instead of restricting it: a
//! salted digest over everything a step inherits (names and values, minus
//! `check_executor::ENVIRONMENT_IDENTITY_DENYLIST`) and over the Project
//! values. The owner computes the same digest again right before it runs. If
//! it differs, the run still executes and is its consumers' verdict, but the
//! owner attests nothing and the result is never reused: a changed
//! environment is a miss, never a wrong hit and never a refusal.
use crate::{Result, ServiceError};
use api_types::*;
use std::collections::BTreeMap;

/// The `system_setting` key of the salt below. Never listed or writable
/// through the admin settings API.
pub const VALUE_REVISION_SALT_KEY: &str = "check_value_revision_salt";

/// Environment values may be secrets, so an identity never holds one or a
/// public hash of one. Every digest of a value is keyed with a random salt
/// this server created once and keeps in its own database: equal values give
/// equal digests for as long as the installation lives, across restarts.
/// Losing the salt changes every identity, so stored results are simply not
/// found again.
async fn salt(db: &db::SqliteDb) -> Result<String> {
    let read = || {
        sqlx::query_scalar::<_, String>("SELECT value FROM system_setting WHERE key=?")
            .bind(VALUE_REVISION_SALT_KEY)
            .fetch_optional(db.pool())
    };
    if let Some(salt) = read().await.map_err(db::DbError::from)? {
        return Ok(salt);
    }
    // First use. Two first users race on the key: one row wins and both read
    // it back.
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
        .ok_or_else(|| ServiceError::invalid_operation("check value salt was not stored"))
}

/// The opaque revision of one Project value.
pub async fn value_revision(db: &db::SqliteDb, key: &str, value: &str) -> Result<String> {
    canonical_digest_with_schema(
        "forge.check-value-revision/1",
        &serde_json::json!({ "salt": salt(db).await?, "key": key, "value": value }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

/// The environment revision: what a step inherits and what the Project adds,
/// names and values, keyed with the installation salt.
fn environment_revision(
    salt: &str,
    inherited: &check_executor::InheritedEnvironment,
    project: &BTreeMap<String, String>,
) -> Result<String> {
    canonical_digest_with_schema(
        "forge.check-inherited-environment/1",
        &serde_json::json!({
            "salt": salt, "inherited": inherited.identity_values(), "project": project,
        }),
    )
    .map_err(|error| ServiceError::invalid_operation(error.to_string()))
}

/// What this owner attests for a canonical run in the environment in force
/// now. `None`: the login shell did not answer, so nothing is attested and
/// nothing is reused. Tool versions are not probed: an in-place upgrade
/// behind an unchanged `PATH` is not seen.
pub async fn attest(
    db: &db::SqliteDb,
    project: &BTreeMap<String, String>,
) -> Result<Option<ServerCheckExecutionInputs>> {
    let Some(inherited) = check_executor::inherited_environment().await else {
        tracing::warn!(
            "the login shell did not report its environment; canonical check results are not reusable"
        );
        return Ok(None);
    };
    Ok(Some(ServerCheckExecutionInputs {
        toolchain_revision: "path-not-probed".into(),
        environment_revision: environment_revision(&salt(db).await?, &inherited, project)?,
        asset_revisions: BTreeMap::new(),
        secret_revisions: BTreeMap::new(),
        shell_revision: inherited.shell_revision,
        runner_revision: format!("{CANONICAL_CI_POLICY}@{}", env!("CARGO_PKG_VERSION")),
    }))
}

/// The digest input of `spec` as its owner will execute it. `project` is the
/// Project environment in force now.
///
/// A canonical spec is reusable only when it is scoped to its worktree (the
/// run sees that worktree's ignored files) and the owner could attest its
/// environment. Otherwise the identity is unattested: the check runs and is
/// never reused.
pub async fn digest_input(
    db: &db::SqliteDb,
    spec: CheckSpec,
    project: &BTreeMap<String, String>,
) -> Result<CheckDigestInput> {
    let canonical = spec.execution_policy == CANONICAL_CI_POLICY;
    let mut environment = BTreeMap::new();
    for key in spec.commands.iter().flat_map(|c| c.environment_keys.iter()) {
        let value = match project.get(key) {
            Some(value) if canonical => {
                CheckEnvironmentValue::SecretRevision(value_revision(db, key, value).await?)
            }
            // The frozen policies inherit the owner's whole environment and
            // attest none of it: nothing is controlled, nothing is reused.
            _ => CheckEnvironmentValue::Volatile,
        };
        environment.insert(key.clone(), value);
    }
    let attested = if canonical && matches!(spec.scope, CheckScope::Workspace { .. }) {
        attest(db, project).await?
    } else {
        None
    };
    let environment_identity = match attested {
        Some(inputs) => inputs.identity().map_err(ServiceError::invalid_operation)?,
        None => CheckEnvironmentIdentity::NotAttested,
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
