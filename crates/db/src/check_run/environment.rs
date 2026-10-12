//! Durable owner attestation, below the identity builder. No secret values.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckEnvironmentAttestation {
    pub inherited_revision: String,
    pub shell_revision: String,
}

impl SqliteDb {
    pub async fn check_environment_attestation(
        &self,
        owner_id: &str,
        generation: &str,
        salt_revision: &str,
    ) -> Result<Option<CheckEnvironmentAttestation>> {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT inherited_revision,shell_revision FROM check_environment_attestation WHERE owner_id=? AND environment_generation=? AND salt_revision=?",
        )
        .bind(owner_id)
        .bind(generation)
        .bind(salt_revision)
        .fetch_optional(self.pool())
        .await?;
        Ok(row.map(
            |(inherited_revision, shell_revision)| CheckEnvironmentAttestation {
                inherited_revision,
                shell_revision,
            },
        ))
    }

    /// First successful answer wins; another handle cannot overwrite it.
    pub async fn store_check_environment_attestation(
        &self,
        owner_id: &str,
        generation: &str,
        salt_revision: &str,
        answer: &CheckEnvironmentAttestation,
    ) -> Result<CheckEnvironmentAttestation> {
        sqlx::query(
            "INSERT OR IGNORE INTO check_environment_attestation(owner_id,environment_generation,salt_revision,inherited_revision,shell_revision,created_at) VALUES(?,?,?,?,?,?)",
        )
        .bind(owner_id)
        .bind(generation)
        .bind(salt_revision)
        .bind(&answer.inherited_revision)
        .bind(&answer.shell_revision)
        .bind(now_rfc3339())
        .execute(self.pool())
        .await?;
        self.check_environment_attestation(owner_id, generation, salt_revision)
            .await?
            .ok_or_else(|| DbError::Check("environment attestation was not stored".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn check_run_environment_attestation_is_immutable_and_owner_generation_salt_scoped() {
        let pool = crate::create_sqlite_pool("sqlite::memory:").await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        let db = SqliteDb::new(pool.clone());
        let first = CheckEnvironmentAttestation {
            inherited_revision: "opaque-first".into(),
            shell_revision: "fixture-bash".into(),
        };
        let changed = CheckEnvironmentAttestation {
            inherited_revision: "opaque-changed".into(),
            ..first.clone()
        };
        assert_eq!(
            db.store_check_environment_attestation("server", "one", "salt", &first)
                .await
                .unwrap(),
            first
        );
        assert_eq!(
            db.store_check_environment_attestation("server", "one", "salt", &changed)
                .await
                .unwrap(),
            first
        );
        // A newly composed handle reads the durable answer, not a local cache.
        let reopened = SqliteDb::new(pool);
        assert_eq!(
            reopened
                .check_environment_attestation("server", "one", "salt")
                .await
                .unwrap(),
            Some(first)
        );
        for (owner, generation, salt) in [
            ("daemon", "one", "salt"),
            ("server", "two", "salt"),
            ("server", "one", "new-salt"),
        ] {
            assert!(reopened
                .check_environment_attestation(owner, generation, salt)
                .await
                .unwrap()
                .is_none());
            assert_eq!(
                reopened
                    .store_check_environment_attestation(owner, generation, salt, &changed)
                    .await
                    .unwrap(),
                changed
            );
        }
    }
}
