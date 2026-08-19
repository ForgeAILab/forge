//! Genesis Project Agent selection.
//!
//! Resolves the Project Agent revision set a Genesis Charter approval will
//! bind.  The session's preferred identity wins while it is still eligible;
//! otherwise Forge auto-selects a deterministic eligible agent so approval is
//! never blocked on an explicit pick the Main Agent did not make.  An agent
//! is eligible when the account owns it, it is not paused, its current
//! profile row exists, and it is not the account's active Main Agent.

use api_types::ProductGenesisSession;
use db::{AgentProfileRepo, AgentRepo, SqliteDb};
use sha2::{Digest, Sha256};

use crate::{Result, ServiceError, PROJECT_OPERATING_SKILL_KEY};

/// The exact revision set frozen into an approval for one Project Agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenesisAgentSelection {
    pub identity_id: String,
    pub display_name: String,
    pub profile_revision_id: String,
    pub operating_skill_revision: String,
    pub policy_digest: String,
}

/// Digest of the tool policy an approval freezes for the selected agent.
pub fn project_agent_policy_digest(tool_policy_json: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"forge.project-agent-policy/v1\0");
    digest.update(tool_policy_json.as_bytes());
    hex::encode(digest.finalize())
}

/// Current active revision of the Project Agent operating skill.
pub async fn current_project_agent_operating_skill_revision(db: &SqliteDb) -> Result<String> {
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT revision.id
         FROM operating_skill AS skill
         JOIN operating_skill_revision AS revision
           ON revision.id = skill.current_revision_id
          AND revision.operating_skill_id = skill.id
          AND revision.skill_key = skill.skill_key
         WHERE skill.skill_key = ?
           AND skill.lifecycle = 'active'
           AND skill.current_revision_id IS NOT NULL
         LIMIT 1",
    )
    .bind(PROJECT_OPERATING_SKILL_KEY)
    .fetch_optional(db.pool())
    .await?
    .flatten()
    .ok_or_else(|| {
        ServiceError::conflict("the Project Agent operating skill has no active revision")
    })
}

/// Resolve the Project Agent selection for a Genesis session.
///
/// Returns `None` only when the account has no eligible agent at all.
pub async fn resolve_genesis_project_agent(
    db: &SqliteDb,
    session: &ProductGenesisSession,
) -> Result<Option<GenesisAgentSelection>> {
    let operating_skill_revision = current_project_agent_operating_skill_revision(db).await?;
    if let Some(preferred) = session.preferred_project_agent_identity_id.as_deref() {
        if let Some(selection) = eligible_selection(
            db,
            preferred,
            &session.account_id,
            &operating_skill_revision,
        )
        .await?
        {
            return Ok(Some(selection));
        }
    }
    let Some(candidate_id) = auto_pick_candidate(db, &session.account_id).await? else {
        return Ok(None);
    };
    eligible_selection(
        db,
        &candidate_id,
        &session.account_id,
        &operating_skill_revision,
    )
    .await
}

/// Deterministic fallback: the oldest unpaused agent the account owns that has
/// a current profile and is not the active Main Agent, preferring identities
/// not already bound to another Project.
async fn auto_pick_candidate(db: &SqliteDb, account_id: &str) -> Result<Option<String>> {
    Ok(sqlx::query_scalar::<_, String>(
        "SELECT identity.id
         FROM agent_identity AS identity
         JOIN agent_profile AS profile
           ON profile.id = identity.selected_profile_id
          AND profile.identity_id = identity.id
         WHERE identity.owner_id = ?
           AND identity.paused = 0
           AND NOT EXISTS (
               SELECT 1 FROM account_main_agent_binding AS main_binding
               WHERE main_binding.identity_id = identity.id
                 AND main_binding.state = 'active'
           )
         ORDER BY EXISTS (
               SELECT 1 FROM project_agent_binding AS project_binding
               WHERE project_binding.identity_id = identity.id
                 AND project_binding.state = 'active'
           ) ASC,
           identity.created_at ASC,
           identity.id ASC
         LIMIT 1",
    )
    .bind(account_id)
    .fetch_optional(db.pool())
    .await?)
}

async fn eligible_selection(
    db: &SqliteDb,
    identity_id: &str,
    account_id: &str,
    operating_skill_revision: &str,
) -> Result<Option<GenesisAgentSelection>> {
    let Some(identity) = AgentRepo::get_by_id(db, identity_id).await? else {
        return Ok(None);
    };
    if identity.owner_id.as_deref() != Some(account_id) || identity.paused {
        return Ok(None);
    }
    let Some(profile) = AgentProfileRepo::get_profile(db, &identity.profile_id)
        .await?
        .filter(|profile| profile.identity_id == identity.id)
    else {
        return Ok(None);
    };
    Ok(Some(GenesisAgentSelection {
        identity_id: identity.id,
        display_name: identity.name,
        profile_revision_id: profile.id,
        operating_skill_revision: operating_skill_revision.to_owned(),
        policy_digest: project_agent_policy_digest(&profile.tool_policy_json),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{ProductGenesisLifecycle, ProductMaturity};
    use db::{
        create_sqlite_pool, now_rfc3339, run_migrations, CreateAgentIdentity, CreateAgentProfile,
        User, UserRepo,
    };
    use std::sync::Arc;

    async fn fixture() -> Arc<SqliteDb> {
        let pool = create_sqlite_pool("sqlite::memory:").await.expect("pool");
        run_migrations(&pool).await.expect("migrations");
        let db = Arc::new(SqliteDb::new(pool));
        let now = now_rfc3339();
        UserRepo::create_user(
            &*db,
            &User {
                id: "user-1".to_owned(),
                email: "user-1@example.test".to_owned(),
                password_hash: "placeholder".to_owned(),
                display_name: None,
                is_admin: false,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("user");
        db
    }

    async fn create_agent(db: &SqliteDb, id: &str, created_at: &str) {
        AgentRepo::create_identity_with_profile(
            db,
            CreateAgentIdentity {
                id: id.to_owned(),
                name: format!("Agent {id}"),
                description: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: db::AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: Some("user-1".to_owned()),
                visibility: "account".to_owned(),
                account_permission_ceiling: "{}".to_owned(),
                created_at: created_at.to_owned(),
                updated_at: created_at.to_owned(),
            },
            CreateAgentProfile {
                id: format!("{id}-profile"),
                identity_id: id.to_owned(),
                backend_kind: "native".to_owned(),
                executor_type: "native".to_owned(),
                provider: None,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "{}".to_owned(),
                tool_policy_json: "{}".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                created_at: created_at.to_owned(),
                updated_at: created_at.to_owned(),
            },
        )
        .await
        .expect("agent identity");
    }

    fn session(preferred: Option<&str>) -> ProductGenesisSession {
        let now = now_rfc3339();
        ProductGenesisSession {
            id: "genesis-1".to_owned(),
            account_id: "user-1".to_owned(),
            main_chat_id: "main-chat".to_owned(),
            prompt_revision: crate::MAIN_OPERATING_SKILL_KEY.to_owned(),
            maturity: ProductMaturity::Mvp,
            initial_idea: None,
            lifecycle: ProductGenesisLifecycle::Discovering,
            source_message_ids: Vec::new(),
            preferred_project_agent_identity_id: preferred.map(str::to_owned),
            charter_id: None,
            charter_revision_id: None,
            charter_approval_id: None,
            charter_version: 0,
            project_id: None,
            handoff_id: None,
            failure_reason: None,
            version: 1,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn auto_picks_the_oldest_eligible_agent_when_none_is_preferred() {
        let db = fixture().await;
        create_agent(&db, "agent-b", "2026-01-02T00:00:00Z").await;
        create_agent(&db, "agent-a", "2026-01-01T00:00:00Z").await;

        let selection = resolve_genesis_project_agent(&db, &session(None))
            .await
            .expect("resolve")
            .expect("selection");
        assert_eq!(selection.identity_id, "agent-a");
        assert_eq!(selection.profile_revision_id, "agent-a-profile");
        assert_eq!(selection.policy_digest, project_agent_policy_digest("{}"));
        assert!(!selection.operating_skill_revision.is_empty());
    }

    #[tokio::test]
    async fn preferred_agent_wins_and_an_ineligible_preference_falls_back() {
        let db = fixture().await;
        create_agent(&db, "agent-a", "2026-01-01T00:00:00Z").await;
        create_agent(&db, "agent-b", "2026-01-02T00:00:00Z").await;

        let selection = resolve_genesis_project_agent(&db, &session(Some("agent-b")))
            .await
            .expect("resolve")
            .expect("selection");
        assert_eq!(selection.identity_id, "agent-b");

        sqlx::query("UPDATE agent_identity SET paused = 1 WHERE id = 'agent-b'")
            .execute(db.pool())
            .await
            .expect("pause");
        let selection = resolve_genesis_project_agent(&db, &session(Some("agent-b")))
            .await
            .expect("resolve")
            .expect("selection");
        assert_eq!(selection.identity_id, "agent-a");
    }

    #[tokio::test]
    async fn active_main_agent_binding_excludes_an_identity_from_auto_pick() {
        let db = fixture().await;
        create_agent(&db, "agent-a", "2026-01-01T00:00:00Z").await;
        let now = now_rfc3339();
        sqlx::query(
            "INSERT INTO account_main_agent_binding
             (id, account_id, identity_id, profile_id, state, created_at, updated_at)
             VALUES ('binding-1', 'user-1', 'agent-a', 'agent-a-profile', 'active', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(db.pool())
        .await
        .expect("main binding");

        let selection = resolve_genesis_project_agent(&db, &session(None))
            .await
            .expect("resolve");
        assert!(selection.is_none());
    }
}
