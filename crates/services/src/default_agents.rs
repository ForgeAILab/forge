use crate::Result;
use db::{
    new_uuid_v4, now_rfc3339, Agent, AgentListQuery, AgentRepo, AgentStatus, CreateAgent,
    PageRequest, SortBy, SortOrder, SqliteDb, SystemSettingRepo,
};
use executors::{AdapterRegistry, ExecutorKind};

pub async fn ensure_default_agents(
    db: &SqliteDb,
    registry: &AdapterRegistry,
) -> Result<Vec<Agent>> {
    let kinds = registry.kinds();
    tracing::info!(
        executor_count = kinds.len(),
        "detecting default agents for registered executors"
    );

    let bootstrap_completed = SystemSettingRepo::get_setting(db, "bootstrap_completed")
        .await?
        .is_some();
    let mut agents = Vec::new();
    for kind in kinds {
        if let Some(agent) = ensure_default_agent(db, kind, bootstrap_completed).await? {
            agents.push(agent);
        }
    }
    Ok(agents)
}

async fn ensure_default_agent(
    db: &SqliteDb,
    kind: ExecutorKind,
    bootstrap_completed: bool,
) -> Result<Option<Agent>> {
    let executor_type = kind.to_string();
    if let Some(agent) = find_default_agent(db, &executor_type).await? {
        return Ok(Some(agent));
    }

    if bootstrap_completed {
        tracing::debug!(
            executor_type,
            "skipping default agent creation after bootstrap"
        );
        return Ok(None);
    }

    let now = now_rfc3339();
    Ok(Some(
        AgentRepo::create(
            db,
            CreateAgent {
                id: new_uuid_v4(),
                name: default_agent_name(&kind),
                description: None,
                executor_type,
                model: None,
                reasoning_effort: None,
                permission_policy: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: true,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                prompt_template: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await?,
    ))
}

async fn find_default_agent(db: &SqliteDb, executor_type: &str) -> db::Result<Option<Agent>> {
    let page = AgentRepo::list(
        db,
        AgentListQuery {
            status: None,
            executor_type: Some(executor_type.to_owned()),
            capabilities: Vec::new(),
            page: PageRequest {
                cursor: None,
                limit: 500,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Asc,
            },
        },
    )
    .await?;
    Ok(page.items.into_iter().find(|agent| agent.is_default))
}

fn default_agent_name(kind: &ExecutorKind) -> String {
    let display_name = match kind {
        ExecutorKind::Embedded => "Embedded",
        ExecutorKind::Shell => "Shell",
        ExecutorKind::Codex => "Codex",
        ExecutorKind::ClaudeCode => "Claude Code",
        ExecutorKind::Cursor => "Cursor",
        ExecutorKind::Opencode => "OpenCode",
        ExecutorKind::Gemini => "Gemini",
        ExecutorKind::Smith => "Smith",
        ExecutorKind::Null => "Null",
    };
    format!("{display_name} Default")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn skips_missing_default_agent_creation_after_bootstrap() {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = SqliteDb::new(pool);
        SystemSettingRepo::set_setting(&db, "bootstrap_completed", "true", &db::now_rfc3339())
            .await
            .expect("setting writes");

        let registry = cli_adapters::test_support::test_registry();
        let agents = ensure_default_agents(&db, &registry)
            .await
            .expect("ensure succeeds");

        assert!(
            agents.is_empty(),
            "startup must not create new global defaults after bootstrap"
        );
    }
}
