//! Post-creation provisioning for Charter-backed (Product Genesis) Projects.
//!
//! Genesis produces a Project with a Charter, Project Agent binding, and
//! handoff — but nothing executable: no repository and no executor role
//! defaults, so every proposed Task would sit in the backlog with the
//! dispatcher silently skipping it. This module closes that gap after the
//! atomic create commits: it initializes a local git repository under the
//! workspace root, registers it as the primary repo, and seeds default
//! coder/reviewer role assignments from the account's executor agents
//! (preferring the provider family the user selected for the Project Agent).
//!
//! Provisioning is idempotent — a Project that already has a primary repo or
//! role defaults is left untouched — and best-effort: a failure is reported
//! to the caller for logging but must not fail the committed Project create.

use std::{path::PathBuf, sync::Arc};

use db::{
    new_uuid_v4, now_rfc3339, Agent, CreateRepo, Project, ProjectRepo, RepoRepo, SqliteDb,
    UpdateProject, WorkMode,
};
use serde_json::{json, Value};
use sqlx::Row;

use crate::{
    agent_service::{compute_effective_status, EffectiveStatus},
    Result, ServiceError,
};

const DEFAULT_BRANCH: &str = "main";

/// Make a freshly created Genesis Project executable. Called after the
/// atomic Charter-approval create commits; safe to call again on replays.
pub async fn provision_genesis_project(db: &Arc<SqliteDb>, project_id: &str) -> Result<()> {
    let Some(project) = ProjectRepo::get_by_id(&**db, project_id).await? else {
        return Ok(());
    };
    let project = provision_primary_repo(db, project).await?;
    seed_default_role_assignments(db, &project).await
}

/// Initialize a local git repository for the Project and register it as the
/// primary repo. The repository lives under `<workspace_root>/repos/` with a
/// deterministic directory name, gets an initial commit (worktrees need a
/// resolvable base ref), and is registered local-first: `remote_url` mirrors
/// the local path, matching what the repo form does for local repos.
async fn provision_primary_repo(db: &Arc<SqliteDb>, project: Project) -> Result<Project> {
    if project.primary_repo_id.is_some() {
        return Ok(project);
    }

    let repo_dir_name = repo_directory_name(&project.name, &project.id);
    let repo_path = repos_root().join(&repo_dir_name);
    tokio::fs::create_dir_all(&repo_path)
        .await
        .map_err(|error| {
            ServiceError::invalid_operation(format!(
                "create Project repository directory {}: {error}",
                repo_path.display()
            ))
        })?;

    if !git::is_git_repo(&repo_path).await {
        git::init(&repo_path).await?;
        let readme = format!(
            "# {}\n\nRepository created by Forge Product Genesis.\n",
            project.name
        );
        tokio::fs::write(repo_path.join("README.md"), readme)
            .await
            .map_err(|error| {
                ServiceError::invalid_operation(format!("write Project repository README: {error}"))
            })?;
        git::commit_all(&repo_path, "Initialize repository").await?;
        if !git::branch_exists(&repo_path, DEFAULT_BRANCH).await? {
            // `git init` follows the host's init.defaultBranch; normalize so
            // the registered default branch always resolves.
            git::rename_current_branch(&repo_path, DEFAULT_BRANCH).await?;
        }
    } else if !git::branch_exists(&repo_path, DEFAULT_BRANCH).await? {
        return Err(ServiceError::invalid_operation(format!(
            "existing repository at {} has no '{DEFAULT_BRANCH}' branch",
            repo_path.display()
        )));
    }

    let local_path = repo_path.to_string_lossy().into_owned();
    let now = now_rfc3339();
    let repo = RepoRepo::create(
        &**db,
        CreateRepo {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            name: repo_dir_name,
            local_path: Some(local_path.clone()),
            remote_url: local_path,
            work_mode: WorkMode::DirectMerge,
            default_branch: DEFAULT_BRANCH.to_owned(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await?;
    let updated = ProjectRepo::update(
        &**db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: None,
            primary_repo_id: Some(Some(repo.id.clone())),
            paused_at: None,
            updated_at: now,
        },
    )
    .await?;
    tracing::info!(
        project_id = %project.id,
        repo_id = %repo.id,
        path = %repo.local_path.as_deref().unwrap_or_default(),
        "provisioned Genesis Project repository"
    );
    Ok(updated)
}

/// Seed `settings.default_role_assignments` with coder/reviewer executor
/// agents so the task dispatcher can schedule Genesis work unattended.
/// Prefers agents whose profile matches the Project Agent's provider family,
/// and never selects Main/Project-bound identities (they can never hold a
/// repository WorkspaceLease).
async fn seed_default_role_assignments(db: &Arc<SqliteDb>, project: &Project) -> Result<()> {
    let mut settings: Value = serde_json::from_str(&project.settings).map_err(|error| {
        ServiceError::invalid_operation(format!("invalid Project settings: {error}"))
    })?;
    let existing = settings
        .get("default_role_assignments")
        .and_then(Value::as_array)
        .is_some_and(|assignments| !assignments.is_empty());
    if existing {
        return Ok(());
    }

    let preferred = sqlx::query(
        "SELECT p.provider, p.executor_type
         FROM project_agent_binding AS b
         JOIN agent_profile AS p ON p.id = b.profile_id
         WHERE b.project_id = ? AND b.state = 'active'
         LIMIT 1",
    )
    .bind(&project.id)
    .fetch_optional(db.pool())
    .await?;
    let preferred_provider: Option<String> = match &preferred {
        Some(row) => row
            .try_get::<Option<String>, _>("provider")?
            .or(row.try_get::<Option<String>, _>("executor_type")?),
        None => None,
    };

    let candidates = eligible_worker_agents(db, project).await?;
    let mut scored: Vec<(i64, Agent)> = Vec::new();
    for agent in candidates {
        let mut score = 0;
        if let Some(preferred) = preferred_provider.as_deref() {
            let matches_provider =
                agent.provider.as_deref() == Some(preferred) || agent.executor_type == preferred;
            if matches_provider {
                score += 2;
            }
        }
        if agent.is_default {
            score += 1;
        }
        // A credential-less gemini agent cannot run headless (the CLI exits
        // immediately without an injected API key); rank it below every agent
        // that can actually execute.
        if agent.executor_type == "gemini" && agent.credential_ref.is_none() {
            score -= 10;
        }
        scored.push((score, agent));
    }
    scored.sort_by(|(left_score, left), (right_score, right)| {
        right_score
            .cmp(left_score)
            .then_with(|| left.name.cmp(&right.name))
    });
    let Some((_, coder)) = scored.first() else {
        tracing::warn!(
            project_id = %project.id,
            "no eligible executor agent found; Genesis Project has no default role assignments"
        );
        return Ok(());
    };
    let reviewer = scored
        .iter()
        .find(|(_, agent)| agent.id != coder.id)
        .map(|(_, agent)| agent)
        .unwrap_or(coder);

    settings["default_role_assignments"] = json!([
        {"role_name": "coder", "assignee_type": "agent", "assignee_id": coder.id},
        {"role_name": "reviewer", "assignee_type": "agent", "assignee_id": reviewer.id},
    ]);
    ProjectRepo::update(
        &**db,
        UpdateProject {
            id: project.id.clone(),
            name: None,
            settings: Some(settings.to_string()),
            primary_repo_id: None,
            paused_at: None,
            updated_at: now_rfc3339(),
        },
    )
    .await?;
    tracing::info!(
        project_id = %project.id,
        coder = %coder.id,
        reviewer = %reviewer.id,
        "seeded Genesis Project default role assignments"
    );
    Ok(())
}

/// Executor agents this Project may dispatch to: usable in the Project,
/// effectively Active, and not bound as a Main or Project Agent identity.
async fn eligible_worker_agents(db: &Arc<SqliteDb>, project: &Project) -> Result<Vec<Agent>> {
    let owner_id = project.owner_id.clone().unwrap_or_default();
    let agents = db
        .list_agents_usable_in_project(&project.id, &owner_id)
        .await?;
    let mut eligible = Vec::new();
    for agent in agents {
        if compute_effective_status(db, &agent).await? != EffectiveStatus::Active {
            continue;
        }
        let orchestration_bound = sqlx::query_scalar::<_, i64>(
            "SELECT EXISTS (
                 SELECT 1 FROM project_agent_binding
                 WHERE identity_id = ? AND state = 'active'
             ) OR EXISTS (
                 SELECT 1 FROM account_main_agent_binding
                 WHERE identity_id = ? AND state = 'active'
             )",
        )
        .bind(&agent.id)
        .bind(&agent.id)
        .fetch_one(db.pool())
        .await?;
        if orchestration_bound != 0 {
            continue;
        }
        eligible.push(agent);
    }
    Ok(eligible)
}

fn repos_root() -> PathBuf {
    crate::task_service::workspace::default_workspace_root().join("repos")
}

/// Deterministic, collision-free directory name: sanitized project name plus
/// the first 8 characters of the project id (mirrors task branch naming).
fn repo_directory_name(project_name: &str, project_id: &str) -> String {
    let slug: String = project_name
        .to_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    let slug = if slug.is_empty() { "project" } else { &slug };
    let id_prefix: String = project_id.chars().take(8).collect();
    format!("{slug}-{id_prefix}")
}

#[cfg(test)]
mod tests {
    use super::repo_directory_name;

    #[test]
    fn repo_directory_name_is_slugged_and_deterministic() {
        assert_eq!(
            repo_directory_name("Simple Todo!", "ab591984-975f-4406"),
            "simple-todo-ab591984"
        );
        assert_eq!(repo_directory_name("---", "12345678-x"), "project-12345678");
    }
}
