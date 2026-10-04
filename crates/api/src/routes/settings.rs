use std::path::Path;

use api_types::{
    ForgeSettingResponse, SettingsResponse, UpdateAgentSettingsRequest, UpdateForgePathsRequest,
    UpdateServerSettingsRequest, UpdateSettingsRequest, UpdateWorkspaceSettingsRequest,
};
use axum::{extract::State, Json};
use config::{ConfigOverrides, ForgeConfig};
use serde::Serialize;
use serde_yaml::{Mapping, Value};

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::RequireAdmin,
    state::AppState,
};

pub async fn get_settings(
    _admin: RequireAdmin,
    State(state): State<AppState>,
) -> ApiResult<Json<SettingsResponse>> {
    Ok(Json(settings_response(&state).await?))
}

pub async fn update_settings(
    _admin: RequireAdmin,
    State(state): State<AppState>,
    Json(request): Json<UpdateSettingsRequest>,
) -> ApiResult<Json<SettingsResponse>> {
    if !has_updates(&request) {
        return Err(ApiError::bad_request("no settings provided"));
    }

    validate_update(&request)?;

    static SETTINGS_WRITE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = SETTINGS_WRITE_LOCK.lock().await;
    let cap_update = request
        .server
        .as_ref()
        .and_then(|server| server.max_concurrent_runs);
    let jobs_update = request
        .server
        .as_ref()
        .and_then(|server| server.build_jobs_per_run);
    let nice_update = request.server.as_ref().and_then(|server| server.run_nice);
    let budget_update = request
        .server
        .as_ref()
        .and_then(|server| server.usage_index_budget_mb);
    let path = state.config_path.as_path();
    let mut config = read_yaml_config(path).await?;
    apply_update(&mut config, request)?;
    write_yaml_config(path, &config).await?;
    if let Some(budget) = budget_update {
        state.usage_ledger_index.set_budget_mb(budget);
    }
    if let Some(cap) = cap_update {
        state.db.server_run_cap.set(
            cap,
            config::resolved_run_cap(cap),
            &config::embedded_machine_id(),
        );
    }

    state
        .run_process_policy
        .update(cap_update, jobs_update, nice_update);
    Ok(Json(settings_response(&state).await?))
}

async fn settings_response(state: &AppState) -> ApiResult<SettingsResponse> {
    let path = state.config_path.as_path();
    let pending_config = load_pending_config(path)?;
    let effective_config = state.effective_config.as_ref();

    let mut transaction = state
        .db
        .pool()
        .begin()
        .await
        .map_err(db::DbError::from)
        .map_err(services::ServiceError::from)?;
    let machine_cap =
        services::placement::capacity::server_machine_cap(&state.db, &mut transaction).await?;
    transaction
        .commit()
        .await
        .map_err(db::DbError::from)
        .map_err(services::ServiceError::from)?;
    let budget = state.run_process_policy.get();
    let settings = vec![
        ForgeSettingResponse {
            key: "server.logical_cores".into(),
            value: serde_json::json!(config::logical_cores()),
            effective_value: serde_json::json!(config::logical_cores()),
            restart_required: false,
        },
        ForgeSettingResponse {
            key: "server.build_jobs_per_run".into(),
            value: serde_json::json!(budget.build_jobs_per_run),
            effective_value: serde_json::json!(budget.build_jobs()),
            restart_required: false,
        },
        ForgeSettingResponse {
            key: "server.run_nice".into(),
            value: serde_json::json!(budget.run_nice),
            effective_value: serde_json::json!(budget.run_nice),
            restart_required: false,
        },
        ForgeSettingResponse {
            key: "server.usage_index_budget_mb".to_owned(),
            value: serde_json::json!(state.usage_ledger_index.configured_budget_mb()),
            effective_value: serde_json::json!(state.usage_ledger_index.budget_mb()),
            restart_required: false,
        },
        ForgeSettingResponse {
            key: "server.max_concurrent_runs".to_owned(),
            value: serde_json::json!(state.db.server_run_cap.configured()),
            effective_value: serde_json::json!(machine_cap),
            restart_required: false,
        },
        setting(
            "forge.data_dir",
            &effective_config.forge.data_dir.display().to_string(),
            &pending_config.forge.data_dir.display().to_string(),
        )?,
        setting(
            "server.bind",
            &effective_config.server.bind,
            &pending_config.server.bind,
        )?,
        setting(
            "server.mcp_enabled",
            &effective_config.server.mcp_enabled,
            &pending_config.server.mcp_enabled,
        )?,
        setting(
            "workspace.root",
            &effective_config.workspace.root.display().to_string(),
            &pending_config.workspace.root.display().to_string(),
        )?,
        setting(
            "workspace.cleanup_delay_seconds",
            &effective_config.workspace.cleanup_delay_seconds,
            &pending_config.workspace.cleanup_delay_seconds,
        )?,
        setting(
            "agent.max_concurrent_tasks",
            &effective_config.agent.max_concurrent_tasks,
            &pending_config.agent.max_concurrent_tasks,
        )?,
        setting(
            "agent.heartbeat_interval_seconds",
            &effective_config.agent.heartbeat_interval_seconds,
            &pending_config.agent.heartbeat_interval_seconds,
        )?,
        setting(
            "agent.max_missed_heartbeats",
            &effective_config.agent.max_missed_heartbeats,
            &pending_config.agent.max_missed_heartbeats,
        )?,
    ];
    let restart_required = settings.iter().any(|setting| setting.restart_required);

    Ok(SettingsResponse {
        config_path: path.to_string_lossy().into_owned(),
        restart_required,
        settings,
    })
}

fn setting<T: Serialize>(
    key: &str,
    effective_value: &T,
    pending_value: &T,
) -> ApiResult<ForgeSettingResponse> {
    let effective_value = serde_json::to_value(effective_value).map_err(|error| {
        ApiError::internal(format!("failed to serialize effective setting: {error}"))
    })?;
    let value = serde_json::to_value(pending_value)
        .map_err(|error| ApiError::internal(format!("failed to serialize setting: {error}")))?;
    let restart_required = effective_value != value;

    Ok(ForgeSettingResponse {
        key: key.to_owned(),
        value,
        effective_value,
        restart_required,
    })
}

fn has_updates(request: &UpdateSettingsRequest) -> bool {
    request
        .forge
        .as_ref()
        .is_some_and(|forge| forge.data_dir.is_some())
        || request.server.as_ref().is_some_and(|server| {
            server.bind.is_some()
                || server.mcp_enabled.is_some()
                || server.max_concurrent_runs.is_some()
                || server.build_jobs_per_run.is_some()
                || server.run_nice.is_some()
                || server.usage_index_budget_mb.is_some()
        })
        || request.workspace.as_ref().is_some_and(|workspace| {
            workspace.root.is_some() || workspace.cleanup_delay_seconds.is_some()
        })
        || request.agent.as_ref().is_some_and(|agent| {
            agent.max_concurrent_tasks.is_some()
                || agent.heartbeat_interval_seconds.is_some()
                || agent.max_missed_heartbeats.is_some()
        })
        || request.project.is_some()
}

fn validate_update(request: &UpdateSettingsRequest) -> ApiResult<()> {
    if let Some(forge) = &request.forge {
        validate_forge(forge)?;
    }
    if let Some(server) = &request.server {
        validate_server(server)?;
    }
    if let Some(workspace) = &request.workspace {
        validate_workspace(workspace)?;
    }
    if let Some(agent) = &request.agent {
        validate_agent(agent)?;
    }
    Ok(())
}

fn validate_forge(forge: &UpdateForgePathsRequest) -> ApiResult<()> {
    if let Some(data_dir) = &forge.data_dir {
        validate_non_empty("forge.data_dir", data_dir)?;
    }
    Ok(())
}

fn validate_server(server: &UpdateServerSettingsRequest) -> ApiResult<()> {
    if server.run_nice.is_some_and(|nice| nice > 19) {
        return Err(ApiError::bad_request(
            "server.run_nice must be between 0 and 19",
        ));
    }
    if let Some(bind) = &server.bind {
        validate_non_empty("server.bind", bind)?;
    }
    Ok(())
}

fn validate_workspace(workspace: &UpdateWorkspaceSettingsRequest) -> ApiResult<()> {
    if let Some(root) = &workspace.root {
        validate_non_empty("workspace.root", root)?;
    }
    if matches!(workspace.cleanup_delay_seconds, Some(0)) {
        return Err(ApiError::bad_request(
            "workspace.cleanup_delay_seconds must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_agent(agent: &UpdateAgentSettingsRequest) -> ApiResult<()> {
    if matches!(agent.max_concurrent_tasks, Some(0)) {
        return Err(ApiError::bad_request(
            "agent.max_concurrent_tasks must be greater than zero",
        ));
    }
    if matches!(agent.heartbeat_interval_seconds, Some(0)) {
        return Err(ApiError::bad_request(
            "agent.heartbeat_interval_seconds must be greater than zero",
        ));
    }
    if matches!(agent.max_missed_heartbeats, Some(0)) {
        return Err(ApiError::bad_request(
            "agent.max_missed_heartbeats must be greater than zero",
        ));
    }
    Ok(())
}

fn validate_non_empty(key: &str, value: &str) -> ApiResult<()> {
    if value.trim().is_empty() {
        return Err(ApiError::bad_request(format!("{key} must not be empty")));
    }
    Ok(())
}

fn apply_update(config: &mut Value, request: UpdateSettingsRequest) -> ApiResult<()> {
    ensure_mapping(config)?;

    if let Some(forge) = request.forge {
        if let Some(data_dir) = forge.data_dir {
            set_path(config, &["forge", "data_dir"], Value::String(data_dir))?;
        }
    }
    if let Some(server) = request.server {
        if let Some(jobs) = server.build_jobs_per_run {
            set_path(
                config,
                &["server", "build_jobs_per_run"],
                yaml_number(jobs)?,
            )?;
        }
        if let Some(nice) = server.run_nice {
            set_path(config, &["server", "run_nice"], yaml_number(nice)?)?;
        }
        if let Some(budget) = server.usage_index_budget_mb {
            set_path(
                config,
                &["server", "usage_index_budget_mb"],
                yaml_number(budget)?,
            )?;
        }
        if let Some(cap) = server.max_concurrent_runs {
            set_path(
                config,
                &["server", "max_concurrent_runs"],
                yaml_number(cap)?,
            )?;
        }
        if let Some(bind) = server.bind {
            set_path(config, &["server", "bind"], Value::String(bind))?;
        }
        if let Some(mcp_enabled) = server.mcp_enabled {
            set_path(config, &["server", "mcp_enabled"], Value::Bool(mcp_enabled))?;
        }
    }
    if let Some(workspace) = request.workspace {
        if let Some(root) = workspace.root {
            set_path(config, &["workspace", "root"], Value::String(root))?;
        }
        if let Some(cleanup_delay_seconds) = workspace.cleanup_delay_seconds {
            set_path(
                config,
                &["workspace", "cleanup_delay_seconds"],
                yaml_number(cleanup_delay_seconds)?,
            )?;
        }
    }
    if let Some(agent) = request.agent {
        if let Some(max_concurrent_tasks) = agent.max_concurrent_tasks {
            set_path(
                config,
                &["agent", "max_concurrent_tasks"],
                yaml_number(max_concurrent_tasks)?,
            )?;
        }
        if let Some(heartbeat_interval_seconds) = agent.heartbeat_interval_seconds {
            set_path(
                config,
                &["agent", "heartbeat_interval_seconds"],
                yaml_number(heartbeat_interval_seconds)?,
            )?;
        }
        if let Some(max_missed_heartbeats) = agent.max_missed_heartbeats {
            set_path(
                config,
                &["agent", "max_missed_heartbeats"],
                yaml_number(max_missed_heartbeats)?,
            )?;
        }
    }
    if let Some(project) = request.project {
        set_path(
            config,
            &["project"],
            serde_yaml::to_value(project).map_err(|error| {
                ApiError::bad_request(format!("invalid project settings: {error}"))
            })?,
        )?;
    }

    Ok(())
}

async fn read_yaml_config(path: &Path) -> ApiResult<Value> {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) if contents.trim().is_empty() => Ok(Value::Mapping(Mapping::new())),
        Ok(contents) => serde_yaml::from_str(&contents).map_err(|error| {
            ApiError::bad_request(format!(
                "failed to parse config {}: {error}",
                path.display()
            ))
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(Value::Mapping(Mapping::new()))
        }
        Err(error) => Err(ApiError::internal(format!(
            "failed to read config {}: {error}",
            path.display()
        ))),
    }
}

async fn write_yaml_config(path: &Path, config: &Value) -> ApiResult<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|error| {
            ApiError::internal(format!(
                "failed to create config directory {}: {error}",
                parent.display()
            ))
        })?;
    }
    let contents = serde_yaml::to_string(config)
        .map_err(|error| ApiError::internal(format!("failed to serialize config: {error}")))?;
    tokio::fs::write(path, contents).await.map_err(|error| {
        ApiError::internal(format!(
            "failed to write config {}: {error}",
            path.display()
        ))
    })
}

fn load_pending_config(path: &Path) -> ApiResult<ForgeConfig> {
    ForgeConfig::load(Some(path), ConfigOverrides::default())
        .map_err(|error| ApiError::bad_request(error.to_string()))
}

fn set_path(config: &mut Value, path: &[&str], value: Value) -> ApiResult<()> {
    let Some((leaf, parents)) = path.split_last() else {
        return Err(ApiError::bad_request("setting path must not be empty"));
    };
    let mut current = config;
    for key in parents {
        let mapping = ensure_mapping(current)?;
        current = mapping
            .entry(Value::String((*key).to_owned()))
            .or_insert_with(|| Value::Mapping(Mapping::new()));
    }
    let mapping = ensure_mapping(current)?;
    mapping.insert(Value::String((*leaf).to_owned()), value);
    Ok(())
}

fn ensure_mapping(value: &mut Value) -> ApiResult<&mut Mapping> {
    if matches!(value, Value::Null) {
        *value = Value::Mapping(Mapping::new());
    }
    value
        .as_mapping_mut()
        .ok_or_else(|| ApiError::bad_request("config root must be a YAML object"))
}

fn yaml_number<T: Serialize>(value: T) -> ApiResult<Value> {
    serde_yaml::to_value(value)
        .map_err(|error| ApiError::internal(format!("failed to serialize setting: {error}")))
}
