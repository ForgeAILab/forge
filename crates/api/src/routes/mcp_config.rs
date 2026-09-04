use std::{
    env,
    path::{Path, PathBuf},
};

use api_types::{
    McpAction, McpAgent, McpConfigActionRequest, McpConfigQuery, McpConfigResponse, McpScope,
};
use axum::{
    extract::{Query, State},
    Json,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::fs;

use crate::{
    errors::{ApiError, ApiResult},
    routes::auth::AuthenticatedUser,
    state::AppState,
};
use db::{now_rfc3339, PersonalAccessTokenRepo, ProjectRepo, RepoRepo};

pub async fn get_mcp_config(
    Query(params): Query<McpConfigQuery>,
    State(state): State<AppState>,
    user: AuthenticatedUser,
) -> ApiResult<Json<McpConfigResponse>> {
    let agent = parse_agent(&params.agent)?;
    let scope = parse_scope(params.scope.as_deref())?;
    let config_path =
        resolve_config_path(&state, &agent, &scope, params.project_id.as_deref()).await?;
    let expected_url = mcp_url_for_scope_from_request(
        &state,
        params.public_base_url.as_deref(),
        &scope,
        params.project_id.as_deref(),
    )?;
    let url = match agent {
        McpAgent::Codex => read_codex_forge_url(&config_path).await?,
        _ => {
            let config = read_config(&config_path).await?;
            forge_url(&config)
        }
    };

    Ok(Json(McpConfigResponse {
        installed: mcp_url_is_usable(&state, &user, url.as_deref(), &expected_url).await,
        url,
        expected_url,
        config_path: config_path.to_string_lossy().into_owned(),
        agents: supported_agents(),
    }))
}

pub async fn update_mcp_config(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(body): Json<McpConfigActionRequest>,
) -> ApiResult<Json<McpConfigResponse>> {
    let agent = parse_agent(&body.agent)?;
    let scope = parse_scope(body.scope.as_deref())?;
    let action = parse_action(&body.action)?;
    let config_path =
        resolve_config_path(&state, &agent, &scope, body.project_id.as_deref()).await?;
    let expected_url = mcp_url_for_scope_from_request(
        &state,
        body.public_base_url.as_deref(),
        &scope,
        body.project_id.as_deref(),
    )?;

    let url = match agent {
        McpAgent::Codex => match action {
            McpAction::Install => {
                let contents = read_codex_config(&config_path).await?;
                let install_url = expected_url.clone();
                write_codex_config_if_changed(
                    &config_path,
                    &contents,
                    set_codex_forge_url(&contents, &install_url),
                )
                .await?;
                Some(install_url)
            }
            McpAction::Uninstall => {
                update_codex_config(&config_path, &expected_url, action).await?
            }
        },
        _ => {
            let mut config = read_config(&config_path).await?;

            match action {
                McpAction::Install => {
                    let install_url = expected_url.clone();
                    set_forge_url(&mut config, &install_url)?;
                    write_config(&config_path, &config).await?;
                }
                McpAction::Uninstall => {
                    if remove_forge(&mut config)? {
                        write_config(&config_path, &config).await?;
                    }
                }
            }

            forge_url(&config)
        }
    };

    Ok(Json(McpConfigResponse {
        installed: mcp_url_is_usable(&state, &user, url.as_deref(), &expected_url).await,
        url,
        expected_url,
        config_path: config_path.to_string_lossy().into_owned(),
        agents: supported_agents(),
    }))
}

fn mcp_url_for_scope_from_request(
    state: &AppState,
    request_public_base_url: Option<&str>,
    scope: &McpScope,
    project_id: Option<&str>,
) -> ApiResult<String> {
    let public_base_url = state
        .effective_config
        .server
        .public_base_url
        .as_deref()
        .or(request_public_base_url);
    mcp_url_for_scope_with_public_base_url(
        &state.effective_config.server.bind,
        public_base_url,
        scope,
        project_id,
    )
}

fn mcp_url_for_scope_with_public_base_url(
    bind: &str,
    public_base_url: Option<&str>,
    scope: &McpScope,
    project_id: Option<&str>,
) -> ApiResult<String> {
    let base = mcp_base_url(bind, public_base_url)?;
    let base = format!("{base}/mcp");
    match (
        scope,
        project_id.map(str::trim).filter(|value| !value.is_empty()),
    ) {
        (McpScope::Project, Some(project_id)) => Ok(format!(
            "{base}?project_id={}",
            percent_encode_query(project_id)
        )),
        (McpScope::Project, None) => Err(ApiError::bad_request_with_code(
            "project_id_required",
            "project MCP install requires project_id",
        )),
        _ => Ok(base),
    }
}

fn mcp_base_url(bind: &str, public_base_url: Option<&str>) -> ApiResult<String> {
    let Some(public_base_url) = public_base_url
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(format!("http://{bind}"));
    };

    let parsed = url::Url::parse(public_base_url).map_err(|_| {
        ApiError::bad_request_with_code(
            "invalid_public_base_url",
            "public_base_url must be an absolute http(s) URL",
        )
    })?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(ApiError::bad_request_with_code(
            "invalid_public_base_url",
            "public_base_url must be an absolute http(s) URL",
        ));
    }
    Ok(parsed.origin().ascii_serialization())
}

fn percent_encode_query(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char)
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

async fn mcp_url_is_usable(
    state: &AppState,
    user: &AuthenticatedUser,
    url: Option<&str>,
    expected_url: &str,
) -> bool {
    let Some(url) = url else {
        return false;
    };
    if url == expected_url {
        return true;
    }
    if !mcp_url_matches_expected_scope(url, expected_url) {
        return false;
    }
    let Some(token) = mcp_url_token(url) else {
        return false;
    };
    mcp_token_belongs_to_user(state, user, &token).await
}

fn mcp_url_matches_expected_scope(url: &str, expected_url: &str) -> bool {
    let (Ok(url), Ok(expected_url)) = (url::Url::parse(url), url::Url::parse(expected_url)) else {
        return false;
    };

    url.path() == expected_url.path()
        && query_param(&url, "project_id") == query_param(&expected_url, "project_id")
}

fn query_param(url: &url::Url, name: &str) -> Option<String> {
    url.query_pairs()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

async fn mcp_token_belongs_to_user(
    state: &AppState,
    user: &AuthenticatedUser,
    token: &str,
) -> bool {
    if !token.starts_with("fg_") {
        return state
            .auth_service
            .verify_token(token)
            .is_ok_and(|(user_id, _, _)| user_id == user.user_id);
    }

    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    let token_hash = hex::encode(hasher.finalize());
    let Ok(Some(pat)) =
        PersonalAccessTokenRepo::get_pat_by_token_hash(&*state.db, &token_hash).await
    else {
        return false;
    };
    if pat.user_id != user.user_id {
        return false;
    }
    if let Some(expires_at) = pat.expires_at {
        expires_at >= now_rfc3339()
    } else {
        true
    }
}

fn mcp_url_token(url: &str) -> Option<String> {
    url::Url::parse(url)
        .ok()?
        .query_pairs()
        .find(|(key, value)| key == "token" && !value.trim().is_empty())
        .map(|(_, value)| value.into_owned())
}

fn parse_agent(value: &str) -> ApiResult<McpAgent> {
    match value {
        "claude" => Ok(McpAgent::Claude),
        "cursor" => Ok(McpAgent::Cursor),
        "codex" => Ok(McpAgent::Codex),
        value => Err(ApiError::bad_request(format!(
            "invalid agent: {value}. valid values: claude, cursor, codex"
        ))),
    }
}

fn parse_scope(value: Option<&str>) -> ApiResult<McpScope> {
    match value.unwrap_or("project") {
        "project" => Ok(McpScope::Project),
        "local" => Ok(McpScope::Local),
        "user" => Ok(McpScope::User),
        value => Err(ApiError::bad_request(format!(
            "invalid scope: {value}. valid values: project, local, user"
        ))),
    }
}

fn parse_action(value: &str) -> ApiResult<McpAction> {
    match value {
        "install" => Ok(McpAction::Install),
        "uninstall" => Ok(McpAction::Uninstall),
        value => Err(ApiError::bad_request(format!(
            "invalid action: {value}. valid values: install, uninstall"
        ))),
    }
}

async fn resolve_config_path(
    state: &AppState,
    agent: &McpAgent,
    scope: &McpScope,
    project_id: Option<&str>,
) -> ApiResult<PathBuf> {
    let root = match scope {
        McpScope::Project | McpScope::Local => match project_id.map(str::trim) {
            Some(project_id) if !project_id.is_empty() => {
                project_config_root(state, project_id).await?
            }
            _ => env::current_dir()?,
        },
        McpScope::User => env::current_dir()?,
    };
    let home = env::var("HOME")
        .map(PathBuf::from)
        .map_err(|_| ApiError::internal("HOME is not set"))?;
    Ok(resolve_config_path_from(agent, scope, &root, &home))
}

async fn project_config_root(state: &AppState, project_id: &str) -> ApiResult<PathBuf> {
    let project = ProjectRepo::get_by_id(&*state.db, project_id)
        .await?
        .ok_or_else(|| ApiError::not_found("project", project_id.to_owned()))?;
    let Some(repo_id) = project.primary_repo_id else {
        return Err(ApiError::bad_request_with_code(
            "primary_repo_required",
            format!("project {project_id} does not have a primary repo"),
        ));
    };
    let repo = RepoRepo::get_by_id(&*state.db, &repo_id)
        .await?
        .ok_or_else(|| ApiError::not_found("repo", repo_id))?;
    let Some(local_path) = repo.local_path else {
        return Err(ApiError::bad_request_with_code(
            "local_repo_path_required",
            format!("project {project_id} primary repo does not have a local path"),
        ));
    };
    Ok(PathBuf::from(local_path))
}

fn resolve_config_path_from(
    agent: &McpAgent,
    scope: &McpScope,
    cwd: &Path,
    home: &Path,
) -> PathBuf {
    match (agent, scope) {
        (McpAgent::Claude, McpScope::Project) => cwd.join(".claude/settings.json"),
        (McpAgent::Claude, McpScope::Local) => cwd.join(".claude/settings.local.json"),
        (McpAgent::Claude, McpScope::User) => home.join(".claude/settings.json"),
        (McpAgent::Cursor, McpScope::Project | McpScope::Local) => cwd.join(".cursor/mcp.json"),
        (McpAgent::Cursor, McpScope::User) => home.join(".cursor/mcp.json"),
        (McpAgent::Codex, McpScope::Project | McpScope::Local) => cwd.join(".codex/config.toml"),
        (McpAgent::Codex, McpScope::User) => home.join(".codex/config.toml"),
    }
}

async fn read_config(path: &Path) -> ApiResult<Value> {
    match fs::read_to_string(path).await {
        Ok(contents) => Ok(serde_json::from_str(&contents)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Value::Object(Map::new())),
        Err(error) => Err(error.into()),
    }
}

async fn write_config(path: &Path, value: &Value) -> ApiResult<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }

    let mut contents = serde_json::to_vec_pretty(value)?;
    contents.push(b'\n');
    fs::write(path, contents).await?;
    Ok(())
}

fn set_forge_url(config: &mut Value, url: &str) -> ApiResult<()> {
    let root = config
        .as_object_mut()
        .ok_or_else(|| ApiError::bad_request("config must be a JSON object"))?;
    let mcp_servers = root
        .entry("mcpServers".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let mcp_servers = mcp_servers
        .as_object_mut()
        .ok_or_else(|| ApiError::bad_request("mcpServers must be a JSON object"))?;
    let forge = mcp_servers
        .entry("forge".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let forge = forge
        .as_object_mut()
        .ok_or_else(|| ApiError::bad_request("mcpServers.forge must be a JSON object"))?;
    forge.insert("type".to_string(), Value::String("http".to_string()));
    forge.insert("url".to_string(), Value::String(url.to_string()));
    Ok(())
}

fn remove_forge(config: &mut Value) -> ApiResult<bool> {
    let root = config
        .as_object_mut()
        .ok_or_else(|| ApiError::bad_request("config must be a JSON object"))?;
    let Some(mcp_servers) = root.get_mut("mcpServers") else {
        return Ok(false);
    };
    let mcp_servers = mcp_servers
        .as_object_mut()
        .ok_or_else(|| ApiError::bad_request("mcpServers must be a JSON object"))?;
    Ok(mcp_servers.remove("forge").is_some())
}

fn forge_url(config: &Value) -> Option<String> {
    config
        .get("mcpServers")
        .and_then(|value| value.get("forge"))
        .and_then(|value| value.get("url"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

async fn read_codex_config(path: &Path) -> ApiResult<String> {
    match fs::read_to_string(path).await {
        Ok(contents) => Ok(contents),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(error.into()),
    }
}

async fn read_codex_forge_url(path: &Path) -> ApiResult<Option<String>> {
    let contents = read_codex_config(path).await?;
    Ok(codex_forge_url(&contents))
}

async fn update_codex_config(
    path: &Path,
    mcp_url: &str,
    action: McpAction,
) -> ApiResult<Option<String>> {
    let contents = read_codex_config(path).await?;
    let next = match action {
        McpAction::Install => set_codex_forge_url(&contents, mcp_url),
        McpAction::Uninstall => remove_codex_forge(&contents),
    };

    write_codex_config_if_changed(path, &contents, next).await?;

    Ok(codex_forge_url(&read_codex_config(path).await?))
}

async fn write_codex_config_if_changed(path: &Path, previous: &str, next: String) -> ApiResult<()> {
    if next != previous {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        fs::write(path, next.as_bytes()).await?;
    }
    Ok(())
}

fn codex_forge_url(contents: &str) -> Option<String> {
    codex_forge_block(contents).and_then(|block| {
        block.lines().find_map(|line| {
            let (key, value) = line.split_once('=')?;
            if key.trim() != "url" {
                return None;
            }
            Some(unquote_toml_string(value.trim()))
        })
    })
}

fn set_codex_forge_url(contents: &str, url: &str) -> String {
    let mut next = remove_codex_forge(contents);
    if !next.ends_with('\n') && !next.is_empty() {
        next.push('\n');
    }
    if !next.is_empty() {
        next.push('\n');
    }
    next.push_str("[mcp_servers.forge]\n");
    next.push_str("url = ");
    next.push_str(&toml_string(url));
    next.push('\n');
    next
}

fn remove_codex_forge(contents: &str) -> String {
    let Some((start, end)) = codex_forge_block_range(contents) else {
        return contents.to_owned();
    };
    let mut next = String::new();
    next.push_str(&contents[..start]);
    next.push_str(&contents[end..]);
    while next.contains("\n\n\n") {
        next = next.replace("\n\n\n", "\n\n");
    }
    next
}

fn codex_forge_block(contents: &str) -> Option<&str> {
    let (start, end) = codex_forge_block_range(contents)?;
    Some(&contents[start..end])
}

fn codex_forge_block_range(contents: &str) -> Option<(usize, usize)> {
    let mut start = None;
    let mut end = contents.len();

    for (offset, line) in toml_lines(contents) {
        let trimmed = line.trim();
        if trimmed == "[mcp_servers.forge]" {
            start = Some(offset);
            continue;
        }
        if start.is_some()
            && trimmed.starts_with('[')
            && trimmed.ends_with(']')
            && trimmed != "[mcp_servers.forge]"
        {
            end = offset;
            break;
        }
    }

    start.map(|start| (start, end))
}

fn toml_lines(contents: &str) -> impl Iterator<Item = (usize, &str)> {
    std::iter::once(0)
        .chain(contents.match_indices('\n').map(|(offset, _)| offset + 1))
        .filter(|offset| *offset < contents.len())
        .map(|offset| {
            let line = contents[offset..].lines().next().unwrap_or("");
            (offset, line)
        })
}

fn toml_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_owned())
}

fn unquote_toml_string(value: &str) -> String {
    serde_json::from_str(value).unwrap_or_else(|_| value.trim_matches('"').to_owned())
}

fn supported_agents() -> Vec<String> {
    vec!["claude".into(), "cursor".into(), "codex".into()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use std::{
        ffi::OsString,
        sync::{Arc, Mutex},
        time::{SystemTime, UNIX_EPOCH},
    };

    static TEST_ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn resolve_config_path_uses_shared_project_file_for_claude() {
        let cwd = Path::new("/repo");
        let home = Path::new("/home/test");

        assert_eq!(
            resolve_config_path_from(&McpAgent::Claude, &McpScope::Project, cwd, home),
            PathBuf::from("/repo/.claude/settings.json")
        );
        assert_eq!(
            resolve_config_path_from(&McpAgent::Claude, &McpScope::Local, cwd, home),
            PathBuf::from("/repo/.claude/settings.local.json")
        );
        assert_eq!(
            resolve_config_path_from(&McpAgent::Claude, &McpScope::User, cwd, home),
            PathBuf::from("/home/test/.claude/settings.json")
        );
    }

    #[test]
    fn resolve_config_path_keeps_cursor_local_and_project_identical() {
        let cwd = Path::new("/repo");
        let home = Path::new("/home/test");

        assert_eq!(
            resolve_config_path_from(&McpAgent::Cursor, &McpScope::Project, cwd, home),
            PathBuf::from("/repo/.cursor/mcp.json")
        );
        assert_eq!(
            resolve_config_path_from(&McpAgent::Cursor, &McpScope::Local, cwd, home),
            PathBuf::from("/repo/.cursor/mcp.json")
        );
        assert_eq!(
            resolve_config_path_from(&McpAgent::Cursor, &McpScope::User, cwd, home),
            PathBuf::from("/home/test/.cursor/mcp.json")
        );
    }

    #[test]
    fn resolve_config_path_uses_codex_config_toml() {
        let cwd = Path::new("/repo");
        let home = Path::new("/home/test");

        assert_eq!(
            resolve_config_path_from(&McpAgent::Codex, &McpScope::Project, cwd, home),
            PathBuf::from("/repo/.codex/config.toml")
        );
        assert_eq!(
            resolve_config_path_from(&McpAgent::Codex, &McpScope::User, cwd, home),
            PathBuf::from("/home/test/.codex/config.toml")
        );
    }

    #[test]
    fn set_forge_url_writes_http_transport_type() {
        let mut config = Value::Object(Map::new());

        set_forge_url(&mut config, "http://127.0.0.1:8080/mcp").expect("set forge url");

        assert_eq!(config["mcpServers"]["forge"]["type"], "http");
        assert_eq!(
            config["mcpServers"]["forge"]["url"],
            "http://127.0.0.1:8080/mcp"
        );
    }

    #[test]
    fn mcp_url_scopes_project_installs() {
        let bind = "127.0.0.1:8080";
        assert_eq!(
            mcp_url_for_scope_with_public_base_url(
                bind,
                None,
                &McpScope::Project,
                Some("project 1"),
            )
            .expect("project URL builds"),
            "http://127.0.0.1:8080/mcp?project_id=project%201"
        );
        assert_eq!(
            mcp_url_for_scope_with_public_base_url(bind, None, &McpScope::User, None)
                .expect("user URL builds"),
            "http://127.0.0.1:8080/mcp"
        );
        assert!(
            mcp_url_for_scope_with_public_base_url(bind, None, &McpScope::Project, None).is_err()
        );
    }

    #[test]
    fn mcp_url_uses_public_base_url_origin_when_provided() {
        assert_eq!(
            mcp_url_for_scope_with_public_base_url(
                "0.0.0.0:8080",
                Some("https://forge.example.com/app"),
                &McpScope::Project,
                Some("project 1"),
            )
            .expect("project URL builds"),
            "https://forge.example.com/mcp?project_id=project%201"
        );
        assert_eq!(
            mcp_url_for_scope_with_public_base_url(
                "0.0.0.0:8080",
                Some("http://192.168.1.20:8080"),
                &McpScope::User,
                None,
            )
            .expect("user URL builds"),
            "http://192.168.1.20:8080/mcp"
        );
    }

    #[test]
    fn mcp_url_rejects_invalid_public_base_url() {
        assert!(mcp_url_for_scope_with_public_base_url(
            "0.0.0.0:8080",
            Some("ftp://forge.example.com"),
            &McpScope::User,
            None,
        )
        .is_err());
    }

    #[test]
    fn legacy_mcp_url_scope_matching_uses_path_and_project_id() {
        let expected = "http://127.0.0.1:8080/mcp?project_id=project%201";

        assert!(mcp_url_matches_expected_scope(
            "http://127.0.0.1:8080/mcp?project_id=project+1&token=fg_test",
            expected
        ));
        assert!(!mcp_url_matches_expected_scope(
            "http://127.0.0.1:8080/mcp?project_id=other&token=fg_test",
            expected
        ));
    }

    #[test]
    fn codex_forge_url_round_trips_config_toml() {
        let initial = r#"model = "gpt-5.2"

[mcp_servers.other]
command = "other"
"#;

        let installed = set_codex_forge_url(initial, "http://127.0.0.1:8080/mcp");
        assert_eq!(
            codex_forge_url(&installed).as_deref(),
            Some("http://127.0.0.1:8080/mcp")
        );
        assert!(installed.contains("[mcp_servers.other]"));

        let removed = remove_codex_forge(&installed);
        assert_eq!(codex_forge_url(&removed), None);
        assert!(removed.contains("[mcp_servers.other]"));
    }

    #[tokio::test]
    async fn resolve_config_path_uses_project_primary_repo_local_path() {
        let (state, project_id) =
            test_state_with_project_repo(Some("/Volumes/Data/codes/games/world".to_owned())).await;

        let config_path = resolve_config_path(
            &state,
            &McpAgent::Claude,
            &McpScope::Project,
            Some(&project_id),
        )
        .await
        .expect("project config path resolves");

        assert_eq!(
            config_path,
            PathBuf::from("/Volumes/Data/codes/games/world/.claude/settings.json")
        );
    }

    #[tokio::test]
    async fn resolve_config_path_rejects_project_repo_without_local_path() {
        let (state, project_id) = test_state_with_project_repo(None).await;

        let error = resolve_config_path(
            &state,
            &McpAgent::Claude,
            &McpScope::Project,
            Some(&project_id),
        )
        .await
        .expect_err("missing local path fails");

        let debug = format!("{error:?}");
        assert!(debug.contains("local_repo_path_required"));
        assert!(debug.contains("local path"));
    }

    #[tokio::test]
    async fn install_project_scope_writes_bare_url() {
        let repo_dir = unique_temp_dir("forge-mcp-project");
        let (state, project_id) =
            test_state_with_project_repo(Some(repo_dir.to_string_lossy().into_owned())).await;

        let response = install_config(&state, "claude", Some("project"), Some(&project_id)).await;
        let url = response.url.expect("install returns url");

        assert!(url.contains("/mcp?project_id=project-1"));
        assert!(!url.contains("token="));
        let _ = std::fs::remove_dir_all(repo_dir);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn install_user_scope_writes_bare_url() {
        let _guard = TEST_ENV_LOCK.lock().expect("test env lock");
        let home_dir = unique_temp_dir("forge-mcp-user");
        std::fs::create_dir_all(&home_dir).expect("home dir creates");
        let _home = EnvVarGuard::set("HOME", home_dir.as_os_str());
        let state = test_state_with_user().await;

        let response = install_config(&state, "claude", Some("user"), None).await;
        let url = response.url.expect("install returns url");
        let expected_url = mcp_url_for_scope_with_public_base_url(
            &state.effective_config.server.bind,
            None,
            &McpScope::User,
            None,
        )
        .expect("expected URL builds");

        assert_eq!(url, expected_url);
        assert!(url.contains("/mcp"));
        assert!(!url.contains("token="));
        let _ = std::fs::remove_dir_all(home_dir);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn install_user_scope_uses_request_public_base_url() {
        let _guard = TEST_ENV_LOCK.lock().expect("test env lock");
        let home_dir = unique_temp_dir("forge-mcp-user-public-base");
        std::fs::create_dir_all(&home_dir).expect("home dir creates");
        let _home = EnvVarGuard::set("HOME", home_dir.as_os_str());
        let state = test_state_with_user().await;

        let response = install_config_with_public_base_url(
            &state,
            "claude",
            Some("user"),
            None,
            Some("http://192.168.1.20:8080/app"),
        )
        .await;
        let url = response.url.expect("install returns url");

        assert_eq!(url, "http://192.168.1.20:8080/mcp");
        let _ = std::fs::remove_dir_all(home_dir);
    }

    #[tokio::test]
    async fn get_mcp_config_reports_installed_for_bare_url() {
        let repo_dir = unique_temp_dir("forge-mcp-bare");
        let (state, project_id) =
            test_state_with_project_repo(Some(repo_dir.to_string_lossy().into_owned())).await;
        let expected_url = mcp_url_for_scope_with_public_base_url(
            &state.effective_config.server.bind,
            None,
            &McpScope::Project,
            Some(&project_id),
        )
        .expect("expected URL builds");
        write_claude_config(&repo_dir, &expected_url);

        let response = get_config(&state, "claude", Some("project"), Some(&project_id)).await;

        assert!(response.installed);
        assert_eq!(response.url.as_deref(), Some(expected_url.as_str()));
        let _ = std::fs::remove_dir_all(repo_dir);
    }

    #[tokio::test]
    async fn get_mcp_config_reports_installed_for_legacy_valid_pat_url() {
        let repo_dir = unique_temp_dir("forge-mcp-pat");
        let (state, project_id) =
            test_state_with_project_repo(Some(repo_dir.to_string_lossy().into_owned())).await;
        let raw_token = "fg_0102030405060708091011121314151617181920";
        create_test_pat(&state, raw_token, "Legacy MCP token").await;
        let expected_url = mcp_url_for_scope_with_public_base_url(
            &state.effective_config.server.bind,
            None,
            &McpScope::Project,
            Some(&project_id),
        )
        .expect("expected URL builds");
        let installed_url = tokenized_url(&expected_url, raw_token);
        write_claude_config(&repo_dir, &installed_url);

        let response = get_config(&state, "claude", Some("project"), Some(&project_id)).await;

        assert!(response.installed);
        assert_eq!(response.url.as_deref(), Some(installed_url.as_str()));
        let _ = std::fs::remove_dir_all(repo_dir);
    }

    #[tokio::test]
    async fn install_does_not_create_personal_access_token_rows() {
        let repo_dir = unique_temp_dir("forge-mcp-no-pat");
        let (state, project_id) =
            test_state_with_project_repo(Some(repo_dir.to_string_lossy().into_owned())).await;
        let before = personal_access_token_count(&state).await;

        let response = install_config(&state, "claude", Some("project"), Some(&project_id)).await;
        let after = personal_access_token_count(&state).await;

        let url = response.url.expect("install returns url");
        assert!(!url.contains("token="));
        assert_eq!(after, before);
        let _ = std::fs::remove_dir_all(repo_dir);
    }

    async fn test_state_with_user() -> AppState {
        let pool = db::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        db::run_migrations(&pool).await.expect("migrations run");
        let db = Arc::new(db::SqliteDb::new(pool));
        db::UserRepo::create_user(
            &*db,
            &db::User {
                id: "test-user-id".to_owned(),
                email: "test@example.com".to_owned(),
                password_hash: "$2b$04$placeholder".to_owned(),
                display_name: None,
                is_admin: false,
                created_at: db::now_rfc3339(),
                updated_at: db::now_rfc3339(),
            },
        )
        .await
        .expect("user creates");
        let event_bus = Arc::new(events::EventBus::new(16));
        AppState::new(Arc::clone(&db), event_bus, true)
    }

    async fn test_state_with_project_repo(local_path: Option<String>) -> (AppState, String) {
        let state = test_state_with_user().await;
        let now = db::now_rfc3339();
        let project_id = "project-1".to_owned();
        let repo_id = "repo-1".to_owned();
        db::ProjectRepo::create(
            &*state.db,
            db::CreateProject {
                id: project_id.clone(),
                name: "Project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");
        db::RepoRepo::create(
            &*state.db,
            db::CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "world".to_owned(),
                remote_url: "https://example.com/world.git".to_owned(),
                local_path,
                work_mode: db::WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        db::ProjectRepo::update_at_version(
            &*state.db,
            db::UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id)),
                paused_at: None,
                updated_at: now,
            },
            db::ProjectRepo::get_by_id(&*state.db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project updates");

        (state, project_id)
    }

    async fn install_config(
        state: &AppState,
        agent: &str,
        scope: Option<&str>,
        project_id: Option<&str>,
    ) -> McpConfigResponse {
        install_config_with_public_base_url(state, agent, scope, project_id, None).await
    }

    async fn install_config_with_public_base_url(
        state: &AppState,
        agent: &str,
        scope: Option<&str>,
        project_id: Option<&str>,
        public_base_url: Option<&str>,
    ) -> McpConfigResponse {
        update_mcp_config(
            State(state.clone()),
            test_authenticated_user(),
            Json(McpConfigActionRequest {
                agent: agent.to_owned(),
                scope: scope.map(str::to_owned),
                project_id: project_id.map(str::to_owned),
                public_base_url: public_base_url.map(str::to_owned),
                action: "install".to_owned(),
            }),
        )
        .await
        .expect("install succeeds")
        .0
    }

    async fn get_config(
        state: &AppState,
        agent: &str,
        scope: Option<&str>,
        project_id: Option<&str>,
    ) -> McpConfigResponse {
        get_mcp_config(
            Query(McpConfigQuery {
                agent: agent.to_owned(),
                scope: scope.map(str::to_owned),
                project_id: project_id.map(str::to_owned),
                public_base_url: None,
            }),
            State(state.clone()),
            test_authenticated_user(),
        )
        .await
        .expect("get config succeeds")
        .0
    }

    fn write_claude_config(repo_dir: &Path, url: &str) {
        std::fs::create_dir_all(repo_dir.join(".claude")).expect("claude config dir creates");
        std::fs::write(
            repo_dir.join(".claude/settings.json"),
            serde_json::json!({
                "mcpServers": {
                    "forge": {
                        "type": "http",
                        "url": url
                    }
                }
            })
            .to_string(),
        )
        .expect("claude config writes");
    }

    fn tokenized_url(url: &str, token: &str) -> String {
        let separator = if url.contains('?') { '&' } else { '?' };
        format!("{url}{separator}token={token}")
    }

    async fn personal_access_token_count(state: &AppState) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM personal_access_token")
            .fetch_one(state.db.pool())
            .await
            .expect("PAT count query succeeds")
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }

    async fn create_test_pat(state: &AppState, raw_token: &str, name: &str) {
        let mut hasher = Sha256::new();
        hasher.update(raw_token.as_bytes());
        let token_hash = hex::encode(hasher.finalize());
        sqlx::query(
            "INSERT INTO personal_access_token \
             (id, user_id, name, token_hash, prefix, scopes, expires_at, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("pat-1")
        .bind("test-user-id")
        .bind(name)
        .bind(token_hash)
        .bind(&raw_token[..7])
        .bind("*")
        .bind(None::<String>)
        .bind(db::now_rfc3339())
        .execute(state.db.pool())
        .await
        .expect("PAT creates");
    }

    fn test_authenticated_user() -> AuthenticatedUser {
        AuthenticatedUser {
            user_id: "test-user-id".to_owned(),
            email: "test@example.com".to_owned(),
            is_admin: false,
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}
