use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use api_types::{AuthResponse, CreateTokenRequest, LoginRequest, TokenResponse};
use clap::Args;
use serde::{Deserialize, Serialize};

use crate::{client::ForgeClient, output::print_json, password_prompt, OutputFormat};

const CREDENTIALS_FILE: &str = "forge_ctl_credentials.json";
const DEFAULT_TOKEN_NAME: &str = "forge-ctl";

#[derive(Args)]
pub struct LoginArgs {
    /// Account email address.
    #[arg(long)]
    email: String,
    /// Account password. Prefer --password-stdin for scripts.
    #[arg(long, conflicts_with = "password_stdin")]
    password: Option<String>,
    /// Read the account password from stdin.
    #[arg(long)]
    password_stdin: bool,
    /// Name for the personal access token created for this CLI login.
    #[arg(long, default_value = DEFAULT_TOKEN_NAME)]
    token_name: String,
}

#[derive(Args)]
pub struct LogoutArgs {}

#[derive(Args)]
pub struct WhoamiArgs {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCredentials {
    pub server_url: String,
    pub token: String,
    pub token_id: Option<String>,
    pub token_prefix: Option<String>,
    pub email: Option<String>,
}

#[derive(Debug, Serialize)]
struct LoginOutput<'a> {
    server_url: &'a str,
    email: &'a str,
    token_id: Option<&'a str>,
    token_prefix: Option<&'a str>,
    credentials_path: String,
}

#[derive(Debug, Serialize)]
struct LogoutOutput {
    credentials_path: String,
    removed: bool,
}

#[derive(Debug, Serialize)]
struct WhoamiOutput<'a> {
    server_url: &'a str,
    email: Option<&'a str>,
    token_id: Option<&'a str>,
    token_prefix: Option<&'a str>,
    credentials_path: String,
}

impl LoginArgs {
    pub async fn run(&self, server: &str, output: &OutputFormat) -> Result<()> {
        let password = self.resolve_password()?;
        let client = ForgeClient::new_without_credentials(server);
        let auth: AuthResponse = client
            .post(
                "/api/v1/auth/login",
                &LoginRequest {
                    email: self.email.clone(),
                    password,
                },
            )
            .await?;
        let pat: TokenResponse = client
            .post_bearer(
                "/api/v1/auth/tokens",
                &auth.access_token,
                &CreateTokenRequest {
                    name: self.token_name.clone(),
                    expires_at: None,
                },
            )
            .await?;
        let token = pat
            .token
            .clone()
            .ok_or_else(|| anyhow!("login succeeded but token response did not include a token"))?;
        let credentials = StoredCredentials {
            server_url: normalize_server_url(server),
            token,
            token_id: Some(pat.id.clone()),
            token_prefix: Some(pat.prefix.clone()),
            email: Some(self.email.clone()),
        };
        let path = credentials_path();
        write_credentials(&path, &credentials)?;

        let response = LoginOutput {
            server_url: &credentials.server_url,
            email: credentials.email.as_deref().unwrap_or(""),
            token_id: credentials.token_id.as_deref(),
            token_prefix: credentials.token_prefix.as_deref(),
            credentials_path: path.to_string_lossy().into_owned(),
        };
        match output {
            OutputFormat::Json => print_json(&response),
            OutputFormat::Table => {
                println!(
                    "Logged in as {} for {}; credentials saved to {}",
                    response.email, response.server_url, response.credentials_path
                );
                Ok(())
            }
        }
    }

    fn resolve_password(&self) -> Result<String> {
        resolve_password_with_sources(
            self.password.as_deref(),
            self.password_stdin,
            password_prompt::stdin_is_terminal,
            || {
                let mut password = String::new();
                io::stdin()
                    .read_to_string(&mut password)
                    .context("read password from stdin")?;
                Ok(trim_stdin_password(password))
            },
            password_prompt::prompt_password,
        )
    }
}

fn resolve_password_with_sources<IsTerminal, ReadStdin, Prompt>(
    explicit_password: Option<&str>,
    password_stdin: bool,
    is_terminal: IsTerminal,
    read_stdin: ReadStdin,
    prompt: Prompt,
) -> Result<String>
where
    IsTerminal: FnOnce() -> bool,
    ReadStdin: FnOnce() -> Result<String>,
    Prompt: FnOnce() -> Result<String>,
{
    if let Some(password) = explicit_password.filter(|value| !value.is_empty()) {
        return Ok(password.to_owned());
    }

    if password_stdin {
        return read_stdin();
    }

    if !is_terminal() {
        return Err(anyhow!(
            "cannot prompt for a password because stdin is not a terminal; use --password-stdin"
        ));
    }

    prompt()
}

fn trim_stdin_password(password: String) -> String {
    password.trim_end_matches(['\r', '\n']).to_owned()
}

impl LogoutArgs {
    pub fn run(&self, output: &OutputFormat) -> Result<()> {
        let path = credentials_path();
        let removed = match fs::remove_file(&path) {
            Ok(()) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => {
                return Err(error).with_context(|| format!("remove {}", path.display()));
            }
        };
        let response = LogoutOutput {
            credentials_path: path.to_string_lossy().into_owned(),
            removed,
        };
        match output {
            OutputFormat::Json => print_json(&response),
            OutputFormat::Table => {
                if removed {
                    println!("Removed credentials at {}", response.credentials_path);
                } else {
                    println!("No stored credentials at {}", response.credentials_path);
                }
                Ok(())
            }
        }
    }
}

impl WhoamiArgs {
    pub fn run(&self, server: &str, output: &OutputFormat) -> Result<()> {
        let path = credentials_path();
        let credentials = read_credentials(&path)?;
        let credentials = credentials.as_ref().filter(|credentials| {
            credentials.server_url == normalize_server_url(server) && !credentials.token.is_empty()
        });
        match (output, credentials) {
            (OutputFormat::Json, Some(credentials)) => print_json(&WhoamiOutput {
                server_url: &credentials.server_url,
                email: credentials.email.as_deref(),
                token_id: credentials.token_id.as_deref(),
                token_prefix: credentials.token_prefix.as_deref(),
                credentials_path: path.to_string_lossy().into_owned(),
            }),
            (OutputFormat::Json, None) => print_json(&serde_json::json!({
                "server_url": normalize_server_url(server),
                "authenticated": false,
                "credentials_path": path.to_string_lossy(),
            })),
            (OutputFormat::Table, Some(credentials)) => {
                println!(
                    "Logged in for {} as {}",
                    credentials.server_url,
                    credentials.email.as_deref().unwrap_or("unknown")
                );
                Ok(())
            }
            (OutputFormat::Table, None) => {
                println!("Not logged in for {}", normalize_server_url(server));
                Ok(())
            }
        }
    }
}

pub fn resolve_access_token_for_server(
    server: &str,
    explicit: Option<&str>,
) -> Result<Option<String>> {
    if let Some(value) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(Some(value.to_owned()));
    }

    if let Some(value) = std::env::var("FORGE_TOKEN")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return Ok(Some(value));
    }

    stored_token_for_server(server)
}

pub fn stored_token_for_server(server: &str) -> Result<Option<String>> {
    let Some(credentials) = read_credentials(&credentials_path())? else {
        return Ok(None);
    };
    if credentials.server_url != normalize_server_url(server) {
        return Ok(None);
    }
    Ok(Some(credentials.token).filter(|token| !token.trim().is_empty()))
}

pub fn stored_server_url() -> Result<Option<String>> {
    let Some(credentials) = read_credentials(&credentials_path())? else {
        return Ok(None);
    };
    if credentials.token.trim().is_empty() {
        return Ok(None);
    }
    let server = normalize_server_url(&credentials.server_url);
    Ok((!server.is_empty()).then_some(server))
}

pub fn normalize_server_url(server: &str) -> String {
    server.trim().trim_end_matches('/').to_owned()
}

fn credentials_path() -> PathBuf {
    default_forge_home().join(CREDENTIALS_FILE)
}

fn default_forge_home() -> PathBuf {
    if let Some(path) = std::env::var_os("FORGE_DATA_DIR").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }
    let data_dir = config::default_data_dir();
    // Keep the client's USERPROFILE fallback when HOME is absent.
    if std::env::var_os("HOME").is_none() {
        if let Some(home) = std::env::var_os("USERPROFILE") {
            return PathBuf::from(home).join(data_dir);
        }
    }
    data_dir
}

fn read_credentials(path: &Path) -> Result<Option<StoredCredentials>> {
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("read Forge credentials from {}", path.display()))?;
    serde_json::from_str(&contents)
        .with_context(|| format!("parse Forge credentials at {}", path.display()))
        .map(Some)
}

fn write_credentials(path: &Path, credentials: &StoredCredentials) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create credentials directory {}", parent.display()))?;
    }
    let contents = serde_json::to_string_pretty(credentials)?;
    write_secret_file(path, contents.as_bytes())
}

#[cfg(unix)]
fn write_secret_file(path: &Path, contents: &[u8]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("write Forge credentials to {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("write Forge credentials to {}", path.display()))
}

#[cfg(not(unix))]
fn write_secret_file(path: &Path, contents: &[u8]) -> Result<()> {
    fs::write(path, contents)
        .with_context(|| format!("write Forge credentials to {}", path.display()))
}

#[cfg(test)]
pub(crate) static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn password_resolution_prefers_explicit_value_without_touching_stdin() {
        let password = resolve_password_with_sources(
            Some("explicit-value"),
            false,
            || panic!("terminal state must not be inspected"),
            || panic!("stdin must not be read"),
            || panic!("interactive prompt must not run"),
        )
        .expect("explicit password resolves");

        assert_eq!(password, "explicit-value");
    }

    #[test]
    fn password_stdin_remains_available_without_a_terminal() {
        let password = resolve_password_with_sources(
            None,
            true,
            || panic!("terminal state must not be inspected"),
            || Ok(trim_stdin_password("stdin-value\r\n".to_owned())),
            || panic!("interactive prompt must not run"),
        )
        .expect("stdin password resolves");

        assert_eq!(password, "stdin-value");
    }

    #[test]
    fn implicit_non_terminal_input_fails_before_reading_or_prompting() {
        let read_called = Cell::new(false);
        let prompt_called = Cell::new(false);

        let error = resolve_password_with_sources(
            None,
            false,
            || false,
            || {
                read_called.set(true);
                Ok("must-not-be-read".to_owned())
            },
            || {
                prompt_called.set(true);
                Ok("must-not-be-prompted".to_owned())
            },
        )
        .expect_err("implicit non-terminal input must fail");

        assert!(error.to_string().contains("--password-stdin"));
        assert!(!read_called.get());
        assert!(!prompt_called.get());
    }

    #[test]
    fn implicit_terminal_input_uses_hidden_prompt() {
        let password = resolve_password_with_sources(
            None,
            false,
            || true,
            || panic!("stdin reader must not run"),
            || Ok("prompt-value".to_owned()),
        )
        .expect("interactive password resolves");

        assert_eq!(password, "prompt-value");
    }

    #[test]
    fn hidden_prompt_errors_propagate_without_reading_stdin() {
        let error = resolve_password_with_sources(
            None,
            false,
            || true,
            || panic!("stdin reader must not run"),
            || Err(anyhow!("password input cancelled")),
        )
        .expect_err("prompt error propagates");

        assert_eq!(error.to_string(), "password input cancelled");
    }

    #[test]
    fn stored_token_matches_normalized_server_url() {
        let _guard = TEST_ENV_LOCK.lock().expect("test env lock");
        let temp = unique_temp_dir("forge-client-auth-token");
        let _env = EnvVarGuard::set("FORGE_DATA_DIR", temp.to_string_lossy().as_ref());
        let path = credentials_path();
        write_credentials(
            &path,
            &StoredCredentials {
                server_url: "http://127.0.0.1:8080".to_owned(),
                token: "fg_stored".to_owned(),
                token_id: Some("token-1".to_owned()),
                token_prefix: Some("fg_st".to_owned()),
                email: Some("user@example.com".to_owned()),
            },
        )
        .expect("credentials write");

        assert_eq!(
            stored_token_for_server("http://127.0.0.1:8080/")
                .expect("token reads")
                .as_deref(),
            Some("fg_stored")
        );
        assert_eq!(
            stored_token_for_server("http://127.0.0.1:9090").expect("token reads"),
            None
        );
        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn access_token_resolution_prefers_explicit_then_env_then_stored() {
        let _guard = TEST_ENV_LOCK.lock().expect("test env lock");
        let temp = unique_temp_dir("forge-client-auth-resolution");
        let _data = EnvVarGuard::set("FORGE_DATA_DIR", temp.to_string_lossy().as_ref());
        let _token = EnvVarGuard::set("FORGE_TOKEN", "fg_env");
        write_credentials(
            &credentials_path(),
            &StoredCredentials {
                server_url: "http://127.0.0.1:8080".to_owned(),
                token: "fg_stored".to_owned(),
                token_id: None,
                token_prefix: None,
                email: None,
            },
        )
        .expect("credentials write");

        assert_eq!(
            resolve_access_token_for_server("http://127.0.0.1:8080", Some("fg_explicit"))
                .expect("token resolves")
                .as_deref(),
            Some("fg_explicit")
        );
        assert_eq!(
            resolve_access_token_for_server("http://127.0.0.1:8080", None)
                .expect("token resolves")
                .as_deref(),
            Some("fg_env")
        );
        drop(_token);
        assert_eq!(
            resolve_access_token_for_server("http://127.0.0.1:8080", None)
                .expect("token resolves")
                .as_deref(),
            Some("fg_stored")
        );
        let _ = fs::remove_dir_all(temp);
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()))
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            if let Some(previous) = &self.previous {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }
}
