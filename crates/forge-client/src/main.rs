use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use config::{data_dir_from_env, read_server_state, server_state_path};
use forge_client::{
    agent, analytics, auth, client::ForgeClient, daemon, embedded, mcp, memory, project, repo, run,
    task, OutputFormat,
};

#[derive(Parser)]
#[command(name = "forge-ctl", version, about = "Forge CLI client")]
struct Cli {
    #[arg(
        long,
        global = true,
        help = "Forge server URL (default: stored login server, then persisted local server)"
    )]
    server: Option<String>,
    #[arg(long, default_value = "table", value_enum)]
    output: OutputFormat,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Login(auth::LoginArgs),
    Logout(auth::LogoutArgs),
    Whoami(auth::WhoamiArgs),
    Task(task::TaskArgs),
    Agent(agent::AgentArgs),
    Daemon(daemon::DaemonArgs),
    Project(project::ProjectArgs),
    Analytics(analytics::AnalyticsArgs),
    Memory(memory::MemoryArgs),
    Repo(repo::RepoArgs),
    Run(run::RunArgs),
    Mcp(mcp::McpArgs),
    Embedded(embedded::EmbeddedArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Login(args) => {
            let server = resolve_server_url(cli.server.as_deref())?;
            args.run(&server, &cli.output).await
        }
        Commands::Logout(args) => args.run(&cli.output),
        Commands::Whoami(args) => {
            let server = resolve_server_url(cli.server.as_deref())?;
            args.run(&server, &cli.output)
        }
        Commands::Task(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
        Commands::Agent(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
        Commands::Daemon(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
        Commands::Project(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
        Commands::Analytics(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
        Commands::Memory(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
        Commands::Repo(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
        Commands::Run(args) => {
            let client = client_for(cli.server.as_deref())?;
            let exit_code = args.run(&client).await?;
            std::process::exit(exit_code);
        }
        Commands::Mcp(args) => {
            let server = resolve_server_url(cli.server.as_deref())?;
            args.run(&server).await
        }
        Commands::Embedded(args) => {
            let client = client_for(cli.server.as_deref())?;
            args.run(&client, &cli.output).await
        }
    }
}

fn client_for(explicit_server: Option<&str>) -> Result<ForgeClient> {
    Ok(ForgeClient::new(resolve_server_url(explicit_server)?))
}

fn resolve_server_url(explicit: Option<&str>) -> Result<String> {
    if let Some(server) = explicit.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(auth::normalize_server_url(server));
    }

    if let Some(server) = auth::stored_server_url()? {
        return Ok(server);
    }

    let data_dir = data_dir_from_env();
    let state_path = server_state_path(&data_dir);
    let state = read_server_state(&data_dir)
        .with_context(|| format!("read Forge server state from {}", state_path.display()))?;

    state
        .and_then(|state| {
            let server_url = auth::normalize_server_url(&state.server_url);
            (!server_url.is_empty()).then_some(server_url)
        })
        .ok_or_else(|| {
            anyhow!(
                "Forge server URL is not configured; start `forge` once or pass `--server http://127.0.0.1:<port>`"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::resolve_server_url;
    use config::{server_state_path, write_server_state, ServerState};
    use std::{
        fs,
        path::PathBuf,
        sync::{Mutex, OnceLock},
        time::{SystemTime, UNIX_EPOCH},
    };

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn explicit_server_url_wins() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let _data = EnvVarGuard::set("FORGE_DATA_DIR", unique_temp_dir("forge-ctl-server"));

        assert_eq!(
            resolve_server_url(Some("http://127.0.0.1:49152/")).expect("server resolves"),
            "http://127.0.0.1:49152"
        );
    }

    #[test]
    fn server_url_reads_persisted_server_state() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let data_dir = unique_temp_dir("forge-ctl-state");
        let _data = EnvVarGuard::set("FORGE_DATA_DIR", data_dir.clone());
        write_server_state(
            &data_dir,
            &ServerState::new("127.0.0.1:49153", "http://127.0.0.1:49153/"),
        )
        .expect("state writes");

        assert_eq!(
            resolve_server_url(None).expect("server resolves"),
            "http://127.0.0.1:49153"
        );

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn server_url_prefers_stored_login_over_persisted_server_state() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let data_dir = unique_temp_dir("forge-ctl-login-state");
        let _data = EnvVarGuard::set("FORGE_DATA_DIR", data_dir.clone());
        fs::create_dir_all(&data_dir).expect("data dir creates");
        write_server_state(
            &data_dir,
            &ServerState::new("127.0.0.1:49153", "http://127.0.0.1:49153/"),
        )
        .expect("state writes");
        fs::write(
            data_dir.join("forge_ctl_credentials.json"),
            serde_json::json!({
                "server_url": "https://forge.example.com/",
                "token": "fg_stored",
                "token_id": "token-1",
                "token_prefix": "fg_st",
                "email": "user@example.com"
            })
            .to_string(),
        )
        .expect("credentials write");

        assert_eq!(
            resolve_server_url(None).expect("server resolves"),
            "https://forge.example.com"
        );

        let _ = fs::remove_dir_all(data_dir);
    }

    #[test]
    fn missing_server_state_returns_actionable_error() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        let data_dir = unique_temp_dir("forge-ctl-missing-state");
        let _data = EnvVarGuard::set("FORGE_DATA_DIR", data_dir.clone());

        let error = resolve_server_url(None).expect_err("server should be missing");

        assert!(error.to_string().contains("start `forge` once"));
        assert!(!server_state_path(&data_dir).exists());
        let _ = fs::remove_dir_all(data_dir);
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
        fn set(key: &'static str, value: PathBuf) -> Self {
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
