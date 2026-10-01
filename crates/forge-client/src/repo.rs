use anyhow::{bail, Result};
use api_types::{
    CreateRepoLocationRequest, CreateRepoRequest, PaginatedResponse, RepoLocationKind,
    RepoLocationOwnerKind, RepoLocationResponse, RepoResponse, UpdateRepoLocationRequest,
    VerifyRepoLocationRequest,
};
use clap::Subcommand;

use crate::{
    client::ForgeClient,
    output::{print_json, print_table_repo_locations, print_table_repos},
    OutputFormat,
};

#[derive(clap::Args)]
pub struct RepoArgs {
    #[command(subcommand)]
    cmd: RepoCmd,
}

#[derive(Subcommand)]
enum RepoCmd {
    Create {
        #[arg(long)]
        project_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        kind: CliRepoSource,
        #[arg(long)]
        local_path: Option<String>,
        #[arg(long)]
        remote_url: Option<String>,
        #[arg(long)]
        default_branch: Option<String>,
    },
    List {
        #[arg(long)]
        project_id: String,
    },
    /// Register and manage machine-local checkouts of a repository.
    Location(RepoLocationArgs),
}

impl RepoArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.cmd {
            RepoCmd::Create {
                project_id,
                name,
                kind,
                local_path,
                remote_url,
                default_branch,
            } => {
                validate_source(*kind, local_path.as_deref(), remote_url.as_deref())?;
                let request = CreateRepoRequest {
                    remote_url: remote_url.clone().filter(|value| !value.trim().is_empty()),
                    local_path: local_path.clone(),
                    name: Some(name.clone()),
                    default_branch: default_branch.clone(),
                };
                let repo: RepoResponse = client
                    .post(&format!("/api/v1/projects/{project_id}/repos"), &request)
                    .await?;
                print_repo(output, &repo)
            }
            RepoCmd::List { project_id } => {
                let response: PaginatedResponse<RepoResponse> = client
                    .get(&format!("/api/v1/projects/{project_id}/repos"))
                    .await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        print_table_repos(&response.items);
                        Ok(())
                    }
                }
            }
            RepoCmd::Location(args) => args.run(client, output).await,
        }
    }
}

#[derive(clap::Args)]
struct RepoLocationArgs {
    #[command(subcommand)]
    cmd: RepoLocationCmd,
}

#[derive(Subcommand)]
enum RepoLocationCmd {
    List {
        #[arg(long)]
        repo_id: String,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long)]
        include_total: bool,
    },
    /// Register a checkout and record its verification result.
    Add {
        #[arg(long)]
        repo_id: String,
        #[arg(long, value_enum, default_value = "server")]
        owner: CliLocationOwner,
        #[arg(long)]
        daemon_id: Option<String>,
        #[arg(long)]
        runtime_id: Option<String>,
        #[arg(long)]
        path: String,
        #[arg(long, value_enum, default_value = "primary_checkout")]
        kind: CliLocationKind,
        #[arg(long)]
        default: bool,
    },
    Verify {
        #[arg(long)]
        repo_id: String,
        location_id: String,
        /// Current location version from `repo location list`.
        #[arg(long)]
        version: i64,
    },
    SetDefault {
        #[arg(long)]
        repo_id: String,
        location_id: String,
        #[arg(long)]
        version: i64,
    },
    Remove {
        #[arg(long)]
        repo_id: String,
        location_id: String,
    },
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
enum CliLocationOwner {
    Server,
    Daemon,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
enum CliLocationKind {
    PrimaryCheckout,
    ManagedClone,
    SharedMount,
}

impl RepoLocationArgs {
    async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        let repo_id = match &self.cmd {
            RepoLocationCmd::List { repo_id, .. }
            | RepoLocationCmd::Add { repo_id, .. }
            | RepoLocationCmd::Verify { repo_id, .. }
            | RepoLocationCmd::SetDefault { repo_id, .. }
            | RepoLocationCmd::Remove { repo_id, .. } => repo_id,
        };
        let base = format!("/api/v1/repos/{repo_id}/locations");
        match &self.cmd {
            RepoLocationCmd::List {
                cursor,
                limit,
                include_total,
                ..
            } => {
                let mut query = url::form_urlencoded::Serializer::new(String::new());
                if let Some(cursor) = cursor {
                    query.append_pair("cursor", cursor);
                }
                if let Some(limit) = limit {
                    query.append_pair("limit", &limit.to_string());
                }
                if *include_total {
                    query.append_pair("include_total", "true");
                }
                let query = query.finish();
                let path = if query.is_empty() {
                    base
                } else {
                    format!("{base}?{query}")
                };
                let response: PaginatedResponse<RepoLocationResponse> = client.get(&path).await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        print_table_repo_locations(&response.items);
                        if let Some(cursor) = response.next_cursor {
                            println!("Next cursor: {cursor}");
                        }
                        Ok(())
                    }
                }
            }
            RepoLocationCmd::Add {
                owner,
                daemon_id,
                runtime_id,
                path,
                kind,
                default,
                ..
            } => {
                let request = CreateRepoLocationRequest {
                    owner_kind: match owner {
                        CliLocationOwner::Server => RepoLocationOwnerKind::Server,
                        CliLocationOwner::Daemon => RepoLocationOwnerKind::Daemon,
                    },
                    daemon_id: daemon_id.clone(),
                    runtime_id: runtime_id.clone(),
                    path: path.clone(),
                    kind: match kind {
                        CliLocationKind::PrimaryCheckout => RepoLocationKind::PrimaryCheckout,
                        CliLocationKind::ManagedClone => RepoLocationKind::ManagedClone,
                        CliLocationKind::SharedMount => RepoLocationKind::SharedMount,
                    },
                    is_default: Some(*default),
                };
                let location = client.post(&base, &request).await?;
                print_location(output, &location)
            }
            RepoLocationCmd::Verify {
                location_id,
                version,
                ..
            } => {
                let location = client
                    .post(
                        &format!("{base}/{location_id}/verify"),
                        &VerifyRepoLocationRequest { version: *version },
                    )
                    .await?;
                print_location(output, &location)
            }
            RepoLocationCmd::SetDefault {
                location_id,
                version,
                ..
            } => {
                let location = client
                    .patch(
                        &format!("{base}/{location_id}"),
                        &UpdateRepoLocationRequest {
                            version: *version,
                            is_default: true,
                        },
                    )
                    .await?;
                print_location(output, &location)
            }
            RepoLocationCmd::Remove { location_id, .. } => {
                client.delete(&format!("{base}/{location_id}")).await
            }
        }
    }
}

fn print_location(output: &OutputFormat, location: &RepoLocationResponse) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(location),
        OutputFormat::Table => {
            print_table_repo_locations(std::slice::from_ref(location));
            Ok(())
        }
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum CliRepoSource {
    Local,
    Remote,
}

fn validate_source(
    kind: CliRepoSource,
    local_path: Option<&str>,
    remote_url: Option<&str>,
) -> Result<()> {
    let has_local_path = local_path
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let has_remote_url = remote_url
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());

    match kind {
        CliRepoSource::Local if has_local_path && !has_remote_url => Ok(()),
        CliRepoSource::Remote if has_remote_url && !has_local_path => Ok(()),
        CliRepoSource::Local => {
            bail!("local repos require --local-path and must not set --remote-url")
        }
        CliRepoSource::Remote => {
            bail!("remote repos require --remote-url and must not set --local-path")
        }
    }
}

fn print_repo(output: &OutputFormat, repo: &RepoResponse) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(repo),
        OutputFormat::Table => {
            print_table_repos(std::slice::from_ref(repo));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::{CommandFactory, Parser};

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        cmd: TestCommand,
    }

    #[derive(clap::Subcommand)]
    enum TestCommand {
        Repo(super::RepoArgs),
    }

    #[test]
    fn repo_location_commands_parse() {
        TestCli::command().debug_assert();
        for arguments in [
            vec![
                "repo",
                "location",
                "list",
                "--repo-id",
                "repo",
                "--cursor",
                "opaque",
                "--limit",
                "1",
                "--include-total",
            ],
            vec![
                "repo",
                "location",
                "add",
                "--repo-id",
                "repo",
                "--owner",
                "daemon",
                "--daemon-id",
                "daemon",
                "--runtime-id",
                "runtime",
                "--path",
                "/remote/repo",
                "--kind",
                "primary_checkout",
                "--default",
            ],
            vec![
                "repo",
                "location",
                "verify",
                "--repo-id",
                "repo",
                "location",
                "--version",
                "2",
            ],
            vec![
                "repo",
                "location",
                "set-default",
                "--repo-id",
                "repo",
                "location",
                "--version",
                "2",
            ],
            vec![
                "repo",
                "location",
                "remove",
                "--repo-id",
                "repo",
                "location",
            ],
        ] {
            let result = TestCli::try_parse_from(
                std::iter::once("forge-ctl").chain(arguments.iter().copied()),
            );
            assert!(
                result.is_ok(),
                "failed to parse {arguments:?}: {}",
                result
                    .err()
                    .map(|error| error.to_string())
                    .unwrap_or_default()
            );
        }
        assert!(TestCli::try_parse_from([
            "forge-ctl",
            "repo",
            "location",
            "verify",
            "--repo-id",
            "repo",
            "location"
        ])
        .is_err());
    }
}
