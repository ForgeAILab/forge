use anyhow::{bail, Result};
use api_types::{CreateRepoRequest, PaginatedResponse, RepoResponse};
use clap::Subcommand;

use crate::{
    client::ForgeClient,
    output::{print_json, print_table_repos},
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
                    work_mode: None,
                    pr_provider: None,
                    pr_provider_config: None,
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
