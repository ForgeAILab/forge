use anyhow::Result;
use api_types::{
    CreateProjectRequest, PaginatedResponse, ProjectAnalyticsResponse, ProjectResponse,
};
use clap::Subcommand;

use crate::{
    client::ForgeClient,
    output::{print_json, print_table_projects},
    OutputFormat,
};

#[derive(clap::Args)]
pub struct ProjectArgs {
    #[command(subcommand)]
    cmd: ProjectCmd,
}

#[derive(Subcommand)]
enum ProjectCmd {
    Create {
        #[arg(long)]
        name: String,
    },
    List,
    /// Token and cost accounting for one Project, by surface, model and agent.
    Analytics {
        /// Project id.
        id: String,
        /// Only count usage recorded at or after this RFC3339 timestamp.
        #[arg(long)]
        from: Option<String>,
        /// Only count usage recorded at or before this RFC3339 timestamp.
        #[arg(long)]
        to: Option<String>,
    },
}

impl ProjectArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.cmd {
            ProjectCmd::Create { name } => {
                let request = CreateProjectRequest {
                    name: name.clone(),
                    settings: None,
                    default_review_config: None,
                    paused: None,
                    project_agent_identity_id: None,
                    project_agent_profile_id: None,
                };
                let project: ProjectResponse = client.post("/api/v1/projects", &request).await?;
                print_project(output, &project)
            }
            ProjectCmd::Analytics { id, from, to } => {
                let mut path = format!("/api/v1/projects/{id}/analytics");
                let mut query = Vec::new();
                if let Some(from) = from {
                    query.push(format!("from={from}"));
                }
                if let Some(to) = to {
                    query.push(format!("to={to}"));
                }
                if !query.is_empty() {
                    path.push('?');
                    path.push_str(&query.join("&"));
                }
                let response: ProjectAnalyticsResponse = client.get(&path).await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        print_token_usage(&response);
                        Ok(())
                    }
                }
            }
            ProjectCmd::List => {
                let response: PaginatedResponse<ProjectResponse> =
                    client.get("/api/v1/projects").await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        print_table_projects(&response.items);
                        Ok(())
                    }
                }
            }
        }
    }
}

fn print_project(output: &OutputFormat, project: &ProjectResponse) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(project),
        OutputFormat::Table => {
            print_table_projects(std::slice::from_ref(project));
            Ok(())
        }
    }
}

/// Clips a cell to `width` so a long Agent or model name cannot shear the
/// table's columns apart.
fn cell(value: &str, width: usize) -> String {
    let value = if value.is_empty() { "-" } else { value };
    if value.chars().count() <= width {
        return format!("{value:<width$}");
    }
    let kept: String = value.chars().take(width.saturating_sub(1)).collect();
    format!("{kept}\u{2026}")
}

/// Renders the token accounting a Project actually incurred. The surface table
/// comes first because it answers the question a reader usually has: how much
/// of this went to writing code, and how much to the Agent conversations
/// around it. Input counters are disjoint, so context is their sum.
fn print_token_usage(response: &ProjectAnalyticsResponse) {
    let usage = &response.token_usage;
    let context =
        usage.total_input_tokens + usage.total_cache_read_tokens + usage.total_cache_write_tokens;
    println!("Tokens");
    println!(
        "  context in {context}  (fresh {}, cache read {}, cache write {})",
        usage.total_input_tokens, usage.total_cache_read_tokens, usage.total_cache_write_tokens
    );
    println!("  output     {}", usage.total_output_tokens);
    match usage.total_cost_usd {
        Some(cost) => println!("  cost       ${cost:.4}"),
        None => println!("  cost       not reported by any executor that ran"),
    }
    println!(
        "  runs       {} task executions, {} chat turns",
        usage.execution_count, usage.chat_turn_count
    );

    if !usage.by_surface.is_empty() {
        println!();
        println!(
            "  {:<15} {:>6} {:>13} {:>13} {:>11}",
            "SURFACE", "RUNS", "CONTEXT IN", "OF IT CACHED", "OUTPUT"
        );
        for entry in &usage.by_surface {
            let surface_context =
                entry.input_tokens + entry.cache_read_tokens + entry.cache_write_tokens;
            println!(
                "  {} {:>6} {:>13} {:>13} {:>11}",
                cell(&entry.surface, 15),
                entry.run_count,
                surface_context,
                entry.cache_read_tokens,
                entry.output_tokens
            );
        }
    }

    if !usage.by_model.is_empty() {
        println!();
        println!(
            "  {:<12} {:<18} {:>6} {:>13} {:>11}",
            "PROVIDER", "MODEL", "RUNS", "CONTEXT IN", "OUTPUT"
        );
        for entry in &usage.by_model {
            let model_context =
                entry.input_tokens + entry.cache_read_tokens + entry.cache_write_tokens;
            println!(
                "  {} {} {:>6} {:>13} {:>11}",
                cell(&entry.provider, 12),
                cell(&entry.model, 18),
                entry.execution_count,
                model_context,
                entry.output_tokens
            );
        }
    }

    if !usage.by_agent.is_empty() {
        println!();
        println!(
            "  {:<24} {:<13} {:>6} {:>13} {:>11}",
            "AGENT", "EXECUTOR", "RUNS", "CONTEXT IN", "OUTPUT"
        );
        for entry in &usage.by_agent {
            let agent_context =
                entry.input_tokens + entry.cache_read_tokens + entry.cache_write_tokens;
            println!(
                "  {} {} {:>6} {:>13} {:>11}",
                cell(&entry.agent_name, 24),
                cell(&entry.executor_type, 13),
                entry.execution_count,
                agent_context,
                entry.output_tokens
            );
        }
    }
}
