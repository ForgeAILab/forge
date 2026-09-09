use anyhow::Result;
use api_types::{AccountUsageAnalyticsResponse, ProjectUsageBreakdown};
use clap::Subcommand;

use crate::{
    client::ForgeClient,
    output::print_json,
    project::{print_cost_details, print_usage_analytics},
    OutputFormat,
};

#[derive(clap::Args)]
pub struct AnalyticsArgs {
    #[command(subcommand)]
    cmd: AnalyticsCmd,
}

#[derive(Subcommand)]
enum AnalyticsCmd {
    /// Account-owned usage and cost across every product surface.
    Usage {
        /// Only count usage recorded at or after this RFC3339 timestamp.
        #[arg(long)]
        from: Option<String>,
        /// Only count usage recorded before this RFC3339 timestamp.
        #[arg(long)]
        to: Option<String>,
    },
}

impl AnalyticsArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.cmd {
            AnalyticsCmd::Usage { from, to } => {
                let path = usage_path(from.as_deref(), to.as_deref());
                let response: AccountUsageAnalyticsResponse = client.get(&path).await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        print_usage_analytics(&response.token_usage);
                        if !response.by_project.is_empty() {
                            println!();
                            println!("Projects");
                            for project in &response.by_project {
                                print_project(project);
                            }
                        }
                        Ok(())
                    }
                }
            }
        }
    }
}

fn usage_path(from: Option<&str>, to: Option<&str>) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(from) = from {
        query.append_pair("from", from);
    }
    if let Some(to) = to {
        query.append_pair("to", to);
    }
    let query = query.finish();
    if query.is_empty() {
        "/api/v1/analytics/usage".to_owned()
    } else {
        format!("/api/v1/analytics/usage?{query}")
    }
}

fn print_project(project: &ProjectUsageBreakdown) {
    println!(
        "  {} ({}) — task {} chat {} inquiry {} attempts {}",
        project.project_name_snapshot.as_deref().unwrap_or("-"),
        project.project_id.as_deref().unwrap_or("-"),
        project.counts.task_execution_count,
        project.counts.chat_turn_count,
        project.counts.inquiry_count,
        project.counts.provider_attempt_count,
    );
    print_cost_details(&project.cost, "    ");
}

#[cfg(test)]
mod tests {
    use super::usage_path;

    #[test]
    fn usage_path_percent_encodes_rfc3339_offset() {
        let path = usage_path(
            Some("2026-09-07T12:00:00+05:30"),
            Some("2026-09-08T12:00:00+05:30"),
        );
        assert_eq!(
            path,
            "/api/v1/analytics/usage?from=2026-09-07T12%3A00%3A00%2B05%3A30&to=2026-09-08T12%3A00%3A00%2B05%3A30"
        );
    }
}
