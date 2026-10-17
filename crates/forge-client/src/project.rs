use anyhow::Result;
use api_types::{
    CostCoverage, CostKind, CreateProjectRequest, PaginatedResponse, ProjectAnalyticsResponse,
    ProjectResponse, UsageAggregate, UsageAnalytics,
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
    /// Add or replace a named, read-only Project environment check.
    EnvCheck {
        project: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        command: String,
        #[arg(long, default_value = "workspace", value_parser = ["workspace", "machine"])]
        scope: String,
        #[arg(long, default_value_t = 120)]
        timeout_seconds: u64,
        #[arg(long = "role")]
        roles: Vec<String>,
    },
    Create {
        #[arg(long)]
        name: String,
    },
    List,
    /// Read recorded Project environment readiness by machine.
    EnvStatus {
        project: String,
    },
    /// Run Project environment checks and resume an environment pause on success.
    EnvRecheck {
        /// Project id.
        project: String,
        #[arg(long)]
        machine: Option<String>,
    },
    /// Owner escalations raised by the Project Agent.
    Escalations {
        #[command(subcommand)]
        cmd: EscalationCmd,
    },
    /// Token and cost accounting for one Project, by surface, model and agent.
    Analytics {
        /// Project id.
        id: String,
        /// Only count usage recorded at or after this RFC3339 timestamp.
        #[arg(long)]
        from: Option<String>,
        /// Only count usage recorded before this RFC3339 timestamp (exclusive).
        #[arg(long)]
        to: Option<String>,
    },
}

#[derive(Subcommand)]
enum EscalationCmd {
    /// List the Project's escalations (owner only).
    List {
        /// Project id.
        project: String,
        #[arg(long, value_parser = ["open", "answered"])]
        status: Option<String>,
    },
    /// Answer an open escalation; the answer wakes the Project Agent.
    Answer {
        /// Project id.
        project: String,
        /// Escalation id.
        id: String,
        #[arg(long)]
        answer: String,
    },
}

impl ProjectArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.cmd {
            ProjectCmd::EnvCheck {
                project,
                name,
                command,
                scope,
                timeout_seconds,
                roles,
            } => {
                let current: ProjectResponse =
                    client.get(&format!("/api/v1/projects/{project}")).await?;
                let settings = check_settings(
                    current.settings.clone(),
                    api_types::EnvironmentCheck {
                        name: name.clone(),
                        command: command.clone(),
                        scope: if scope == "machine" {
                            api_types::EnvironmentCheckScope::Machine
                        } else {
                            api_types::EnvironmentCheckScope::Workspace
                        },
                        roles: roles.clone(),
                        timeout_seconds: *timeout_seconds,
                    },
                )?;
                let saved: ProjectResponse = client
                    .patch(
                        &format!("/api/v1/projects/{project}"),
                        &serde_json::json!({"version":current.version,"settings":settings}),
                    )
                    .await?;
                print_project(output, &saved)
            }
            ProjectCmd::Escalations { cmd } => match cmd {
                EscalationCmd::List { project, status } => {
                    let path = match status {
                        Some(status) => {
                            format!("/api/v1/projects/{project}/escalations?status={status}")
                        }
                        None => format!("/api/v1/projects/{project}/escalations"),
                    };
                    let response: api_types::ProjectEscalationListResponse =
                        client.get(&path).await?;
                    match output {
                        OutputFormat::Json => print_json(&response),
                        OutputFormat::Table => {
                            if response.items.is_empty() {
                                println!("No escalations.");
                            }
                            for item in &response.items {
                                println!("{} · {} · v{}", item.id, item.status, item.version);
                                println!("{}", item.need);
                                if let Some(answer) = &item.answer {
                                    println!("answer: {answer}");
                                }
                            }
                            Ok(())
                        }
                    }
                }
                EscalationCmd::Answer {
                    project,
                    id,
                    answer,
                } => {
                    let path = format!("/api/v1/projects/{project}/escalations/{id}");
                    let current: api_types::ProjectEscalationResponse = client.get(&path).await?;
                    let response: api_types::ProjectEscalationResponse = client
                        .post(
                            &format!("{path}/answer"),
                            &api_types::AnswerProjectEscalationRequest {
                                expected_version: current.version,
                                answer: answer.clone(),
                            },
                        )
                        .await?;
                    match output {
                        OutputFormat::Json => print_json(&response),
                        OutputFormat::Table => {
                            println!(
                                "{} · {} · v{}",
                                response.id, response.status, response.version
                            );
                            Ok(())
                        }
                    }
                }
            },
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
                let path = analytics_path(
                    &format!("/api/v1/projects/{id}/analytics"),
                    from.as_deref(),
                    to.as_deref(),
                );
                let response: ProjectAnalyticsResponse = client.get(&path).await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        print_project_analytics(&response);
                        Ok(())
                    }
                }
            }
            ProjectCmd::EnvStatus { project } => {
                let response: ProjectResponse =
                    client.get(&format!("/api/v1/projects/{project}")).await?;
                match output {
                    OutputFormat::Json => print_json(&response.environment_readiness),
                    OutputFormat::Table => {
                        if response.environment_readiness.is_empty() {
                            println!("No environment readiness recorded.");
                        }
                        for row in &response.environment_readiness {
                            println!(
                                "{} ({}) · {} · {} · checked {} · next {}",
                                row.machine.name,
                                row.machine.id,
                                match row.status {
                                    api_types::EnvironmentReadinessStatus::Ready => "ready",
                                    api_types::EnvironmentReadinessStatus::NotReady => "not_ready",
                                    api_types::EnvironmentReadinessStatus::Unknown => "unknown",
                                },
                                row.failing_checks
                                    .iter()
                                    .map(|c| c.name.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", "),
                                row.checked_at.as_deref().unwrap_or("-"),
                                row.next_check_at.as_deref().unwrap_or("-")
                            );
                            if !row.output_tail.is_empty() {
                                println!("{}", row.output_tail);
                            }
                        }
                        Ok(())
                    }
                }
            }
            ProjectCmd::EnvRecheck { project, machine } => {
                let response: api_types::ProjectEnvironmentRecheckResponse = client
                    .post(
                        &format!("/api/v1/projects/{project}/environment/recheck"),
                        &api_types::ProjectEnvironmentRecheckRequest {
                            machine: machine.clone(),
                        },
                    )
                    .await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        for machine in &response.machines {
                            println!("{} ({})", machine.machine.name, machine.machine.id);
                            if let Some(error) = &machine.error {
                                println!("{error}");
                            }
                            for check in &machine.checks {
                                let status = if check.passed { "passed" } else { "failed" };
                                let exit = check
                                    .exit_code
                                    .map(|code| code.to_string())
                                    .unwrap_or_else(|| "-".to_owned());
                                println!("{}: {status} (exit {exit})", check.name);
                                if !check.output_tail.is_empty() {
                                    println!("{}", check.output_tail);
                                }
                            }
                        }
                        print_table_projects(std::slice::from_ref(&response.project));
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

fn check_settings(
    mut settings: serde_json::Value,
    check: api_types::EnvironmentCheck,
) -> Result<serde_json::Value> {
    let mut environment: api_types::ProjectEnvironment = settings
        .get("environment")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    if let Some(existing) = environment
        .checks
        .iter_mut()
        .find(|existing| existing.name == check.name)
    {
        *existing = check;
    } else {
        environment.checks.push(check);
    }
    settings["environment"] = serde_json::to_value(environment)?;
    Ok(settings)
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
fn analytics_path(path: &str, from: Option<&str>, to: Option<&str>) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    if let Some(from) = from {
        query.append_pair("from", from);
    }
    if let Some(to) = to {
        query.append_pair("to", to);
    }
    let query = query.finish();
    if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query}")
    }
}

pub fn print_project_analytics(response: &ProjectAnalyticsResponse) {
    print_usage_analytics(&response.token_usage);
    let outcome = &response.outcome_economics;
    let status = outcome_status(outcome.eligibility);
    let amount = outcome
        .amount_per_outcome
        .as_ref()
        .map(|money| money.decimal.as_str())
        .unwrap_or("-");
    println!();
    println!(
        "Released milestones: {} ({}), cost/outcome {}",
        outcome.denominator, status, amount
    );
}

fn outcome_status(eligibility: api_types::OutcomeEligibility) -> &'static str {
    match eligibility {
        api_types::OutcomeEligibility::Eligible => "eligible",
        api_types::OutcomeEligibility::NoOutcomes => "no outcomes",
        api_types::OutcomeEligibility::IncompleteCost => "incomplete cost",
        api_types::OutcomeEligibility::PendingCost => "pending cost",
    }
}

pub fn print_usage_analytics(usage: &UsageAnalytics) {
    println!("Tokens and cost");
    let total = UsageAggregate {
        counts: usage.counts.clone(),
        tokens: usage.tokens.clone(),
        cost: usage.cost.clone(),
    };
    print_aggregate("  total", &total);
    print_cost_details(&usage.cost, "  ");

    if !usage.by_surface.is_empty() {
        println!();
        println!("  SURFACE         TASK   CHAT  INQUIRY  ATTEMPTS  COST");
        for entry in &usage.by_surface {
            println!(
                "  {:<15} {:>6} {:>6} {:>8} {:>9}  {}",
                cell(&surface_name(entry.surface), 15),
                entry.counts.task_execution_count,
                entry.counts.chat_turn_count,
                entry.counts.inquiry_count,
                entry.counts.provider_attempt_count,
                cost_label(&entry.cost),
            );
        }
    }

    if !usage.by_model.is_empty() {
        println!();
        println!("  PROVIDER      MODEL              ATTEMPTS  COST");
        for entry in &usage.by_model {
            println!(
                "  {:<12} {:<18} {:>8}  {}",
                cell(entry.provider_id.as_deref().unwrap_or("-"), 12),
                cell(entry.model_id.as_deref().unwrap_or("-"), 18),
                entry.counts.provider_attempt_count,
                cost_label(&entry.cost),
            );
        }
    }

    if !usage.by_agent.is_empty() {
        println!();
        println!("  AGENT                     PROFILE       EXECUTOR       ATTEMPTS  COST");
        for entry in &usage.by_agent {
            println!(
                "  {:<24} {:<13} {:<13} {:>8}  {}",
                cell(entry.agent_name_snapshot.as_deref().unwrap_or("-"), 24),
                cell(entry.profile_id.as_deref().unwrap_or("-"), 13),
                cell(entry.executor_type.as_deref().unwrap_or("-"), 13),
                entry.counts.provider_attempt_count,
                cost_label(&entry.cost),
            );
        }
    }
}

fn print_aggregate(label: &str, aggregate: &UsageAggregate) {
    let tokens = &aggregate.tokens;
    println!(
        "{label}: input {} output {} cache-read {} cache-write {}",
        tokens.input_tokens,
        tokens.output_tokens,
        tokens.cache_read_tokens,
        tokens.cache_write_tokens,
    );
    println!(
        "{label} counts: task {} chat {} inquiry {} attempts {}",
        aggregate.counts.task_execution_count,
        aggregate.counts.chat_turn_count,
        aggregate.counts.inquiry_count,
        aggregate.counts.provider_attempt_count,
    );
}

pub(crate) fn print_cost_details(cost: &api_types::CostSummary, prefix: &str) {
    println!("{prefix}cost: {}", cost_label(cost));
    println!(
        "{prefix}reported {}  estimated {}  known {}  complete {}",
        money_text(cost.provider_reported.as_ref()),
        money_text(cost.estimated.as_ref()),
        money_text(cost.known_subtotal.as_ref()),
        money_text(cost.complete_total.as_ref()),
    );
}

fn money_text(amount: Option<&api_types::MoneyAmount>) -> &str {
    amount.map(|amount| amount.decimal.as_str()).unwrap_or("-")
}

fn cost_label(cost: &api_types::CostSummary) -> String {
    let kind = match cost.kind {
        CostKind::ProviderReported => "reported",
        CostKind::Estimated => "estimated",
        CostKind::Mixed => "mixed",
        CostKind::Unknown => "unknown",
        CostKind::None => "none",
    };
    let coverage = match cost.coverage {
        CostCoverage::Complete => "complete",
        CostCoverage::Partial => "partial",
        CostCoverage::Unavailable => "unavailable",
        CostCoverage::Pending => "pending",
        CostCoverage::NoUsage => "no_usage",
    };
    format!("{kind}/{coverage}")
}

fn surface_name(surface: api_types::UsageSurface) -> String {
    match surface {
        api_types::UsageSurface::TaskExecution => "task_execution",
        api_types::UsageSurface::ProjectChat => "project_chat",
        api_types::UsageSurface::MainChat => "main_chat",
        api_types::UsageSurface::GenesisChat => "genesis_chat",
        api_types::UsageSurface::MainInquiry => "main_inquiry",
    }
    .to_owned()
}

#[cfg(test)]
mod tests {
    #[test]
    fn environment_check_scope_flag_and_upsert_preserve_settings() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: super::ProjectArgs,
        }
        let cli = Cli::try_parse_from([
            "forge-ctl",
            "env-check",
            "p",
            "--name",
            "cargo",
            "--command",
            "cargo --version",
            "--scope",
            "machine",
        ])
        .unwrap();
        assert!(
            matches!(cli.args.cmd, super::ProjectCmd::EnvCheck { scope, .. } if scope == "machine")
        );
        assert!(Cli::try_parse_from([
            "forge-ctl",
            "env-check",
            "p",
            "--name",
            "x",
            "--command",
            "true",
            "--scope",
            "host"
        ])
        .is_err());
        let check: api_types::EnvironmentCheck = serde_json::from_value(
            serde_json::json!({"name":"cargo","command":"true","scope":"machine"}),
        )
        .unwrap();
        let settings = super::check_settings(serde_json::json!({"placement":{"provision":"never"},"environment":{"env":{"X":"value"},"checks":[{"name":"cargo","command":"false"}]}}), check).unwrap();
        assert_eq!(
            settings["environment"]["checks"].as_array().unwrap().len(),
            1
        );
        assert_eq!(settings["environment"]["checks"][0]["scope"], "machine");
        assert_eq!(settings["environment"]["env"]["X"], "value");
        assert_eq!(settings["placement"]["provision"], "never");
    }

    use super::analytics_path;

    #[test]
    fn environment_recheck_subcommand_parses_project() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: super::ProjectArgs,
        }
        let cli = Cli::try_parse_from(["project", "env-recheck", "project-1"]).unwrap();
        assert!(
            matches!(cli.args.cmd, super::ProjectCmd::EnvRecheck { project, machine: None } if project == "project-1")
        );
    }

    #[test]
    fn environment_status_and_machine_selector_parse() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(flatten)]
            args: super::ProjectArgs,
        }
        let cli = Cli::try_parse_from(["project", "env-status", "project-1"]).unwrap();
        assert!(
            matches!(cli.args.cmd, super::ProjectCmd::EnvStatus { project } if project == "project-1")
        );
        let cli =
            Cli::try_parse_from(["project", "env-recheck", "project-1", "--machine", "server"])
                .unwrap();
        assert!(
            matches!(cli.args.cmd, super::ProjectCmd::EnvRecheck { project, machine: Some(machine) } if project == "project-1" && machine == "server")
        );
    }

    #[test]
    fn analytics_path_percent_encodes_rfc3339_offset() {
        let path = analytics_path(
            "/api/v1/projects/project-1/analytics",
            Some("2026-09-07T12:00:00+05:30"),
            Some("2026-09-08T12:00:00+05:30"),
        );
        assert_eq!(
            path,
            "/api/v1/projects/project-1/analytics?from=2026-09-07T12%3A00%3A00%2B05%3A30&to=2026-09-08T12%3A00%3A00%2B05%3A30"
        );
    }
}
