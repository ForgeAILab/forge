use anyhow::Result;
use api_types::{
    CheckBacklog, ChecksStatus, DeadLetterActionResponse, DeadLetterListResponse,
    DeadLetterOutcome, DismissDeadLetterRequest, OperatorStatusResponse,
};
use clap::Subcommand;

use crate::{client::ForgeClient, output::print_json, OutputFormat};

#[derive(clap::Args)]
pub struct OperationsArgs {
    #[command(subcommand)]
    cmd: OperationsCmd,
}
#[derive(Subcommand)]
enum OperationsCmd {
    /// Show the server's operations status, with the check runner's backlog
    /// (admin only). `--output json` prints the whole status.
    Status,
    /// Inspect or resolve durable consumer failures (admin only).
    DeadLetters {
        #[command(subcommand)]
        cmd: DeadLettersCmd,
    },
}
#[derive(Subcommand)]
enum DeadLettersCmd {
    /// List open or resolved quarantines with their replay eligibility.
    List {
        #[arg(long)]
        consumer: Option<String>,
        #[arg(long, default_value = "open", value_parser = ["open", "resolved"])]
        state: String,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u32).range(1..=100))]
        limit: u32,
    },
    /// Deliver one event to its original consumer, without rewinding its cursor.
    Replay { id: String },
    /// Resolve without delivering; retain the optional reason in the audit.
    Dismiss {
        id: String,
        #[arg(long)]
        reason: Option<String>,
    },
}
impl OperationsArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        let cmd = match &self.cmd {
            OperationsCmd::Status => {
                let status: OperatorStatusResponse =
                    client.get("/api/v1/operations/status").await?;
                return match output {
                    OutputFormat::Json => print_json(&status),
                    OutputFormat::Table => {
                        println!(
                            "Operations: {}  active executions {}  blocked Tasks {}  recent errors {}",
                            status.overall_severity,
                            status.active_executions.len(),
                            status.blocked_tasks.len(),
                            status.recent_errors.len()
                        );
                        print!("{}", checks_table(&status.checks));
                        Ok(())
                    }
                };
            }
            OperationsCmd::DeadLetters { cmd } => cmd,
        };
        match cmd {
            DeadLettersCmd::List {
                consumer,
                state,
                cursor,
                limit,
            } => {
                let mut query = url::form_urlencoded::Serializer::new(String::new());
                query
                    .append_pair("state", state)
                    .append_pair("limit", &limit.to_string());
                if let Some(consumer) = consumer {
                    query.append_pair("consumer", consumer);
                }
                if let Some(cursor) = cursor {
                    query.append_pair("cursor", cursor);
                }
                let response: DeadLetterListResponse = client
                    .get(&format!(
                        "/api/v1/operations/dead-letters?{}",
                        query.finish()
                    ))
                    .await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        for row in response.items {
                            println!(
                                "{}  {}  {}  {} attempts  {}\n  {}",
                                row.summary.id,
                                row.summary.consumer_name,
                                row.summary.event_type,
                                row.summary.attempts,
                                row.state,
                                row.summary.reason
                            );
                            println!(
                                "  replayable={} event_created_at={} events_since={}",
                                row.summary.replayable,
                                row.summary.event_created_at.as_deref().unwrap_or("-"),
                                row.summary.events_since
                            );
                            if let Some(resolution) = row.resolution {
                                println!(
                                    "  {resolution} by {} at {}",
                                    row.resolved_by.as_deref().unwrap_or("-"),
                                    row.resolved_at.as_deref().unwrap_or("-")
                                );
                            }
                        }
                        if let Some(cursor) = response.next_cursor {
                            println!("Next cursor: {cursor}");
                        }
                        Ok(())
                    }
                }
            }
            DeadLettersCmd::Replay { id } => {
                let response: DeadLetterActionResponse = client
                    .post(&action_path(id, "replay"), &serde_json::json!({}))
                    .await?;
                print_action(response, output)
            }
            DeadLettersCmd::Dismiss { id, reason } => {
                let response: DeadLetterActionResponse = client
                    .post(
                        &action_path(id, "dismiss"),
                        &DismissDeadLetterRequest {
                            reason: reason.clone(),
                        },
                    )
                    .await?;
                print_action(response, output)
            }
        }
    }
}
/// The check runner's section of the operations status, as text.
fn checks_table(checks: &ChecksStatus) -> String {
    let backlog = |label: &str, backlog: &CheckBacklog| match backlog.oldest_age_seconds {
        Some(age) if backlog.count > 0 => {
            format!(
                "  {label:<18}{:>5}  oldest {}\n",
                backlog.count,
                age_text(age)
            )
        }
        _ => format!("  {label:<18}{:>5}\n", backlog.count),
    };
    let mut text = String::from("Checks:\n");
    text.push_str(&backlog("queued", &checks.queued));
    text.push_str(&backlog("running", &checks.running));
    text.push_str(&backlog("waiting for slot", &checks.waiting_for_slot));
    text.push_str(&backlog("waiting for disk", &checks.waiting_for_disk));
    text.push_str(&format!(
        "  holding a slot: {} ({} on their Task's own slot); reusable results: {}\n",
        checks.admitted_runs, checks.borrowed_runs, checks.reusable_results
    ));
    text.push_str(&format!(
        "  last 24 h: {} run(s), {} answered from a stored result\n",
        checks.runs_24h, checks.reuse_hits_24h
    ));
    match &checks.last_worker_error {
        Some(error) => text.push_str(&format!(
            "  last worker error ({}): {}\n",
            error.occurred_at, error.error
        )),
        None => text.push_str("  last worker error: none\n"),
    }
    text
}

fn age_text(seconds: i64) -> String {
    match seconds {
        ..=119 => format!("{seconds} s"),
        120..=7199 => format!("{} min", seconds / 60),
        _ => format!("{} h", seconds / 3600),
    }
}

fn action_path(id: &str, action: &str) -> String {
    // Encode the ID as one path segment, including arbitrary user input.
    let mut url =
        url::Url::parse("http://localhost/api/v1/operations/dead-letters/").expect("static URL");
    url.path_segments_mut()
        .expect("hierarchical URL")
        .pop_if_empty()
        .push(id)
        .push(action);
    url.path().to_owned()
}
fn print_action(response: DeadLetterActionResponse, output: &OutputFormat) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(&response)?,
        OutputFormat::Table => {
            println!(
                "{}: {} ({} attempts)",
                response.dead_letter.summary.id,
                response.outcome,
                response.dead_letter.summary.attempts
            );
            if response.outcome == DeadLetterOutcome::ReplayFailed {
                println!("  {}", response.dead_letter.summary.reason);
            }
        }
    }
    if response.outcome == DeadLetterOutcome::ReplayFailed {
        anyhow::bail!("replay failed; dead letter remains open")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: OperationsArgs,
    }

    #[test]
    fn parses_dead_letter_commands_and_filters() {
        assert_eq!(api_types::DeadLetterState::Open.to_string(), "open");
        assert_eq!(api_types::DeadLetterState::Resolved.to_string(), "resolved");
        assert_eq!(DeadLetterOutcome::ReplayFailed.to_string(), "replay_failed");
        assert!(Cli::command()
            .find_subcommand("dead-letters")
            .unwrap()
            .find_subcommand("list")
            .unwrap()
            .get_about()
            .unwrap()
            .to_string()
            .contains("replay eligibility"));
        let cli = Cli::try_parse_from([
            "ctl",
            "dead-letters",
            "list",
            "--state",
            "resolved",
            "--consumer",
            "attention_projection",
            "--limit",
            "2",
            "--cursor",
            "opaque",
        ])
        .unwrap();
        let OperationsCmd::DeadLetters {
            cmd:
                DeadLettersCmd::List {
                    limit,
                    consumer,
                    state,
                    cursor,
                },
        } = cli.args.cmd
        else {
            panic!("list");
        };
        assert_eq!(limit, 2);
        assert_eq!(state, "resolved");
        assert_eq!(consumer.as_deref(), Some("attention_projection"));
        assert_eq!(cursor.as_deref(), Some("opaque"));
        assert!(
            Cli::try_parse_from(["ctl", "dead-letters", "list", "--state", "anything"]).is_err()
        );
        assert!(Cli::try_parse_from(["ctl", "dead-letters", "list", "--limit", "0"]).is_err());
        assert!(Cli::try_parse_from(["ctl", "dead-letters", "replay", "dead-1"]).is_ok());
        let cli = Cli::try_parse_from([
            "ctl",
            "dead-letters",
            "dismiss",
            "dead-1",
            "--reason",
            "obsolete",
        ])
        .unwrap();
        let OperationsCmd::DeadLetters {
            cmd: DeadLettersCmd::Dismiss { id, reason },
        } = cli.args.cmd
        else {
            panic!("dismiss");
        };
        assert_eq!(id, "dead-1");
        assert_eq!(reason.as_deref(), Some("obsolete"));
        assert_eq!(
            action_path("a/b", "replay"),
            "/api/v1/operations/dead-letters/a%2Fb/replay"
        );
    }

    #[test]
    fn status_prints_the_check_runner_section() {
        assert!(matches!(
            Cli::try_parse_from(["ctl", "status"]).unwrap().args.cmd,
            OperationsCmd::Status
        ));
        let checks = ChecksStatus {
            queued: CheckBacklog {
                count: 2,
                oldest_age_seconds: Some(45),
            },
            running: CheckBacklog {
                count: 1,
                oldest_age_seconds: Some(600),
            },
            waiting_for_slot: CheckBacklog {
                count: 3,
                oldest_age_seconds: Some(9000),
            },
            waiting_for_disk: CheckBacklog::default(),
            admitted_runs: 1,
            borrowed_runs: 0,
            reusable_results: 4,
            by_state: Default::default(),
            reuse_hits_24h: 7,
            runs_24h: 12,
            last_worker_error: Some(api_types::CheckWorkerError {
                error: "database is locked".into(),
                occurred_at: "2026-10-11T00:00:00Z".into(),
            }),
        };
        assert_eq!(
            checks_table(&checks),
            "Checks:\n  queued                2  oldest 45 s\n  running               1  oldest 10 min\n  waiting for slot      3  oldest 2 h\n  waiting for disk      0\n  holding a slot: 1 (0 on their Task's own slot); reusable results: 4\n  last 24 h: 12 run(s), 7 answered from a stored result\n  last worker error (2026-10-11T00:00:00Z): database is locked\n"
        );
        assert!(checks_table(&ChecksStatus::default()).ends_with("last worker error: none\n"));
    }

    #[tokio::test]
    async fn dismiss_round_trip_uses_body_and_reports_conflicts() {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route("/api/v1/operations/dead-letters/dead-1/dismiss", post(|Json(body): Json<serde_json::Value>| async move {
            assert_eq!(body["reason"], "obsolete");
            Json(serde_json::json!({ "outcome": "dismissed", "dead_letter": {
                "summary": { "id": "dead-1", "item_key": "1", "consumer_name": "consumer", "event_type": "test", "attempts": 8, "event_sequence": 1, "replayable": true, "event_created_at": "2026-10-03T00:00:00Z", "events_since": 0, "reason": "bad event", "occurred_at": "2026-10-03T00:00:00Z" },
                "state": "resolved", "error_kind": "failure", "first_failed_at": "2026-10-03T00:00:00Z", "last_failed_at": "2026-10-03T00:00:00Z", "resolved_at": "2026-10-03T01:00:00Z", "resolved_by": "admin", "resolution": "dismissed", "resolution_reason": "obsolete"
            }}))
        })).route("/api/v1/operations/dead-letters/dead-1/replay", post(|| async { (axum::http::StatusCode::CONFLICT, Json(serde_json::json!({"code": "version_conflict", "message": "already resolved"}))) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = ForgeClient::new_without_credentials(format!(
            "http://{}",
            listener.local_addr().unwrap()
        ));
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let cli = Cli::try_parse_from([
            "ctl",
            "dead-letters",
            "dismiss",
            "dead-1",
            "--reason",
            "obsolete",
        ])
        .unwrap();
        cli.args.run(&client, &OutputFormat::Json).await.unwrap();
        let replay = Cli::try_parse_from(["ctl", "dead-letters", "replay", "dead-1"]).unwrap();
        assert!(replay
            .args
            .run(&client, &OutputFormat::Table)
            .await
            .unwrap_err()
            .to_string()
            .contains("already resolved"));
        server.abort();
    }
}
