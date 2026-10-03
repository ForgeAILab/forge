use anyhow::Result;
use api_types::{DeadLetterActionResponse, DeadLetterListResponse, DismissDeadLetterRequest};
use clap::Subcommand;

use crate::{client::ForgeClient, output::print_json, OutputFormat};

#[derive(clap::Args)]
pub struct OperationsArgs {
    #[command(subcommand)]
    cmd: OperationsCmd,
}
#[derive(Subcommand)]
enum OperationsCmd {
    /// Inspect or resolve durable consumer failures (admin only).
    DeadLetters {
        #[command(subcommand)]
        cmd: DeadLettersCmd,
    },
}
#[derive(Subcommand)]
enum DeadLettersCmd {
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
        let OperationsCmd::DeadLetters { cmd } = &self.cmd;
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
                                "{}  {}  {}  {} attempts  {:?}\n  {}",
                                row.summary.id,
                                row.summary.consumer_name,
                                row.summary.event_type,
                                row.summary.attempts,
                                row.state,
                                row.summary.reason
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
            if response.outcome == "replay_failed" {
                println!("  {}", response.dead_letter.summary.reason);
            }
        }
    }
    if response.outcome == "replay_failed" {
        anyhow::bail!("replay failed; dead letter remains open")
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        args: OperationsArgs,
    }

    #[test]
    fn parses_dead_letter_commands_and_filters() {
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

    #[tokio::test]
    async fn dismiss_round_trip_uses_body_and_reports_conflicts() {
        use axum::{routing::post, Json, Router};
        let app = Router::new().route("/api/v1/operations/dead-letters/dead-1/dismiss", post(|Json(body): Json<serde_json::Value>| async move {
            assert_eq!(body["reason"], "obsolete");
            Json(serde_json::json!({ "outcome": "dismissed", "dead_letter": {
                "summary": { "id": "dead-1", "item_key": "1", "consumer_name": "consumer", "event_type": "test", "attempts": 8, "event_sequence": 1, "reason": "bad event", "occurred_at": "2026-10-03T00:00:00Z" },
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
