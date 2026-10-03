use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use api_types::{
    ClaimTaskRequest, CommentResponse, CreateCommentRequest, CreateTaskRequest, PaginatedResponse,
    PromptPreviewResponse, TaskListItemResponse, TaskMediaResponse, TaskResponse,
    TransitionTaskRequest, TransitionTaskResponse,
};
use clap::Subcommand;
use reqwest::multipart::Form;
use serde_json::json;

use crate::{
    client::ForgeClient,
    output::{print_json, print_table_tasks},
    OutputFormat,
};

#[derive(clap::Args)]
pub struct TaskArgs {
    #[command(subcommand)]
    pub cmd: TaskCmd,
}

#[derive(Subcommand)]
pub enum TaskCmd {
    Create {
        #[arg(long)]
        project_id: String,
        #[arg(long)]
        title: String,
        #[arg(long)]
        description: Option<String>,
        #[arg(long)]
        priority: Option<i64>,
    },
    List {
        #[arg(long)]
        project_id: String,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
    },
    Get {
        id: String,
    },
    Claim {
        id: String,
        #[arg(long)]
        agent_id: String,
    },
    Transition {
        id: String,
        status: String,
        version: i64,
    },
    Actions {
        id: String,
    },
    Action {
        id: String,
        #[arg(value_parser = ["start", "hold", "release", "retry", "send_back", "approve", "restart", "cancel"])]
        verb: String,
        #[arg(long)]
        version: Option<i64>,
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        fresh_session: Option<bool>,
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        refresh_workspace: Option<bool>,
        #[arg(long, num_args = 0..=1, default_missing_value = "true")]
        reset_budget: Option<bool>,
        #[arg(long)]
        guidance: Option<String>,
        #[arg(long = "override", num_args = 0..=1, default_missing_value = "true")]
        override_checks: Option<bool>,
        #[arg(long)]
        reason: Option<String>,
    },
    PromptPreview {
        task_id: String,
        #[arg(long)]
        role: String,
        #[arg(long)]
        trigger: Option<String>,
    },
    Media(MediaArgs),
}

#[derive(clap::Args)]
pub struct MediaArgs {
    #[command(subcommand)]
    pub cmd: MediaCmd,
}

#[derive(Subcommand)]
pub enum MediaCmd {
    Upload {
        #[arg(long)]
        task_id: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        author_name: Option<String>,
    },
    Comment {
        #[arg(long)]
        task_id: String,
        #[arg(long)]
        content: String,
        #[arg(long)]
        author_name: Option<String>,
        #[arg(long)]
        media_url: Vec<String>,
    },
}

impl TaskArgs {
    pub async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.cmd {
            TaskCmd::Create {
                project_id,
                title,
                description,
                priority,
            } => {
                let request = CreateTaskRequest {
                    title: title.clone(),
                    description: description.clone(),
                    parent_task_id: None,
                    task_type: None,
                    priority: *priority,
                    review_config: None,
                    merge_config: None,
                    role_assignments: None,
                    governance: None,
                };
                let task: TaskResponse = client
                    .post(&format!("/api/v1/projects/{project_id}/tasks"), &request)
                    .await?;
                print_task(output, &task)
            }
            TaskCmd::List {
                project_id,
                status,
                limit,
            } => {
                let response: PaginatedResponse<TaskListItemResponse> = client
                    .get(&task_list_path(project_id, status.as_deref(), *limit))
                    .await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        print_table_tasks(&response.items);
                        Ok(())
                    }
                }
            }
            TaskCmd::Get { id } => {
                let task: TaskResponse = client.get(&format!("/api/v1/tasks/{id}")).await?;
                print_task(output, &task)
            }
            TaskCmd::Claim { id, agent_id } => {
                let request = ClaimTaskRequest {
                    agent_id: agent_id.clone(),
                    overrides: None,
                };
                let task: TaskResponse = client
                    .post(&format!("/api/v1/tasks/{id}/claim"), &request)
                    .await?;
                print_task(output, &task)
            }
            TaskCmd::Transition {
                id,
                status,
                version,
            } => {
                let request = TransitionTaskRequest {
                    status: status.to_string(),
                    version: *version,
                    reason: None,
                    source: None,
                };
                let response: TransitionTaskResponse = client
                    .post(&format!("/api/v1/tasks/{id}/transition"), &request)
                    .await?;
                print_task(output, &response.task)
            }
            TaskCmd::Actions { id } => {
                let response: api_types::TaskActionsResponse =
                    client.get(&format!("/api/v1/tasks/{id}/actions")).await?;
                match output {
                    OutputFormat::Json => print_json(&response),
                    OutputFormat::Table => {
                        println!("Version: {}", response.version);
                        for offer in response.available_actions {
                            println!("{}: {}", offer.action.verb(), offer.label);
                        }
                        Ok(())
                    }
                }
            }
            TaskCmd::Action {
                id,
                verb,
                version,
                fresh_session,
                refresh_workspace,
                reset_budget,
                guidance,
                override_checks,
                reason,
            } => {
                let action = build_action(
                    verb,
                    *fresh_session,
                    *refresh_workspace,
                    *reset_budget,
                    guidance.as_deref(),
                    *override_checks,
                    reason.as_deref(),
                )?;
                let version = match version {
                    Some(version) => *version,
                    None => {
                        client
                            .get::<api_types::TaskActionsResponse>(&format!(
                                "/api/v1/tasks/{id}/actions"
                            ))
                            .await?
                            .version
                    }
                };
                let task: Result<TaskResponse> = client
                    .post(
                        &format!("/api/v1/tasks/{id}/actions"),
                        &api_types::TaskActionRequest { action, version },
                    )
                    .await;
                match task {
                    Ok(task) => print_task(output, &task),
                    Err(error) => {
                        if let Some(unavailable) =
                            error.downcast_ref::<crate::client::ActionUnavailable>()
                        {
                            match output {
                                OutputFormat::Json => print_json(&unavailable.response)?,
                                OutputFormat::Table => {
                                    eprintln!("Action unavailable. Available now:");
                                    if let Some(offers) = unavailable
                                        .response
                                        .pointer("/details/available_actions")
                                        .and_then(serde_json::Value::as_array)
                                    {
                                        for offer in offers {
                                            eprintln!(
                                                "{}: {}",
                                                offer
                                                    .pointer("/action/verb")
                                                    .and_then(serde_json::Value::as_str)
                                                    .unwrap_or("?"),
                                                offer
                                                    .get("label")
                                                    .and_then(serde_json::Value::as_str)
                                                    .unwrap_or("")
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        Err(error)
                    }
                }
            }
            TaskCmd::PromptPreview {
                task_id,
                role,
                trigger,
            } => {
                let preview = client
                    .prompt_preview(task_id, role, trigger.as_deref())
                    .await?;
                print_prompt_preview(output, &preview)
            }
            TaskCmd::Media(args) => args.run(client, output).await,
        }
    }
}

impl MediaArgs {
    async fn run(&self, client: &ForgeClient, output: &OutputFormat) -> Result<()> {
        match &self.cmd {
            MediaCmd::Upload {
                task_id,
                file,
                author_name,
            } => {
                let media = upload_media(client, task_id, file, author_name.as_deref()).await?;
                print_media(output, &media)
            }
            MediaCmd::Comment {
                task_id,
                content,
                author_name,
                media_url,
            } => {
                let comment = create_media_comment(
                    client,
                    task_id,
                    content,
                    author_name.as_deref(),
                    media_url,
                )
                .await?;
                print_json(&comment)
            }
        }
    }
}

async fn upload_media(
    client: &ForgeClient,
    task_id: &str,
    file: &Path,
    author_name: Option<&str>,
) -> Result<TaskMediaResponse> {
    let mut form = Form::new()
        .file("file", file)
        .await
        .with_context(|| format!("read media file {}", file.display()))?;
    if let Some(author_name) = non_empty_author(author_name) {
        form = form.text("author_name", author_name.to_owned());
    }

    client
        .post_multipart(&format!("/api/v1/tasks/{task_id}/media"), form)
        .await
}

async fn create_media_comment(
    client: &ForgeClient,
    task_id: &str,
    content: &str,
    author_name: Option<&str>,
    media_urls: &[String],
) -> Result<CommentResponse> {
    let request = CreateCommentRequest {
        content: comment_content_with_media(content, media_urls),
        author_name: non_empty_author(author_name).unwrap_or("Agent").to_owned(),
    };
    client
        .post(&format!("/api/v1/tasks/{task_id}/comments"), &request)
        .await
}

fn task_list_path(project_id: &str, status: Option<&str>, limit: Option<i64>) -> String {
    let mut params = Vec::new();
    if let Some(status) = status {
        params.push(format!("status={status}"));
    }
    if let Some(limit) = limit {
        params.push(format!("limit={limit}"));
    }

    if params.is_empty() {
        format!("/api/v1/projects/{project_id}/tasks")
    } else {
        format!("/api/v1/projects/{project_id}/tasks?{}", params.join("&"))
    }
}

fn print_task(output: &OutputFormat, task: &TaskResponse) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(task),
        OutputFormat::Table => {
            print_table_tasks(&[task.clone().into()]);
            Ok(())
        }
    }
}

fn print_media(output: &OutputFormat, media: &TaskMediaResponse) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(media),
        OutputFormat::Table => {
            println!(
                "{}  {}  {}  {}",
                media.id, media.content_type, media.byte_size, media.url
            );
            Ok(())
        }
    }
}

fn print_prompt_preview(output: &OutputFormat, preview: &PromptPreviewResponse) -> Result<()> {
    match output {
        OutputFormat::Json => print_json(preview),
        OutputFormat::Table => {
            println!("System:\n{}\n", preview.system);
            println!("User:\n{}\n", preview.user);
            let tools = preview
                .tools
                .as_ref()
                .filter(|tools| !tools.is_empty())
                .map(|tools| tools.join(", "))
                .unwrap_or_else(|| "none".to_owned());
            println!("Tools:\n{tools}");
            Ok(())
        }
    }
}

fn non_empty_author(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn comment_content_with_media(content: &str, media_urls: &[String]) -> String {
    if media_urls.is_empty() {
        return content.to_owned();
    }

    let mut rendered = content.to_owned();
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    rendered.push('\n');
    for media_url in media_urls {
        rendered.push_str(&media_markdown(media_url));
        rendered.push('\n');
    }
    rendered
}

fn media_markdown(media_url: &str) -> String {
    match media_extension(media_url).as_deref() {
        Some("jpg" | "jpeg" | "png" | "gif" | "webp" | "svg") => {
            format!("![media]({media_url})")
        }
        Some("mp4" | "webm" | "mov") => {
            format!(
                "<video src=\"{}\" controls></video>",
                html_attr_escape(media_url)
            )
        }
        _ => format!("[media]({media_url})"),
    }
}

fn media_extension(media_url: &str) -> Option<String> {
    let path = url::Url::parse(media_url)
        .ok()
        .map(|url| url.path().to_owned())
        .unwrap_or_else(|| {
            media_url
                .split(['?', '#'])
                .next()
                .unwrap_or(media_url)
                .to_owned()
        });
    Path::new(&path)
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
}

fn html_attr_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '"' => escaped.push_str("&quot;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn build_action(
    verb: &str,
    fresh: Option<bool>,
    refresh: Option<bool>,
    reset: Option<bool>,
    guidance: Option<&str>,
    override_checks: Option<bool>,
    reason: Option<&str>,
) -> Result<api_types::TaskAction> {
    for (name, supplied, allowed) in [
        ("--fresh-session", fresh.is_some(), verb == "retry"),
        ("--refresh-workspace", refresh.is_some(), verb == "retry"),
        ("--reset-budget", reset.is_some(), verb == "retry"),
        (
            "--guidance",
            guidance.is_some(),
            matches!(verb, "retry" | "send_back"),
        ),
        ("--override", override_checks.is_some(), verb == "approve"),
        (
            "--reason",
            reason.is_some(),
            !matches!(verb, "start" | "send_back"),
        ),
    ] {
        anyhow::ensure!(!supplied || allowed, "{name} does not belong to {verb}");
    }
    if verb == "send_back" {
        anyhow::ensure!(
            guidance.is_some_and(|value| !value.trim().is_empty()),
            "send_back requires nonblank --guidance"
        );
    }
    if override_checks == Some(true) || reset == Some(false) {
        anyhow::ensure!(
            reason.is_some_and(|value| !value.trim().is_empty()),
            "this action requires nonblank --reason"
        );
    }
    let mut action = json!({"verb": verb});
    for (name, value) in [
        ("fresh_session", fresh),
        ("refresh_workspace", refresh),
        ("reset_budget", reset),
    ] {
        if let Some(value) = value {
            action[name] = json!(value);
        }
    }
    if let Some(value) = guidance {
        action["guidance"] = json!(value);
    }
    if let Some(value) = reason {
        action["reason"] = json!(value);
    }
    if verb == "approve" {
        action["override"] = json!(override_checks.unwrap_or(false));
    }
    Ok(serde_json::from_value(action)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    #[derive(Parser)]
    struct Cli {
        #[command(flatten)]
        task: TaskArgs,
    }
    #[test]
    fn actions_and_action_commands_parse() {
        assert!(matches!(
            Cli::try_parse_from(["ctl", "actions", "t"])
                .unwrap()
                .task
                .cmd,
            TaskCmd::Actions { .. }
        ));
        let parsed = Cli::try_parse_from([
            "ctl",
            "action",
            "t",
            "retry",
            "--fresh-session",
            "--reset-budget",
            "false",
            "--reason",
            "inspect first",
            "--guidance",
            "fix check",
        ])
        .unwrap();
        assert!(matches!(
            parsed.task.cmd,
            TaskCmd::Action {
                fresh_session: Some(true),
                reset_budget: Some(false),
                reason: Some(_),
                ..
            }
        ));
    }
    #[test]
    fn action_parameters_are_validated_locally() {
        assert!(build_action("send_back", None, None, None, None, None, None).is_err());
        assert!(build_action("send_back", None, None, None, Some("  "), None, None).is_err());
        assert!(build_action("cancel", Some(false), None, None, None, None, None).is_err());
        assert!(build_action("hold", None, None, None, None, Some(false), None).is_err());
        assert!(build_action("start", None, None, None, None, None, Some("why")).is_err());
        assert!(build_action("approve", None, None, None, None, Some(true), None).is_err());
        assert_eq!(
            serde_json::to_value(
                build_action(
                    "send_back",
                    None,
                    None,
                    None,
                    Some("Fix the failing check"),
                    None,
                    None
                )
                .unwrap()
            )
            .unwrap(),
            json!({"verb":"send_back","guidance":"Fix the failing check"})
        );
    }
}
