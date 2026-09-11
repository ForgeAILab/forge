use cli_adapters::CodexAdapter;
use executors::{CodingExecutorAdapter, ExecutionContext, ExecutionOutcome};
use serde_json::{Value, json};
use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

struct ChatProbe {
    called: std::sync::atomic::AtomicBool,
    arguments: std::sync::Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl cli_adapters::codex::client::ChatToolHandler for ChatProbe {
    fn specs(&self) -> Vec<cli_adapters::codex::protocol::DynamicToolSpec> {
        vec![cli_adapters::codex::protocol::DynamicToolSpec {
            name: "forge_chat_probe".to_owned(),
            description: "Record the nested probe request and return a marker.".to_owned(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "request": {
                        "type": "object",
                        "properties": {
                            "kind": {"type": "string", "enum": ["probe"]},
                            "payload": {
                                "type": "object",
                                "properties": {
                                    "message": {"type": "string", "enum": ["nested callback"]},
                                    "metadata": {
                                        "type": "object",
                                        "properties": {
                                            "priority": {"type": "string", "enum": ["high"]}
                                        },
                                        "required": ["priority"],
                                        "additionalProperties": false
                                    }
                                },
                                "required": ["message", "metadata"],
                                "additionalProperties": false
                            }
                        },
                        "required": ["kind", "payload"],
                        "additionalProperties": false
                    }
                },
                "required": ["request"],
                "additionalProperties": false
            }),
        }]
    }

    async fn call(&self, name: &str, call_id: &str, arguments: Value) -> Result<Value, String> {
        assert_eq!(name, "forge_chat_probe");
        assert!(!call_id.is_empty());
        self.arguments
            .lock()
            .expect("probe argument lock")
            .push(arguments);
        self.called.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(json!({"marker":"FORGE_CHAT_CALLBACK_OK"}))
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the managed Codex CLI and an authenticated account"]
async fn codex_chat_invokes_host_tool_without_a_git_repository() -> TestResult {
    let sandbox = tempfile::tempdir()?;
    let logs = tempfile::tempdir()?;
    let handler = std::sync::Arc::new(ChatProbe {
        called: std::sync::atomic::AtomicBool::new(false),
        arguments: std::sync::Mutex::new(Vec::new()),
    });
    let adapter = CodexAdapter::new().with_chat_tools(handler.clone());
    let result = tokio::time::timeout(Duration::from_secs(120), adapter.execute(ExecutionContext {
        task_id: "chat-probe".to_owned(), execution_id: "chat-probe-turn".to_owned(),
        worktree_path: sandbox.path().to_string_lossy().into_owned(),
        description: "Call forge_chat_probe once with arguments that conform to its advertised schema. Then reply with exactly the marker it returns. Use no other tools.".to_owned(),
        // The host capability must override this Task-style configuration.
        agent_config: json!({"auto_commit":true,"sandbox":"workspace-write","permission_policy":"auto"}),
        logs_path: logs.path().join("chat.jsonl").to_string_lossy().into_owned(),
        heartbeat_interval_seconds: 30, max_turns: None, log_sender: None,
    })).await??;
    assert_eq!(result.status, ExecutionOutcome::Completed, "{result:?}");
    assert!(
        handler.called.load(std::sync::atomic::Ordering::SeqCst),
        "host callback did not run: {result:?}"
    );
    let arguments = handler.arguments.lock().expect("probe argument lock");
    assert_eq!(arguments.len(), 1, "expected exactly one probe callback");
    assert_eq!(arguments[0]["request"]["kind"], "probe");
    assert_eq!(
        arguments[0]["request"]["payload"]["message"],
        "nested callback"
    );
    assert_eq!(
        arguments[0]["request"]["payload"]["metadata"]["priority"],
        "high"
    );
    assert!(
        result
            .summary
            .as_deref()
            .is_some_and(|text| text.contains("FORGE_CHAT_CALLBACK_OK")),
        "{result:?}"
    );
    assert_eq!(result.after_sha, None);
    assert!(!sandbox.path().join(".git").exists());
    Ok(())
}

struct DescriptionOnlyProbe {
    called: std::sync::atomic::AtomicBool,
    arguments: std::sync::Mutex<Vec<Value>>,
}

#[async_trait::async_trait]
impl cli_adapters::codex::client::ChatToolHandler for DescriptionOnlyProbe {
    fn specs(&self) -> Vec<cli_adapters::codex::protocol::DynamicToolSpec> {
        let guidance =
            "For operation probe, payload.message must be exactly `description callback`.";
        let mut properties = serde_json::Map::new();
        properties.insert(
            "operation".to_owned(),
            json!({"type":"string","enum":["probe"]}),
        );
        properties.insert(
            "payload".to_owned(),
            json!({
                "type":"object",
                "description":guidance,
                "additionalProperties":true
            }),
        );
        // Force Codex's 0.154 large-schema compaction pass. The payload
        // guidance is intentionally description-only and must survive via the
        // dynamic function description that the adapter promotes it to.
        properties.insert(
            "padding".to_owned(),
            json!({"type":"string","description":"padding ".repeat(1500)}),
        );
        vec![cli_adapters::codex::protocol::DynamicToolSpec {
            name: "forge_description_probe".to_owned(),
            description: "Record the description-guided probe and return a marker.".to_owned(),
            input_schema: json!({
                "type":"object",
                "properties":properties,
                "required":["operation","payload"],
                "additionalProperties":false
            }),
        }]
    }

    async fn call(&self, name: &str, call_id: &str, arguments: Value) -> Result<Value, String> {
        if name != "forge_description_probe" || call_id.is_empty() {
            return Err(format!("unexpected probe call {name}/{call_id}"));
        }
        self.arguments
            .lock()
            .expect("description probe argument lock")
            .push(arguments);
        self.called.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(json!({"marker":"FORGE_DESCRIPTION_GUIDANCE_OK"}))
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires the managed Codex CLI and an authenticated account"]
async fn codex_chat_uses_payload_description_after_large_schema_compaction() -> TestResult {
    let sandbox = tempfile::tempdir()?;
    let logs = tempfile::tempdir()?;
    let handler = std::sync::Arc::new(DescriptionOnlyProbe {
        called: std::sync::atomic::AtomicBool::new(false),
        arguments: std::sync::Mutex::new(Vec::new()),
    });
    let adapter = CodexAdapter::new().with_chat_tools(handler.clone());
    let result = tokio::time::timeout(
        Duration::from_secs(120),
        adapter.execute(ExecutionContext {
            task_id: "description-probe".to_owned(),
            execution_id: "description-probe-turn".to_owned(),
            worktree_path: sandbox.path().to_string_lossy().into_owned(),
            description: "Call forge_description_probe once using its advertised contract, then reply with exactly the marker it returns. Use no other tools.".to_owned(),
            agent_config: json!({"auto_commit":true,"sandbox":"workspace-write","permission_policy":"auto"}),
            logs_path: logs.path().join("chat.jsonl").to_string_lossy().into_owned(),
            heartbeat_interval_seconds: 30,
            max_turns: None,
            log_sender: None,
        }),
    )
    .await??;
    assert_eq!(result.status, ExecutionOutcome::Completed, "{result:?}");
    assert!(
        handler.called.load(std::sync::atomic::Ordering::SeqCst),
        "description-guided callback did not run: {result:?}"
    );
    let arguments = handler
        .arguments
        .lock()
        .expect("description probe argument lock");
    assert_eq!(
        arguments.len(),
        1,
        "expected exactly one description probe callback"
    );
    assert_eq!(arguments[0]["operation"], "probe");
    assert_eq!(arguments[0]["payload"]["message"], "description callback");
    assert!(
        result
            .summary
            .as_deref()
            .is_some_and(|text| text.contains("FORGE_DESCRIPTION_GUIDANCE_OK")),
        "{result:?}"
    );
    assert_eq!(result.after_sha, None);
    assert!(!sandbox.path().join(".git").exists());
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn codex_adapter_writes_file_in_live_repo() -> TestResult {
    if which::which("codex")
        .or_else(|_| which::which("npx"))
        .is_err()
    {
        println!("skipping Codex E2E: codex/npx not installed");
        return Ok(());
    }

    if !codex_auth_path().exists() {
        println!("skipping Codex E2E: ~/.codex/auth.json missing");
        return Ok(());
    }

    if !npx_package_available_offline(["--offline", "-y", "@openai/codex@0.154.0", "--version"]) {
        println!("skipping Codex E2E: @openai/codex npx package not available offline");
        return Ok(());
    }

    let tempdir = tempfile::tempdir()?;
    init_git_repo(tempdir.path())?;

    let logs_path = tempdir.path().join("log.jsonl");
    let ctx = ExecutionContext {
        worktree_path: tempdir.path().to_string_lossy().into_owned(),
        task_id: "smoke".to_owned(),
        execution_id: "smoke-exec".to_owned(),
        description: "Create a file called HELLO.md in the repo root with the single word banana."
            .to_owned(),
        agent_config: json!({
            "sandbox": "workspace-write",
            "permission_policy": "supervised"
        }),
        logs_path: logs_path.to_string_lossy().into_owned(),
        heartbeat_interval_seconds: 30,
        max_turns: None,
        log_sender: None,
    };

    let adapter = CodexAdapter::new();
    let result = tokio::time::timeout(Duration::from_secs(120), adapter.execute(ctx)).await??;

    assert_eq!(
        result.status,
        ExecutionOutcome::Completed,
        "unexpected Codex result: {result:?}"
    );
    assert!(
        result.agent_session_id.is_some(),
        "expected Codex thread id to be captured"
    );

    let hello_path = tempdir.path().join("HELLO.md");
    if let Err(error) = assert_hello_contains_banana(&hello_path) {
        if let Some(message) = last_assistant_message(&logs_path) {
            println!("last Codex assistant message: {message}");
        }
        return Err(error);
    }

    Ok(())
}

fn codex_auth_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".codex")
        .join("auth.json")
}

fn init_git_repo(path: &Path) -> TestResult {
    run_git(path, ["init", "-b", "main"])?;
    run_git(path, ["config", "user.email", "forge-e2e@example.com"])?;
    run_git(path, ["config", "user.name", "Forge E2E"])?;
    fs::write(path.join("README.md"), "smoke\n")?;
    run_git(path, ["add", "README.md"])?;
    run_git(path, ["commit", "-m", "initial commit"])?;
    Ok(())
}

fn run_git<const N: usize>(path: &Path, args: [&str; N]) -> TestResult {
    let output = Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(std::io::Error::other(format!("git command failed: {stderr}")).into())
    }
}

fn npx_package_available_offline<const N: usize>(args: [&str; N]) -> bool {
    Command::new("npx")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn assert_hello_contains_banana(path: &Path) -> TestResult {
    assert!(path.exists(), "expected HELLO.md to exist");
    let contents = fs::read_to_string(path)?;
    assert!(
        contents.to_ascii_lowercase().contains("banana"),
        "expected HELLO.md to contain banana, got: {contents:?}"
    );
    Ok(())
}

fn last_assistant_message(logs_path: &Path) -> Option<String> {
    let logs = fs::read_to_string(logs_path).ok()?;
    logs.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|entry| entry.get("kind").and_then(Value::as_str) == Some("assistant"))
        .filter_map(|entry| assistant_message_from_payload(entry.get("payload")?))
        .next_back()
}

fn assistant_message_from_payload(payload: &Value) -> Option<String> {
    for path in [
        &["params", "message"][..],
        &["params", "text"],
        &["params", "content"],
        &["params", "msg", "message"],
        &["params", "msg", "text"],
        &["message"],
        &["text"],
        &["content"],
    ] {
        if let Some(text) = value_at_path(payload, path).and_then(Value::as_str)
            && !text.trim().is_empty()
        {
            return Some(text.to_owned());
        }
    }

    Some(payload.to_string())
}

fn value_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}
