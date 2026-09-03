use super::{
    jsonrpc::{JsonRpcPeer, ServerMessage},
    normalize::{is_turn_completed, normalize_event},
    protocol::{
        ApprovalDecision, CancelTurnParams, CancelTurnResponse, CommandExecutionApprovalResponse,
        DynamicToolCallOutputContentItem, DynamicToolCallResponse, FileChangeApprovalResponse,
        InitializeCapabilities, InitializeParams, InitializeResponse, McpElicitationAction,
        McpElicitationResponse, RequestId, ReviewStartParams, ReviewStartResponse, ReviewTarget,
        ThreadForkParams, ThreadForkResponse, ThreadResumeParams, ThreadResumeResponse,
        ThreadStartParams, ThreadStartResponse, TurnHandle, TurnStartParams, TurnStartResponse,
        UserInput,
    },
};
use executors::{ExecutionOutcome, ExecutorError, LogKind, LogStream, LogWriter, TokenUsage};
use serde_json::{Value, json};
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::{
    process::{ChildStdin, ChildStdout},
    sync::{Mutex as AsyncMutex, mpsc},
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

pub struct CodexClient {
    rpc: JsonRpcPeer,
    messages: mpsc::Receiver<ServerMessage>,
    worktree_path: PathBuf,
    cancel: CancellationToken,
}

#[derive(Debug, Default)]
pub struct TurnRunResult {
    pub outcome: Option<ExecutionOutcome>,
    pub thread_id: Option<String>,
    pub summary: Option<String>,
    pub error: Option<String>,
    pub usage: Option<TokenUsage>,
}

impl CodexClient {
    pub fn spawn(
        stdin: ChildStdin,
        stdout: ChildStdout,
        worktree_path: impl Into<PathBuf>,
        cancel: CancellationToken,
    ) -> Self {
        let (rpc, messages) = JsonRpcPeer::spawn(stdin, stdout, cancel.clone());
        Self {
            rpc,
            messages,
            worktree_path: normalize_path(worktree_path.into()),
            cancel,
        }
    }

    pub async fn initialize(&self) -> Result<InitializeResponse, ExecutorError> {
        self.rpc
            .request(
                "initialize",
                InitializeParams {
                    client_info: super::protocol::ClientInfo {
                        name: "forge-codex-adapter".to_owned(),
                        title: Some("Forge Codex Adapter".to_owned()),
                        version: env!("CARGO_PKG_VERSION").to_owned(),
                    },
                    capabilities: InitializeCapabilities {
                        experimental_api: true,
                    },
                },
            )
            .await
    }

    pub async fn initialized(&self) -> Result<(), ExecutorError> {
        self.rpc.notify::<Value>("initialized", None).await
    }

    pub async fn thread_start(
        &self,
        params: ThreadStartParams,
    ) -> Result<ThreadStartResponse, ExecutorError> {
        self.rpc.request("thread/start", params).await
    }

    pub async fn thread_fork(
        &self,
        params: ThreadForkParams,
    ) -> Result<ThreadForkResponse, ExecutorError> {
        self.rpc.request("thread/fork", params).await
    }

    pub async fn thread_resume(
        &self,
        params: ThreadResumeParams,
    ) -> Result<ThreadResumeResponse, ExecutorError> {
        self.rpc.request("thread/resume", params).await
    }

    pub async fn turn_start(
        &self,
        thread_id: String,
        prompt: String,
    ) -> Result<TurnHandle, ExecutorError> {
        let response: TurnStartResponse = self
            .rpc
            .request(
                "turn/start",
                TurnStartParams {
                    thread_id,
                    input: vec![UserInput::Text {
                        text: prompt,
                        text_elements: vec![],
                    }],
                    collaboration_mode: None,
                },
            )
            .await?;
        Ok(response.into())
    }

    pub async fn start_review(
        &self,
        thread_id: String,
        target: ReviewTarget,
    ) -> Result<ReviewStartResponse, ExecutorError> {
        self.rpc
            .request(
                "review/start",
                ReviewStartParams {
                    thread_id,
                    target,
                    delivery: None,
                },
            )
            .await
    }

    pub async fn cancel_turn(
        &self,
        thread_id: String,
        turn_id: Option<String>,
    ) -> Result<CancelTurnResponse, ExecutorError> {
        self.rpc
            .request("turn/cancel", CancelTurnParams { thread_id, turn_id })
            .await
    }

    pub async fn run_until_turn_complete(
        &mut self,
        writer: Arc<AsyncMutex<LogWriter>>,
        mut stderr_rx: mpsc::Receiver<String>,
        heartbeat_interval_seconds: u64,
    ) -> Result<TurnRunResult, ExecutorError> {
        let mut result = TurnRunResult::default();
        let heartbeat_interval = std::time::Duration::from_secs(heartbeat_interval_seconds.max(1));
        let mut heartbeat = time::interval_at(
            time::Instant::now() + heartbeat_interval,
            heartbeat_interval,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => {
                    result.outcome = Some(ExecutionOutcome::Cancelled);
                    return Ok(result);
                }
                _ = heartbeat.tick() => {
                    write_log_stream(
                        &writer,
                        LogKind::SessionInfo,
                        LogStream::Heartbeat,
                        json!({ "type": "codex_turn_heartbeat" }),
                    )
                    .await?;
                }
                Some(line) = stderr_rx.recv() => {
                    write_log(&writer, LogKind::Stderr, json!({ "line": line })).await?;
                }
                message = self.messages.recv() => {
                    let Some(message) = message else {
                        result.outcome = Some(ExecutionOutcome::Failed);
                        result.error = Some("codex app-server stdout closed".to_owned());
                        return Ok(result);
                    };
                    if self.handle_server_message(message, &writer, &mut result).await? {
                        result.outcome = Some(if result.error.is_some() {
                            ExecutionOutcome::Failed
                        } else {
                            ExecutionOutcome::Completed
                        });
                        return Ok(result);
                    }
                }
            }
        }
    }

    async fn handle_server_message(
        &self,
        message: ServerMessage,
        writer: &Arc<AsyncMutex<LogWriter>>,
        result: &mut TurnRunResult,
    ) -> Result<bool, ExecutorError> {
        match message {
            ServerMessage::Request(request, raw) => {
                let diagnostic = set_error_if_present(&raw, result);
                self.write_normalized(writer, raw.clone(), result).await?;
                write_error_diagnostic(writer, diagnostic).await?;
                self.handle_server_request(request.id, &request.method, request.params, writer)
                    .await?;
                Ok(false)
            }
            ServerMessage::Notification(_notification, raw) => {
                let completed = is_turn_completed(&raw);
                let diagnostic = set_error_if_present(&raw, result);
                self.write_normalized(writer, raw, result).await?;
                write_error_diagnostic(writer, diagnostic).await?;
                Ok(completed)
            }
            ServerMessage::Response(raw) => {
                let diagnostic = set_error_if_present(&raw, result);
                self.write_normalized(writer, raw, result).await?;
                write_error_diagnostic(writer, diagnostic).await?;
                Ok(false)
            }
            ServerMessage::RawLine(line) => {
                write_log(writer, LogKind::Stderr, json!({ "line": line })).await?;
                Ok(false)
            }
        }
    }

    async fn write_normalized(
        &self,
        writer: &Arc<AsyncMutex<LogWriter>>,
        raw: Value,
        result: &mut TurnRunResult,
    ) -> Result<(), ExecutorError> {
        if let Some(usage) = extract_token_usage(&raw) {
            // Codex emits one `thread/tokenUsage/updated` per turn carrying that
            // turn's delta in `last`. Overwriting kept only the final turn and
            // discarded every earlier one — a whole session's work recorded as
            // its last exchange. The deltas sum to the `total` Codex reports.
            result.usage = super::merge_usage(result.usage.take(), Some(usage));
        }
        let normalized = normalize_event(raw);
        if let Some(thread_id) = normalized.thread_id {
            result.thread_id = Some(thread_id);
        }
        if let Some(message) = normalized.assistant_message {
            result.summary = Some(message);
        }
        write_log(writer, normalized.kind, normalized.payload).await
    }

    async fn handle_server_request(
        &self,
        id: RequestId,
        method: &str,
        params: Value,
        writer: &Arc<AsyncMutex<LogWriter>>,
    ) -> Result<(), ExecutorError> {
        let lower = method.to_ascii_lowercase();
        if lower == "item/tool/call" || lower.contains("dynamictoolcall") {
            self.handle_dynamic_tool_call(id, params, writer).await
        } else if lower == "mcpserver/elicitation/request" {
            self.handle_mcp_elicitation_request(id, params, writer)
                .await
        } else if lower.contains("commandexecution") && lower.contains("approval") {
            self.handle_approval_request(id, params, writer, ApprovalRequestKind::Command)
                .await
        } else if lower.contains("filechange") && lower.contains("approval") {
            self.handle_approval_request(id, params, writer, ApprovalRequestKind::File)
                .await
        } else {
            self.rpc.respond(id, Value::Null).await
        }
    }

    async fn handle_dynamic_tool_call(
        &self,
        id: RequestId,
        params: Value,
        writer: &Arc<AsyncMutex<LogWriter>>,
    ) -> Result<(), ExecutorError> {
        let tool = string_field(&params, &["tool", "name"]).unwrap_or("unknown");
        let call_id = string_field(&params, &["callId", "call_id", "id", "itemId", "item_id"])
            .unwrap_or("unknown");
        write_log(
            writer,
            LogKind::ToolCall,
            json!({
                "type": "dynamic_tool_call",
                "tool": tool,
                "call_id": call_id,
                "params": params,
            }),
        )
        .await?;
        self.rpc
            .respond(
                id,
                DynamicToolCallResponse {
                    content_items: vec![DynamicToolCallOutputContentItem::InputText {
                        text: "tool not supported by forge adapter".to_owned(),
                    }],
                    success: false,
                },
            )
            .await
    }

    async fn handle_mcp_elicitation_request(
        &self,
        id: RequestId,
        params: Value,
        writer: &Arc<AsyncMutex<LogWriter>>,
    ) -> Result<(), ExecutorError> {
        let allowed = mcp_tool_elicitation_allowed(&params);
        let response = if allowed {
            McpElicitationResponse {
                action: McpElicitationAction::Accept,
                content: Some(json!({})),
            }
        } else {
            McpElicitationResponse {
                action: McpElicitationAction::Decline,
                content: None,
            }
        };
        self.rpc.respond(id, response).await?;

        write_log(
            writer,
            if allowed {
                LogKind::ToolCall
            } else {
                LogKind::ToolResult
            },
            json!({
                "type": "mcp_elicitation_response",
                "decision": if allowed { "accept" } else { "decline" },
                "params": params,
            }),
        )
        .await
    }

    async fn handle_approval_request(
        &self,
        id: RequestId,
        params: Value,
        writer: &Arc<AsyncMutex<LogWriter>>,
        kind: ApprovalRequestKind,
    ) -> Result<(), ExecutorError> {
        let allowed = self.approval_allowed(&params);
        let decision = if allowed {
            ApprovalDecision::Accept
        } else {
            ApprovalDecision::Decline
        };
        match kind {
            ApprovalRequestKind::Command => {
                self.rpc
                    .respond(
                        id,
                        CommandExecutionApprovalResponse {
                            decision: decision.clone(),
                        },
                    )
                    .await?;
            }
            ApprovalRequestKind::File => {
                self.rpc
                    .respond(
                        id,
                        FileChangeApprovalResponse {
                            decision: decision.clone(),
                        },
                    )
                    .await?;
            }
        }

        if !allowed {
            write_log(
                writer,
                LogKind::ToolResult,
                json!({
                    "type": "approval_denied",
                    "rationale": "requested path is outside the worktree",
                    "params": params,
                }),
            )
            .await?;
        }
        Ok(())
    }

    fn approval_allowed(&self, params: &Value) -> bool {
        let mut paths = Vec::new();
        collect_path_candidates(params, &mut paths);
        paths
            .iter()
            .filter(|path| !path.trim().is_empty())
            .all(|path| self.path_inside_worktree(path))
    }

    fn path_inside_worktree(&self, path: &str) -> bool {
        let candidate = Path::new(path);
        let absolute = if candidate.is_absolute() {
            normalize_path(candidate.to_path_buf())
        } else {
            normalize_path(self.worktree_path.join(candidate))
        };
        absolute.starts_with(&self.worktree_path)
    }
}

fn mcp_tool_elicitation_allowed(params: &Value) -> bool {
    let approval_kind = params
        .get("_meta")
        .and_then(|value| string_field(value, &["codex_approval_kind"]));
    approval_kind == Some("mcp_tool_call")
}

fn set_error_if_present(raw: &Value, result: &mut TurnRunResult) -> Option<String> {
    let error = super::codex_event_error_message(raw)?;

    let should_replace = match result.error.as_deref() {
        None => true,
        Some(existing) => {
            existing == super::CODEX_SYSTEM_ERROR_FALLBACK
                && error != super::CODEX_SYSTEM_ERROR_FALLBACK
        }
    };

    if should_replace {
        result.error = Some(error.clone());
        Some(error)
    } else {
        None
    }
}

async fn write_error_diagnostic(
    writer: &Arc<AsyncMutex<LogWriter>>,
    error: Option<String>,
) -> Result<(), ExecutorError> {
    let Some(error) = error else {
        return Ok(());
    };

    write_log(
        writer,
        LogKind::System,
        json!({
            "type": "codex_protocol_error",
            "message": error,
        }),
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum ApprovalRequestKind {
    Command,
    File,
}

async fn write_log(
    writer: &Arc<AsyncMutex<LogWriter>>,
    kind: LogKind,
    payload: Value,
) -> Result<(), ExecutorError> {
    write_log_stream(writer, kind, LogStream::Main, payload).await
}

async fn write_log_stream(
    writer: &Arc<AsyncMutex<LogWriter>>,
    kind: LogKind,
    stream: LogStream,
    payload: Value,
) -> Result<(), ExecutorError> {
    writer
        .lock()
        .await
        .write(kind, stream, payload)
        .await
        .map_err(ExecutorError::Io)
}

fn string_field<'a>(value: &'a Value, fields: &[&str]) -> Option<&'a str> {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
}

fn extract_token_usage(raw: &Value) -> Option<TokenUsage> {
    let token_usage = raw
        .get("params")
        .and_then(|params| params.get("tokenUsage"))
        .or_else(|| raw.get("tokenUsage"))?;
    // `last` is this turn's delta and the caller accumulates it. `total` is
    // Codex's running thread total, which must not be added to anything.
    let usage = token_usage.get("last")?;
    // Codex reports `inputTokens` inclusive of `cachedInputTokens` — its own
    // `totalTokens` equals `inputTokens + outputTokens`. `TokenUsage` keeps the
    // three input counters disjoint, so the cached prefix comes back out.
    let cache_read_tokens = i64_field(usage, &["cachedInputTokens", "cached_input_tokens"]);
    Some(TokenUsage {
        input_tokens: i64_field(usage, &["inputTokens", "input_tokens"])
            .saturating_sub(cache_read_tokens)
            .max(0),
        // Reasoning output is billed output; the embedded host already folds it in.
        output_tokens: i64_field(usage, &["outputTokens", "output_tokens"]).saturating_add(
            i64_field(usage, &["reasoningOutputTokens", "reasoning_output_tokens"]),
        ),
        cache_read_tokens,
        cache_write_tokens: i64_field(
            usage,
            &["cacheWriteInputTokens", "cache_write_input_tokens"],
        ),
        cost_usd: None,
        model: string_field(usage, &["model"]).map(str::to_owned),
    })
}

fn i64_field(value: &Value, fields: &[&str]) -> i64 {
    fields
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_i64))
        .unwrap_or(0)
}

fn collect_path_candidates(value: &Value, output: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let lower = key.to_ascii_lowercase();
                if matches!(lower.as_str(), "path" | "cwd" | "workingdirectory")
                    && let Some(path) = value.as_str()
                {
                    output.push(path.to_owned());
                }
                collect_path_candidates(value, output);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_path_candidates(item, output);
            }
        }
        _ => {}
    }
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_forge_mcp_tool_elicitation() {
        let params = json!({
            "_meta": {
                "codex_approval_kind": "mcp_tool_call",
                "tool_params": {},
            },
            "message": "Allow the forge MCP server to run tool \"forge_create_task\"?",
            "serverName": "forge",
        });

        assert!(mcp_tool_elicitation_allowed(&params));
    }

    #[test]
    fn allows_context_mcp_tool_elicitation() {
        let params = json!({
            "_meta": {
                "codex_approval_kind": "mcp_tool_call",
                "message": "Allow MCP tool \"resolve-library-id\"?",
            },
            "serverName": "context7",
        });

        assert!(mcp_tool_elicitation_allowed(&params));
    }

    #[test]
    fn rejects_non_mcp_tool_elicitation() {
        let params = json!({
            "_meta": {
                "codex_approval_kind": "file_write",
            },
            "message": "Allow the chrome-devtools MCP server to run tool \"list_pages\"?",
            "serverName": "chrome-devtools",
        });

        assert!(!mcp_tool_elicitation_allowed(&params));
    }

    #[test]
    fn extracts_codex_thread_token_usage_last_turn() {
        let raw = json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "last": {
                        "cachedInputTokens": 48512,
                        "inputTokens": 49052,
                        "outputTokens": 25,
                        "reasoningOutputTokens": 0,
                        "totalTokens": 49077
                    },
                    "total": {
                        "cachedInputTokens": 650240,
                        "inputTokens": 705590,
                        "outputTokens": 2957,
                        "reasoningOutputTokens": 257,
                        "totalTokens": 708547
                    }
                }
            }
        });

        let usage = extract_token_usage(&raw).expect("usage extracted");

        // 49052 reported inclusive of the 48512-token cached prefix.
        assert_eq!(usage.input_tokens, 540);
        assert_eq!(usage.output_tokens, 25);
        assert_eq!(usage.cache_read_tokens, 48512);
        assert_eq!(usage.cache_write_tokens, 0);
        assert_eq!(usage.cost_usd, None);
    }

    #[test]
    fn codex_thread_token_usage_reads_the_turn_delta_not_the_running_total() {
        // A mid-session notification: `last` is one turn, `total` is every turn
        // so far. Reading `total` and accumulating it would compound.
        let raw = json!({
            "method": "thread/tokenUsage/updated",
            "params": {
                "tokenUsage": {
                    "last": {
                        "cachedInputTokens": 100,
                        "inputTokens": 150,
                        "outputTokens": 10,
                        "reasoningOutputTokens": 4,
                        "totalTokens": 160
                    },
                    "total": {
                        "cachedInputTokens": 9000,
                        "inputTokens": 10000,
                        "outputTokens": 700,
                        "reasoningOutputTokens": 300,
                        "totalTokens": 10700
                    }
                }
            }
        });

        let usage = extract_token_usage(&raw).expect("usage extracted");

        assert_eq!(
            usage.input_tokens, 50,
            "fresh input excludes the cached prefix"
        );
        assert_eq!(usage.cache_read_tokens, 100);
        assert_eq!(usage.output_tokens, 14, "reasoning output is billed output");
    }

    #[test]
    fn codex_turn_deltas_accumulate_to_the_session_total() {
        // Three turns of a real session: Codex's own running `total` is the sum
        // of the deltas, so accumulating the deltas reproduces it.
        let deltas = [
            (41290_i64, 40960_i64, 247_i64, 65_i64),
            (12000, 9000, 300, 40),
            (5000, 1000, 90, 10),
        ];
        let mut acc: Option<TokenUsage> = None;
        for (input, cached, output, reasoning) in deltas {
            let raw = json!({
                "method": "thread/tokenUsage/updated",
                "params": {"tokenUsage": {"last": {
                    "inputTokens": input, "cachedInputTokens": cached,
                    "outputTokens": output, "reasoningOutputTokens": reasoning
                }}}
            });
            acc = super::super::merge_usage(acc, extract_token_usage(&raw));
        }
        let usage = acc.expect("accumulated");
        let expected_input: i64 = deltas.iter().map(|d| d.0 - d.1).sum();
        let expected_cached: i64 = deltas.iter().map(|d| d.1).sum();
        let expected_output: i64 = deltas.iter().map(|d| d.2 + d.3).sum();
        assert_eq!(usage.input_tokens, expected_input);
        assert_eq!(usage.cache_read_tokens, expected_cached);
        assert_eq!(usage.output_tokens, expected_output);
        // Context consumed equals what Codex reports as its cumulative input.
        assert_eq!(
            usage.input_tokens + usage.cache_read_tokens,
            deltas.iter().map(|d| d.0).sum::<i64>()
        );
    }

    #[tokio::test]
    async fn writes_codex_heartbeat_on_heartbeat_stream() {
        let dir = tempfile::tempdir().expect("tempdir creates");
        let log_path = dir.path().join("codex.jsonl");
        let writer = Arc::new(AsyncMutex::new(LogWriter::new(
            &log_path,
            "execution-id".to_owned(),
            1024 * 1024,
        )));

        write_log_stream(
            &writer,
            LogKind::SessionInfo,
            LogStream::Heartbeat,
            json!({ "type": "codex_turn_heartbeat" }),
        )
        .await
        .expect("heartbeat writes");

        let entries = executors::LogReader::read(&log_path, 0, 10)
            .await
            .expect("log reads")
            .entries;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].stream, LogStream::Heartbeat);
        assert_eq!(entries[0].kind, LogKind::SessionInfo);
        assert_eq!(entries[0].payload["type"], "codex_turn_heartbeat");
    }
}
