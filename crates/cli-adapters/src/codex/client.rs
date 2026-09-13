use super::{
    jsonrpc::{JsonRpcPeer, ServerMessage},
    normalize::{is_turn_completed, normalize_event},
    protocol::{
        ApprovalDecision, CancelTurnParams, CancelTurnResponse, CommandExecutionApprovalResponse,
        DynamicToolCallOutputContentItem, DynamicToolCallResponse, DynamicToolSpec,
        FileChangeApprovalResponse, InitializeCapabilities, InitializeParams, InitializeResponse,
        McpElicitationAction, McpElicitationResponse, RequestId, ReviewStartParams,
        ReviewStartResponse, ReviewTarget, ThreadForkParams, ThreadForkResponse,
        ThreadResumeParams, ThreadResumeResponse, ThreadStartParams, ThreadStartResponse,
        TurnHandle, TurnStartParams, TurnStartResponse, UserInput,
    },
};
use async_trait::async_trait;
use executors::{
    ExecutionOutcome, ExecutorError, LogKind, LogStream, LogWriter, UsageCounters, UsageReport,
};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};
use tokio::{
    process::{ChildStdin, ChildStdout},
    sync::{Mutex as AsyncMutex, mpsc},
    time::{self, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

pub struct CodexClient {
    rpc: JsonRpcPeer,
    messages: mpsc::Receiver<ServerMessage>,
    cancel: CancellationToken,
    chat_tools: Option<Arc<dyn ChatToolHandler>>,
    suppress_next_response: bool,
}

/// Host-owned dynamic tools available to one Codex chat turn.
///
/// The adapter advertises only the specs returned by this handler and rejects
/// every other `item/tool/call` request before invoking the host.
#[async_trait]
pub trait ChatToolHandler: Send + Sync {
    fn specs(&self) -> Vec<DynamicToolSpec>;

    async fn call(&self, name: &str, call_id: &str, arguments: Value) -> Result<Value, String>;
}

#[derive(Debug, Default)]
pub struct TurnRunResult {
    pub outcome: Option<ExecutionOutcome>,
    pub thread_id: Option<String>,
    pub summary: Option<String>,
    pub error: Option<String>,
    pub usage_reports: Vec<UsageReport>,
}

impl CodexClient {
    pub fn spawn(
        stdin: ChildStdin,
        stdout: ChildStdout,
        worktree_path: impl Into<PathBuf>,
        cancel: CancellationToken,
    ) -> Self {
        Self::spawn_with_chat_tools(stdin, stdout, worktree_path, cancel, None)
    }

    pub fn spawn_with_chat_tools(
        stdin: ChildStdin,
        stdout: ChildStdout,
        _worktree_path: impl Into<PathBuf>,
        cancel: CancellationToken,
        chat_tools: Option<Arc<dyn ChatToolHandler>>,
    ) -> Self {
        let (rpc, messages) = JsonRpcPeer::spawn(stdin, stdout, cancel.clone());
        Self {
            rpc,
            messages,
            cancel,
            chat_tools,
            suppress_next_response: false,
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

    /// Read Codex's effective configuration for a working directory. Chat
    /// turns use this only to discover inherited MCP server names so they can
    /// disable each one in the thread-local config overlay.
    pub async fn config_read(&mut self, cwd: impl Into<String>) -> Result<Value, ExecutorError> {
        self.suppress_next_response = true;
        let response = self
            .rpc
            .request(
                "config/read",
                json!({
                    "cwd": cwd.into(),
                    "includeLayers": false,
                }),
            )
            .await;
        if response.is_err() {
            self.suppress_next_response = false;
        }
        response
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
        &mut self,
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
                if self.suppress_next_response {
                    self.suppress_next_response = false;
                    return Ok(false);
                }
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
        if let Some(usage) = extract_token_usage(&raw)? {
            // Codex emits one `thread/tokenUsage/updated` per turn carrying
            // that turn's non-overlapping delta in `last`. Keep each provider
            // report and merge only the anonymous deltas for this invocation.
            append_usage_report(&mut result.usage_reports, usage);
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

        let response = match dispatch_chat_tool_response(self.chat_tools.as_deref(), &params).await
        {
            Ok(response) => response,
            Err(message) => DynamicToolCallResponse {
                content_items: vec![DynamicToolCallOutputContentItem::InputText { text: message }],
                success: false,
            },
        };
        let success = response.success;
        let result_text = response
            .content_items
            .first()
            .map(|item| match item {
                DynamicToolCallOutputContentItem::InputText { text } => text.clone(),
            })
            .unwrap_or_default();
        let result_value = serde_json::from_str::<Value>(&result_text)
            .unwrap_or_else(|_| Value::String(result_text.clone()));

        write_log(
            writer,
            LogKind::ToolResult,
            json!({
                "type": "dynamic_tool_result",
                "tool": tool,
                "call_id": call_id,
                "success": success,
                "text": result_text,
                "result": result_value,
            }),
        )
        .await?;

        self.rpc.respond(id, response).await
    }

    async fn handle_mcp_elicitation_request(
        &self,
        id: RequestId,
        params: Value,
        writer: &Arc<AsyncMutex<LogWriter>>,
    ) -> Result<(), ExecutorError> {
        let allowed = self.chat_tools.is_none() && mcp_tool_elicitation_allowed(&params);
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
        // Forge has no user-facing bridge for Codex's built-in command/file
        // approval prompts. Auto-accepting here would turn a request to leave
        // the Task sandbox into host authority, so every such request is
        // fail-closed. Commands that fit the declared sandbox run without
        // reaching this callback.
        let decision = ApprovalDecision::Decline;
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

        write_log(
            writer,
            LogKind::ToolResult,
            json!({
                "type": "approval_response",
                "request_kind": match kind {
                    ApprovalRequestKind::Command => "command",
                    ApprovalRequestKind::File => "file",
                },
                "decision": "decline",
                "rationale": if self.chat_tools.is_some() {
                    "builtin command/file approvals are disabled for chat turns"
                } else {
                    "managed executions cannot escalate beyond the declared Task sandbox"
                },
                "params": params,
            }),
        )
        .await
    }
}

async fn dispatch_chat_tool_response(
    handler: Option<&dyn ChatToolHandler>,
    params: &Value,
) -> Result<DynamicToolCallResponse, String> {
    let Some(handler) = handler else {
        return Err("tool not supported by forge adapter".to_owned());
    };
    let Some(tool) = string_field(params, &["tool", "name"]) else {
        return Err("dynamic tool call is missing tool".to_owned());
    };
    let Some(call_id) = string_field(params, &["callId", "call_id", "id", "itemId", "item_id"])
    else {
        return Err("dynamic tool call is missing callId".to_owned());
    };
    if !handler.specs().iter().any(|spec| spec.name == tool) {
        return Err(format!("dynamic tool is not registered: {tool}"));
    }
    if params
        .get("namespace")
        .is_some_and(|namespace| !namespace.is_null())
    {
        return Err(format!("namespaced dynamic tool is not registered: {tool}"));
    }
    let Some(arguments) = params.get("arguments").cloned() else {
        return Err(format!("dynamic tool call is missing arguments: {tool}"));
    };

    match handler.call(tool, call_id, arguments).await {
        Ok(value) => Ok(DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText {
                text: serde_json::to_string(&value)
                    .map_err(|error| format!("failed to encode dynamic tool result: {error}"))?,
            }],
            success: true,
        }),
        Err(error) => Ok(DynamicToolCallResponse {
            content_items: vec![DynamicToolCallOutputContentItem::InputText { text: error }],
            success: false,
        }),
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

fn extract_token_usage(raw: &Value) -> Result<Option<UsageReport>, ExecutorError> {
    let Some(token_usage) = raw
        .get("params")
        .and_then(|params| params.get("tokenUsage"))
        .or_else(|| raw.get("tokenUsage"))
    else {
        return Ok(None);
    };
    // `last` is this turn's delta and the caller accumulates it. `total` is
    // Codex's running thread total, which must not be added to anything.
    let Some(usage) = token_usage.get("last") else {
        return Ok(None);
    };
    // Codex reports `inputTokens` inclusive of `cachedInputTokens` — its own
    // `totalTokens` equals `inputTokens + outputTokens`. The report keeps the
    // input counters disjoint, so the cached prefix comes back out.
    let cache_read_tokens =
        optional_u64_field(usage, &["cachedInputTokens", "cached_input_tokens"]);
    let raw_input = optional_u64_field(usage, &["inputTokens", "input_tokens"]);
    let input_tokens = match (raw_input, cache_read_tokens) {
        (Some(input), Some(cached)) => Some(input.checked_sub(cached).ok_or_else(|| {
            ExecutorError::Other(
                "codex cached input token count exceeds total input token count".to_owned(),
            )
        })?),
        (Some(input), None) => Some(input),
        (None, _) => None,
    };
    // Reasoning output is billed output; the embedded host already folds it in.
    let output_tokens = match (
        optional_u64_field(usage, &["outputTokens", "output_tokens"]),
        optional_u64_field(usage, &["reasoningOutputTokens", "reasoning_output_tokens"]),
    ) {
        (None, None) => None,
        (Some(value), None) | (None, Some(value)) => Some(value),
        (Some(output), Some(reasoning)) => {
            Some(output.checked_add(reasoning).ok_or_else(|| {
                ExecutorError::Other("codex output token counter overflow".to_owned())
            })?)
        }
    };
    let counters = UsageCounters {
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_write_tokens: optional_u64_field(
            usage,
            &["cacheWriteInputTokens", "cache_write_input_tokens"],
        ),
    };
    if !counters.has_any() {
        return Ok(None);
    }
    let mut report = UsageReport::metered(String::new(), counters);
    report.report_id = string_field(
        usage,
        &["report_id", "reportId", "id", "request_id", "requestId"],
    )
    .or_else(|| string_field(raw, &["report_id", "reportId", "id"]))
    .map(str::to_owned)
    .unwrap_or_default();
    report.request_id = string_field(usage, &["request_id", "requestId", "turn_id", "turnId"])
        .or_else(|| string_field(raw, &["request_id", "requestId", "turn_id", "turnId"]))
        .map(str::to_owned);
    report.provider_id = string_field(
        usage,
        &["provider_id", "provider", "model_provider", "modelProvider"],
    )
    .or_else(|| {
        string_field(
            raw,
            &["provider_id", "provider", "model_provider", "modelProvider"],
        )
    })
    .map(str::to_owned);
    report.model_id = string_field(usage, &["model_id", "model"])
        .or_else(|| string_field(raw, &["model_id", "model"]))
        .map(str::to_owned);
    Ok(Some(report))
}

fn optional_u64_field(value: &Value, fields: &[&str]) -> Option<u64> {
    fields.iter().find_map(|field| {
        value.get(*field).and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_i64().filter(|value| *value >= 0).map(|v| v as u64))
        })
    })
}

/// Preserve distinct request reports, while combining the anonymous Codex
/// turn deltas that are explicitly documented as non-overlapping.
pub(crate) fn append_usage_report(reports: &mut Vec<UsageReport>, next: UsageReport) {
    if let Some(existing) = reports.iter_mut().find(|existing| {
        (next.request_id.is_some() && next.request_id == existing.request_id)
            || (!next.report_id.is_empty()
                && !existing.report_id.is_empty()
                && next.report_id == existing.report_id)
    }) {
        *existing = next;
        return;
    }
    if next.request_id.is_none()
        && next.report_id.is_empty()
        && let Some(existing) = reports.last_mut()
        && existing.request_id.is_none()
        && existing.report_id.is_empty()
        && existing.merge_delta(&next).is_ok()
    {
        return;
    }
    reports.push(next);
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    #[derive(Clone, Default)]
    struct TestChatToolHandler {
        calls: Arc<Mutex<Vec<(String, String, Value)>>>,
    }

    #[async_trait]
    impl ChatToolHandler for TestChatToolHandler {
        fn specs(&self) -> Vec<DynamicToolSpec> {
            vec![DynamicToolSpec {
                name: "forge_echo".to_owned(),
                description: "Echo a JSON value".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": { "value": {} },
                }),
            }]
        }

        async fn call(&self, name: &str, call_id: &str, arguments: Value) -> Result<Value, String> {
            self.calls.lock().expect("test handler lock").push((
                name.to_owned(),
                call_id.to_owned(),
                arguments.clone(),
            ));
            Ok(json!({ "name": name, "call_id": call_id, "arguments": arguments }))
        }
    }

    #[tokio::test]
    async fn dynamic_tool_call_rejects_unregistered_tool_without_dispatch() {
        let handler = TestChatToolHandler::default();
        let result = dispatch_chat_tool_response(
            Some(&handler),
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "callId": "call-1",
                "namespace": null,
                "tool": "forge_delete_everything",
                "arguments": {},
            }),
        )
        .await;

        assert_eq!(
            result.expect_err("unknown tool must be rejected"),
            "dynamic tool is not registered: forge_delete_everything"
        );
        assert!(handler.calls.lock().expect("test handler lock").is_empty());
    }

    #[tokio::test]
    async fn dynamic_tool_call_dispatches_and_encodes_json_result_as_input_text() {
        let handler = TestChatToolHandler::default();
        let result = dispatch_chat_tool_response(
            Some(&handler),
            &json!({
                "threadId": "thread-1",
                "turnId": "turn-1",
                "callId": "call-1",
                "namespace": null,
                "tool": "forge_echo",
                "arguments": { "value": 42 },
            }),
        )
        .await
        .expect("registered tool dispatches");

        assert!(result.success);
        let DynamicToolCallOutputContentItem::InputText { text } = &result.content_items[0];
        let decoded: Value = serde_json::from_str(text).expect("result is JSON text");
        assert_eq!(decoded["name"], "forge_echo");
        assert_eq!(decoded["call_id"], "call-1");
        assert_eq!(decoded["arguments"]["value"], 42);
        assert_eq!(
            handler.calls.lock().expect("test handler lock").as_slice(),
            &[(
                "forge_echo".to_owned(),
                "call-1".to_owned(),
                json!({ "value": 42 }),
            )]
        );
    }

    #[tokio::test]
    async fn dynamic_tool_request_dispatches_and_logs_result() {
        let dir = tempfile::tempdir().expect("tempdir creates");
        let log_path = dir.path().join("codex.jsonl");
        let mut child = tokio::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("cat starts");
        let stdin = child.stdin.take().expect("cat stdin");
        let stdout = child.stdout.take().expect("cat stdout");
        let cancel = CancellationToken::new();
        let handler = TestChatToolHandler::default();
        let mut client = CodexClient::spawn_with_chat_tools(
            stdin,
            stdout,
            dir.path(),
            cancel.clone(),
            Some(Arc::new(handler)),
        );
        let writer = Arc::new(AsyncMutex::new(LogWriter::new(
            &log_path,
            "execution-id".to_owned(),
            1024 * 1024,
        )));

        client
            .handle_server_request(
                RequestId::Number(7),
                "item/tool/call",
                json!({
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "callId": "call-1",
                    "namespace": null,
                    "tool": "forge_echo",
                    "arguments": { "value": 42 },
                }),
                &writer,
            )
            .await
            .expect("dynamic tool request handled");

        let message =
            tokio::time::timeout(std::time::Duration::from_secs(1), client.messages.recv())
                .await
                .expect("response arrives")
                .expect("response message");
        let ServerMessage::Response(raw) = message else {
            panic!("expected JSON-RPC response, got another message");
        };
        assert_eq!(raw["id"], 7);
        assert_eq!(raw["result"]["success"], true);
        assert_eq!(raw["result"]["contentItems"][0]["type"], "inputText");
        let returned: Value = serde_json::from_str(
            raw["result"]["contentItems"][0]["text"]
                .as_str()
                .expect("input text"),
        )
        .expect("input text contains JSON");
        assert_eq!(returned["arguments"]["value"], 42);

        let entries = executors::LogReader::read(&log_path, 0, 20)
            .await
            .expect("logs read")
            .entries;
        let result_log = entries
            .iter()
            .find(|entry| entry.payload["type"] == "dynamic_tool_result")
            .expect("dynamic tool result logged");
        assert_eq!(result_log.kind, LogKind::ToolResult);
        assert_eq!(result_log.payload["success"], true);
        assert_eq!(result_log.payload["result"]["call_id"], "call-1");
        assert!(
            result_log.payload["text"]
                .as_str()
                .unwrap()
                .contains("forge_echo")
        );

        cancel.cancel();
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

    #[tokio::test]
    async fn builtin_approvals_fail_closed_with_or_without_path_metadata() {
        let dir = tempfile::tempdir().expect("tempdir creates");
        let log_path = dir.path().join("codex.jsonl");
        let mut child = tokio::process::Command::new("cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("cat starts");
        let stdin = child.stdin.take().expect("cat stdin");
        let stdout = child.stdout.take().expect("cat stdout");
        let cancel = CancellationToken::new();
        let mut client = CodexClient::spawn(stdin, stdout, dir.path(), cancel.clone());
        let writer = Arc::new(AsyncMutex::new(LogWriter::new(
            &log_path,
            "execution-id".to_owned(),
            1024 * 1024,
        )));

        for (id, method, params) in [
            (
                11,
                "item/commandExecution/requestApproval",
                json!({"command": "git merge task/unsafe"}),
            ),
            (
                12,
                "item/fileChange/requestApproval",
                json!({"path": dir.path().join("inside.txt")}),
            ),
        ] {
            client
                .handle_server_request(RequestId::Number(id), method, params, &writer)
                .await
                .expect("approval request handled");
            let message =
                tokio::time::timeout(std::time::Duration::from_secs(1), client.messages.recv())
                    .await
                    .expect("response arrives")
                    .expect("response message");
            let ServerMessage::Response(raw) = message else {
                panic!("expected JSON-RPC response, got another message");
            };
            assert_eq!(raw["id"], id);
            assert_eq!(raw["result"]["decision"], "decline");
        }

        let entries = executors::LogReader::read(&log_path, 0, 20)
            .await
            .expect("logs read")
            .entries;
        let decisions: Vec<_> = entries
            .iter()
            .filter(|entry| entry.payload["type"] == "approval_response")
            .collect();
        assert_eq!(decisions.len(), 2);
        assert!(
            decisions
                .iter()
                .all(|entry| entry.payload["decision"] == "decline")
        );

        cancel.cancel();
        let _ = child.start_kill();
        let _ = child.wait().await;
    }

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

        let usage = extract_token_usage(&raw)
            .expect("usage extraction succeeds")
            .expect("usage extracted");

        // 49052 reported inclusive of the 48512-token cached prefix.
        assert_eq!(usage.counters.input_tokens, Some(540));
        assert_eq!(usage.counters.output_tokens, Some(25));
        assert_eq!(usage.counters.cache_read_tokens, Some(48512));
        assert_eq!(usage.counters.cache_write_tokens, None);
        assert_eq!(usage.reported_cost_usd, None);
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

        let usage = extract_token_usage(&raw)
            .expect("usage extraction succeeds")
            .expect("usage extracted");

        assert_eq!(
            usage.counters.input_tokens,
            Some(50),
            "fresh input excludes the cached prefix"
        );
        assert_eq!(usage.counters.cache_read_tokens, Some(100));
        assert_eq!(
            usage.counters.output_tokens,
            Some(14),
            "reasoning output is billed output"
        );
    }

    #[test]
    fn codex_turn_deltas_accumulate_to_the_session_total() {
        // Three turns of a real session: Codex's own running `total` is the sum
        // of the deltas, so accumulating the deltas reproduces it.
        let deltas = [
            (41290_u64, 40960_u64, 247_u64, 65_u64),
            (12000, 9000, 300, 40),
            (5000, 1000, 90, 10),
        ];
        let mut reports = Vec::new();
        for (input, cached, output, reasoning) in deltas {
            let raw = json!({
                "method": "thread/tokenUsage/updated",
                "params": {"tokenUsage": {"last": {
                    "inputTokens": input, "cachedInputTokens": cached,
                    "outputTokens": output, "reasoningOutputTokens": reasoning
                }}}
            });
            if let Some(report) = extract_token_usage(&raw).expect("usage extraction succeeds") {
                append_usage_report(&mut reports, report);
            }
        }
        let usage = reports.first().expect("accumulated");
        let expected_input: u64 = deltas.iter().map(|d| d.0 - d.1).sum();
        let expected_cached: u64 = deltas.iter().map(|d| d.1).sum();
        let expected_output: u64 = deltas.iter().map(|d| d.2 + d.3).sum();
        assert_eq!(usage.counters.input_tokens, Some(expected_input));
        assert_eq!(usage.counters.cache_read_tokens, Some(expected_cached));
        assert_eq!(usage.counters.output_tokens, Some(expected_output));
        // Context consumed equals what Codex reports as its cumulative input.
        assert_eq!(
            usage.counters.input_tokens.unwrap() + usage.counters.cache_read_tokens.unwrap(),
            deltas.iter().map(|d| d.0).sum::<u64>()
        );
    }

    #[test]
    fn codex_usage_counter_overflow_is_explicit() {
        let raw = json!({
            "method": "thread/tokenUsage/updated",
            "params": {"tokenUsage": {"last": {
                "outputTokens": u64::MAX,
                "reasoningOutputTokens": 1
            }}}
        });

        let error = extract_token_usage(&raw).expect_err("overflow must not be hidden");
        assert!(error.to_string().contains("output token counter overflow"));
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
