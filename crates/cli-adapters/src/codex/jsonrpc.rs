use super::protocol::{
    ClientNotification, ClientRequest, ClientResponse, RequestId, RpcErrorObject,
    ServerNotificationMessage, ServerRequestMessage,
};
use executors::ExecutorError;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub enum ServerMessage {
    Request(ServerRequestMessage, Value),
    Notification(ServerNotificationMessage, Value),
    Response(Value),
    RawLine(String),
}

type PendingResult = Result<Value, RpcErrorObject>;
type PendingSender = oneshot::Sender<PendingResult>;

#[derive(Clone)]
pub struct JsonRpcPeer {
    stdin: Arc<Mutex<ChildStdin>>,
    pending: Arc<Mutex<HashMap<RequestId, PendingSender>>>,
    next_id: Arc<AtomicU64>,
    cancel: CancellationToken,
}

impl JsonRpcPeer {
    pub fn spawn(
        stdin: ChildStdin,
        stdout: ChildStdout,
        cancel: CancellationToken,
    ) -> (Self, mpsc::Receiver<ServerMessage>) {
        let (message_tx, message_rx) = mpsc::channel(256);
        let peer = Self {
            stdin: Arc::new(Mutex::new(stdin)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            cancel,
        };

        let pending = peer.pending.clone();
        let reader_cancel = peer.cancel.clone();
        tokio::spawn(async move {
            read_stdout(stdout, pending, message_tx, reader_cancel).await;
        });

        (peer, message_rx)
    }

    pub async fn request<P, R>(&self, method: &'static str, params: P) -> Result<R, ExecutorError>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let id = RequestId::from(self.next_id.fetch_add(1, Ordering::Relaxed));
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id.clone(), tx);

        let request = ClientRequest::new(id.clone(), method, params);
        if let Err(error) = self.write_json(&request).await {
            self.pending.lock().await.remove(&id);
            return Err(error);
        }

        let response = tokio::select! {
            _ = self.cancel.cancelled() => {
                self.pending.lock().await.remove(&id);
                return Err(ExecutorError::Other(format!("{method} request cancelled")));
            }
            response = rx => response,
        };

        match response {
            Ok(Ok(value)) => serde_json::from_value(value).map_err(|error| {
                ExecutorError::Other(format!("failed to decode {method} response: {error}"))
            }),
            Ok(Err(error)) => {
                let mut capacity = crate::capacity::CapacitySignal::default();
                capacity.observe(
                    &serde_json::json!({
                        "code": error.code,
                        "message": error.message,
                        "data": error.data,
                    })
                    .to_string(),
                );
                if let Some(retry_after) = capacity.retry_after {
                    return Err(ExecutorError::UsageExhausted {
                        retry_after,
                        usage_reports: Vec::new(),
                    });
                }
                Err(ExecutorError::Other(format!(
                    "{method} failed: {} ({})",
                    error.message, error.code
                )))
            }
            Err(_) => Err(ExecutorError::Other(format!(
                "{method} response channel closed"
            ))),
        }
    }

    pub async fn notify<P>(
        &self,
        method: &'static str,
        params: Option<P>,
    ) -> Result<(), ExecutorError>
    where
        P: Serialize,
    {
        self.write_json(&ClientNotification::new(method, params))
            .await
    }

    pub async fn respond<R>(&self, id: RequestId, result: R) -> Result<(), ExecutorError>
    where
        R: Serialize,
    {
        self.write_json(&ClientResponse::new(id, result)).await
    }

    async fn write_json<T>(&self, message: &T) -> Result<(), ExecutorError>
    where
        T: Serialize,
    {
        let raw = serde_json::to_vec(message)
            .map_err(|error| ExecutorError::Other(format!("failed to encode JSON-RPC: {error}")))?;
        let mut stdin = self.stdin.lock().await;
        stdin.write_all(&raw).await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        Ok(())
    }
}

async fn read_stdout(
    stdout: ChildStdout,
    pending: Arc<Mutex<HashMap<RequestId, PendingSender>>>,
    message_tx: mpsc::Sender<ServerMessage>,
    cancel: CancellationToken,
) {
    let mut lines = BufReader::new(stdout).lines();
    loop {
        let line = tokio::select! {
            _ = cancel.cancelled() => break,
            line = lines.next_line() => line,
        };

        let line = match line {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                let _ = message_tx
                    .send(ServerMessage::RawLine(format!(
                        "failed to read codex stdout: {error}"
                    )))
                    .await;
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let raw = match serde_json::from_str::<Value>(&line) {
            Ok(raw) => raw,
            Err(_) => {
                if message_tx.send(ServerMessage::RawLine(line)).await.is_err() {
                    break;
                }
                continue;
            }
        };

        if let Some(id) = raw
            .get("id")
            .cloned()
            .and_then(|value| serde_json::from_value::<RequestId>(value).ok())
        {
            if let Some(result) = raw.get("result").cloned() {
                if let Some(sender) = pending.lock().await.remove(&id) {
                    let _ = sender.send(Ok(result));
                }
                if message_tx.send(ServerMessage::Response(raw)).await.is_err() {
                    break;
                }
                continue;
            }

            if let Some(error) = raw
                .get("error")
                .cloned()
                .and_then(|value| serde_json::from_value::<RpcErrorObject>(value).ok())
            {
                if let Some(sender) = pending.lock().await.remove(&id) {
                    let _ = sender.send(Err(error));
                }
                if message_tx.send(ServerMessage::Response(raw)).await.is_err() {
                    break;
                }
                continue;
            }
        }

        if raw.get("id").is_some() && raw.get("method").is_some() {
            match serde_json::from_value::<ServerRequestMessage>(raw.clone()) {
                Ok(request) => {
                    if message_tx
                        .send(ServerMessage::Request(request, raw))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => {
                    if message_tx.send(ServerMessage::Response(raw)).await.is_err() {
                        break;
                    }
                }
            }
            continue;
        }

        if raw.get("method").is_some() {
            match serde_json::from_value::<ServerNotificationMessage>(raw.clone()) {
                Ok(notification) => {
                    if message_tx
                        .send(ServerMessage::Notification(notification, raw))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(_) => {
                    if message_tx.send(ServerMessage::Response(raw)).await.is_err() {
                        break;
                    }
                }
            }
            continue;
        }

        if message_tx.send(ServerMessage::Response(raw)).await.is_err() {
            break;
        }
    }

    let mut guard = pending.lock().await;
    for (_, sender) in guard.drain() {
        let _ = sender.send(Err(RpcErrorObject {
            code: -32000,
            message: "codex app-server stdout closed".to_owned(),
            data: None,
        }));
    }
}
