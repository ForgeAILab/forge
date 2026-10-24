use super::*;
use agent_runtime::{
    core::{
        provider::{Provider, ProviderError},
        provider_credential::ProviderCredentialTarget,
        store::Secret,
    },
    provider::{
        openai::{OpenAiConfig, OpenAiProvider},
        transport::{ByteStream, HttpRequest, HttpTransport},
    },
};
use async_trait::async_trait;
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use forge_agent_host::{CreateOAuthCredential, OAuthCredentialBundle};
use std::{
    collections::VecDeque,
    sync::{Mutex, atomic::Ordering},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const API: &str = "sk-J3aR8vT2pN7xQ4mS9dH6bF1cL5wK0zY+api/=";
const ACCESS: &str = "oa-U4bS9wG2rM8yD5kV1eH7aC3nP6xL0tZ+old/=";
const REFRESH: &str = "or-N9cT4xH6sB2zE8lW3fJ1dD7qR5yM0uA+old/=";
const NEW_ACCESS: &str = "na-P6dV1yJ8tC3aF9mX4gK2eE7rS5zN0wB+new/=";
const NEW_REFRESH: &str = "nr-Q7eW2zK9uD4bG1nY5hL3fF8sT6aP0xC+new/=";
const CLIENT: &str = "cs-R8fX3aL1vE5cH2pZ6jM4gG9tU7bQ0yD+sec/=";
const SECRETS: [&str; 6] = [API, ACCESS, REFRESH, NEW_ACCESS, NEW_REFRESH, CLIENT];

#[derive(Clone, Debug)]
struct Replay {
    replies: Arc<Mutex<VecDeque<Vec<u8>>>>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}
#[async_trait]
impl HttpTransport for Replay {
    async fn post_stream(&self, request: HttpRequest) -> Result<ByteStream, ProviderError> {
        self.requests.lock().unwrap().push(request);
        let bytes = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted provider call");
        Ok(Box::pin(futures_util::stream::iter([Ok(bytes)])))
    }
}
fn replay() -> Replay {
    let tool = serde_json::json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"credential-tool","type":"function","function":{"name":"echo","arguments":"{\"text\":\"durable tool result\"}"}}]},"finish_reason":"tool_calls"}]});
    let reply = serde_json::json!({"choices":[{"index":0,"delta":{"content":"complete"},"finish_reason":"stop"}]});
    Replay {
        replies: Arc::new(Mutex::new(
            [tool, reply]
                .map(|v| format!("data: {v}\n\ndata: [DONE]\n\n").into_bytes())
                .into(),
        )),
        requests: Arc::new(Mutex::new(vec![])),
    }
}
// Same folding and 16-character-window rule as Forge's provider-reason echo
// check, extended here to encoded forms too. Canary checks below prevent vacuity.
fn fold(text: &str) -> String {
    text.chars().filter(|c| !c.is_whitespace() && !c.is_control() && !matches!(*c,
        '\u{00AD}'|'\u{034F}'|'\u{061C}'|'\u{115F}'|'\u{1160}'|'\u{17B4}'|'\u{17B5}'| '\u{180B}'..='\u{180F}'|'\u{200B}'..='\u{200F}'|'\u{202A}'..='\u{202E}'|'\u{2060}'..='\u{206F}'|'\u{3164}'|'\u{FE00}'..='\u{FE0F}'|'\u{FEFF}'|'\u{FFA0}'|'\u{1BCA0}'..='\u{1BCA3}'|'\u{1D173}'..='\u{1D17A}'|'\u{E0000}'..='\u{E0FFF}'
    )).map(|c|c.to_ascii_lowercase()).collect()
}
fn forms(secret: &str) -> Vec<String> {
    vec![
        secret.into(),
        url::form_urlencoded::byte_serialize(secret.as_bytes()).collect(),
        STANDARD.encode(secret),
        URL_SAFE_NO_PAD.encode(secret),
    ]
}
fn reflects(raw: &[u8]) -> bool {
    let text = String::from_utf8_lossy(raw);
    let folded = fold(&text);
    SECRETS.iter().flat_map(|s| forms(s)).any(|s| {
        text.contains(&s)
            || fold(&s)
                .as_bytes()
                .windows(16)
                .any(|run| folded.as_bytes().windows(run.len()).any(|v| v == run))
    })
}
// Decode storage serialization only: JSON string escaping and byte vectors
// must not hide a credential in a session/pin/catalog's opaque engine bytes.
// No runtime-private engine format or field semantics are interpreted.
fn contains_credential(raw: &[u8]) -> bool {
    fn encoded(raw: &[u8], depth: usize) -> bool {
        assert!(
            depth < 64,
            "credential audit serialization nesting exceeds its bound"
        );
        if reflects(raw) {
            return true;
        }
        serde_json::from_slice::<serde_json::Value>(raw).is_ok_and(|value| visit(&value, depth + 1))
    }
    fn visit(value: &serde_json::Value, depth: usize) -> bool {
        match value {
            serde_json::Value::String(text) => encoded(text.as_bytes(), depth),
            serde_json::Value::Array(values) => {
                let bytes: Option<Vec<u8>> = values
                    .iter()
                    .map(|v| v.as_u64().and_then(|n| u8::try_from(n).ok()))
                    .collect();
                match bytes {
                    Some(bytes) if !bytes.is_empty() => encoded(&bytes, depth),
                    _ => values.iter().any(|v| visit(v, depth)),
                }
            }
            serde_json::Value::Object(values) => values
                .iter()
                .any(|(key, value)| reflects(key.as_bytes()) || visit(value, depth)),
            _ => false,
        }
    }
    encoded(raw, 0)
}

async fn audit(store: &SqliteProtectedRuntimeStore) {
    let units = store
        .journals()
        .storage()
        .decrypted_units_for_test()
        .await
        .unwrap();
    for role in [JournalRole::Ordinary, JournalRole::Protected] {
        for kind in [
            "index",
            "body",
            "session",
            "operation_pack",
            "pin",
            "catalog",
            "object",
        ] {
            assert!(
                units.iter().any(|(r, k, _)| *r == role && *k == kind),
                "missing audited unit {role:?}/{kind}"
            );
        }
    }
    for (role, kind, raw) in &units {
        assert!(
            !contains_credential(raw),
            "credential in decrypted {role:?}/{kind}"
        );
    }
}

pub async fn run() {
    for secret in SECRETS {
        for shape in forms(secret) {
            assert!(contains_credential(shape.as_bytes()));
            let spaced = shape
                .to_ascii_uppercase()
                .chars()
                .map(|c| format!("{c}\u{200B} \n"))
                .collect::<String>();
            assert!(contains_credential(spaced.as_bytes()));
            for value in [
                serde_json::json!({"text":spaced}),
                serde_json::json!({"opaque":shape.as_bytes()}),
            ] {
                let serialized = serde_json::to_vec(&value).unwrap();
                assert!(contains_credential(&serialized));
                assert!(contains_credential(
                    &serde_json::to_vec(&serialized).unwrap()
                ));
            }
            for part in shape.as_bytes().windows(16) {
                assert!(contains_credential(part));
            }
        }
    }
    assert!(!contains_credential(
        b"ordinary conversation and durable tool result"
    ));
    let f = FileDb::new().await.unwrap();
    let store = Arc::new(SqliteProtectedRuntimeStore::new(f.database(), [7; 32], 1));
    let now = db::now_rfc3339();
    db::UserRepo::create_user(
        &*f.database(),
        &db::User {
            id: "journal-owner".into(),
            email: "journal@example.test".into(),
            password_hash: "hash".into(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    store
        .create_credential(
            "journal-api",
            "journal-owner",
            "openai",
            "API",
            Secret::new(API),
            &now,
        )
        .await
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/token", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut buf = [0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            request.extend_from_slice(&buf[..n]);
            if let Some(end) = request.windows(4).position(|s| s == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&request[..end]).to_ascii_lowercase();
                let len: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length: "))
                    .unwrap()
                    .parse()
                    .unwrap();
                if request.len() >= end + 4 + len {
                    break;
                }
            }
        }
        let raw = String::from_utf8(request).unwrap();
        let encoded: String = url::form_urlencoded::byte_serialize(REFRESH.as_bytes()).collect();
        assert!(raw.contains(&encoded));
        let encoded: String = url::form_urlencoded::byte_serialize(CLIENT.as_bytes()).collect();
        assert!(raw.contains(&encoded));
        let body=serde_json::json!({"access_token":NEW_ACCESS,"refresh_token":NEW_REFRESH,"expires_in":3600}).to_string();
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    });
    store
        .create_oauth_credential(CreateOAuthCredential {
            id: "journal-oauth",
            owner_user_id: "journal-owner",
            provider: "openai",
            label: "OAuth",
            bundle: &OAuthCredentialBundle {
                schema_version: 1,
                access_token: ACCESS.into(),
                refresh_token: REFRESH.into(),
                expires_at_ms: 0,
                token_endpoint: endpoint,
                client_id: "public-client".into(),
                client_secret: Some(CLIENT.into()),
                scopes: vec![],
                provider_account_id: None,
            },
            metadata_json: "{}",
            now: &now,
        })
        .await
        .unwrap();
    let mut checkpoints = vec![];
    for (handle, expected) in [("journal-api", API), ("journal-oauth", NEW_ACCESS)] {
        let id = SessionId::new(handle);
        f.ensure_session(&id).await.unwrap();
        let transport = replay();
        let provider: Arc<dyn Provider> = Arc::new(
            OpenAiProvider::with_credential_source(
                transport.clone(),
                OpenAiConfig::new("https://provider.invalid/v1", "fake"),
                ProviderCredentialTarget::new(handle).unwrap(),
                store.credential_source("journal-owner", handle),
            )
            .unwrap(),
        );
        let io = Arc::new(import_matrix::IoCounts::default());
        let rt = import_matrix::configure(
            RuntimeBuilder::new(ModelId::new("fake"))
                .provider_name("openai")
                .provider(provider)
                .model_profile(ResolvedModelProfile::explicit(
                    "openai",
                    ModelId::new("fake"),
                    ModelLimits::new(32_000, 28_000, 4_000),
                ))
                .session_store(store.clone())
                .checkpoint_store(store.clone()),
            import_matrix::Profile::Tools,
            io.clone(),
        )
        .build()
        .unwrap();
        let session = rt
            .start_session(StartSession::create(id.clone(), vec![]))
            .await
            .unwrap();
        session
            .run(UserInput::text("use echo and finish"))
            .await
            .unwrap();
        assert_eq!(io.tools.load(Ordering::SeqCst), 1);
        assert_eq!(transport.requests.lock().unwrap().len(), 2);
        for request in transport.requests.lock().unwrap().iter() {
            assert!(
                request
                    .headers
                    .iter()
                    .any(|(k, v)| k.eq_ignore_ascii_case("authorization")
                        && v == &format!("Bearer {expected}"))
            );
            assert!(!contains_credential(&request.body));
        }
        session.shutdown().await.unwrap();
        drop(session);
        drop(rt);
        for engine in [store.journals().protected(), store.journals().ordinary()] {
            let lease = engine
                .open(
                    &id,
                    open("credential-retained", JournalOpenIntent::ReadOnly),
                )
                .await
                .unwrap();
            engine.retain_root(&lease).await.unwrap();
            engine.close(lease).await.unwrap();
        }
        let engine = store.journals().protected();
        let lease = engine
            .open(&id, open("credential-exact", JournalOpenIntent::ReadOnly))
            .await
            .unwrap();
        let exact = JournalReader::new(&engine, &lease, limits())
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .unwrap();
        assert!(
            exact
                .snapshot
                .history
                .iter()
                .any(|m| format!("{m:?}").contains("durable tool result"))
        );
        engine.close(lease).await.unwrap();
        checkpoints.push((id, exact));
    }
    server.await.unwrap();
    let version: i64 =
        sqlx::query_scalar("SELECT version FROM credential_handle WHERE id='journal-oauth'")
            .fetch_one(f.database().pool())
            .await
            .unwrap();
    assert_eq!(version, 2);
    // Audit every physical unit before restart, independently of recovery checks.
    audit(&store).await;
    drop(store);
    let cold = SqliteProtectedRuntimeStore::new(f.reopen().await.unwrap(), [7; 32], 1);
    audit(&cold).await;
    // Exact recovery is a separate assertion; ordinary redaction remains covered
    // independently by native_delta_and_projection.
    for (id, expected) in checkpoints {
        let engine = cold.journals().protected();
        let lease = engine
            .open(&id, open("cold-exact", JournalOpenIntent::ReadOnly))
            .await
            .unwrap();
        let actual = JournalReader::new(&engine, &lease, limits())
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
        engine.close(lease).await.unwrap();
    }
}
