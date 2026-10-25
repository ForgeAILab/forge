//! Provider capability, OAuth callback, and authorization state tests.

use super::*;
use db::{User, UserRepo};

async fn test_service() -> (ProviderAuthorizationService, String) {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let owner = new_uuid_v4();
    let now = now_rfc3339();
    UserRepo::create_user(
        &*db,
        &User {
            id: owner.clone(),
            email: "owner@example.com".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Owner".to_owned()),
            is_admin: true,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("owner creates");
    let embedded = Arc::new(EmbeddedAgentService::new(
        Arc::clone(&db),
        b"provider-auth-test-key",
    ));
    (
        ProviderAuthorizationService::new(db, embedded, vec!["http://localhost:5173".to_owned()]),
        owner,
    )
}

/// Client-owned loopback: the caller already holds the socket, so these
/// tests never bind a real port.
fn browser_request() -> StartProviderAuthorizationRequest {
    StartProviderAuthorizationRequest {
        provider: AgentProviderId::OpenAi,
        method: ProviderCredentialMethod::BrowserOauth,
        redirect_origin: "http://localhost:5173".to_owned(),
        credential_label: "Test login".to_owned(),
        loopback_owner: LoopbackOwner::Client,
        loopback_port: Some(LOOPBACK_CALLBACK_PORTS[0]),
    }
}

fn callback_state(operation: &ProviderAuthorizationOperation) -> String {
    Url::parse(
        operation
            .authorization_url
            .as_deref()
            .expect("browser URL exists"),
    )
    .expect("browser URL parses")
    .query_pairs()
    .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
    .expect("callback state exists")
}

#[tokio::test]
async fn registry_exposes_reviewed_provider_boundaries() {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let embedded = Arc::new(EmbeddedAgentService::new(
        Arc::clone(&db),
        b"provider-auth-test-key",
    ));
    let service =
        ProviderAuthorizationService::new(db, embedded, vec!["http://localhost:5173".to_owned()]);
    let registry = service.capabilities();
    let openai = registry
        .items
        .iter()
        .find(|item| item.provider == AgentProviderId::OpenAi)
        .expect("OpenAI capability exists");
    assert!(openai.credential_methods.iter().any(|method| {
        method.method == ProviderCredentialMethod::BrowserOauth
            && method.action_label == "Continue with ChatGPT"
            && method.support_level == ProviderSupportLevel::Experimental
    }));
    let xai = registry
        .items
        .iter()
        .find(|item| item.provider == AgentProviderId::XAi)
        .expect("xAI capability exists");
    assert!(xai.credential_methods.iter().any(|method| {
        method.method == ProviderCredentialMethod::DeviceOauth
            && method.support_level == ProviderSupportLevel::Experimental
    }));
    let gemini = registry
        .items
        .iter()
        .find(|item| item.provider == AgentProviderId::Gemini)
        .expect("Gemini capability exists");
    assert_eq!(
        gemini.default_model.as_deref(),
        Some("gemini-3.1-pro-preview")
    );
    assert_eq!(
        service
            .validate_redirect_origin("http://localhost:5173", true, None)
            .await
            .expect("configured origin is accepted"),
        "http://localhost:5173"
    );
    assert!(service
        .validate_redirect_origin("https://attacker.example", true, None)
        .await
        .is_err());
    // The origin the request itself arrived from is trusted even when the
    // operator never configured it — the bounce lands where the browser
    // already is.
    assert_eq!(
        service
            .validate_redirect_origin("http://10.0.0.2:8080", true, Some("http://10.0.0.2:8080"))
            .await
            .expect("the request's own origin is accepted"),
        "http://10.0.0.2:8080"
    );
    assert!(service
        .validate_redirect_origin(
            "https://attacker.example",
            true,
            Some("http://10.0.0.2:8080")
        )
        .await
        .is_err());
    // Device flows record the origin without trusting it: nothing ever
    // redirects a browser there.
    assert_eq!(
        service
            .validate_redirect_origin("https://forge.example.com", false, None)
            .await
            .expect("device flows accept any well-formed origin"),
        "https://forge.example.com"
    );
    assert!(service
        .validate_redirect_origin("not-a-url", false, None)
        .await
        .is_err());
}

#[test]
fn browser_oauth_uses_pkce_and_keeps_verifier_protected() {
    let (public_url, protected) = browser_authorization(
        AgentProviderId::OpenAi,
        &loopback_redirect_uri(LOOPBACK_CALLBACK_PORTS[0]),
        "public-state",
        "private-verifier",
        "public-challenge",
    )
    .expect("authorization builds");
    assert!(public_url.contains("code_challenge=public-challenge"));
    assert!(public_url.contains("state=public-state"));
    assert!(!public_url.contains("private-verifier"));
    assert_eq!(protected.code_verifier.as_deref(), Some("private-verifier"));
    // ChatGPT reads plan and account from the id token's org claims, and
    // must not receive Google's offline-access params.
    assert!(public_url.contains("id_token_add_organizations=true"));
    assert!(public_url.contains("codex_cli_simplified_flow=true"));
    assert!(!public_url.contains("access_type"));
    assert!(!public_url.contains("prompt=consent"));
    assert!(public_url.contains("redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback"));
}

#[test]
fn only_openai_uses_the_loopback_callback() {
    assert!(provider_uses_loopback_callback(AgentProviderId::OpenAi));
    // Gemini's client is operator-registered against Forge's own route.
    assert!(!provider_uses_loopback_callback(AgentProviderId::Gemini));
    assert!(is_loopback_origin("http://localhost:5173"));
    assert!(is_loopback_origin("http://127.0.0.1:8080"));
    assert!(!is_loopback_origin("https://forge.example.com"));
}

#[tokio::test]
async fn client_owned_loopback_requires_a_whitelisted_port() {
    let (service, owner) = test_service().await;
    let mut request = browser_request();
    request.loopback_port = None;
    assert!(service.start(owner.clone(), request, None).await.is_err());

    let mut request = browser_request();
    request.loopback_port = Some(9999);
    assert!(service.start(owner.clone(), request, None).await.is_err());
}

#[tokio::test]
async fn server_owned_loopback_needs_a_local_browser() {
    let (service, owner) = test_service().await;
    let mut request = browser_request();
    request.loopback_owner = LoopbackOwner::Server;
    request.loopback_port = None;
    request.redirect_origin = "https://forge.example.com".to_owned();
    service.set_trusted_origins(vec!["https://forge.example.com".to_owned()]);
    // Trusted, but the browser is not on this machine, so nothing could
    // ever answer the localhost callback.
    let error = service
        .start(owner, request, None)
        .await
        .expect_err("remote browser cannot own a localhost callback");
    assert!(error.to_string().contains("same machine"));
}

/// End to end over a real socket: Forge binds the callback, the "browser"
/// hits it, and the operation lands in a terminal state.
#[tokio::test]
async fn server_owned_loopback_serves_the_browser_callback() {
    let (service, owner) = test_service().await;
    let mut request = browser_request();
    request.loopback_owner = LoopbackOwner::Server;
    request.loopback_port = None;
    let operation = service
        .start(owner.clone(), request, None)
        .await
        .expect("browser authorization starts");
    let authorization_url =
        Url::parse(operation.authorization_url.as_deref().expect("browser URL"))
            .expect("browser URL parses");
    let redirect_uri = authorization_url
        .query_pairs()
        .find_map(|(key, value)| (key == "redirect_uri").then(|| value.into_owned()))
        .expect("redirect_uri is present");
    let port = Url::parse(&redirect_uri)
        .expect("redirect_uri parses")
        .port()
        .expect("redirect_uri carries a port");
    assert!(LOOPBACK_CALLBACK_PORTS.contains(&port));
    let state = callback_state(&operation);

    // A denial keeps the exchange local: no provider token endpoint call.
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port))
        .await
        .expect("callback listener is bound");
    let request_line = format!(
        "GET /auth/callback?state={state}&error=access_denied HTTP/1.1\r\n\
         Host: localhost\r\nConnection: close\r\n\r\n"
    );
    stream
        .write_all(request_line.as_bytes())
        .await
        .expect("callback request sends");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("callback responds");
    assert!(response.starts_with("HTTP/1.1 303 See Other"));
    assert!(response.contains("Location: http://localhost:5173/agents?provider=openai"));
    assert_eq!(
        service
            .get(&operation.id, &owner)
            .await
            .expect("owner can read operation")
            .status,
        "denied"
    );
}

#[tokio::test]
async fn browser_denial_is_terminal_redacted_and_replay_safe() {
    let (service, owner) = test_service().await;
    let operation = service
        .start(owner.clone(), browser_request(), None)
        .await
        .expect("browser authorization starts");
    let state = callback_state(&operation);
    assert!(service
        .callback("openai", "mismatched-state", Some("code"), None)
        .await
        .is_err());
    assert_eq!(
        service
            .get(&operation.id, &owner)
            .await
            .expect("state mismatch leaves operation pending")
            .status,
        "awaiting_browser"
    );
    let denied = service
        .callback("openai", &state, None, Some("access_denied: secret detail"))
        .await
        .expect("denial is recorded");
    assert_eq!(denied.status, "denied");
    assert_eq!(denied.error_code.as_deref(), Some("provider_denied"));
    assert_eq!(
        denied.error_message.as_deref(),
        Some("Provider authorization was denied")
    );
    assert!(!denied.error_message.unwrap().contains("secret"));
    assert!(service
        .callback("openai", &state, Some("replayed-code"), None)
        .await
        .is_err());
    assert_eq!(
        service
            .get(&operation.id, &owner)
            .await
            .expect("owner can read operation")
            .status,
        "denied"
    );
}

#[tokio::test]
async fn expired_callback_never_reaches_token_exchange() {
    let (service, owner) = test_service().await;
    let operation = service
        .start(owner.clone(), browser_request(), None)
        .await
        .expect("browser authorization starts");
    let state = callback_state(&operation);
    sqlx::query("UPDATE provider_authorization_operation SET expires_at = ? WHERE id = ?")
        .bind("2000-01-01T00:00:00Z")
        .bind(&operation.id)
        .execute(service.db.pool())
        .await
        .expect("operation expires");

    let expired = service
        .callback("openai", &state, Some("must-not-be-exchanged"), None)
        .await
        .expect("expiry is recorded");
    assert_eq!(expired.status, "expired");
    assert_eq!(expired.error_code.as_deref(), Some("expired"));
}

#[tokio::test]
async fn failed_finalization_is_terminal_and_erases_transient_state() {
    let (service, owner) = test_service().await;
    let operation = service
        .start(owner.clone(), browser_request(), None)
        .await
        .expect("browser authorization starts");
    let exchanging = service
        .advance(&operation, "exchanging")
        .await
        .expect("operation advances");

    let failed = service
        .finalize_or_fail(
            exchanging,
            OAuthTokenResponse {
                access_token: "access-secret".to_owned(),
                refresh_token: None,
                expires_in: Some(3600),
                scope: None,
                id_token: None,
            },
        )
        .await
        .expect("failure is recorded as an operation result");

    assert_eq!(failed.status, "failed");
    assert_eq!(failed.error_code.as_deref(), Some("publication_failed"));
    assert_eq!(
        failed.error_message.as_deref(),
        Some("Provider authorization failed")
    );
    assert!(!failed.error_message.unwrap().contains("access-secret"));
    assert!(service
        .protected_store
        .open_provider_authorization_state(&operation.id)
        .await
        .is_err());
}

#[tokio::test]
async fn malformed_callback_is_terminal_and_redacted() {
    let (service, owner) = test_service().await;
    let operation = service
        .start(owner, browser_request(), None)
        .await
        .expect("browser authorization starts");
    let state = callback_state(&operation);

    let failed = service
        .callback("openai", &state, None, None)
        .await
        .expect("malformed callback is recorded");

    assert_eq!(failed.status, "failed");
    assert_eq!(failed.error_code.as_deref(), Some("malformed_callback"));
    assert_eq!(
        failed.error_message.as_deref(),
        Some("Provider authorization failed")
    );
}

#[tokio::test]
async fn authorization_start_is_account_rate_limited() {
    let (service, owner) = test_service().await;
    for _ in 0..5 {
        service
            .start(owner.clone(), browser_request(), None)
            .await
            .expect("start remains inside account budget");
    }
    assert!(matches!(
        service.start(owner, browser_request(), None).await,
        Err(ServiceError::RateLimited {
            retry_after_seconds: 60
        })
    ));
}

#[test]
fn device_errors_and_discovery_boundaries_are_deterministic() {
    assert!(matches!(
        device_poll_from_error(Some("authorization_pending")),
        DevicePoll::Pending
    ));
    assert!(matches!(
        device_poll_from_error(Some("slow_down")),
        DevicePoll::SlowDown
    ));
    assert!(matches!(
        device_poll_from_error(Some("access_denied")),
        DevicePoll::Denied
    ));
    assert!(trusted_oidc_endpoint("https://auth.x.ai/oauth/token", XAI_ISSUER).is_ok());
    assert!(trusted_oidc_endpoint("http://auth.x.ai/oauth/token", XAI_ISSUER).is_err());
    assert!(trusted_oidc_endpoint("https://attacker.example/token", XAI_ISSUER).is_err());
    assert_eq!(
        bounded_error("upstream outage includes bearer secret"),
        "Provider authorization failed"
    );
}
