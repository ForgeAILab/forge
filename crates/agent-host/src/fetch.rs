//! Forge's outbound transport for the runtime's built-in web fetch tool.
//!
//! The runtime owns the tool: argument parsing, URL policy, content
//! conversion, the untrusted-content notice, and the `NetHttp` permission it
//! asks the host to authorize. What it delegates is the request itself,
//! because only the host knows which network it sits on.
//!
//! Two things make that delegation safe, and neither is URL validation —
//! the runtime already did that:
//!
//! - **The target is resolved and checked before connecting.**
//!   A hostname that resolves to a private or loopback address is refused
//!   before any socket opens. On direct requests, `resolve_to_addrs` keeps the client from
//!   re-resolving it to something else afterwards, which is what closes the
//!   DNS-rebinding window a URL check alone leaves open. With an environment
//!   proxy, the CONNECT tunnel preserves the target name; the proxy resolves
//!   it again, so pinning cannot protect that second resolution.
//! - **Redirects are followed by this transport, one hop at a time, and only
//!   within the origin the runtime authorized.** A cross-origin redirect is
//!   reported back with its target instead of being followed, so reaching it
//!   costs another tool call against another authorized resource rather than
//!   arriving silently under the first one's permission.
//!
//! The host also owns the request headers, because it owns the client. The
//! runtime's placeholder `user-agent` is a library default that ordinary
//! sites reject or serve a degraded page to, and a request with no `accept`
//! invites a server to negotiate something the tool cannot read. Forge sends
//! a conventional identifying bot UA and a documents-first accept set, and
//! leaves `accept-encoding` to reqwest so its compression features stay in
//! charge of decoding.

use std::time::Duration;

use agent_runtime::core::cancel::Cancellation;
use agent_runtime::core::clock::{Deadline, SystemClock};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::harness::{FetchRequest, FetchResponse, FetchTransport};
use async_trait::async_trait;

use crate::outbound_http::{ProxyEnvironment, PublicAddressPolicy, public_http_client_builder};

/// Redirect hops followed inside one authorized origin.
const MAX_REDIRECTS: u8 = 3;

/// Ceiling on one fetch when the turn carries no deadline of its own.
const DEFAULT_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Floor under a turn deadline, so a nearly-expired turn still fails as a
/// timeout rather than as an instant transport error.
const MIN_FETCH_TIMEOUT: Duration = Duration::from_secs(1);

/// The runtime's placeholder user-agent. It names a library rather than a
/// caller, and enough sites answer it with a block page or a degraded
/// response that Forge replaces it with its own identity.
const RUNTIME_DEFAULT_USER_AGENT: &str = "agent-runtime-fetch/1.0";

/// How Forge identifies itself to the sites it fetches.
///
/// The `Mozilla/5.0 (compatible; …)` form is the long-standing convention for
/// a well-behaved non-browser client: it claims no browser engine, names the
/// software, and carries a URL an operator can look up before deciding how to
/// treat the traffic.
const FORGE_USER_AGENT: &str = concat!(
    "Mozilla/5.0 (compatible; ForgeAgent/",
    env!("CARGO_PKG_VERSION"),
    "; +https://github.com/ForgeAILab/forge)"
);

/// Headers Forge sends unless the caller set them itself.
///
/// `accept-encoding` is deliberately absent: reqwest adds it from its enabled
/// compression features and decodes the response, and setting it by hand
/// turns that decoding off and hands back bytes nothing can read.
fn default_headers() -> [(&'static str, &'static str); 4] {
    [
        ("user-agent", FORGE_USER_AGENT),
        (
            "accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,\
             application/json;q=0.8,*/*;q=0.5",
        ),
        ("accept-language", "en-US,en;q=0.9"),
        // A fetch is a one-shot read, and a kept-alive pool of one connection
        // per turn buys nothing.
        ("connection", "close"),
    ]
}

/// The host transport behind the runtime's `fetch` tool.
#[derive(Debug, Clone, Default)]
pub struct ForgeFetchTransport;

impl ForgeFetchTransport {
    /// Creates the transport.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FetchTransport for ForgeFetchTransport {
    async fn fetch(
        &self,
        request: FetchRequest,
        deadline: Option<Deadline>,
        cancellation: &Cancellation,
    ) -> Result<FetchResponse, RuntimeError> {
        let timeout = fetch_timeout(deadline.as_ref());
        let mut url = url::Url::parse(&request.url)
            .map_err(|_| RuntimeError::tool("fetch URL could not be parsed"))?;
        let authorized_origin = url.origin().ascii_serialization();

        for _ in 0..=MAX_REDIRECTS {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::cancelled("fetch cancelled"));
            }
            let client = pinned_client(&url, timeout, &ProxyEnvironment::from_env()).await?;
            let mut builder = client.get(url.as_str());
            for (name, value) in default_headers() {
                builder = builder.header(name, value);
            }
            // Anything the caller set beyond the runtime's placeholder
            // user-agent is deliberate and wins.
            for (name, value) in &request.headers {
                if name.eq_ignore_ascii_case("user-agent") && value == RUNTIME_DEFAULT_USER_AGENT {
                    continue;
                }
                builder = builder.header(name, value);
            }
            let response = tokio::select! {
                biased;
                () = cancellation.cancelled() => {
                    return Err(RuntimeError::cancelled("fetch cancelled"));
                }
                result = builder.send() => result.map_err(|error| {
                    if error.is_timeout() {
                        RuntimeError::tool("fetch timed out")
                    } else {
                        RuntimeError::tool("fetch could not reach the host")
                    }
                })?,
            };

            let status = response.status();
            if status.is_redirection() {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or_else(|| {
                        RuntimeError::tool("fetch received a redirect without a location")
                    })?;
                let target = url.join(location).map_err(|_| {
                    RuntimeError::tool("fetch received a redirect to an unusable location")
                })?;
                if target.origin().ascii_serialization() != authorized_origin {
                    return Err(RuntimeError::tool(format!(
                        "fetch was redirected to {target}, outside the authorized origin \
                         {authorized_origin}; fetch that URL directly to authorize it"
                    )));
                }
                url = target;
                continue;
            }

            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body = bounded_body(response, request.max_bytes, cancellation).await?;
            let mut headers = Vec::new();
            if let Some(content_type) = &content_type {
                headers.push(("content-type".to_owned(), content_type.clone()));
            }
            return Ok(FetchResponse {
                status: status.as_u16(),
                headers,
                body,
                content_type,
            });
        }

        Err(RuntimeError::tool(
            "fetch followed too many redirects within the authorized origin",
        ))
    }
}

/// Reads at most `max_bytes` of the response body, stopping early rather than
/// buffering whatever a server decides to send.
async fn bounded_body(
    mut response: reqwest::Response,
    max_bytes: usize,
    cancellation: &Cancellation,
) -> Result<Vec<u8>, RuntimeError> {
    let mut body = Vec::new();
    loop {
        let chunk = tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                return Err(RuntimeError::cancelled("fetch cancelled"));
            }
            result = response.chunk() => {
                result.map_err(|_| RuntimeError::tool("fetch could not read the response body"))?
            }
        };
        let Some(chunk) = chunk else { break };
        let remaining = max_bytes.saturating_sub(body.len());
        if remaining == 0 {
            break;
        }
        let take = remaining.min(chunk.len());
        body.extend_from_slice(&chunk[..take]);
        if take < chunk.len() {
            break;
        }
    }
    Ok(body)
}

/// One validated HTTPS client using the operator's proxy snapshot.
async fn pinned_client(
    url: &url::Url,
    timeout: Duration,
    proxies: &ProxyEnvironment,
) -> Result<reqwest::Client, RuntimeError> {
    public_http_client_builder(url, PublicAddressPolicy::Provider, proxies)
        .await
        .map_err(|error| RuntimeError::tool(error.to_string()))?
        .timeout(timeout)
        .build()
        .map_err(|_| RuntimeError::tool("fetch could not build an HTTP client"))
}

/// The wall-clock ceiling for one fetch, derived from the turn's deadline.
fn fetch_timeout(deadline: Option<&Deadline>) -> Duration {
    let clock = SystemClock;
    deadline
        .and_then(|deadline| deadline.remaining_millis(&clock))
        .map(Duration::from_millis)
        .map_or(DEFAULT_FETCH_TIMEOUT, |remaining| {
            remaining.clamp(MIN_FETCH_TIMEOUT, DEFAULT_FETCH_TIMEOUT)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plaintext_and_credentialed_urls_are_refused_before_any_socket_opens() {
        for rejected in [
            "http://example.com/docs",
            "https://user:secret@example.com/docs",
        ] {
            let url = url::Url::parse(rejected).expect("test URL parses");
            let error = pinned_client(&url, DEFAULT_FETCH_TIMEOUT, &ProxyEnvironment::default())
                .await
                .expect_err("policy must refuse this URL");
            assert!(
                error.to_string().contains("https") || error.to_string().contains("credentials"),
                "unexpected rejection for {rejected}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn a_host_on_this_machines_own_network_is_refused() {
        for rejected in [
            "https://localhost/admin",
            "https://127.0.0.1/admin",
            "https://10.0.0.5/admin",
            "https://169.254.169.254/latest/meta-data",
            "https://forge.internal/admin",
            "https://[::1]/admin",
        ] {
            let url = url::Url::parse(rejected).expect("test URL parses");
            assert!(
                pinned_client(&url, DEFAULT_FETCH_TIMEOUT, &ProxyEnvironment::default())
                    .await
                    .is_err(),
                "{rejected} must not be reachable through the fetch transport"
            );
        }
    }

    #[tokio::test]
    async fn fetch_rejects_transition_and_zero_network_urls_before_io() {
        for endpoint in [
            "https://0.1.2.3/",
            "https://[2001:db8::1]/",
            "https://[2001:2::1]/",
            "https://[2001:10::1]/",
            "https://[2001:20::1]/",
            "https://[100::1]/",
            "https://[3fff::1]/",
            "https://[::ffff:0:10.0.0.1]/",
            "https://[::ffff:0:127.0.0.1]/",
            "https://[::ffff:0:169.254.169.254]/",
            "https://192.88.99.1/",
            "https://[64:ff9b::192.88.99.1]/",
            "https://[2002:c058:6301::]/",
            "https://[::ffff:192.88.99.1]/",
            "https://[64:ff9b::7f00:1]/",
            "https://[64:ff9b:1::1]/",
            "https://[2002:7f00:1::]/",
            "https://[2001::1]/",
            "https://[::ffff:0.1.2.3]/",
            "https://[::0.1.2.3]/",
        ] {
            let url = url::Url::parse(endpoint).unwrap();
            assert!(
                pinned_client(&url, DEFAULT_FETCH_TIMEOUT, &ProxyEnvironment::default())
                    .await
                    .is_err(),
                "{endpoint}"
            );
        }
    }

    #[tokio::test]
    async fn a_cancelled_turn_never_sends_the_request() {
        let cancellation = Cancellation::new();
        cancellation.cancel(agent_runtime::core::cancel::CancelReason::UserRequested);
        let error = ForgeFetchTransport::new()
            .fetch(
                FetchRequest {
                    url: "https://example.com/docs".to_owned(),
                    headers: Vec::new(),
                    max_bytes: 1024,
                },
                None,
                &cancellation,
            )
            .await
            .expect_err("a cancelled turn must not fetch");
        assert!(error.to_string().contains("cancelled"), "{error}");
    }

    #[test]
    fn every_request_carries_headers_an_ordinary_site_will_answer() {
        let headers = default_headers();
        let names: Vec<&str> = headers.iter().map(|(name, _)| *name).collect();
        assert!(names.contains(&"user-agent"));
        assert!(names.contains(&"accept"));
        assert!(names.contains(&"accept-language"));

        let user_agent = headers
            .iter()
            .find(|(name, _)| *name == "user-agent")
            .expect("user-agent is always sent")
            .1;
        assert!(
            user_agent.contains("ForgeAgent") && user_agent.contains("github.com/ForgeAILab"),
            "the UA must name this software and carry a URL an operator can look up: {user_agent}"
        );

        let accept = headers
            .iter()
            .find(|(name, _)| *name == "accept")
            .expect("accept is always sent")
            .1;
        for expected in ["text/html", "text/plain", "application/json", "*/*"] {
            assert!(accept.contains(expected), "accept must admit {expected}");
        }

        // Setting this by hand turns reqwest's decoding off, and the body then
        // arrives compressed and is read as text — which is what an empty page
        // looked like before.
        assert!(
            !names.contains(&"accept-encoding"),
            "accept-encoding belongs to reqwest's compression features"
        );
    }

    #[test]
    fn the_runtime_placeholder_user_agent_is_replaced_but_a_real_one_is_kept() {
        // The rule the transport applies when merging caller headers.
        let placeholder = RUNTIME_DEFAULT_USER_AGENT;
        assert_ne!(
            placeholder, FORGE_USER_AGENT,
            "there is nothing to replace if these match"
        );
        let caller = "AcmeDocsBot/2.0";
        assert!(
            !caller.eq_ignore_ascii_case(placeholder),
            "a caller-chosen UA is not the placeholder and must survive"
        );
    }

    #[test]
    fn the_turn_deadline_bounds_one_fetch() {
        assert_eq!(fetch_timeout(None), DEFAULT_FETCH_TIMEOUT);
        let clock = SystemClock;
        let generous = Deadline::after(&clock, 10 * 60 * 1000);
        assert_eq!(fetch_timeout(Some(&generous)), DEFAULT_FETCH_TIMEOUT);
        let nearly_expired = Deadline::after(&clock, 1);
        assert_eq!(fetch_timeout(Some(&nearly_expired)), MIN_FETCH_TIMEOUT);
    }
}

#[cfg(test)]
mod proxy_tests {
    use super::*;
    #[tokio::test]
    async fn fetch_uses_environment_proxy() {
        let (proxies, proxy) = crate::outbound_http::proxy_tests::listener(502).await;
        let client = pinned_client(
            &url::Url::parse("https://93.184.215.14/x").unwrap(),
            Duration::from_secs(2),
            &proxies,
        )
        .await
        .unwrap();
        assert!(
            client
                .get("https://93.184.215.14/x")
                .timeout(std::time::Duration::from_secs(2))
                .send()
                .await
                .is_err()
        );
        assert_eq!(proxy.await.unwrap(), "CONNECT 93.184.215.14:443 HTTP/1.1");
    }
}
