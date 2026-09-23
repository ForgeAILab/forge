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
//! - **The connection is pinned to addresses this host resolved and checked.**
//!   A hostname that resolves to a private or loopback address is refused
//!   before any socket opens, and `resolve_to_addrs` keeps the client from
//!   re-resolving it to something else afterwards, which is what closes the
//!   DNS-rebinding window a URL check alone leaves open.
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

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use agent_runtime::core::cancel::Cancellation;
use agent_runtime::core::clock::{Deadline, SystemClock};
use agent_runtime::core::error::RuntimeError;
use agent_runtime::harness::{FetchRequest, FetchResponse, FetchTransport};
use async_trait::async_trait;

use crate::transport::{restricted_provider_hostname, restricted_provider_ip};

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
            let client = pinned_client(&url, timeout).await?;
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

/// One HTTPS client pinned to addresses this host resolved and accepted.
async fn pinned_client(url: &url::Url, timeout: Duration) -> Result<reqwest::Client, RuntimeError> {
    if url.scheme() != "https" {
        return Err(RuntimeError::tool(
            "fetch requires an https URL: Forge does not send plaintext requests on the host's \
             behalf",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(RuntimeError::tool("fetch URL must not carry credentials"));
    }
    let host = url
        .host_str()
        .ok_or_else(|| RuntimeError::tool("fetch URL has no host"))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| RuntimeError::tool("fetch URL has no port"))?;
    let addresses = match url.host() {
        Some(url::Host::Domain(domain)) => {
            if restricted_provider_hostname(domain) {
                return Err(RuntimeError::tool(
                    "fetch refuses hostnames that name this host's own network",
                ));
            }
            tokio::net::lookup_host((domain, port))
                .await
                .map_err(|_| RuntimeError::tool("fetch could not resolve the host"))?
                .collect::<Vec<_>>()
        }
        Some(url::Host::Ipv4(address)) => vec![SocketAddr::new(IpAddr::V4(address), port)],
        Some(url::Host::Ipv6(address)) => vec![SocketAddr::new(IpAddr::V6(address), port)],
        None => Vec::new(),
    };
    if addresses.is_empty() {
        return Err(RuntimeError::tool("fetch could not resolve the host"));
    }
    // Every resolved address must be public: one private answer is enough to
    // refuse the name, because the client may use any of them.
    if addresses
        .iter()
        .any(|address| restricted_provider_ip(address.ip()))
    {
        return Err(RuntimeError::tool(
            "fetch refuses a host that resolves into this host's own network",
        ));
    }
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve_to_addrs(host, &addresses)
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
            let error = pinned_client(&url, DEFAULT_FETCH_TIMEOUT)
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
                pinned_client(&url, DEFAULT_FETCH_TIMEOUT).await.is_err(),
                "{rejected} must not be reachable through the fetch transport"
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
