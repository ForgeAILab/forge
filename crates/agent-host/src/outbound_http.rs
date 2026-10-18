//! Validated public HTTPS egress. Direct connections pin local DNS answers;
//! proxy tunnels retain the target name, so the operator's proxy resolves it again.

use std::net::{IpAddr, SocketAddr};

use config::{
    is_blocked_public_search_address, is_restricted_public_address, is_restricted_public_hostname,
};
use hyper_util::client::proxy::matcher::Matcher;

/// The refusal for a proxy variable Forge cannot use. It never echoes the
/// value, which can carry the operator's proxy credentials.
pub const UNUSABLE_PROXY_MESSAGE: &str = "public request refuses an unusable HTTPS_PROXY/ALL_PROXY \
     value: only http:// and https:// proxy URLs are supported";

/// A per-client snapshot, using reqwest's own system-proxy matcher and precedence.
/// Deliberately has no Debug implementation: proxy configuration can hold secrets.
pub struct ProxyEnvironment {
    matcher: Matcher,
    /// The value that applies to https targets is set but unusable.
    unusable: bool,
}

impl Default for ProxyEnvironment {
    fn default() -> Self {
        Self {
            matcher: Matcher::builder().build(),
            unusable: false,
        }
    }
}

impl ProxyEnvironment {
    pub fn from_env() -> Self {
        Self::from_vars(|key| std::env::var(key).ok())
    }

    /// Injection seam: never mutate the process environment to configure a client.
    pub fn from_vars(mut get: impl FnMut(&str) -> Option<String>) -> Self {
        let mut first = |upper, lower| get(upper).or_else(|| get(lower)).unwrap_or_default();
        let https = first("HTTPS_PROXY", "https_proxy");
        let all = first("ALL_PROXY", "all_proxy");
        let no = first("NO_PROXY", "no_proxy");
        // The matcher would quietly skip an unparseable or unsupported value,
        // falling through to ALL_PROXY or to a direct connection. Forge fails
        // closed instead: the value that applies to https targets must be usable.
        let applicable = if https.is_empty() { &all } else { &https };
        let unusable = !applicable.is_empty() && !usable_proxy(applicable);
        Self {
            matcher: Matcher::builder().https(https).all(all).no(no).build(),
            unusable,
        }
    }

    fn proxy_for(&self, url: &url::Url) -> Result<Option<reqwest::Proxy>, PublicHttpError> {
        if self.unusable {
            return Err(PublicHttpError::UnusableProxy);
        }
        let uri = url[..url::Position::BeforePath]
            .parse()
            .map_err(|_| PublicHttpError::InvalidUrl)?;
        let Some(intercept) = self.matcher.intercept(&uri) else {
            return Ok(None);
        };
        // Matcher strips userinfo from this URI. Authentication travels only in
        // a sensitive header; neither setup errors nor request errors get a URL
        // containing the operator's credentials.
        let mut proxy = reqwest::Proxy::https(intercept.uri().to_string())
            .map_err(|_| PublicHttpError::ClientUnavailable)?;
        if let Some(auth) = intercept.basic_auth() {
            let mut auth = auth.clone();
            auth.set_sensitive(true);
            proxy = proxy.custom_http_auth(auth);
        }
        Ok(Some(proxy))
    }
}

/// Parsed exactly as the matcher parses it. This reqwest build has no SOCKS
/// support, so only an HTTP(S) proxy can carry the CONNECT tunnel.
fn usable_proxy(value: &str) -> bool {
    let probe = "https://proxy-probe.invalid/"
        .parse()
        .expect("static probe URI parses");
    Matcher::builder()
        .https(value)
        .build()
        .intercept(&probe)
        .is_some_and(|proxy| matches!(proxy.uri().scheme_str(), Some("http" | "https")))
}

#[derive(Clone, Copy)]
pub enum PublicAddressPolicy {
    Provider,
    Search,
}

#[derive(Debug, thiserror::Error)]
pub enum PublicHttpError {
    #[error("public request requires an HTTPS URL without credentials")]
    InvalidUrl,
    #[error("public request refuses a local hostname")]
    RestrictedHostname,
    #[error("public request could not resolve the host")]
    ResolutionFailed,
    #[error("public request refuses a private or local address")]
    RestrictedAddress,
    #[error("public HTTP client unavailable")]
    ClientUnavailable,
    #[error("{}", UNUSABLE_PROXY_MESSAGE)]
    UnusableProxy,
}

/// Reject a mixed DNS answer as a whole, rather than keeping its public subset.
pub fn validate_public_addresses(
    addresses: Vec<SocketAddr>,
    policy: PublicAddressPolicy,
) -> Result<Vec<SocketAddr>, PublicHttpError> {
    if addresses.is_empty() {
        return Err(PublicHttpError::ResolutionFailed);
    }
    let restricted = match policy {
        PublicAddressPolicy::Provider => is_restricted_public_address,
        PublicAddressPolicy::Search => is_blocked_public_search_address,
    };
    if addresses.iter().any(|address| restricted(address.ip())) {
        return Err(PublicHttpError::RestrictedAddress);
    }
    Ok(addresses)
}

/// Validate locally before connecting, even when the proxy will resolve again.
/// Literal IPs never cause a DNS query. The proxy address is operator-controlled
/// and intentionally does not pass through this destination policy.
pub async fn resolve_public_url(
    url: &url::Url,
    policy: PublicAddressPolicy,
) -> Result<Vec<SocketAddr>, PublicHttpError> {
    if url.scheme() != "https" || !url.username().is_empty() || url.password().is_some() {
        return Err(PublicHttpError::InvalidUrl);
    }
    let port = url
        .port_or_known_default()
        .ok_or(PublicHttpError::InvalidUrl)?;
    let addresses = match url.host() {
        Some(url::Host::Domain(domain)) => {
            if is_restricted_public_hostname(domain) {
                return Err(PublicHttpError::RestrictedHostname);
            }
            tokio::net::lookup_host((domain, port))
                .await
                .map_err(|_| PublicHttpError::ResolutionFailed)?
                .collect()
        }
        Some(url::Host::Ipv4(ip)) => vec![SocketAddr::new(IpAddr::V4(ip), port)],
        Some(url::Host::Ipv6(ip)) => vec![SocketAddr::new(IpAddr::V6(ip), port)],
        None => return Err(PublicHttpError::InvalidUrl),
    };
    validate_public_addresses(addresses, policy)
}

pub async fn public_http_client_builder(
    url: &url::Url,
    policy: PublicAddressPolicy,
    proxies: &ProxyEnvironment,
) -> Result<reqwest::ClientBuilder, PublicHttpError> {
    let addresses = resolve_public_url(url, policy).await?;
    pinned_client_builder(url, &addresses, proxies)
}

fn pinned_client_builder(
    url: &url::Url,
    addresses: &[SocketAddr],
    proxies: &ProxyEnvironment,
) -> Result<reqwest::ClientBuilder, PublicHttpError> {
    // Disable reqwest's implicit environment read: this build uses only the
    // snapshot passed above. Automatic redirects must never escape validation.
    let builder = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none());
    match proxies.proxy_for(url)? {
        Some(proxy) => Ok(builder.proxy(proxy)),
        None => Ok(builder.resolve_to_addrs(
            url.host_str().ok_or(PublicHttpError::InvalidUrl)?,
            addresses,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::dns::{Name, Resolve, Resolving};
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    // A second resolution must fail locally, without issuing a DNS query. The
    // prevalidated address override must win over this resolver.
    #[derive(Debug)]
    pub(super) struct RefuseDns;

    impl Resolve for RefuseDns {
        fn resolve(&self, _name: Name) -> Resolving {
            Box::pin(async { Err(std::io::Error::other("unexpected second DNS lookup").into()) })
        }
    }

    pub(super) async fn read_request(stream: &mut TcpStream) -> String {
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut chunk = [0; 1024];
            let size = stream.read(&mut chunk).await.unwrap();
            assert!(size > 0 && request.len() + size <= 16 * 1024);
            request.extend_from_slice(&chunk[..size]);
        }
        String::from_utf8(request).unwrap()
    }

    fn local_client(address: SocketAddr) -> (reqwest::Client, String) {
        // HTTP is confined to this transport-policy test. Production enters via
        // public_http_client_builder, which validates HTTPS and rejects loopback first.
        let endpoint = format!("http://provider.invalid:{}/models", address.port());
        let parsed = url::Url::parse(&endpoint).unwrap();
        let client = pinned_client_builder(&parsed, &[address], &ProxyEnvironment::default())
            .unwrap()
            .timeout(Duration::from_secs(2))
            .dns_resolver(std::sync::Arc::new(RefuseDns))
            .build()
            .unwrap();
        (client, endpoint)
    }

    #[tokio::test]
    async fn provider_client_pins_the_validated_address_without_resolving_again() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, endpoint) = local_client(address);
        let server = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request(&mut stream).await;
            assert!(request.starts_with("GET /models HTTP/1.1\r\n"));
            assert!(request.contains("host: provider.invalid:"));
            assert!(request.contains("authorization: Bearer sentinel"));
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        };
        let request = async {
            let response = client
                .get(endpoint)
                .bearer_auth("sentinel")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 200);
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(server, request)
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn provider_client_does_not_follow_redirects_to_internal_hosts() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (client, endpoint) = local_client(listener.local_addr().unwrap());
        let location = format!("http://{}/private", target.local_addr().unwrap());
        let server =
            async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_request(&mut stream).await;
                assert!(request.contains("x-goog-api-key: sentinel"));
                stream
            .write_all(
                format!("HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
            };
        let request = async {
            let response = client
                .get(endpoint)
                .header("x-goog-api-key", "sentinel")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 302);
        };
        let trap = async {
            assert!(
                tokio::time::timeout(Duration::from_millis(250), target.accept())
                    .await
                    .is_err(),
                "redirect destination must receive no connection or credential"
            );
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(server, request, trap)
        })
        .await
        .unwrap();
    }
}

#[cfg(test)]
pub(crate) mod proxy_tests {
    use super::*;
    use std::time::Duration;
    use tokio::{io::AsyncWriteExt, net::TcpListener, task::JoinHandle};

    pub(crate) async fn listener(status: u16) -> (ProxyEnvironment, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = format!(
            "http://proxy-user:proxy-secret@{}",
            listener.local_addr().unwrap()
        );
        let proxies =
            ProxyEnvironment::from_vars(|key| (key == "HTTPS_PROXY").then(|| proxy.clone()));
        let task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(3), async {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = super::tests::read_request(&mut stream).await;
                assert!(
                    request
                        .to_ascii_lowercase()
                        .contains("proxy-authorization: basic ")
                );
                stream
                    .write_all(
                        format!("HTTP/1.1 {status} Rejected\r\nContent-Length: 0\r\n\r\n")
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                request.lines().next().unwrap().to_owned()
            })
            .await
            .expect("proxy receives a request")
        });
        (proxies, task)
    }

    #[tokio::test]
    async fn environment_proxy_connects_public_literal_and_redacts_credentials() {
        for status in [407, 502] {
            let (proxies, proxy) = listener(status).await;
            let url = url::Url::parse("https://93.184.215.14/x").unwrap();
            let client = public_http_client_builder(&url, PublicAddressPolicy::Provider, &proxies)
                .await
                .unwrap()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            let error = client.get(url).send().await.unwrap_err();
            let text = format!("{error} {error:?}");
            for secret in [
                "proxy-user",
                "proxy-secret",
                "cHJveHktdXNlcjpwcm94eS1zZWNyZXQ=",
            ] {
                assert!(!text.contains(secret));
            }
            assert_eq!(proxy.await.unwrap(), "CONNECT 93.184.215.14:443 HTTP/1.1");
        }
    }

    #[tokio::test]
    async fn environment_proxy_never_sees_restricted_targets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxies = ProxyEnvironment::from_vars(|key| {
            (key == "ALL_PROXY").then(|| format!("http://{}", listener.local_addr().unwrap()))
        });
        for policy in [PublicAddressPolicy::Provider, PublicAddressPolicy::Search] {
            for target in [
                "127.0.0.1",
                "10.0.0.1",
                "169.254.169.254",
                "[::1]",
                "LOCALHOST.",
                "service.internal",
                "home.arpa",
            ] {
                let url = url::Url::parse(&format!("https://{target}/x")).unwrap();
                assert!(
                    public_http_client_builder(&url, policy, &proxies)
                        .await
                        .is_err()
                );
            }
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn environment_proxy_selection_matches_reqwest_precedence_and_exclusions() {
        let target = url::Url::parse("https://api.example.test/x").unwrap();
        let selected = |values: &[(&str, &str)], url: &url::Url| {
            let env = ProxyEnvironment::from_vars(|key| {
                values
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| (*value).to_owned())
            });
            let uri = url.as_str().parse().unwrap();
            env.matcher
                .intercept(&uri)
                .map(|proxy| proxy.uri().to_string())
        };
        for key in ["HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"] {
            assert_eq!(
                selected(&[(key, "http://127.0.0.1:1234")], &target).as_deref(),
                Some("http://127.0.0.1:1234/")
            );
        }
        assert_eq!(
            selected(
                &[
                    ("HTTPS_PROXY", "http://127.0.0.1:1234"),
                    ("https_proxy", "http://127.0.0.1:2345"),
                    ("ALL_PROXY", "http://127.0.0.1:3456")
                ],
                &target
            )
            .as_deref(),
            Some("http://127.0.0.1:1234/")
        );
        assert_eq!(
            selected(
                &[
                    ("HTTPS_PROXY", ""),
                    ("https_proxy", "http://127.0.0.1:2345"),
                    ("ALL_PROXY", "http://127.0.0.1:3456")
                ],
                &target
            )
            .as_deref(),
            Some("http://127.0.0.1:3456/")
        );
        for key in ["NO_PROXY", "no_proxy"] {
            for exclusion in [
                "api.example.test",
                ".example.test",
                "example.test",
                "*",
                "other.test, api.example.test",
            ] {
                assert!(
                    selected(
                        &[("HTTPS_PROXY", "http://127.0.0.1:1234"), (key, exclusion)],
                        &target
                    )
                    .is_none()
                );
            }
            let literal = url::Url::parse("https://93.184.215.14/x").unwrap();
            assert!(
                selected(
                    &[
                        ("ALL_PROXY", "http://127.0.0.1:1234"),
                        (key, "93.184.215.0/24")
                    ],
                    &literal
                )
                .is_none()
            );
        }
        assert!(selected(&[], &target).is_none());
        // reqwest's matcher, not curl's: `*` and domain entries never match a
        // literal IP target, and entries with a port or IPv6 brackets match nothing.
        let literal = url::Url::parse("https://93.184.215.14/x").unwrap();
        let v6 = url::Url::parse("https://[2606:4700::1]/x").unwrap();
        for (exclusion, url, bypassed) in [
            ("*", &literal, false),
            ("api.example.test:443", &target, false),
            ("[2606:4700::1]", &v6, false),
            ("2606:4700::1", &v6, true),
            ("API.Example.TEST", &target, true),
            ("xample.test", &target, false),
        ] {
            assert_eq!(
                selected(
                    &[
                        ("ALL_PROXY", "http://127.0.0.1:1234"),
                        ("NO_PROXY", exclusion)
                    ],
                    url
                )
                .is_none(),
                bypassed,
                "{exclusion}"
            );
        }
        // Plaintext HTTP_PROXY never applies to these https targets.
        assert!(selected(&[("HTTP_PROXY", "http://127.0.0.1:1234")], &target).is_none());
    }

    #[tokio::test]
    async fn unusable_environment_proxy_fails_closed_instead_of_going_direct() {
        let target = url::Url::parse("https://93.184.215.14/x").unwrap();
        let build = |values: Vec<(&'static str, String)>| {
            let proxies = ProxyEnvironment::from_vars(|key| {
                values
                    .iter()
                    .find(|(name, _)| *name == key)
                    .map(|(_, value)| value.clone())
            });
            let target = target.clone();
            async move {
                public_http_client_builder(&target, PublicAddressPolicy::Provider, &proxies)
                    .await
                    .map(|_| ())
            }
        };
        let valid = "http://127.0.0.1:9".to_owned();
        for unusable in [
            "http://bad host:3128",
            "http://",
            "/path-only",
            "ftp://unusable-secret@127.0.0.1:21",
            "socks5h://unusable-secret@127.0.0.1:1080",
            "socks5://127.0.0.1:1080",
        ] {
            let unusable = unusable.to_owned();
            for values in [
                vec![("HTTPS_PROXY", unusable.clone())],
                vec![("https_proxy", unusable.clone())],
                vec![("ALL_PROXY", unusable.clone())],
                vec![("all_proxy", unusable.clone())],
                // An unusable HTTPS value never falls through to ALL_PROXY.
                vec![
                    ("HTTPS_PROXY", unusable.clone()),
                    ("ALL_PROXY", valid.clone()),
                ],
                // NO_PROXY does not turn a misconfiguration into a direct path.
                vec![("HTTPS_PROXY", unusable.clone()), ("NO_PROXY", "*".into())],
            ] {
                let error = build(values).await.expect_err(&unusable);
                assert!(
                    matches!(error, PublicHttpError::UnusableProxy),
                    "{unusable}"
                );
                let text = format!("{error} {error:?}");
                assert!(!text.contains("unusable-secret") && !text.contains("127.0.0.1"));
            }
        }
        // ALL_PROXY does not apply when a usable HTTPS value is set, and an
        // empty value means unset.
        for values in [
            vec![
                ("HTTPS_PROXY", valid.clone()),
                ("ALL_PROXY", "ftp://x".into()),
            ],
            vec![("HTTPS_PROXY", String::new()), ("ALL_PROXY", valid.clone())],
            vec![("HTTPS_PROXY", "https://127.0.0.1:9".into())],
            vec![("HTTPS_PROXY", "127.0.0.1:9".into())],
            vec![("HTTP_PROXY", "ftp://x".into())],
        ] {
            assert!(build(values).await.is_ok());
        }
    }

    #[tokio::test]
    async fn environment_no_proxy_uses_direct_pinned_addresses() {
        use reqwest::dns::Resolve;
        let direct = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxies = ProxyEnvironment::from_vars(|key| match key {
            "HTTPS_PROXY" => Some(format!("http://{}", proxy.local_addr().unwrap())),
            "NO_PROXY" => Some("provider.invalid".into()),
            _ => None,
        });
        let url = url::Url::parse(&format!(
            "https://provider.invalid:{}/models",
            direct.local_addr().unwrap().port()
        ))
        .unwrap();
        assert!(proxies.proxy_for(&url).unwrap().is_none());
        // Exercise pinning below validation with local HTTP, like the direct
        // transport tests. No TLS test server or public connection is needed.
        let client = pinned_client_builder(&url, &[direct.local_addr().unwrap()], &proxies)
            .unwrap()
            .dns_resolver(std::sync::Arc::new(super::tests::RefuseDns))
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let mut request_url = url;
        request_url.set_scheme("http").unwrap();
        let server = async {
            let (mut stream, _) = direct.accept().await.unwrap();
            let _ = super::tests::read_request(&mut stream).await;
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
        };
        let request = async {
            assert_eq!(client.get(request_url).send().await.unwrap().status(), 200);
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(server, request);
        })
        .await
        .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), proxy.accept())
                .await
                .is_err()
        );
        // The fallback DNS resolver cannot resolve anything, even locally.
        assert!(
            super::tests::RefuseDns
                .resolve("provider.invalid".parse().unwrap())
                .await
                .is_err()
        );
    }
}
