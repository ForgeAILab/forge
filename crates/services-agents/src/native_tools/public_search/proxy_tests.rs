//! Tests for public search proxy handling, lookup failures, and timeouts.

use super::*;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

#[tokio::test(start_paused = true)]
async fn public_search_timeout_shares_resolution_and_request_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/search", listener.local_addr().unwrap());
    let timeout = Duration::from_secs(1);
    let started = tokio::time::Instant::now();
    let client = public_search_http_client_with_builder(timeout, async {
        tokio::time::sleep(Duration::from_millis(600)).await;
        Ok(reqwest::Client::builder().no_proxy())
    })
    .await
    .unwrap();
    let request = async {
        let error = client.get(endpoint).send().await.unwrap_err();
        assert!(error.is_timeout());
    };
    let server = async {
        let (stream, _) = listener.accept().await.unwrap();
        // Keep the local connection open without sending a response.
        std::future::pending::<()>().await;
        drop(stream);
    };
    tokio::select! {
        () = request => {}
        () = server => panic!("server must wait for the request timeout"),
    }
    assert!(
        started.elapsed() <= timeout + Duration::from_millis(10),
        "resolution must consume part of the request budget"
    );
}

#[tokio::test]
async fn public_search_timeout_refuses_exhausted_budget() {
    let error = public_search_http_client_with_builder(Duration::ZERO, async {
        // Timeout polls a ready resolver before its timer. An exhausted
        // budget must still refuse building a client.
        Ok(reqwest::Client::builder().no_proxy())
    })
    .await
    .unwrap_err();
    assert!(matches!(error, AgentHostError::Runtime(message)
            if message == "public search resolution timed out"));
}

#[tokio::test]
async fn public_search_uses_environment_proxy() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxies = ProxyEnvironment::from_vars(|key| {
        (key == "all_proxy").then(|| {
            format!(
                "http://search-user:search-secret@{}",
                listener.local_addr().unwrap()
            )
        })
    });
    let endpoint = url::Url::parse("https://93.184.215.14/search").unwrap();
    let client = public_search_http_client(&endpoint, Duration::from_secs(2), &proxies)
        .await
        .unwrap();
    let server = async {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = vec![];
        while !request.ends_with(b"\r\n\r\n") {
            let mut bytes = [0; 1024];
            let n = stream.read(&mut bytes).await.unwrap();
            assert!(n > 0 && request.len() + n < 16384);
            request.extend_from_slice(&bytes[..n]);
        }
        assert!(String::from_utf8(request)
            .unwrap()
            .starts_with("CONNECT 93.184.215.14:443 HTTP/1.1\r\n"));
        stream
            .write_all(b"HTTP/1.1 502 Rejected\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    };
    let request = async {
        let error = client.get(endpoint).send().await.unwrap_err();
        let text = format!("{error} {error:?}");
        assert!(!text.contains("search-user") && !text.contains("search-secret"));
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(server, request);
    })
    .await
    .unwrap();
}

#[test]
fn public_search_lookup_failure_is_a_runtime_failure() {
    assert!(matches!(
        public_search_egress_refusal(PublicHttpError::ResolutionFailed),
        AgentHostError::Runtime(message) if message.contains("could not be resolved")
    ));
    assert!(matches!(
        public_search_egress_refusal(PublicHttpError::RestrictedAddress),
        AgentHostError::Configuration(message) if message.contains("exclusively to public")
    ));
}

#[tokio::test]
async fn public_search_refuses_an_unusable_environment_proxy() {
    let proxies = ProxyEnvironment::from_vars(|key| {
        (key == "HTTPS_PROXY").then(|| "socks5h://127.0.0.1:1080".to_owned())
    });
    let endpoint = url::Url::parse("https://93.184.215.14/search").unwrap();
    let error = public_search_http_client(&endpoint, Duration::from_secs(2), &proxies)
        .await
        .expect_err("an unusable proxy must not fall back to a direct connection");
    assert!(
        matches!(&error, AgentHostError::Configuration(message) if message.contains("HTTPS_PROXY")),
        "{error:?}"
    );
}
