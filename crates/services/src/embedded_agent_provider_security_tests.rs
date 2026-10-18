use super::*;
#[tokio::test]
async fn provider_endpoints_reject_transition_and_zero_network_addresses() {
    for ip in [
        "0.0.0.1",
        "2001:db8::1",
        "2001:2::1",
        "2001:10::1",
        "2001:20::1",
        "100::1",
        "3fff::1",
        "::ffff:0:10.0.0.1",
        "::ffff:0:127.0.0.1",
        "::ffff:0:169.254.169.254",
        "0.255.255.255",
        "192.88.99.0",
        "192.88.99.1",
        "192.88.99.255",
        "64:ff9b::192.88.99.1",
        "2002:c058:6301::",
        "::ffff:192.88.99.1",
        "::192.88.99.1",
        "64:ff9b::7f00:1",
        "64:ff9b::a00:1",
        "64:ff9b::a9fe:a9fe",
        "64:ff9b::ffff:ffff",
        "64:ff9b:1::",
        "64:ff9b:1:ffff:ffff:ffff:ffff:ffff",
        "2002:7f00:1::",
        "2002:a00:1::",
        "2002:a9fe:a9fe::",
        "2002:ff:ffff::",
        "2001::",
        "2001:0:ffff:ffff:ffff:ffff:ffff:ffff",
        "::ffff:127.0.0.1",
        "::127.0.0.1",
        "::ffff:0.1.2.3",
        "::0.1.2.3",
        "::ffff:10.0.0.1",
        "::10.0.0.1",
        "::ffff:169.254.169.254",
        "::169.254.169.254",
    ] {
        let address: IpAddr = ip.parse().unwrap();
        assert!(is_restricted_public_address(address), "{ip}");
        let host = match address {
            IpAddr::V4(_) => ip.to_owned(),
            IpAddr::V6(_) => format!("[{ip}]"),
        };
        let endpoint = format!("https://{host}/v1");
        assert!(
            validate_provider_url_shape(&endpoint).is_err(),
            "{endpoint}"
        );
        assert!(
            provider_http_client(
                &endpoint,
                Duration::from_secs(1),
                &ProxyEnvironment::default()
            )
            .await
            .is_err(),
            "{endpoint}"
        );
    }
    for ip in [
        "1.0.0.1",
        "8.8.8.8",
        "::ffff:0:8.8.8.8",
        // Well-known NAT64 is judged by its embedded IPv4 destination.
        "64:ff9b::808:808",
        "64:ff9b:0:1::",
        "64:ff9b:2::",
        "2001:1::",
        "2002:808:808::",
        "::ffff:8.8.8.8",
        "::8.8.8.8",
        "2606:4700:4700::1111",
    ] {
        assert!(
            !is_restricted_public_address(ip.parse().unwrap()),
            "public control {ip}"
        );
    }
}

#[test]
fn hostname_refusal_provider_endpoints() {
    for reserved in [
        "localhost",
        "localhost.localdomain",
        "internal",
        "home.arpa",
        "local",
    ] {
        for host in [
            reserved.to_owned(),
            format!("search.{reserved}"),
            format!("nested.search.{reserved}"),
        ] {
            for host in [
                host.clone(),
                host.to_ascii_uppercase(),
                format!("{host}."),
                format!("{}.", host.to_ascii_uppercase()),
            ] {
                let endpoint = format!("https://{host}/v1");
                assert!(validate_provider_url_shape(&endpoint).is_err(), "{host}");
            }
        }
    }
    for host in [
        "example.internal.com",
        "internal.example.com",
        "home.arpa.example.com",
        "local.example.com",
        "localhost.example.com",
        "localhost.localdomain.example.com",
        "notinternal",
        "notlocal",
    ] {
        let endpoint = format!("https://{host}/v1");
        assert!(validate_provider_url_shape(&endpoint).is_ok(), "{host}");
    }
}

#[tokio::test]
async fn provider_probe_and_connection_test_use_environment_proxy() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxies = ProxyEnvironment::from_vars(|key| {
        (key == "HTTPS_PROXY").then(|| {
            format!(
                "http://probe-user:probe-secret@{}",
                listener.local_addr().unwrap()
            )
        })
    });
    let client = provider_http_client("https://93.184.215.14/x", Duration::from_secs(2), &proxies)
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
            .write_all(b"HTTP/1.1 407 Rejected\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
    };
    let request = async {
        let error = client
            .get("https://93.184.215.14/x")
            .send()
            .await
            .unwrap_err();
        let text = format!("{error} {error:?}");
        assert!(!text.contains("probe-user") && !text.contains("probe-secret"));
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(server, request);
    })
    .await
    .unwrap();
}
