use super::*;
use reqwest::dns::{Name, Resolve, Resolving};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// A second resolution must fail locally, without issuing a DNS query. The
// prevalidated address override must win over this resolver.
#[derive(Debug)]
struct RefuseDns;

impl Resolve for RefuseDns {
    fn resolve(&self, _name: Name) -> Resolving {
        Box::pin(async { Err(std::io::Error::other("unexpected second DNS lookup").into()) })
    }
}

async fn read_request(stream: &mut TcpStream) -> String {
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
    // provider_http_client, which validates HTTPS and rejects loopback first.
    let endpoint = format!("http://provider.invalid:{}/models", address.port());
    let parsed = url::Url::parse(&endpoint).unwrap();
    let client = pinned_provider_http_client_builder(&parsed, &[address], Duration::from_secs(2))
        .unwrap()
        .dns_resolver(Arc::new(RefuseDns))
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
    let server = async {
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

#[tokio::test]
async fn provider_endpoints_reject_transition_and_zero_network_addresses() {
    for ip in [
        "0.0.0.1",
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
            provider_http_client(&endpoint, Duration::from_secs(1))
                .await
                .is_err(),
            "{endpoint}"
        );
    }
    for ip in [
        "1.0.0.1",
        "8.8.8.8",
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
