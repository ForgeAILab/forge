//! Tests for public search result URLs, address policy, and client validation.

use super::*;

#[test]
fn address_tightening_public_search_result_urls_refuse_local_dns_suffixes() {
    for host in [
        "search.internal",
        "nested.search.internal",
        "SEARCH.INTERNAL.",
        "search.home.arpa",
        "nested.search.home.arpa",
        "SEARCH.HOME.ARPA.",
        // The reserved names themselves, not only their subdomains.
        "internal",
        "INTERNAL.",
        "home.arpa",
        "HOME.ARPA.",
    ] {
        assert!(
            normalize_public_result_url(&format!("https://{host}/result")).is_err(),
            "{host}"
        );
    }
    for host in [
        "search.internal.example",
        "search.home.arpa.example",
        "searchinternal",
        "example.internal.com",
        "internal.example.com",
        "home.arpa.example.com",
    ] {
        assert!(
            normalize_public_result_url(&format!("https://{host}/result")).is_ok(),
            "{host}"
        );
    }
}

#[test]
fn hostname_refusal_public_search_results() {
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
                assert!(
                    normalize_public_result_url(&format!("https://{host}/result")).is_err(),
                    "{host}"
                );
                assert!(is_private_or_local_search_host(&host), "resolver: {host}");
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
        assert!(
            normalize_public_result_url(&format!("https://{host}/result")).is_ok(),
            "{host}"
        );
        assert!(!is_private_or_local_search_host(host), "resolver: {host}");
    }
}

#[test]
fn address_tightening_public_search_refuses_any_blocked_dns_answer() {
    let public_v4 = "8.8.8.8:443".parse().unwrap();
    let public_v6 = "[2001:4860:4860::8888]:443".parse().unwrap();
    for host in [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "192.88.99.1",
        "::1",
        "2001:db8::1",
        "64:ff9b::10.0.0.1",
        "::ffff:0:10.0.0.1",
    ] {
        let blocked = std::net::SocketAddr::new(host.parse().unwrap(), 443);
        for addresses in [
            vec![blocked],
            vec![public_v4, blocked, public_v6],
            vec![blocked, public_v4],
            vec![public_v6, blocked],
        ] {
            let error = forge_agent_host::outbound_http::validate_public_addresses(
                addresses,
                PublicAddressPolicy::Search,
            )
            .expect_err("one blocked answer must refuse the entire hostname");
            assert!(matches!(
                error,
                forge_agent_host::outbound_http::PublicHttpError::RestrictedAddress
            ));
        }
    }
    assert!(forge_agent_host::outbound_http::validate_public_addresses(
        vec![],
        PublicAddressPolicy::Search
    )
    .is_err());
    let public = vec![public_v4, public_v6];
    let approved = forge_agent_host::outbound_http::validate_public_addresses(
        public.clone(),
        PublicAddressPolicy::Search,
    )
    .expect("all-public DNS answers");
    assert_eq!(approved, public);
}

#[test]
fn public_search_result_urls_reject_private_and_special_use_hosts() {
    for url in [
        "https://localhost/result",
        "https://127.0.0.1/result",
        "https://10.0.0.1/result",
        "https://169.254.169.254/result",
        "https://[::1]/result",
        "https://[::ffff:0:10.0.0.1]/result",
        "https://[::ffff:0:127.0.0.1]/result",
        "https://[::ffff:0:169.254.169.254]/result",
        "https://[::ffff:127.0.0.1]/result",
        "https://[::ffff:8.8.8.8]/result",
        "https://[::8.8.8.8]/result",
        "https://[64:ff9b::192.0.2.1]/result",
        "https://[fe80::1%25en0]/result",
        "https://192.0.2.1/result",
        "https://[2001:db8::1]/result",
        "https://[ff02::1]/result",
        "https://user@example.com/result",
        "https://example.com/result#fragment",
    ] {
        assert!(
            normalize_public_result_url(url).is_err(),
            "result URL must be rejected: {url}"
        );
    }
    assert_eq!(
        normalize_public_result_url("https://example.com/result").expect("public URL"),
        "https://example.com/result"
    );
    assert_eq!(
        normalize_public_result_url("http://example.com/result").expect("public URL"),
        "http://example.com/result"
    );
    assert!(normalize_public_result_url("https://example.com/\u{000a}").is_err());
}

#[test]
fn public_search_ietf_protocol_assignments_are_only_a_slash24() {
    for (host, allowed) in [
        ("192.0.1.1", true),
        ("192.0.3.1", true),
        ("192.0.255.1", true),
        ("192.0.0.8", false),
        ("192.0.2.1", false),
        ("192.88.99.1", false),
        ("198.51.100.1", false),
        ("203.0.113.1", false),
    ] {
        assert_eq!(
            is_blocked_public_search_address(host.parse().unwrap()),
            !allowed,
            "{host}"
        );
        assert_eq!(
            normalize_public_result_url(&format!("https://{host}/result")).is_ok(),
            allowed,
            "{host}"
        );
    }
}

#[test]
fn public_search_nat64_result_urls_apply_embedded_ipv4_policy() {
    for (host, allowed) in [
        ("64:ff9b::8.8.8.8", true),
        ("64:ff9b::1.1.1.1", true),
        ("64:ff9b::10.0.0.1", false),
        ("64:ff9b::127.0.0.1", false),
        ("64:ff9b::169.254.169.254", false),
        ("64:ff9b::192.88.99.1", false),
        ("64:ff9b:1::8.8.8.8", false),
        ("64:ff9b:1:1234:5678:9abc:808:808", false),
    ] {
        assert_eq!(
            normalize_public_result_url(&format!("https://[{host}]/result")).is_ok(),
            allowed,
            "{host}"
        );
    }
}

#[test]
fn public_search_and_provider_address_policies_agree_except_documented_exclusions() {
    for (host, blocked) in [
        ("0.0.0.0", true),
        ("0.255.255.255", true),
        ("1.0.0.1", false),
        ("8.8.8.8", false),
        ("10.0.0.1", true),
        ("100.63.255.255", false),
        ("100.64.0.0", true),
        ("100.127.255.255", true),
        ("100.128.0.0", false),
        ("127.0.0.1", true),
        ("169.254.169.254", true),
        ("172.16.0.1", true),
        ("192.0.0.1", true),
        ("192.0.1.1", false),
        ("192.0.2.1", true),
        ("192.0.3.1", false),
        ("192.88.98.255", false),
        ("192.88.99.0", true),
        ("192.88.99.1", true),
        ("192.88.99.255", true),
        ("192.88.100.0", false),
        ("192.168.0.1", true),
        ("198.18.0.1", true),
        ("198.19.255.255", true),
        ("198.20.0.0", false),
        ("198.51.100.1", true),
        ("203.0.113.1", true),
        ("224.0.0.1", true),
        ("240.0.0.1", true),
        ("255.255.255.255", true),
        ("::", true),
        ("::1", true),
        ("::ffff:10.0.0.1", true),
        ("::ffff:192.88.99.1", true),
        ("::192.88.99.1", true),
        ("::ffff:0:8.8.8.8", false),
        ("::ffff:0:10.0.0.1", true),
        ("::ffff:0:127.0.0.1", true),
        ("::ffff:0:169.254.169.254", true),
        ("::ffff:0:192.88.99.1", true),
        ("64:ff9b::8.8.8.8", false),
        ("64:ff9b::1.1.1.1", false),
        ("64:ff9b::10.0.0.1", true),
        ("64:ff9b::127.0.0.1", true),
        ("64:ff9b::169.254.169.254", true),
        ("64:ff9b::192.88.99.1", true),
        ("64:ff9b::255.255.255.255", true),
        ("64:ff9b:1::", true),
        ("64:ff9b:1:ffff:ffff:ffff:ffff:ffff", true),
        ("64:ff9b:2::", false),
        ("100:0:0:1::", false),
        ("100::", true),
        ("100::ffff:ffff:ffff:ffff", true),
        ("2001::1", true),
        ("2001:2::1", true),
        ("2001:db8::1", true),
        ("2001:10::1", true),
        ("2001:1f:ffff:ffff:ffff:ffff:ffff:ffff", true),
        ("2001:20::1", true),
        ("2001:2f:ffff:ffff:ffff:ffff:ffff:ffff", true),
        ("2001:6::1", false),
        ("2001:30::1", false),
        ("2001:4860:4860::8888", false),
        ("2002:7f00:1::", true),
        ("2002:c058:6301::", true),
        ("3fef::1", false),
        ("3fff::1", true),
        ("3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff", true),
        ("4000::1", false),
        ("fc00::1", true),
        ("fdff::1", true),
        ("fe80::1", true),
        ("febf::1", true),
        ("fec0::1", true),
        ("feff::1", true),
        ("ff02::1", true),
    ] {
        let address = host.parse().expect("test IP address");
        assert_eq!(
            config::is_restricted_public_address(address),
            blocked,
            "provider/fetch: {host}"
        );
        assert_eq!(
            is_blocked_public_search_address(address),
            blocked,
            "public search: {host}"
        );
    }

    // Deliberate search-only exclusions listed in docs/architecture.md.
    for host in [
        "::ffff:8.8.8.8",
        "::8.8.8.8",
        "2002:808:808::",
        "2001:1::1",
        "2001:2:1::1",
        "2001:3::1",
        "2001:4::1",
        "2001:5:ffff:ffff:ffff:ffff:ffff:ffff",
        "3ff0::1",
        "3fff:1000::1",
        "3fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
        "64:ff9b:0:1::",
        "64:ff9b::1:0:0",
        "64:ff9b:0:ffff:ffff:ffff:ffff:ffff",
    ] {
        let address = host.parse().expect("test IP address");
        assert!(
            !config::is_restricted_public_address(address),
            "provider/fetch control: {host}"
        );
        assert!(
            is_blocked_public_search_address(address),
            "public search exclusion: {host}"
        );
    }
}

#[test]
fn public_search_address_filter_rejects_private_mapped_and_special_use_ranges() {
    for address in [
        "0.0.0.0",
        "10.0.0.1",
        "100.64.0.1",
        "127.0.0.1",
        "169.254.1.1",
        "192.0.0.1",
        "192.0.2.1",
        "192.88.99.1",
        "198.18.0.1",
        "198.51.100.1",
        "203.0.113.1",
        "224.0.0.1",
        "::1",
        "::ffff:127.0.0.1",
        "::ffff:8.8.8.8",
        "::8.8.8.8",
        "fc00::1",
        "fe80::1",
        "64:ff9b::192.0.2.1",
        "2001:2::1",
        "2001:db8::1",
        "ff02::1",
    ] {
        let address = address.parse().expect("valid test address");
        assert!(
            is_blocked_public_search_address(address),
            "address must be blocked: {address}"
        );
    }
    assert!(!is_blocked_public_search_address(
        "8.8.8.8".parse().expect("public IPv4")
    ));
    assert!(!is_blocked_public_search_address(
        "2001:4860:4860::8888".parse().expect("public IPv6")
    ));
}
#[tokio::test]
async fn public_search_client_rejects_local_hosts_before_resolution() {
    for host in [
        "localhost",
        "SEARCH.INTERNAL.",
        "SEARCH.HOME.ARPA.",
        "INTERNAL.",
        "HOME.ARPA.",
    ] {
        let endpoint = url::Url::parse(&format!("https://{host}/search")).unwrap();
        assert!(
            public_search_http_client(
                &endpoint,
                Duration::from_secs(1),
                &ProxyEnvironment::default()
            )
            .await
            .is_err(),
            "{host}"
        );
    }
}
