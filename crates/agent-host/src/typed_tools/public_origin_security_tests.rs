//! Public HTTPS origin restrictions for fetch authorization.

#[test]
fn fetch_gate_rejects_transition_and_zero_network_origins() {
    for origin in [
        "https://0.1.2.3",
        "https://[2001:db8::1]",
        "https://[2001:2::1]",
        "https://[2001:10::1]",
        "https://[2001:20::1]",
        "https://[100::1]",
        "https://[3fff::1]",
        "https://[::ffff:0:10.0.0.1]",
        "https://[::ffff:0:127.0.0.1]",
        "https://[::ffff:0:169.254.169.254]",
        "https://192.88.99.1",
        "https://[64:ff9b::192.88.99.1]",
        "https://[2002:c058:6301::]",
        "https://[::ffff:192.88.99.1]",
        "https://[64:ff9b::7f00:1]",
        "https://[64:ff9b:1::1]",
        "https://[2002:7f00:1::]",
        "https://[2001::1]",
        "https://[::ffff:0.1.2.3]",
        "https://[::0.1.2.3]",
    ] {
        assert!(!super::public_https_origin(origin), "{origin}");
    }
    assert!(super::public_https_origin("https://8.8.8.8"));
    assert!(super::public_https_origin("https://[::ffff:0:8.8.8.8]"));
}
