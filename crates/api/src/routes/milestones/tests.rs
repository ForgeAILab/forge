//! Test timestamp validation for milestone authorization events.

use super::*;

#[test]
fn authorization_timestamp_requires_rfc3339_and_recent_input() {
    let now = Utc::now().to_rfc3339();
    assert!(well_formed_authorization_timestamp(&now));
    assert!(valid_authorization_timestamp(&now));
    assert!(!well_formed_authorization_timestamp("not-a-timestamp"));
    assert!(!well_formed_authorization_timestamp(&format!(" {now}")));
    let old = (Utc::now() - chrono::Duration::hours(49)).to_rfc3339();
    assert!(well_formed_authorization_timestamp(&old));
    assert!(!valid_authorization_timestamp(&old));
}
