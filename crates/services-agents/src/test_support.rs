use std::{
    collections::HashSet,
    sync::{Mutex, OnceLock},
};

static AFTER_DOMAIN_COMMIT: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

fn armed() -> &'static Mutex<HashSet<String>> {
    AFTER_DOMAIN_COMMIT.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(crate) fn arm_after_domain_commit(action_id: &str) {
    armed()
        .lock()
        .expect("characterization failpoint lock")
        .insert(action_id.to_owned());
}

pub(crate) fn take_after_domain_commit(action_id: &str) -> bool {
    armed()
        .lock()
        .expect("characterization failpoint lock")
        .remove(action_id)
}
