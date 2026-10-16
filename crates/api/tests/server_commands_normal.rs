//! Normal-mode counterpart runs in its own process with the default policy.
mod common;
#[path = "server_command_support/mod.rs"]
mod support;
#[tokio::test]
async fn normal_registered_owner_can_save_script_hook() {
    support::registered_owner_can_save_hook(false).await;
}
#[tokio::test]
async fn normal_stored_script_lifecycle_event_runs() {
    support::stored_lifecycle_event(false).await;
}
#[tokio::test]
async fn normal_blocking_script_runs() {
    support::blocking_hook(false).await;
}
#[tokio::test]
async fn normal_stored_ci_review_entry_runs() {
    support::stored_ci_review_entry(false).await;
}
#[tokio::test]
async fn normal_environment_setup_and_probe_run() {
    support::environment_setup(false).await;
}
#[tokio::test]
async fn normal_provider_credentials_and_chat_turns_unchanged() {
    support::provider_and_chat_turns(false).await;
}
#[tokio::test]
async fn normal_repository_inputs_unchanged() {
    support::repository_inputs(false).await;
}
