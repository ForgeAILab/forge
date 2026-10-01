mod common;

#[tokio::test]
async fn shared_harness_data_dir_is_owned_across_state_clones() {
    let workspace = tempfile::tempdir().unwrap();
    let harness = common::test_app(workspace.path(), "data-isolation").await;
    let state = std::sync::Arc::clone(&harness.state);
    let root = state.effective_config.forge.data_dir.clone();
    assert!(root.starts_with(std::env::temp_dir()));
    assert!(root.is_dir());
    assert_eq!(state.config_path.as_ref(), &root.join("forge.yaml"));
    drop(harness);
    assert!(
        root.is_dir(),
        "state clones must retain the temporary directory"
    );
    drop(state);
    assert!(
        !root.exists(),
        "the last state must release the temporary directory"
    );
}
