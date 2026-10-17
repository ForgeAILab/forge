use config::{ConfigOverrides, ForgeConfig};

#[test]
#[should_panic(expected = "a test tried to use the real Forge data directory")]
fn dependency_default_data_dir_is_guarded_in_an_integration_test() {
    assert_eq!(
        std::env::var("FORGE_TEST_FORBID_DEFAULT_DATA_DIR").as_deref(),
        Ok("1")
    );
    let _ = config::default_data_dir();
}

#[test]
fn explicit_data_dir_constructs_defaults_without_resolving_home() {
    let directory = tempfile::tempdir().unwrap();
    let config = ForgeConfig::with_data_dir(directory.path().to_path_buf());
    assert_eq!(config.db_path(), directory.path().join("forge.db"));
    assert_eq!(config.workflows_dir(), directory.path().join("workflows"));
}

#[test]
fn explicit_file_data_dir_bypasses_the_tripwire() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("forge.yaml");
    std::fs::write(
        &path,
        format!("forge:\n  data_dir: {}\n", directory.path().display()),
    )
    .unwrap();
    let config = ForgeConfig::load(Some(&path), ConfigOverrides::default()).unwrap();
    assert_eq!(config.forge.data_dir, directory.path());
}

#[test]
fn explicit_cli_data_dir_bypasses_the_tripwire_without_a_config_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing-forge.yaml");
    let config = ForgeConfig::load(
        Some(&path),
        ConfigOverrides {
            data_dir: Some(directory.path().to_path_buf()),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(config.forge.data_dir, directory.path());
}
