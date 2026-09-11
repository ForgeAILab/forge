use std::path::PathBuf;

use forge_solo::{
    cli::{AgentExecutor, Cli, PreflightError, TerminalPreflight},
    data_dir::{resolve_data_paths_from, SOLO_DATA_DIR_NAME},
    startup::{SoloStartup, StartupError},
};
use tempfile::TempDir;
use uuid::Uuid;

#[tokio::test]
async fn noninteractive_preflight_has_no_startup_side_effects() {
    let workspace = TempDir::new().expect("temporary workspace");
    let state = workspace.path().join("solo-state");
    let cli = Cli {
        path: workspace.path().join("not-a-repository"),
        data_dir: Some(state.clone()),
        agent: Some(AgentExecutor::Codex),
    };

    let result = SoloStartup::initialize_with_preflight(
        cli,
        TerminalPreflight {
            stdin_is_terminal: false,
            stdout_is_terminal: true,
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(StartupError::Preflight(
            PreflightError::NonInteractive { .. }
        ))
    ));
    assert!(!state.exists(), "preflight must precede data-root mutation");
}

#[test]
fn default_root_is_exactly_repo_scoped_and_outside_checkout() {
    let workspace = TempDir::new().expect("temporary workspace");
    let forge_root = workspace.path().join("forge");
    let checkout = workspace.path().join("checkout");
    std::fs::create_dir(&checkout).expect("checkout directory");
    let repository_id =
        Uuid::parse_str("00000000-0000-4000-8000-000000000001").expect("valid UUID v4");
    let current_dir = PathBuf::from("/");

    let paths = resolve_data_paths_from(&checkout, repository_id, &forge_root, None, &current_dir)
        .expect("default data paths");

    assert_eq!(
        paths.root,
        std::fs::canonicalize(workspace.path())
            .expect("canonical temporary workspace")
            .join("forge")
            .join(SOLO_DATA_DIR_NAME)
            .join(repository_id.to_string())
    );
    assert!(!paths.root.starts_with(&checkout));
    assert!(paths.worktrees.starts_with(&paths.root));
    assert!(paths.logs.starts_with(&paths.root));
}
