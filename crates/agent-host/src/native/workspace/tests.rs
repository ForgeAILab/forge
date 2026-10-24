//! Workspace scope, path component, and symlink boundary tests.

use super::*;
use crate::WorkspaceAccess;
use agent_runtime::core::workspace::Workspace;

#[test]
fn task_workspace_is_component_bounded() {
    let root = std::env::temp_dir().join(format!("forge-task-workspace-{}", std::process::id()));
    std::fs::create_dir_all(root.join("src")).expect("workspace creates");
    let workspace = TaskWorkspace::new(&root);
    let canonical_root = PathBuf::from(workspace.root());
    assert!(workspace.contains(workspace.root()));
    assert!(workspace.contains(canonical_root.join("src/main.rs").to_str().unwrap()));
    assert!(
        !workspace.contains(
            canonical_root
                .parent()
                .unwrap()
                .join("forge-task-workspace-sibling/src/main.rs")
                .to_str()
                .unwrap()
        )
    );
    assert!(
        !workspace.contains(
            canonical_root
                .join("../forge-task-workspace-sibling")
                .to_str()
                .unwrap()
        )
    );
    std::fs::remove_dir_all(root).expect("workspace cleans");
}

#[cfg(unix)]
#[test]
fn task_workspace_rejects_symlinked_read_and_write_paths() {
    use std::os::unix::fs::symlink;

    let root = std::env::temp_dir().join(format!(
        "forge-task-workspace-symlink-{}",
        std::process::id()
    ));
    let outside = std::env::temp_dir().join(format!(
        "forge-task-workspace-outside-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).expect("workspace creates");
    std::fs::create_dir_all(&outside).expect("outside creates");
    std::fs::write(outside.join("secret.txt"), "outside").expect("outside file writes");
    symlink(&outside, root.join("linked")).expect("symlink creates");
    symlink(outside.join("does-not-exist"), root.join("broken-link"))
        .expect("broken symlink creates");

    let workspace = TaskWorkspace::new(&root);
    assert!(!workspace.contains(root.join("linked/secret.txt").to_str().unwrap()));
    assert!(!workspace.contains(root.join("linked/new.txt").to_str().unwrap()));
    assert!(!workspace.contains(root.join("broken-link/new.txt").to_str().unwrap()));

    std::fs::remove_dir_all(root).expect("workspace cleans");
    std::fs::remove_dir_all(outside).expect("outside cleans");
}

#[test]
fn non_task_scopes_cannot_supply_workspace() {
    for scope_type in [
        CanonicalScopeType::Account,
        CanonicalScopeType::Project,
        CanonicalScopeType::AgentChat,
    ] {
        let scope = CanonicalScope {
            scope_type,
            scope_id: "scope-1".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        };
        let error = workspace_for_scope(&scope, Some("/tmp/repo")).unwrap_err();
        assert!(matches!(error, AgentHostError::Authority(_)));
        let deny_all = workspace_for_scope(&scope, None).expect("deny-all workspace");
        assert!(!deny_all.contains("/tmp/repo/file.rs"));
    }
}

#[test]
fn task_scope_requires_a_host_issued_workspace() {
    for access in [WorkspaceAccess::TaskRead, WorkspaceAccess::TaskWrite] {
        let scope = CanonicalScope {
            scope_type: CanonicalScopeType::Task,
            scope_id: "task-1".to_owned(),
            workspace_access: access,
        };
        let error = workspace_for_scope(&scope, None).unwrap_err();
        assert!(matches!(error, AgentHostError::Authority(_)));
    }
}

#[test]
fn canonical_scope_only_grants_task_read_or_write() {
    for scope_type in [
        CanonicalScopeType::Account,
        CanonicalScopeType::Project,
        CanonicalScopeType::AgentChat,
    ] {
        assert!(
            CanonicalScope {
                scope_type,
                scope_id: "scope-1".to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            }
            .validate()
            .is_ok()
        );
        for access in [WorkspaceAccess::TaskRead, WorkspaceAccess::TaskWrite] {
            assert!(
                CanonicalScope {
                    scope_type,
                    scope_id: "scope-1".to_owned(),
                    workspace_access: access,
                }
                .validate()
                .is_err()
            );
        }
    }
    for access in [WorkspaceAccess::TaskRead, WorkspaceAccess::TaskWrite] {
        assert!(
            CanonicalScope {
                scope_type: CanonicalScopeType::Task,
                scope_id: "task-1".to_owned(),
                workspace_access: access,
            }
            .validate()
            .is_ok()
        );
    }
    assert!(
        CanonicalScope {
            scope_type: CanonicalScopeType::Task,
            scope_id: "task-1".to_owned(),
            workspace_access: WorkspaceAccess::Deny,
        }
        .validate()
        .is_err()
    );
}
