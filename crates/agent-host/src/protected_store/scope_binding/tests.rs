//! Permission intersections and scope ceilings fail closed.

use super::*;

#[test]
fn effective_permission_intersection_fails_closed() {
    let mut effective = permission_set(r#"{"allowed":["read_project","task_write"]}"#);
    intersect_permissions(&mut effective, &permission_set(r#"["read_project"]"#));
    assert_eq!(effective, BTreeSet::from(["read_project".to_owned()]));

    let mut malformed = permission_set("not-json");
    intersect_permissions(
        &mut malformed,
        &scope_permission_set(
            crate::CanonicalScopeType::Project,
            crate::WorkspaceAccess::Deny,
            false,
            false,
        ),
    );
    assert!(malformed.is_empty());
}

#[test]
fn task_scope_ceiling_is_the_only_filesystem_policy() {
    let task = scope_permission_set(
        crate::CanonicalScopeType::Task,
        crate::WorkspaceAccess::TaskWrite,
        false,
        false,
    );
    assert!(task.contains("task_read"));
    assert!(task.contains("task_write"));

    let project = scope_permission_set(
        crate::CanonicalScopeType::Project,
        crate::WorkspaceAccess::Deny,
        false,
        false,
    );
    assert!(!project.contains("task_read"));
    assert!(!project.contains("task_write"));
}

#[test]
fn legacy_project_setup_scope_keeps_only_adoption_permission() {
    let setup = scope_permission_set(
        crate::CanonicalScopeType::Project,
        crate::WorkspaceAccess::Deny,
        true,
        true,
    );
    assert!(setup.contains("read_project"));
    assert!(setup.contains("propose_project"));
    assert!(setup.contains("propose_message"));
    assert!(!setup.contains("propose_task"));
    assert!(!setup.contains("propose_review"));
    assert!(!setup.contains("propose_session"));
}
