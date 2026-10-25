//! Shared fixtures for daemon workspace protocol tests.

use super::*;

pub(super) async fn owner_params(
    fixture: &Fixture,
    id: &str,
    handle: &str,
    operation: WorkspaceOwnerOperation,
) -> WorkspaceOwnerOperationParams {
    let owned = fixture.backend.state.lock().unwrap().handles[handle].clone();
    WorkspaceOwnerOperationParams {
        fence: fence(
            id,
            owned.generation,
            &git::get_current_sha(&owned.path).await.unwrap(),
        ),
        workspace_handle: handle.into(),
        operation,
    }
}

pub(super) async fn owner(
    fixture: &Fixture,
    id: &str,
    handle: &str,
    operation: WorkspaceOwnerOperation,
) -> WorkspaceOwnerOperationResult {
    serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_RESET,
                serde_json::to_value(owner_params(fixture, id, handle, operation).await).unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap()
}

pub(super) async fn inspect(
    fixture: &Fixture,
    query: WorkspaceGitQuery,
    optional: bool,
    limit: u64,
) -> CommandResult<WorkspaceInspectResult> {
    fixture
        .backend
        .handle(
            METHOD_WORKSPACE_READ,
            serde_json::to_value(WorkspaceInspectParams::Git {
                workspace: fixture.reference(),
                query,
                optional,
                limit,
            })
            .unwrap(),
            Vec::new,
        )
        .await
        .and_then(decode)
}

pub(in super::super) fn merge_params(
    fixture: &Fixture,
    id: &str,
    candidate: &str,
    target: &str,
    reviewed: bool,
) -> WorkspaceReviewedMergeParams {
    WorkspaceReviewedMergeParams {
        reviewed_commit_sha: reviewed.then(|| candidate.into()),
        merge: WorkspaceMergeParams {
            fence: fence(id, 1, candidate),
            workspace_handle: fixture.prepared.workspace.workspace_handle.clone(),
            repo_location_id: "location-1".into(),
            target_branch: "main".into(),
            expected_target_sha: target.into(),
            handed_off_paths: Vec::new(),
        },
        candidate_transfer_key: None,
    }
}

pub(in super::super) fn retain_intent(fixture: &Fixture, method: &str, request: Value) {
    let fence: WorkspaceMutationFence = decode(request.clone()).unwrap();
    fixture
        .journal
        .retain_entry(&JournalEntry::Operation {
            operation: JournalOperation {
                entry_id: operation_entry_id(&fence.operation_id),
                workspace_handle: request["workspace_handle"].as_str().map(str::to_owned),
                fence,
                method: method.into(),
                request,
                outcome: None,
                acknowledged: false,
                effect_started: true,
            },
        })
        .unwrap();
}

pub(super) async fn reconcile(fixture: &Fixture, id: &str) -> WorkspaceReconcileResult {
    serde_json::from_value(
        fixture
            .backend
            .handle(
                METHOD_WORKSPACE_DESCRIBE,
                serde_json::to_value(WorkspaceReconcileParams {
                    integration: api_types::WorkspaceIntegrationBinding::TaskStep,
                    workspace: fixture.reference(),
                    operation: WorkspaceReconcileOperation::Reconcile,
                    operation_id: id.into(),
                })
                .unwrap(),
                Vec::new,
            )
            .await
            .unwrap(),
    )
    .unwrap()
}

pub(in super::super) fn attempt_request(
    fixture: &Fixture,
    head: &str,
    target: &str,
    generation: i64,
    kind: WorkspaceIntegrationKind,
) -> WorkspaceIntegrationRequest {
    let location = fixture.backend.state.lock().unwrap().locations["location-1"].clone();
    WorkspaceIntegrationRequest {
        fence: IntegrationOwnerFence {
            queue_id: "queue-1".into(),
            attempt_id: "attempt-1".into(),
            generation,
            lease_owner: "queue-worker".into(),
            target_owner: serde_json::json!({"location_id":"location-1","owner_kind":"daemon","daemon_id":"daemon-1","runtime_id":"runtime-1","generation":location.version}),
        },
        kind,
        witness: serde_json::json!({"workspace":{"workspace_id":"workspace-1","placement_id":"placement-1","generation":1,"handle":fixture.prepared.workspace.workspace_handle,"owner":{"kind":"daemon","daemon_id":"daemon-1","runtime_id":"runtime-1"}},"target_branch":"main","expected_head_sha":head,"expected_target_sha":target,"handoff_conflicts":false}),
    }
}

pub(in super::super) fn attempt_merge_params(
    fixture: &Fixture,
    request: WorkspaceIntegrationRequest,
) -> WorkspaceReviewedMergeParams {
    let mut params = merge_params(
        fixture,
        &request.operation_id(),
        request.witness["expected_head_sha"].as_str().unwrap(),
        request.witness["expected_target_sha"].as_str().unwrap(),
        true,
    );
    params.merge.fence.integration = WorkspaceIntegrationBinding::Attempt { request };
    params
}

pub(super) fn task_step_merge_params(
    fixture: &Fixture,
    id: &str,
    head: &str,
    target: &str,
    step_attempts: i64,
    location_version: i64,
) -> WorkspaceReviewedMergeParams {
    let mut request = attempt_request(
        fixture,
        head,
        target,
        step_attempts,
        WorkspaceIntegrationKind::Merge,
    );
    request.fence.lease_owner = "task-step:step-1".into();
    request.fence.target_owner["generation"] = serde_json::json!(location_version);
    let mut params = merge_params(fixture, id, head, target, true);
    params.merge.fence.integration = WorkspaceIntegrationBinding::TaskStepEffect { request };
    params
}
