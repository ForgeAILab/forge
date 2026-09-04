use super::*;

async fn call_as(
    state: &AppState,
    user_id: &str,
    project_id: Option<&str>,
    name: &str,
    arguments: Value,
) -> Result<Value, McpToolError> {
    let result = dispatch_with_context(
        state,
        &McpContext {
            user_id: Some(user_id.to_owned()),
            project_id: project_id.map(str::to_owned),
        },
        "tools/call",
        json!({"name": name, "arguments": arguments}),
    )
    .await?;
    Ok(serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap())
}

async fn private_project(state: &AppState, owner_id: &str) -> String {
    let (project_id, _) = seed_project_repo(state).await;
    sqlx::query("UPDATE project SET owner_id = ? WHERE id = ?")
        .bind(owner_id)
        .bind(&project_id)
        .execute(state.db.pool())
        .await
        .unwrap();
    project_id
}

#[tokio::test]
async fn project_updates_share_rest_versions_and_return_safe_conflict_corrections() {
    let state = sqlite_state().await;
    let project_id = private_project(&state, "mcp-test-user").await;
    let initial = call_as(
        &state,
        "mcp-test-user",
        None,
        "forge_get_project",
        json!({"project_id": project_id}),
    )
    .await
    .unwrap();
    let version = initial["version"].as_i64().unwrap();
    let updated = call_as(
        &state,
        "mcp-test-user",
        None,
        "forge_update_project",
        json!({
            "project_id": project_id, "version": version,
            "settings": {"retry_budgets": {"review": 3}}, "name": "MCP edit"
        }),
    )
    .await
    .unwrap();
    assert_eq!(updated["version"], version + 1);

    // This is the exact CAS repository boundary used by REST PATCH /projects/{id}.
    let rest_update = UpdateProject {
        id: project_id.clone(),
        name: Some("REST edit".to_owned()),
        settings: None,
        primary_repo_id: None,
        paused_at: None,
        updated_at: now_rfc3339(),
    };
    assert!(matches!(
        ProjectRepo::update_at_version(&*state.db, rest_update.clone(), version, None).await,
        Err(db::DbError::VersionConflict)
    ));
    let current = ProjectRepo::update_at_version(&*state.db, rest_update, version + 1, None)
        .await
        .unwrap();
    for (name, fields) in [
        (
            "forge_update_project",
            json!({"settings": {"retry_budgets": {"review": 9}}}),
        ),
        (
            "forge_update_project_lifecycle_hooks",
            json!({"lifecycle_hooks": {}}),
        ),
    ] {
        let mut arguments = fields;
        arguments["project_id"] = json!(project_id);
        arguments["version"] = json!(version + 1);
        let error = call_as(&state, "mcp-test-user", None, name, arguments)
            .await
            .unwrap_err();
        let result = error.into_tool_response(json!(1)).result.unwrap();
        assert_eq!(result["structuredContent"]["code"], "version_conflict");
        assert_eq!(
            result["structuredContent"]["retry"]["arguments"]["version"],
            current.version
        );
        assert_eq!(
            result["structuredContent"]["current_version_or_revision"]["resource_id"],
            project_id
        );
    }
    let after = ProjectRepo::get_by_id(&*state.db, &project_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.version, current.version);
    assert_eq!(after.name, "REST edit");
    assert_eq!(
        serde_json::from_str::<Value>(&after.settings).unwrap()["retry_budgets"]["review"],
        3
    );

    for (name, fields) in [
        ("forge_update_project", json!({"name": "missing version"})),
        (
            "forge_update_project_lifecycle_hooks",
            json!({"lifecycle_hooks": {}}),
        ),
    ] {
        let mut arguments = fields;
        arguments["project_id"] = json!(project_id);
        assert_eq!(
            call_as(&state, "mcp-test-user", None, name, arguments)
                .await
                .unwrap_err()
                .code,
            -32602
        );
    }
}

#[tokio::test]
async fn unscoped_mcp_authorizes_projects_tasks_executions_and_dependency_references() {
    let state = sqlite_state().await;
    let now = now_rfc3339();
    UserRepo::create_user(
        &*state.db,
        &db::User {
            id: "bob".to_owned(),
            email: "bob@example.test".to_owned(),
            password_hash: "test".to_owned(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let bob_project = private_project(&state, "bob").await;
    let (public_project, _) = seed_project_repo(&state).await;
    let alice_project = private_project(&state, "mcp-test-user").await;
    let alice_task = seed_task_in_project(&state, alice_project.clone()).await;
    let bob_task = seed_task_in_project(&state, bob_project.clone()).await;
    let execution_id = seed_execution(&state, alice_task.id.clone()).await;

    // The newest row is private to Alice. Filtering after pagination would
    // produce an empty first page and expose its cursor/count to Bob.
    let first = call_as(
        &state,
        "bob",
        None,
        "forge_list_projects",
        json!({"limit": 1}),
    )
    .await
    .unwrap();
    assert_eq!(first["data"].as_array().unwrap().len(), 1);
    let second = call_as(
        &state,
        "bob",
        None,
        "forge_list_projects",
        json!({"limit": 1, "cursor": first["next_cursor"]}),
    )
    .await
    .unwrap();
    let mut ids = vec![
        first["data"][0]["id"].as_str().unwrap().to_owned(),
        second["data"][0]["id"].as_str().unwrap().to_owned(),
    ];
    ids.sort();
    let mut expected = vec![bob_project.clone(), public_project.clone()];
    expected.sort();
    assert_eq!(ids, expected);
    assert_eq!(second["has_more"], false);
    let visible = ProjectRepo::list_visible(
        &*state.db,
        "bob",
        PageRequest {
            cursor: None,
            limit: 1,
            include_total: true,
            sort_by: SortBy::CreatedAt,
            sort_order: SortOrder::Desc,
        },
    )
    .await
    .unwrap();
    assert_eq!(visible.total_count, Some(2));

    for (name, arguments) in [
        ("forge_get_project", json!({"project_id": alice_project})),
        (
            "forge_update_project",
            json!({"project_id": alice_project, "version": 1, "name": "unauthorized"}),
        ),
        (
            "forge_update_project_lifecycle_hooks",
            json!({"project_id": alice_project, "version": 1, "lifecycle_hooks": {}}),
        ),
        (
            "forge_create_task",
            json!({"project_id": alice_project, "title": "unauthorized"}),
        ),
        ("forge_get_task", json!({"task_id": alice_task.id})),
        (
            "forge_update_task",
            json!({"task_id": alice_task.id, "version": alice_task.version, "title": "unauthorized"}),
        ),
        ("forge_list_executions", json!({"task_id": alice_task.id})),
        (
            "forge_follow_up_execution",
            json!({"execution_id": execution_id, "message": "unauthorized"}),
        ),
        (
            "forge_list_task_dependencies",
            json!({"task_id": alice_task.id}),
        ),
        (
            "forge_add_task_dependency",
            json!({"task_id": bob_task.id, "depends_on_id": alice_task.id}),
        ),
        (
            "forge_remove_task_dependency",
            json!({"task_id": bob_task.id, "depends_on_id": alice_task.id}),
        ),
        (
            "forge_create_task",
            json!({"project_id": bob_project, "parent_task_id": alice_task.id, "title": "unauthorized child"}),
        ),
    ] {
        let error = call_as(&state, "bob", None, name, arguments)
            .await
            .unwrap_err();
        assert_eq!(error.code, -32004, "{name}");
        let result = error.into_tool_response(json!(1)).result.unwrap();
        assert!(result["structuredContent"]
            .get("current_version_or_revision")
            .is_none());
    }
    assert_eq!(
        ProjectRepo::get_by_id(&*state.db, &alice_project)
            .await
            .unwrap()
            .unwrap()
            .name,
        "Forge"
    );
    assert_eq!(
        TaskRepo::get_by_id(&*state.db, &alice_task.id, false)
            .await
            .unwrap()
            .unwrap()
            .title,
        alice_task.title
    );

    // Legacy relationships must not expose inaccessible dependency targets.
    db::TaskDependencyRepo::add_dependency(&*state.db, &bob_task.id, &alice_task.id, &now)
        .await
        .unwrap();
    assert_eq!(
        call_as(
            &state,
            "bob",
            None,
            "forge_list_task_dependencies",
            json!({"task_id": bob_task.id})
        )
        .await
        .unwrap_err()
        .code,
        -32004
    );

    // Explicit membership restores existing REST access; the optional scope
    // continues to narrow references even when both Projects are visible.
    ProjectMemberRepo::add_member(
        &*state.db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: alice_project.clone(),
            user_id: "bob".to_owned(),
            role: "member".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .unwrap();
    call_as(
        &state,
        "bob",
        None,
        "forge_get_project",
        json!({"project_id": alice_project}),
    )
    .await
    .unwrap();
    assert_eq!(
        call_as(
            &state,
            "bob",
            None,
            "forge_list_task_dependencies",
            json!({"task_id": bob_task.id})
        )
        .await
        .unwrap()["depends_on"],
        json!([alice_task.id])
    );
    assert_eq!(
        call_as(
            &state,
            "bob",
            Some(&bob_project),
            "forge_list_task_dependencies",
            json!({"task_id": bob_task.id})
        )
        .await
        .unwrap_err()
        .code,
        -32602
    );
    for name in ["forge_add_task_dependency", "forge_remove_task_dependency"] {
        assert_eq!(
            call_as(
                &state,
                "bob",
                Some(&bob_project),
                name,
                json!({"task_id": bob_task.id, "depends_on_id": alice_task.id})
            )
            .await
            .unwrap_err()
            .code,
            -32602
        );
    }
}

#[tokio::test]
async fn mcp_creation_binds_owner_and_known_tools_require_a_principal() {
    let state = sqlite_state().await;
    let project = call_as(
        &state,
        "mcp-test-user",
        None,
        "forge_create_project",
        json!({"name": "Owned"}),
    )
    .await
    .unwrap();
    let stored = ProjectRepo::get_by_id(&*state.db, project["id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.owner_id.as_deref(), Some("mcp-test-user"));
    assert_eq!(project["version"], stored.version);
    let error = dispatch_with_context(
        &state,
        &McpContext::default(),
        "tools/call",
        json!({"name": "forge_list_projects", "arguments": {}}),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, -32001);
}
