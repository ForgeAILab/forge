use super::*;

#[tokio::test]
async fn project_and_task_tools_enforce_visibility_and_member_roles() {
    let state = sqlite_state().await;
    let now = now_rfc3339();
    let other_id = "other-authz-user";
    UserRepo::create_user(
        &*state.db,
        &db::User {
            id: other_id.into(),
            email: "other-authz@test.invalid".into(),
            password_hash: "unused".into(),
            display_name: None,
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let project = ProjectRepo::create(
        &*state.db,
        CreateProject {
            id: new_uuid_v4(),
            name: "Private".into(),
            settings: "{}".into(),
            workflow_definition: serde_json::to_string(
                &services::workflow::default_workflow::default_workflow(),
            )
            .unwrap(),
            primary_repo_id: None,
            owner_id: Some("mcp-test-user".into()),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    let task = call_tool(
        &state,
        "forge_create_task",
        json!({"project_id":project.id,"title":"Private task"}),
    )
    .await;
    let task_id = task["id"].as_str().unwrap();
    let calls = [
        ("forge_get_project", json!({"project_id":project.id}), false),
        ("forge_list_tasks", json!({"project_id":project.id}), false),
        ("forge_get_task", json!({"task_id":task_id}), false),
        ("forge_list_executions", json!({"task_id":task_id}), false),
        (
            "forge_create_task",
            json!({"project_id":project.id,"title":"New task"}),
            true,
        ),
        (
            "forge_update_task",
            json!({"task_id":task_id,"title":"Edited","version":task["version"]}),
            true,
        ),
        (
            "forge_update_project",
            json!({"project_id":project.id,"name":"Edited","version":project.version}),
            true,
        ),
        (
            "forge_update_project_lifecycle_hooks",
            json!({"project_id":project.id,"lifecycle_hooks":{},"version":project.version}),
            true,
        ),
    ];
    for constrained in [false, true] {
        for (name, arguments, _) in &calls {
            let error = dispatch_with_context(
                &state,
                &McpContext {
                    user_id: Some(other_id.into()),
                    project_id: constrained.then(|| project.id.clone()),
                },
                "tools/call",
                json!({"name":name,"arguments":arguments}),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, -32004, "non-member {name}: {error:?}");
        }
    }
    ProjectMemberRepo::add_member(
        &*state.db,
        CreateProjectMember {
            id: new_uuid_v4(),
            project_id: project.id.clone(),
            user_id: other_id.into(),
            role: "viewer".into(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .unwrap();
    for constrained in [false, true] {
        for (name, arguments, write) in &calls {
            let result = dispatch_with_context(
                &state,
                &McpContext {
                    user_id: Some(other_id.into()),
                    project_id: constrained.then(|| project.id.clone()),
                },
                "tools/call",
                json!({"name":name,"arguments":arguments}),
            )
            .await;
            if *write {
                assert_eq!(result.unwrap_err().code, -32003, "viewer {name}");
            } else {
                assert!(result.is_ok(), "viewer {name}: {result:?}");
            }
        }
    }
    ProjectMemberRepo::update_member_role(
        &*state.db,
        &project.id,
        other_id,
        "member",
        &now_rfc3339(),
    )
    .await
    .unwrap();
    let context = McpContext {
        user_id: Some(other_id.into()),
        project_id: None,
    };
    assert!(dispatch_with_context(&state,&context,"tools/call",json!({"name":"forge_create_task","arguments":{"project_id":project.id,"title":"Member task"}})).await.is_ok());
    assert_eq!(dispatch_with_context(&state,&context,"tools/call",json!({"name":"forge_update_project","arguments":{"project_id":project.id,"name":"No","version":project.version}})).await.unwrap_err().code,-32003);
    let updated = call_tool(
        &state,
        "forge_update_project",
        json!({"project_id":project.id,"name":"Owner edit","version":project.version}),
    )
    .await;
    assert_eq!(updated["name"], "Owner edit");
}
