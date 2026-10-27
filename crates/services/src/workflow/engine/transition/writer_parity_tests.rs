use crate::recovery::test_support::writer_fixture::case;

#[tokio::test]
async fn transition_deferred_marker() {
    case!("transition-deferred", "todo", f, {
        let mut workflow = (*f.ctx.workflow).clone();
        for state in &mut workflow.states {
            state.hooks = Default::default();
        }
        workflow
            .states
            .iter_mut()
            .find(|state| state.name == "planning")
            .unwrap()
            .hooks
            .on_enter
            .push(api_types::HookSpec {
                action: "dispatch_role_agent".into(),
                params: serde_json::json!({}),
                applies_to: api_types::HookAudience::All,
                on_failure: api_types::FailurePolicy::Block,
            });
        let engine = super::WorkflowEngine::new(f.db.clone(), f.ctx.event_bus.clone());
        engine
            .bind(&f.ctx.task_service)
            .transition_with_deferred_dispatch(
                &f.task.id,
                "planning",
                f.task.version,
                &workflow,
                &api_types::Actor::system(api_types::SystemComponent::Workflow),
                "deferred",
                false,
                Some("2099-01-01T00:00:00Z".into()),
            )
            .await
            .unwrap();
        let row = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
            .await
            .unwrap()
            .unwrap();
        assert!(row
            .metadata_json
            .as_deref()
            .unwrap()
            .contains("deferred_dispatch"));
    });
}
