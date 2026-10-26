use crate::worker_runtime::queue::lane_conditions::Fixture;
use db::TaskStepRepo;

#[tokio::test]
async fn blocked_entry_barrier() {
    for target in ["in_progress", "review"] {
        let _clock = db::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap());
        let f = Fixture::new(if target == "review" {
            "in_progress"
        } else {
            "todo"
        })
        .await;
        let mut workflow = (*f.ctx.workflow).clone();
        for state in &mut workflow.states {
            state.hooks = Default::default();
        }
        let state = workflow
            .states
            .iter_mut()
            .find(|state| state.name == target)
            .unwrap();
        if let Some(gate) = state.gate_config.as_mut() {
            gate.reject_target = None;
        }
        state.hooks.before_enter.push(api_types::HookSpec {
            action: "require_clean_worktree".into(),
            params: serde_json::json!({}),
            applies_to: api_types::HookAudience::All,
            on_failure: api_types::FailurePolicy::Block,
        });
        let engine = super::WorkflowEngine::new(f.db.clone(), f.ctx.event_bus.clone());
        db::task_writer::in_task_step(f.step.clone(), async {
            engine
                .bind(&f.ctx.task_service)
                .transition(
                    &f.task.id,
                    target,
                    f.task.version,
                    &workflow,
                    &api_types::Actor::system(api_types::SystemComponent::Workflow),
                    "entry",
                    false,
                    Default::default(),
                )
                .await
                .unwrap();
            sqlx::query(
                "UPDATE task_step SET status='done',lease_until=NULL,claimed_by=NULL WHERE id=?",
            )
            .bind(&f.step.id)
            .execute(f.db.pool())
            .await
            .unwrap();
        })
        .await;
        let step =
            f.db.claim_step("lane-entry", Some(&f.task.id), "2099-01-01T00:00:00Z")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(step.kind, "hooks");
        f.db.start_hook(&step, 0).await.unwrap();
        f.db.finish_hook(
            &step,
            0,
            &serde_json::to_string(&crate::workflow::HookResult::Failed {
                reason: "entry failed".into(),
            })
            .unwrap(),
        )
        .await
        .unwrap();
        db::task_writer::in_task_step(step.clone(), async {
            engine
                .bind(&f.ctx.task_service)
                .execute_hook_step(
                    &step,
                    &serde_json::from_str::<super::HookPayload>(&step.payload_json).unwrap(),
                )
                .await
                .unwrap();
            let row = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
                .await
                .unwrap()
                .unwrap();
            assert!(row.entry_barrier_json.is_some());
            f.emit(&format!("durable-barrier-{target}")).await;
        })
        .await;
    }
}
