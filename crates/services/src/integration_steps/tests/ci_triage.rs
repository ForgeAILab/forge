//! Head CI triage, shared red bases and failure messages.
use super::*;

/// Red results are shared only by the base-comparison identity, and never
/// certify the candidate. Both heads remain eligible to merge under full.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_ci_triage_two_tasks_share_red_base_and_no_pre_existing_send_back() {
    let world = World::new().await;
    sqlx::query("UPDATE project SET settings=json_set(settings,'$.review_ci_triage','full','$.default_review_config',json(?)) WHERE id='p'")
        .bind(json!({"setup_steps":["true"]}).to_string()).execute(world.db.pool()).await.unwrap();
    let production = Production::new(&world);
    for task in ["triage-one", "triage-two"] {
        let a = world.add_task(task, task, "one\n").await;
        review_ci(
            &world,
            task,
            &["printf 'test suite::base ... FAILED\\n'; exit 1"],
        )
        .await;
        let asked = world.place(&a.id, "checking", 1).await;
        world.ask(&asked, IntegrationStepAction::RequestCheck).await;
        eventually("triaged red head has a permit", || async {
            world.attempt(&a.id).await.permit_json.is_some()
        })
        .await;
        assert_eq!(world.task(task).await.status, "merging");
        assert!(world
            .step(task, IntegrationStepAction::SendBack)
            .await
            .is_empty());
    }
    let bases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run WHERE json_extract(input_json,'$.spec.commands[0].id')='triage-base:setup:0'").fetch_one(world.db.pool()).await.unwrap();
    assert_eq!(bases, 1);
    let candidate_passes: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM check_result WHERE certified=1")
            .fetch_one(world.db.pool())
            .await
            .unwrap();
    assert_eq!(
        candidate_passes, 0,
        "all candidate, rerun and base results really failed"
    );
    let item: String = sqlx::query_scalar(
        "SELECT details_json FROM attention_projection WHERE attention_type='base_check_failing'",
    )
    .fetch_one(world.db.pool())
    .await
    .unwrap();
    let item: Value = serde_json::from_str(&item).unwrap();
    assert_eq!(item["count"], 2);
    assert_eq!(item["task_ids"].as_array().unwrap().len(), 2);
    drop(production);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_ci_triage_flaky_queue_head_does_not_send_back() {
    let world = World::new().await;
    let a = world.add_task("flake-head", "candidate", "one\n").await;
    review_ci(
        &world,
        "flake-head",
        &["if [ ! -f .flake ]; then touch .flake; printf 'test flaky ... FAILED\\n'; exit 1; fi"],
    )
    .await;
    let production = Production::new(&world);
    let asked = world.place(&a.id, "checking", 1).await;
    world.ask(&asked, IntegrationStepAction::RequestCheck).await;
    eventually("flaky head has a permit", || async {
        world.attempt(&a.id).await.permit_json.is_some()
    })
    .await;
    assert!(world
        .step("flake-head", IntegrationStepAction::SendBack)
        .await
        .is_empty());
    assert_eq!(world.task("flake-head").await.status, "merging");
    drop(production);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_ci_triage_pass_is_not_reused_by_unchanged_queue_head() {
    let mut world = World::new().await;
    world.stop_steps().await;
    sqlx::query("UPDATE project SET primary_repo_id='r',settings=json_set(settings,'$.review_ci_triage','full','$.default_review_config',json(?)) WHERE id='p'")
        .bind(json!({"setup_steps":["true"]}).to_string()).execute(world.db.pool()).await.unwrap();
    world
        .legacy_task("triaged-review", "candidate", "review")
        .await;
    let execution = world
        .seed_execution("triaged-review", "executor", None)
        .await;
    review_ci(
        &world,
        "triaged-review",
        &["printf 'test suite::base ... FAILED\\n'; exit 1"],
    )
    .await;
    let production = Production::new(&world);
    let project = db::ProjectRepo::get_by_id(&*world.db, "p")
        .await
        .unwrap()
        .unwrap();
    let task = world.task("triaged-review").await;
    let workflow = crate::workflow::engine::WorkflowEngine::resolve_workflow_for_task(
        &task,
        &project.workflow_definition,
        &crate::worker_runtime::queue::cascade_actor(),
    );
    let state = workflow
        .states
        .iter()
        .find(|state| state.name == "review")
        .unwrap();
    let ctx = crate::workflow::HookContext {
        task_id: task.id.clone(),
        project_id: "p".into(),
        from_state: "in_progress".into(),
        to_state: "review".into(),
        db: world.db.clone(),
        event_bus: world.bus.clone(),
        gate_config: state.gate_config.clone(),
        state_config: crate::workflow::engine::effective_state_config(
            state,
            Some(&project),
            task.task_state_config.as_deref(),
        ),
        workflow: Arc::new(workflow),
        project_version: Some(project.version),
        project_workflow_definition: Some(project.workflow_definition),
        triggered_by: crate::worker_runtime::queue::cascade_actor(),
        merge_service: world.service.merge_service.clone(),
        cleanup_scheduler: None,
        task_service: world.service.clone(),
        daemon_connections: None,
        workspace_exec_locks: None,
        terminal_activity: None,
        workspace_root: world.temp.path().join("workspaces"),
        repo_cache_locks: None,
        workspace_backend_router: world.service.workspace_backend_router.clone(),
        workspace_id: Some("w-triaged-review".into()),
        agent_id: None,
        execution_id: Some(execution),
    };
    let ci = crate::workflow::actions::run_ci_steps_in_step(
        &ctx,
        &world.service.check_worker_or_embedded(),
    )
    .await;
    assert!(matches!(ci, crate::workflow::HookResult::Ok), "{ci:?}");
    assert!(world.db.review_ci_passed_by_triage(&task.id).await.unwrap());
    let runs_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(world.db.pool())
        .await
        .unwrap();
    assert_eq!(runs_before, 3);
    sqlx::query("UPDATE task SET status='merging',status_epoch=status_epoch+1,version=version+1 WHERE id='triaged-review'").execute(world.db.pool()).await.unwrap();
    let (stop, stopped) = tokio::sync::watch::channel(false);
    world.stop = stop;
    world.step_worker = Some(world.service.task_step_worker().start(stopped));
    let admitted = world
        .service
        .admit_to_integration_queue("triaged-review")
        .await
        .unwrap();
    let running = production.run();
    // The Task is done in the result step, before the worker completes
    // the attempt: wait for both before production stops.
    eventually("the triaged unchanged candidate merges", || async {
        world.task("triaged-review").await.status == "done"
            && world.attempt(&admitted.id).await.state == S::Completed
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let runs_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(world.db.pool())
        .await
        .unwrap();
    assert_eq!(
        runs_after,
        runs_before + 2,
        "head must run its own candidate and rerun; only the red base is shared"
    );
    assert_eq!(world.attempt(&admitted.id).await.state, S::Completed);
    assert!(world
        .step("triaged-review", IntegrationStepAction::SendBack)
        .await
        .is_empty());
}

/// The re-ask path applies stored evidence just like a delivery. Exercise
/// both triage phases without an owner worker so the terminal receipt can
/// be an actual infrastructure failure rather than a command exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_ci_triage_infrastructure_is_not_a_pending_verdict_or_send_back() {
    for phase in ["rerun", "base"] {
        let world = World::new().await;
        let a = world.add_task("infra-triage", "candidate", "one\n").await;
        sqlx::query("UPDATE project SET settings=json_set(settings,'$.default_review_config',json(?)) WHERE id='p'")
            .bind(json!({"setup_steps":["true"]}).to_string()).execute(world.db.pool()).await.unwrap();
        review_ci(
            &world,
            "infra-triage",
            &["printf 'test base ... FAILED\\n'; exit 1"],
        )
        .await;
        world.service.compose_checks(
            Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers()),
            Duration::from_secs(60),
            &crate::integration_worker::IntegrationWorkerConfig::default(),
        );
        let ask = |round| {
            let world = &world;
            let id = a.id.clone();
            async move {
                let asked = world.place(&id, "checking", round).await;
                world.ask(&asked, IntegrationStepAction::RequestCheck).await;
                eventually("triage request settles", || async {
                    let steps = world
                        .step("infra-triage", IntegrationStepAction::RequestCheck)
                        .await;
                    steps.len() == round as usize && steps.iter().all(|step| step.status == "done")
                })
                .await;
            }
        };
        let finish = |stage: &'static str, outcome: &'static str| {
            let world = &world;
            async move {
                let (consumer, run): (String, String) = sqlx::query_as("SELECT id,run_id FROM check_consumer WHERE task_id='infra-triage' AND request_key LIKE ?")
                    .bind(format!("%|{stage}%")).fetch_one(world.db.pool()).await.unwrap();
                let now = db::now_rfc3339();
                let result = format!("result-{stage}");
                let commands = json!([{"index":0,"command":"tests","exit_code":1,"stderr_tail":"","output_tail":"test base ... FAILED","failures":["base"],"started_at":now,"finished_at":now}]);
                sqlx::query("UPDATE check_run SET state='failed',finished_at=? WHERE id=?")
                    .bind(&now)
                    .bind(&run)
                    .execute(world.db.pool())
                    .await
                    .unwrap();
                sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) SELECT ?,id,identity_key,?,'not_performed',0,0,?,0,? FROM check_run WHERE id=?")
                    .bind(&result).bind(outcome).bind(commands.to_string()).bind(&now).bind(&run).execute(world.db.pool()).await.unwrap();
                sqlx::query("UPDATE check_consumer SET result_id=? WHERE id=?")
                    .bind(&result)
                    .bind(&consumer)
                    .execute(world.db.pool())
                    .await
                    .unwrap();
            }
        };
        ask(1).await;
        finish("integration:", "fail").await;
        ask(2).await;
        let round = if phase == "base" {
            finish("ci-triage:rerun:", "fail").await;
            ask(3).await;
            4
        } else {
            3
        };
        finish(
            if phase == "base" {
                "ci-triage:base:"
            } else {
                "ci-triage:rerun:"
            },
            "infrastructure_failed",
        )
        .await;
        ask(round).await;
        let attempt = world.attempt(&a.id).await;
        let answer = ack(&attempt, IntegrationStepAction::Settle)
            .expect("infrastructure is a terminal answer, not a pending wait");
        assert_eq!(
            answer.outcome,
            IntegrationStepOutcome::Infrastructure,
            "{phase}: {answer:?}"
        );
        assert!(answer.message.unwrap().contains("infrastructure"));
        assert_eq!(world.task("infra-triage").await.status, "merging");
        assert!(world
            .step("infra-triage", IntegrationStepAction::SendBack)
            .await
            .is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_ci_triage_base_attention_is_one_command_across_commits_and_resolves() {
    let world = World::new().await;
    sqlx::query("UPDATE project SET settings=json_set(settings,'$.default_review_config',json(?)) WHERE id='p'")
        .bind(json!({"setup_steps":["true"]}).to_string()).execute(world.db.pool()).await.unwrap();
    let production = Production::new(&world);
    let command = "if grep -q green base && [ ! -f candidate ]; then exit 0; fi; printf 'test base ... FAILED\\n'; exit 1";
    let mut item_id = None;
    let mut previous_commit = String::new();
    for (index, contents) in ["red-one\n", "red-two\n", "green\n"].iter().enumerate() {
        std::fs::write(world.repo().join("base"), contents).unwrap();
        git::commit_all(&world.repo(), contents).await.unwrap();
        let tip = git::get_current_sha(&world.repo()).await.unwrap();
        let task = format!("base-{index}");
        let a = world.add_task(&task, "candidate", "one\n").await;
        review_ci(&world, &task, &[command]).await;
        let asked = world.place(&a.id, "checking", 1).await;
        world.ask(&asked, IntegrationStepAction::RequestCheck).await;
        eventually("base triage settles", || async {
            ack(&world.attempt(&a.id).await, IntegrationStepAction::Settle).is_some()
        })
        .await;
        let rows: Vec<(String, String, String)> = sqlx::query_as("SELECT id,status,details_json FROM attention_projection WHERE attention_type='base_check_failing'")
            .fetch_all(world.db.pool()).await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "one item per Project and command across base commits"
        );
        let (id, status, details) = &rows[0];
        assert_eq!(item_id.get_or_insert_with(|| id.clone()), id);
        let details: Value = serde_json::from_str(details).unwrap();
        if index < 2 {
            assert_eq!(status, "open");
            assert_eq!(details["base_commit"], tip);
            assert_ne!(tip, previous_commit);
            previous_commit = tip;
        } else {
            assert_eq!(
                status, "resolved",
                "a newer passing base resolves the command's incident"
            );
        }
    }
    drop(production);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_ci_triage_project_assets_do_not_prevent_shared_red_base() {
    let world = World::new().await;
    let source = world.temp.path().join("assets-source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("input"), "asset-content").unwrap();
    let environment = api_types::ProjectEnvironment {
        assets: vec![api_types::EnvironmentAsset {
            source: source.to_str().unwrap().into(),
            target: "assets".into(),
        }],
        ..Default::default()
    };
    sqlx::query("UPDATE project SET settings=json_set(settings,'$.environment',json(?),'$.default_review_config',json(?)) WHERE id='p'")
        .bind(serde_json::to_string(&environment).unwrap()).bind(json!({"setup_steps":["test \"$(cat assets/input)\" = asset-content"]}).to_string()).execute(world.db.pool()).await.unwrap();
    let production = Production::new(&world);
    for task in ["assets-one", "assets-two"] {
        // Both candidates ignore their assets; the older base has no such
        // ignore rule. Its Forge-materialized assets still are declared inputs.
        let a = world.add_task(task, ".gitignore", "assets/\n").await;
        executors::environment::materialize_assets(&world.tree(task), &environment.assets)
            .await
            .unwrap();
        review_ci(&world, task, &["test \"$(cat assets/input)\" = asset-content || exit 2; printf 'test base ... FAILED\\n'; exit 1"]).await;
        let asked = world.place(&a.id, "checking", 1).await;
        world.ask(&asked, IntegrationStepAction::RequestCheck).await;
        eventually("asset head triaged", || async {
            world.attempt(&a.id).await.permit_json.is_some()
        })
        .await;
    }
    let bases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run WHERE json_extract(input_json,'$.spec.commands[0].id')='triage-base:setup:0'")
        .fetch_one(world.db.pool()).await.unwrap();
    assert_eq!(
        bases, 1,
        "materialized assets must not disable red comparison sharing"
    );
    drop(production);
}

/// A triaged red head names its blocking failures and keeps the end of the
/// failing output, as an untriaged one does: the head's send-back message is
/// the only evidence a MERGE-CI brief carries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_ci_triage_red_head_message_keeps_the_end_of_the_failing_output() {
    let world = World::new().await;
    let a = world.add_task("red-head", "candidate", "one\n").await;
    review_ci(
        &world,
        "red-head",
        &["for i in $(seq 1 80); do echo \"   Compiling noise-$i v0.1.0\"; done; printf 'test suite::introduced ... FAILED\\n'; echo \"assertion END-OF-OUTPUT-$((40+2))\"; exit 1"],
    )
    .await;
    let production = Production::new(&world);
    let asked = world.place(&a.id, "checking", 1).await;
    world.ask(&asked, IntegrationStepAction::RequestCheck).await;
    eventually("triaged red head is answered", || async {
        ack(&world.attempt(&a.id).await, IntegrationStepAction::Settle).is_some()
    })
    .await;
    let answer = ack(&world.attempt(&a.id).await, IntegrationStepAction::Settle).unwrap();
    assert_eq!(
        answer.outcome,
        IntegrationStepOutcome::CandidateCheckFailed,
        "{answer:?}"
    );
    let message = answer.message.unwrap();
    assert!(message.contains("set `setup_steps`"), "{message}");
    assert!(message.contains("assertion END-OF-OUTPUT-42"), "{message}");
    assert!(
        message.len() <= super::CI_MESSAGE_LIMIT,
        "{}",
        message.len()
    );
    drop(production);
}
