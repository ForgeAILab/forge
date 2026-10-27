//! Integration check verdicts, carry rules and review-entry evidence.
use super::*;

/// The merge-path consumer family, applied as the check's own delivery step
/// applies it (inside a claimed step of the Task).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_check_verdict_is_the_settle_answer_with_timing_for_the_waiting_worker() {
    use crate::check_runner::consumer::CheckConsumerFamily;
    let world = World::new().await;
    // This seam deliberately supplies scripted receipts, not runnable checks.
    // Exercise the raw verdict mapping under off; full triage uses real owners.
    sqlx::query(
        "UPDATE project SET settings=json_set(settings,'$.review_ci_triage','off') WHERE id='p'",
    )
    .execute(world.db.pool())
    .await
    .unwrap();
    world.service.compose_checks(
        Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers()),
        Duration::from_secs(60),
        &crate::integration_worker::IntegrationWorkerConfig::default(),
    );

    let family =
        IntegrationCheckFamily::new(Arc::new(IntegrationSteps::new(world.service.clone())));
    for (task, outcome, exit_code, expected) in [
        (
            "pass",
            db::CheckResultOutcome::Pass,
            0,
            IntegrationStepOutcome::Permit,
        ),
        (
            "red",
            db::CheckResultOutcome::Fail,
            2,
            IntegrationStepOutcome::CandidateCheckFailed,
        ),
        (
            "late",
            db::CheckResultOutcome::TimedOut,
            124,
            IntegrationStepOutcome::CandidateCheckFailed,
        ),
        // The run timed out after its last recorded step passed: no failing
        // step for triage to classify, and still no pass.
        (
            "silent",
            db::CheckResultOutcome::TimedOut,
            0,
            IntegrationStepOutcome::CandidateCheckFailed,
        ),
        (
            "lost",
            db::CheckResultOutcome::InfrastructureFailed,
            1,
            IntegrationStepOutcome::Infrastructure,
        ),
    ] {
        let a = world.add_task(task, task, "one\n").await;
        // The worker asked under effect_seq 2 and, after a takeover, waits
        // under effect_seq 3: the answer is for whoever waits now.
        let checking = world.place(&a.id, "checking", 3).await;
        assert_eq!(
            family.current_authority(task, 0).await.unwrap(),
            settle::check_authority(&checking)
        );
        assert_eq!(
            family.current_authority(task, 1).await.unwrap(),
            None,
            "another entry"
        );
        let delivery = application(&checking, outcome, exit_code);
        let lease = crate::test_support::TestTaskLease::claim(&world.db, task).await;
        lease.run(family.apply(&delivery)).await.unwrap();
        // Redelivery after a crash: the same answer, written once.
        let answered = world.attempt(&a.id).await;
        lease.run(family.apply(&delivery)).await.unwrap();
        assert_eq!(world.attempt(&a.id).await.revision, answered.revision);
        lease.release().await;

        let answer = ack(&answered, IntegrationStepAction::Settle).expect("settle answered");
        assert_eq!(answer.outcome, expected, "{task}");
        assert_eq!(
            (answer.effect_seq, answer.generation),
            (3, checking.slot_generation)
        );
        let permitted = expected == IntegrationStepOutcome::Permit;
        assert_eq!(answered.permit_json.is_some(), permitted, "{task}");
        assert_eq!(
            world.step(task, IntegrationStepAction::Result).await.len(),
            usize::from(permitted),
            "{task}"
        );
        match expected {
            IntegrationStepOutcome::Infrastructure => assert!(answer.check.is_none()),
            _ => assert_eq!(
                answer.check,
                Some(db::IntegrationCheckTiming::Ran {
                    slot_wait_ms: 3000,
                    run_ms: 7000
                })
            ),
        }
        if expected == IntegrationStepOutcome::CandidateCheckFailed {
            assert!(answer.message.unwrap().contains(if exit_code == 0 {
                "the check ended timed_out"
            } else {
                "`make test` exited"
            }));
        }
        assert_eq!(world.task(task).await.status, "merging");
    }
    // An attempt that is no longer checking has no authority: nothing applies.
    let a = world.add_task("gone", "gone", "one\n").await;
    let checking = world.place(&a.id, "checking", 1).await;
    let delivery = application(&checking, db::CheckResultOutcome::Pass, 0);
    world.set(&a.id, "state='parked'").await;
    assert_eq!(family.current_authority("gone", 0).await.unwrap(), None);
    let lease = crate::test_support::TestTaskLease::claim(&world.db, "gone").await;
    lease.run(family.apply(&delivery)).await.unwrap();
    lease.release().await;
    assert!(world.attempt(&a.id).await.acknowledged_at.is_none());
}

#[test]
fn the_shared_carry_rule_bounds_carries_and_keeps_the_reviewed_change_set() {
    use crate::workflow::actions::carry::{
        carry_budget_refusal, carry_path_refusal, gate_refuses_carry,
    };
    let limit = i64::from(db::budget::Kind::ReviewCarry.default_limit());
    assert!(carry_budget_refusal(limit - 1).is_none());
    assert!(carry_budget_refusal(limit)
        .unwrap()
        .contains("integrations were already carried under this review"));
    let reviewed = vec!["src/a.rs".to_owned(), "src/b.rs".to_owned()];
    assert!(carry_path_refusal(&["src/a.rs".to_owned()], &reviewed).is_none());
    assert_eq!(
        carry_path_refusal(&["src/a.rs".to_owned(), "src/c.rs".to_owned()], &reviewed).unwrap(),
        "`src/c.rs` is outside the reviewed change set"
    );
    assert!(gate_refuses_carry(None).is_none());
}

// ----- the real queue worker over this consumer --------------------------

/// The cost of the head check, measured. Each Task has a real `ci_steps`
/// command. (a) Three Tasks that each reach the head with the target where
/// their review left it merge with no head-check run at all. (b) Three Tasks
/// reviewed on one base, with the target moved once from outside: every one
/// of them is rebased (by the outside commit, then by the members ahead of
/// it) and checked exactly once: one run per rebased commit, none repeated.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn head_check_runs_are_zero_on_an_unchanged_target_and_one_per_rebased_task() {
    let world = World::new().await;
    let runs = || async {
        sqlx::query_as::<_, (String, String)>(
            "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id ORDER BY r.created_at",
        )
        .fetch_all(world.db.pool())
        .await
        .unwrap()
    };
    let production = Production::new(&world);
    let running = production.run();
    // (a) One at a time: each Task is cut from the tip it merges into.
    for name in ["a1", "a2", "a3"] {
        let file = format!("{name}.txt");
        world.add_task(name, &file, "x\n").await;
        review_ci(&world, name, &[&format!("test -f {file}")]).await;
        production.worker.notify_answered();
        eventually("an unrebased Task merges", || async {
            world.task(name).await.status == "done"
        })
        .await;
    }
    assert_eq!(runs().await, vec![], "no head check on an unchanged target");

    // (b) Three Tasks on one base, and the target moves once.
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let mut attempts = Vec::new();
    for name in ["b1", "b2", "b3"] {
        let file = format!("{name}.txt");
        attempts.push(world.add_task(name, &file, "x\n").await);
        review_ci(
            &world,
            name,
            &[&format!("test -f {file}"), "test -f outside.txt"],
        )
        .await;
    }
    std::fs::write(world.repo().join("outside.txt"), "outside\n").unwrap();
    git::commit_all(&world.repo(), "outside").await.unwrap();
    let production = Production::new(&world);
    let running = production.run();
    eventually("the rebased Tasks merge", || async {
        let mut done = true;
        for name in ["b1", "b2", "b3"] {
            done &= world.task(name).await.status == "done";
        }
        done
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    let ran = runs().await;
    let mut expected = Vec::new();
    for attempt in &attempts {
        let merged = world.attempt(&attempt.id).await;
        assert_ne!(merged.integrated_sha, merged.original_candidate_sha);
        expected.push((merged.integrated_sha.clone().unwrap(), "pass".to_owned()));
    }
    let mut sorted = ran.clone();
    sorted.sort();
    expected.sort();
    assert_eq!(
        sorted, expected,
        "exactly one run per rebased Task: {ran:?}"
    );
}

/// D2 checklist item 11. Three Tasks through the real worker, the production
/// ports and the real check runner on a server-owned target, each with a real
/// `ci_steps` command. The first merges as reviewed (no check: the target did
/// not move). The second is rebased by the queue and its command passes only
/// on the rebased commit: green, merged. The third is rebased and its command
/// exits non-zero: red, ejected, sent back, never merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn real_ci_steps_decide_the_head_through_the_production_ports() {
    let world = World::new().await;
    let first = world.add_task("one", "one.txt", "one\n").await;
    let green = world.add_task("two", "two.txt", "two\n").await;
    let red = world.add_task("three", "three.txt", "three\n").await;
    review_ci(&world, "one", &["test -f one.txt"]).await;
    // Passes only on top of the first Task's commit: the rebased commit.
    review_ci(&world, "two", &["test -f one.txt", "test -f two.txt"]).await;
    review_ci(&world, "three", &["test -f two.txt", "exit 3"]).await;
    let production = Production::new(&world);
    let running = production.run();
    eventually("the first two merge and the third is sent back", || async {
        world.task("one").await.status == "done"
            && world.task("two").await.status == "done"
            && world.attempt(&red.id).await.state == S::Ejected
            && world.task("three").await.status != "merging"
    })
    .await;
    eventually("the merged attempts complete", || async {
        world.attempt(&first.id).await.state == S::Completed
            && world.attempt(&green.id).await.state == S::Completed
    })
    .await;
    let _ = production.stop.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;

    let (one, two, three) = (
        world.attempt(&first.id).await,
        world.attempt(&green.id).await,
        world.attempt(&red.id).await,
    );
    // Git: main holds the first two, the second rebased; never the third.
    let tip = git_out(&world.repo(), &["rev-parse", "refs/heads/main"])
        .await
        .unwrap();
    assert_eq!(two.integrated_sha.as_deref(), Some(tip.as_str()));
    assert_eq!(one.integrated_sha, one.original_candidate_sha);
    assert_ne!(two.integrated_sha, two.original_candidate_sha);
    for (file, present) in [("one.txt", true), ("two.txt", true), ("three.txt", false)] {
        assert_eq!(
            git_out(&world.repo(), &["cat-file", "-e", &format!("{tip}:{file}")])
                .await
                .is_some(),
            present,
            "{file}"
        );
    }
    // One primary run per rebased commit, plus the default-full rerun of
    // the red step. No check runs for the Task that merged as reviewed.
    let runs: Vec<(String, String)> = sqlx::query_as(
        "SELECT r.commit_sha, s.outcome FROM check_run r JOIN check_result s ON s.run_id=r.id ORDER BY r.created_at",
    )
    .fetch_all(world.db.pool())
    .await
    .unwrap();
    assert_eq!(
        runs,
        vec![
            (two.candidate_sha.clone().unwrap(), "pass".to_owned()),
            (three.candidate_sha.clone().unwrap(), "fail".to_owned()),
            (three.candidate_sha.clone().unwrap(), "fail".to_owned()),
        ],
        "{runs:?}"
    );
    assert!(matches!(
        two.phase_timings
            .as_ref()
            .and_then(|timings| timings.check.clone()),
        Some(db::IntegrationCheckTiming::Ran { .. })
    ));
    assert_eq!(
        three.failure_kind,
        Some(IntegrationFailureKind::CandidateCheckFailed)
    );
    assert!(
        three
            .failure_message
            .as_deref()
            .unwrap()
            .contains("exited 3"),
        "{:?}",
        three.failure_message
    );
    assert!(world
        .step("one", IntegrationStepAction::RequestCheck)
        .await
        .is_empty());
    // The red Task is told why, by its `send_back` step.
    assert_eq!(
        world
            .step("three", IntegrationStepAction::SendBack)
            .await
            .len(),
        1
    );
    assert!(world
        .db
        .integration_queue(three.queue_id.as_deref().unwrap())
        .await
        .unwrap()
        .unwrap()
        .head_attempt_id
        .is_none());
}

/// A check that ended "no verdict after its retries" says nothing about the
/// commit. The round that waited for it is told so (and parks, counted). A
/// later round of the same commit must not read that stored answer again:
/// it would park on every retry and end at the owner without the check ever
/// running again. The new round's ask re-arms the consumer (a fresh
/// infrastructure budget, a new run) and waits. Only an answer whose cleanup
/// is unproven still stands, which the counted retries bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_no_verdict_answer_is_not_kept_for_the_commit_and_a_new_round_asks_again() {
    let world = World::new().await;
    let a = world.add_task("one", "one.txt", "one\n").await;
    review_ci(&world, "one", &["true"]).await;
    world.service.compose_checks(
        Arc::new(crate::daemon_transport::DaemonConnectionRegistry::without_handlers()),
        Duration::from_secs(60),
        &crate::integration_worker::IntegrationWorkerConfig::default(),
    );
    let asked = |round: i64| {
        let world = &world;
        let id = a.id.clone();
        async move {
            let placed = world.place(&id, "checking", round).await;
            world
                .ask(&placed, IntegrationStepAction::RequestCheck)
                .await;
            eventually("the request_check step to finish", || async {
                let steps = world.step("one", IntegrationStepAction::RequestCheck).await;
                steps.len() as i64 == round && steps.iter().all(|step| step.status == "done")
            })
            .await;
            world.attempt(&id).await
        }
    };
    // Round 1 asks; the run exists and nothing has answered yet.
    let first = asked(1).await;
    assert_eq!(first.state, S::Checking);
    assert!(ack(&first, IntegrationStepAction::Settle).is_none());
    let (consumer, run): (String, String) =
        sqlx::query_as("SELECT id,run_id FROM check_consumer WHERE task_id='one'")
            .fetch_one(world.db.pool())
            .await
            .unwrap();
    // The run ends without a verdict after its retries, and that answer was
    // delivered and applied (round 1 parked on it).
    let now = db::now_rfc3339();
    let step = world.step("one", IntegrationStepAction::RequestCheck).await[0]
        .id
        .clone();
    let exhaust = |cleanup: &'static str, result: &'static str| {
        let (world, run, consumer, now, step) = (
            &world,
            run.clone(),
            consumer.clone(),
            now.clone(),
            step.clone(),
        );
        async move {
            let pool = world.db.pool();
            sqlx::query("UPDATE check_run SET state='failed',lease_owner=NULL,lease_until=NULL,finished_at=? WHERE id=?")
                .bind(&now).bind(&run).execute(pool).await.unwrap();
            sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) SELECT ?,id,identity_key,'infrastructure_failed',?,0,0,'[]',0,? FROM check_run WHERE id=?")
                .bind(result).bind(cleanup).bind(&now).bind(&run).execute(pool).await.unwrap();
            sqlx::query("UPDATE check_consumer SET run_id=?,result_id=?,infrastructure_retries=2,delivery_step_id=?,applied_at=? WHERE id=?")
                .bind(&run).bind(result).bind(&step).bind(&now).bind(&consumer).execute(pool).await.unwrap();
        }
    };
    exhaust("not_performed", "no-verdict-1").await;

    // Round 2, same commit: asked again, not answered from the stored row.
    let second = asked(2).await;
    assert_eq!(second.candidate_sha, first.candidate_sha);
    assert_eq!(second.state, S::Checking);
    assert!(
        ack(&second, IntegrationStepAction::Settle).is_none(),
        "{:?}",
        second.effect_ack_json
    );
    let (retries, delivery, applied): (i64, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT infrastructure_retries,delivery_step_id,applied_at FROM check_consumer WHERE id=?",
    )
    .bind(&consumer)
    .fetch_one(world.db.pool())
    .await
    .unwrap();
    assert_eq!((retries, delivery, applied), (0, None, None));
    assert_eq!(
        world
            .scalar::<i64>("SELECT COUNT(*) FROM check_consumer WHERE task_id=?", "one")
            .await,
        1
    );

    // An exhausted answer whose cleanup is unproven is not run again from
    // here: it stands as "no verdict" for this round too.
    exhaust("uncertain", "no-verdict-2").await;
    let third = asked(3).await;
    let answer = ack(&third, IntegrationStepAction::Settle).expect("answered");
    assert_eq!(answer.outcome, IntegrationStepOutcome::Infrastructure);
}

/// The server's login shell gives no environment: review entry cannot
/// identify its check. Its first attempts are the hooks step's own (the
/// opened attempt and the checkpoint stay, nothing is shown). After
/// `ENVIRONMENT_ATTEMPTS` the Task states the review-CI infrastructure
/// interruption with its cause and a deferred re-entry; the unstarted Review
/// attempt is discarded, and no check, consumer or failed Review exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_review_entry_without_an_environment_answer_retries_then_states_the_interruption() {
    // The workflow's own entry, and the entry the owner's Retry of the
    // review asks for: both land the same way.
    for owner_asked in [false, true] {
        review_entry_without_an_environment_answer(owner_asked).await;
    }
}

/// Run real review-entry CI under the hooks lease, then enter the queue.
/// The unchanged head must get a real Hit; changing the target changes the
/// rebased commit and must produce one new run and Review evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn entry_ci_and_head_ci_share_identity_reuse_a_real_hit_and_record_review_evidence() {
    for moved in [false, true] {
        let mut world = World::new().await;
        sqlx::query("CREATE TABLE test_check_identity_inputs(input_json TEXT NOT NULL)")
            .execute(world.db.pool())
            .await
            .unwrap();
        let _ = world.stop.send(true);
        tokio::time::timeout(Duration::from_secs(10), world.step_worker.take().unwrap())
            .await
            .unwrap()
            .unwrap();
        sqlx::query("UPDATE project SET primary_repo_id='r' WHERE id='p'")
            .execute(world.db.pool())
            .await
            .unwrap();
        let original = world
            .legacy_task("evidence", "evidence.txt", "review")
            .await;
        let execution = world.seed_execution("evidence", "executor", None).await;
        let counter = world.temp.path().join("check-executions");
        let command = format!("echo run >> '{}'", counter.display());
        review_ci(&world, "evidence", &[&command]).await;
        let production = Production::new(&world);
        let project = db::ProjectRepo::get_by_id(&*world.db, "p")
            .await
            .unwrap()
            .unwrap();
        let task = world.task("evidence").await;
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
            workspace_id: Some("w-evidence".into()),
            agent_id: None,
            execution_id: Some(execution),
        };
        let ci = crate::workflow::actions::run_ci_steps_in_step(
            &ctx,
            &world.service.check_worker_or_embedded(),
        )
        .await;
        assert!(matches!(ci, crate::workflow::HookResult::Ok), "{ci:?}");
        let entry: (String,String,String) = sqlx::query_as("SELECT c.result_id,c.identity_key,r.commit_sha FROM check_consumer c JOIN check_run r ON r.id=c.run_id WHERE c.task_id='evidence' AND c.origin='entry'")
            .fetch_one(world.db.pool()).await.unwrap();
        assert_eq!(entry.2, original);
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            1
        );
        sqlx::query("UPDATE task SET status='merging',status_epoch=status_epoch+1,version=version+1 WHERE id='evidence'")
            .execute(world.db.pool()).await.unwrap();
        if moved {
            std::fs::write(world.repo().join("outside"), "moved\n").unwrap();
            git::commit_all(&world.repo(), "target moved after entry CI")
                .await
                .unwrap();
        }
        let (stop_steps, stopped_steps) = tokio::sync::watch::channel(false);
        world.stop = stop_steps;
        world.step_worker = Some(world.service.task_step_worker().start(stopped_steps));
        let admitted = world
            .service
            .admit_to_integration_queue("evidence")
            .await
            .unwrap();
        let running = production.run();
        eventually("the checked Task merges", || async {
            world.task("evidence").await.status == "done"
        })
        .await;
        let _ = production.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
        let row: String = sqlx::query_scalar("SELECT step_results_json FROM review WHERE task_id='evidence' ORDER BY attempt_number DESC LIMIT 1")
            .fetch_one(world.db.pool()).await.unwrap();
        let details: Value = serde_json::from_str(&row).unwrap();
        let merged = world.attempt(&admitted.id).await;
        let evidence = &details["execution"]["integration_checks"][0];
        // What the step wrote is what the reviews endpoint parses.
        let typed: api_types::ReviewDetails = serde_json::from_str(&row).unwrap();
        if typed.execution.is_none() {
            let probes = crate::check_runner::policy::probe_log::describe(&world.db).await;
            let identities: Vec<String> = sqlx::query_scalar(
                "SELECT input_json FROM test_check_identity_inputs ORDER BY rowid",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            let identities: Vec<Value> = identities
                .iter()
                .map(|raw| serde_json::from_str(raw).unwrap())
                .collect();
            let differing_fields: Vec<_> = identities
                .windows(2)
                .flat_map(|pair| {
                    let mut fields = Vec::new();
                    for field in ["project_id", "repo_id", "commit_sha"] {
                        if pair[0][field] != pair[1][field] {
                            fields.push(field.to_owned());
                        }
                    }
                    for field in [
                        "spec",
                        "environment",
                        "environment_identity",
                        "execution_revision",
                    ] {
                        if pair[0]["inputs"][field] != pair[1]["inputs"][field] {
                            fields.push(format!("inputs.{field}"));
                        }
                    }
                    fields
                })
                .collect();
            let reviews: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT id,status,step_results_json FROM review WHERE task_id='evidence' ORDER BY attempt_number",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            /// Origin, request key, run, result, cancelled at.
            type Consumer = (
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
            );
            let consumers: Vec<Consumer> = sqlx::query_as(
                "SELECT origin,request_key,run_id,result_id,cancelled_at FROM check_consumer WHERE task_id='evidence' ORDER BY created_at",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            let steps: Vec<(String, String, Option<String>, i64)> = sqlx::query_as(
                "SELECT kind,status,last_error,attempts FROM task_step WHERE task_id='evidence' ORDER BY created_at",
            )
            .fetch_all(world.db.pool())
            .await
            .unwrap();
            panic!(
                "moved={moved}: no head-check evidence\ndiffering fields {differing_fields:?}\nidentity inputs {identities:#?}\nprobes {probes}\nreviews {reviews:?}\nconsumers {consumers:?}\nsteps {steps:?}\nattempt ack {:?} timings {:?}",
                merged.effect_ack_json, merged.phase_timings
            );
        }
        assert_eq!(typed.execution.unwrap().integration_checks.len(), 1);
        // No wait was stated for the synchronous reuse and none is left.
        let reviews: i64 = world
            .scalar("SELECT COUNT(*) FROM review WHERE task_id=?", "evidence")
            .await;
        assert_eq!(reviews, 1, "a head check never creates a Review");
        assert_eq!(evidence["attempt_id"], admitted.id);
        assert!(!evidence["ci_steps"].as_array().unwrap().is_empty());
        assert_eq!(
            evidence["commit_sha"].as_str(),
            merged.integrated_sha.as_deref()
        );
        if moved {
            assert_ne!(evidence["commit_sha"], original);
            assert_ne!(evidence["result_id"], entry.0);
            assert_eq!(evidence["source"], "executed");
        } else {
            assert_eq!(evidence["identity_key"], entry.1);
            assert_eq!(evidence["result_id"], entry.0);
            assert_eq!(evidence["source"], "cache_hit", "{details}");
            // The reuse was applied by the `settle` step that recorded it:
            // its consumer is answered and no delivery step is spent on it,
            // now or at a later look of the check worker.
            let _ = db::CheckDeliveryRepo::enqueue_check_result_steps(&*world.db, 100)
                .await
                .unwrap();
            let head: (Option<String>, Option<String>, i64) = sqlx::query_as(
                "SELECT c.applied_at,c.delivery_step_id,(SELECT COUNT(*) FROM task_step s WHERE s.task_id=c.task_id AND s.causation_key LIKE 'check-result:'||c.id||':%') FROM check_consumer c WHERE c.task_id='evidence' AND c.origin='integration'",
            )
            .fetch_one(world.db.pool())
            .await
            .unwrap();
            assert!(head.0.is_some(), "the head's reuse is created answered");
            assert_eq!((head.1, head.2), (None, 0), "no delivery step for a reuse");
        }
        let runs: i64 = world.scalar("SELECT COUNT(*) FROM check_run WHERE id IN(SELECT run_id FROM check_consumer WHERE task_id=?)", "evidence").await;
        // Both consumers of a hit reference the one entry run. A rebased
        // commit has the entry run and exactly one new head run.
        assert_eq!(runs, if moved { 2 } else { 1 });
        assert_eq!(
            std::fs::read_to_string(&counter).unwrap().lines().count(),
            if moved { 2 } else { 1 }
        );
        let probes = crate::check_runner::policy::probe_log::count(&world.db).await;
        assert_eq!(probes, 1, "moved={moved}: review entry + merge probe once");
        eprintln!("moved={moved}: review entry + merge probes={probes}, check runs={runs}");
    }
}
