//! Canonical check identities, environment revisions, and reusable receipts.

use super::*;

#[tokio::test]
async fn a_second_request_for_an_unchanged_commit_and_spec_runs_nothing() {
    let f = canonical_fixture().await;
    let step = f.counting_step();
    let first = f.ask(f.request("review-1", &step).await).await;
    assert!(first.consumer.result_id.is_none());
    let run = scheduled(first);
    assert!(run.cacheable);
    let result = f.drive(&run).await;
    assert_eq!(result.outcome, CheckResultOutcome::Pass);
    assert!(result.certified && result.cacheable);
    assert_eq!(f.executions(), 1);

    // The re-review: same commit, same spec, same environment, same worktree.
    let again = f.ask(f.request("review-2", &step).await).await;
    assert!(
        matches!(&again.outcome, CheckRequestOutcome::Hit(hit) if hit.id == result.id),
        "an unchanged commit and spec must reuse the stored result"
    );
    assert_eq!(
        again.consumer.result_id.as_deref(),
        Some(result.id.as_str())
    );
    // Measured: zero executor invocations, no second run row, no slot.
    assert_eq!(f.executions(), 1);
    assert_eq!(f.runs().await, 1);
    assert_eq!(f.store.check_run_counts().await.unwrap().admitted_runs, 0);
    // Another Task's worktree at that commit is another identity: the result
    // was produced with this worktree's ignored files, not that one's.
    let other = f.request_in("review-other", "w2", &[&step]).await;
    assert_ne!(
        other.identity.key().unwrap(),
        f.request("review-3", &step).await.identity.key().unwrap()
    );
    assert!(matches!(
        f.ask(other).await.outcome,
        CheckRequestOutcome::Scheduled(_)
    ));
    assert_eq!(f.executions(), 1);
}

#[tokio::test]
async fn two_requests_for_one_worktree_commit_and_spec_share_one_run() {
    let f = canonical_fixture().await;
    let step = f.counting_step();
    let run = scheduled(f.ask(f.request("first", &step).await).await);
    let joined = f.ask(f.request("second", &step).await).await;
    assert!(matches!(&joined.outcome, CheckRequestOutcome::Joined(same) if same.id == run.id));
    let result = f.drive(&run).await;
    assert_eq!(f.executions(), 1);
    let attached: Vec<Option<String>> =
        sqlx::query_scalar("SELECT result_id FROM check_consumer ORDER BY created_at")
            .fetch_all(f.store.pool())
            .await
            .unwrap();
    assert_eq!(attached, [Some(result.id.clone()), Some(result.id)]);
}

#[tokio::test]
async fn nothing_is_reused_when_the_commit_spec_environment_or_policy_differs() {
    let f = canonical_fixture().await;
    let step = f.counting_step();
    f.set_project_value("vanilla").await;
    let base = f.request("base", &step).await;
    let base_key = base.identity.key().unwrap();
    let result = f.drive(&scheduled(f.ask(base.clone()).await)).await;
    assert!(result.cacheable);
    assert_eq!(f.executions(), 1);
    let scheduled_fresh = |reply: RequestedCheck| {
        assert!(
            matches!(reply.outcome, CheckRequestOutcome::Scheduled(_)),
            "a different identity must run its own check"
        );
        scheduled(reply)
    };

    // Another spec: one more command character.
    let spec = f.request("spec", &format!("{step}; true")).await;
    assert_ne!(spec.identity.key().unwrap(), base_key);
    let run = scheduled_fresh(f.ask(spec).await);
    assert!(f.drive(&run).await.cacheable);
    assert_eq!(f.executions(), 2);

    // Another Project value.
    f.set_project_value("chocolate").await;
    let value = f.request("value", &step).await;
    assert_ne!(value.identity.key().unwrap(), base_key);
    let run = scheduled_fresh(f.ask(value).await);
    assert!(f.drive(&run).await.cacheable);
    assert_eq!(f.executions(), 3);
    f.set_project_value("vanilla").await;

    // Another value of a variable the steps inherit from the owner.
    std::env::set_var(INHERITED, "two");
    // An owner restart observes the changed inherited inputs in a new
    // generation; identity builds do not re-probe inside a generation.
    super::policy::renew_environment_generation_for_test(&f.store)
        .await
        .unwrap();
    let inherited = f.request("inherited", &step).await;
    assert_ne!(inherited.identity.key().unwrap(), base_key);
    let run = scheduled_fresh(f.ask(inherited).await);
    assert!(f.drive(&run).await.cacheable);
    assert_eq!(f.executions(), 4);
    std::env::set_var(INHERITED, "one");
    super::policy::renew_environment_generation_for_test(&f.store)
        .await
        .unwrap();

    // An environment identity this owner does not have (a request stated
    // before a restart into another environment). The run still executes and
    // answers its consumer; it attests nothing, so it is never reused.
    let mut env = base.clone();
    env.request_key = "environment".into();
    env.identity.inputs.environment_identity = CheckEnvironmentIdentity::Attested {
        input_digest: "b".repeat(64),
    };
    assert_ne!(env.identity.key().unwrap(), base_key);
    let run = scheduled_fresh(f.ask(env.clone()).await);
    let foreign = f.drive(&run).await;
    assert_eq!(foreign.outcome, CheckResultOutcome::Pass);
    assert!(foreign.certified && !foreign.cacheable);
    assert_eq!(f.executions(), 5);
    env.request_key = "environment-again".into();
    scheduled_fresh(f.ask(env).await);

    // The frozen policy: never reusable, and never served by a canonical pass.
    let mut frozen = base.clone();
    frozen.request_key = "frozen".into();
    frozen.identity.inputs.spec.execution_policy = "legacy-server/1".into();
    frozen.identity.inputs.spec.commands[0].cacheability = CheckCacheability::Uncacheable;
    assert_ne!(frozen.identity.key().unwrap(), base_key);
    assert!(!scheduled_fresh(f.ask(frozen).await).cacheable);

    // Another commit.
    fixture_git(
        &f.checkout,
        &["commit", "-q", "--allow-empty", "-m", "next"],
    )
    .await;
    let mut commit = f.request("commit", &step).await;
    commit.identity.commit_sha = git::get_current_sha(&f.checkout).await.unwrap();
    assert_ne!(commit.identity.key().unwrap(), base_key);
    scheduled_fresh(f.ask(commit).await);

    // And the unchanged request is still a hit.
    let mut same = base;
    same.request_key = "same".into();
    assert!(matches!(
        f.ask(same).await.outcome,
        CheckRequestOutcome::Hit(_)
    ));
}

/// Decision A. A step reads a variable it only inherits from the owner; the
/// environment moving between the request and the run is neither a refusal
/// nor a wrong hit.
#[tokio::test]
async fn a_step_inherits_the_owner_environment_and_a_moved_environment_is_only_a_miss() {
    let f = canonical_fixture().await;
    let reads = format!("{}; test \"${INHERITED}\" = one", f.counting_step());
    let result = f
        .drive(&scheduled(f.ask(f.request("reads", &reads).await).await))
        .await;
    assert_eq!(result.outcome, CheckResultOutcome::Pass);
    assert!(result.cacheable);
    // The owner's environment changes while a run is queued.
    let step = format!("{}; test -n \"${INHERITED}\"", f.counting_step());
    let queued = scheduled(f.ask(f.request("queued", &step).await).await);
    assert!(queued.cacheable);
    std::env::set_var(INHERITED, "two");
    super::policy::renew_environment_generation_for_test(&f.store)
        .await
        .unwrap();
    let verdict = f.drive(&queued).await;
    assert_eq!(verdict.outcome, CheckResultOutcome::Pass);
    assert!(verdict.certified && !verdict.cacheable);
    // Neither environment is answered by it.
    let now = f.ask(f.request("after", &step).await).await;
    assert!(matches!(now.outcome, CheckRequestOutcome::Scheduled(_)));
    std::env::set_var(INHERITED, "one");
    super::policy::renew_environment_generation_for_test(&f.store)
        .await
        .unwrap();
    let back = f.ask(f.request("back", &step).await).await;
    assert!(matches!(&back.outcome, CheckRequestOutcome::Scheduled(run) if run.id != queued.id));
    assert_eq!(f.executions(), 2);

    // A Project value edited while a run is queued: the same.
    let f2_step = format!("{}; test \"$FLAVOR\" = chocolate", f.counting_step());
    f.set_project_value("vanilla").await;
    let queued = scheduled(f.ask(f.request("project", &f2_step).await).await);
    f.set_project_value("chocolate").await;
    let verdict = f.drive(&queued).await;
    assert_eq!(verdict.outcome, CheckResultOutcome::Pass);
    assert!(verdict.certified && !verdict.cacheable);
}

#[tokio::test]
async fn a_run_that_dirtied_the_tree_or_timed_out_is_a_verdict_but_never_reused() {
    // A step that rewrites a tracked file: today's verdict, not reusable.
    let f = canonical_fixture().await;
    let step = format!("{}; echo two >> tracked", f.counting_step());
    let run = scheduled(f.ask(f.request("dirty-1", &step).await).await);
    assert!(run.cacheable);
    let result = f.drive(&run).await;
    assert_eq!(result.outcome, CheckResultOutcome::Pass);
    assert!(result.certified && !result.cacheable);
    assert_eq!(
        f.store.check_run(&run.id).await.unwrap().unwrap().state,
        CheckRunState::Succeeded
    );
    // The consumer that asked is answered by its own run, never as a hit.
    let repeat = f.ask(f.request("dirty-1", &step).await).await;
    assert!(matches!(&repeat.outcome, CheckRequestOutcome::Joined(same) if same.id == run.id));
    assert_eq!(
        repeat.consumer.result_id.as_deref(),
        Some(result.id.as_str())
    );
    // A new consumer runs it again (the worktree is still dirty: nor is
    // that one reusable).
    let next = f.ask(f.request("dirty-2", &step).await).await;
    let next = scheduled(next);
    assert!(!f.drive(&next).await.cacheable);
    assert_eq!(f.executions(), 2);
    assert_eq!(
        f.store.check_run_counts().await.unwrap().reusable_results,
        0
    );

    // A run stopped by its wall limit.
    let f = canonical_fixture_after(f).await;
    let step = format!("{}; sleep 30", f.counting_step());
    let mut slow = f.request("slow-1", &step).await;
    slow.wall_timeout_seconds = 1;
    let result = f.drive(&scheduled(f.ask(slow).await)).await;
    assert_eq!(result.outcome, CheckResultOutcome::TimedOut);
    assert!(!result.certified && !result.cacheable);
    let again = f.ask(f.request("slow-2", &step).await).await;
    assert!(matches!(again.outcome, CheckRequestOutcome::Scheduled(_)));
}

/// The run executes in the Task worktree. A file the commit does not contain
/// and Git does not ignore can change the result, so a run that started with
/// one is not reusable. Ignored files are the worktree's own state: they do
/// not block reuse, and the result is only ever this worktree's.
#[tokio::test]
async fn a_run_that_started_with_an_untracked_file_is_a_verdict_but_never_reused() {
    let f = canonical_fixture().await;
    let step = f.counting_step();
    std::fs::create_dir(f.checkout.join("build")).unwrap();
    std::fs::write(f.checkout.join("build/output"), "ignored").unwrap();
    let clean = f
        .drive(&scheduled(f.ask(f.request("ignored", &step).await).await))
        .await;
    assert!(clean.certified && clean.cacheable);

    std::fs::write(f.checkout.join("leftover"), "untracked").unwrap();
    let step = format!("{step}; true");
    let run = scheduled(f.ask(f.request("untracked-1", &step).await).await);
    let result = f.drive(&run).await;
    assert_eq!(result.outcome, CheckResultOutcome::Pass);
    assert!(result.certified && !result.cacheable);
    let next = f.ask(f.request("untracked-2", &step).await).await;
    assert!(matches!(next.outcome, CheckRequestOutcome::Scheduled(_)));
}

/// A red result answers the request that produced it and nobody else: a
/// re-review (a new request) of the same commit runs again. A pass whose
/// process tree was not verified stopped is not reusable either.
#[tokio::test]
async fn a_red_result_and_an_unstopped_tree_are_never_reused() {
    let f = canonical_fixture().await;
    let step = format!("{}; false", f.counting_step());
    let run = scheduled(f.ask(f.request("red-1", &step).await).await);
    let red = f.drive(&run).await;
    assert_eq!(red.outcome, CheckResultOutcome::Fail);
    assert!(!red.certified && !red.cacheable);
    // The same request again is answered by its own run, not as a hit.
    let repeat = f.ask(f.request("red-1", &step).await).await;
    assert!(matches!(&repeat.outcome, CheckRequestOutcome::Joined(same) if same.id == run.id));
    assert_eq!(repeat.consumer.result_id.as_deref(), Some(red.id.as_str()));
    // The re-review runs.
    let again = f.ask(f.request("red-2", &step).await).await;
    assert!(matches!(again.outcome, CheckRequestOutcome::Scheduled(_)));
    assert_eq!(f.executions(), 1);

    // A complete passing receipt is reusable evidence; the same receipt with
    // one step's tree not verified stopped is a verdict only.
    let step = f.counting_step();
    let run = scheduled(f.ask(f.request("tree", &step).await).await);
    assert!(f.drive(&run).await.cacheable);
    let record = f.store.check_worker_record(&run.id).await.unwrap();
    let intent = record.dispatch.clone().unwrap();
    let mut receipt = record.receipt.clone().unwrap();
    let evidence = super::receipt::validate_receipt(&record.run, &intent, &receipt).unwrap();
    assert!(evidence.reusable);
    receipt.commands[0].process_tree_stopped = false;
    let evidence = super::receipt::validate_receipt(&record.run, &intent, &receipt).unwrap();
    assert_eq!(evidence.outcome, CheckResultOutcome::Pass);
    assert!(!evidence.reusable);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_salt_is_created_once_survives_a_restart_and_no_identity_holds_a_value() {
    let f = canonical_fixture().await;
    f.set_project_value("s3cret-value").await;
    // First use by many requests at once: one salt.
    let first_users = (0..8)
        .map(|_| {
            let store = f.store.clone();
            tokio::spawn(async move {
                super::policy::value_revision(&store, "FLAVOR", "s3cret-value").await
            })
        })
        .collect::<Vec<_>>();
    let mut revisions = Vec::new();
    for user in first_users {
        revisions.push(user.await.unwrap().unwrap());
    }
    let revision = revisions[0].clone();
    assert!(revisions.iter().all(|r| *r == revision));
    let salts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM system_setting WHERE key=?")
        .bind(super::policy::VALUE_REVISION_SALT_KEY)
        .fetch_one(f.store.pool())
        .await
        .unwrap();
    assert_eq!(salts, 1);
    let req = f.request("before-restart", "true").await;
    let stored = serde_json::to_string(&req.identity.inputs).unwrap();
    assert!(!stored.contains("s3cret-value"));
    assert!(matches!(
        &req.identity.inputs.environment["FLAVOR"],
        CheckEnvironmentValue::SecretRevision(stated) if *stated == revision
    ));
    assert_ne!(
        revision,
        super::policy::value_revision(&f.store, "FLAVOR", "other")
            .await
            .unwrap()
    );
    // A request stated before a restart is the same identity after it.
    let before = req.identity.key().unwrap();
    f.store.pool().close().await;
    let reopened = SqliteDb::new(
        db::create_sqlite_pool(&format!(
            "sqlite://{}",
            f.temp.path().join("checks.sqlite").display()
        ))
        .await
        .unwrap(),
    );
    assert_eq!(
        super::policy::value_revision(&reopened, "FLAVOR", "s3cret-value")
            .await
            .unwrap(),
        revision
    );
    let project = review::contract::project_environment(&reopened, "t")
        .await
        .unwrap()
        .env;
    let mut again = req.clone();
    again.identity.inputs =
        super::policy::digest_input(&reopened, req.identity.inputs.spec.clone(), &project)
            .await
            .unwrap();
    assert_eq!(again.identity.key().unwrap(), before);
    // Losing the salt only loses hits: the next identity is a different one.
    sqlx::query("DELETE FROM system_setting WHERE key=?")
        .bind(super::policy::VALUE_REVISION_SALT_KEY)
        .execute(reopened.pool())
        .await
        .unwrap();
    again.identity.inputs =
        super::policy::digest_input(&reopened, req.identity.inputs.spec.clone(), &project)
            .await
            .unwrap();
    assert!(again.identity.inputs.reusable_inputs());
    assert_ne!(again.identity.key().unwrap(), before);
}

/// Plan 3.4 F: the shared compiler cache is something Forge adds to a run
/// (`RUSTC_WRAPPER` and the wrapper's cache directory), per run and per
/// repository. It is not part of the environment the owner inherits or of
/// the Project environment, which are all the identity names, so turning
/// it on or off leaves a check's identity (and every stored result) alone.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_compiler_cache_wrapper_does_not_change_a_check_identity() {
    let f = canonical_fixture().await;
    let step = f.counting_step();
    let off = f.request("k", &step).await;
    assert!(off.identity.inputs.reusable_inputs());

    // A workspace root with the cache on, and a Task worktree in it that
    // really is handed the wrapper.
    let root = f.temp.path().join("ws");
    let worktree = root.join("task").join("repo");
    std::fs::create_dir_all(&worktree).unwrap();
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    std::fs::write(
        worktree.join(".git"),
        format!(
            "gitdir: {}\n",
            root.join(".repos/r/worktrees/task").display()
        ),
    )
    .unwrap();
    // The repository names the worktree back, as Git does; without it the
    // worktree's own `.git` file claims nothing.
    std::fs::create_dir_all(root.join(".repos/r/worktrees/task")).unwrap();
    std::fs::write(
        root.join(".repos/r/worktrees/task/gitdir"),
        format!("{}\n", worktree.join(".git").display()),
    )
    .unwrap();
    let wrapper = f.temp.path().join("kache");
    process_supervisor::executable::write_executable(&wrapper, "#!/bin/sh\nexec \"$@\"\n").unwrap();
    executors::compiler_cache::install(
        &root,
        Some(executors::compiler_cache::CompilerCache {
            kind: executors::compiler_cache::WrapperKind::of(&wrapper),
            wrapper: wrapper.clone(),
            dir: root.join(executors::compiler_cache::CACHE_DIR),
            max_bytes: 1 << 30,
        }),
    );
    let handed = executors::sandbox::SandboxEnv::for_task(&worktree);
    assert_eq!(
        handed.compiler_cache().map(|cache| cache.wrapper()),
        Some(wrapper.as_path())
    );

    let on = f.request("k", &step).await;
    executors::compiler_cache::install(&root, None);
    assert!(on.identity.inputs.reusable_inputs());
    assert_eq!(on.identity.inputs, off.identity.inputs);
    assert_eq!(on.identity.key().unwrap(), off.identity.key().unwrap());
}

#[tokio::test]
async fn older_daemon_receipt_without_failures_round_trips_and_certifies() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(
        runner
            .request(cacheable_request("older-daemon"))
            .await
            .unwrap(),
    );
    let run = admit(&store, &run).await;
    let deadline = store
        .check_worker_record(&run.id)
        .await
        .unwrap()
        .deadline_at
        .unwrap();
    store
        .record_check_dispatch(&fence(&run), &intent(), &db::now_rfc3339(), &deadline)
        .await
        .unwrap();
    let record = store.check_worker_record(&run.id).await.unwrap();
    let old = receipt(&record);
    let json = serde_json::to_string(&old).unwrap();
    assert!(!json.contains("failures"));
    let restored: CheckReceipt = serde_json::from_str(&json).unwrap();
    assert!(restored.commands[0].failures.is_empty());
    assert!(!restored.commands[0].failures_truncated);
    assert_eq!(serde_json::to_string(&restored).unwrap(), json);
    let evidence =
        super::receipt::validate_receipt(&run, record.dispatch.as_ref().unwrap(), &restored)
            .unwrap();
    assert!(evidence.commands[0].failures.is_empty());
    assert!(!serde_json::to_string(&evidence.commands[0])
        .unwrap()
        .contains("failures"));
    assert!(evidence.reusable);
}

#[tokio::test]
async fn daemon_receipt_failure_signatures_and_ansi_tails_reach_durable_evidence() {
    let (_temp, store, runner) = fixture().await;
    let run = scheduled(
        runner
            .request(cacheable_request("failure-signatures"))
            .await
            .unwrap(),
    );
    let run = admit(&store, &run).await;
    let deadline = store
        .check_worker_record(&run.id)
        .await
        .unwrap()
        .deadline_at
        .unwrap();
    store
        .record_check_dispatch(&fence(&run), &intent(), &db::now_rfc3339(), &deadline)
        .await
        .unwrap();
    let record = store.check_worker_record(&run.id).await.unwrap();
    let mut failed = receipt(&record);
    failed.outcome = CheckExecutionOutcome::Failed;
    let command = &mut failed.commands[0];
    command.exit_code = Some(1);
    command.outcome = CheckExecutionOutcome::Failed;
    command.failures = vec!["parser::empty".into(), "parser::unicode".into()];
    command.failures_truncated = true;
    command.stdout_tail = "\x1b[31mfailure\x1b[0m".into();
    command.stderr_tail = "\x1b[32mdetail\x1b[0m".into();
    let evidence =
        super::receipt::validate_receipt(&run, record.dispatch.as_ref().unwrap(), &failed).unwrap();
    assert_eq!(
        evidence.commands[0].failures,
        ["parser::empty", "parser::unicode"]
    );
    assert!(evidence.commands[0].failures_truncated);
    assert_eq!(evidence.commands[0].output_tail, "failure\ndetail");
    assert_eq!(evidence.commands[0].stderr_tail, "detail");
    let step: StepResultEntry =
        serde_json::from_value(serde_json::to_value(&evidence.commands[0]).unwrap()).unwrap();
    assert_eq!(step.failures, ["parser::empty", "parser::unicode"]);
    let mut old = serde_json::to_value(&step).unwrap();
    old.as_object_mut().unwrap().remove("failures");
    old.as_object_mut().unwrap().remove("failures_truncated");
    let old: StepResultEntry = serde_json::from_value(old).unwrap();
    assert!(old.failures.is_empty());
}
