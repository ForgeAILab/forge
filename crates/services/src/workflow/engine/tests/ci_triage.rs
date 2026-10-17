use super::*;

const RED: &str = "printf 'test suite::base ... FAILED\\n'; exit 1";

async fn fixture(mode: &str, setup: &[&str], commands: &[&str]) -> FailedCiFixture {
    let mut f = failed_ci_fixture(3, FailurePolicy::Block).await;
    // Keep the Review at its entry verdict: dispatch/merge is tested separately.
    let state = f
        .workflow
        .states
        .iter_mut()
        .find(|s| s.name == "review")
        .unwrap();
    state.hooks.on_enter.clear();
    state.hooks.after_enter.clear();
    sqlx::query("UPDATE project SET workflow_definition=?,settings=json_set(settings,'$.review_ci_triage',?) WHERE id=?")
        .bind(serde_json::to_string(&f.workflow).unwrap()).bind(mode).bind(&f.task.project_id).execute(f.db.pool()).await.unwrap();
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"retry_budgets":{"review":3},"review":{"setup_steps":setup,"ci_steps":commands}}).to_string())
        .bind(&f.task.id).execute(f.db.pool()).await.unwrap();
    f
}
async fn enter(f: &FailedCiFixture) -> (db::Task, db::Review, serde_json::Value) {
    let task = TaskRepo::get_by_id(&*f.db, &f.task.id, false)
        .await
        .unwrap()
        .unwrap();
    let result = f
        .engine
        .workflow_execution()
        .transition_with_authority(
            &task.id,
            "review",
            task.version,
            &f.workflow,
            &api_types::Actor::system(api_types::SystemComponent::Workflow),
            "candidate completed",
            false,
            f.workflow_authority().await,
        )
        .await
        .unwrap();
    let task = drain_result(f.engine.clone(), result).await.task;
    let review = db::ReviewRepo::list_by_task(&*f.db, &f.task.id)
        .await
        .unwrap()
        .into_iter()
        .max_by_key(|r| r.attempt_number)
        .unwrap();
    let details = serde_json::from_str(&review.step_results_json).unwrap();
    (task, review, details)
}
async fn count(f: &FailedCiFixture) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM check_run")
        .fetch_one(f.db.pool())
        .await
        .unwrap()
}
async fn attention(f: &FailedCiFixture, category: &str) -> Vec<serde_json::Value> {
    sqlx::query_scalar::<_, String>(
        "SELECT details_json FROM attention_projection WHERE attention_type=? AND status='open'",
    )
    .bind(category)
    .fetch_all(f.db.pool())
    .await
    .unwrap()
    .iter()
    .map(|s| serde_json::from_str(s).unwrap())
    .collect()
}
#[tokio::test]
async fn ci_triage_flaky_step_passes_on_rerun_without_a_base_run() {
    let f = fixture("full", &[], &["if [ ! -f .flake ]; then touch .flake; printf 'test suite::flake ... FAILED\\n'; exit 1; fi"]).await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Passed, "{details}");
    assert_eq!(details["ci_steps"][0]["exit_code"], 1);
    assert_eq!(details["ci_steps"][0]["triage"]["class"], "flaky");
    assert_eq!(count(&f).await, 2);
    assert_eq!(attention(&f, "flaky_check").await.len(), 1);
    let false_passes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM check_result WHERE outcome='fail' AND certified=1",
    )
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(false_passes, 0);
}
#[tokio::test]
async fn ci_triage_consistent_failures_go_to_base_and_red_base_continues_to_reviewer() {
    let f = fixture("full", &["true"], &[RED]).await;
    assign_agent_role(&f.db, &f.task.id, "reviewer", "reviewer-for-entry").await;
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Running, "{details}");
    let triage = &details["ci_steps"][0]["triage"];
    assert_eq!(triage["base_status"], "available", "{details}");
    assert_eq!(triage["failures"][0]["class"], "pre_existing");
    assert_eq!(triage["blocking"], false);
    assert_eq!(count(&f).await, 3);
    assert_eq!(attention(&f, "base_check_failing").await.len(), 1);
    assert_eq!(
        db::budget::spent(f.db.pool(), &f.task.id, "review")
            .await
            .unwrap(),
        0
    );
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    assert!(ledger
        .iter()
        .all(|entry| entry.status == api_types::ReviewFindingStatus::PreExisting));
}
#[tokio::test]
async fn ci_triage_one_introduced_failure_reaches_coder() {
    let f = fixture("full", &["true"], &["printf 'test suite::base ... FAILED\\n'; if [ -f introduced ]; then printf 'test suite::new ... FAILED\\n'; fi; exit 1"]).await;
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    std::fs::write(path.join("introduced"), "introduced").unwrap();
    run_git(&path, &["add", "introduced"]);
    run_git(
        &path,
        &[
            "-c",
            "user.name=Forge Test",
            "-c",
            "user.email=test@forge.dev",
            "commit",
            "-m",
            "candidate",
        ],
    );
    let (task, review, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress", "{details}");
    assert_eq!(review.status, db::ReviewStatus::Failed);
    let ledger = db::review_findings::finding_ledger(&[review]).unwrap();
    let blocking: Vec<_> = ledger
        .iter()
        .filter(|entry| entry.severity == api_types::ReviewFindingSeverity::Blocking)
        .collect();
    assert_eq!(blocking.len(), 1, "{ledger:?}");
    assert_eq!(blocking[0].observed.as_deref(), Some("suite::new"));
}
#[tokio::test]
async fn ci_triage_report_blocks_pre_existing_failures() {
    let f = fixture("report", &["true"], &[RED]).await;
    let (task, review, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(review.status, db::ReviewStatus::Failed);
    assert_eq!(
        details["ci_steps"][0]["triage"]["failures"][0]["class"],
        "pre_existing"
    );
    assert_eq!(count(&f).await, 3);
}
#[tokio::test]
async fn ci_triage_off_does_not_rerun() {
    let f = fixture("off", &["true"], &[RED]).await;
    let (task, _, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress");
    assert!(details["ci_steps"][0].get("triage").is_none());
    assert_eq!(count(&f).await, 1);
}
#[tokio::test]
async fn ci_triage_no_setup_is_base_unavailable_and_blocks() {
    let f = fixture("full", &[], &[RED]).await;
    let (task, _, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(
        details["ci_steps"][0]["triage"]["base_status"],
        "base_unavailable"
    );
    assert!(details["ci_steps"][0]["triage"]["reason"]
        .as_str()
        .unwrap()
        .contains("setup_steps"));
    assert_eq!(count(&f).await, 2);
}
#[tokio::test]
async fn ci_triage_install_failure_is_invalid_and_blocks() {
    let f = fixture("full", &["exit 1"], &[RED]).await;
    let (task, _, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(
        details["ci_steps"][0]["triage"]["base_status"],
        "base_run_invalid"
    );
    assert!(details["ci_steps"][0]["triage"]["failures"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(details["ci_steps"][0]["triage"]["class"].is_null());
    assert_eq!(count(&f).await, 3);
}
#[tokio::test]
async fn ci_triage_signatureless_red_base_still_blocks() {
    let f = fixture("full", &["true"], &["exit 1"]).await;
    let (task, _, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(
        details["ci_steps"][0]["triage"]["class"],
        "pre_existing_unverified"
    );
}
#[tokio::test]
async fn ci_triage_nonblocking_step_does_not_skip_later_introduced_failure() {
    let f = fixture(
        "full",
        &["true"],
        &[
            RED,
            "if [ -f introduced ]; then printf 'test later ... FAILED\\n'; exit 1; fi",
        ],
    )
    .await;
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    std::fs::write(path.join("introduced"), "introduced").unwrap();
    run_git(&path, &["add", "introduced"]);
    run_git(
        &path,
        &[
            "-c",
            "user.name=Forge Test",
            "-c",
            "user.email=test@forge.dev",
            "commit",
            "-m",
            "candidate",
        ],
    );
    let (task, _, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress", "{details}");
    assert_eq!(details["ci_steps"].as_array().unwrap().len(), 2);
    assert_eq!(details["ci_steps"][0]["triage"]["blocking"], false);
    assert_eq!(details["ci_steps"][1]["triage"]["blocking"], true);
}

#[tokio::test]
async fn ci_triage_base_uses_candidate_project_env_assets_and_build_environment() {
    let f = fixture("full", &["true"], &[RED]).await;
    let source = f._repo_dir.path().join("host-asset");
    std::fs::write(&source, "asset").unwrap();
    let build_witness = f._repo_dir.path().join("build-witness");
    let command = format!("printf '%s' \"$CARGO_TARGET_DIR:$CARGO_BUILD_JOBS\" > '{}'; printf 'test suite::base ... FAILED\\n'; exit 1", build_witness.display());
    let setup = format!("test \"$TRIAGE_VALUE\" = ok && test \"$(cat assets/fixture)\" = asset && test \"$CARGO_TARGET_DIR:$CARGO_BUILD_JOBS\" = \"$(cat '{}')\"", build_witness.display());
    sqlx::query(
        "UPDATE project SET settings=json_set(settings,'$.environment',json(?)) WHERE id=?",
    )
    .bind(
        json!({"env":{"TRIAGE_VALUE":"ok"},"assets":[{"source":source,"target":"assets/fixture"}]})
            .to_string(),
    )
    .bind(&f.task.project_id)
    .execute(f.db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE task SET task_state_config=? WHERE id=?")
        .bind(json!({"review":{"setup_steps":[setup],"ci_steps":[command]}}).to_string())
        .bind(&f.task.id)
        .execute(f.db.pool())
        .await
        .unwrap();
    let (_, review, details) = enter(&f).await;
    assert_eq!(review.status, db::ReviewStatus::Passed, "{details}");
    assert_eq!(
        details["ci_steps"][0]["triage"]["base_status"], "available",
        "{details}"
    );
}

#[tokio::test]
async fn ci_triage_candidate_signatures_with_signatureless_base_are_invalid() {
    let f = fixture(
        "full",
        &["true"],
        &["if [ -f introduced ]; then printf 'test suite::new ... FAILED\\n'; fi; exit 1"],
    )
    .await;
    let path = std::path::PathBuf::from(f.workspace.embedded_worktree_path_for_backend());
    std::fs::write(path.join("introduced"), "introduced").unwrap();
    run_git(&path, &["add", "introduced"]);
    run_git(
        &path,
        &[
            "-c",
            "user.name=Forge Test",
            "-c",
            "user.email=test@forge.dev",
            "commit",
            "-m",
            "candidate",
        ],
    );
    let (task, _, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress");
    assert_eq!(
        details["ci_steps"][0]["triage"]["base_status"],
        "base_run_invalid"
    );
}

#[tokio::test]
async fn ci_triage_rerun_failing_with_other_signatures_goes_to_the_base() {
    // A fail-fast runner reports whichever failure comes first: the rerun
    // fails too, with another test. Nothing here is a flake.
    let f = fixture(
        "full",
        &["true"],
        &["if [ -f .seen ]; then printf 'test suite::second ... FAILED\\n'; else touch .seen; printf 'test suite::first ... FAILED\\n'; fi; exit 1"],
    )
    .await;
    let (task, review, details) = enter(&f).await;
    assert_eq!(task.status, "in_progress", "{details}");
    assert_eq!(review.status, db::ReviewStatus::Failed);
    let triage = &details["ci_steps"][0]["triage"];
    assert_eq!(triage["base_status"], "available", "{details}");
    assert_eq!(triage["blocking"], true);
    let classes: Vec<_> = triage["failures"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["signature"].as_str().unwrap(),
                f["class"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        classes,
        [
            ("suite::first", "pre_existing"),
            ("suite::second", "introduced")
        ]
    );
    assert_eq!(count(&f).await, 3);
}

#[tokio::test]
async fn ci_triage_base_whose_setup_failed_is_not_reused() {
    // A registry outage during install says nothing about the base commit:
    // the next triage on that base runs setup again instead of joining it.
    let marker = tempfile::tempdir().unwrap();
    let setup = format!("test -f '{}'", marker.path().join("registry-up").display());
    let f = fixture("full", &[setup.as_str()], &[RED]).await;
    let (_, _, details) = enter(&f).await;
    assert_eq!(
        details["ci_steps"][0]["triage"]["base_status"], "base_run_invalid",
        "{details}"
    );
    std::fs::write(marker.path().join("registry-up"), "up").unwrap();
    let (_, review, details) = enter(&f).await;
    let triage = &details["ci_steps"][0]["triage"];
    assert_eq!(triage["base_status"], "available", "{details}");
    assert_eq!(triage["failures"][0]["class"], "pre_existing");
    assert_eq!(review.status, db::ReviewStatus::Passed, "{details}");
    let bases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM check_run WHERE json_extract(input_json,'$.spec.commands[0].id')='triage-base:setup:0'")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    assert_eq!(bases, 2);
}

#[tokio::test]
async fn ci_triage_full_changes_cached_identity_and_documents_upgrade() {
    let f = fixture("full", &[], &[RED, "true"]).await;
    let (_, _, details) = enter(&f).await;
    assert_eq!(details["ci_steps"].as_array().unwrap().len(), 2);
    let id: String = sqlx::query_scalar("SELECT id FROM check_run ORDER BY created_at,id LIMIT 1")
        .fetch_one(f.db.pool())
        .await
        .unwrap();
    let run = db::CheckRunRepo::check_run(&*f.db, &id)
        .await
        .unwrap()
        .unwrap();
    let mut old = run.identity.clone();
    for command in &mut old.inputs.spec.commands {
        assert_eq!(
            command.failure_policy,
            api_types::CheckFailurePolicy::Continue
        );
        command.failure_policy = api_types::CheckFailurePolicy::StopBundle;
    }
    assert_ne!(
        run.identity.key().unwrap(),
        old.key().unwrap(),
        "the full-mode policy must miss old cached passes"
    );
    let changelog = include_str!("../../../../../../CHANGELOG.md");
    assert!(changelog.contains("cached CI passes are missed once after upgrade"));
}
