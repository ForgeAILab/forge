use super::super::tests::source;
use super::*;
use serde_json::json;

#[tokio::test]
async fn prior_review_prompt_includes_window_open_ids_and_an_8k_report_slice() {
    use db::TransitionLogRepo;
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = SqliteDb::new(pool);
    let start = "2026-10-15T00:00:00Z";
    sqlx::query(
        "INSERT INTO project(id,name,created_at,updated_at) VALUES('project','Review',?,?)",
    )
    .bind(start)
    .bind(start)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('task','project','Review','review',?,?)")
            .bind(start).bind(start).execute(db.pool()).await.unwrap();
    for id in ["candidate", "prior-reviewer"] {
        sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES(?,'task','reviewer','completed',?,?)")
                .bind(id).bind(start).bind(start).execute(db.pool()).await.unwrap();
    }
    let contract = ReviewContract {
        execution_id: "prior-reviewer".into(),
        policy: REVIEW_CONFORMANCE_POLICY.into(),
        commit_sha: "same".into(),
        base_sha: "same".into(),
        candidate_changed_paths: Vec::new(),
        context: context_from_source(&source()).unwrap(),
        check_results: Vec::new(),
        digest: "prior-contract".into(),
    };
    // A historical contract fixture lets this test exercise prompt reads
    // without requiring Git or admitting a new reviewer execution.
    sqlx::query("INSERT INTO execution_review_contract VALUES(?,?,?,?,?,?)")
        .bind(&contract.execution_id)
        .bind("task")
        .bind(&contract.digest)
        .bind(&contract.context.source_digest)
        .bind(serde_json::to_string(&contract).unwrap())
        .bind(start)
        .execute(db.pool())
        .await
        .unwrap();
    // Two short findings, then 48 at the 1,000-character field limit.
    let mut findings = vec![json!({"summary":"one"}), json!({"summary":"two"})];
    findings.extend(
        (3..=50).map(
            |n| json!({"summary":format!("{n}{}", "S".repeat(1_000)),"where":"W".repeat(1_000)}),
        ),
    );
    let block = json!({"result":"fail","reason":"observed problem","findings":findings});
    let assessment = parse_assessment(&format!("{}END\n{block}", "R".repeat(9000))).unwrap();
    assert_eq!(assessment.findings.len(), 50);
    let conformance = ReviewConformance {
        status: ConformanceStatus::Failed,
        contract: Some(contract.clone()),
        assessment: Some(assessment),
        checks: Vec::new(),
        reason: Some("observed problem".into()),
    };
    db.record_review_conformance(&conformance).await.unwrap();
    for (attempt, details) in [
        (1, json!({"conformance":conformance})),
        (
            2,
            json!({"ci_steps":[{"index":2,"command":"cargo test","exit_code":101,"stderr_tail":"failed"}]}),
        ),
    ] {
        let review = ReviewRepo::create(
            &db,
            db::CreateReview {
                id: format!("review-{attempt}"),
                task_id: "task".into(),
                execution_id: "candidate".into(),
                attempt_number: attempt,
                status: db::ReviewStatus::Failed,
                step_results_json: details.to_string(),
                started_at: format!("2026-10-15T00:00:0{attempt}Z"),
                created_at: format!("2026-10-15T00:00:0{attempt}Z"),
                updated_at: start.into(),
            },
        )
        .await
        .unwrap();
        if attempt == 1 {
            sqlx::query("UPDATE review SET reviewer_execution_id='prior-reviewer' WHERE id=?")
                .bind(review.id)
                .execute(db.pool())
                .await
                .unwrap();
        }
    }
    let directory = tempfile::tempdir().unwrap();
    let section = prior_review_section(
        &db,
        "task",
        "next-reviewer",
        &directory.path().to_path_buf(),
        &contract,
        32 * 1024,
    )
    .await
    .unwrap();
    for expected in [
        "F1: one",
        "F2: two",
        "CI-2: cargo test failed",
        "copy its id into findings",
        "in resolved",
        "Frozen requirement ids:",
        "After the first review in this window",
        "candidate_changed_paths",
        "same_cause_as",
        "[report truncated]",
    ] {
        assert!(section.contains(expected), "missing {expected}");
    }
    for requirement in &contract.context.requirements {
        assert!(section.contains(&requirement.id));
    }
    assert!(section.contains(&"R".repeat(8192)));
    assert!(!section.contains(&"R".repeat(8193)));
    assert!(!section.contains("END"));
    // 50 open findings at the field limits stay bounded, and every id is
    // offered for continuation.
    let overflow: Vec<&str> = section
        .lines()
        .find_map(|line| line.strip_prefix("Also open (details in the Review history): "))
        .map(|ids| ids.split(", ").collect())
        .unwrap_or_default();
    for n in 1..=50 {
        let id = format!("F{n}");
        assert!(
            section.contains(&format!("- {id}: ")) || overflow.contains(&id.as_str()),
            "missing {id}"
        );
    }
    assert!(!section.contains(&"S".repeat(201)));
    assert!(section.len() < 18 * 1024, "{}", section.len());
    TransitionLogRepo::insert_recovery_marker(
        &db,
        "task",
        "review",
        "reset_retry_window",
        "test",
        "new window",
    )
    .await
    .unwrap();
    // A recovery reset restarts the ledger, not the reviewer's context:
    // the previous verdict and report stay; no stale ids are offered.
    let section = prior_review_section(
        &db,
        "task",
        "next-reviewer",
        &directory.path().to_path_buf(),
        &contract,
        32 * 1024,
    )
    .await
    .unwrap();
    assert!(section.contains("Previous review (attempt 1, result fail"));
    assert!(section.contains(&"R".repeat(8192)));
    assert!(!section.contains("Open findings"));
    assert!(!section.contains("F1: one"));
}

#[tokio::test]
async fn reviewer_diff_section_inlines_a_small_diff_and_summarizes_a_large_one() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path();
    git::init(path).await.unwrap();
    tokio::fs::write(path.join("lib.txt"), "before\n")
        .await
        .unwrap();
    let base = git::commit_all(path, "base").await.unwrap();
    tokio::fs::write(path.join("lib.txt"), "after\n")
        .await
        .unwrap();
    let candidate = git::commit_all(path, "candidate").await.unwrap();
    let range = format!("{base}..{candidate}");

    let inlined = diff_section(path, &range, MAX_INLINE_DIFF_BYTES)
        .await
        .unwrap();
    assert!(inlined.contains("lib.txt | 2"));
    assert!(inlined.contains("-before\n+after"));

    tokio::fs::write(path.join("lib.txt"), "changed line\n".repeat(200))
        .await
        .unwrap();
    let large = git::commit_all(path, "large").await.unwrap();
    let summarized = diff_section(path, &format!("{base}..{large}"), 1024)
        .await
        .unwrap();
    assert!(summarized.contains("lib.txt"));
    assert!(summarized.contains("too large to inline"));
    assert!(!summarized.contains("+changed line"));
}
