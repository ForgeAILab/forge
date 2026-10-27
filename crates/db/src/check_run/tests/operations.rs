//! Operations repository tests.
use super::*;

/// The operator summary: counts per place a check can wait, with the age of
/// the oldest, and the day's runs against the requests a stored result
/// answered.
#[tokio::test]
async fn operator_summary_counts_backlogs_with_ages_and_the_days_reuse() {
    let db = fixture().await;
    assert_eq!(
        db.check_operator_summary().await.unwrap(),
        CheckOperatorSummary::default()
    );
    // One fixed instant: the rows are dated from it and the summary is read
    // as of it, so the ages do not depend on how long the test takes.
    let now = chrono::Utc::now();
    let ago = |seconds: i64| (now - chrono::Duration::seconds(seconds)).to_rfc3339();
    for (id, state, created, wait, admitted) in [
        ("q1", "queued", ago(30), None, None),
        ("q2", "queued", ago(90), None, None),
        ("s1", "queued", ago(900), Some(ago(600)), None),
        ("r1", "running", ago(500), None, Some(ago(400))),
        ("u1", "uncertain", ago(5000), None, Some(ago(4000))),
        ("done", "succeeded", ago(7200), None, Some(ago(7100))),
        ("old", "succeeded", ago(200_000), None, Some(ago(200_000))),
    ] {
        sqlx::query("INSERT INTO check_run(id,project_id,repo_id,commit_sha,spec_digest,identity_key,input_json,cacheable,state,operation_id,created_at,updated_at,capacity_wait_since,admitted_at) VALUES(?,'p','r','c','d',?,'{}',1,?,?,?,?,?,?)")
            .bind(id).bind(id).bind(state).bind(id).bind(&created).bind(&created).bind(wait).bind(admitted)
            .execute(db.pool()).await.unwrap();
    }
    sqlx::query("INSERT INTO check_result(id,run_id,identity_key,outcome,cleanup,certified,cacheable,steps_json,output_truncated,created_at) VALUES('x','done','done','pass','success',1,1,'[]',0,?)")
        .bind(ago(7000)).execute(db.pool()).await.unwrap();
    // One consumer waited for the run; two later requests hit its result;
    // a hit from two days ago is outside the window.
    for (id, created) in [
        ("waited", ago(7200)),
        ("hit-1", ago(3000)),
        ("hit-2", ago(60)),
    ] {
        sqlx::query("INSERT INTO check_consumer(id,project_id,repo_id,task_id,status_epoch,origin,request_key,identity_key,run_id,result_id,created_at) VALUES(?,'p','r','t',0,'entry',?,'done','done','x',?)")
            .bind(id).bind(id).bind(created).execute(db.pool()).await.unwrap();
    }
    sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at,awaited_consumer_id,suspended_until) VALUES('disk-step','t',1,'hooks','{}','k','k',1,'todo',1,'suspended',?,?,?,?,?)")
        .bind(ago(120)).bind(ago(120)).bind(ago(5)).bind(DISK_WAIT_SENTINEL).bind(ago(-30))
        .execute(db.pool()).await.unwrap();
    let summary = db
        .check_operator_summary_at(&now.to_rfc3339())
        .await
        .unwrap();
    // Julian-day arithmetic is floating point: a whole number of seconds may
    // truncate one short, never more.
    let near = |(count, age): CheckBacklogRow, expected: (i64, i64)| {
        assert_eq!(count, expected.0);
        let age = age.expect("an age");
        assert!(
            (expected.1 - 1..=expected.1).contains(&age),
            "{age} vs {expected:?}"
        );
    };
    near(summary.queued, (2, 90));
    near(summary.waiting_for_slot, (1, 600));
    near(summary.running, (2, 4000));
    near(summary.waiting_for_disk, (1, 120));
    assert_eq!((summary.runs_24h, summary.reuse_hits_24h), (6, 2));
}
