use crate::recovery::test_support::writer_fixture::{
    assert_main_parity, ClockMode, Comparison, Fixture,
};
use serde_json::json;

#[tokio::test]
async fn merge_annotations() {
    let _clock = db::test_clock::freeze("2026-10-25T00:00:00.123456+00:00".parse().unwrap());
    assert_main_parity(
        "merge-budget/raw",
        &json!(super::merge_budget_annotation("exhausted")),
        Comparison::VerbatimWithProjectionHash,
    );
    let f = Fixture::with_clock("merging", ClockMode::PreserveCaller).await;
    assert_main_parity(
        "manual-merge/raw",
        &json!(super::manual_merge_recovery_annotation(
            &f.ctx,
            api_types::FailureKind::MergeConflict,
            "recover",
            "user",
        )),
        Comparison::VerbatimWithProjectionHash,
    );
}

#[tokio::test]
async fn merge_failure_stores_annotations() {
    for kind in [
        api_types::FailureKind::MergeConflict,
        api_types::FailureKind::CiFailed,
    ] {
        crate::recovery::test_support::writer_fixture::case!(
            &format!("merge-failure-{kind}"),
            "merging",
            f,
            {
                let mut ctx = f.ctx.clone();
                ctx.state_config = serde_json::json!({"retry_budgets":{"merge_fix":0}});
                let result =
                    super::merge_failure_result(&ctx, &f.task, "conflicts".into(), kind).await;
                assert!(
                    matches!(result, crate::workflow::HookResult::Ok),
                    "{result:?}"
                );
                let row = db::TaskRepo::get_by_id(&*f.db, &f.task.id, false)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(row.error_annotation.is_some());
                assert!(row.blocked_json.is_some());
            }
        );
    }
}
