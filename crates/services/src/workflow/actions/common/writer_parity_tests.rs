use crate::recovery::test_support::writer_fixture::case;

#[tokio::test]
async fn merge_error_and_block() {
    for kind in [
        api_types::FailureKind::MergeConflict,
        api_types::FailureKind::CiFailed,
    ] {
        case!(&format!("detected-{kind}"), "merging", f, {
            super::persist_merge_error(&f.ctx, &f.task, kind, "conflicts")
                .await
                .unwrap();
        });
    }
    case!("common-block", "merging", f, {
        super::block_task(
            &f.ctx,
            &f.task,
            "budget",
            api_types::FailureKind::MergeFixBudgetExhausted,
            Some("merge"),
        )
        .await
        .unwrap();
    });
}

#[tokio::test]
async fn manual_review_metadata() {
    case!("manual-review-metadata", "review", f, {
        super::set_review_awaiting_human_metadata(&f.ctx)
            .await
            .unwrap();
    });
}
