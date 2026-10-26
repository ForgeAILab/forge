use super::*;
use crate::demo::lane_fixture as fixture;

#[tokio::test]
async fn lane_b_demo_annotations_match_main_literal_bytes() {
    let _clock = crate::recovery::test_support::conditions::freeze_clock();
    let mut f = fixture::Fixture::new("in_progress").await;
    let db = f.db.clone();
    install_demo_data(&db).await.unwrap();
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT title,error_annotation FROM task WHERE error_annotation IS NOT NULL ORDER BY title",
    )
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(rows, vec![
        ("Migrate user sessions to Redis".into(), r#"{"type":"workspace_reset_required","message":"workspace reset required: task branch no longer exists in repo"}"#.into()),
        ("Upgrade OpenAPI spec generator".into(), r#"{"type":"workspace_error","message":"workspace error: repository directory not found on disk"}"#.into()),
    ]);
    let ids: Vec<String> =
        sqlx::query_scalar("SELECT id FROM task WHERE error_annotation IS NOT NULL ORDER BY title")
            .fetch_all(db.pool())
            .await
            .unwrap();
    for (i, id) in ids.iter().enumerate() {
        f.task = TaskRepo::get_by_id(&*db, id, false).await.unwrap().unwrap();
        f.step = crate::recovery::test_support::conditions::claim(&db, &f.task).await;
        db::task_writer::in_task_step(f.step.clone(), async {
            fixture::after(&f, if i == 0 { "demo-reset" } else { "demo-error" }).await;
        })
        .await;
    }
}
